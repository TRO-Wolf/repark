use std::num::FpCategory;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeIterator, TreeNodeRecursion,
};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::logical_expr::expr::{Case, GroupingSet, InList, TryCast};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{
    Between, BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, Operator, and, lit, or,
};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyExpressions};
use datafusion::optimizer::{AnalyzerRule, ApplyOrder, OptimizerConfig, OptimizerRule};

const RULE_NAME: &str = "signed_zero_literals";

#[derive(Debug, Default)]
pub(crate) struct SignedZeroLiterals;

impl AnalyzerRule for SignedZeroLiterals {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|node| rewrite_node(node, None))
            .data()
    }

    fn name(&self) -> &str {
        RULE_NAME
    }
}

impl OptimizerRule for SignedZeroLiterals {
    fn name(&self) -> &str {
        RULE_NAME
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let folder = ExprSimplifier::new(
            SimplifyContext::builder()
                .with_schema(Arc::new(DFSchema::empty()))
                .with_config_options(config.options())
                .with_query_execution_start_time(config.query_execution_start_time())
                .build(),
        );
        let rewritten = rewrite_node(plan, Some(&folder))?;
        if !rewritten.transformed {
            return Ok(rewritten);
        }
        let simplified = SimplifyExpressions::default().rewrite(rewritten.data, config)?;
        Ok(Transformed::yes(simplified.data))
    }
}

#[derive(Clone, Copy)]
enum Form {
    Below,
    AtMost,
    Above,
    AtLeast,
    Zero,
    NonZero,
    NullSafeZero,
    DistinctFromZero,
}

impl Form {
    fn of(op: Operator) -> Option<Self> {
        match op {
            Operator::Lt => Some(Self::Below),
            Operator::LtEq => Some(Self::AtMost),
            Operator::Gt => Some(Self::Above),
            Operator::GtEq => Some(Self::AtLeast),
            Operator::Eq => Some(Self::Zero),
            Operator::NotEq => Some(Self::NonZero),
            Operator::IsNotDistinctFrom => Some(Self::NullSafeZero),
            Operator::IsDistinctFrom => Some(Self::DistinctFromZero),
            _ => None,
        }
    }

    fn swapped(self) -> Self {
        match self {
            Self::Below => Self::Above,
            Self::AtMost => Self::AtLeast,
            Self::Above => Self::Below,
            Self::AtLeast => Self::AtMost,
            symmetric => symmetric,
        }
    }

    fn single_bound(self) -> Option<(Operator, bool)> {
        match self {
            Self::Below => Some((Operator::Lt, true)),
            Self::AtMost => Some((Operator::LtEq, false)),
            Self::Above => Some((Operator::Gt, false)),
            Self::AtLeast => Some((Operator::GtEq, true)),
            _ => None,
        }
    }
}

struct ZeroComparison {
    zero_on_right: bool,
    zero_type: DataType,
    form: Form,
}

fn rewrite_node(
    plan: LogicalPlan,
    folder: Option<&ExprSimplifier>,
) -> Result<Transformed<LogicalPlan>> {
    let mut carries = false;
    plan.apply_expressions(|expr| {
        carries = carries_rewrite(expr, folder.is_some())?;
        Ok(if carries {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    if !carries {
        return Ok(Transformed::no(plan));
    }
    let preserver = NamePreserver::new(&plan);
    plan.map_expressions(|expr| match expr {
        Expr::GroupingSet(set) => {
            Ok(rewrite_grouping_set(set, &preserver, folder)?.update_data(Expr::GroupingSet))
        }
        other => rewrite_named(other, &preserver, folder),
    })
}

fn rewrite_named(
    expr: Expr,
    preserver: &NamePreserver,
    folder: Option<&ExprSimplifier>,
) -> Result<Transformed<Expr>> {
    if !carries_rewrite(&expr, folder.is_some())? {
        return Ok(Transformed::no(expr));
    }
    let saved = preserver.save(&expr);
    Ok(expr
        .transform_up(|node| Ok(rewrite_expr(fold_operands(node, folder))))?
        .update_data(|node| saved.restore(node)))
}

fn rewrite_grouping_set(
    set: GroupingSet,
    preserver: &NamePreserver,
    folder: Option<&ExprSimplifier>,
) -> Result<Transformed<GroupingSet>> {
    Ok(match set {
        GroupingSet::Rollup(exprs) => {
            rewrite_members(exprs, preserver, folder)?.update_data(GroupingSet::Rollup)
        }
        GroupingSet::Cube(exprs) => {
            rewrite_members(exprs, preserver, folder)?.update_data(GroupingSet::Cube)
        }
        GroupingSet::GroupingSets(sets) => sets
            .into_iter()
            .map_until_stop_and_collect(|exprs| rewrite_members(exprs, preserver, folder))?
            .update_data(GroupingSet::GroupingSets),
    })
}

fn rewrite_members(
    exprs: Vec<Expr>,
    preserver: &NamePreserver,
    folder: Option<&ExprSimplifier>,
) -> Result<Transformed<Vec<Expr>>> {
    exprs
        .into_iter()
        .map_until_stop_and_collect(|expr| rewrite_named(expr, preserver, folder))
}

fn carries_rewrite(expr: &Expr, folds: bool) -> Result<bool> {
    expr.exists(|node| Ok(rewrites(node) || (folds && has_constant_operand(node))))
}

fn comparison_operands(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryExpr(binary) if Form::of(binary.op).is_some() => {
            vec![binary.left.as_ref(), binary.right.as_ref()]
        }
        Expr::InList(in_list) => std::iter::once(in_list.expr.as_ref())
            .chain(in_list.list.iter())
            .collect(),
        Expr::Between(between) => vec![
            between.expr.as_ref(),
            between.low.as_ref(),
            between.high.as_ref(),
        ],
        Expr::Case(Case {
            expr: Some(operand),
            when_then_expr,
            ..
        }) => std::iter::once(operand.as_ref())
            .chain(when_then_expr.iter().map(|(when, _)| when.as_ref()))
            .collect(),
        _ => Vec::new(),
    }
}

fn has_constant_operand(expr: &Expr) -> bool {
    comparison_operands(expr)
        .into_iter()
        .any(is_constant_float_operand)
}

fn is_constant_float_operand(expr: &Expr) -> bool {
    if matches!(expr, Expr::Literal(_, _)) || expr.is_volatile() {
        return false;
    }
    let constant = !expr
        .exists(|node| {
            Ok(!matches!(
                node,
                Expr::Literal(_, _)
                    | Expr::Alias(_)
                    | Expr::Cast(_)
                    | Expr::TryCast(_)
                    | Expr::Negative(_)
                    | Expr::BinaryExpr(_)
                    | Expr::ScalarFunction(_)
            ))
        })
        .unwrap_or(true);
    constant
        && matches!(
            expr.get_type(&DFSchema::empty()),
            Ok(DataType::Float32 | DataType::Float64)
        )
}

fn fold_operand(expr: Expr, folder: &ExprSimplifier) -> Expr {
    if !is_constant_float_operand(&expr) {
        return expr;
    }
    match folder.simplify(expr.clone()) {
        Ok(literal @ Expr::Literal(_, _)) if zero_type(&literal).is_some() => literal,
        _ => expr,
    }
}

fn fold_boxed(expr: Expr, folder: &ExprSimplifier) -> Box<Expr> {
    Box::new(fold_operand(expr, folder))
}

fn fold_operands(expr: Expr, folder: Option<&ExprSimplifier>) -> Expr {
    let Some(folder) = folder else {
        return expr;
    };
    if !has_constant_operand(&expr) {
        return expr;
    }
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => Expr::BinaryExpr(BinaryExpr::new(
            fold_boxed(*left, folder),
            op,
            fold_boxed(*right, folder),
        )),
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => Expr::InList(InList::new(
            fold_boxed(*expr, folder),
            list.into_iter()
                .map(|item| fold_operand(item, folder))
                .collect(),
            negated,
        )),
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => Expr::Between(Between::new(
            fold_boxed(*expr, folder),
            negated,
            fold_boxed(*low, folder),
            fold_boxed(*high, folder),
        )),
        Expr::Case(Case {
            expr: Some(operand),
            when_then_expr,
            else_expr,
        }) => Expr::Case(Case {
            expr: Some(fold_boxed(*operand, folder)),
            when_then_expr: when_then_expr
                .into_iter()
                .map(|(when, then)| (fold_boxed(*when, folder), then))
                .collect(),
            else_expr,
        }),
        other => other,
    }
}

fn rewrites(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => zero_comparison(binary).is_some_and(|comparison| {
            let operand = if comparison.zero_on_right {
                &binary.left
            } else {
                &binary.right
            };
            match comparison.form.single_bound() {
                Some((_, negative)) => {
                    let canonical = signed_zero(&comparison.zero_type, negative);
                    !(comparison.zero_on_right && is_literal(&binary.right, &canonical))
                }
                None => !operand.is_volatile(),
            }
        }),
        Expr::InList(in_list) => {
            (zero_type(&in_list.expr).is_some() || in_list.list.iter().any(is_zero_operand))
                && !in_list.expr.is_volatile()
                && !in_list.list.iter().any(Expr::is_volatile)
        }
        Expr::Between(between) => {
            [&between.expr, &between.low, &between.high]
                .into_iter()
                .any(|side| is_zero_operand(side))
                && !between.expr.is_volatile()
        }
        Expr::Case(case) => case.expr.as_ref().is_some_and(|operand| {
            !operand.is_volatile()
                && (is_zero_operand(operand)
                    || case
                        .when_then_expr
                        .iter()
                        .any(|(when, _)| is_zero_operand(when)))
        }),
        _ => false,
    }
}

fn rewrite_expr(expr: Expr) -> Transformed<Expr> {
    if !rewrites(&expr) {
        return Transformed::no(expr);
    }
    Transformed::yes(match expr {
        Expr::BinaryExpr(binary) => rewrite_binary(binary),
        Expr::InList(in_list) => rewrite_in_list(in_list),
        Expr::Between(between) => rewrite_between(between),
        Expr::Case(case) => rewrite_case(case),
        other => other,
    })
}

fn zero_comparison(binary: &BinaryExpr) -> Option<ZeroComparison> {
    let form = Form::of(binary.op)?;
    if let Some(zero_type) = zero_type(&binary.right) {
        return Some(ZeroComparison {
            zero_on_right: true,
            zero_type,
            form,
        });
    }
    Some(ZeroComparison {
        zero_on_right: false,
        zero_type: zero_type(&binary.left)?,
        form: form.swapped(),
    })
}

fn rewrite_binary(binary: BinaryExpr) -> Expr {
    let Some(comparison) = zero_comparison(&binary) else {
        return Expr::BinaryExpr(binary);
    };
    let operand = if comparison.zero_on_right {
        *binary.left
    } else {
        *binary.right
    };
    bound_form(operand, comparison.form, &comparison.zero_type)
}

fn bound_form(operand: Expr, form: Form, zero_type: &DataType) -> Expr {
    if let Some((op, negative)) = form.single_bound() {
        return compare(operand, op, signed_zero(zero_type, negative));
    }
    match form {
        Form::Zero => is_zero(operand, zero_type),
        Form::NullSafeZero => and(operand.clone().is_not_null(), is_zero(operand, zero_type)),
        Form::DistinctFromZero => or(operand.clone().is_null(), is_non_zero(operand, zero_type)),
        _ => is_non_zero(operand, zero_type),
    }
}

fn is_zero(operand: Expr, zero_type: &DataType) -> Expr {
    and(
        compare(
            operand.clone(),
            Operator::GtEq,
            signed_zero(zero_type, true),
        ),
        compare(operand, Operator::LtEq, signed_zero(zero_type, false)),
    )
}

fn is_non_zero(operand: Expr, zero_type: &DataType) -> Expr {
    or(
        compare(operand.clone(), Operator::Lt, signed_zero(zero_type, true)),
        compare(operand, Operator::Gt, signed_zero(zero_type, false)),
    )
}

fn rewrite_in_list(in_list: InList) -> Expr {
    let InList {
        expr,
        list,
        negated,
    } = in_list;
    let op = if negated {
        Operator::NotEq
    } else {
        Operator::Eq
    };
    if zero_type(&expr).is_some() {
        let mut comparisons = list
            .into_iter()
            .map(|item| rewrite_binary(BinaryExpr::new(Box::new(item), op, expr.clone())));
        let first = comparisons.next();
        return match first {
            Some(first) => comparisons.fold(first, |joined, next| {
                if negated {
                    and(joined, next)
                } else {
                    or(joined, next)
                }
            }),
            None => Expr::InList(InList::new(expr, Vec::new(), negated)),
        };
    }
    let Some(zero) = list.iter().find_map(zero_type) else {
        return Expr::InList(InList::new(expr, list, negated));
    };
    let rest: Vec<Expr> = list
        .into_iter()
        .filter(|item| !is_zero_operand(item))
        .collect();
    let form = if negated {
        is_non_zero(*expr.clone(), &zero)
    } else {
        is_zero(*expr.clone(), &zero)
    };
    if rest.is_empty() {
        return form;
    }
    let remaining = Expr::InList(InList::new(expr, rest, negated));
    if negated {
        and(remaining, form)
    } else {
        or(remaining, form)
    }
}

fn rewrite_between(between: Between) -> Expr {
    let Between {
        expr,
        negated,
        low,
        high,
    } = between;
    let (low_op, high_op) = if negated {
        (Operator::Lt, Operator::Gt)
    } else {
        (Operator::GtEq, Operator::LtEq)
    };
    let low = rewrite_binary(BinaryExpr::new(expr.clone(), low_op, low));
    let high = rewrite_binary(BinaryExpr::new(expr, high_op, high));
    if negated {
        or(low, high)
    } else {
        and(low, high)
    }
}

fn rewrite_case(case: Case) -> Expr {
    let Case {
        expr,
        when_then_expr,
        else_expr,
    } = case;
    let Some(operand) = expr else {
        return Expr::Case(Case {
            expr: None,
            when_then_expr,
            else_expr,
        });
    };
    let when_then_expr = when_then_expr
        .into_iter()
        .map(|(when, then)| {
            let test = rewrite_binary(BinaryExpr::new(operand.clone(), Operator::Eq, when));
            (Box::new(test), then)
        })
        .collect();
    Expr::Case(Case {
        expr: None,
        when_then_expr,
        else_expr,
    })
}

fn compare(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

fn signed_zero(zero_type: &DataType, negative: bool) -> Expr {
    match (zero_type, negative) {
        (DataType::Float32, true) => lit(-0.0_f32),
        (DataType::Float32, false) => lit(0.0_f32),
        (_, true) => lit(-0.0_f64),
        (_, false) => lit(0.0_f64),
    }
}

fn is_literal(expr: &Expr, expected: &Expr) -> bool {
    matches!(
        (expr, expected),
        (Expr::Literal(value, _), Expr::Literal(wanted, _)) if value == wanted
    )
}

fn is_zero_operand(expr: &Expr) -> bool {
    zero_type(expr).is_some()
}

fn zero_type(expr: &Expr) -> Option<DataType> {
    match expr {
        Expr::Literal(value, _) => float_zero_type(value),
        Expr::Negative(inner) => zero_type(inner),
        Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field }) => {
            let target = field.data_type();
            if !matches!(target, DataType::Float32 | DataType::Float64) {
                return None;
            }
            let value = literal_value(expr)?.cast_to(target).ok()?;
            float_zero_type(&value)
        }
        _ => None,
    }
}

fn literal_value(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Negative(inner) => literal_value(inner)?.arithmetic_negate().ok(),
        _ => None,
    }
}

fn float_zero_type(value: &ScalarValue) -> Option<DataType> {
    match value {
        ScalarValue::Float32(Some(value)) if value.classify() == FpCategory::Zero => {
            Some(DataType::Float32)
        }
        ScalarValue::Float64(Some(value)) if value.classify() == FpCategory::Zero => {
            Some(DataType::Float64)
        }
        _ => None,
    }
}
