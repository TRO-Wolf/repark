use std::num::FpCategory;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::expr::{Case, InList, TryCast};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Between, BinaryExpr, Cast, Expr, LogicalPlan, Operator, and, lit, or,
};
use datafusion::optimizer::{AnalyzerRule, ApplyOrder, OptimizerConfig, OptimizerRule};

const RULE_NAME: &str = "signed_zero_literals";

#[derive(Debug, Default)]
pub(crate) struct SignedZeroLiterals;

impl AnalyzerRule for SignedZeroLiterals {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_node).data()
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
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        rewrite_node(plan)
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

fn rewrite_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut carries = false;
    plan.apply_expressions(|expr| {
        carries = carries_rewrite(expr)?;
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
    plan.map_expressions(|expr| {
        if matches!(expr, Expr::GroupingSet(_)) || !carries_rewrite(&expr)? {
            return Ok(Transformed::no(expr));
        }
        let saved = preserver.save(&expr);
        Ok(expr
            .transform_up(|node| Ok(rewrite_expr(node)))?
            .update_data(|node| saved.restore(node)))
    })
}

fn carries_rewrite(expr: &Expr) -> Result<bool> {
    expr.exists(|node| Ok(rewrites(node)))
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
