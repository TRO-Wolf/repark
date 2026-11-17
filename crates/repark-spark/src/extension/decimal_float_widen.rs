use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, NullEquality, Result, ScalarValue};
use datafusion::logical_expr::expr::{Case, InList, InSubquery};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Between, BinaryExpr, Distinct, Expr, ExprSchemable, Join, JoinType, LogicalPlan, Operator,
    Projection, Subquery, Union, WriteOp,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{AnalyzerRule, OptimizerConfig, OptimizerRule};

use super::decimal_common_type::{
    Mode, Target, array_contains_target, conform_insert, in_subquery_target, literal_digits,
    retarget, retarget_typed, target_of, target_of_types, widen_array_contains,
};

const ALL_ARGUMENTS: &[&str] = &[
    "coalesce",
    "nvl",
    "ifnull",
    "greatest",
    "least",
    "array",
    "make_array",
    "map_concat",
];
const AFTER_FIRST_ARGUMENT: &[&str] = &["if", "nvl2"];
const MAP_CONSTRUCTORS: &[&str] = &["__repark_create_map__", "create_map"];
const ARRAY_CONTAINS: &str = "array_contains";

#[derive(Debug, Default)]
pub struct SparkDecimalFloatWiden;

impl AnalyzerRule for SparkDecimalFloatWiden {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let decimals = repark_functions::decimal_legacy::spark_decimal_rule_enabled(config);
        if !plan_widens(&plan, decimals)? {
            return Ok(plan);
        }
        plan.transform_up_with_subqueries(|node| rewrite_plan(node, decimals))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_decimal_float_widen"
    }
}

pub fn insert_decimal_float_widen_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return Err(datafusion::error::DataFusionError::Plan(
            "the decimal and float widening requires the default type_coercion analyzer rule"
                .to_string(),
        ));
    };
    rules.insert(position, Arc::new(SparkDecimalFloatWiden));
    Ok(rules)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Class {
    Decimal,
    Float,
    Other,
}

fn class_of(data_type: &DataType) -> Class {
    match data_type {
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Class::Decimal,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Class::Float,
        _ => Class::Other,
    }
}

fn expr_class(expr: &Expr, schema: &DFSchema) -> Class {
    expr.get_type(schema)
        .map_or(Class::Other, |data_type| class_of(&data_type))
}

fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

fn function_groups(name: &str, arity: usize) -> Option<Vec<Vec<usize>>> {
    if ALL_ARGUMENTS.contains(&name) {
        return Some(vec![(0..arity).collect()]);
    }
    if AFTER_FIRST_ARGUMENT.contains(&name) {
        return Some(vec![(1..arity).collect()]);
    }
    if MAP_CONSTRUCTORS.contains(&name) {
        return Some(vec![
            (0..arity).step_by(2).collect(),
            (1..arity).step_by(2).collect(),
        ]);
    }
    None
}

fn is_candidate(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => is_comparison(binary.op),
        Expr::InList(_) | Expr::Between(_) | Expr::Case(_) | Expr::InSubquery(_) => true,
        Expr::ScalarFunction(function) => {
            let name = function.func.name();
            name == "nullif"
                || name == ARRAY_CONTAINS
                || function_groups(name, function.args.len()).is_some()
        }
        _ => false,
    }
}

fn pair_target(left: &Expr, right: &Expr, schema: &DFSchema, decimals: bool) -> Option<Target> {
    target_of(&[left, right], schema, Mode::Compare, decimals)
}

fn in_list_target(in_list: &InList, schema: &DFSchema, decimals: bool) -> Option<Target> {
    let operands = std::iter::once(in_list.expr.as_ref())
        .chain(in_list.list.iter())
        .collect::<Vec<_>>();
    target_of(&operands, schema, Mode::Unify, decimals)
}

fn group_target(
    args: &[Expr],
    positions: &[usize],
    schema: &DFSchema,
    decimals: bool,
) -> Option<Target> {
    let chosen = positions
        .iter()
        .filter_map(|&position| args.get(position))
        .collect::<Vec<_>>();
    target_of(&chosen, schema, Mode::Unify, decimals)
}

fn expr_widens(expr: &Expr, schema: &DFSchema, decimals: bool) -> bool {
    match expr {
        Expr::BinaryExpr(binary) if is_comparison(binary.op) => {
            pair_target(&binary.left, &binary.right, schema, decimals).is_some()
        }
        Expr::InList(in_list) => in_list_target(in_list, schema, decimals).is_some(),
        Expr::Between(between) => {
            pair_target(&between.expr, &between.low, schema, decimals).is_some()
                || pair_target(&between.expr, &between.high, schema, decimals).is_some()
        }
        Expr::Case(case) => case_widens(case, schema, decimals),
        Expr::InSubquery(in_subquery) => {
            in_subquery_target(in_subquery, schema, decimals).is_some()
        }
        Expr::ScalarFunction(function) => {
            let name = function.func.name();
            let args = &function.args;
            if name == "nullif" {
                return args.len() == 2
                    && pair_target(&args[0], &args[1], schema, decimals).is_some();
            }
            if name == ARRAY_CONTAINS {
                return array_contains_target(args, schema, decimals).is_some();
            }
            function_groups(name, args.len()).is_some_and(|groups| {
                groups
                    .iter()
                    .any(|positions| group_target(args, positions, schema, decimals).is_some())
            })
        }
        _ => false,
    }
}

fn case_results(case: &Case) -> Vec<&Expr> {
    case.when_then_expr
        .iter()
        .map(|(_, then)| then.as_ref())
        .chain(case.else_expr.as_deref())
        .collect()
}

fn case_searched(operand: &Expr, whens: &[&Expr], schema: &DFSchema, decimals: bool) -> bool {
    let operand_class = expr_class(operand, schema);
    let classes = whens
        .iter()
        .map(|when| expr_class(when, schema))
        .collect::<Vec<_>>();
    if operand_class == Class::Decimal
        && classes.contains(&Class::Float)
        && classes.iter().any(|class| *class != Class::Float)
    {
        return true;
    }
    if !decimals {
        return false;
    }
    let all = std::iter::once(operand)
        .chain(whens.iter().copied())
        .collect::<Vec<_>>();
    matches!(
        target_of(&all, schema, Mode::Compare, decimals),
        Some(Target::Spark(_))
    ) || whens.iter().any(|when| {
        matches!(
            pair_target(operand, when, schema, decimals),
            Some(Target::Spark(_))
        )
    })
}

fn case_widens(case: &Case, schema: &DFSchema, decimals: bool) -> bool {
    if target_of(&case_results(case), schema, Mode::Unify, decimals).is_some() {
        return true;
    }
    case.expr.as_ref().is_some_and(|operand| {
        let whens = case
            .when_then_expr
            .iter()
            .map(|(when, _)| when.as_ref())
            .collect::<Vec<_>>();
        whens
            .iter()
            .any(|when| pair_target(operand, when, schema, decimals).is_some())
            || case_searched(operand, &whens, schema, decimals)
    })
}

fn merged_input_schema(plan: &LogicalPlan) -> DFSchema {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    schema
}

fn node_widens(plan: &LogicalPlan, decimals: bool) -> Result<bool> {
    match plan {
        LogicalPlan::Union(union) => return Ok(!union_targets(union, decimals).is_empty()),
        LogicalPlan::Join(join) if !join_widening(join, decimals).is_empty() => return Ok(true),
        _ => {}
    }
    let mut candidate = false;
    plan.apply_expressions(|expr| {
        candidate = expr.exists(|node| Ok(is_candidate(node)))?;
        Ok(if candidate {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    if !candidate {
        return Ok(false);
    }
    let schema = merged_input_schema(plan);
    let mut found = false;
    plan.apply_expressions(|expr| {
        found = expr.exists(|node| Ok(expr_widens(node, &schema, decimals)))?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn plan_widens(plan: &LogicalPlan, decimals: bool) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        found = node_widens(node, decimals)?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn union_targets(union: &Union, decimals: bool) -> Vec<(usize, Target)> {
    let Some(width) = union
        .inputs
        .first()
        .map(|input| input.schema().fields().len())
    else {
        return Vec::new();
    };
    (0..width)
        .filter_map(|position| {
            let types = union
                .inputs
                .iter()
                .map(|input| {
                    input
                        .schema()
                        .fields()
                        .get(position)
                        .map(|field| field.data_type())
                })
                .collect::<Option<Vec<_>>>()?;
            target_of_types(&types, &[], Mode::Unify, decimals).map(|target| (position, target))
        })
        .collect()
}

fn project_to(input: Arc<LogicalPlan>, targets: &[(usize, Target)]) -> Result<(LogicalPlan, bool)> {
    let schema = input.schema();
    let mut changed = false;
    let exprs = schema
        .iter()
        .enumerate()
        .map(|(position, (qualifier, field))| {
            let column = Expr::Column(Column::new(qualifier.cloned(), field.name()));
            let Some((_, target)) = targets.iter().find(|(chosen, _)| *chosen == position) else {
                return column;
            };
            let cast = retarget_typed(column.clone(), field.data_type(), target);
            if cast == column {
                return column;
            }
            changed = true;
            cast.alias_qualified(qualifier.cloned(), field.name())
        })
        .collect::<Vec<_>>();
    if !changed {
        return Ok((Arc::unwrap_or_clone(input), false));
    }
    Ok((
        LogicalPlan::Projection(Projection::try_new(exprs, input)?),
        true,
    ))
}

fn rewrite_union(union: Union, decimals: bool) -> Result<Transformed<LogicalPlan>> {
    let targets = union_targets(&union, decimals);
    if targets.is_empty() {
        return rebuild_stale_union(union);
    }
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in union.inputs {
        let (projected, _) = project_to(input, &targets)?;
        inputs.push(Arc::new(projected));
    }
    Ok(Transformed::yes(LogicalPlan::Union(
        Union::try_new_with_loose_types(inputs)?,
    )))
}

fn rebuild_stale_union(union: Union) -> Result<Transformed<LogicalPlan>> {
    let stale = union.inputs.first().is_some_and(|first| {
        let mine = first.schema().fields();
        let merged = union.schema.fields();
        mine.len() == merged.len()
            && mine
                .iter()
                .zip(merged.iter())
                .any(|(mine, merged)| mine.data_type() != merged.data_type())
    });
    if !stale {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    }
    Ok(Transformed::yes(LogicalPlan::Union(
        Union::try_new_with_loose_types(union.inputs)?,
    )))
}

enum JoinWidening {
    None,
    Keys(Vec<(usize, Target)>),
    SetOperation(Vec<(usize, Target)>),
}

impl JoinWidening {
    fn is_empty(&self) -> bool {
        match self {
            Self::None => true,
            Self::Keys(targets) | Self::SetOperation(targets) => targets.is_empty(),
        }
    }
}

fn is_set_operation(join: &Join) -> bool {
    if !matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        || !matches!(join.left.as_ref(), LogicalPlan::Distinct(Distinct::All(_)))
        || join.null_equality != NullEquality::NullEqualsNull
        || join.filter.is_some()
    {
        return false;
    }
    let left = join.left.schema().fields();
    let right = join.right.schema().fields();
    join.on.len() == left.len()
        && join.on.len() == right.len()
        && join
            .on
            .iter()
            .zip(left.iter().zip(right.iter()))
            .all(|((left_expr, right_expr), (left_field, right_field))| {
                matches!(left_expr, Expr::Column(column) if column.name == *left_field.name())
                    && matches!(right_expr, Expr::Column(column) if column.name == *right_field.name())
            })
}

fn join_widening(join: &Join, decimals: bool) -> JoinWidening {
    if join.on.is_empty() {
        return JoinWidening::None;
    }
    let set_operation = is_set_operation(join);
    let mode = if set_operation {
        Mode::Unify
    } else {
        Mode::Compare
    };
    let targets = join
        .on
        .iter()
        .enumerate()
        .filter_map(|(position, (left, right))| {
            let types = [
                left.get_type(join.left.schema()).ok()?,
                right.get_type(join.right.schema()).ok()?,
            ];
            let literals = [literal_digits(left), literal_digits(right)];
            target_of_types(&[&types[0], &types[1]], &literals, mode, decimals)
                .map(|target| (position, target))
        })
        .collect::<Vec<_>>();
    if targets.is_empty() {
        JoinWidening::None
    } else if set_operation {
        JoinWidening::SetOperation(targets)
    } else {
        JoinWidening::Keys(targets)
    }
}

fn rewrite_join(mut join: Join, decimals: bool) -> Result<Transformed<LogicalPlan>> {
    match join_widening(&join, decimals) {
        JoinWidening::None => Ok(Transformed::no(LogicalPlan::Join(join))),
        JoinWidening::Keys(targets) => {
            for (position, target) in targets {
                let Some((left, right)) = join.on.get_mut(position) else {
                    continue;
                };
                *left = retarget(left.clone(), join.left.schema(), &target);
                *right = retarget(right.clone(), join.right.schema(), &target);
            }
            Ok(Transformed::yes(LogicalPlan::Join(join)))
        }
        JoinWidening::SetOperation(targets) => {
            if let LogicalPlan::Distinct(Distinct::All(inner)) = join.left.as_ref() {
                let (projected, _) = project_to(Arc::clone(inner), &targets)?;
                join.left = Arc::new(LogicalPlan::Distinct(Distinct::All(Arc::new(projected))));
            }
            let (right, _) = project_to(Arc::clone(&join.right), &targets)?;
            join.right = Arc::new(right);
            Ok(Transformed::yes(LogicalPlan::Join(join)))
        }
    }
}

fn widen_pair(left: Expr, right: Expr, schema: &DFSchema, decimals: bool) -> (Expr, Expr) {
    match pair_target(&left, &right, schema, decimals) {
        Some(target) => (
            retarget(left, schema, &target),
            retarget(right, schema, &target),
        ),
        None => (left, right),
    }
}

fn widen_all(exprs: Vec<Expr>, schema: &DFSchema, mode: Mode, decimals: bool) -> Vec<Expr> {
    let Some(target) = target_of(&exprs.iter().collect::<Vec<_>>(), schema, mode, decimals) else {
        return exprs;
    };
    exprs
        .into_iter()
        .map(|expr| retarget(expr, schema, &target))
        .collect()
}

fn compare(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

fn widen_between(between: Between, schema: &DFSchema, decimals: bool) -> Expr {
    let Between {
        expr,
        negated,
        low,
        high,
    } = between;
    let spark = |bound: &Expr| {
        matches!(
            pair_target(&expr, bound, schema, decimals),
            Some(Target::Spark(_))
        )
    };
    let operand = expr_class(&expr, schema);
    let bounds = [expr_class(&low, schema), expr_class(&high, schema)];
    if spark(&low) || spark(&high) || (operand == Class::Decimal && bounds[0] != bounds[1]) {
        let (lower, low) = widen_pair(*expr.clone(), *low, schema, decimals);
        let (upper, high) = widen_pair(*expr, *high, schema, decimals);
        let both = compare(
            compare(lower, Operator::GtEq, low),
            Operator::And,
            compare(upper, Operator::LtEq, high),
        );
        return if negated {
            Expr::Not(Box::new(both))
        } else {
            both
        };
    }
    let target = target_of(&[&expr, &low, &high], schema, Mode::Compare, false);
    let cast = |expr: Expr| match &target {
        Some(target) => retarget(expr, schema, target),
        None => expr,
    };
    Expr::Between(Between::new(
        Box::new(cast(*expr)),
        negated,
        Box::new(cast(*low)),
        Box::new(cast(*high)),
    ))
}

fn widen_case(case: Case, schema: &DFSchema, decimals: bool) -> Expr {
    let Case {
        expr,
        when_then_expr,
        else_expr,
    } = case;
    let (whens, thens): (Vec<Expr>, Vec<Expr>) = when_then_expr
        .into_iter()
        .map(|(when, then)| (*when, *then))
        .unzip();
    let mut results = thens;
    let has_else = else_expr.is_some();
    results.extend(else_expr.map(|other| *other));
    let mut results = widen_all(results, schema, Mode::Unify, decimals);
    let else_expr = if has_else {
        results.pop().map(Box::new)
    } else {
        None
    };
    let (expr, whens) = match expr {
        None => (None, whens),
        Some(operand) => {
            if case_searched(
                &operand,
                &whens.iter().collect::<Vec<_>>(),
                schema,
                decimals,
            ) {
                let searched = whens
                    .into_iter()
                    .map(|when| {
                        let (left, right) = widen_pair(*operand.clone(), when, schema, decimals);
                        compare(left, Operator::Eq, right)
                    })
                    .collect();
                (None, searched)
            } else {
                let mut widened = widen_all(
                    std::iter::once(*operand).chain(whens).collect(),
                    schema,
                    Mode::Compare,
                    false,
                );
                let operand = widened.remove(0);
                (Some(Box::new(operand)), widened)
            }
        }
    };
    Expr::Case(Case::new(
        expr,
        whens
            .into_iter()
            .zip(results)
            .map(|(when, then)| (Box::new(when), Box::new(then)))
            .collect(),
        else_expr,
    ))
}

fn widen_in_subquery(in_subquery: InSubquery, schema: &DFSchema, decimals: bool) -> Result<Expr> {
    let Some(target) = in_subquery_target(&in_subquery, schema, decimals) else {
        return Ok(Expr::InSubquery(in_subquery));
    };
    let InSubquery {
        expr,
        subquery,
        negated,
    } = in_subquery;
    let (projected, changed) = project_to(Arc::clone(&subquery.subquery), &[(0, target.clone())])?;
    let subquery = if changed {
        Subquery {
            subquery: Arc::new(projected),
            ..subquery
        }
    } else {
        subquery
    };
    Ok(Expr::InSubquery(InSubquery::new(
        Box::new(retarget(*expr, schema, &target)),
        subquery,
        negated,
    )))
}

fn widen_function(
    function: datafusion::logical_expr::expr::ScalarFunction,
    schema: &DFSchema,
    decimals: bool,
) -> Expr {
    let name = function.func.name().to_string();
    let mut args = function.args;
    if name == "nullif" && args.len() == 2 {
        let second = args.remove(1);
        let value = args.remove(0);
        let (left, right) = widen_pair(value.clone(), second, schema, decimals);
        return Expr::Case(Case::new(
            None,
            vec![(
                Box::new(compare(left, Operator::Eq, right)),
                Box::new(Expr::Literal(ScalarValue::Null, None)),
            )],
            Some(Box::new(value)),
        ));
    }
    if name == ARRAY_CONTAINS {
        args = widen_array_contains(args, schema, decimals);
    } else if let Some(groups) = function_groups(&name, args.len()) {
        for positions in groups {
            let Some(target) = group_target(&args, &positions, schema, decimals) else {
                continue;
            };
            for position in positions {
                if let Some(slot) = args.get_mut(position) {
                    *slot = retarget(slot.clone(), schema, &target);
                }
            }
        }
    }
    Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
        function.func,
        args,
    ))
}

fn widen_expr(expr: Expr, schema: &DFSchema, decimals: bool) -> Result<Transformed<Expr>> {
    if !expr_widens(&expr, schema, decimals) {
        return Ok(Transformed::no(expr));
    }
    let widened = match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (left, right) = widen_pair(*left, *right, schema, decimals);
            compare(left, op, right)
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let mut widened = widen_all(
                std::iter::once(*expr).chain(list).collect(),
                schema,
                Mode::Unify,
                decimals,
            );
            let expr = widened.remove(0);
            Expr::InList(InList::new(Box::new(expr), widened, negated))
        }
        Expr::Between(between) => widen_between(between, schema, decimals),
        Expr::Case(case) => widen_case(case, schema, decimals),
        Expr::InSubquery(in_subquery) => widen_in_subquery(in_subquery, schema, decimals)?,
        Expr::ScalarFunction(function) => widen_function(function, schema, decimals),
        other => return Ok(Transformed::no(other)),
    };
    Ok(Transformed::yes(widened))
}

fn rewrite_plan(plan: LogicalPlan, decimals: bool) -> Result<Transformed<LogicalPlan>> {
    let plan = match plan {
        LogicalPlan::Union(union) => return rewrite_union(union, decimals),
        LogicalPlan::Join(join) => rewrite_join(join, decimals)?,
        LogicalPlan::Dml(dml) if decimals && matches!(dml.op, WriteOp::Insert(_)) => {
            return conform_insert(dml);
        }
        other => Transformed::no(other),
    };
    let keyed = plan.transformed;
    let plan = plan.data;
    let schema = merged_input_schema(&plan);
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let widened = expr.transform_up(|node| widen_expr(node, &schema, decimals))?;
        Ok(widened.update_data(|node| saved_name.restore(node)))
    })?;
    let changed = keyed || transformed.transformed;
    let recomputed = transformed.data.recompute_schema()?;
    Ok(Transformed::new(
        recomputed,
        changed,
        TreeNodeRecursion::Continue,
    ))
}

pub const DECIMAL_IN_RANGE_LIMIT: usize = 32;
const DECIMAL_TO_DOUBLE: &str = "__repark_decimal_to_double__";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecimalInForm {
    Members(Vec<i128>),
    Ranges {
        members: Vec<i128>,
        ranges: Vec<(i128, i128)>,
    },
    PerRow,
}

fn decimal_top(precision: u8) -> Option<i128> {
    10i128
        .checked_pow(u32::from(precision))
        .and_then(|power| power.checked_sub(1))
        .filter(|&top| top > 0)
}

#[must_use]
#[allow(clippy::float_cmp)]
pub fn decimal_range_of_double(constant: f64, precision: u8, scale: i8) -> Option<(i128, i128)> {
    if !constant.is_finite() {
        return None;
    }
    let constant = if constant == 0.0 { 0.0 } else { constant };
    let top = decimal_top(precision)?;
    if let Some((lower, upper)) =
        repark_common::java_double::decimal_preimage_of_double(constant, precision, scale)
    {
        return (lower < upper).then_some((lower, upper));
    }
    if repark_common::java_double::java_decimal_to_double(top, scale) != constant {
        return None;
    }
    let (_, lower) = repark_common::java_double::decimal_preimage_of_double(
        constant.next_down(),
        precision,
        scale,
    )?;
    Some((lower, top + 1))
}

#[must_use]
pub fn decimal_in_form(constants: &[f64], precision: u8, scale: i8, limit: usize) -> DecimalInForm {
    let mut ranges: Vec<(i128, i128)> = Vec::with_capacity(constants.len());
    let mut wide_seen = BTreeSet::new();
    for &constant in constants {
        let Some(range) = decimal_range_of_double(constant, precision, scale) else {
            continue;
        };
        if range.1 - range.0 != 1 && wide_seen.insert(range) && wide_seen.len() > limit {
            return DecimalInForm::PerRow;
        }
        ranges.push(range);
    }
    ranges.sort_unstable();
    ranges.dedup();
    let (singles, wide): (Vec<_>, Vec<_>) = ranges
        .into_iter()
        .partition(|(lower, upper)| upper - lower == 1);
    let members = singles.into_iter().map(|(lower, _)| lower).collect();
    if wide.is_empty() {
        return DecimalInForm::Members(members);
    }
    if wide.len() > limit {
        return DecimalInForm::PerRow;
    }
    DecimalInForm::Ranges {
        members,
        ranges: wide,
    }
}

#[derive(Debug, Default)]
pub struct DecimalInRanges;

impl OptimizerRule for DecimalInRanges {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_decimal_in_ranges"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !node_has_decimal_in(&plan)? {
            return Ok(Transformed::no(plan));
        }
        let schema = match &plan {
            LogicalPlan::TableScan(scan) => DFSchema::try_from_qualified_schema(
                scan.table_name.clone(),
                scan.source.schema().as_ref(),
            )?,
            other => merged_input_schema(other),
        };
        let name_preserver = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let saved_name = name_preserver.save(&expr);
            let rewritten = expr.transform_up(|node| Ok(rewrite_decimal_in(node, &schema)))?;
            Ok(rewritten.update_data(|node| saved_name.restore(node)))
        })
    }
}

fn decimal_to_double_argument(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::ScalarFunction(function)
            if function.func.name() == DECIMAL_TO_DOUBLE && function.args.len() == 1 =>
        {
            function.args.first()
        }
        _ => None,
    }
}

fn is_decimal_in(expr: &Expr) -> bool {
    match expr {
        Expr::InList(in_list) => {
            decimal_to_double_argument(&in_list.expr).is_some()
                && in_list
                    .list
                    .iter()
                    .all(|item| matches!(item, Expr::Literal(ScalarValue::Float64(_), _)))
        }
        _ => false,
    }
}

fn node_has_decimal_in(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(|expr| {
        found = expr.exists(|node| Ok(is_decimal_in(node)))?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn rewrite_decimal_in(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    if !is_decimal_in(&expr) {
        return Transformed::no(expr);
    }
    let Expr::InList(in_list) = &expr else {
        return Transformed::no(expr);
    };
    let Some(argument) = decimal_to_double_argument(&in_list.expr) else {
        return Transformed::no(expr);
    };
    let Ok(DataType::Decimal128(precision, scale)) = argument.get_type(schema) else {
        return Transformed::no(expr);
    };
    if argument.is_volatile() {
        return Transformed::no(expr);
    }
    let mut constants = Vec::with_capacity(in_list.list.len());
    let mut has_null = false;
    for item in &in_list.list {
        match item {
            Expr::Literal(ScalarValue::Float64(Some(constant)), _) => constants.push(*constant),
            _ => has_null = true,
        }
    }
    let form = decimal_in_form(&constants, precision, scale, DECIMAL_IN_RANGE_LIMIT);
    let Some(top) = decimal_top(precision) else {
        return Transformed::no(expr);
    };
    let decimal = |unscaled: i128| {
        Expr::Literal(
            ScalarValue::Decimal128(Some(unscaled), precision, scale),
            None,
        )
    };
    let mut terms = Vec::new();
    let push_members = |members: Vec<i128>, terms: &mut Vec<Expr>| match members.as_slice() {
        [] => {}
        [only] => terms.push(argument.clone().eq(decimal(*only))),
        _ => terms.push(Expr::InList(InList::new(
            Box::new(argument.clone()),
            members.into_iter().map(decimal).collect(),
            false,
        ))),
    };
    match form {
        DecimalInForm::PerRow => return Transformed::no(expr),
        DecimalInForm::Members(members) => push_members(members, &mut terms),
        DecimalInForm::Ranges { members, ranges } => {
            push_members(members, &mut terms);
            for (lower, upper) in ranges {
                let low = argument.clone().gt_eq(decimal(lower));
                terms.push(if upper > top {
                    low
                } else {
                    low.and(argument.clone().lt(decimal(upper)))
                });
            }
        }
    }
    if has_null {
        terms.push(Expr::Literal(ScalarValue::Boolean(None), None));
    }
    let combined = terms.into_iter().reduce(Expr::or).unwrap_or_else(|| {
        Expr::Case(Case::new(
            None,
            vec![(
                Box::new(argument.clone().is_null()),
                Box::new(Expr::Literal(ScalarValue::Boolean(None), None)),
            )],
            Some(Box::new(Expr::Literal(
                ScalarValue::Boolean(Some(false)),
                None,
            ))),
        ))
    });
    Transformed::yes(if in_list.negated {
        Expr::Not(Box::new(combined))
    } else {
        combined
    })
}

#[cfg(test)]
mod tests;
