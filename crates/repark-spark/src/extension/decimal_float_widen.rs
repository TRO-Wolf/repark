use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, NullEquality, Result, ScalarValue};
use datafusion::logical_expr::expr::{Case, InList, InSubquery};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Between, BinaryExpr, Cast, Distinct, Expr, ExprSchemable, Join, JoinType, LogicalPlan,
    Operator, Projection, Subquery, Union,
};
use datafusion::optimizer::AnalyzerRule;

const ALL_ARGUMENTS: &[&str] = &[
    "coalesce",
    "nvl",
    "ifnull",
    "greatest",
    "least",
    "array",
    "make_array",
];
const AFTER_FIRST_ARGUMENT: &[&str] = &["if", "nvl2"];

#[derive(Debug, Default)]
pub struct SparkDecimalFloatWiden;

impl AnalyzerRule for SparkDecimalFloatWiden {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        if !plan_widens(&plan)? {
            return Ok(plan);
        }
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_decimal_float_widen"
    }
}

#[expect(
    clippy::missing_errors_doc,
    reason = "The error contract is documented in map.md under the owner comment ban."
)]
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

fn mixed(classes: &[Class]) -> bool {
    classes.contains(&Class::Decimal) && classes.contains(&Class::Float)
}

fn to_double(expr: Expr) -> Expr {
    Expr::Cast(Cast::new(Box::new(expr), DataType::Float64))
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

fn function_positions(name: &str, arity: usize) -> Option<std::ops::Range<usize>> {
    if ALL_ARGUMENTS.contains(&name) {
        return Some(0..arity);
    }
    if AFTER_FIRST_ARGUMENT.contains(&name) {
        return Some(1.min(arity)..arity);
    }
    None
}

fn is_candidate(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => is_comparison(binary.op),
        Expr::InList(_) | Expr::Between(_) | Expr::Case(_) | Expr::InSubquery(_) => true,
        Expr::ScalarFunction(function) => {
            let name = function.func.name();
            name == "nullif" || function_positions(name, function.args.len()).is_some()
        }
        _ => false,
    }
}

fn expr_widens(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        Expr::BinaryExpr(binary) if is_comparison(binary.op) => mixed(&[
            expr_class(&binary.left, schema),
            expr_class(&binary.right, schema),
        ]),
        Expr::InList(in_list) => mixed(
            &std::iter::once(in_list.expr.as_ref())
                .chain(in_list.list.iter())
                .map(|item| expr_class(item, schema))
                .collect::<Vec<_>>(),
        ),
        Expr::Between(between) => {
            let operand = expr_class(&between.expr, schema);
            mixed(&[operand, expr_class(&between.low, schema)])
                || mixed(&[operand, expr_class(&between.high, schema)])
        }
        Expr::Case(case) => case_widens(case, schema),
        Expr::InSubquery(in_subquery) => mixed(&[
            expr_class(&in_subquery.expr, schema),
            subquery_class(&in_subquery.subquery),
        ]),
        Expr::ScalarFunction(function) => {
            let name = function.func.name();
            if name == "nullif" {
                return function.args.len() == 2
                    && mixed(&[
                        expr_class(&function.args[0], schema),
                        expr_class(&function.args[1], schema),
                    ]);
            }
            function_positions(name, function.args.len()).is_some_and(|positions| {
                mixed(
                    &function.args[positions]
                        .iter()
                        .map(|arg| expr_class(arg, schema))
                        .collect::<Vec<_>>(),
                )
            })
        }
        _ => false,
    }
}

fn case_widens(case: &Case, schema: &DFSchema) -> bool {
    let results = case
        .when_then_expr
        .iter()
        .map(|(_, then)| expr_class(then, schema))
        .chain(case.else_expr.iter().map(|other| expr_class(other, schema)))
        .collect::<Vec<_>>();
    if mixed(&results) {
        return true;
    }
    case.expr.as_ref().is_some_and(|operand| {
        let operand = expr_class(operand, schema);
        case.when_then_expr
            .iter()
            .any(|(when, _)| mixed(&[operand, expr_class(when, schema)]))
    })
}

fn subquery_class(subquery: &Subquery) -> Class {
    subquery
        .subquery
        .schema()
        .fields()
        .first()
        .map_or(Class::Other, |field| class_of(field.data_type()))
}

fn merged_input_schema(plan: &LogicalPlan) -> DFSchema {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    schema
}

fn node_widens(plan: &LogicalPlan) -> Result<bool> {
    match plan {
        LogicalPlan::Union(union) => return Ok(!union_positions(union).is_empty()),
        LogicalPlan::Join(join) if !join_widening(join).is_empty() => return Ok(true),
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
        found = expr.exists(|node| Ok(expr_widens(node, &schema)))?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn plan_widens(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        found = node_widens(node)?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn union_positions(union: &Union) -> Vec<usize> {
    let Some(width) = union
        .inputs
        .first()
        .map(|input| input.schema().fields().len())
    else {
        return Vec::new();
    };
    (0..width)
        .filter(|&position| {
            mixed(
                &union
                    .inputs
                    .iter()
                    .map(|input| {
                        input
                            .schema()
                            .fields()
                            .get(position)
                            .map_or(Class::Other, |field| class_of(field.data_type()))
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

fn project_to_double(input: Arc<LogicalPlan>, positions: &[usize]) -> Result<(LogicalPlan, bool)> {
    let schema = input.schema();
    let mut changed = false;
    let exprs = schema
        .iter()
        .enumerate()
        .map(|(position, (qualifier, field))| {
            let column = Expr::Column(Column::new(qualifier.cloned(), field.name()));
            if positions.contains(&position) && class_of(field.data_type()) == Class::Decimal {
                changed = true;
                to_double(column).alias_qualified(qualifier.cloned(), field.name())
            } else {
                column
            }
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

fn rewrite_union(union: Union) -> Result<Transformed<LogicalPlan>> {
    let positions = union_positions(&union);
    if positions.is_empty() {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    }
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in union.inputs {
        let (projected, _) = project_to_double(input, &positions)?;
        inputs.push(Arc::new(projected));
    }
    Ok(Transformed::yes(LogicalPlan::Union(
        Union::try_new_with_loose_types(inputs)?,
    )))
}

enum JoinWidening {
    None,
    Keys(Vec<usize>),
    SetOperation(Vec<usize>),
}

impl JoinWidening {
    fn is_empty(&self) -> bool {
        match self {
            Self::None => true,
            Self::Keys(positions) | Self::SetOperation(positions) => positions.is_empty(),
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

fn join_widening(join: &Join) -> JoinWidening {
    let positions = join
        .on
        .iter()
        .enumerate()
        .filter(|(_, (left, right))| {
            mixed(&[
                expr_class(left, join.left.schema()),
                expr_class(right, join.right.schema()),
            ])
        })
        .map(|(position, _)| position)
        .collect::<Vec<_>>();
    if positions.is_empty() {
        return JoinWidening::None;
    }
    if is_set_operation(join) {
        JoinWidening::SetOperation(positions)
    } else {
        JoinWidening::Keys(positions)
    }
}

fn rewrite_join(mut join: Join) -> Result<Transformed<LogicalPlan>> {
    match join_widening(&join) {
        JoinWidening::None => Ok(Transformed::no(LogicalPlan::Join(join))),
        JoinWidening::Keys(positions) => {
            for position in positions {
                let Some((left, right)) = join.on.get_mut(position) else {
                    continue;
                };
                if expr_class(left, join.left.schema()) == Class::Decimal {
                    *left = to_double(left.clone());
                }
                if expr_class(right, join.right.schema()) == Class::Decimal {
                    *right = to_double(right.clone());
                }
            }
            Ok(Transformed::yes(LogicalPlan::Join(join)))
        }
        JoinWidening::SetOperation(positions) => {
            if let LogicalPlan::Distinct(Distinct::All(inner)) = join.left.as_ref() {
                let (projected, _) = project_to_double(Arc::clone(inner), &positions)?;
                join.left = Arc::new(LogicalPlan::Distinct(Distinct::All(Arc::new(projected))));
            }
            let (right, _) = project_to_double(Arc::clone(&join.right), &positions)?;
            join.right = Arc::new(right);
            Ok(Transformed::yes(LogicalPlan::Join(join)))
        }
    }
}

fn widen_pair(left: Expr, right: Expr, schema: &DFSchema) -> (Expr, Expr) {
    match (expr_class(&left, schema), expr_class(&right, schema)) {
        (Class::Decimal, Class::Float) => (to_double(left), right),
        (Class::Float, Class::Decimal) => (left, to_double(right)),
        _ => (left, right),
    }
}

fn widen_all(exprs: Vec<Expr>, schema: &DFSchema) -> Vec<Expr> {
    let classes = exprs
        .iter()
        .map(|expr| expr_class(expr, schema))
        .collect::<Vec<_>>();
    if !mixed(&classes) {
        return exprs;
    }
    exprs
        .into_iter()
        .zip(classes)
        .map(|(expr, class)| {
            if class == Class::Decimal {
                to_double(expr)
            } else {
                expr
            }
        })
        .collect()
}

fn widen_between(between: Between, schema: &DFSchema) -> Expr {
    let Between {
        expr,
        negated,
        low,
        high,
    } = between;
    let operand = expr_class(&expr, schema);
    let bounds = [expr_class(&low, schema), expr_class(&high, schema)];
    if operand == Class::Decimal && bounds[0] != bounds[1] {
        let (lower, low) = widen_pair(*expr.clone(), *low, schema);
        let (upper, high) = widen_pair(*expr, *high, schema);
        let both = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(lower),
                Operator::GtEq,
                Box::new(low),
            ))),
            Operator::And,
            Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(upper),
                Operator::LtEq,
                Box::new(high),
            ))),
        ));
        return if negated {
            Expr::Not(Box::new(both))
        } else {
            both
        };
    }
    let widen = mixed(&[operand, bounds[0], bounds[1]]);
    let cast = |expr: Expr, class: Class| {
        if widen && class == Class::Decimal {
            to_double(expr)
        } else {
            expr
        }
    };
    Expr::Between(Between::new(
        Box::new(cast(*expr, operand)),
        negated,
        Box::new(cast(*low, bounds[0])),
        Box::new(cast(*high, bounds[1])),
    ))
}

fn widen_case(case: Case, schema: &DFSchema) -> Expr {
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
    let mut results = widen_all(results, schema);
    let else_expr = if has_else {
        results.pop().map(Box::new)
    } else {
        None
    };
    let (expr, whens) = match expr {
        None => (None, whens),
        Some(operand) => {
            let operand_class = expr_class(&operand, schema);
            let classes = whens
                .iter()
                .map(|when| expr_class(when, schema))
                .collect::<Vec<_>>();
            if operand_class == Class::Decimal
                && classes.contains(&Class::Float)
                && classes.iter().any(|class| *class != Class::Float)
            {
                let searched = whens
                    .into_iter()
                    .map(|when| {
                        let (left, right) = widen_pair(*operand.clone(), when, schema);
                        Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(left),
                            Operator::Eq,
                            Box::new(right),
                        ))
                    })
                    .collect();
                (None, searched)
            } else {
                let mut widened =
                    widen_all(std::iter::once(*operand).chain(whens).collect(), schema);
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

fn widen_in_subquery(in_subquery: InSubquery, schema: &DFSchema) -> Result<Expr> {
    let InSubquery {
        expr,
        subquery,
        negated,
    } = in_subquery;
    match (expr_class(&expr, schema), subquery_class(&subquery)) {
        (Class::Decimal, Class::Float) => Ok(Expr::InSubquery(InSubquery::new(
            Box::new(to_double(*expr)),
            subquery,
            negated,
        ))),
        (Class::Float, Class::Decimal) => {
            let (projected, _) = project_to_double(Arc::clone(&subquery.subquery), &[0])?;
            Ok(Expr::InSubquery(InSubquery::new(
                expr,
                Subquery {
                    subquery: Arc::new(projected),
                    ..subquery
                },
                negated,
            )))
        }
        _ => Ok(Expr::InSubquery(InSubquery::new(expr, subquery, negated))),
    }
}

fn widen_function(
    function: datafusion::logical_expr::expr::ScalarFunction,
    schema: &DFSchema,
) -> Expr {
    let name = function.func.name().to_string();
    let mut args = function.args;
    if name == "nullif" && args.len() == 2 {
        let first = expr_class(&args[0], schema);
        if first == Class::Decimal && expr_class(&args[1], schema) == Class::Float {
            let second = args.remove(1);
            let value = args.remove(0);
            return Expr::Case(Case::new(
                None,
                vec![(
                    Box::new(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(to_double(value.clone())),
                        Operator::Eq,
                        Box::new(second),
                    ))),
                    Box::new(Expr::Literal(ScalarValue::Null, None)),
                )],
                Some(Box::new(value)),
            ));
        }
        args = widen_all(args, schema);
    } else if let Some(positions) = function_positions(&name, args.len()) {
        let tail = args.split_off(positions.start);
        args.extend(widen_all(tail, schema));
    }
    Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
        function.func,
        args,
    ))
}

fn widen_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if !expr_widens(&expr, schema) {
        return Ok(Transformed::no(expr));
    }
    let widened = match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (left, right) = widen_pair(*left, *right, schema);
            Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let mut widened = widen_all(std::iter::once(*expr).chain(list).collect(), schema);
            let expr = widened.remove(0);
            Expr::InList(InList::new(Box::new(expr), widened, negated))
        }
        Expr::Between(between) => widen_between(between, schema),
        Expr::Case(case) => widen_case(case, schema),
        Expr::InSubquery(in_subquery) => widen_in_subquery(in_subquery, schema)?,
        Expr::ScalarFunction(function) => widen_function(function, schema),
        other => return Ok(Transformed::no(other)),
    };
    Ok(Transformed::yes(widened))
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let plan = match plan {
        LogicalPlan::Union(union) => return rewrite_union(union),
        LogicalPlan::Join(join) => rewrite_join(join)?,
        other => Transformed::no(other),
    };
    let keyed = plan.transformed;
    let plan = plan.data;
    let schema = merged_input_schema(&plan);
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let widened = expr.transform_up(|node| widen_expr(node, &schema))?;
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

#[cfg(test)]
mod tests;
