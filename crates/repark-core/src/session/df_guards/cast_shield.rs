use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::logical_expr::expr::{InList, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Between, BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, Operator, TryCast, lit,
};
use datafusion::optimizer::{AnalyzerRule, ApplyOrder, OptimizerConfig, OptimizerRule};

mod stats_barrier;
mod udf;

pub(crate) use stats_barrier::{StatsBarrierRule, stats_barrier_udf};
pub use udf::cast_shield_udf;
use udf::{cast_parts, cast_preimage, cast_shield_call, is_lossless, is_narrowing, shielded_cast};

const RULE_NAME: &str = "repark_cast_shield";

const DECIMAL_TO_DOUBLE_NAME: &str = "__repark_decimal_to_double__";

pub(crate) const IN_RANGE_LIMIT: usize = 32;

#[derive(Debug, Default)]
pub(crate) struct CastShieldRule;

impl AnalyzerRule for CastShieldRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        shield_plan(plan).data()
    }

    fn name(&self) -> &str {
        RULE_NAME
    }
}

impl OptimizerRule for CastShieldRule {
    fn name(&self) -> &str {
        RULE_NAME
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        shield_plan(plan)
    }
}

pub(crate) fn shield_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if !plan_has_candidate(&plan)? {
        return Ok(Transformed::no(plan));
    }
    plan.transform_up_with_subqueries(shield_node)
}

fn plan_has_candidate(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        found = node_has_candidate(node)?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn node_has_candidate(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(|expr| {
        found = expr.exists(|node| Ok(is_candidate(node)))?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn is_candidate(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(*op) => {
            is_cast_operand(left) || is_cast_operand(right)
        }
        Expr::InList(in_list) => {
            is_cast_operand(&in_list.expr) || shielded_cast(&in_list.expr).is_some()
        }
        Expr::Between(between) => is_cast_operand(&between.expr),
        _ => false,
    }
}

fn decimal_to_double_argument(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::ScalarFunction(call) if call.name() == DECIMAL_TO_DOUBLE_NAME => {
            match call.args.as_slice() {
                [argument] => Some(argument),
                _ => None,
            }
        }
        _ => None,
    }
}

fn is_cast_operand(expr: &Expr) -> bool {
    cast_parts(expr).is_some() || decimal_to_double_argument(expr).is_some()
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

fn node_schema(plan: &LogicalPlan) -> Result<DFSchema> {
    if let LogicalPlan::TableScan(scan) = plan {
        return DFSchema::try_from_qualified_schema(
            scan.table_name.clone(),
            scan.source.schema().as_ref(),
        );
    }
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    Ok(schema)
}

fn shield_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if !node_has_candidate(&plan)? {
        return Ok(Transformed::no(plan));
    }
    let schema = node_schema(&plan)?;
    let preserver = NamePreserver::new(&plan);
    let rewritten = plan.map_expressions(|expr| {
        let saved = preserver.save(&expr);
        let shielded = expr.transform_up(|node| Ok(shield_comparison(node, &schema)))?;
        Ok(shielded.update_data(|node| saved.restore(node)))
    })?;
    if rewritten.transformed {
        rewritten.map_data(LogicalPlan::recompute_schema)
    } else {
        Ok(rewritten)
    }
}

fn is_constant_side(expr: &Expr) -> bool {
    !expr
        .exists(|node| {
            Ok(matches!(
                node,
                Expr::Column(_)
                    | Expr::OuterReferenceColumn(..)
                    | Expr::ScalarSubquery(_)
                    | Expr::InSubquery(_)
                    | Expr::Exists(_)
            ))
        })
        .unwrap_or(true)
}

fn reads_input(expr: &Expr) -> bool {
    expr.exists(|node| {
        Ok(matches!(
            node,
            Expr::Column(_) | Expr::OuterReferenceColumn(..)
        ))
    })
    .unwrap_or(true)
}

fn shield_operand(expr: &Expr, schema: &DFSchema) -> Option<Expr> {
    if let (Some(argument), Expr::ScalarFunction(call)) = (decimal_to_double_argument(expr), expr) {
        let shielded = shield_operand(argument, schema)?;
        return Some(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::clone(&call.func),
            vec![shielded],
        )));
    }
    let (operand, target) = cast_parts(expr)?;
    let source = operand.get_type(schema).ok()?;
    if is_narrowing(&source, target) {
        return reads_input(operand).then(|| cast_shield_call(expr.clone()));
    }
    if !is_lossless(&source, target) {
        return None;
    }
    let inner = Box::new(shield_operand(operand, schema)?);
    Some(match expr {
        Expr::TryCast(cast) => Expr::TryCast(TryCast {
            expr: inner,
            field: cast.field.clone(),
        }),
        Expr::Cast(cast) => Expr::Cast(Cast {
            expr: inner,
            field: cast.field.clone(),
        }),
        other => other.clone(),
    })
}

fn shield_comparison(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
            let new_left = is_constant_side(&right)
                .then(|| shield_operand(&left, schema))
                .flatten();
            let new_right = is_constant_side(&left)
                .then(|| shield_operand(&right, schema))
                .flatten();
            if new_left.is_none() && new_right.is_none() {
                return Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right }));
            }
            Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                left: new_left.map_or(left, Box::new),
                op,
                right: new_right.map_or(right, Box::new),
            }))
        }
        Expr::InList(in_list) if in_list.list.iter().all(is_constant_side) => {
            shield_in_list(in_list, schema)
        }
        Expr::Between(between)
            if is_constant_side(&between.low) && is_constant_side(&between.high) =>
        {
            match shield_operand(&between.expr, schema) {
                Some(shielded) => Transformed::yes(Expr::Between(Between {
                    expr: Box::new(shielded),
                    ..between
                })),
                None => Transformed::no(Expr::Between(between)),
            }
        }
        other => Transformed::no(other),
    }
}

fn shield_in_list(in_list: InList, schema: &DFSchema) -> Transformed<Expr> {
    let shielded = shield_operand(&in_list.expr, schema);
    let changed = shielded.is_some();
    let in_list = InList {
        expr: shielded.map_or(in_list.expr, Box::new),
        ..in_list
    };
    match in_list_ranges(&in_list, schema) {
        Some(ranges) => Transformed::yes(ranges),
        None if changed => Transformed::yes(Expr::InList(in_list)),
        None => Transformed::no(Expr::InList(in_list)),
    }
}

fn in_list_ranges(in_list: &InList, schema: &DFSchema) -> Option<Expr> {
    if in_list.list.is_empty() || in_list.list.len() > IN_RANGE_LIMIT {
        return None;
    }
    let cast = shielded_cast(&in_list.expr)?;
    let (operand, target) = cast_parts(cast)?;
    if operand.is_volatile() {
        return None;
    }
    let source = operand.get_type(schema).ok()?;
    let mut terms = Vec::with_capacity(in_list.list.len());
    for item in &in_list.list {
        let Expr::Literal(constant, _) = item else {
            return None;
        };
        let (lower, upper) = cast_preimage(&source, target, constant)?;
        terms.push(range_term(operand, lower, upper, in_list.negated));
    }
    let combine = if in_list.negated { Expr::and } else { Expr::or };
    terms.into_iter().reduce(combine)
}

fn range_term(operand: &Expr, lower: ScalarValue, upper: ScalarValue, negated: bool) -> Expr {
    if negated {
        operand
            .clone()
            .lt(lit(lower))
            .or(operand.clone().gt_eq(lit(upper)))
    } else {
        operand
            .clone()
            .gt_eq(lit(lower))
            .and(operand.clone().lt(lit(upper)))
    }
}

#[cfg(test)]
mod stats_barrier_tests;

#[cfg(test)]
mod tests;
