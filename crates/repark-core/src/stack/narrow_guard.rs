use std::borrow::Cow;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, internal_err};
use datafusion::logical_expr::expr::{BinaryExpr, Case};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{Expr, ExprSchemable, Filter, LogicalPlan, Operator};

pub(crate) const NARROW_DECIMAL_PREFIX: &str = "__repark_narrow_to_decimal_";

pub(crate) fn guard_narrow_decimal_plan(plan: &LogicalPlan) -> Result<Cow<'_, LogicalPlan>> {
    if !plan_has_narrow_call(plan)? {
        return Ok(Cow::Borrowed(plan));
    }
    let rewritten = plan.clone().transform_up_with_subqueries(guard_node)?;
    if rewritten.transformed {
        Ok(Cow::Owned(rewritten.data))
    } else {
        Ok(Cow::Borrowed(plan))
    }
}

fn is_narrow_name(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(call) if call.name().starts_with(NARROW_DECIMAL_PREFIX))
}

fn holds_narrow_call(expr: &Expr) -> bool {
    expr.exists(|node| Ok(is_narrow_name(node)))
        .unwrap_or(false)
}

fn node_holds_narrow_call(plan: &LogicalPlan) -> Result<bool> {
    if matches!(plan, LogicalPlan::TableScan(_)) {
        return Ok(false);
    }
    let mut found = false;
    plan.apply_expressions(|expr| {
        found = holds_narrow_call(expr);
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn plan_has_narrow_call(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        found = node_holds_narrow_call(node)?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn input_schema(plan: &LogicalPlan) -> DFSchema {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    schema
}

fn guard_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if !node_holds_narrow_call(&plan)? {
        return Ok(Transformed::no(plan));
    }
    let schema = input_schema(&plan);
    if let LogicalPlan::Filter(filter) = plan {
        return guard_filter(filter, &schema);
    }
    let preserver = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let saved = preserver.save(&expr);
        Ok(guard_expr(expr, None, &schema)?.update_data(|guarded| saved.restore(guarded)))
    })
}

fn guard_filter(filter: Filter, schema: &DFSchema) -> Result<Transformed<LogicalPlan>> {
    let nested = matches!(filter.input.as_ref(), LogicalPlan::Values(_));
    let mut guarded = Vec::new();
    let mut condition: Option<Expr> = None;
    let mut changed = false;
    for conjunct in split_conjunction_owned(filter.predicate.clone()) {
        let result = guard_expr(conjunct, condition.clone(), schema)?;
        changed |= result.transformed;
        if is_copyable(&result.data) {
            let part = if nested {
                Expr::IsNotFalse(Box::new(result.data.clone()))
            } else {
                result.data.clone()
            };
            condition = Some(extend(condition, part));
        }
        guarded.push(result.data);
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
    }
    let Some(predicate) = conjunction(guarded) else {
        return internal_err!("a guarded filter lost its predicate");
    };
    Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
        predicate,
        filter.input,
    )?)))
}

fn extend(condition: Option<Expr>, part: Expr) -> Expr {
    match condition {
        Some(before) => before.and(part),
        None => part,
    }
}

fn is_copyable(expr: &Expr) -> bool {
    !expr.is_volatile()
        && !expr
            .exists(|node| {
                Ok(matches!(
                    node,
                    Expr::ScalarSubquery(_)
                        | Expr::InSubquery(_)
                        | Expr::Exists(_)
                        | Expr::OuterReferenceColumn(..)
                ))
            })
            .unwrap_or(true)
}

fn has_integer_argument(args: &[Expr], schema: &DFSchema) -> bool {
    matches!(args, [argument] if matches!(
        argument.get_type(schema),
        Ok(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64)
    ))
}

fn guard_expr(expr: Expr, condition: Option<Expr>, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if !holds_narrow_call(&expr) {
        return Ok(Transformed::no(expr));
    }
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(op, Operator::And | Operator::Or) =>
        {
            let left = guard_expr(*left, condition.clone(), schema)?;
            let right_condition = if holds_narrow_call(&right) && is_copyable(&left.data) {
                let gate = if op == Operator::And {
                    Expr::IsNotFalse(Box::new(left.data.clone()))
                } else {
                    Expr::IsNotTrue(Box::new(left.data.clone()))
                };
                Some(extend(condition, gate))
            } else {
                condition
            };
            let right = guard_expr(*right, right_condition, schema)?;
            let changed = left.transformed || right.transformed;
            let rebuilt = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left.data),
                op,
                Box::new(right.data),
            ));
            Ok(if changed {
                Transformed::yes(rebuilt)
            } else {
                Transformed::no(rebuilt)
            })
        }
        Expr::ScalarFunction(call)
            if call.name().starts_with(NARROW_DECIMAL_PREFIX)
                && has_integer_argument(&call.args, schema) =>
        {
            let call = Expr::ScalarFunction(call);
            Ok(match condition {
                Some(when) => Transformed::yes(Expr::Case(Case::new(
                    None,
                    vec![(Box::new(when), Box::new(call))],
                    None,
                ))),
                None => Transformed::no(call),
            })
        }
        other => other.map_children(|child| guard_expr(child, condition.clone(), schema)),
    }
}

#[cfg(test)]
mod tests;
