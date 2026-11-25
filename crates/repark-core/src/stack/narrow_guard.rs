use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result, internal_err};
use datafusion::logical_expr::expr::{
    AggregateFunction, AggregateFunctionParams, BinaryExpr, Case, ScalarFunction,
};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{
    Aggregate, Expr, ExprSchemable, Filter, Join, LogicalPlan, Operator, Projection, lit,
};

pub(crate) const NARROW_DECIMAL_PREFIX: &str = "__repark_narrow_to_decimal_";
const GUARD_PREFIX: &str = "__repark_narrow_guard_";
static NEXT_GUARD: AtomicUsize = AtomicUsize::new(0);

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
    let preserver = NamePreserver::new(&plan);
    match plan {
        LogicalPlan::Filter(filter) => {
            let predicate = chain(filter.predicate.clone(), None, &schema)?;
            if !predicate.transformed {
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            }
            Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                predicate.data,
                filter.input,
            )?)))
        }
        LogicalPlan::Join(join) => guard_join(join, &schema),
        LogicalPlan::Projection(projection) => guard_projection(projection, &schema, &preserver),
        LogicalPlan::Aggregate(aggregate) => guard_aggregate(aggregate, &schema, &preserver),
        other => Ok(Transformed::no(other)),
    }
}

fn guard_join(join: Join, schema: &DFSchema) -> Result<Transformed<LogicalPlan>> {
    let Some(filter) = join.filter.clone() else {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    };
    let guarded = chain(filter, None, schema)?;
    if !guarded.transformed {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    Ok(Transformed::yes(LogicalPlan::Join(Join {
        filter: Some(guarded.data),
        ..join
    })))
}

fn guard_projection(
    projection: Projection,
    schema: &DFSchema,
    preserver: &NamePreserver,
) -> Result<Transformed<LogicalPlan>> {
    let Projection {
        expr,
        input,
        schema: output,
        ..
    } = projection;
    let mut layers = Layers::default();
    let mut changed = false;
    let mut guarded = Vec::with_capacity(expr.len());
    for item in expr {
        let saved = preserver.save(&item);
        let result = value(item, None, schema, &mut layers)?;
        changed |= result.transformed;
        guarded.push(saved.restore(result.data));
    }
    let input = layers.build(input, &guarded)?;
    let rebuilt = LogicalPlan::Projection(Projection::try_new_with_schema(guarded, input, output)?);
    Ok(if changed {
        Transformed::yes(rebuilt)
    } else {
        Transformed::no(rebuilt)
    })
}

fn guard_aggregate(
    aggregate: Aggregate,
    schema: &DFSchema,
    preserver: &NamePreserver,
) -> Result<Transformed<LogicalPlan>> {
    let Aggregate {
        input,
        group_expr,
        aggr_expr,
        schema: output,
        ..
    } = aggregate;
    let mut layers = Layers::default();
    let mut changed = false;
    let mut guarded = Vec::with_capacity(aggr_expr.len());
    for item in aggr_expr {
        let saved = preserver.save(&item);
        let result = value(item, None, schema, &mut layers)?;
        changed |= result.transformed;
        guarded.push(saved.restore(result.data));
    }
    let input = layers.build(input, &guarded)?;
    let rebuilt = LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
        input, group_expr, guarded, output,
    )?);
    Ok(if changed {
        Transformed::yes(rebuilt)
    } else {
        Transformed::no(rebuilt)
    })
}

fn when_else(when: Expr, then: Expr, otherwise: Expr) -> Expr {
    Expr::Case(Case::new(
        None,
        vec![(Box::new(when), Box::new(then))],
        Some(Box::new(otherwise)),
    ))
}

fn has_integer_argument(args: &[Expr], schema: &DFSchema) -> bool {
    matches!(args, [argument] if matches!(
        argument.get_type(schema),
        Ok(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64)
    ))
}

fn is_target(call: &ScalarFunction, schema: &DFSchema) -> bool {
    call.name().starts_with(NARROW_DECIMAL_PREFIX) && has_integer_argument(&call.args, schema)
}

fn holds_target(expr: &Expr, schema: &DFSchema) -> bool {
    holds_narrow_call(expr)
        && expr
            .exists(|node| {
                Ok(matches!(node, Expr::ScalarFunction(call) if is_target(call, schema)))
            })
            .unwrap_or(false)
}

fn wrap_call(call: ScalarFunction, condition: Option<&Expr>) -> Transformed<Expr> {
    let call = Expr::ScalarFunction(call);
    match condition {
        Some(when) => Transformed::yes(Expr::Case(Case::new(
            None,
            vec![(Box::new(when.clone()), Box::new(call))],
            None,
        ))),
        None => Transformed::no(call),
    }
}

fn is_lazy_connective(op: Operator) -> bool {
    matches!(op, Operator::And | Operator::Or)
}

fn chain(
    predicate: Expr,
    condition: Option<&Expr>,
    schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    if !holds_target(&predicate, schema) {
        return Ok(Transformed::no(predicate));
    }
    let (volatile, steady): (Vec<Expr>, Vec<Expr>) = split_conjunction_owned(predicate.clone())
        .into_iter()
        .partition(Expr::is_volatile);
    let conjuncts: Vec<Expr> = volatile.into_iter().chain(steady).collect();
    let Some(last) = conjuncts
        .iter()
        .rposition(|conjunct| holds_target(conjunct, schema))
    else {
        return Ok(Transformed::no(predicate));
    };
    let mut changed = last > 0;
    let mut guarded = Vec::with_capacity(conjuncts.len());
    for conjunct in conjuncts {
        let result = truth(conjunct, condition, schema)?;
        changed |= result.transformed;
        guarded.push(result.data);
    }
    if !changed {
        return Ok(Transformed::no(predicate));
    }
    let rest = guarded.split_off(last + 1);
    let Some(mut nested) = guarded.pop() else {
        return internal_err!("a guarded conjunct chain lost its last conjunct");
    };
    while let Some(before) = guarded.pop() {
        nested = when_else(before, nested, lit(false));
    }
    let Some(predicate) = conjunction(std::iter::once(nested).chain(rest)) else {
        return internal_err!("a guarded conjunct chain lost its predicate");
    };
    Ok(Transformed::yes(predicate))
}

fn truth(expr: Expr, condition: Option<&Expr>, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if !holds_target(&expr, schema) {
        return Ok(Transformed::no(expr));
    }
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if is_lazy_connective(op) && holds_target(&right, schema) =>
        {
            let left = truth(*left, condition, schema)?.data;
            let right = truth(*right, condition, schema)?.data;
            Ok(Transformed::yes(if op == Operator::And {
                when_else(left, right, lit(false))
            } else {
                when_else(left, lit(true), right)
            }))
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_lazy_connective(op) => {
            let left = truth(*left, condition, schema)?;
            Ok(left
                .update_data(|left| Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, right))))
        }
        other => plain(other, condition, schema),
    }
}

fn plain(expr: Expr, condition: Option<&Expr>, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if !holds_target(&expr, schema) {
        return Ok(Transformed::no(expr));
    }
    match expr {
        Expr::Case(case) => guard_case(case, condition, schema),
        Expr::ScalarFunction(call) if is_target(&call, schema) => Ok(wrap_call(call, condition)),
        other => other.map_children(|child| plain(child, condition, schema)),
    }
}

fn guard_case(
    case: Case,
    condition: Option<&Expr>,
    schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    let Case {
        expr,
        when_then_expr,
        else_expr,
    } = case;
    let mut changed = false;
    let base = match expr {
        Some(base) => {
            let result = plain(*base, condition, schema)?;
            changed |= result.transformed;
            Some(Box::new(result.data))
        }
        None => None,
    };
    let mut branches = Vec::with_capacity(when_then_expr.len());
    for (when, then) in when_then_expr {
        let when = truth(*when, condition, schema)?;
        let then = plain(*then, condition, schema)?;
        changed |= when.transformed || then.transformed;
        branches.push((Box::new(when.data), Box::new(then.data)));
    }
    let otherwise = match else_expr {
        Some(otherwise) => {
            let result = plain(*otherwise, condition, schema)?;
            changed |= result.transformed;
            Some(Box::new(result.data))
        }
        None => None,
    };
    let rebuilt = Expr::Case(Case::new(base, branches, otherwise));
    Ok(if changed {
        Transformed::yes(rebuilt)
    } else {
        Transformed::no(rebuilt)
    })
}

fn is_materializable(expr: &Expr) -> bool {
    !expr
        .exists(|node| {
            Ok(matches!(
                node,
                Expr::ScalarSubquery(_)
                    | Expr::InSubquery(_)
                    | Expr::Exists(_)
                    | Expr::OuterReferenceColumn(..)
                    | Expr::AggregateFunction(_)
                    | Expr::WindowFunction(_)
            ))
        })
        .unwrap_or(true)
}

fn value(
    expr: Expr,
    condition: Option<&Expr>,
    schema: &DFSchema,
    layers: &mut Layers,
) -> Result<Transformed<Expr>> {
    if !holds_target(&expr, schema) {
        return Ok(Transformed::no(expr));
    }
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if is_lazy_connective(op)
                && holds_target(&right, schema)
                && is_materializable(&left) =>
        {
            let left = value(*left, condition, schema, layers)?.data;
            let column = layers.define(left);
            let gate = if op == Operator::And {
                Expr::IsNotFalse(Box::new(column.clone()))
            } else {
                Expr::IsNotTrue(Box::new(column.clone()))
            };
            let gated = match condition {
                Some(before) => layers.define(before.clone().and(gate)),
                None => gate,
            };
            let right = value(*right, Some(&gated), schema, layers)?.data;
            Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(column),
                op,
                Box::new(right),
            ))))
        }
        Expr::Case(case) => guard_case(case, condition, schema),
        Expr::AggregateFunction(function) => {
            guard_aggregate_function(function, condition, schema, layers)
        }
        Expr::ScalarFunction(call) if is_target(&call, schema) => Ok(wrap_call(call, condition)),
        other => other.map_children(|child| value(child, condition, schema, layers)),
    }
}

fn guard_aggregate_function(
    function: AggregateFunction,
    condition: Option<&Expr>,
    schema: &DFSchema,
    layers: &mut Layers,
) -> Result<Transformed<Expr>> {
    let AggregateFunction { func, params } = function;
    let AggregateFunctionParams {
        args,
        distinct,
        filter,
        order_by,
        null_treatment,
    } = params;
    let mut changed = false;
    let mut guarded = Vec::with_capacity(args.len());
    for argument in args {
        let result = value(argument, condition, schema, layers)?;
        changed |= result.transformed;
        guarded.push(result.data);
    }
    let filter = match filter {
        Some(filter) => {
            let result = chain(*filter, condition, schema)?;
            changed |= result.transformed;
            Some(Box::new(result.data))
        }
        None => None,
    };
    let rebuilt = Expr::AggregateFunction(AggregateFunction {
        func,
        params: AggregateFunctionParams {
            args: guarded,
            distinct,
            filter,
            order_by,
            null_treatment,
        },
    });
    Ok(if changed {
        Transformed::yes(rebuilt)
    } else {
        Transformed::no(rebuilt)
    })
}

#[derive(Default)]
struct Layers {
    defs: Vec<(String, Expr, usize)>,
    levels: HashMap<String, usize>,
}

impl Layers {
    fn level_of(&self, expr: &Expr) -> usize {
        expr.column_refs()
            .iter()
            .filter(|column| column.relation.is_none())
            .filter_map(|column| self.levels.get(&column.name))
            .max()
            .copied()
            .unwrap_or(0)
    }

    fn define(&mut self, expr: Expr) -> Expr {
        let level = self.level_of(&expr) + 1;
        let name = format!(
            "{GUARD_PREFIX}{}__",
            NEXT_GUARD.fetch_add(1, AtomicOrdering::Relaxed)
        );
        self.levels.insert(name.clone(), level);
        self.defs.push((name.clone(), expr, level));
        Expr::Column(Column::new_unqualified(name))
    }

    fn last_uses(&self, top: &[Expr], depth: usize) -> HashMap<String, usize> {
        let mut last: HashMap<String, usize> = HashMap::new();
        for (_, expr, level) in &self.defs {
            for column in expr.column_refs() {
                if column.relation.is_none() && self.levels.contains_key(&column.name) {
                    let entry = last.entry(column.name.clone()).or_insert(0);
                    *entry = (*entry).max(*level);
                }
            }
        }
        for expr in top {
            for column in expr.column_refs() {
                if column.relation.is_none() && self.levels.contains_key(&column.name) {
                    last.insert(column.name.clone(), depth + 1);
                }
            }
        }
        last
    }

    fn build(self, input: Arc<LogicalPlan>, top: &[Expr]) -> Result<Arc<LogicalPlan>> {
        let Some(depth) = self.defs.iter().map(|(_, _, level)| *level).max() else {
            return Ok(input);
        };
        let last = self.last_uses(top, depth);
        let base: Vec<Expr> = input
            .schema()
            .iter()
            .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
            .collect();
        let mut by_level: Vec<Vec<(String, Expr)>> = vec![Vec::new(); depth + 1];
        let mut defined: Vec<(String, usize)> = Vec::with_capacity(self.defs.len());
        for (name, expr, level) in self.defs {
            defined.push((name.clone(), level));
            if let Some(slot) = by_level.get_mut(level) {
                slot.push((name, expr));
            }
        }
        let mut plan = input;
        for (level, definitions) in by_level.into_iter().enumerate().skip(1) {
            let mut exprs = base.clone();
            for (name, defined_at) in &defined {
                if *defined_at < level && last.get(name).copied().unwrap_or(0) > level {
                    exprs.push(Expr::Column(Column::new_unqualified(name.clone())));
                }
            }
            for (name, expr) in definitions {
                exprs.push(expr.alias(name));
            }
            plan = Arc::new(LogicalPlan::Projection(Projection::try_new(exprs, plan)?));
        }
        Ok(plan)
    }
}

#[cfg(test)]
mod tests;
