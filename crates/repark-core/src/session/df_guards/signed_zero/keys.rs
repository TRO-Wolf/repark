use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, Result, ScalarValue};
use datafusion::logical_expr::expr::{
    Between, Case, GroupingSet, InList, Sort as SortExpr, WindowFunction,
};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::grouping_set_to_exprlist;
use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Distinct, Expr, ExprSchemable, LogicalPlan, Operator, Partitioning,
    Projection, RecursiveQuery, Window,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use super::udf::{canonical_literal, is_float, is_normalize_zero_call, normalize_zero_call};

const RULE_NAME: &str = "signed_zero_keys";

const LINEAGE_DEPTH: usize = 64;

#[derive(Debug, Default)]
pub(crate) struct SignedZeroKeys;

impl OptimizerRule for SignedZeroKeys {
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

struct Scope {
    schema: DFSchemaRef,
    inputs: Vec<Arc<LogicalPlan>>,
}

impl Scope {
    fn over(input: &Arc<LogicalPlan>) -> Self {
        Self {
            schema: Arc::clone(input.schema()),
            inputs: vec![Arc::clone(input)],
        }
    }

    fn over_pair(left: &Arc<LogicalPlan>, right: &Arc<LogicalPlan>) -> Self {
        let mut merged = left.schema().as_ref().clone();
        merged.merge(right.schema());
        Self {
            schema: Arc::new(merged),
            inputs: vec![Arc::clone(left), Arc::clone(right)],
        }
    }

    fn is_float(&self, expr: &Expr) -> bool {
        expr.get_type(self.schema.as_ref())
            .is_ok_and(|data_type| is_float(&data_type))
    }

    fn normalized(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Alias(alias) => self.normalized(&alias.expr),
            Expr::Column(column) => self
                .inputs
                .iter()
                .any(|input| defines_normalized(input, column, 0)),
            other => is_normalize_zero_call(other),
        }
    }
}

pub(crate) fn rewrite_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if fast_exit(&plan)? {
        return Ok(Transformed::no(plan));
    }
    match plan {
        LogicalPlan::Aggregate(aggregate) => rewrite_aggregate(aggregate),
        LogicalPlan::Window(window) => rewrite_window(window),
        LogicalPlan::Join(mut join) => {
            let scope = Scope::over_pair(&join.left, &join.right);
            let mut changed = false;
            let mut on = Vec::with_capacity(join.on.len());
            for (left, right) in std::mem::take(&mut join.on) {
                if scope.is_float(&left) || scope.is_float(&right) {
                    let left = wrap_key(left, &scope);
                    let right = wrap_key(right, &scope);
                    changed |= left.transformed || right.transformed;
                    on.push((left.data, right.data));
                } else {
                    on.push((left, right));
                }
            }
            join.on = on;
            if let Some(filter) = join.filter.take() {
                let filter = comparisons(filter, &scope)?;
                changed |= filter.transformed;
                join.filter = Some(filter.data);
            }
            Ok(Transformed::new_transformed(
                LogicalPlan::Join(join),
                changed,
            ))
        }
        LogicalPlan::Sort(mut sort) => {
            let scope = Scope::over(&sort.input);
            let mut changed = false;
            let keys = std::mem::take(&mut sort.expr);
            sort.expr = keys
                .into_iter()
                .map(|key| {
                    let key = sort_key(key, &scope)?;
                    changed |= key.transformed;
                    Ok(key.data)
                })
                .collect::<Result<_>>()?;
            Ok(Transformed::new_transformed(
                LogicalPlan::Sort(sort),
                changed,
            ))
        }
        LogicalPlan::Repartition(mut repartition) => {
            let scope = Scope::over(&repartition.input);
            let mut changed = false;
            let mut wrap_all = |exprs: Vec<Expr>| -> Vec<Expr> {
                exprs
                    .into_iter()
                    .map(|expr| {
                        let expr = wrap_key(expr, &scope);
                        changed |= expr.transformed;
                        expr.data
                    })
                    .collect()
            };
            repartition.partitioning_scheme = match repartition.partitioning_scheme {
                Partitioning::Hash(exprs, count) => Partitioning::Hash(wrap_all(exprs), count),
                Partitioning::DistributeBy(exprs) => Partitioning::DistributeBy(wrap_all(exprs)),
                other @ Partitioning::RoundRobinBatch(_) => other,
            };
            Ok(Transformed::new_transformed(
                LogicalPlan::Repartition(repartition),
                changed,
            ))
        }
        LogicalPlan::Filter(mut filter) => {
            let scope = Scope::over(&filter.input);
            let predicate = std::mem::replace(
                &mut filter.predicate,
                Expr::Literal(ScalarValue::Null, None),
            );
            let predicate = comparisons(predicate, &scope)?;
            filter.predicate = predicate.data;
            Ok(Transformed::new_transformed(
                LogicalPlan::Filter(filter),
                predicate.transformed,
            ))
        }
        LogicalPlan::Projection(projection) => rewrite_projection(projection),
        LogicalPlan::Distinct(Distinct::All(input)) => Ok(normalized_projection(input)?
            .update_data(|input| LogicalPlan::Distinct(Distinct::All(input)))),
        LogicalPlan::RecursiveQuery(query) if query.is_distinct => rewrite_recursive(query),
        other => Ok(Transformed::no(other)),
    }
}

fn fast_exit(plan: &LogicalPlan) -> Result<bool> {
    if !matches!(
        plan,
        LogicalPlan::Aggregate(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Projection(_)
            | LogicalPlan::Distinct(Distinct::All(_))
            | LogicalPlan::RecursiveQuery(_)
    ) {
        return Ok(true);
    }
    let float_input = plan.inputs().iter().any(|input| {
        input
            .schema()
            .fields()
            .iter()
            .any(|field| is_float(field.data_type()))
    });
    if float_input {
        return Ok(false);
    }
    let mut may_be_float = false;
    plan.apply_expressions(|expr| {
        may_be_float = expr.exists(|node| Ok(makes_float(node)))?;
        Ok(if may_be_float {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(!may_be_float)
}

fn makes_float(expr: &Expr) -> bool {
    match expr {
        Expr::Cast(cast) => is_float(cast.field.data_type()),
        Expr::TryCast(cast) => is_float(cast.field.data_type()),
        Expr::Literal(scalar, _) => is_float(&scalar.data_type()),
        Expr::ScalarFunction(_)
        | Expr::AggregateFunction(_)
        | Expr::WindowFunction(_)
        | Expr::Placeholder(_)
        | Expr::ScalarSubquery(_)
        | Expr::OuterReferenceColumn(..) => true,
        _ => false,
    }
}

fn rewrite_aggregate(aggregate: Aggregate) -> Result<Transformed<LogicalPlan>> {
    let scope = Scope::over(&aggregate.input);
    let preserver = NamePreserver::new_for_projection();
    let Aggregate {
        input,
        group_expr,
        aggr_expr,
        schema,
        ..
    } = aggregate;
    let mut changed = false;
    let mut group = Vec::with_capacity(group_expr.len());
    for expr in group_expr {
        let expr = match expr {
            Expr::GroupingSet(set) => {
                rewrite_grouping_set(set, &scope, &preserver)?.update_data(Expr::GroupingSet)
            }
            other => named(other, &preserver, |expr| key_with_comparisons(expr, &scope))?,
        };
        changed |= expr.transformed;
        group.push(expr.data);
    }
    let mut aggregates = Vec::with_capacity(aggr_expr.len());
    for expr in aggr_expr {
        let expr = named(expr, &preserver, |expr| aggregate_call(expr, &scope))?;
        changed |= expr.transformed;
        aggregates.push(expr.data);
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Aggregate(
            Aggregate::try_new_with_schema(input, group, aggregates, schema)?,
        )));
    }
    Ok(Transformed::yes(LogicalPlan::Aggregate(
        Aggregate::try_new(input, group, aggregates)?,
    )))
}

fn rewrite_grouping_set(
    set: GroupingSet,
    scope: &Scope,
    preserver: &NamePreserver,
) -> Result<Transformed<GroupingSet>> {
    let members = |exprs: Vec<Expr>| -> Result<Transformed<Vec<Expr>>> {
        let mut changed = false;
        let exprs = exprs
            .into_iter()
            .map(|expr| {
                let expr = named(expr, preserver, |expr| key_with_comparisons(expr, scope))?;
                changed |= expr.transformed;
                Ok(expr.data)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Transformed::new_transformed(exprs, changed))
    };
    Ok(match set {
        GroupingSet::Rollup(exprs) => members(exprs)?.update_data(GroupingSet::Rollup),
        GroupingSet::Cube(exprs) => members(exprs)?.update_data(GroupingSet::Cube),
        GroupingSet::GroupingSets(sets) => {
            let mut changed = false;
            let sets = sets
                .into_iter()
                .map(|exprs| {
                    let exprs = members(exprs)?;
                    changed |= exprs.transformed;
                    Ok(exprs.data)
                })
                .collect::<Result<Vec<_>>>()?;
            Transformed::new_transformed(GroupingSet::GroupingSets(sets), changed)
        }
    })
}

fn aggregate_call(expr: Expr, scope: &Scope) -> Result<Transformed<Expr>> {
    let expr = comparisons(expr, scope)?;
    let changed = expr.transformed;
    let rewritten = expr.data.transform_down(|node| match node {
        Expr::AggregateFunction(mut call) => {
            let mut changed = false;
            if call.params.distinct {
                let args = std::mem::take(&mut call.params.args);
                call.params.args = args
                    .into_iter()
                    .map(|arg| {
                        let arg = wrap_key(arg, scope);
                        changed |= arg.transformed;
                        arg.data
                    })
                    .collect();
            }
            let order = std::mem::take(&mut call.params.order_by);
            call.params.order_by = sort_keys(order, scope, &mut changed)?;
            Ok(Transformed::new(
                Expr::AggregateFunction(call),
                changed,
                TreeNodeRecursion::Jump,
            ))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(Transformed::new_transformed(
        rewritten.data,
        changed || rewritten.transformed,
    ))
}

fn rewrite_window(window: Window) -> Result<Transformed<LogicalPlan>> {
    let scope = Scope::over(&window.input);
    let preserver = NamePreserver::new_for_projection();
    let Window {
        input,
        window_expr,
        schema,
        ..
    } = window;
    let mut changed = false;
    let mut exprs = Vec::with_capacity(window_expr.len());
    for expr in window_expr {
        let expr = named(expr, &preserver, |expr| window_call(expr, &scope))?;
        changed |= expr.transformed;
        exprs.push(expr.data);
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Window(
            Window::try_new_with_schema(exprs, input, schema)?,
        )));
    }
    Ok(Transformed::yes(LogicalPlan::Window(Window::try_new(
        exprs, input,
    )?)))
}

fn window_call(expr: Expr, scope: &Scope) -> Result<Transformed<Expr>> {
    let expr = comparisons(expr, scope)?;
    let changed = expr.transformed;
    let rewritten = expr.data.transform_down(|node| match node {
        Expr::WindowFunction(mut call) => {
            let mut changed = false;
            let WindowFunction { params, .. } = call.as_mut();
            if params.distinct {
                let args = std::mem::take(&mut params.args);
                params.args = wrap_all(args, scope, &mut changed);
            }
            let partition = std::mem::take(&mut params.partition_by);
            params.partition_by = wrap_all(partition, scope, &mut changed);
            let order = std::mem::take(&mut params.order_by);
            params.order_by = sort_keys(order, scope, &mut changed)?;
            Ok(Transformed::new(
                Expr::WindowFunction(call),
                changed,
                TreeNodeRecursion::Jump,
            ))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(Transformed::new_transformed(
        rewritten.data,
        changed || rewritten.transformed,
    ))
}

fn rewrite_projection(projection: Projection) -> Result<Transformed<LogicalPlan>> {
    if !projection.expr.iter().any(|expr| {
        expr.exists(|node| Ok(is_comparison_candidate(node)))
            .unwrap_or(true)
    }) {
        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
    }
    let scope = Scope::over(&projection.input);
    let preserver = NamePreserver::new_for_projection();
    let Projection {
        expr: exprs,
        input,
        schema,
        ..
    } = projection;
    let mut changed = false;
    let mut rewritten = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let expr = named(expr, &preserver, |expr| comparisons(expr, &scope))?;
        changed |= expr.transformed;
        rewritten.push(expr.data);
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Projection(
            Projection::try_new_with_schema(rewritten, input, schema)?,
        )));
    }
    Ok(Transformed::yes(LogicalPlan::Projection(
        Projection::try_new(rewritten, input)?,
    )))
}

fn rewrite_recursive(mut query: RecursiveQuery) -> Result<Transformed<LogicalPlan>> {
    let static_term = normalized_projection(Arc::clone(&query.static_term))?;
    let recursive_term = normalized_projection(Arc::clone(&query.recursive_term))?;
    let changed = static_term.transformed || recursive_term.transformed;
    query.static_term = static_term.data;
    query.recursive_term = recursive_term.data;
    Ok(Transformed::new_transformed(
        LogicalPlan::RecursiveQuery(query),
        changed,
    ))
}

fn normalized_projection(input: Arc<LogicalPlan>) -> Result<Transformed<Arc<LogicalPlan>>> {
    let scope = Scope::over(&input);
    let mut changed = false;
    let exprs = input
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            let column = Expr::Column(Column::new(qualifier.cloned(), field.name()));
            if !is_float(field.data_type()) {
                return column;
            }
            let wrapped = wrap_key(column, &scope);
            if !wrapped.transformed {
                return wrapped.data;
            }
            changed = true;
            wrapped
                .data
                .alias_qualified(qualifier.cloned(), field.name())
        })
        .collect::<Vec<_>>();
    if !changed {
        return Ok(Transformed::no(input));
    }
    Ok(Transformed::yes(Arc::new(LogicalPlan::Projection(
        Projection::try_new(exprs, input)?,
    ))))
}

fn named(
    expr: Expr,
    preserver: &NamePreserver,
    rewrite: impl FnOnce(Expr) -> Result<Transformed<Expr>>,
) -> Result<Transformed<Expr>> {
    let saved = preserver.save(&expr);
    Ok(rewrite(expr)?.update_data(|expr| saved.restore(expr)))
}

fn key_with_comparisons(expr: Expr, scope: &Scope) -> Result<Transformed<Expr>> {
    let expr = comparisons(expr, scope)?;
    let changed = expr.transformed;
    let wrapped = wrap_key(expr.data, scope);
    Ok(Transformed::new_transformed(
        wrapped.data,
        changed || wrapped.transformed,
    ))
}

fn sort_key(key: SortExpr, scope: &Scope) -> Result<Transformed<SortExpr>> {
    let SortExpr {
        expr,
        asc,
        nulls_first,
    } = key;
    Ok(
        key_with_comparisons(expr, scope)?.update_data(|expr| SortExpr {
            expr,
            asc,
            nulls_first,
        }),
    )
}

fn sort_keys(keys: Vec<SortExpr>, scope: &Scope, changed: &mut bool) -> Result<Vec<SortExpr>> {
    keys.into_iter()
        .map(|key| {
            let key = sort_key(key, scope)?;
            *changed |= key.transformed;
            Ok(key.data)
        })
        .collect()
}

fn wrap_all(exprs: Vec<Expr>, scope: &Scope, changed: &mut bool) -> Vec<Expr> {
    exprs
        .into_iter()
        .map(|expr| {
            let expr = wrap_key(expr, scope);
            *changed |= expr.transformed;
            expr.data
        })
        .collect()
}

fn wrap_key(expr: Expr, scope: &Scope) -> Transformed<Expr> {
    match expr {
        Expr::Alias(mut alias) => {
            let inner = wrap_key(*alias.expr, scope);
            let changed = inner.transformed;
            alias.expr = Box::new(inner.data);
            Transformed::new_transformed(Expr::Alias(alias), changed)
        }
        Expr::Literal(scalar, metadata) => match canonical_literal(&scalar) {
            Some(canonical) => Transformed::yes(Expr::Literal(canonical, metadata)),
            None => Transformed::no(Expr::Literal(scalar, metadata)),
        },
        other if is_literal_like(&other) || !scope.is_float(&other) || scope.normalized(&other) => {
            Transformed::no(other)
        }
        other => Transformed::yes(normalize_zero_call(other)),
    }
}

fn is_literal_like(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(..) => true,
        Expr::Cast(cast) => is_literal_like(&cast.expr),
        Expr::TryCast(cast) => is_literal_like(&cast.expr),
        Expr::Negative(inner) => is_literal_like(inner),
        _ => false,
    }
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

fn is_comparison_candidate(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => is_comparison(binary.op),
        Expr::Between(_) | Expr::InList(_) => true,
        Expr::Case(case) => case.expr.is_some(),
        _ => false,
    }
}

fn comparisons(expr: Expr, scope: &Scope) -> Result<Transformed<Expr>> {
    if !expr.exists(|node| Ok(is_comparison_candidate(node)))? {
        return Ok(Transformed::no(expr));
    }
    expr.transform_up(|node| Ok(compare_node(node, scope)))
}

fn compare_node(node: Expr, scope: &Scope) -> Transformed<Expr> {
    match node {
        Expr::BinaryExpr(binary)
            if is_comparison(binary.op)
                && !is_literal_like(&binary.left)
                && !is_literal_like(&binary.right)
                && scope.is_float(&binary.left)
                && scope.is_float(&binary.right) =>
        {
            let BinaryExpr { left, op, right } = binary;
            let left = wrap_key(*left, scope);
            let right = wrap_key(*right, scope);
            let changed = left.transformed || right.transformed;
            Transformed::new_transformed(
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left.data),
                    op,
                    Box::new(right.data),
                )),
                changed,
            )
        }
        Expr::Between(between)
            if scope.is_float(&between.expr)
                && !(is_literal_like(&between.low) && is_literal_like(&between.high)) =>
        {
            let Between {
                expr,
                negated,
                low,
                high,
            } = between;
            let expr = wrap_key(*expr, scope);
            let low = wrap_key(*low, scope);
            let high = wrap_key(*high, scope);
            let changed = expr.transformed || low.transformed || high.transformed;
            Transformed::new_transformed(
                Expr::Between(Between::new(
                    Box::new(expr.data),
                    negated,
                    Box::new(low.data),
                    Box::new(high.data),
                )),
                changed,
            )
        }
        Expr::InList(in_list)
            if scope.is_float(&in_list.expr)
                && in_list.list.iter().any(|item| !is_literal_like(item)) =>
        {
            let InList {
                expr,
                list,
                negated,
            } = in_list;
            let mut changed = false;
            let expr = wrap_key(*expr, scope);
            changed |= expr.transformed;
            let list = wrap_all(list, scope, &mut changed);
            Transformed::new_transformed(
                Expr::InList(InList::new(Box::new(expr.data), list, negated)),
                changed,
            )
        }
        Expr::Case(case) if simple_case_compares(&case, scope) => {
            let Case {
                expr,
                when_then_expr,
                else_expr,
            } = case;
            let mut changed = false;
            let expr = expr.map(|operand| {
                let operand = wrap_key(*operand, scope);
                changed |= operand.transformed;
                Box::new(operand.data)
            });
            let when_then_expr = when_then_expr
                .into_iter()
                .map(|(when, then)| {
                    let when = wrap_key(*when, scope);
                    changed |= when.transformed;
                    (Box::new(when.data), then)
                })
                .collect();
            Transformed::new_transformed(
                Expr::Case(Case {
                    expr,
                    when_then_expr,
                    else_expr,
                }),
                changed,
            )
        }
        other => Transformed::no(other),
    }
}

fn simple_case_compares(case: &Case, scope: &Scope) -> bool {
    let Some(operand) = case.expr.as_deref() else {
        return false;
    };
    scope.is_float(operand)
        && (!is_literal_like(operand)
            || case
                .when_then_expr
                .iter()
                .any(|(when, _)| !is_literal_like(when)))
}

fn defines_normalized(plan: &LogicalPlan, column: &Column, depth: usize) -> bool {
    if depth > LINEAGE_DEPTH {
        return false;
    }
    let Some(index) = plan.schema().maybe_index_of_column(column) else {
        return false;
    };
    match plan {
        LogicalPlan::Projection(projection) => projection
            .expr
            .get(index)
            .is_some_and(|expr| defined_by(expr, &projection.input, depth)),
        LogicalPlan::Aggregate(aggregate) => grouping_set_to_exprlist(&aggregate.group_expr)
            .ok()
            .and_then(|exprs| exprs.get(index).copied())
            .is_some_and(|expr| defined_by(expr, &aggregate.input, depth)),
        LogicalPlan::SubqueryAlias(alias) => {
            through_index(&alias.input, alias.input.schema(), index, depth)
        }
        LogicalPlan::Window(window) => {
            through_index(&window.input, window.input.schema(), index, depth)
        }
        LogicalPlan::Filter(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Distinct(Distinct::All(_)) => plan
            .inputs()
            .first()
            .is_some_and(|input| defines_normalized(input, column, depth + 1)),
        LogicalPlan::Join(join) => [&join.left, &join.right]
            .into_iter()
            .find(|side| side.schema().maybe_index_of_column(column).is_some())
            .is_some_and(|side| defines_normalized(side, column, depth + 1)),
        _ => false,
    }
}

fn through_index(input: &LogicalPlan, schema: &DFSchema, index: usize, depth: usize) -> bool {
    if index >= schema.fields().len() {
        return false;
    }
    let (qualifier, field) = schema.qualified_field(index);
    let column = Column::new(qualifier.cloned(), field.name());
    defines_normalized(input, &column, depth + 1)
}

fn defined_by(expr: &Expr, input: &LogicalPlan, depth: usize) -> bool {
    match expr {
        Expr::Alias(alias) => defined_by(&alias.expr, input, depth),
        Expr::Column(column) => defines_normalized(input, column, depth + 1),
        other => is_normalize_zero_call(other),
    }
}
