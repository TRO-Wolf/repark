use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::source_as_provider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Distinct, Expr, ExprSchemable, JoinType, LogicalPlan};
use datafusion::prelude::SessionContext;

use super::negated_null_store::ViewDefinitionPlans;

pub const NARROWED_BESIDE_NULL_UDF_NAME: &str = "__repark_narrowed_beside_null__";
pub const NARROW_TIMESTAMP_NS_UDF_NAME: &str = "__repark_narrow_timestamp_ns__";

#[allow(clippy::missing_errors_doc)]
pub fn refuse_narrowed_ns_writes<'a>(
    ctx: &SessionContext,
    table: &str,
    plan: &LogicalPlan,
    targets: impl IntoIterator<Item = (&'a str, &'a DataType)>,
) -> Result<()> {
    let targets: Vec<(&str, &DataType)> = targets.into_iter().collect();
    if !targets
        .iter()
        .any(|(_, target)| nanosecond_target(target).is_some())
    {
        return Ok(());
    }
    let state = ctx.state();
    let Ok(analyzed) =
        state
            .analyzer()
            .execute_and_check(plan.clone(), state.config_options(), |_, _| {})
    else {
        return Ok(());
    };
    let views = state.config().get_extension::<ViewDefinitionPlans>();
    refuse_narrowed_ns_columns(table, &analyzed, targets, views.as_deref())
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_narrowed_ns_columns<'a>(
    table: &str,
    analyzed: &LogicalPlan,
    targets: impl IntoIterator<Item = (&'a str, &'a DataType)>,
    views: Option<&ViewDefinitionPlans>,
) -> Result<()> {
    let targets: Vec<(&str, &DataType)> = targets.into_iter().collect();
    if analyzed.schema().fields().len() != targets.len() {
        return Ok(());
    }
    for (index, (column, target)) in targets.into_iter().enumerate() {
        let Some(zoned) = nanosecond_target(target) else {
            continue;
        };
        if column_narrowed(analyzed, index, views) {
            return Err(narrowed_refusal(table, column, zoned));
        }
    }
    Ok(())
}

#[must_use]
pub fn narrowed_refusal(table: &str, column: &str, zoned: bool) -> DataFusionError {
    let (upper, lower) = if zoned {
        ("TIMESTAMPTZ_NS", "timestamptz_ns")
    } else {
        ("TIMESTAMP_NS", "timestamp_ns")
    };
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table {table}: Cannot safely cast `{column}` \"TIMESTAMP\" to \"{upper}\". The \
         value was narrowed from nanoseconds to microseconds before the store; give the NULL \
         beside it the type {lower}. SQLSTATE: KD000"
    ))
}

fn nanosecond_target(target: &DataType) -> Option<bool> {
    match target {
        DataType::Timestamp(TimeUnit::Nanosecond, zone) => Some(zone.is_some()),
        _ => None,
    }
}

#[must_use]
pub fn column_narrowed(
    plan: &LogicalPlan,
    index: usize,
    views: Option<&ViewDefinitionPlans>,
) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => projection
            .expr
            .get(index)
            .is_some_and(|expr| expr_narrowed(expr, Some(&projection.input), views)),
        LogicalPlan::SubqueryAlias(alias) => column_narrowed(&alias.input, index, views),
        LogicalPlan::Filter(filter) => column_narrowed(&filter.input, index, views),
        LogicalPlan::Sort(sort) => column_narrowed(&sort.input, index, views),
        LogicalPlan::Limit(limit) => column_narrowed(&limit.input, index, views),
        LogicalPlan::Repartition(repartition) => column_narrowed(&repartition.input, index, views),
        LogicalPlan::Distinct(Distinct::All(input)) => column_narrowed(input, index, views),
        LogicalPlan::Distinct(Distinct::On(on)) => on
            .select_expr
            .get(index)
            .is_some_and(|expr| expr_narrowed(expr, Some(&on.input), views)),
        LogicalPlan::Values(values) => values.values.iter().any(|row| {
            row.get(index)
                .is_some_and(|expr| expr_narrowed(expr, None, views))
        }),
        LogicalPlan::Union(union) => union.inputs.iter().any(|input| {
            if marked_null_branch(input, index) {
                union
                    .inputs
                    .iter()
                    .any(|other| narrowed_branch(other, index))
            } else {
                column_narrowed(input, index, views)
            }
        }),
        LogicalPlan::Join(join) => {
            let left_width = join.left.schema().fields().len();
            match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    if index < left_width {
                        column_narrowed(&join.left, index, views)
                    } else {
                        column_narrowed(&join.right, index - left_width, views)
                    }
                }
                JoinType::LeftSemi | JoinType::LeftAnti => {
                    column_narrowed(&join.left, index, views)
                }
                JoinType::RightSemi | JoinType::RightAnti => {
                    column_narrowed(&join.right, index, views)
                }
                JoinType::LeftMark | JoinType::RightMark => subtree_narrowed(plan),
            }
        }
        LogicalPlan::Aggregate(aggregate) => {
            let grouped = aggregate.group_expr.len();
            let produced = grouped + aggregate.aggr_expr.len();
            if aggregate.schema.fields().len() != produced {
                return subtree_narrowed(plan);
            }
            aggregate
                .group_expr
                .iter()
                .chain(&aggregate.aggr_expr)
                .nth(index)
                .is_some_and(|expr| expr_narrowed(expr, Some(&aggregate.input), views))
        }
        LogicalPlan::Window(window) => {
            let carried = window.input.schema().fields().len();
            if index < carried {
                column_narrowed(&window.input, index, views)
            } else {
                window
                    .window_expr
                    .get(index - carried)
                    .is_some_and(|expr| expr_narrowed(expr, Some(&window.input), views))
            }
        }
        LogicalPlan::TableScan(scan) => {
            let source_index = match &scan.projection {
                Some(projection) => projection.get(index).copied(),
                None => Some(index),
            };
            let Some(source_index) = source_index else {
                return false;
            };
            if let Some(source) = scan.source.get_logical_plan() {
                return column_narrowed(&source, source_index, views);
            }
            views
                .zip(source_as_provider(&scan.source).ok())
                .and_then(|(views, provider)| views.definition_plan(provider.as_ref()))
                .is_some_and(|source| column_narrowed(&source, source_index, views))
        }
        LogicalPlan::EmptyRelation(_) => false,
        _ => subtree_narrowed(plan),
    }
}

fn expr_narrowed(
    expr: &Expr,
    input: Option<&LogicalPlan>,
    views: Option<&ViewDefinitionPlans>,
) -> bool {
    let carried =
        |branch: &Expr| carries_timestamp(branch, input) && expr_narrowed(branch, input, views);
    match expr {
        Expr::ScalarFunction(function) if function.func.name() == NARROWED_BESIDE_NULL_UDF_NAME => {
            function.args.first().is_some_and(|marked| {
                beside_a_narrowing(marked, input) || expr_narrowed(marked, input, views)
            })
        }
        Expr::Column(column) => input.is_some_and(|input| {
            input
                .schema()
                .index_of_column(column)
                .is_ok_and(|position| column_narrowed(input, position, views))
        }),
        Expr::ScalarSubquery(subquery) => column_narrowed(&subquery.subquery, 0, views),
        Expr::Case(case) => {
            case.when_then_expr.iter().any(|(_, then)| carried(then))
                || case.else_expr.as_deref().is_some_and(carried)
        }
        Expr::HigherOrderFunction(function) => {
            function.args.iter().any(|argument| match argument {
                Expr::Lambda(lambda) => expr_narrowed(&lambda.body, input, views),
                value => carried(value),
            })
        }
        Expr::AggregateFunction(aggregate) => aggregate.params.args.iter().any(carried),
        Expr::WindowFunction(window) => window.params.args.iter().any(carried),
        other => {
            let mut found = false;
            let walked = other.apply_children(|branch| {
                found = carried(branch);
                Ok(if found {
                    TreeNodeRecursion::Stop
                } else {
                    TreeNodeRecursion::Continue
                })
            });
            walked.is_ok() && found
        }
    }
}

fn beside_a_narrowing(marked: &Expr, input: Option<&LogicalPlan>) -> bool {
    let narrowed = |branch: &Expr| narrows_nanoseconds(branch, input);
    match marked {
        Expr::Case(case) => {
            case.when_then_expr.iter().any(|(_, then)| narrowed(then))
                || case.else_expr.as_deref().is_some_and(narrowed)
        }
        other => {
            let mut found = false;
            let walked = other.apply_children(|branch| {
                found = narrowed(branch);
                Ok(if found {
                    TreeNodeRecursion::Stop
                } else {
                    TreeNodeRecursion::Continue
                })
            });
            walked.is_ok() && found
        }
    }
}

fn narrows_nanoseconds(expr: &Expr, input: Option<&LogicalPlan>) -> bool {
    let nanoseconds = |source: &Expr| {
        matches!(
            expr_type(source, input),
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, _))
        )
    };
    let coarser = |target: &DataType| matches!(target, DataType::Timestamp(unit, _) if *unit != TimeUnit::Nanosecond);
    match expr {
        Expr::Alias(alias) => narrows_nanoseconds(&alias.expr, input),
        Expr::Cast(cast) => {
            coarser(cast.field.data_type()) && nanoseconds(&cast.expr) && !null_literal(&cast.expr)
        }
        Expr::TryCast(cast) => {
            coarser(cast.field.data_type()) && nanoseconds(&cast.expr) && !null_literal(&cast.expr)
        }
        Expr::ScalarFunction(function) => {
            function.func.name() == NARROW_TIMESTAMP_NS_UDF_NAME
                && function.args.first().is_some_and(nanoseconds)
        }
        _ => false,
    }
}

fn marked_null_branch(input: &LogicalPlan, index: usize) -> bool {
    let LogicalPlan::Projection(projection) = input else {
        return false;
    };
    let mut branch = projection.expr.get(index);
    while let Some(Expr::Alias(alias)) = branch {
        branch = Some(alias.expr.as_ref());
    }
    let Some(Expr::ScalarFunction(function)) = branch else {
        return false;
    };
    function.func.name() == NARROWED_BESIDE_NULL_UDF_NAME
        && function.args.first().is_some_and(null_literal)
}

fn null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(value, _) => value.is_null(),
        Expr::Cast(cast) => null_literal(&cast.expr),
        Expr::TryCast(cast) => null_literal(&cast.expr),
        _ => false,
    }
}

fn narrowed_branch(input: &LogicalPlan, index: usize) -> bool {
    let LogicalPlan::Projection(projection) = input else {
        return false;
    };
    projection
        .expr
        .get(index)
        .is_some_and(|branch| narrows_nanoseconds(branch, Some(&projection.input)))
}

fn expr_type(expr: &Expr, input: Option<&LogicalPlan>) -> Result<DataType> {
    match input {
        Some(input) => expr.get_type(input.schema().as_ref()),
        None => expr.get_type(&DFSchema::empty()),
    }
}

fn carries_timestamp(expr: &Expr, input: Option<&LogicalPlan>) -> bool {
    expr_type(expr, input).map_or(true, |data_type| holds_timestamp(&data_type))
}

fn holds_timestamp(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, _) => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| holds_timestamp(field.data_type())),
        DataType::Map(field, _)
        | DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::RunEndEncoded(_, field) => holds_timestamp(field.data_type()),
        DataType::Dictionary(_, values) => holds_timestamp(values),
        _ => false,
    }
}

fn subtree_narrowed(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let walked = plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            found = expr
                .exists(|inner| {
                    Ok(matches!(
                        inner,
                        Expr::ScalarFunction(function)
                            if function.func.name() == NARROWED_BESIDE_NULL_UDF_NAME
                    ))
                })
                .unwrap_or(false);
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    walked.is_ok() && found
}

#[cfg(test)]
mod tests;
