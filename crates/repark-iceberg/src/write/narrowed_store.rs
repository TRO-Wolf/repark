use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::source_as_provider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Distinct, Expr, ExprSchemable, JoinType, LogicalPlan};
use datafusion::prelude::SessionContext;

use super::negated_null_store::ViewDefinitionPlans;

pub const NARROWED_BESIDE_NULL_UDF_NAME: &str = "__repark_narrowed_beside_null__";
pub const NARROWED_BESIDE_VALUE_UDF_NAME: &str = "__repark_narrowed_beside_value__";
pub const NARROW_TIMESTAMP_NS_UDF_NAME: &str = "__repark_narrow_timestamp_ns__";
const DATE_TRUNC_UDF_NAME: &str = "date_trunc";
const TIMESTAMP_TO_DATE_UDF_NAME: &str = "__repark_timestamp_to_date__";

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
        if let Some(narrowing) = column_narrowed(analyzed, index, views) {
            return Err(narrowed_refusal(table, column, zoned, &narrowing));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Narrowing {
    pub value: String,
    pub beside_untyped_null: bool,
    pub zoned: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct Written {
    over_zoned: bool,
    over_naive: bool,
}

#[must_use]
pub fn narrowed_refusal(
    table: &str,
    column: &str,
    zoned: bool,
    narrowing: &Narrowing,
) -> DataFusionError {
    let (upper, lower) = if zoned {
        ("TIMESTAMPTZ_NS", "timestamptz_ns")
    } else {
        ("TIMESTAMP_NS", "timestamp_ns")
    };
    let head = format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
         the table {table}: Cannot safely cast `{column}` \"TIMESTAMP\" to \"{upper}\"."
    );
    if narrowing.beside_untyped_null {
        return DataFusionError::Plan(format!(
            "{head} The value was narrowed from nanoseconds to microseconds before the store; \
             give the NULL beside it the type {lower}. SQLSTATE: KD000"
        ));
    }
    let value = &narrowing.value;
    DataFusionError::Plan(format!(
        "{head} The value {value} was narrowed from nanoseconds to microseconds before the \
         store, to match the microsecond value beside it; write CAST({value} AS TIMESTAMP) if \
         microseconds are intended, or give the value beside it a nanosecond type. SQLSTATE: \
         KD000"
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
) -> Option<Narrowing> {
    column_lineage(plan, index, views, Written::default())
}

fn column_lineage(
    plan: &LogicalPlan,
    index: usize,
    views: Option<&ViewDefinitionPlans>,
    written: Written,
) -> Option<Narrowing> {
    let below = |input: &LogicalPlan, index: usize| column_lineage(input, index, views, written);
    let of = |expr: &Expr, input: &LogicalPlan| expr_lineage(expr, Some(input), views, written);
    match plan {
        LogicalPlan::Projection(projection) => of(projection.expr.get(index)?, &projection.input),
        LogicalPlan::SubqueryAlias(alias) => below(&alias.input, index),
        LogicalPlan::Filter(filter) => below(&filter.input, index),
        LogicalPlan::Sort(sort) => below(&sort.input, index),
        LogicalPlan::Limit(limit) => below(&limit.input, index),
        LogicalPlan::Repartition(repartition) => below(&repartition.input, index),
        LogicalPlan::Distinct(Distinct::All(input)) => below(input, index),
        LogicalPlan::Distinct(Distinct::On(on)) => of(on.select_expr.get(index)?, &on.input),
        LogicalPlan::Values(values) => values.values.iter().find_map(|row| {
            row.get(index)
                .and_then(|expr| expr_lineage(expr, None, views, written))
        }),
        LogicalPlan::Union(union) => union.inputs.iter().find_map(|input| below(input, index)),
        LogicalPlan::Join(join) => {
            let left_width = join.left.schema().fields().len();
            match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    if index < left_width {
                        below(&join.left, index)
                    } else {
                        below(&join.right, index - left_width)
                    }
                }
                JoinType::LeftSemi | JoinType::LeftAnti => below(&join.left, index),
                JoinType::RightSemi | JoinType::RightAnti => below(&join.right, index),
                JoinType::LeftMark | JoinType::RightMark => subtree_narrowed(plan),
            }
        }
        LogicalPlan::Aggregate(aggregate) => {
            let grouped = aggregate.group_expr.len();
            let produced = grouped + aggregate.aggr_expr.len();
            if aggregate.schema.fields().len() != produced {
                return subtree_narrowed(plan);
            }
            let expr = (aggregate.group_expr.iter())
                .chain(&aggregate.aggr_expr)
                .nth(index)?;
            of(expr, &aggregate.input)
        }
        LogicalPlan::Window(window) => {
            let carried = window.input.schema().fields().len();
            if index < carried {
                below(&window.input, index)
            } else {
                of(window.window_expr.get(index - carried)?, &window.input)
            }
        }
        LogicalPlan::TableScan(scan) => {
            let source_index = match &scan.projection {
                Some(projection) => projection.get(index).copied()?,
                None => index,
            };
            if let Some(source) = scan.source.get_logical_plan() {
                return below(&source, source_index);
            }
            let source = views
                .zip(source_as_provider(&scan.source).ok())
                .and_then(|(views, provider)| views.definition_plan(provider.as_ref()))?;
            below(&source, source_index)
        }
        LogicalPlan::EmptyRelation(_) => None,
        _ => subtree_narrowed(plan),
    }
}

fn expr_lineage(
    expr: &Expr,
    input: Option<&LogicalPlan>,
    views: Option<&ViewDefinitionPlans>,
    written: Written,
) -> Option<Narrowing> {
    let walk = |branch: &Expr, written: Written| {
        if carries_timestamp(branch, input) {
            expr_lineage(branch, input, views, written)
        } else {
            None
        }
    };
    let carried = |branch: &Expr| walk(branch, written);
    match expr {
        Expr::Cast(cast) if coarser(cast.field.data_type()) => {
            narrowing_of(&cast.expr, input, written).or_else(|| {
                if instant(cast.field.data_type()) {
                    None
                } else {
                    carried(&cast.expr)
                }
            })
        }
        Expr::Cast(cast) if matches!(cast.field.data_type(), DataType::Date32) => walk(
            &cast.expr,
            Written {
                over_naive: true,
                ..written
            },
        ),
        Expr::ScalarFunction(function) if function.func.name() == NARROW_TIMESTAMP_NS_UDF_NAME => {
            narrowing_of(function.args.first()?, input, written)
        }
        Expr::ScalarFunction(function) if function.func.name() == TIMESTAMP_TO_DATE_UDF_NAME => {
            (function.args.iter()).find_map(|argument| {
                walk(
                    argument,
                    Written {
                        over_naive: true,
                        ..written
                    },
                )
            })
        }
        Expr::ScalarFunction(function) if function.func.name() == DATE_TRUNC_UDF_NAME => {
            (function.args.iter()).find_map(|argument| {
                walk(
                    argument,
                    Written {
                        over_zoned: true,
                        ..written
                    },
                )
            })
        }
        Expr::Column(column) => {
            let input = input?;
            let position = input.schema().index_of_column(column).ok()?;
            column_lineage(input, position, views, written)
        }
        Expr::ScalarSubquery(subquery) => column_lineage(&subquery.subquery, 0, views, written),
        Expr::Case(case) => (case.when_then_expr.iter())
            .find_map(|(_, then)| carried(then))
            .or_else(|| case.else_expr.as_deref().and_then(carried)),
        Expr::HigherOrderFunction(function) => {
            function.args.iter().find_map(|argument| match argument {
                Expr::Lambda(lambda) => expr_lineage(&lambda.body, input, views, written),
                value => carried(value),
            })
        }
        Expr::AggregateFunction(aggregate) => aggregate.params.args.iter().find_map(carried),
        Expr::WindowFunction(window) => window.params.args.iter().find_map(carried),
        other => {
            let mut found = None;
            let walked = other.apply_children(|branch| {
                found = carried(branch);
                Ok(if found.is_some() {
                    TreeNodeRecursion::Stop
                } else {
                    TreeNodeRecursion::Continue
                })
            });
            walked.ok().and(found)
        }
    }
}

fn narrowing_of(
    narrowed: &Expr,
    input: Option<&LogicalPlan>,
    written: Written,
) -> Option<Narrowing> {
    let Expr::ScalarFunction(mark) = narrowed else {
        return None;
    };
    let beside_untyped_null = match mark.func.name() {
        NARROWED_BESIDE_NULL_UDF_NAME => true,
        NARROWED_BESIDE_VALUE_UDF_NAME => false,
        _ => return None,
    };
    let value = mark.args.first()?;
    let zoned = matches!(expr_type(value, input), Ok(DataType::Timestamp(_, Some(_))));
    if (zoned && written.over_zoned) || (!zoned && written.over_naive) {
        return None;
    }
    Some(Narrowing {
        value: value.human_display().to_string(),
        beside_untyped_null,
        zoned,
    })
}

fn coarser(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(unit, _) if *unit != TimeUnit::Nanosecond)
}

fn instant(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, Some(_)))
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
        DataType::Timestamp(_, _) | DataType::Date32 => true,
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

fn subtree_narrowed(plan: &LogicalPlan) -> Option<Narrowing> {
    let mut found = None;
    let walked = plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|inner| {
                if let Expr::Cast(cast) = inner
                    && coarser(cast.field.data_type())
                {
                    found = narrowing_of(&cast.expr, None, Written::default());
                }
                if let Expr::ScalarFunction(function) = inner
                    && function.func.name() == NARROW_TIMESTAMP_NS_UDF_NAME
                {
                    found = (function.args.first())
                        .and_then(|value| narrowing_of(value, None, Written::default()));
                }
                Ok(if found.is_some() {
                    TreeNodeRecursion::Stop
                } else {
                    TreeNodeRecursion::Continue
                })
            })
        })
    });
    walked.ok().and(found)
}

#[cfg(test)]
mod tests;
