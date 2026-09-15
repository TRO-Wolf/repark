use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result, ScalarValue, plan_err};
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::functions_window::lead_lag::lag_udwf;
use datafusion::logical_expr::expr::{
    AggregateFunction, Case, Cast, ScalarFunction, Sort, WindowFunction, WindowFunctionParams,
};
use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, Operator, Projection, ScalarUDF,
    SubqueryAlias, WindowFrame, WindowFrameBound, WindowFrameUnits, lit,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::Partitioning;

use crate::decimal_cast::spark_nonnull_udf;
use crate::spark_session_window::{
    SESSION_END_COLUMN, SESSION_END_TS_COLUMN, SESSION_FUNCTION_NAME, SESSION_GAP_COLUMN,
    SESSION_INDEX_COLUMN, SESSION_NEW_COLUMN, SESSION_OUTPUT_NAME, SESSION_PREV_END_COLUMN,
    SESSION_PREV_GAP_COLUMN, SESSION_PREV_TS_COLUMN, SESSION_START_COLUMN, SESSION_TS_COLUMN,
    session_assemble_udf, session_end_udf, session_ts_udf,
};
use crate::spark_time_window::parse_session_gap;

#[derive(Debug, Default)]
pub struct SparkSessionWindow;

impl AnalyzerRule for SparkSessionWindow {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let partitions = config.execution.target_partitions.max(1);
        plan.transform_up_with_subqueries(|plan| rewrite_session_plan(plan, partitions))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_session_window"
    }
}

#[derive(Clone)]
enum SessionGap {
    Fixed(i64),
    PerRow(Expr),
}

#[derive(Clone)]
struct SessionSpec {
    time: Expr,
    gap: SessionGap,
}

impl SessionSpec {
    fn same_specification(&self, other: &SessionSpec) -> bool {
        if self.time != other.time {
            return false;
        }
        match (&self.gap, &other.gap) {
            (SessionGap::Fixed(first), SessionGap::Fixed(second)) => first == second,
            (SessionGap::PerRow(first), SessionGap::PerRow(second)) => first == second,
            _ => false,
        }
    }
}

fn session_call_of(expression: &Expr) -> Result<Option<(Option<String>, SessionSpec)>> {
    let (call, alias) = match expression {
        Expr::ScalarFunction(call) if call.func.name() == SESSION_FUNCTION_NAME => {
            (call.clone(), None)
        }
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::ScalarFunction(call) if call.func.name() == SESSION_FUNCTION_NAME => {
                (call.clone(), Some(alias.name.clone()))
            }
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    if call.args.len() != 2 {
        return Ok(None);
    }
    let gap = match &call.args[1] {
        Expr::Literal(ScalarValue::Utf8(Some(text)) | ScalarValue::LargeUtf8(Some(text)), _) => {
            SessionGap::Fixed(parse_session_gap(text.as_str())?)
        }
        Expr::Literal(ScalarValue::Null, _) => {
            return plan_err!("'session_window' gapDuration must be a duration string, got NULL");
        }
        other => SessionGap::PerRow(other.clone()),
    };
    Ok(Some((
        alias,
        SessionSpec {
            time: call.args[0].clone(),
            gap,
        },
    )))
}

fn is_bare_session_column(expression: &Expr) -> bool {
    match expression {
        Expr::Column(column) => column.name == SESSION_OUTPUT_NAME,
        _ => false,
    }
}

fn marker_session_spec(input: &LogicalPlan) -> Result<Option<(Option<String>, SessionSpec)>> {
    marker_session_spec_inner(input, &mut None)
}

fn marker_session_spec_inner(
    input: &LogicalPlan,
    seen: &mut Option<(Option<String>, SessionSpec)>,
) -> Result<Option<(Option<String>, SessionSpec)>> {
    match input {
        LogicalPlan::Projection(projection) => {
            for expression in &projection.expr {
                let Some((alias, spec)) = session_call_of(expression)? else {
                    continue;
                };
                match seen.as_ref() {
                    Some((_, previous)) if !previous.same_specification(&spec) => {
                        return plan_err!(
                            "'session_window' takes one gap specification per query block; found a second one"
                        );
                    }
                    Some(_) => {}
                    None => {
                        *seen = Some((alias, spec));
                    }
                }
            }
            marker_session_spec_inner(&projection.input, seen)
        }
        LogicalPlan::SubqueryAlias(alias) => marker_session_spec_inner(&alias.input, seen),
        _ => Ok(seen.clone()),
    }
}

fn strip_session_markers(input: &LogicalPlan, spec: &SessionSpec) -> Result<LogicalPlan> {
    Ok(strip_session_markers_inner(input, spec)?.unwrap_or_else(|| input.clone()))
}

fn strip_session_markers_inner(
    input: &LogicalPlan,
    spec: &SessionSpec,
) -> Result<Option<LogicalPlan>> {
    match input {
        LogicalPlan::Projection(projection) => {
            let mut kept: Vec<Expr> = Vec::with_capacity(projection.expr.len());
            let mut stripped = false;
            for expression in &projection.expr {
                match session_call_of(expression)? {
                    Some((_, marker)) if marker.same_specification(spec) => {
                        stripped = true;
                    }
                    _ => kept.push(expression.clone()),
                }
            }
            let next_inner = strip_session_markers_inner(&projection.input, spec)?;
            let changed = next_inner.is_some();
            let next_input = next_inner.unwrap_or_else(|| projection.input.as_ref().clone());
            if (!stripped || kept.is_empty()) && !changed {
                return Ok(None);
            }
            let kept = if stripped && !kept.is_empty() {
                kept
            } else {
                projection.expr.clone()
            };
            Ok(Some(
                LogicalPlanBuilder::from(next_input)
                    .project(kept)?
                    .build()?,
            ))
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let next = strip_session_markers_inner(&alias.input, spec)?;
            let rebuilt = next
                .map(|plan| SubqueryAlias::try_new(Arc::new(plan), alias.alias.clone()))
                .transpose()?
                .map(LogicalPlan::SubqueryAlias);
            Ok(rebuilt)
        }
        _ => Ok(None),
    }
}

fn lag_over(value: Expr, keys: &[Expr], time: &Expr) -> Expr {
    Expr::WindowFunction(Box::new(WindowFunction {
        fun: lag_udwf().into(),
        params: WindowFunctionParams {
            args: vec![
                value,
                Expr::Literal(ScalarValue::Int64(Some(1)), None),
                Expr::Literal(ScalarValue::Null, None),
            ],
            partition_by: keys.to_vec(),
            order_by: vec![Sort::new(time.clone(), true, true)],
            window_frame: WindowFrame::new(None),
            filter: None,
            null_treatment: None,
            distinct: false,
        },
    }))
}

fn running_max_over(value: Expr, keys: &[Expr], time: &Expr) -> Expr {
    Expr::WindowFunction(Box::new(WindowFunction {
        fun: max_udaf().into(),
        params: WindowFunctionParams {
            args: vec![value],
            partition_by: keys.to_vec(),
            order_by: vec![Sort::new(time.clone(), true, true)],
            window_frame: WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
            ),
            filter: None,
            null_treatment: None,
            distinct: false,
        },
    }))
}

fn running_sum_over(value: Expr, keys: &[Expr], time: &Expr) -> Expr {
    Expr::WindowFunction(Box::new(WindowFunction {
        fun: sum_udaf().into(),
        params: WindowFunctionParams {
            args: vec![value],
            partition_by: keys.to_vec(),
            order_by: vec![Sort::new(time.clone(), true, true)],
            window_frame: WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            ),
            filter: None,
            null_treatment: None,
            distinct: false,
        },
    }))
}

pub(crate) fn binary(left: Expr, operator: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op: operator,
        right: Box::new(right),
    })
}

fn new_session_flag(ts: &Expr, previous: &Expr, gap: &Expr) -> Expr {
    let previous_missing = Expr::IsNull(Box::new(previous.clone()));
    let ts_missing = Expr::IsNull(Box::new(ts.clone()));
    let gap_exceeded = binary(
        binary(ts.clone(), Operator::Minus, previous.clone()),
        Operator::Gt,
        gap.clone(),
    );
    let started = binary(
        binary(previous_missing, Operator::Or, ts_missing),
        Operator::Or,
        gap_exceeded,
    );
    Expr::Case(Case::new(
        None,
        vec![(Box::new(started), Box::new(lit(1i64)))],
        Some(Box::new(lit(0i64))),
    ))
}

fn new_session_flag_after_end(ts: &Expr, previous_end: &Expr) -> Expr {
    let previous_missing = Expr::IsNull(Box::new(previous_end.clone()));
    let ts_missing = Expr::IsNull(Box::new(ts.clone()));
    let gap_exceeded = binary(ts.clone(), Operator::Gt, previous_end.clone());
    let started = binary(
        binary(previous_missing, Operator::Or, ts_missing),
        Operator::Or,
        gap_exceeded,
    );
    Expr::Case(Case::new(
        None,
        vec![(Box::new(started), Box::new(lit(1i64)))],
        Some(Box::new(lit(0i64))),
    ))
}

fn session_aggregate_udf(udf: Arc<ScalarUDF>, arguments: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(udf, arguments))
}

fn column_expression(name: &str) -> Expr {
    Expr::Column(Column::from_name(name))
}

fn session_group_parts(aggregate: &Aggregate) -> Result<Option<(Vec<Expr>, String, SessionSpec)>> {
    let mut keys: Vec<Expr> = Vec::new();
    let mut seen: Option<(Option<String>, SessionSpec)> = None;
    let mut bare_session_key = false;
    for expression in &aggregate.group_expr {
        let Some((alias, spec)) = session_call_of(expression)? else {
            if is_bare_session_column(expression) {
                bare_session_key = true;
                continue;
            }
            keys.push(expression.clone());
            continue;
        };
        match seen.as_ref() {
            Some((_, previous)) if !previous.same_specification(&spec) => {
                return plan_err!(
                    "'session_window' takes one gap specification per query block; found a second one"
                );
            }
            Some(_) => {}
            None => {
                seen = Some((alias, spec));
            }
        }
    }
    let Some((alias, spec)) = seen else {
        if !bare_session_key {
            return Ok(None);
        }
        let Some((alias, spec)) = marker_session_spec(&aggregate.input)? else {
            return Ok(None);
        };
        let name = alias.unwrap_or_else(|| SESSION_OUTPUT_NAME.to_string());
        return Ok(Some((keys, name, spec)));
    };
    let name = alias.unwrap_or_else(|| SESSION_OUTPUT_NAME.to_string());
    Ok(Some((keys, name, spec)))
}

fn enrich_session_input(base: LogicalPlan, spec: &SessionSpec) -> Result<LogicalPlan> {
    let base_columns: Vec<Expr> = base
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            Expr::Column(Column::new(qualifier.cloned(), field.name().clone()))
        })
        .collect();
    match &spec.gap {
        SessionGap::PerRow(expression) => {
            let end_ts = session_aggregate_udf(
                session_end_udf(),
                vec![spec.time.clone(), expression.clone()],
            )
            .alias(SESSION_END_TS_COLUMN);
            let projected = LogicalPlanBuilder::from(base)
                .project([base_columns, vec![end_ts]].concat())?
                .build()?;
            LogicalPlanBuilder::from(projected)
                .filter(Expr::IsNotNull(Box::new(column_expression(
                    SESSION_END_TS_COLUMN,
                ))))?
                .build()
        }
        SessionGap::Fixed(micros) => {
            let ts_micros = session_aggregate_udf(session_ts_udf(), vec![spec.time.clone()])
                .alias(SESSION_TS_COLUMN);
            let gap_projected =
                Expr::Literal(ScalarValue::Int64(Some(*micros)), None).alias(SESSION_GAP_COLUMN);
            LogicalPlanBuilder::from(base)
                .project([base_columns, vec![ts_micros, gap_projected]].concat())?
                .build()
        }
    }
}

fn sessionize_input(
    input: &LogicalPlan,
    keys: &[Expr],
    spec: &SessionSpec,
    partitions: usize,
) -> Result<LogicalPlan> {
    let base = strip_session_markers(input, spec)?;
    let row_filter = match &spec.gap {
        SessionGap::Fixed(micros) if *micros <= 0 => {
            Expr::Literal(ScalarValue::Boolean(Some(false)), None)
        }
        _ => Expr::IsNotNull(Box::new(spec.time.clone())),
    };
    let base = LogicalPlanBuilder::from(base).filter(row_filter)?.build()?;
    let dynamic = matches!(spec.gap, SessionGap::PerRow(_));
    let enriched = enrich_session_input(base, spec)?;
    let mut sorts: Vec<Expr> = keys.to_vec();
    sorts.push(spec.time.clone());
    let partitioning = if keys.is_empty() {
        Partitioning::RoundRobinBatch(1)
    } else {
        Partitioning::Hash(keys.to_vec(), partitions)
    };
    let ordered = LogicalPlanBuilder::from(enriched)
        .repartition(partitioning)?
        .sort(
            sorts
                .iter()
                .map(|key| key.clone().sort(true, true))
                .collect::<Vec<_>>(),
        )?
        .build()?;
    let windowed = if dynamic {
        let previous_end =
            running_max_over(column_expression(SESSION_END_TS_COLUMN), keys, &spec.time)
                .alias(SESSION_PREV_END_COLUMN);
        LogicalPlanBuilder::from(ordered)
            .window(vec![previous_end])?
            .build()?
    } else {
        let ts_column = column_expression(SESSION_TS_COLUMN);
        let gap_column = column_expression(SESSION_GAP_COLUMN);
        let previous_ts =
            lag_over(ts_column.clone(), keys, &spec.time).alias(SESSION_PREV_TS_COLUMN);
        let previous_gap =
            lag_over(gap_column.clone(), keys, &spec.time).alias(SESSION_PREV_GAP_COLUMN);
        LogicalPlanBuilder::from(ordered)
            .window(vec![previous_ts, previous_gap])?
            .build()?
    };
    let window_columns: Vec<Expr> = windowed
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            Expr::Column(Column::new(qualifier.cloned(), field.name().clone()))
        })
        .collect();
    let flag = if dynamic {
        new_session_flag_after_end(&spec.time, &column_expression(SESSION_PREV_END_COLUMN))
    } else {
        new_session_flag(
            &column_expression(SESSION_TS_COLUMN),
            &column_expression(SESSION_PREV_TS_COLUMN),
            &column_expression(SESSION_PREV_GAP_COLUMN),
        )
    }
    .alias(SESSION_NEW_COLUMN);
    let flagged = LogicalPlanBuilder::from(windowed)
        .project([window_columns, vec![flag]].concat())?
        .build()?;
    let session_index = running_sum_over(column_expression(SESSION_NEW_COLUMN), keys, &spec.time)
        .alias(SESSION_INDEX_COLUMN);
    let indexed = LogicalPlanBuilder::from(flagged)
        .window(vec![session_index])?
        .build()?;
    Ok(indexed)
}

fn cast_date_time(time: Expr, schema: &DFSchema) -> Expr {
    if matches!(&time, Expr::Cast(_)) {
        return time;
    }
    let Expr::Column(column) = &time else {
        return time;
    };
    let is_date = schema
        .fields()
        .iter()
        .any(|field| field.name() == &column.name && matches!(field.data_type(), DataType::Date32));
    if is_date {
        Expr::Cast(Cast::new(
            Box::new(time),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ))
    } else {
        time
    }
}

fn sessionize_output(
    aggregate: &Aggregate,
    indexed: LogicalPlan,
    keys: &[Expr],
    name: &str,
    spec: &SessionSpec,
) -> Result<LogicalPlan> {
    let ts_column = column_expression(SESSION_TS_COLUMN);
    let gap_column = column_expression(SESSION_GAP_COLUMN);
    let starts = Expr::AggregateFunction(AggregateFunction::new_udf(
        min_udaf(),
        vec![spec.time.clone()],
        false,
        None,
        Vec::new(),
        None,
    ))
    .alias(SESSION_START_COLUMN);
    let end_input = match spec.gap {
        SessionGap::Fixed(_) => binary(ts_column.clone(), Operator::Plus, gap_column.clone()),
        SessionGap::PerRow(_) => column_expression(SESSION_END_TS_COLUMN),
    };
    let ends = Expr::AggregateFunction(AggregateFunction::new_udf(
        max_udaf(),
        vec![end_input],
        false,
        None,
        Vec::new(),
        None,
    ))
    .alias(SESSION_END_COLUMN);
    let mut session_groups = keys.to_vec();
    session_groups.push(column_expression(SESSION_INDEX_COLUMN));
    let mut session_aggrs = aggregate.aggr_expr.clone();
    session_aggrs.push(starts);
    session_aggrs.push(ends);
    let grouped = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(indexed),
        session_groups,
        session_aggrs,
    )?);
    let schema = grouped.schema().clone();
    let group_count = keys.len() + 1;
    let mut projected: Vec<Expr> = Vec::new();
    for field in schema.fields().iter().take(group_count) {
        if field.name() != SESSION_INDEX_COLUMN {
            projected.push(column_expression(field.name()));
        }
    }
    let assembled = session_aggregate_udf(
        session_assemble_udf(),
        vec![
            column_expression(SESSION_START_COLUMN),
            column_expression(SESSION_END_COLUMN),
        ],
    );
    let windowed_output = Expr::ScalarFunction(ScalarFunction::new_udf(
        spark_nonnull_udf(),
        vec![assembled],
    ))
    .alias(name);
    projected.push(windowed_output);
    for field in schema.fields().iter().skip(group_count) {
        if field.name() != SESSION_START_COLUMN && field.name() != SESSION_END_COLUMN {
            projected.push(column_expression(field.name()));
        }
    }
    LogicalPlanBuilder::from(grouped)
        .project(projected)?
        .build()
}

pub(crate) fn rebase_expression(expression: Expr, schema: &DFSchema) -> Expr {
    let fallback = expression.clone();
    match expression
        .transform(|node| {
            let Expr::Column(column) = &node else {
                return Ok(Transformed::no(node));
            };
            let mut qualified: Option<Column> = None;
            let mut ambiguous = false;
            for (qualifier, field) in schema.iter() {
                if field.name() != &column.name {
                    continue;
                }
                if qualified.is_some() {
                    ambiguous = true;
                    break;
                }
                qualified = Some(match qualifier {
                    Some(relation) => Column::new(Some(relation.clone()), column.name.clone()),
                    None => Column::from_name(column.name.clone()),
                });
            }
            match (ambiguous, qualified) {
                (false, Some(next)) => Ok(Transformed::yes(Expr::Column(next))),
                _ => Ok(Transformed::no(node)),
            }
        })
        .data()
    {
        Ok(rewritten) => rewritten,
        Err(_) => fallback,
    }
}

fn rebase_spec(spec: &SessionSpec, schema: &DFSchema) -> SessionSpec {
    let gap = match &spec.gap {
        SessionGap::Fixed(micros) => SessionGap::Fixed(*micros),
        SessionGap::PerRow(expression) => {
            SessionGap::PerRow(rebase_expression(expression.clone(), schema))
        }
    };
    SessionSpec {
        time: rebase_expression(spec.time.clone(), schema),
        gap,
    }
}

fn sessionize_aggregate(aggregate: &Aggregate, partitions: usize) -> Result<Option<LogicalPlan>> {
    let Some((keys, name, spec)) = session_group_parts(aggregate)? else {
        return Ok(None);
    };
    let spec = rebase_spec(&spec, aggregate.input.schema());
    let spec = SessionSpec {
        time: cast_date_time(spec.time, aggregate.input.schema()),
        gap: spec.gap,
    };
    let indexed = sessionize_input(&aggregate.input, &keys, &spec, partitions)?;
    let rewritten = sessionize_output(aggregate, indexed, &keys, &name, &spec)?;
    Ok(Some(rewritten))
}

fn is_session_display(name: &str) -> bool {
    name.starts_with("session_window(") && name.ends_with(')')
}

fn remap_stale_session_refs(expression: Expr, input: &LogicalPlan) -> Expr {
    let names: Vec<&str> = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    if names
        .iter()
        .filter(|name| **name == SESSION_OUTPUT_NAME)
        .count()
        != 1
    {
        return expression;
    }
    expression
        .clone()
        .transform_up(|node| match node {
            Expr::Column(column)
                if column.name == SESSION_OUTPUT_NAME && column.relation.is_some() =>
            {
                Ok(Transformed::yes(Expr::Column(Column::from_name(
                    SESSION_OUTPUT_NAME,
                ))))
            }
            Expr::Column(column)
                if column.relation.is_none()
                    && column.name != SESSION_OUTPUT_NAME
                    && is_session_display(column.name.as_str())
                    && !names.contains(&column.name.as_str()) =>
            {
                Ok(Transformed::yes(Expr::Column(Column::from_name(
                    SESSION_OUTPUT_NAME,
                ))))
            }
            other => Ok(Transformed::no(other)),
        })
        .data()
        .unwrap_or(expression)
}

fn rewrite_session_plan(plan: LogicalPlan, partitions: usize) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => match sessionize_aggregate(&aggregate, partitions)? {
            Some(rewritten) => Ok(Transformed::new(
                rewritten,
                true,
                TreeNodeRecursion::Continue,
            )),
            None => Ok(Transformed::no(LogicalPlan::Aggregate(aggregate))),
        },
        LogicalPlan::Projection(projection) => {
            let expressions: Vec<Expr> = projection
                .expr
                .iter()
                .map(|expression| remap_stale_session_refs(expression.clone(), &projection.input))
                .collect();
            if expressions == projection.expr {
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            }
            Ok(Transformed::new(
                LogicalPlan::Projection(Projection::try_new(
                    expressions,
                    Arc::clone(&projection.input),
                )?),
                true,
                TreeNodeRecursion::Continue,
            ))
        }
        other => Ok(Transformed::no(other)),
    }
}
