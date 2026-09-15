use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, Result, ScalarValue, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Aggregate, Expr, Join, LogicalPlan, LogicalPlanBuilder, Projection,
};
use datafusion::optimizer::AnalyzerRule;

use crate::decimal_cast::spark_nonnull_udf;
use crate::spark_session_window::SESSION_FUNCTION_NAME;
use crate::spark_time_window::{
    WINDOW_FUNCTION_NAME, WINDOW_OUTPUT_NAME, check_window_spec, parse_window_duration,
    parse_window_offset, window_starts_udf,
};
use crate::spark_window_time::WINDOW_TIME_FUNCTION_NAME;

pub(crate) const EXPANDED_WINDOW: &str = "__repark_time_window__";

#[derive(Debug, Default)]
pub struct SparkTimeWindow;

impl AnalyzerRule for SparkTimeWindow {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_time_window"
    }
}

#[derive(Debug, Default)]
pub struct SparkWindowTimeGrouping;

impl AnalyzerRule for SparkWindowTimeGrouping {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(refuse_window_time_in_aggregate)
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_window_time_grouping"
    }
}

fn window_time_call(expression: &Expr) -> Option<ScalarFunction> {
    let mut found: Option<ScalarFunction> = None;
    expression
        .apply(|node| match node {
            Expr::ScalarFunction(call) if call.func.name() == WINDOW_TIME_FUNCTION_NAME => {
                found = Some(call.clone());
                Ok(TreeNodeRecursion::Stop)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        })
        .ok()?;
    found
}

fn refuse_window_time_in_aggregate(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match &plan {
        LogicalPlan::Aggregate(aggregate) => {
            for expression in &aggregate.aggr_expr {
                if let Some(call) = window_time_call(expression) {
                    let rendered = Expr::ScalarFunction(call).to_string();
                    return plan_err!(
                        "[MISSING_AGGREGATION] The non-aggregating expression \"{rendered}\" is based on \
                         columns which are not participating in the GROUP BY clause."
                    );
                }
            }
            Ok(Transformed::no(plan))
        }
        LogicalPlan::Projection(projection) => {
            for expression in &projection.expr {
                if let Some(call) = window_time_call(expression) {
                    let Some(argument) = call.args.first() else {
                        continue;
                    };
                    if windowed_argument(argument, &projection.input) {
                        continue;
                    }
                    let rendered = Expr::ScalarFunction(call).to_string();
                    return plan_err!(
                        "[_LEGACY_ERROR_TEMP_3101] The input is not a correct window column: \
                         {rendered};"
                    );
                }
            }
            Ok(Transformed::no(plan))
        }
        _ => Ok(Transformed::no(plan)),
    }
}

fn windowed_argument(argument: &Expr, input: &LogicalPlan) -> bool {
    match argument {
        Expr::ScalarFunction(call)
            if call.func.name() == WINDOW_FUNCTION_NAME
                || call.func.name() == SESSION_FUNCTION_NAME =>
        {
            true
        }
        Expr::Column(column) => windowed_column(column, input),
        Expr::Alias(alias) => windowed_argument(&alias.expr, input),
        Expr::Cast(cast) => windowed_argument(&cast.expr, input),
        Expr::Literal(ScalarValue::Null, _) => true,
        _ => false,
    }
}

fn windowed_column(column: &Column, input: &LogicalPlan) -> bool {
    match input {
        LogicalPlan::Projection(projection) => {
            for expression in &projection.expr {
                match expression {
                    Expr::Alias(alias) if alias.name == column.name => {
                        return windowed_argument(&alias.expr, &projection.input);
                    }
                    Expr::Column(inner) if inner.name == column.name => {
                        return windowed_column(inner, &projection.input);
                    }
                    _ => {}
                }
            }
            false
        }
        LogicalPlan::SubqueryAlias(alias) => windowed_column(column, &alias.input),
        LogicalPlan::Filter(filter) => windowed_column(column, &filter.input),
        LogicalPlan::Limit(limit) => windowed_column(column, &limit.input),
        LogicalPlan::Sort(sort) => windowed_column(column, &sort.input),
        LogicalPlan::Distinct(distinct) => windowed_column(column, distinct.input()),
        LogicalPlan::Repartition(repartition) => windowed_column(column, &repartition.input),
        LogicalPlan::Subquery(subquery) => windowed_column(column, &subquery.subquery),
        LogicalPlan::Join(join) => windowed_join_column(column, join),
        LogicalPlan::Union(union) => {
            !union.inputs.is_empty()
                && union
                    .inputs
                    .iter()
                    .all(|child| windowed_column(column, child))
        }
        LogicalPlan::Aggregate(_) | LogicalPlan::TableScan(_) | LogicalPlan::Window(_) => true,
        _ => false,
    }
}

fn windowed_join_column(column: &Column, join: &Join) -> bool {
    let mut owners = [&join.left, &join.right].into_iter().filter(|child| {
        child
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == &column.name)
    });
    match (owners.next(), owners.next()) {
        (Some(owner), None) => windowed_column(column, owner),
        _ => false,
    }
}

pub(crate) mod session_window;

pub(crate) use session_window::SparkSessionWindow;

#[derive(Clone)]
struct WindowCall {
    time: Expr,
    window: i64,
    slide: i64,
    offset: i64,
}

impl WindowCall {
    fn same_specification(&self, other: &WindowCall) -> bool {
        self.window == other.window
            && self.slide == other.slide
            && self.offset == other.offset
            && self.time == other.time
    }
}

fn duration_literal(argument: &Expr) -> Result<Option<String>> {
    match argument {
        Expr::Literal(ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value), _) => {
            Ok(value.clone())
        }
        Expr::Literal(ScalarValue::Null, _) => Ok(None),
        _ => plan_err!("'window' duration arguments must be literal strings"),
    }
}

fn window_call_of(
    expression: &Expr,
) -> Result<Option<(ScalarFunction, Option<String>, WindowCall)>> {
    let (call, alias) = match expression {
        Expr::ScalarFunction(call) if call.func.name() == WINDOW_FUNCTION_NAME => {
            (call.clone(), None)
        }
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::ScalarFunction(call) if call.func.name() == WINDOW_FUNCTION_NAME => {
                (call.clone(), Some(alias.name.clone()))
            }
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    let Some(spec) = window_spec_of(&call)? else {
        return Ok(None);
    };
    Ok(Some((call.clone(), alias, spec)))
}

fn window_spec_of(call: &ScalarFunction) -> Result<Option<WindowCall>> {
    if !(2..=4).contains(&call.args.len()) {
        return Ok(None);
    }
    let window = match duration_literal(&call.args[1])? {
        Some(text) => parse_window_duration(text.as_str())?,
        None => return Ok(None),
    };
    let slide = match call.args.get(2) {
        None => window,
        Some(argument) => match duration_literal(argument)? {
            Some(text) => parse_window_duration(text.as_str())?,
            None => window,
        },
    };
    let offset = match call.args.get(3) {
        None => 0,
        Some(argument) => match duration_literal(argument)? {
            Some(text) => parse_window_offset(text.as_str())?,
            None => 0,
        },
    };
    let rendered = match &call.args[0] {
        Expr::Column(column) => column.name.clone(),
        other => other.to_string(),
    };
    check_window_spec(window, slide, offset, rendered.as_str())?;
    Ok(Some(WindowCall {
        time: call.args[0].clone(),
        window,
        slide,
        offset,
    }))
}

fn note_window_spec(seen: &mut Option<WindowCall>, spec: WindowCall) -> Result<()> {
    match seen {
        Some(previous) if !previous.same_specification(&spec) => {
            plan_err!("'window' takes one window specification per query block; found a second one")
        }
        Some(_) => Ok(()),
        None => {
            *seen = Some(spec);
            Ok(())
        }
    }
}

fn rewrite_nested_windows(
    expression: Expr,
    input: &LogicalPlan,
    seen: &mut Option<WindowCall>,
    scalar: &mut bool,
    expanded: &mut Option<LogicalPlan>,
) -> Result<Expr> {
    expression
        .transform_up(|node| {
            let Expr::ScalarFunction(call) = &node else {
                return Ok(Transformed::no(node));
            };
            if call.func.name() != WINDOW_FUNCTION_NAME {
                return Ok(Transformed::no(node));
            }
            let Some(spec) = window_spec_of(call)? else {
                return Ok(Transformed::no(node));
            };
            note_window_spec(seen, spec.clone())?;
            if spec.window == spec.slide {
                *scalar = true;
                Ok(Transformed::no(node))
            } else {
                if expanded.is_none() {
                    *expanded = Some(expand_input(input, &spec)?);
                }
                Ok(Transformed::yes(expanded_column()))
            }
        })
        .data()
}

fn expanded_column() -> Expr {
    Expr::Column(Column::from_name(EXPANDED_WINDOW))
}

fn expand_input(input: &LogicalPlan, call: &WindowCall) -> Result<LogicalPlan> {
    let mut projection: Vec<Expr> = input
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            Expr::Column(Column::new(qualifier.cloned(), field.name().clone()))
        })
        .collect();
    projection.push(
        Expr::ScalarFunction(ScalarFunction::new_udf(
            window_starts_udf(),
            vec![
                call.time.clone(),
                Expr::Literal(ScalarValue::Int64(Some(call.window)), None),
                Expr::Literal(ScalarValue::Int64(Some(call.slide)), None),
                Expr::Literal(ScalarValue::Int64(Some(call.offset)), None),
            ],
        ))
        .alias(EXPANDED_WINDOW),
    );
    let projected = LogicalPlanBuilder::from(input.clone())
        .project(projection)?
        .build()?;
    LogicalPlanBuilder::from(projected)
        .unnest_column(Column::from_name(EXPANDED_WINDOW))?
        .build()
}

struct Rewritten {
    expressions: Vec<Expr>,
    expanded: Option<LogicalPlan>,
    touched: bool,
    scalar: bool,
    time: Option<Expr>,
}

fn is_window_display(name: &str) -> bool {
    name.starts_with("window(") && name.ends_with(')')
}

fn remap_dangling_window_refs(expression: Expr, input: &LogicalPlan) -> Expr {
    let names: Vec<&str> = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    if !names.contains(&WINDOW_OUTPUT_NAME) {
        return expression;
    }
    expression
        .clone()
        .transform_up(|node| match node {
            Expr::Column(column)
                if column.relation.is_none()
                    && column.name != WINDOW_OUTPUT_NAME
                    && is_window_display(column.name.as_str())
                    && !names.contains(&column.name.as_str()) =>
            {
                Ok(Transformed::yes(Expr::Column(Column::from_name(
                    WINDOW_OUTPUT_NAME,
                ))))
            }
            other => Ok(Transformed::no(other)),
        })
        .data()
        .unwrap_or(expression)
}

fn nonnull_window(expression: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        spark_nonnull_udf(),
        vec![expression],
    ))
}

fn rewrite_expressions(
    expressions: Vec<Expr>,
    input: &LogicalPlan,
    in_group: bool,
) -> Result<Rewritten> {
    let mut rewritten = Vec::with_capacity(expressions.len());
    let mut expanded: Option<LogicalPlan> = None;
    let mut scalar = false;
    let mut seen: Option<WindowCall> = None;
    for expression in expressions {
        let Some((call, alias, window_call)) = window_call_of(&expression)? else {
            rewritten.push(rewrite_nested_windows(
                expression,
                input,
                &mut seen,
                &mut scalar,
                &mut expanded,
            )?);
            continue;
        };
        note_window_spec(&mut seen, window_call.clone())?;
        let name = alias.unwrap_or_else(|| WINDOW_OUTPUT_NAME.to_string());
        if window_call.window == window_call.slide {
            let valued = Expr::ScalarFunction(call);
            let valued = if in_group {
                nonnull_window(valued)
            } else {
                scalar = true;
                valued
            };
            rewritten.push(valued.alias(name));
            continue;
        }
        if expanded.is_none() {
            expanded = Some(expand_input(input, &window_call)?);
        }
        let valued = expanded_column();
        let valued = if in_group {
            nonnull_window(valued)
        } else {
            valued
        };
        rewritten.push(valued.alias(name));
    }
    Ok(Rewritten {
        touched: seen.is_some(),
        expressions: rewritten,
        expanded,
        scalar,
        time: seen.map(|call| call.time),
    })
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => {
            let rewritten =
                rewrite_expressions(aggregate.group_expr.clone(), &aggregate.input, true)?;
            if !rewritten.touched {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            }
            let input = match rewritten.expanded {
                Some(plan) => Arc::new(plan),
                None => Arc::clone(&aggregate.input),
            };
            let input = match rewritten.time {
                Some(time) => Arc::new(
                    LogicalPlanBuilder::from(input)
                        .filter(Expr::IsNotNull(Box::new(time)))?
                        .build()?,
                ),
                None => input,
            };
            Ok(Transformed::new(
                LogicalPlan::Aggregate(Aggregate::try_new(
                    input,
                    rewritten.expressions,
                    aggregate.aggr_expr.clone(),
                )?),
                true,
                TreeNodeRecursion::Continue,
            ))
        }
        LogicalPlan::Projection(projection) => {
            let rewritten = rewrite_expressions(projection.expr.clone(), &projection.input, false)?;
            let input = match rewritten.expanded {
                Some(plan) => Arc::new(plan),
                None => Arc::clone(&projection.input),
            };
            let input = match (rewritten.scalar, &rewritten.time) {
                (true, Some(time)) => Arc::new(
                    LogicalPlanBuilder::from(input)
                        .filter(Expr::IsNotNull(Box::new(time.clone())))?
                        .build()?,
                ),
                _ => input,
            };
            let expressions: Vec<Expr> = rewritten
                .expressions
                .into_iter()
                .map(|expression| remap_dangling_window_refs(expression, &input))
                .map(|expression| {
                    if rewritten.touched {
                        session_window::rebase_expression(expression, input.schema())
                    } else {
                        expression
                    }
                })
                .collect();
            if !rewritten.touched && expressions == projection.expr {
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            }
            Ok(Transformed::new(
                LogicalPlan::Projection(Projection::try_new(expressions, input)?),
                true,
                TreeNodeRecursion::Continue,
            ))
        }
        other => Ok(Transformed::no(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::session_window::binary;
    use super::*;

    use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, StructArray};
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::expr::Case;
    use datafusion::logical_expr::{Operator, col, lit};
    use datafusion::prelude::{SessionConfig, SessionContext};

    const MINUTE_MICROS: i64 = 60_000_000;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx.add_analyzer_rule(Arc::new(SparkTimeWindow));
        ctx.add_analyzer_rule(Arc::new(SparkWindowTimeGrouping));
        ctx.add_analyzer_rule(Arc::new(SparkSessionWindow));
        ctx
    }

    fn ctx_two_partitions() -> SessionContext {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        crate::register_all(&ctx);
        ctx.add_analyzer_rule(Arc::new(SparkTimeWindow));
        ctx.add_analyzer_rule(Arc::new(SparkWindowTimeGrouping));
        ctx.add_analyzer_rule(Arc::new(SparkSessionWindow));
        ctx
    }

    async fn batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert!(!batches.is_empty(), "expected rows for {sql}");
        datafusion::arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap()
    }

    async fn rule_error(ctx: &SessionContext, sql: &str) -> String {
        let state = ctx.state();
        let plan = state.create_logical_plan(sql).await.unwrap();
        match crate::analyze_eagerly(&state, plan) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected the rule to refuse {sql}"),
        }
    }

    fn frame_sql() -> String {
        "SELECT * FROM (VALUES (1, TIMESTAMP '2024-01-01 10:07:30'), (2, TIMESTAMP '2024-01-01 10:12:00'), \
         (3, TIMESTAMP '2024-01-01 10:31:00')) AS t(id, ts)"
            .to_string()
    }

    fn window_starts(batch: &RecordBatch) -> Vec<Option<i64>> {
        let structs = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap_or_else(|| panic!("expected Struct, got {:?}", batch.schema()));
        let starts = structs.column(0);
        (0..structs.len())
            .map(|row| {
                if structs.is_null(row) {
                    None
                } else {
                    Some(match starts.data_type() {
                        DataType::Timestamp(TimeUnit::Nanosecond, _) => starts
                            .as_primitive::<datafusion::arrow::datatypes::TimestampNanosecondType>()
                            .value(row)
                            / 1_000,
                        other => panic!("expected a timestamp child, got {other}"),
                    })
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn tumbling_group_key_is_named_window() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT window(ts, '10 minutes') AS w, count(*) AS c FROM (SELECT TIMESTAMP '2024-01-01 10:00:00' AS ts) GROUP BY w",
        )
        .await;
        assert_eq!(produced.schema().field(0).name(), "w");
        let bare = batch(
            &ctx,
            "SELECT count(*) AS c FROM (SELECT TIMESTAMP '2024-01-01 10:00:00' AS ts) GROUP BY window(ts, '10 minutes')",
        )
        .await;
        assert_eq!(bare.schema().field(0).name(), "c");
        assert_eq!(bare.num_columns(), 1);
    }

    #[tokio::test]
    async fn sliding_groupby_expands_to_overlapping_buckets() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            &format!(
                "SELECT window(ts, '10 minutes', '5 minutes') AS w, sum(id) AS s FROM ({}) GROUP BY w",
                frame_sql()
            ),
        )
        .await;
        assert_eq!(produced.num_rows(), 5);
        let sums: Vec<i64> = produced
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        let mut sorted = sums.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![1, 2, 3, 3, 3]);
        let base = 1_704_103_200_000_000_i64;
        let mut starts = window_starts(&produced);
        starts.sort_unstable();
        assert_eq!(
            starts,
            vec![
                Some(base),
                Some(base + 5 * MINUTE_MICROS),
                Some(base + 10 * MINUTE_MICROS),
                Some(base + 25 * MINUTE_MICROS),
                Some(base + 30 * MINUTE_MICROS),
            ]
        );
    }

    #[tokio::test]
    async fn sliding_select_expands_rows() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            &format!(
                "SELECT window(ts, '10 minutes', '5 minutes') AS w FROM ({})",
                frame_sql()
            ),
        )
        .await;
        assert_eq!(produced.num_rows(), 6);
    }

    #[tokio::test]
    async fn explicit_alias_survives() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            &format!(
                "SELECT window(ts, '10 minutes') AS bucket, count(*) AS c FROM ({}) GROUP BY bucket",
                frame_sql()
            ),
        )
        .await;
        assert_eq!(produced.schema().field(0).name(), "bucket");
        assert_eq!(produced.num_rows(), 3);
    }

    #[tokio::test]
    async fn native_aggregate_names_bare_group_call_window() {
        use datafusion::logical_expr::expr::ScalarFunction;
        use datafusion::logical_expr::{col, lit};
        let ctx = ctx();
        let frame = ctx
            .sql("SELECT id, ts FROM (VALUES (1, TIMESTAMP '2024-01-01 10:07:30')) AS t(id, ts)")
            .await
            .unwrap();
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(
            crate::spark_time_window::window_udf(),
            vec![col("ts"), lit("10 minutes")],
        ));
        let produced = frame
            .aggregate(vec![call], vec![])
            .unwrap()
            .collect()
            .await
            .unwrap();
        let combined =
            datafusion::arrow::compute::concat_batches(&produced[0].schema(), produced.iter())
                .unwrap();
        assert_eq!(combined.schema().field(0).name(), WINDOW_OUTPUT_NAME);
        assert!(matches!(
            combined.schema().field(0).data_type(),
            DataType::Struct(_)
        ));
        assert_eq!(combined.num_rows(), 1);
    }

    #[tokio::test]
    async fn bad_duration_fails_analysis() {
        let ctx = ctx();
        let message = rule_error(&ctx, "SELECT window(ts, '10 parsecs') AS w FROM (SELECT TIMESTAMP '2024-01-01 10:00:00' AS ts)").await;
        assert!(
            message.contains("[CANNOT_PARSE_INTERVAL]"),
            "expected the condition, got {message}"
        );
    }

    #[tokio::test]
    async fn second_window_specification_refuses() {
        let ctx = ctx();
        let message = rule_error(
            &ctx,
            "SELECT window(ts, '10 minutes') AS a, window(ts, '5 minutes') AS b FROM (SELECT TIMESTAMP '2024-01-01 10:00:00' AS ts)",
        )
        .await;
        assert!(
            message.contains("one window specification"),
            "expected the refusal, got {message}"
        );
    }

    #[tokio::test]
    async fn window_time_in_aggregate_reports_missing_aggregation() {
        use datafusion::logical_expr::expr::ScalarFunction;
        use datafusion::logical_expr::{col, lit};
        let ctx = ctx();
        let frame = ctx
            .sql("SELECT window(TIMESTAMP '2024-01-01 10:00:00', '10 minutes') AS w")
            .await
            .unwrap();
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(
            crate::spark_window_time::window_time_udf(),
            vec![col("w")],
        ));
        let (state, plan) = frame.into_parts();
        let grouped = datafusion::logical_expr::LogicalPlanBuilder::from(plan)
            .aggregate(vec![col("w")], vec![call, lit(1)])
            .unwrap()
            .build()
            .unwrap();
        let message = match crate::analyze_eagerly(&state, grouped) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected the grouping refusal"),
        };
        assert!(
            message.contains("[MISSING_AGGREGATION]"),
            "expected the condition, got {message}"
        );
        assert!(
            message.contains("window_time(w)"),
            "expected the quoted call, got {message}"
        );
    }

    #[tokio::test]
    async fn window_time_in_projection_answers() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT window_time(window(TIMESTAMP '2024-01-01 10:00:00', '10 minutes')) AS wt",
        )
        .await;
        assert!(matches!(
            produced.schema().field(0).data_type(),
            DataType::Timestamp(_, _)
        ));
        assert_eq!(produced.num_rows(), 1);
    }

    fn session_frame_sql() -> String {
        "SELECT * FROM (VALUES (1, TIMESTAMP '2024-01-01 10:07:30', 'k1'), (2, TIMESTAMP '2024-01-01 10:12:00', 'k1'), \
         (3, TIMESTAMP '2024-01-01 10:31:00', 'k2')) AS t(id, ts, key)"
            .to_string()
    }

    fn session_bounds(batch: &RecordBatch, column: usize) -> Vec<(Option<i64>, Option<i64>)> {
        let structs = batch
            .column(column)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap_or_else(|| panic!("expected Struct, got {:?}", batch.schema()));
        let (starts, ends) = (structs.column(0), structs.column(1));
        (0..structs.len())
            .map(|row| {
                if structs.is_null(row) {
                    (None, None)
                } else {
                    (
                        Some(timestamp_nanos_to_micros(starts, row)),
                        Some(timestamp_nanos_to_micros(ends, row)),
                    )
                }
            })
            .collect()
    }

    fn timestamp_nanos_to_micros(array: &ArrayRef, row: usize) -> i64 {
        match array.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                array
                    .as_primitive::<datafusion::arrow::datatypes::TimestampNanosecondType>()
                    .value(row)
                    / 1_000
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => array
                .as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>()
                .value(row),
            other => panic!("expected a timestamp child, got {other}"),
        }
    }

    fn session_call(time: Expr, gap: Expr) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            crate::spark_session_window::session_window_udf(),
            vec![time, gap],
        ))
    }

    async fn sessioned(ctx: &SessionContext, groups: Vec<Expr>, gap: Expr) -> RecordBatch {
        let frame = ctx.sql(&session_frame_sql()).await.unwrap();
        let call = session_call(col("ts"), gap);
        let mut grouping = groups;
        grouping.push(call);
        let batches = frame
            .aggregate(grouping, Vec::<Expr>::new())
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(!batches.is_empty(), "expected session rows");
        datafusion::arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap()
    }

    #[tokio::test]
    async fn static_gap_sessions_match_spark() {
        use datafusion::logical_expr::{col, lit};
        let ctx = ctx();
        let produced = sessioned(&ctx, vec![col("key")], lit("5 minutes")).await;
        assert_eq!(produced.schema().field(1).name(), "session_window");
        let mut rows: Vec<(String, Option<i64>, Option<i64>)> = (0..produced.num_rows())
            .map(|row| {
                let keys = produced
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .unwrap();
                let bounds = session_bounds(&produced, 1);
                (keys.value(row).to_string(), bounds[row].0, bounds[row].1)
            })
            .collect();
        rows.sort();
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            rows,
            vec![
                (
                    "k1".to_string(),
                    Some(base + 450_000_000),
                    Some(base + 1_020_000_000)
                ),
                (
                    "k2".to_string(),
                    Some(base + 1_860_000_000),
                    Some(base + 2_160_000_000)
                ),
            ]
        );
    }

    #[tokio::test]
    async fn wide_gap_merges_all_rows() {
        use datafusion::logical_expr::lit;
        let ctx = ctx();
        let produced = sessioned(&ctx, Vec::new(), lit("30 minutes")).await;
        assert_eq!(produced.num_rows(), 1);
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            session_bounds(&produced, 0),
            vec![(Some(base + 450_000_000), Some(base + 3_660_000_000))]
        );
    }

    #[tokio::test]
    async fn dynamic_gap_uses_previous_row_gap() {
        let ctx = ctx();
        let gap = Expr::Case(Case::new(
            None,
            vec![(
                Box::new(binary(col("id"), Operator::Eq, lit(1))),
                Box::new(lit("20 minutes")),
            )],
            Some(Box::new(lit("1 minute"))),
        ));
        let produced = sessioned(&ctx, Vec::new(), gap).await;
        let mut bounds = session_bounds(&produced, 0);
        bounds.sort();
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            bounds,
            vec![
                (Some(base + 450_000_000), Some(base + 1_650_000_000)),
                (Some(base + 1_860_000_000), Some(base + 1_920_000_000)),
            ]
        );
    }

    #[tokio::test]
    async fn dynamic_gap_chains_on_the_running_end_across_batches() {
        use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::memory::MemTable;
        use datafusion::functions_aggregate::count::count_udaf;
        use datafusion::logical_expr::expr::AggregateFunction;
        let ctx = ctx();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("gap", DataType::Utf8, false),
        ]));
        let base = 1_704_103_200_000_000_000_i64;
        let batch_of = |rows: Vec<(i64, i64, &str)>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(
                        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                    )) as ArrayRef,
                    Arc::new(TimestampNanosecondArray::from(
                        rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                    )) as ArrayRef,
                    Arc::new(StringArray::from(
                        rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                    )) as ArrayRef,
                ],
            )
            .unwrap()
        };
        let first = batch_of(vec![(1, base, "60 minutes")]);
        let second = batch_of(vec![
            (2, base + 600_000_000_000, "1 minute"),
            (3, base + 1_200_000_000_000, "5 minutes"),
        ]);
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![first, second]]).unwrap();
        ctx.register_table("chained", Arc::new(table)).unwrap();
        let frame = ctx.table("chained").await.unwrap();
        let counted = Expr::AggregateFunction(AggregateFunction::new_udf(
            count_udaf(),
            vec![lit(1)],
            false,
            None,
            Vec::new(),
            None,
        ))
        .alias("c");
        let batches = frame
            .aggregate(vec![session_call(col("ts"), col("gap"))], vec![counted])
            .unwrap()
            .collect()
            .await
            .unwrap();
        let produced =
            datafusion::arrow::compute::concat_batches(&batches[0].schema(), batches.iter())
                .unwrap();
        assert_eq!(produced.num_rows(), 1);
        assert_eq!(
            session_bounds(&produced, 0),
            vec![(Some(base / 1_000), Some(base / 1_000 + 3_600_000_000))]
        );
        let counts = produced
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 3);
    }

    #[tokio::test]
    async fn static_gap_sessions_match_on_two_partitions() {
        use datafusion::logical_expr::{col, lit};
        let ctx = ctx_two_partitions();
        let produced = sessioned(&ctx, vec![col("key")], lit("5 minutes")).await;
        assert_eq!(produced.num_rows(), 2);
        let mut bounds = session_bounds(&produced, 1);
        bounds.sort();
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            bounds,
            vec![
                (Some(base + 450_000_000), Some(base + 1_020_000_000)),
                (Some(base + 1_860_000_000), Some(base + 2_160_000_000)),
            ]
        );
    }

    #[tokio::test]
    async fn null_time_rows_leave_no_session() {
        use datafusion::logical_expr::{col, lit};
        let ctx = ctx();
        let frame = ctx
            .sql(
                "SELECT * FROM (VALUES (1, CAST(NULL AS TIMESTAMP), 'k1'), (2, TIMESTAMP '2024-01-01 10:07:30', 'k2')) AS t(id, ts, key)",
            )
            .await
            .unwrap();
        let batches = frame
            .aggregate(
                vec![col("key"), session_call(col("ts"), lit("5 minutes"))],
                Vec::<Expr>::new(),
            )
            .unwrap()
            .collect()
            .await
            .unwrap();
        let produced =
            datafusion::arrow::compute::concat_batches(&batches[0].schema(), batches.iter())
                .unwrap();
        assert_eq!(produced.num_rows(), 1);
        let keys = produced
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(keys.value(0), "k2");
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            session_bounds(&produced, 1),
            vec![(Some(base + 450_000_000), Some(base + 750_000_000))]
        );
    }

    #[tokio::test]
    async fn staged_marker_shape_sessionizes() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            &format!(
                "SELECT key, session_window, count(*) AS c FROM (SELECT *, session_window(ts, '5 minutes') AS session_window FROM ({})) GROUP BY key, session_window",
                session_frame_sql()
            ),
        )
        .await;
        assert_eq!(produced.num_rows(), 2);
        let mut rows: Vec<(String, Option<i64>, Option<i64>, i64)> = (0..produced.num_rows())
            .map(|row| {
                let keys = produced
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .unwrap();
                let bounds = session_bounds(&produced, 1);
                let counts = produced
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (
                    keys.value(row).to_string(),
                    bounds[row].0,
                    bounds[row].1,
                    counts.value(row),
                )
            })
            .collect();
        rows.sort();
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            rows,
            vec![
                (
                    "k1".to_string(),
                    Some(base + 450_000_000),
                    Some(base + 1_020_000_000),
                    2
                ),
                (
                    "k2".to_string(),
                    Some(base + 1_860_000_000),
                    Some(base + 2_160_000_000),
                    1
                ),
            ]
        );
    }

    #[tokio::test]
    async fn second_gap_specification_refuses() {
        let ctx = ctx();
        let message = rule_error(
            &ctx,
            "SELECT count(*) AS c FROM (SELECT TIMESTAMP '2024-01-01 10:00:00' AS ts) GROUP BY session_window(ts, '5 minutes'), session_window(ts, '10 minutes')",
        )
        .await;
        assert!(
            message.contains("one gap specification"),
            "expected the refusal, got {message}"
        );
    }

    #[tokio::test]
    async fn qualified_session_columns_survive_a_join() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT a.session_window AS w FROM (SELECT 1 AS id, 'x' AS session_window) AS a \
             JOIN (SELECT 1 AS id, 'y' AS session_window) AS b ON a.id = b.id",
        )
        .await;
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn month_gap_session_refuses() {
        let ctx = ctx();
        let message = rule_error(
            &ctx,
            "SELECT count(*) AS c FROM (SELECT TIMESTAMP '2024-01-01 10:00:00' AS ts) GROUP BY session_window(ts, '1 month')",
        )
        .await;
        assert!(
            message.contains("months or years"),
            "expected the refusal, got {message}"
        );
    }

    #[tokio::test]
    async fn sliding_groupby_is_correct_on_two_partitions() {
        let ctx = ctx_two_partitions();
        let produced = batch(
            &ctx,
            &format!(
                "SELECT window(ts, '10 minutes', '5 minutes') AS w, sum(id) AS s FROM ({}) GROUP BY w",
                frame_sql()
            ),
        )
        .await;
        assert_eq!(produced.num_rows(), 5);
        let sums: Vec<i64> = produced
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        let mut sorted = sums.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![1, 2, 3, 3, 3]);
    }

    #[tokio::test]
    async fn tumbling_projection_drops_null_time() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT window(ts, '10 minutes') AS w, id FROM (VALUES (1, TIMESTAMP '2024-01-01 10:00:00'), \
             (2, CAST(NULL AS TIMESTAMP))) AS t(id, ts)",
        )
        .await;
        assert_eq!(produced.num_rows(), 1);
    }

    #[tokio::test]
    async fn sliding_projection_drops_null_time() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT window(ts, '10 minutes', '5 minutes') AS w, id FROM (VALUES (1, TIMESTAMP '2024-01-01 10:00:00'), \
             (2, CAST(NULL AS TIMESTAMP))) AS t(id, ts)",
        )
        .await;
        assert_eq!(produced.num_rows(), 2);
    }

    #[tokio::test]
    async fn slide_above_window_refuses() {
        let ctx = ctx();
        let message = rule_error(
            &ctx,
            "SELECT window(ts, '5 minutes', '10 minutes') AS w FROM (SELECT TIMESTAMP '2024-01-01 10:00:00' AS ts)",
        )
        .await;
        assert!(
            message.contains("PARAMETER_CONSTRAINT_VIOLATION"),
            "expected the condition, got {message}"
        );
        assert!(
            message.contains("must be <= the `window_duration`(300000000L)"),
            "expected the rendering, got {message}"
        );
    }

    #[tokio::test]
    async fn negative_start_shifts_the_grid() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            &format!(
                "SELECT window(ts, '10 minutes', '5 minutes', '-2 minutes') AS w, count(*) AS c FROM ({}) GROUP BY w",
                frame_sql()
            ),
        )
        .await;
        assert_eq!(produced.num_rows(), 5);
        let mut starts = window_starts(&produced);
        starts.sort_unstable();
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            starts,
            vec![
                Some(base - 120_000_000),
                Some(base + 180_000_000),
                Some(base + 480_000_000),
                Some(base + 1_380_000_000),
                Some(base + 1_680_000_000),
            ]
        );
    }

    #[tokio::test]
    async fn zero_start_answers_the_default_grid() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            &format!(
                "SELECT window(ts, '10 minutes', '10 minutes', '0 seconds') AS w, count(*) AS c FROM ({}) GROUP BY w",
                frame_sql()
            ),
        )
        .await;
        assert_eq!(produced.num_rows(), 3);
    }

    #[tokio::test]
    async fn negative_start_abs_ge_slide_refuses() {
        let ctx = ctx();
        let message = rule_error(
            &ctx,
            "SELECT window(ts, '10 minutes', '5 minutes', '-5 minutes') AS w FROM (SELECT TIMESTAMP '2024-01-01 10:00:00' AS ts)",
        )
        .await;
        assert!(
            message.contains("PARAMETER_CONSTRAINT_VIOLATION"),
            "expected the condition, got {message}"
        );
        assert!(
            message.contains("must be < the `slide_duration`(300000000L)"),
            "expected the rendering, got {message}"
        );
    }

    #[tokio::test]
    async fn window_time_plain_struct_refuses() {
        let ctx = ctx();
        let message = rule_error(
            &ctx,
            "SELECT window_time(named_struct('start', TIMESTAMP '2024-01-01 10:00:00', 'end', TIMESTAMP '2024-01-01 10:10:00')) AS wt",
        )
        .await;
        assert!(
            message.contains("[_LEGACY_ERROR_TEMP_3101]"),
            "expected the condition, got {message}"
        );
        assert!(
            message.contains("The input is not a correct window column:"),
            "expected the reason, got {message}"
        );
    }

    #[tokio::test]
    async fn window_time_struct_behind_unary_nodes_refuses() {
        let ctx = ctx();
        let shaped = [
            "SELECT window_time(w) AS wt FROM (SELECT named_struct('start', TIMESTAMP '2024-01-01 10:00:00', 'end', TIMESTAMP '2024-01-01 10:10:00') AS w) AS t WHERE true",
            "SELECT window_time(w) AS wt FROM (SELECT named_struct('start', TIMESTAMP '2024-01-01 10:00:00', 'end', TIMESTAMP '2024-01-01 10:10:00') AS w) AS t LIMIT 10",
            "SELECT window_time(w) AS wt FROM (SELECT named_struct('start', TIMESTAMP '2024-01-01 10:00:00', 'end', TIMESTAMP '2024-01-01 10:10:00') AS w) AS t ORDER BY w",
            "SELECT DISTINCT window_time(w) AS wt FROM (SELECT named_struct('start', TIMESTAMP '2024-01-01 10:00:00', 'end', TIMESTAMP '2024-01-01 10:10:00') AS w) AS t",
        ];
        for sql in shaped {
            let message = rule_error(&ctx, sql).await;
            assert!(
                message.contains("[_LEGACY_ERROR_TEMP_3101]"),
                "expected the condition for {sql}, got {message}"
            );
        }
        let unioned = rule_error(
            &ctx,
            "SELECT window_time(w) AS wt FROM (SELECT named_struct('start', TIMESTAMP '2024-01-01 10:00:00', 'end', TIMESTAMP '2024-01-01 10:10:00') AS w UNION ALL SELECT named_struct('start', TIMESTAMP '2024-01-01 10:00:00', 'end', TIMESTAMP '2024-01-01 10:10:00') AS w) AS t",
        )
        .await;
        assert!(
            unioned.contains("[_LEGACY_ERROR_TEMP_3101]"),
            "expected the condition for the union, got {unioned}"
        );
        let joined = rule_error(
            &ctx,
            "SELECT window_time(w) AS wt FROM (SELECT named_struct('start', TIMESTAMP '2024-01-01 10:00:00', 'end', TIMESTAMP '2024-01-01 10:10:00') AS w) AS t CROSS JOIN (SELECT 1 AS id) AS u",
        )
        .await;
        assert!(
            joined.contains("[_LEGACY_ERROR_TEMP_3101]"),
            "expected the condition for the join, got {joined}"
        );
    }

    #[tokio::test]
    async fn window_time_grouped_window_behind_filter_answers() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT window_time(window) AS wt FROM (SELECT window(ts, '10 minutes') AS window, count(*) AS c FROM (SELECT * FROM (VALUES (1, TIMESTAMP '2024-01-01 10:07:30'), (2, TIMESTAMP '2024-01-01 10:12:00')) AS t(id, ts)) GROUP BY window(ts, '10 minutes')) AS u WHERE c > 0",
        )
        .await;
        assert_eq!(produced.num_rows(), 2);
    }

    #[tokio::test]
    async fn exact_gap_session_merges() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT key, session_window(ts, '5 minutes') AS w, count(*) AS c FROM (SELECT * FROM (VALUES (1, TIMESTAMP '2024-01-01 10:00:00', 'k'), \
             (2, TIMESTAMP '2024-01-01 10:05:00', 'k')) AS t(id, ts, key)) GROUP BY key, w",
        )
        .await;
        assert_eq!(produced.num_rows(), 1);
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            session_bounds(&produced, 1),
            vec![(Some(base), Some(base + 600_000_000))]
        );
    }

    #[tokio::test]
    async fn dynamic_zero_gap_drops_the_row() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT session_window(ts, CASE WHEN id = 1 THEN '0 seconds' ELSE '30 minutes' END) AS w FROM (SELECT * FROM (VALUES (1, TIMESTAMP '2024-01-01 10:07:30'), \
             (2, TIMESTAMP '2024-01-01 10:12:00'), (3, TIMESTAMP '2024-01-01 10:31:00')) AS t(id, ts)) \
             GROUP BY w",
        )
        .await;
        assert_eq!(produced.num_rows(), 1);
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            session_bounds(&produced, 0),
            vec![(Some(base + 720_000_000), Some(base + 3_660_000_000))]
        );
    }

    #[tokio::test]
    async fn dynamic_null_gap_drops_the_row() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT session_window(ts, CASE WHEN id = 2 THEN NULL ELSE '30 minutes' END) AS w FROM (SELECT * FROM (VALUES (1, TIMESTAMP '2024-01-01 10:07:30'), \
             (2, TIMESTAMP '2024-01-01 10:12:00'), (3, TIMESTAMP '2024-01-01 10:31:00')) AS t(id, ts)) \
             GROUP BY w",
        )
        .await;
        assert_eq!(produced.num_rows(), 1);
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            session_bounds(&produced, 0),
            vec![(Some(base + 450_000_000), Some(base + 3_660_000_000))]
        );
    }

    #[tokio::test]
    async fn dynamic_month_gap_answers_a_calendar_session() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT session_window(ts, CASE WHEN id = 1 THEN '1 month' ELSE '30 minutes' END) AS w FROM (SELECT * FROM (VALUES (1, TIMESTAMP '2024-01-01 10:07:30'), \
             (2, TIMESTAMP '2024-01-01 10:12:00'), (3, TIMESTAMP '2024-01-01 10:31:00')) AS t(id, ts)) \
             GROUP BY w",
        )
        .await;
        assert_eq!(produced.num_rows(), 1);
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            session_bounds(&produced, 0),
            vec![(Some(base + 450_000_000), Some(1_706_782_050_000_000))]
        );
    }

    #[tokio::test]
    async fn dynamic_gap_matches_on_two_partitions() {
        let ctx = ctx_two_partitions();
        let produced = batch(
            &ctx,
            "SELECT key, session_window(ts, CASE WHEN id = 1 THEN '20 minutes' ELSE '1 minute' END) AS w FROM \
             (SELECT * FROM (VALUES (1, TIMESTAMP '2024-01-01 10:07:30', 'k1'), \
             (2, TIMESTAMP '2024-01-01 10:12:00', 'k1'), (3, TIMESTAMP '2024-01-01 10:31:00', 'k1')) AS t(id, ts, key)) \
             GROUP BY key, w",
        )
        .await;
        assert_eq!(produced.num_rows(), 2);
        let mut bounds = session_bounds(&produced, 1);
        bounds.sort();
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            bounds,
            vec![
                (Some(base + 450_000_000), Some(base + 1_650_000_000)),
                (Some(base + 1_860_000_000), Some(base + 1_920_000_000)),
            ]
        );
    }

    #[tokio::test]
    async fn date_session_window_answers_day_sessions() {
        let ctx = ctx();
        let produced = batch(
            &ctx,
            "SELECT session_window(ts, '1 day') AS w FROM (SELECT * FROM (VALUES (1, DATE '2024-01-01'), \
             (2, DATE '2024-01-02'), (3, DATE '2024-01-05')) AS t(id, ts)) \
             GROUP BY w",
        )
        .await;
        assert_eq!(produced.num_rows(), 2);
    }
}
