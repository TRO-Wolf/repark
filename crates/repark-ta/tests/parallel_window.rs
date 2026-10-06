#![cfg(feature = "datafusion")]

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;
use repark_core::ReparkSession;
use repark_core::frame_names::{AttrId, stamp, strip_for_execution};
use repark_ta::TaExtension;

const ROWS: i64 = 50_000;
const BATCH_ROWS: i64 = 6_400;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
    ]))
}

fn price(row: i64) -> (f64, f64, f64) {
    let x = f64::from(u32::try_from(row).unwrap_or_default());
    let close = 4_000.0 + 40.0 * (x * 0.0021).sin() + 7.0 * (x * 0.37).cos() + x * 0.0003;
    let high = close + 0.25 + 1.5 * (x * 0.53).sin().abs();
    let low = close - 0.25 - 1.25 * (x * 0.71).cos().abs();
    (high, low, close)
}

fn batches() -> Vec<RecordBatch> {
    let mut out = Vec::new();
    let mut start = 0;
    while start < ROWS {
        let end = (start + BATCH_ROWS).min(ROWS);
        let rows: Vec<(f64, f64, f64)> = (start..end).map(price).collect();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from((start..end).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ];
        out.push(RecordBatch::try_new(schema(), columns).expect("batch"));
        start = end;
    }
    out
}

fn session(parallel: bool) -> SessionContext {
    let session = ReparkSession::builder()
        .target_partitions(16)
        .with_extension(Arc::new(TaExtension))
        .parallel_single_partition(parallel)
        .build()
        .expect("session");
    let context = session.context().clone();
    let table = MemTable::try_new(schema(), vec![batches()]).expect("table");
    context
        .register_table("bars", Arc::new(table))
        .expect("register");
    context
}

async fn run(context: &SessionContext, query: &str) -> (String, RecordBatch) {
    let frame = context.sql(query).await.expect("sql");
    let plan = frame.create_physical_plan().await.expect("plan");
    let text = displayable(plan.as_ref()).indent(true).to_string();
    let schema = plan.schema();
    let batches = collect(plan, context.task_ctx()).await.expect("collect");
    (text, concat_batches(&schema, &batches).expect("concat"))
}

const L3: &str = "WITH lagged AS (SELECT ts, high, low, close, \
    ta_trange(high, low, close) OVER (ORDER BY ts) AS tr FROM bars) \
    SELECT ts, close, tr, \
    ta_ema(close, 5) OVER (ORDER BY ts) AS ema5, \
    ta_rsi(close, 13) OVER (ORDER BY ts) AS rsi13, \
    ta_rsi(close, 21) OVER (ORDER BY ts) AS rsi21, \
    ta_rsi(close, 34) OVER (ORDER BY ts) AS rsi34, \
    ta_sma(close, 10) OVER (ORDER BY ts) AS sma10, \
    ta_sma(close, 20) OVER (ORDER BY ts) AS sma20, \
    ta_sma(close, 34) OVER (ORDER BY ts) AS sma34, \
    ta_ema(tr, 5) OVER (ORDER BY ts) AS etr5, \
    ta_ema(tr, 13) OVER (ORDER BY ts) AS etr13, \
    ta_ema(tr, 21) OVER (ORDER BY ts) AS etr21, \
    ta_linearreg(close, 5) OVER (ORDER BY ts) AS lr5, \
    ta_adx(high, low, close, 5) OVER (ORDER BY ts) AS adx5, \
    ta_adx(high, low, close, 13) OVER (ORDER BY ts) AS adx13, \
    ta_adx(high, low, close, 21) OVER (ORDER BY ts) AS adx21 \
    FROM lagged";

const SIBLINGS: &str = "SELECT ts, \
    ta_bbands_upper(close, 20, 2, 2) OVER (ORDER BY ts) AS upper, \
    ta_macd(close, 12, 26, 9) OVER (ORDER BY ts) AS macd, \
    ta_bbands_middle(close, 20, 2, 2) OVER (ORDER BY ts) AS middle, \
    ta_macd_signal(close, 12, 26, 9) OVER (ORDER BY ts) AS signal, \
    ta_bbands_lower(close, 20, 2, 2) OVER (ORDER BY ts) AS lower, \
    ta_stoch_slowk(high, low, close, 5, 3, 0, 3, 0) OVER (ORDER BY ts) AS slowk, \
    ta_stoch_slowd(high, low, close, 5, 3, 0, 3, 0) OVER (ORDER BY ts) AS slowd \
    FROM bars";

fn window_expressions(plan: &str, node: &str) -> usize {
    plan.lines()
        .find(|line| line.trim_start().starts_with(node))
        .map_or(0, |line| line.matches(", frame: ").count())
}

fn assert_bit_identical(got: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(got.schema(), expected.schema(), "column names and order");
    assert_eq!(got.num_rows(), expected.num_rows());
    for (index, (left, right)) in got.columns().iter().zip(expected.columns()).enumerate() {
        let name = got.schema().field(index).name().clone();
        assert_eq!(
            left.to_data().nulls(),
            right.to_data().nulls(),
            "{name} validity"
        );
        match (
            left.as_any().downcast_ref::<Float64Array>(),
            right.as_any().downcast_ref::<Float64Array>(),
        ) {
            (Some(left), Some(right)) => {
                for (row, (a, b)) in left.values().iter().zip(right.values().iter()).enumerate() {
                    assert_eq!(a.to_bits(), b.to_bits(), "{name} row {row}: {a:?} vs {b:?}");
                }
            }
            _ => assert_eq!(left, right, "{name}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_window_bit_identical() {
    let parallel = session(true);
    let serial = session(false);
    let (plan, got) = run(&parallel, L3).await;
    let (serial_plan, expected) = run(&serial, L3).await;
    assert_eq!(
        window_expressions(&plan, "ParallelWindowExec"),
        14,
        "{plan}"
    );
    assert_eq!(
        window_expressions(&serial_plan, "WindowAggExec"),
        14,
        "{serial_plan}"
    );
    assert!(!serial_plan.contains("ParallelWindowExec"), "{serial_plan}");
    assert!(!plan.contains("BoundedWindowAggExec"), "{plan}");
    assert_eq!(got.num_rows(), 50_000);
    assert_eq!(got.num_columns(), 17);
    assert_bit_identical(&got, &expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_window_multi_output_siblings_bit_identical() {
    let (plan, got) = run(&session(true), SIBLINGS).await;
    let (_, expected) = run(&session(false), SIBLINGS).await;
    assert_eq!(window_expressions(&plan, "ParallelWindowExec"), 7, "{plan}");
    assert_bit_identical(&got, &expected);
}

const STAMPED_LEVEL: &str = "SELECT ts, \
    ta_sma(close, 10) OVER (ORDER BY ts) AS sma10, \
    ta_sma(close, 20) OVER (ORDER BY ts) AS sma20, \
    ta_ema(close, 5) OVER (ORDER BY ts) AS ema5 \
    FROM bars";

fn plan_carries_id(plan: &LogicalPlan) -> bool {
    plan.schema()
        .fields()
        .iter()
        .any(|field| AttrId::of(field).is_some())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stamped_frame_parallel_window_matches_serial_answer() {
    let parallel = session(true);
    let frame = parallel.sql(STAMPED_LEVEL).await.expect("sql");
    let (state, plan) = frame.into_parts();
    let stamped = stamp(plan).expect("stamp");
    assert!(plan_carries_id(&stamped));
    let stripped = strip_for_execution(stamped).expect("strip");
    assert!(!plan_carries_id(&stripped));
    let twin = DataFrame::new(state, stripped);
    let physical = twin.create_physical_plan().await.expect("plan");
    let text = displayable(physical.as_ref()).indent(true).to_string();
    assert_eq!(window_expressions(&text, "ParallelWindowExec"), 3, "{text}");
    let schema = physical.schema();
    let batches = collect(physical, parallel.task_ctx())
        .await
        .expect("collect");
    let got = concat_batches(&schema, &batches).expect("concat");
    let (_, expected) = run(&session(false), STAMPED_LEVEL).await;
    assert_bit_identical(&got, &expected);
}

fn physical_leaks(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<String>) {
    for field in plan.schema().fields() {
        if field
            .metadata()
            .keys()
            .any(|key| key.starts_with("repark.attr"))
            || AttrId::of(field).is_some()
        {
            out.push(format!("{} :: {}", plan.name(), field.name()));
        }
    }
    for child in plan.children() {
        physical_leaks(child, out);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stamped_plan_through_optimizer_reaches_executors_stripped() {
    let parallel = session(true);
    let first = "SELECT ts, close * 2.0 AS c2, close + 1.0 AS c3, ta_sma(close, 10) OVER (ORDER BY ts) AS s10, ta_ema(close, 5) OVER (ORDER BY ts) AS e5 FROM bars";
    let frame = parallel.sql(first).await.expect("sql");
    let (state, plan) = frame.into_parts();
    let stamped = stamp(plan).expect("stamp");
    assert!(plan_carries_id(&stamped));
    let optimized = DataFrame::new(state.clone(), stamped.clone())
        .into_optimized_plan()
        .expect("optimize");
    assert!(!plan_carries_id(&optimized));
    let physical = DataFrame::new(state, stamped)
        .create_physical_plan()
        .await
        .expect("plan");
    let text = displayable(physical.as_ref()).indent(true).to_string();
    let mut found = Vec::new();
    physical_leaks(&physical, &mut found);
    assert!(found.is_empty(), "ids reach executors: {found:?}\n{text}");
    assert!(text.contains("ParallelWindowExec"), "{text}");
    let second = "SELECT ts, s10 * 2.0 AS a, s10 + e5 AS b FROM (SELECT ts, ta_sma(close, 10) OVER (ORDER BY ts) AS s10, ta_ema(close, 5) OVER (ORDER BY ts) AS e5 FROM bars)";
    let frame = parallel.sql(second).await.expect("sql");
    let (state, plan) = frame.into_parts();
    let stamped = stamp(plan).expect("stamp");
    assert!(plan_carries_id(&stamped));
    let optimized = DataFrame::new(state.clone(), stamped.clone())
        .into_optimized_plan()
        .expect("optimize");
    assert!(!plan_carries_id(&optimized));
    let physical = DataFrame::new(state, stamped)
        .create_physical_plan()
        .await
        .expect("plan");
    let text = displayable(physical.as_ref()).indent(true).to_string();
    let mut found = Vec::new();
    physical_leaks(&physical, &mut found);
    assert!(found.is_empty(), "ids reach executors: {found:?}\n{text}");
    let batches = collect(physical, parallel.task_ctx())
        .await
        .expect("collect");
    for batch in &batches {
        assert!(
            batch
                .schema()
                .fields()
                .iter()
                .all(|f| AttrId::of(f).is_none()),
            "batch schema carries ids"
        );
    }
    assert!(text.contains("ParallelProjectionExec"), "{text}");
}
