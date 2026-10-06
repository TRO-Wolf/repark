#![cfg(feature = "datafusion")]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::{Expr, ExprFunctionExt, WindowFunctionDefinition, col, lit, when};
use datafusion::prelude::{DataFrame, SessionContext};
use repark_core::ReparkSession;
use repark_ta::TaExtension;
use repark_ta::udf::{is_ta_window, window_udf, window_udf_with_null_prefix, window_udfs};

const ROWS_PER_SYMBOL: i64 = 600;
const LOOKBACK: usize = 12;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
    ]))
}

fn bars() -> RecordBatch {
    let mut sym = Vec::new();
    let mut ts = Vec::new();
    let mut high = Vec::new();
    let mut low = Vec::new();
    let mut close = Vec::new();
    for row in 0..ROWS_PER_SYMBOL * 2 {
        let x = f64::from(u32::try_from(row).unwrap_or_default());
        let level = if row % 2 == 0 { 4_000.0 } else { 120.0 };
        let price = level + 9.0 * (x * 0.013).sin() + 2.0 * (x * 0.41).cos();
        sym.push(if row % 2 == 0 { "ES" } else { "CL" });
        ts.push(row);
        high.push(price + 0.5 + (x * 0.7).sin().abs());
        low.push(price - 0.5 - (x * 0.3).cos().abs());
        close.push(price);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(sym)),
        Arc::new(Int64Array::from(ts)),
        Arc::new(Float64Array::from(high)),
        Arc::new(Float64Array::from(low)),
        Arc::new(Float64Array::from(close)),
    ];
    RecordBatch::try_new(schema(), columns).expect("bars")
}

fn session() -> SessionContext {
    let session = ReparkSession::builder()
        .target_partitions(4)
        .with_extension(Arc::new(TaExtension))
        .build()
        .expect("session");
    let context = session.context().clone();
    let table = MemTable::try_new(schema(), vec![vec![bars()]]).expect("table");
    context
        .register_table("bars", Arc::new(table))
        .expect("register");
    context
}

async fn true_range(context: &SessionContext) -> DataFrame {
    context
        .sql(
            "SELECT sym, ts, close, \
             ta_trange(high, low, close) OVER (PARTITION BY sym ORDER BY ts) AS tr FROM bars",
        )
        .await
        .expect("true range")
}

fn ta(name: &str, null_prefix: usize, args: Vec<Expr>) -> Expr {
    let udf = window_udf_with_null_prefix(name, null_prefix).expect("a TA window function");
    Expr::from(WindowFunction::new(
        WindowFunctionDefinition::WindowUDF(udf),
        args,
    ))
}

fn over(expr: Expr, partition_by: Vec<Expr>) -> Expr {
    expr.partition_by(partition_by)
        .order_by(vec![col("ts").sort(true, true)])
        .build()
        .expect("window")
}

async fn collect(frame: DataFrame) -> RecordBatch {
    let frame = frame
        .sort(vec![
            col("sym").sort(true, true),
            col("ts").sort(true, true),
        ])
        .expect("sort");
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let batches = frame.collect().await.expect("collect");
    concat_batches(&schema, &batches).expect("concat")
}

fn floats<'a>(batch: &'a RecordBatch, name: &str) -> &'a Float64Array {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
        .expect("a Float64 column")
}

fn symbols(batch: &RecordBatch) -> Vec<String> {
    let column = batch
        .column_by_name("sym")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .expect("sym");
    (0..column.len())
        .map(|row| column.value(row).to_owned())
        .collect()
}

fn assert_same_cells(got: &Float64Array, expected: &Float64Array, label: &str) {
    assert_eq!(got.len(), expected.len(), "{label} length");
    assert_eq!(got.nulls(), expected.nulls(), "{label} validity");
    for row in 0..got.len() {
        if got.is_valid(row) {
            assert_eq!(
                got.value(row).to_bits(),
                expected.value(row).to_bits(),
                "{label} row {row}"
            );
        }
    }
}

fn shape_per_symbol(batch: &RecordBatch, name: &str) -> Vec<(String, usize, usize)> {
    let values = floats(batch, name);
    let symbols = symbols(batch);
    let mut shapes: Vec<(String, usize, usize)> = Vec::new();
    for (row, symbol) in symbols.iter().enumerate() {
        if shapes.last().is_none_or(|(last, _, _)| last != symbol) {
            shapes.push((symbol.clone(), 0, 0));
        }
        let entry = shapes.last_mut().expect("an entry");
        if values.is_null(row) {
            entry.1 += 1;
        } else if values.value(row).is_nan() {
            entry.2 += 1;
        }
    }
    shapes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn null_lookback_native_matches_row_number_rewrite() {
    let context = session();
    let frame = true_range(&context).await;
    let by_symbol = vec![col("sym")];
    let native = over(
        ta("ta_ema", LOOKBACK, vec![col("tr"), lit(13_i64)]),
        by_symbol.clone(),
    );
    let rewrite = when(
        over(row_number(), by_symbol.clone()).gt(lit(12_i64)),
        over(ta("ta_ema", 0, vec![col("tr"), lit(13_i64)]), by_symbol),
    )
    .end()
    .expect("case");
    let batch = collect(
        frame
            .select(vec![
                col("sym"),
                col("ts"),
                native.alias("native"),
                rewrite.alias("rewrite"),
            ])
            .expect("select"),
    )
    .await;
    assert_same_cells(
        floats(&batch, "native"),
        floats(&batch, "rewrite"),
        "partitioned",
    );
    assert_eq!(
        shape_per_symbol(&batch, "native"),
        vec![("CL".to_owned(), 12, 1), ("ES".to_owned(), 12, 1)]
    );

    let frame = true_range(&context).await;
    let whole = over(
        ta("ta_ema", LOOKBACK, vec![col("close"), lit(13_i64)]),
        vec![],
    );
    let whole_rewrite = when(
        over(row_number(), vec![]).gt(lit(12_i64)),
        over(ta("ta_ema", 0, vec![col("close"), lit(13_i64)]), vec![]),
    )
    .end()
    .expect("case");
    let batch = collect(
        frame
            .select(vec![
                col("sym"),
                col("ts"),
                whole.alias("native"),
                whole_rewrite.alias("rewrite"),
            ])
            .expect("select"),
    )
    .await;
    assert_same_cells(
        floats(&batch, "native"),
        floats(&batch, "rewrite"),
        "whole frame",
    );
    assert_eq!(floats(&batch, "native").null_count(), 12);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn null_prefix_instances_never_cse() {
    let plain = window_udf("ta_ema").expect("plain");
    let prefixed = window_udf_with_null_prefix("ta_ema", LOOKBACK).expect("prefixed");
    let other = window_udf_with_null_prefix("ta_ema", LOOKBACK + 1).expect("other prefix");
    let digest = |udf: &datafusion::logical_expr::WindowUDF| {
        let mut hasher = DefaultHasher::new();
        udf.hash(&mut hasher);
        hasher.finish()
    };
    assert_ne!(*plain, *prefixed);
    assert_ne!(*prefixed, *other);
    assert_eq!(
        *prefixed,
        *window_udf_with_null_prefix("ta_ema", LOOKBACK).expect("again")
    );
    assert_ne!(digest(&plain), digest(&prefixed));
    assert_ne!(digest(&prefixed), digest(&other));
    assert_eq!(plain.name(), "ta_ema");
    assert_eq!(prefixed.name(), "ta_ema_null_prefix_12");
    let Expr::WindowFunction(function) = ta("ta_ema", LOOKBACK, vec![col("tr"), lit(13_i64)])
    else {
        panic!("a window function");
    };
    assert!(is_ta_window(&function));

    let context = session();
    let frame = true_range(&context).await;
    let by_symbol = vec![col("sym")];
    let batch = collect(
        frame
            .select(vec![
                col("sym"),
                col("ts"),
                over(
                    ta("ta_ema", 0, vec![col("tr"), lit(13_i64)]),
                    by_symbol.clone(),
                )
                .alias("nan_prefix"),
                over(
                    ta("ta_ema", LOOKBACK, vec![col("tr"), lit(13_i64)]),
                    by_symbol,
                )
                .alias("null_prefix"),
            ])
            .expect("select"),
    )
    .await;
    assert_eq!(
        shape_per_symbol(&batch, "nan_prefix"),
        vec![("CL".to_owned(), 0, 13), ("ES".to_owned(), 0, 13)]
    );
    assert_eq!(
        shape_per_symbol(&batch, "null_prefix"),
        vec![("CL".to_owned(), 12, 1), ("ES".to_owned(), 12, 1)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_ta_unchanged() {
    assert!(
        window_udfs()
            .iter()
            .all(|udf| !udf.name().contains("_null_prefix_"))
    );
    let context = session();
    let batch = collect(
        context
            .sql(
                "SELECT sym, ts, close, \
                 ta_ema(close, 13) OVER (PARTITION BY sym ORDER BY ts) AS ema13 FROM bars",
            )
            .await
            .expect("sql"),
    )
    .await;
    let close = floats(&batch, "close");
    let ema = floats(&batch, "ema13");
    assert_eq!(ema.null_count(), 0);
    let symbols = symbols(&batch);
    for symbol in ["CL", "ES"] {
        let rows: Vec<usize> = (0..symbols.len())
            .filter(|&row| symbols[row] == symbol)
            .collect();
        let series: Vec<f64> = rows.iter().map(|&row| close.value(row)).collect();
        let expected = repark_ta::ema(&series, 13).expect("kernel");
        for (offset, &row) in rows.iter().enumerate() {
            assert_eq!(
                ema.value(row).to_bits(),
                expected[offset].to_bits(),
                "{symbol} row {offset}"
            );
        }
        assert!(expected[..12].iter().all(|value| value.is_nan()));
    }
}
