//! PERF-10 ratio micro-benches for Spark date-format and substring shims.
//!
//! Gates compare related work-unit ratios, not absolute wall time; ceilings are fixed by the r24 tip.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{Date32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

// ===========================================================================================
// Final ratio ceilings from the r24 tip.
// ===========================================================================================
const DATE_FORMAT_VS_TO_CHAR_CEILING: f64 = 2.0;
const SUBSTRING_VS_UPPER_CEILING: f64 = 3.0;

/// Rows per batch — large enough that UDF work dominates session overhead.
const ROW_COUNT: usize = 8_192;
/// Timed iterations per sample (median-of-N style inside each criterion iter).
const INNER_ITERS: u32 = 8;

fn session() -> SessionContext {
    let ctx = SessionContext::new();
    repark_functions::register_all(&ctx);
    ctx
}

fn register_date_table(ctx: &SessionContext) {
    let days: Vec<i32> = (0..ROW_COUNT)
        .map(|i| 18_262 + i32::try_from(i % 3_650).unwrap_or(0))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)])),
        vec![Arc::new(Date32Array::from(days))],
    )
    .expect("date batch");
    ctx.register_batch("dates", batch).expect("register dates");
}

fn register_string_table(ctx: &SessionContext) {
    let values: Vec<String> = (0..ROW_COUNT)
        .map(|i| format!("spark-shim-row-{i:05}-abcdefghij"))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(
            values.iter().map(String::as_str).collect::<Vec<_>>(),
        ))],
    )
    .expect("string batch");
    ctx.register_batch("strings", batch)
        .expect("register strings");
}

async fn collect_sql(ctx: &SessionContext, sql: &str) {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect {sql}: {error}"));
    black_box(batches);
}

async fn median_wall(ctx: &SessionContext, sql: &str, samples: u32) -> Duration {
    let mut walls = Vec::with_capacity(samples as usize);
    collect_sql(ctx, sql).await;
    for _ in 0..samples {
        let start = Instant::now();
        for _ in 0..INNER_ITERS {
            collect_sql(ctx, sql).await;
        }
        walls.push(start.elapsed());
    }
    walls.sort_unstable();
    walls[walls.len() / 2]
}

fn ratio_or_inf(numerator: Duration, denominator: Duration) -> f64 {
    let denom = denominator.as_secs_f64();
    if denom <= f64::EPSILON {
        f64::INFINITY
    } else {
        numerator.as_secs_f64() / denom
    }
}

fn bench_date_format_vs_to_char(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let ctx = session();
    register_date_table(&ctx);

    let date_format_sql = "SELECT date_format(d, 'yyyy-MM-dd') AS r FROM dates";
    let to_char_sql = "SELECT to_char(d, '%Y-%m-%d') AS r FROM dates";

    c.bench_function("date_format_batch", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            collect_sql(&ctx, date_format_sql).await;
        });
    });
    c.bench_function("to_char_batch", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            collect_sql(&ctx, to_char_sql).await;
        });
    });

    let date_format_wall = runtime.block_on(median_wall(&ctx, date_format_sql, 7));
    let to_char_wall = runtime.block_on(median_wall(&ctx, to_char_sql, 7));
    let ratio = ratio_or_inf(date_format_wall, to_char_wall);
    eprintln!(
        "PERF-10 date_format/to_char ratio={ratio:.3} \
         (date_format={date_format_wall:?}, to_char={to_char_wall:?}, \
         ceiling={DATE_FORMAT_VS_TO_CHAR_CEILING})"
    );
    assert!(
        ratio.is_finite() && ratio <= DATE_FORMAT_VS_TO_CHAR_CEILING,
        "date_format/to_char ratio {ratio:.3} exceeded ceiling \
         {DATE_FORMAT_VS_TO_CHAR_CEILING} (date_format={date_format_wall:?}, \
         to_char={to_char_wall:?})"
    );
}

fn bench_substring_vs_upper(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let ctx = session();
    register_string_table(&ctx);

    let substring_sql = "SELECT substring(s, 1, 12) AS r FROM strings";
    let upper_sql = "SELECT upper(s) AS r FROM strings";

    c.bench_function("substring_batch", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            collect_sql(&ctx, substring_sql).await;
        });
    });
    c.bench_function("upper_batch", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            collect_sql(&ctx, upper_sql).await;
        });
    });

    let substring_wall = runtime.block_on(median_wall(&ctx, substring_sql, 7));
    let upper_wall = runtime.block_on(median_wall(&ctx, upper_sql, 7));
    let ratio = ratio_or_inf(substring_wall, upper_wall);
    eprintln!(
        "PERF-10 substring/upper ratio={ratio:.3} \
         (substring={substring_wall:?}, upper={upper_wall:?}, \
         ceiling={SUBSTRING_VS_UPPER_CEILING})"
    );
    assert!(
        ratio.is_finite() && ratio <= SUBSTRING_VS_UPPER_CEILING,
        "substring/upper ratio {ratio:.3} exceeded ceiling \
         {SUBSTRING_VS_UPPER_CEILING} (substring={substring_wall:?}, upper={upper_wall:?})"
    );
}

criterion_group!(
    name = ratio_benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(8));
    targets = bench_date_format_vs_to_char, bench_substring_vs_upper
);
criterion_main!(ratio_benches);
