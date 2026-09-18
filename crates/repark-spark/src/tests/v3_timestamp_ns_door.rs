use datafusion::arrow::array::{AsArray, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type, TimeUnit};

use super::super::*;
use super::common::*;

const NS_TABLE: &str = "CREATE TABLE ice.sales.tsns (id INT, ts timestamp_ns, tz timestamptz_ns) \
                        USING iceberg TBLPROPERTIES ('format-version' = '3')";

async fn setup_ns(warehouse: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup_allow_create_format_version_3(warehouse).await;
    for zoned in [false, true] {
        ctx.register_udf(
            repark_functions::timestamp_ns_cast::timestamp_ns_cast_udf(zoned)
                .as_ref()
                .clone(),
        );
    }
    (ctx, catalogs)
}

async fn batches(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<RecordBatch> {
    execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

fn int64_column(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let ints = cast(batch.column(column), &DataType::Int64).expect("ticks as int64");
            ints.as_primitive::<Int64Type>().iter().collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn string_casts_keep_nine_digits_and_the_types() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ns(&warehouse).await;
    let out = batches(
        &ctx,
        &catalogs,
        "SELECT CAST('2026-01-03 00:00:00.000000001' AS timestamp_ns) AS a, \
         CAST('2026-01-03 00:00:00.000000001+01:00' AS TIMESTAMPTZ_NS) AS b",
    )
    .await;
    let schema = out[0].schema();
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
    );
    assert_eq!(int64_column(&out, 0), vec![Some(1_767_398_400_000_000_001)]);
    assert_eq!(int64_column(&out, 1), vec![Some(1_767_394_800_000_000_001)]);
}

#[tokio::test]
async fn insert_values_widens_timestamp_literals_and_strings() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ns(&warehouse).await;
    batches(&ctx, &catalogs, NS_TABLE).await;
    batches(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.tsns VALUES \
         (1, TIMESTAMP '2026-01-03 23:59:59.999999', TIMESTAMP '2026-01-03 23:59:59.999999'), \
         (2, '2026-01-04 00:00:00.000000001', '2026-01-04 00:00:00.000000001'), \
         (3, CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns), \
             CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns))",
    )
    .await;
    let out = batches(
        &ctx,
        &catalogs,
        "SELECT id, ts, tz FROM ice.sales.tsns ORDER BY id",
    )
    .await;
    let expected = vec![
        Some(1_767_484_799_999_999_000),
        Some(1_767_484_800_000_000_001),
        Some(1_767_323_045_123_456_789),
    ];
    assert_eq!(int64_column(&out, 1), expected);
    assert_eq!(int64_column(&out, 2), expected);
}

#[tokio::test]
async fn insert_select_widens_microsecond_columns() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ns(&warehouse).await;
    batches(&ctx, &catalogs, NS_TABLE).await;
    batches(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.src (id INT, a TIMESTAMP, b TIMESTAMP) USING iceberg",
    )
    .await;
    batches(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.src VALUES \
         (1, TIMESTAMP '2026-01-03 23:59:59.999999', TIMESTAMP '2026-01-02 03:04:05.5')",
    )
    .await;
    batches(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.tsns SELECT id, a, b FROM ice.sales.src",
    )
    .await;
    let out = batches(&ctx, &catalogs, "SELECT id, ts, tz FROM ice.sales.tsns").await;
    assert_eq!(int64_column(&out, 1), vec![Some(1_767_484_799_999_999_000)]);
    assert_eq!(int64_column(&out, 2), vec![Some(1_767_323_045_500_000_000)]);
}

#[tokio::test]
async fn ns_string_rendering_and_predicates_keep_nanoseconds() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ns(&warehouse).await;
    batches(&ctx, &catalogs, NS_TABLE).await;
    batches(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.tsns VALUES \
         (1, CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns), NULL), \
         (2, CAST('2026-01-02 03:04:05.123456788' AS timestamp_ns), NULL)",
    )
    .await;
    let out = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(ts AS STRING) AS s FROM ice.sales.tsns \
         WHERE ts = CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns)",
    )
    .await;
    let rendered: Vec<Option<String>> = out
        .iter()
        .flat_map(|batch| {
            let strings = cast(batch.column(0), &DataType::Utf8).expect("utf8");
            let strings = strings
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string array")
                .clone();
            strings
                .iter()
                .map(|value| value.map(str::to_string))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        rendered,
        vec![Some("2026-01-02 03:04:05.123456789".to_string())]
    );
}
