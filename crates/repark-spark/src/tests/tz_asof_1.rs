use super::super::*;
use super::common::*;

use datafusion::arrow::array::{Array, Int32Array, LargeStringArray, StringArray, StringViewArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;

async fn setup_tz(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    repark_functions::integer_spark::register_spark_integer_planner(&ctx);
    (ctx, catalogs)
}

async fn seed(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.y (id INT, ts TIMESTAMP, s STRING) USING iceberg",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.y VALUES (2, TIMESTAMP '2024-01-02 00:00:00', 'b'), \
         (1, TIMESTAMP '2024-01-01 00:00:00', 'a')",
    )
    .await;
}

async fn seed_z(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.z (id INT, ts TIMESTAMP, s STRING, \
         st STRUCT<a: INT, s: STRING>) USING iceberg",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.z VALUES \
         (10, TIMESTAMP '2024-01-02 00:00:00', 'b', named_struct('a', 1, 's', 'x')), \
         (2, TIMESTAMP '2024-01-01 00:00:00', 'b', named_struct('a', 2, 's', 'z')), \
         (3, TIMESTAMP '2024-01-03 00:00:00', 'a', named_struct('a', 3, 's', 'y'))",
    )
    .await;
}

async fn batches(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<RecordBatch> {
    execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` failed to plan: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("`{sql}` failed to collect: {error}"))
}

fn field_names(batches: &[RecordBatch]) -> Vec<String> {
    batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

fn field_types(batches: &[RecordBatch]) -> Vec<DataType> {
    batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect()
}

fn text_col(batches: &[RecordBatch], col: usize) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column(col);
        if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
            out.extend((0..array.len()).map(|row| array.value(row).to_string()));
        } else if let Some(array) = column.as_any().downcast_ref::<LargeStringArray>() {
            out.extend((0..array.len()).map(|row| array.value(row).to_string()));
        } else {
            let array = column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("string column");
            out.extend((0..array.len()).map(|row| array.value(row).to_string()));
        }
    }
    out
}

fn int_col(batches: &[RecordBatch], col: usize) -> Vec<i32> {
    let mut out = Vec::new();
    for batch in batches {
        let array = batch
            .column(col)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        out.extend((0..array.len()).map(|row| array.value(row)));
    }
    out
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn trimmed_stamp(text: &str) -> bool {
    if text.len() < 19 || !text.starts_with("20") {
        return false;
    }
    let bytes = text.as_bytes();
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b' '
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return false;
    }
    if text.len() == 19 {
        return true;
    }
    let fraction = &text[20..];
    if !text[19..20].starts_with('.') || fraction.is_empty() || fraction.len() > 6 {
        return false;
    }
    fraction.bytes().all(|byte| byte.is_ascii_digit()) && !fraction.ends_with('0')
}

#[tokio::test]
async fn cast_over_its_order_key_answers_like_spark() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(ts AS STRING) FROM ice.sales.y ORDER BY ts",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["ts".to_string()]);
    assert!(is_string(&field_types(&rows)[0]));
    assert_eq!(
        text_col(&rows, 0),
        vec![
            "2024-01-01 00:00:00".to_string(),
            "2024-01-02 00:00:00".to_string()
        ]
    );
}

#[tokio::test]
async fn cast_over_its_order_key_desc_reverses() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(ts AS STRING) FROM ice.sales.y ORDER BY ts DESC",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["ts".to_string()]);
    assert_eq!(
        text_col(&rows, 0),
        vec![
            "2024-01-02 00:00:00".to_string(),
            "2024-01-01 00:00:00".to_string()
        ]
    );
}

#[tokio::test]
async fn cast_over_its_order_key_with_limit_keeps_one_row() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(ts AS STRING) FROM ice.sales.y ORDER BY ts LIMIT 1",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["ts".to_string()]);
    assert_eq!(text_col(&rows, 0), vec!["2024-01-01 00:00:00".to_string()]);
}

#[tokio::test]
async fn cast_aliased_like_the_key_keeps_alias_sort() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(ts AS STRING) AS ts FROM ice.sales.y ORDER BY ts",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["ts".to_string()]);
    assert_eq!(
        text_col(&rows, 0),
        vec![
            "2024-01-01 00:00:00".to_string(),
            "2024-01-02 00:00:00".to_string()
        ]
    );
}

#[tokio::test]
async fn cast_aliased_apart_keeps_the_alias() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(ts AS STRING) AS t2 FROM ice.sales.y ORDER BY ts",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["t2".to_string()]);
    assert_eq!(
        text_col(&rows, 0),
        vec![
            "2024-01-01 00:00:00".to_string(),
            "2024-01-02 00:00:00".to_string()
        ]
    );
}

#[tokio::test]
async fn arithmetic_projection_names_the_paren_form() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT id + 1 FROM ice.sales.y ORDER BY id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["(id + 1)".to_string()]);
    assert_eq!(field_types(&rows), vec![DataType::Int32]);
    assert_eq!(int_col(&rows, 0), vec![2, 3]);
}

#[tokio::test]
async fn function_projection_names_the_call_form() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT upper(s) FROM ice.sales.y ORDER BY s",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["upper(s)".to_string()]);
    assert_eq!(text_col(&rows, 0), vec!["A".to_string(), "B".to_string()]);
}

#[tokio::test]
async fn two_casts_over_two_keys_answer_like_spark() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(ts AS STRING), CAST(id AS STRING) FROM ice.sales.y ORDER BY ts, id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["ts".to_string(), "id".to_string()]);
    assert_eq!(
        text_col(&rows, 0),
        vec![
            "2024-01-01 00:00:00".to_string(),
            "2024-01-02 00:00:00".to_string()
        ]
    );
    assert_eq!(text_col(&rows, 1), vec!["1".to_string(), "2".to_string()]);
}

#[tokio::test]
async fn distinct_cast_with_unprojected_key_answers() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT DISTINCT CAST(ts AS STRING) FROM ice.sales.y ORDER BY ts",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["ts".to_string()]);
    assert_eq!(
        text_col(&rows, 0),
        vec![
            "2024-01-01 00:00:00".to_string(),
            "2024-01-02 00:00:00".to_string()
        ]
    );
}

#[tokio::test]
async fn cast_of_aggregate_names_the_full_cast() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(max(ts) AS STRING) FROM ice.sales.y GROUP BY s ORDER BY max(ts)",
    )
    .await;
    assert_eq!(
        field_names(&rows),
        vec!["CAST(max(ts) AS STRING)".to_string()]
    );
    assert_eq!(
        text_col(&rows, 0),
        vec![
            "2024-01-01 00:00:00".to_string(),
            "2024-01-02 00:00:00".to_string()
        ]
    );
}

#[tokio::test]
async fn snapshots_cast_orders_and_trims_like_spark() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(committed_at AS STRING) FROM ice.sales.y.snapshots ORDER BY committed_at",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["committed_at".to_string()]);
    let texts = text_col(&rows, 0);
    assert!(!texts.is_empty(), "snapshots must answer rows");
    for text in &texts {
        assert!(trimmed_stamp(text), "spark trims trailing zeros: {text}");
    }
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(TIMESTAMP '2026-09-26 19:41:02.56' AS STRING)",
    )
    .await;
    assert_eq!(
        text_col(&rows, 0),
        vec!["2026-09-26 19:41:02.56".to_string()]
    );
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(TIMESTAMP '2024-06-01 12:34:56.123456' AS STRING)",
    )
    .await;
    assert_eq!(
        text_col(&rows, 0),
        vec!["2024-06-01 12:34:56.123456".to_string()]
    );
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(TIMESTAMP '2024-01-01 00:00:00' AS STRING)",
    )
    .await;
    assert_eq!(text_col(&rows, 0), vec!["2024-01-01 00:00:00".to_string()]);
}

#[tokio::test]
async fn projected_keys_stars_and_unions_keep_todays_names() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed(&ctx, &catalogs).await;
    let rows = batches(&ctx, &catalogs, "SELECT ts FROM ice.sales.y ORDER BY ts").await;
    assert_eq!(field_names(&rows), vec!["ts".to_string()]);
    let rows = batches(&ctx, &catalogs, "SELECT * FROM ice.sales.y ORDER BY ts").await;
    assert_eq!(
        field_names(&rows),
        vec!["id".to_string(), "ts".to_string(), "s".to_string()]
    );
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.y UNION ALL SELECT id FROM ice.sales.y ORDER BY id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["id".to_string()]);
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT count(*) FROM ice.sales.y GROUP BY s ORDER BY s",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["count(*)".to_string()]);
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(ts AS STRING) FROM ice.sales.y",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["ice.sales.y.ts".to_string()]);
}

#[tokio::test]
async fn struct_field_key_never_binds_a_same_named_column() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed_z(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT s FROM ice.sales.z ORDER BY st.s, ts",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["s".to_string()]);
    assert_eq!(
        text_col(&rows, 0),
        vec!["b".to_string(), "a".to_string(), "b".to_string()]
    );
}

#[tokio::test]
async fn bare_key_matching_the_display_name_sorts_the_output_column() {
    let wh = TempDir::new().expect("tempdir");
    let (ctx, catalogs) = setup_tz(&wh).await;
    seed_z(&ctx, &catalogs).await;
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(id AS STRING) FROM ice.sales.z ORDER BY id",
    )
    .await;
    assert_eq!(field_names(&rows), vec!["id".to_string()]);
    assert_eq!(
        text_col(&rows, 0),
        vec!["10".to_string(), "2".to_string(), "3".to_string()]
    );
    let rows = batches(
        &ctx,
        &catalogs,
        "SELECT CAST(id AS STRING) FROM ice.sales.z ORDER BY id DESC",
    )
    .await;
    assert_eq!(
        text_col(&rows, 0),
        vec!["3".to_string(), "2".to_string(), "10".to_string()]
    );
}
