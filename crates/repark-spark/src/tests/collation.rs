//! G15 — Spark-door collation refuse pins (parse altitude).
//!
//! Each live SQL spelling (expression COLLATE, ORDER BY COLLATE, CREATE TABLE
//! column COLLATE) has class + needle pins. A default (non-COLLATE) path stays
//! executable so the valve does not refuse everything.

use super::super::*;
use super::common::*;

fn assert_g15_refusal(error: &DataFusionError, requested: &str) {
    let text = error.to_string();
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "G15 must be NotImplemented, got {error:?}"
    );
    assert!(
        text.contains(COLLATION_REFUSAL_NEEDLE),
        "must name the unimplemented class: {text}"
    );
    assert!(
        text.contains(requested),
        "must name the requested collation `{requested}`: {text}"
    );
    assert!(
        text.contains("binary/default"),
        "must steer to binary/default ordering: {text}"
    );
}

/// Expression `COLLATE` refuses with the G15 class and names the requested collation.
#[tokio::test]
async fn select_collate_expression_refuses() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(&ctx, &catalogs, "SELECT 'Alice' COLLATE UTF8_LCASE")
        .await
        .expect_err("expression COLLATE must refuse");
    assert_g15_refusal(&error, "UTF8_LCASE");
}

/// A second expression spelling (`UNICODE_CI`) is its own pin, not the same row.
#[tokio::test]
async fn select_collate_unicode_ci_refuses() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(&ctx, &catalogs, "SELECT 'Alice' COLLATE UNICODE_CI")
        .await
        .expect_err("UNICODE_CI COLLATE must refuse");
    assert_g15_refusal(&error, "UNICODE_CI");
}

/// `ORDER BY … COLLATE` refuses (compare/order-changing path).
#[tokio::test]
async fn order_by_collate_refuses() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(
        &ctx,
        &catalogs,
        "SELECT name FROM t ORDER BY name COLLATE UTF8_LCASE",
    )
    .await
    .expect_err("ORDER BY COLLATE must refuse");
    assert_g15_refusal(&error, "UTF8_LCASE");
}

/// Column-def `STRING COLLATE` refuses at the router parse (never a silent Iceberg string).
#[tokio::test]
async fn create_table_column_collate_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.collated (name STRING COLLATE UTF8_LCASE) USING iceberg",
    )
    .await
    .expect_err("CREATE TABLE column COLLATE must refuse");
    assert_g15_refusal(&error, "UTF8_LCASE");
}

/// A `COLLATE` token inside a string literal is not a collation request.
#[tokio::test]
async fn collate_inside_string_literal_is_not_refused() {
    let (ctx, catalogs) = ctx_passthrough();
    execute(&ctx, &catalogs, "SELECT 'COLLATE UTF8_LCASE' AS note")
        .await
        .unwrap_or_else(|error| panic!("literal must pass: {error}"));
}

/// Default (non-COLLATE) ORDER BY still returns Spark's null-placement rows.
#[tokio::test]
async fn default_order_by_without_collate_is_untouched() {
    let (ctx, catalogs) = ctx_passthrough();
    let batches = execute(&ctx, &catalogs, "SELECT name FROM t ORDER BY name")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(column.value(0), "a");
    assert_eq!(column.value(1), "b");
    assert_eq!(column.value(2), "c");
}

/// Detector unit: SET of a Spark 4.1.2 collation `SQLConf` key is a collation request.
#[test]
fn set_collation_session_key_is_detected() {
    let error = refuse_collation_in_sql("SET spark.sql.collation.objectLevel.enabled = true")
        .expect_err("session collation conf must refuse");
    assert_g15_refusal(&error, "spark.sql.collation.objectLevel.enabled");
}

fn ctx_passthrough() -> (SessionContext, CatalogRegistry) {
    let ctx = SessionContext::new();
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            Some("b"),
            Some("a"),
            Some("c"),
        ]))],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();
    (ctx, CatalogRegistry::new())
}
