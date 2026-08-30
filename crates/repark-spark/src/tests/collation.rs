//! G15 — Spark-door collation refuse pins (parse altitude).

use super::super::*;
use super::common::*;

use datafusion::sql::sqlparser::ast::{Ident, ObjectName, Set};

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

/// `ORDER BY … COLLATE` second name (charter 2–3 pins / Q-004).
#[tokio::test]
async fn order_by_collate_unicode_ci_refuses() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(
        &ctx,
        &catalogs,
        "SELECT name FROM t ORDER BY name COLLATE UNICODE_CI",
    )
    .await
    .expect_err("ORDER BY COLLATE UNICODE_CI must refuse");
    assert_g15_refusal(&error, "UNICODE_CI");
}

/// Column-def `STRING COLLATE` refuses at the router parse (never a silent Iceberg string).
#[tokio::test]
async fn create_table_column_collate_refuses() {
    let warehouse = TempDir::new().expect("temp warehouse for CREATE TABLE COLLATE pin");
    let (ctx, catalogs) = setup(&warehouse).await;
    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.collated (name STRING COLLATE UTF8_LCASE) USING iceberg",
    )
    .await
    .expect_err("CREATE TABLE column COLLATE must refuse");
    assert_g15_refusal(&error, "UTF8_LCASE");
}

/// Spark-live `CAST(x AS STRING COLLATE name)` is G15, not a generic parse error (Q-002).
#[tokio::test]
async fn cast_as_string_collate_refuses() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(
        &ctx,
        &catalogs,
        "SELECT CAST('Alice' AS STRING COLLATE UTF8_LCASE)",
    )
    .await
    .expect_err("CAST AS STRING COLLATE must refuse G15");
    assert_g15_refusal(&error, "UTF8_LCASE");
}

/// Binding-shaped fragment: type-position COLLATE is G15 even when the CAST is not a statement.
#[test]
fn cast_as_string_collate_fragment_is_detected() {
    let error = refuse_collation_in_sql("CAST('Alice' AS STRING COLLATE UNICODE_CI)")
        .expect_err("CAST fragment must refuse");
    assert_g15_refusal(&error, "UNICODE_CI");
}

/// A `COLLATE` token inside a string literal is not a collation request.
#[tokio::test]
async fn collate_inside_string_literal_is_not_refused() {
    let (ctx, catalogs) = ctx_passthrough();
    execute(&ctx, &catalogs, "SELECT 'COLLATE UTF8_LCASE' AS note")
        .await
        .unwrap_or_else(|error| panic!("literal must pass: {error}"));
    execute(
        &ctx,
        &catalogs,
        "SELECT 'CAST(x AS STRING COLLATE UTF8_LCASE)' AS note",
    )
    .await
    .unwrap_or_else(|error| panic!("CAST COLLATE inside a literal must pass: {error}"));
}

/// Default (non-COLLATE) ORDER BY still returns Spark's null-placement rows.
#[tokio::test]
async fn default_order_by_without_collate_is_untouched() {
    let (ctx, catalogs) = ctx_passthrough();
    let batches = execute(&ctx, &catalogs, "SELECT name FROM t ORDER BY name")
        .await
        .expect("default ORDER BY must plan")
        .collect()
        .await
        .expect("default ORDER BY must collect");
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column is Utf8");
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

/// SQL SET through `execute` (not just the helper) — Q-004 / SEC-003.
#[tokio::test]
async fn set_collation_session_key_refuses_via_execute() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(
        &ctx,
        &catalogs,
        "SET spark.sql.collation.objectLevel.enabled = true",
    )
    .await
    .expect_err("SQL SET collation key must refuse via execute");
    assert_g15_refusal(&error, "spark.sql.collation.objectLevel.enabled");
}

/// Parenthesized SET is not `_ => None` (SEC-003).
#[test]
fn parenthesized_set_collation_key_is_detected() {
    let statement = Statement::Set(Set::ParenthesizedAssignments {
        variables: vec![ObjectName::from(vec![
            Ident::new("spark"),
            Ident::new("sql"),
            Ident::new("collation"),
            Ident::new("schemaLevel"),
            Ident::new("enabled"),
        ])],
        values: vec![],
    });
    let error = refuse_collation_in_statement(&statement)
        .expect_err("parenthesized SET collation key must refuse");
    assert_g15_refusal(&error, "spark.sql.collation.schemaLevel.enabled");
}

/// RESET of a collation key is G15 at the executing parse (SEC-003).
#[tokio::test]
async fn reset_collation_session_key_refuses_via_execute() {
    let (ctx, catalogs) = ctx_passthrough();
    let error = execute(
        &ctx,
        &catalogs,
        "RESET spark.sql.collation.objectLevel.enabled",
    )
    .await
    .expect_err("RESET collation key must refuse");
    assert_g15_refusal(&error, "spark.sql.collation.objectLevel.enabled");
}

/// Q-001: the executing-parse attach in `spark_ast::execute_passthrough` is pinned directly.
#[tokio::test]
async fn execute_passthrough_attaches_collation_valve() {
    let (ctx, catalogs) = ctx_passthrough();
    let error =
        crate::spark_ast::execute_passthrough(&ctx, &catalogs, "SELECT 'Alice' COLLATE UTF8_LCASE")
            .await
            .expect_err("execute_passthrough must refuse COLLATE without the router");
    assert_g15_refusal(&error, "UTF8_LCASE");
}

/// Q-001 source pin: the call site itself must remain in `spark_ast.rs`.
#[test]
fn spark_ast_source_attaches_collation_valve() {
    let source = include_str!("../spark_ast.rs");
    let attached = source.lines().any(|line| {
        let trimmed = line.trim();
        trimmed.starts_with("crate::refuse_collation_in_statement") && !trimmed.starts_with("//")
    });
    assert!(
        attached,
        "A10 / Q-001: spark_ast execute_passthrough must call \
         refuse_collation_in_statement (router is not a substitute)"
    );
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
    .expect("passthrough fixture batch");
    ctx.register_batch("t", batch)
        .expect("register passthrough fixture view");
    (ctx, CatalogRegistry::new())
}
