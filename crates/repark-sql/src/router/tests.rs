//! Router tests pin which statements are intercepted and which are delegated.

use std::collections::HashSet;

use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::{SessionConfig, SessionContext};
use repark_core::CatalogRegistry;

use super::*;

/// A native session: no extension, no dialect — the profile every ANSI matrix row claims.
fn native_ctx() -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().with_information_schema(true))
}

async fn run(ctx: &SessionContext, sql: &str) -> Result<DataFrame> {
    let catalogs = CatalogRegistry::new();
    let read_only = HashSet::new();
    execute(EngineContext::new(ctx, &catalogs, &read_only), sql).await
}

/// A plain `SELECT` is delegated to DataFusion untouched — value AND type on the Arrow path.
#[tokio::test]
async fn select_delegates_to_datafusion() {
    let ctx = native_ctx();
    let batches = run(&ctx, "SELECT 41 + 1 AS answer, 'x' AS label")
        .await
        .expect("a SELECT must delegate")
        .collect()
        .await
        .expect("collect");

    assert_eq!(
        batches
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "answer");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "type");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "type");
    let answer = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Int64 column");
    assert_eq!(answer.value(0), 42, "value");
}

/// A `$`-suffixed metadata-table reference reaches delegation through the ordinary `_ =>` arm.
#[tokio::test]
async fn metadata_dollar_form_passes_through() {
    let ctx = native_ctx();
    let err = run(&ctx, "SELECT * FROM ice.sales.orders$snapshots")
        .await
        .expect_err("no such table on this session")
        .to_string();
    assert!(
        err.contains("orders$snapshots"),
        "the delegated planner error must name the metadata table: {err}"
    );
}

/// A `$` ANYWHERE in a statement must not route a `CREATE TABLE` away from its handler.
/// This pins the rule that `$` metadata references do not bypass the CREATE TABLE handler.
#[tokio::test]
async fn metadata_reference_does_not_bypass_the_create_handler() {
    let ctx = native_ctx();
    let err = run(
        &ctx,
        "CREATE TABLE snapbak AS SELECT * FROM ice.sales.orders$snapshots",
    )
    .await
    .expect_err("an unregistered CTAS target must refuse even when the query names `$`")
    .to_string();
    assert!(
        err.contains("qualify") || err.contains("registered"),
        "must be the Q15 routing refusal, not a MemTable: {err}"
    );
    assert!(
        !ctx.table_exist("snapbak").unwrap_or(false),
        "nothing may be created in the session"
    );
    // The same holds for a DROP whose name carries `$`: it reaches the DROP handler.
    let err = run(&ctx, "DROP TABLE nosuch$x")
        .await
        .expect_err("an unregistered DROP target must refuse")
        .to_string();
    assert!(
        err.contains("qualify") || err.contains("registered"),
        "DROP must reach its handler too: {err}"
    );
}

/// The guard set runs before parsing, so a multi-statement script refuses before parsing.
#[tokio::test]
async fn guards_run_before_parsing() {
    let ctx = native_ctx();
    let err = run(&ctx, "DROP TABLE ice.s.t; SELECT 1")
        .await
        .expect_err("a script must refuse")
        .to_string();
    assert!(err.contains("[PARSE_SYNTAX_ERROR]"), "guard class: {err}");
}

/// An unparsable statement carrying a Spark-ism is upgraded by the sniff on the way out.
#[tokio::test]
async fn parse_failure_is_upgraded_by_the_sniff() {
    let ctx = native_ctx();
    let err = run(&ctx, "CREATE TABLE ice.s.t USING iceberg AS SELECT 1 AS a")
        .await
        .expect_err("Spark syntax must fail")
        .to_string();
    assert!(err.contains("Spark SQL"), "must be upgraded: {err}");
    assert!(err.contains("USING"), "must name the token: {err}");
}

/// A PLAN failure (not just a parse failure) is upgraded too — the sniff sits on both paths.
#[tokio::test]
async fn plan_failure_is_upgraded_by_the_sniff() {
    let ctx = native_ctx();
    let err = run(&ctx, "SELECT * FROM `nosuch`")
        .await
        .expect_err("backticked name must fail")
        .to_string();
    assert!(err.contains("backtick"), "must be upgraded: {err}");
}

/// `CREATE SCHEMA … AUTHORIZATION` refuses rather than silently ignoring the clause.
#[tokio::test]
async fn create_schema_authorization_refuses() {
    let ctx = native_ctx();
    let err = run(&ctx, "CREATE SCHEMA AUTHORIZATION someone")
        .await
        .expect_err("AUTHORIZATION must refuse")
        .to_string();
    assert!(err.contains("AUTHORIZATION"), "must name the clause: {err}");
}

/// A DataFusion parser extension statement (`COPY TO`) reaches delegation, where the SEC-02 guard runs.
#[tokio::test]
async fn datafusion_extension_statements_reach_the_local_filesystem_guard() {
    let ctx = native_ctx();
    let err = run(
        &ctx,
        "COPY (SELECT 1 AS a) TO '/etc/repark_leak' STORED AS PARQUET",
    )
    .await
    .expect_err("COPY TO a local path must refuse")
    .to_string();
    assert!(
        err.contains("repark.sql.allowLocalFilesystemDDL"),
        "must be the SEC-02 refusal: {err}"
    );
}

/// Temp views work through this door (they are pure delegation) — the shape most tests need.
#[tokio::test]
async fn temp_views_delegate() {
    let ctx = native_ctx();
    run(&ctx, "CREATE OR REPLACE VIEW v AS SELECT 7 AS n")
        .await
        .expect("view creation delegates")
        .collect()
        .await
        .expect("collect");
    let batches = run(&ctx, "SELECT n FROM v")
        .await
        .expect("view read delegates")
        .collect()
        .await
        .expect("collect");
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Int64");
    assert_eq!(column.value(0), 7);
}

/// Crate ANSI door: `execute` refuses every armed name with the registry reason.
///
/// `repark.sql()` does not take this path; it is a bind-level pre-valve on
/// `PyReparkSession.sql`. pins: fnp-15-16/C-001, C-008, C-009, C-010, C-011
#[tokio::test]
async fn execute_refuses_every_armed_declared_name() {
    let ctx = native_ctx();
    let names = crate::declared_refuse::armed_names();
    assert_eq!(names.len(), 62, "roster is 6 unreachable plus 56 deferred");
    for name in names {
        let error = run(&ctx, &format!("SELECT {name}(1)"))
            .await
            .expect_err(name);
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "{name} must be NotImplemented, got {error:?}"
        );
        let text = error.to_string();
        assert!(text.contains(name), "must name {name}: {text}");
        assert!(
            text.contains("docs/spark-sql-iceberg-parity.md"),
            "{name} must cite the registry: {text}"
        );
        let unreachable = text.contains("unreachable");
        let deferred = text.contains("deferred by cost");
        assert!(
            unreachable ^ deferred,
            "{name} must be unreachable xor deferred by cost: {text}"
        );
    }
}
