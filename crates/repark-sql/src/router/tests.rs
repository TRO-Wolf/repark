//! Router tests: the routing DECISIONS (which statements are intercepted, which are delegated,
//! and in what order the guards run), as distinct from what each handler then does.

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
///
/// This is the delegation baseline the whole door rests on: if reads did not pass through
/// cleanly, every other interception would be beside the point.
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

/// A `$`-suffixed metadata-table reference is routed to delegation, NOT to the statement match.
///
/// Asserted through the routing OUTCOME rather than a mock: with no such table registered, the
/// delegated plan fails with DataFusion's table-not-found error. A statement-match route would
/// have produced a different error entirely (or silently done nothing).
#[tokio::test]
async fn metadata_dollar_form_passes_through() {
    assert!(
        references_metadata_table("SELECT * FROM ice.sales.orders$snapshots"),
        "a `$` name must route to delegation"
    );
    // …and a `$` inside a literal must NOT hijack the routing decision.
    assert!(
        !references_metadata_table(
            "CREATE TABLE ice.s.t WITH (location = 'file:///w/a$b') AS SELECT 1 AS a"
        ),
        "a `$` inside a literal must not route a CREATE away from its handler"
    );

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

/// The guard set runs BEFORE the parse: a script refuses even when its first statement is one
/// the router would have intercepted.
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

/// A DataFusion parser EXTENSION statement (`COPY TO`) reaches delegation, where the SEC-02
/// guard sees the plan — the route that would otherwise be the door's local-filesystem hole.
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
