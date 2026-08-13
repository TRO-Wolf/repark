//! The door's REACHABILITY, pinned end to end: `AnsiDialect` installed on a real
//! [`ReparkSession`] through the frozen seam (`ReparkSessionBuilder::with_sql_dialect`), driving
//! Iceberg DDL + DML through `session.sql`.
//!
//! Why this file exists as a separate binary rather than a unit test: `surfaces::SQL_DIALECT_SEAM`
//! means "the door is reachable through a SESSION", and a unit test that calls
//! `AnsiDialect.execute(EngineContext::new(…))` on a bare `SessionContext` does not show that —
//! it would stay green even if nothing ever installed the dialect. The Spark door pins its own
//! reachability the same way (`crates/repark-spark/tests/*_sessions.rs`).
//!
//! Native profile throughout: NO `SessionExtension` is installed (this door ships none), so the
//! semantics under test are stock DataFusion's, which is the whole claim of the ANSI door.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

/// A session whose DEFAULT dialect is the ANSI door, with one in-memory Iceberg catalog.
async fn ansi_session(warehouse: &str) -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("session must build");
    session
        .register_memory_catalog("ice", warehouse)
        .await
        .expect("catalog must register");
    session
}

/// End to end through `ReparkSession::sql`: schema DDL, CTAS into the registered Iceberg catalog,
/// a delegated INSERT, and a read asserted on the Arrow path (value AND type).
///
/// Mutation: dropping `.with_sql_dialect(...)` from the builder turns this RED — the session
/// default becomes `DataFusionDialect`, whose CTAS makes a session-local `MemTable` and never
/// creates `ice.sales.orders`, so the catalog-visible read fails.
#[tokio::test]
async fn ansi_dialect_on_a_repark_session_runs_the_door() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;

    // The ANSI spelling — `CREATE SCHEMA … WITH (location = …)`, not Spark's NAMESPACE form.
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run through the door");
    session
        .sql("CREATE TABLE ice.sales.orders AS SELECT 1 AS id, 'a' AS label")
        .await
        .expect("CTAS must run through the door");
    session
        .sql("INSERT INTO ice.sales.orders VALUES (2, 'b')")
        .await
        .expect("INSERT must run through the door")
        .collect()
        .await
        .expect("collect");

    let frame = session
        .sql("SELECT id, label FROM ice.sales.orders ORDER BY id")
        .await
        .expect("read must run through the door");
    let schema = frame.schema().as_arrow().clone();
    let batches = frame.collect().await.expect("collect");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "id type");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "label type");

    let mut pairs: Vec<(i64, String)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 id");
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 label");
        for row in 0..batch.num_rows() {
            pairs.push((ids.value(row), labels.value(row).to_string()));
        }
    }
    assert_eq!(
        pairs,
        vec![(1, "a".to_string()), (2, "b".to_string())],
        "the door must have created and written a real Iceberg table"
    );
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2,
        "row count"
    );
}

/// The door's REFUSALS reach the user through the session too — a session-installed dialect must
/// not lose the Q15 routing refusal on its way out of `ReparkSession::sql`.
#[tokio::test]
async fn ansi_door_refusals_surface_through_the_session() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;

    let err = session
        .sql("CREATE TABLE unqualified AS SELECT 1 AS id")
        .await
        .expect_err("an unregistered CTAS target must refuse")
        .to_string();
    assert!(
        err.contains("qualify") || err.contains("registered"),
        "the Q15 refusal must survive the session boundary: {err}"
    );

    // …and nothing was created anywhere — no session-local `MemTable` consolation prize.
    let err = session
        .sql("SELECT * FROM unqualified")
        .await
        .expect_err("nothing may have been created");
    assert!(
        err.to_string().contains("unqualified"),
        "no session-local table may exist: {err}"
    );
}

/// A11 probe (2026-08-13): native ANSI `CREATE TABLE (ts timestamp)` still derives
/// `timestamp_ns` via `CAST(NULL AS TIMESTAMP)` and Iceberg v2 refuses it. The
/// named grant to edit `repark-sql/src/create_table.rs` fires only when that path
/// writes Iceberg `timestamp` (naive); this residual is `timestamp_ns` → morning.
#[tokio::test]
async fn ansi_column_def_timestamp_still_rejects_ns_on_v2() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA");

    let err = session
        .sql("CREATE TABLE ice.sales.ts_ddl (ts timestamp)")
        .await
        .expect_err("native ANSI column-def TIMESTAMP is still ns")
        .to_string();
    assert!(
        err.contains("timestamp_ns") && err.contains("v3"),
        "A11 residual must stay the v2 ns reject, not a silent timestamp/timestamptz write: {err}"
    );
}
