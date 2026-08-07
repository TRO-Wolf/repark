//! [`SparkDialect`] seam-adaptation tests: installed on a [`ReparkSession`] via
//! `with_sql_dialect`, the dialect routes every `sql()` call through the ported v1 router —
//! the Spark ORDER BY defaults and the PR-2 refuse arms are observable end to end. The P11
//! `read_only` field adaptation is pinned router-side (`router/tests.rs` pins the message; the
//! session threads its postgres snapshot into `EngineContext::read_only` — core session tests).

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use repark_core::{ReparkSession, SqlDialect};

use super::SparkDialect;

fn spark_session() -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect) as Arc<dyn SqlDialect>)
        .build()
        .expect("session build")
}

/// The dialect routes through the ported router: Spark's ASC → NULLS FIRST default applies,
/// which the phase-1 `DataFusionDialect` (plain `SessionContext::sql`) would invert.
#[tokio::test]
async fn dialect_execute_runs_the_spark_router() {
    let session = spark_session();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![Some(2), None, Some(1)]))],
    )
    .expect("fixture batch");
    session
        .context()
        .register_batch("t", batch)
        .expect("register");
    let batches = session
        .sql("SELECT v FROM t ORDER BY v")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("i32 column");
    assert!(column.is_null(0), "Spark ASC default is NULLS FIRST");
}

/// The PR-2 refuse arms are reachable through the seam (the dialect passes the SQL through
/// unmodified — no shadow routing) and survive the session's error fold.
#[tokio::test]
async fn dialect_surfaces_router_refusals() {
    let session = spark_session();
    let error = session
        .sql("CREATE TABLE ice.ns.t AS SELECT 1 AS v")
        .await
        .expect_err("CTAS refuses in PR-2")
        .to_string();
    assert!(error.contains("lands in phase-2 PR-3a"), "{error}");
}
