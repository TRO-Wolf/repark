//! `SparkDialect` seam tests: `with_sql_dialect` routes every `sql()` call through the Spark router.

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

/// The dialect routes through the Spark router.
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

/// The dialect passes router refusals through the seam and preserves the session error fold.
#[tokio::test]
async fn dialect_surfaces_router_refusals() {
    let session = spark_session();
    let error = session
        .sql("TRUNCATE TABLE ice.ns.t")
        .await
        .expect_err("TRUNCATE refuses loud (C4-L-001)")
        .to_string();
    assert!(
        error.contains("TRUNCATE TABLE is not supported yet"),
        "{error}"
    );
}
