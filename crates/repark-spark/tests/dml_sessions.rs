//! Deferred row #3 (v1 `repark_session::tests::session_sql_bare_dml_applies_eagerly`, deferred
//! at phase-1 PR-C): the eager-DML half of the Spark door — a bare `INSERT` whose returned
//! `DataFrame` is dropped without collecting must still apply (the F-BR-2 trap), exercised
//! end-to-end through `session.sql` on a real session built with the door installed
//! (`SparkExtension` + `SparkDialect`). Landed phase-2 PR-3b per `task/port/deferred-tests.md`
//! (the CTAS setup unblocked at PR-3a; the DML arm routing completed at PR-3b).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

/// A two-row batch: an id (Int32) and a label (Utf8) — the v1 session fixture shape.
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap()
}

/// Build the Spark-doored session the way a v1 session was assembled: extension at the two
/// build hooks, dialect as the session default (v1's `ReparkSession::new()`).
fn spark_session() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .unwrap()
}

/// F-BR-2: a bare `INSERT` through `session.sql` applies eagerly even when the returned
/// `DataFrame` is dropped uncollected; the follow-up SELECT sees the new row with its Int32
/// type intact (the downcast proves the column type survived the insert).
#[tokio::test]
async fn session_sql_bare_dml_applies_eagerly() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap().to_string();

    let spark = spark_session();
    spark
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    spark
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
    spark
        .create_or_replace_temp_view("src", vec![sample_batch()])
        .unwrap();
    spark
        .sql("CREATE TABLE ice.sales.t AS SELECT id, label FROM src")
        .await
        .unwrap();

    // A bare INSERT whose returned DataFrame is dropped without collecting — the F-BR-2 trap.
    spark
        .sql("INSERT INTO ice.sales.t VALUES (3, 'c')")
        .await
        .unwrap();

    // The write applied eagerly: a subsequent SELECT (collected) sees all three ids, value and
    // Int32 type — the downcast also proves the column type survived the insert.
    let batches = spark
        .sql("SELECT id FROM ice.sales.t ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for index in 0..column.len() {
            ids.push(column.value(index));
        }
    }
    assert_eq!(
        ids,
        vec![1, 2, 3],
        "the bare INSERT must apply eagerly through session.sql() (3 rows, not 2)"
    );
}
