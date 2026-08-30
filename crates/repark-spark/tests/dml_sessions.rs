//! End-to-end Spark-door DML tests.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

/// A two-row batch with an Int32 id and Utf8 label.
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

/// Build a Spark-doored session with the extension and dialect installed as defaults.
fn spark_session() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .unwrap()
}

/// F-BR-2: a bare `INSERT` through `session.sql` applies eagerly even.
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

    // The write applied eagerly: a subsequent SELECT sees all three ids, value and Int32 type.
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
