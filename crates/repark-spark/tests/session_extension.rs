//! End-to-end session test for Spark function shims through `SparkExtension` and `SparkDialect`.

use std::sync::Arc;

use datafusion::arrow::array::{Date32Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};

/// A two-row batch: an id (Int32), a label (Utf8), and a date (Date32, days since epoch).
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("d", DataType::Date32, false),
    ]));
    // 2024-03-15 is 19797 days since the epoch; 2021-01-01 is 18628.
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Date32Array::from(vec![19797, 18628])),
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

#[tokio::test]
async fn temp_view_then_sql_runs_the_spark_function_shim() {
    let session = spark_session();
    session
        .create_or_replace_temp_view("iv_temp", vec![sample_batch()])
        .unwrap();

    // The date shim (`year`, `weekofyear`) must be reachable through `spark.sql`.
    let batches = session
        .sql("SELECT year(d) AS y, weekofyear(d) AS w FROM iv_temp ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let years = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let weeks = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!((years.value(0), weeks.value(0)), (2024, 11)); // 2024-03-15
    assert_eq!((years.value(1), weeks.value(1)), (2021, 53)); // 2021-01-01 -> ISO week 53
}
