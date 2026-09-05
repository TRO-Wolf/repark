use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

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

#[tokio::test]
async fn ctas_stores_every_derived_column_optional() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA");
    session
        .sql(
            "CREATE TABLE ice.sales.orders AS SELECT 1 AS id, 'a' AS label, \
             coalesce(CAST(NULL AS INT), 0) AS units",
        )
        .await
        .expect("CTAS");
    let frame = session
        .sql("SELECT id, label, units FROM ice.sales.orders")
        .await
        .expect("readback");
    let schema = frame.schema().as_arrow().clone();
    assert_eq!(
        schema.field_with_name("id").expect("id").data_type(),
        &DataType::Int64
    );
    assert!(schema.field_with_name("id").expect("id").is_nullable());
    assert_eq!(
        schema.field_with_name("label").expect("label").data_type(),
        &DataType::Utf8
    );
    assert!(
        schema
            .field_with_name("label")
            .expect("label")
            .is_nullable()
    );
    assert_eq!(
        schema.field_with_name("units").expect("units").data_type(),
        &DataType::Int64
    );
    assert!(
        schema
            .field_with_name("units")
            .expect("units")
            .is_nullable()
    );
    let batches = frame.collect().await.expect("collect");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 id");
    let labels = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 label");
    let units = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 units");
    assert_eq!((ids.value(0), labels.value(0), units.value(0)), (1, "a", 0));
}

#[tokio::test]
async fn column_def_not_null_still_stores_required() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA");
    session
        .sql("CREATE TABLE ice.sales.strict (id INT NOT NULL, label VARCHAR)")
        .await
        .expect("column-def CREATE");
    let frame = session
        .sql("SELECT id, label FROM ice.sales.strict")
        .await
        .expect("readback");
    let schema = frame.schema().as_arrow().clone();
    assert_eq!(
        schema.field_with_name("id").expect("id").data_type(),
        &DataType::Int32
    );
    assert!(!schema.field_with_name("id").expect("id").is_nullable());
    assert!(
        schema
            .field_with_name("label")
            .expect("label")
            .is_nullable()
    );
    let batches = frame.collect().await.expect("collect");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}
