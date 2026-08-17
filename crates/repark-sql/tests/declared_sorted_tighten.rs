//! SE-1 PR-D1: ANSI-door Iceberg-CREATE refuse of a `tightenNulls` frame.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
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

fn nullable_sorted_rows() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["AAA", "AAA", "BBB"])),
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(1)])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn ansi_ctas_of_tightened_frame_refuses_insert_into_existing_allowed() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run");

    let rows = nullable_sorted_rows();
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &["symbol".to_string(), "ts".to_string()], true)
        .await
        .unwrap();

    let refused = session
        .sql("CREATE TABLE ice.sales.tightened AS SELECT * FROM tight")
        .await
        .expect_err("tightened CTAS must refuse");
    let message = refused.to_string();
    assert!(
        message.contains("tightenNulls"),
        "names the flag: {message}"
    );
    assert!(message.contains("PR-D2"), "names the follow-up: {message}");

    session
        .sql("CREATE TABLE ice.sales.bars AS SELECT * FROM plain")
        .await
        .expect("untightened CTAS must succeed");
    session
        .sql("INSERT INTO ice.sales.bars SELECT * FROM tight")
        .await
        .expect("INSERT into an existing table stays allowed")
        .collect()
        .await
        .expect("collect insert");
}

#[tokio::test]
async fn ansi_ctas_from_derived_expression_over_tightened_source_refuses() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run");
    let rows = nullable_sorted_rows();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &["symbol".to_string(), "ts".to_string()], true)
        .await
        .unwrap();
    let refused = session
        .sql("CREATE TABLE ice.sales.derived AS SELECT ts + 1 AS ts2 FROM tight")
        .await
        .expect_err("derived-expression CTAS must refuse via the source walk");
    let message = refused.to_string();
    assert!(
        message.contains("tightenNulls"),
        "names the flag: {message}"
    );
}
