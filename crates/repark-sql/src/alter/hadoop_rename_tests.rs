use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use repark_core::ReparkSession;
use tempfile::TempDir;

use crate::AnsiDialect;

const HADOOP_RENAME_REFUSAL: &str = "Cannot rename Hadoop tables";

async fn configured_session(catalog_type: &str, warehouse: &TempDir) -> ReparkSession {
    let session = ReparkSession::builder()
        .with_sql_dialect(Arc::new(AnsiDialect))
        .config("spark.sql.catalog.ice.type", catalog_type)
        .config(
            "spark.sql.catalog.ice.warehouse",
            warehouse.path().to_str().unwrap(),
        )
        .build()
        .unwrap();
    session.register_configured_catalogs().await.unwrap();
    session
        .create_namespace("ice", "db", HashMap::new())
        .await
        .unwrap();
    for statement in [
        "CREATE TABLE ice.db.t (id INTEGER)",
        "INSERT INTO ice.db.t VALUES (1)",
        "INSERT INTO ice.db.t VALUES (2)",
    ] {
        run(&session, statement).await;
    }
    session
}

async fn run(session: &ReparkSession, sql: &str) -> Vec<RecordBatch> {
    session.sql(sql).await.unwrap().collect().await.unwrap()
}

async fn row_count(session: &ReparkSession, table: &str) -> usize {
    run(session, &format!("SELECT * FROM {table}"))
        .await
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

#[tokio::test]
async fn hadoop_type_rename_refuses_with_the_java_message() {
    let warehouse = TempDir::new().unwrap();
    let session = configured_session("hadoop", &warehouse).await;
    let error = session
        .sql("ALTER TABLE ice.db.t RENAME TO ice.db.u")
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), HADOOP_RENAME_REFUSAL);
    assert!(
        matches!(error, repark_common::Error::NotImplemented(_)),
        "{error:?}"
    );
    assert_eq!(row_count(&session, "ice.db.t").await, 2);
    assert!(session.sql("SELECT * FROM ice.db.u").await.is_err());
}

#[tokio::test]
async fn memory_type_rename_still_succeeds() {
    let warehouse = TempDir::new().unwrap();
    let session = configured_session("memory", &warehouse).await;
    run(&session, "ALTER TABLE ice.db.t RENAME TO ice.db.u").await;
    assert_eq!(row_count(&session, "ice.db.u").await, 2);
}
