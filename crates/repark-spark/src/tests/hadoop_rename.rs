use std::path::Path;

use repark_core::ReparkSession;

use super::common::*;
use crate::{SparkDialect, SparkExtension};

const HADOOP_RENAME_REFUSAL: &str = "Cannot rename Hadoop tables";

async fn configured_session(catalog_type: &str, warehouse: &Path) -> ReparkSession {
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .config("spark.sql.catalog.ice.type", catalog_type)
        .config(
            "spark.sql.catalog.ice.warehouse",
            warehouse.to_str().unwrap(),
        )
        .build()
        .unwrap();
    session.register_configured_catalogs().await.unwrap();
    session
        .create_namespace("ice", "db", HashMap::new())
        .await
        .unwrap();
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

async fn create_and_insert_twice(session: &ReparkSession) {
    run(session, "CREATE TABLE ice.db.t (id INT) USING iceberg").await;
    run(session, "INSERT INTO ice.db.t VALUES (1)").await;
    run(session, "INSERT INTO ice.db.t VALUES (2)").await;
}

#[tokio::test]
async fn hadoop_type_rename_refuses_with_the_java_message() {
    let warehouse = TempDir::new().unwrap();
    let session = configured_session("hadoop", warehouse.path()).await;
    create_and_insert_twice(&session).await;
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
    let session = configured_session("memory", warehouse.path()).await;
    create_and_insert_twice(&session).await;
    run(&session, "ALTER TABLE ice.db.t RENAME TO ice.db.u").await;
    assert_eq!(row_count(&session, "ice.db.u").await, 2);
}

#[tokio::test]
async fn hadoop_type_staged_create_still_writes_uuid_names_divergence() {
    let warehouse = TempDir::new().unwrap();
    let session = configured_session("hadoop", warehouse.path()).await;
    create_and_insert_twice(&session).await;
    let names: Vec<String> = std::fs::read_dir(warehouse.path().join("db/t/metadata"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !names.iter().any(|name| name == "version-hint.text"),
        "{names:?}"
    );
    let mut versions: Vec<&str> = names
        .iter()
        .filter(|name| name.ends_with(".metadata.json"))
        .map(|name| name.split('-').next().unwrap())
        .collect();
    versions.sort_unstable();
    assert_eq!(versions, ["00000", "00001", "00002"], "{names:?}");
}
