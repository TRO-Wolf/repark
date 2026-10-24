use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

async fn ansi_door() -> (ReparkSession, TempDir) {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("native ansi session");
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    for setup in [
        "CREATE SCHEMA IF NOT EXISTS ice.ns",
        "CREATE TABLE ice.ns.t (id INT, k INT) WITH (partitioning = ARRAY['k'])",
        "INSERT INTO ice.ns.t VALUES (1, 0)",
    ] {
        run(&session, setup).await;
    }
    (session, warehouse)
}

async fn run(session: &ReparkSession, sql: &str) -> Vec<RecordBatch> {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("the ANSI door plans {sql}: {error}"));
    frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("the ANSI door runs {sql}: {error}"))
}

async fn rows(session: &ReparkSession, table: &str) -> Vec<(i32, i32)> {
    let batches = run(session, &format!("SELECT id, k FROM {table} ORDER BY id")).await;
    let mut out = Vec::new();
    for batch in &batches {
        let ids = batch.column(0).as_primitive::<Int32Type>();
        let keys = batch.column(1).as_primitive::<Int32Type>();
        for row in 0..batch.num_rows() {
            assert!(!ids.is_null(row) && !keys.is_null(row));
            out.push((ids.value(row), keys.value(row)));
        }
    }
    out
}

#[tokio::test]
async fn d15_native_insert_with_a_session_write_conf_stores_the_row() {
    let (session, _warehouse) = ansi_door().await;
    assert!(session.set_iceberg_session_write_conf("spark.sql.iceberg.compression-codec", "zstd"));
    run(&session, "INSERT INTO ice.ns.t VALUES (2, 0)").await;
    assert_eq!(rows(&session, "ice.ns.t").await, [(1, 0), (2, 0)]);
}

#[tokio::test]
async fn d16_native_insert_overwrite_partition_replaces_the_partition() {
    let (session, _warehouse) = ansi_door().await;
    run(&session, "INSERT INTO ice.ns.t VALUES (5, 1)").await;
    run(
        &session,
        "INSERT OVERWRITE ice.ns.t PARTITION (k = 0) SELECT 2 AS id",
    )
    .await;
    assert_eq!(rows(&session, "ice.ns.t").await, [(2, 0), (5, 1)]);
}

#[tokio::test]
async fn d17_native_create_table_as_stores_the_query_rows() {
    let (session, _warehouse) = ansi_door().await;
    run(
        &session,
        "CREATE TABLE ice.ns.c AS SELECT CAST(3 AS INT) AS id, CAST(4 AS INT) AS k",
    )
    .await;
    assert_eq!(rows(&session, "ice.ns.c").await, [(3, 4)]);
}

#[tokio::test]
async fn d18_the_rust_append_api_stores_the_batch() {
    let (session, _warehouse) = ansi_door().await;
    let catalogs = session.catalogs_snapshot();
    let catalog = catalogs.get("ice").expect("the ice catalog");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("k", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![Some(6)])),
            Arc::new(Int32Array::from(vec![Some(0)])),
        ],
    )
    .expect("batch");
    let ident = TableIdent::new(NamespaceIdent::new("ns".to_string()), "t".to_string());
    repark_iceberg::append(catalog, &ident, vec![batch])
        .await
        .expect("append");
    assert_eq!(rows(&session, "ice.ns.t").await, [(1, 0), (6, 0)]);
}

#[tokio::test]
async fn d19_native_merge_inserts_and_updates() {
    let (session, _warehouse) = ansi_door().await;
    run(
        &session,
        "MERGE INTO ice.ns.t t USING (SELECT 1 AS id, 9 AS k UNION ALL SELECT 7 AS id, 0 AS k) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET k = s.k \
         WHEN NOT MATCHED THEN INSERT (id, k) VALUES (s.id, s.k)",
    )
    .await;
    assert_eq!(rows(&session, "ice.ns.t").await, [(1, 9), (7, 0)]);
}

#[tokio::test]
async fn d20_native_update_and_delete_change_the_rows() {
    let (session, _warehouse) = ansi_door().await;
    run(&session, "INSERT INTO ice.ns.t VALUES (8, 0)").await;
    run(&session, "UPDATE ice.ns.t SET k = 3 WHERE id = 1").await;
    run(&session, "DELETE FROM ice.ns.t WHERE id = 8").await;
    assert_eq!(rows(&session, "ice.ns.t").await, [(1, 3)]);
}
