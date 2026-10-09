use std::sync::Arc;

use datafusion::arrow::array::TimestampNanosecondArray;
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

fn session() -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "America/New_York")
        .build()
        .expect("session")
}

async fn run(session: &ReparkSession, sql: &str) {
    session
        .sql(sql)
        .await
        .expect("SQL plan")
        .collect()
        .await
        .expect("SQL execute");
}

#[tokio::test]
async fn insert_and_merge_store_the_same_nanosecond_wall_time() {
    let warehouse = TempDir::new().expect("warehouse");
    let session = session();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id INT, ts timestamp_ns) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.t VALUES (1, TIMESTAMP '2026-01-02 03:04:05.123456789')",
    )
    .await;
    run(
        &session,
        "MERGE INTO ice.ns.t t USING (SELECT 2 AS id, \
         TIMESTAMP '2026-01-02 03:04:05.123456789' AS ts) s ON t.id=s.id \
         WHEN NOT MATCHED THEN INSERT *",
    )
    .await;
    let batches = session
        .sql("SELECT ts FROM ice.ns.t ORDER BY id")
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ns values")
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(values.len(), 2);
    println!("INSERT={} MERGE={}", values[0], values[1]);
    assert_eq!(
        values[0], values[1],
        "same input must store the same wall time across write doors"
    );
}
