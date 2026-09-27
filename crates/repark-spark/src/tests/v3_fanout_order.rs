use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::ReparkSession;
use tempfile::TempDir;

use crate::{SparkDialect, SparkExtension};

async fn v3_session(wh: &TempDir, shuffle_partitions: Option<&str>) -> ReparkSession {
    let warehouse = wh.path().to_str().unwrap().to_string();
    let builder = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .config("repark.sql.allowCreateFormatVersion3", "true");
    let builder = match shuffle_partitions {
        Some(value) => builder.config("spark.sql.shuffle.partitions", value),
        None => builder,
    };
    let session = builder.build().unwrap();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "ns",
            HashMap::from([("location".to_string(), format!("{warehouse}/ns"))]),
        )
        .await
        .unwrap();
    session
}

async fn run(session: &ReparkSession, sql: &str) {
    session.sql(sql).await.unwrap().collect().await.unwrap();
}

async fn batches(session: &ReparkSession, sql: &str) -> Vec<RecordBatch> {
    session.sql(sql).await.unwrap().collect().await.unwrap()
}

fn int_column(batch: &RecordBatch, position: usize) -> Vec<i64> {
    let array = batch
        .column(position)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..array.len())
        .map(|row| {
            assert!(!array.is_null(row));
            array.value(row)
        })
        .collect()
}

async fn triples(session: &ReparkSession, sql: &str) -> Vec<(i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches(session, sql).await {
        let first = int_column(&batch, 0);
        let second = int_column(&batch, 1);
        let third = int_column(&batch, 2);
        rows.extend(
            first
                .into_iter()
                .zip(second)
                .zip(third)
                .map(|((id, row_id), seq)| (id, row_id, seq)),
        );
    }
    rows
}

async fn pairs(session: &ReparkSession, sql: &str) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches(session, sql).await {
        let first = int_column(&batch, 0);
        let second = int_column(&batch, 1);
        rows.extend(first.into_iter().zip(second));
    }
    rows
}

#[tokio::test]
async fn row_id_v3_cell_answers_spark() {
    let _: &str = "pins: row-lineage-order-1/C-008";
    let wh = TempDir::new().unwrap();
    let session = v3_session(&wh, None).await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id BIGINT, data STRING, cat STRING) USING iceberg PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.t VALUES (1, 'a', 'x'), (2, 'b', 'y'), (4, 'd', 'x')",
    )
    .await;
    run(&session, "INSERT INTO ice.ns.t VALUES (3, 'c', 'x')").await;
    run(&session, "DELETE FROM ice.ns.t WHERE id = 1").await;
    assert_eq!(
        triples(
            &session,
            "SELECT id, _row_id, _last_updated_sequence_number FROM ice.ns.t ORDER BY id",
        )
        .await,
        vec![(2, 0, 1), (3, 3, 2), (4, 2, 1)]
    );
}

#[tokio::test]
async fn seven_keys_take_spark_file_order() {
    let _: &str = "pins: row-lineage-order-1/C-009";
    let wh = TempDir::new().unwrap();
    let session = v3_session(&wh, Some("4")).await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id BIGINT, data STRING, cat STRING) USING iceberg PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.t VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z'), (4, 'd', 'w'), (5, 'e', 'a1'), (6, 'f', 'b1'), (7, 'g', 'q')",
    )
    .await;
    assert_eq!(
        pairs(&session, "SELECT id, _row_id FROM ice.ns.t ORDER BY id").await,
        vec![(1, 3), (2, 0), (3, 1), (4, 2), (5, 5), (6, 6), (7, 4)]
    );
}

#[tokio::test]
async fn bucket_tie_follows_the_configured_reducer() {
    let _: &str = "pins: row-lineage-order-1/C-010";
    let wh = TempDir::new().unwrap();
    let session = v3_session(&wh, Some("4")).await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id BIGINT, cat STRING) USING iceberg PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.t VALUES (1, 'br'), (2, 'aq'), (3, 'bb'), (4, 'aa')",
    )
    .await;
    let rows = pairs(
        &session,
        "SELECT id, _row_id FROM ice.ns.t ORDER BY _row_id",
    )
    .await;
    assert_eq!(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        vec![2, 3, 1, 4]
    );
    let wh_default = TempDir::new().unwrap();
    let default = v3_session(&wh_default, None).await;
    run(
        &default,
        "CREATE TABLE ice.ns.t (id BIGINT, cat STRING) USING iceberg PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &default,
        "INSERT INTO ice.ns.t VALUES (1, 'br'), (2, 'aq'), (3, 'bb'), (4, 'aa')",
    )
    .await;
    let rows = pairs(
        &default,
        "SELECT id, _row_id FROM ice.ns.t ORDER BY _row_id",
    )
    .await;
    assert_eq!(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        vec![1, 3, 2, 4]
    );
}

#[tokio::test]
async fn unpartitioned_insert_keeps_its_order() {
    let _: &str = "pins: row-lineage-order-1/C-011";
    let wh = TempDir::new().unwrap();
    let session = v3_session(&wh, None).await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id BIGINT, data STRING) USING iceberg TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    assert_eq!(
        pairs(&session, "SELECT id, _row_id FROM ice.ns.t ORDER BY id").await,
        vec![(1, 0), (2, 1), (3, 2)]
    );
}

#[tokio::test]
async fn sorted_table_keeps_ascending() {
    let _: &str = "pins: row-lineage-order-1/C-012";
    let wh = TempDir::new().unwrap();
    let session = v3_session(&wh, None).await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id BIGINT, data STRING, cat STRING) USING iceberg PARTITIONED BY (cat) TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(&session, "ALTER TABLE ice.ns.t WRITE ORDERED BY (id)").await;
    run(
        &session,
        "INSERT INTO ice.ns.t VALUES (5, 'e', 'e'), (4, 'd', 'd'), (3, 'c', 'c'), (2, 'b', 'b'), (1, 'a', 'a')",
    )
    .await;
    assert_eq!(
        pairs(&session, "SELECT id, _row_id FROM ice.ns.t ORDER BY id").await,
        vec![(1, 0), (2, 1), (3, 2), (4, 3), (5, 4)]
    );
}
