use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
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

async fn door_with_naive_table() -> (ReparkSession, TempDir) {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run");
    session
        .sql("CREATE TABLE ice.sales.t (id BIGINT, ts TIMESTAMP(6))")
        .await
        .expect("CREATE TABLE must run");
    session
        .sql(
            "INSERT INTO ice.sales.t VALUES (1, TIMESTAMP '2024-01-01 12:00:00'), (2, TIMESTAMP \
             '2024-01-02 12:00:00')",
        )
        .await
        .expect("INSERT must run")
        .collect()
        .await
        .expect("collect");
    (session, warehouse_dir)
}

async fn ids_where(session: &ReparkSession, wall: &str) -> Vec<i64> {
    let batches = session
        .sql(&format!(
            "SELECT id FROM ice.sales.t WHERE ts = TIMESTAMP '{wall}' ORDER BY id"
        ))
        .await
        .expect("filtered read must run")
        .collect()
        .await
        .expect("collect");
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 id");
        for row in 0..batch.num_rows() {
            ids.push(column.value(row));
        }
    }
    ids
}

async fn naive_column_type(session: &ReparkSession) -> DataType {
    session
        .sql("SELECT ts FROM ice.sales.t LIMIT 1")
        .await
        .expect("read must run")
        .schema()
        .as_arrow()
        .field(0)
        .data_type()
        .clone()
}

#[tokio::test]
async fn ansi_update_into_a_naive_timestamp_column_stores_the_wall() {
    let (session, _warehouse) = door_with_naive_table().await;
    session
        .sql("UPDATE ice.sales.t SET ts = TIMESTAMP '2025-05-05 05:05:05' WHERE id = 1")
        .await
        .expect("UPDATE must plan")
        .collect()
        .await
        .expect("UPDATE must run");
    assert_eq!(
        ids_where(&session, "2025-05-05 05:05:05").await,
        vec![1],
        "the updated row must carry the new wall"
    );
    assert_eq!(
        ids_where(&session, "2024-01-02 12:00:00").await,
        vec![2],
        "the untouched row must keep its wall"
    );
    assert_eq!(
        naive_column_type(&session).await,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        "the naive column must stay naive"
    );
}

#[tokio::test]
async fn ansi_merge_into_a_naive_timestamp_column_stores_the_walls() {
    let (session, _warehouse) = door_with_naive_table().await;
    session
        .sql(
            "MERGE INTO ice.sales.t AS t USING (SELECT CAST(2 AS BIGINT) AS id, TIMESTAMP \
             '2026-06-06 06:06:06' AS ts UNION ALL SELECT CAST(3 AS BIGINT) AS id, TIMESTAMP \
             '2027-07-07 07:07:07' AS ts) AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET ts = \
             s.ts WHEN NOT MATCHED THEN INSERT (id, ts) VALUES (s.id, s.ts)",
        )
        .await
        .expect("MERGE must plan")
        .collect()
        .await
        .expect("MERGE must run");
    assert_eq!(
        ids_where(&session, "2024-01-01 12:00:00").await,
        vec![1],
        "the unmatched row must keep its wall"
    );
    assert_eq!(
        ids_where(&session, "2026-06-06 06:06:06").await,
        vec![2],
        "the matched row must carry the source wall"
    );
    assert_eq!(
        ids_where(&session, "2027-07-07 07:07:07").await,
        vec![3],
        "the inserted row must carry the source wall"
    );
    assert_eq!(
        naive_column_type(&session).await,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        "the naive column must stay naive"
    );
}

async fn instant_micros(session: &ReparkSession) -> Vec<(i64, i64)> {
    let batches = session
        .sql("SELECT id, ts FROM ice.sales.z ORDER BY id")
        .await
        .expect("read must run")
        .collect()
        .await
        .expect("collect");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 id");
        let ticks = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>()
            .expect("microsecond instants");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), ticks.value(row)));
        }
    }
    rows
}

#[tokio::test]
async fn ansi_merge_into_an_instant_column_keeps_the_utc_reading() {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ansi_session(&warehouse).await;
    session
        .sql(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
        ))
        .await
        .expect("CREATE SCHEMA must run");
    session
        .sql("CREATE TABLE ice.sales.z (id BIGINT, ts TIMESTAMP(6) WITH TIME ZONE)")
        .await
        .expect("CREATE TABLE must run");
    session
        .sql("INSERT INTO ice.sales.z VALUES (2, NULL)")
        .await
        .expect("INSERT must plan")
        .collect()
        .await
        .expect("INSERT must run");
    session
        .sql(
            "MERGE INTO ice.sales.z AS t USING (SELECT CAST(2 AS BIGINT) AS id, TIMESTAMP \
             '2026-06-06 06:06:06' AS ts UNION ALL SELECT CAST(3 AS BIGINT) AS id, TIMESTAMP \
             '2027-07-07 07:07:07' AS ts) AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET ts = \
             s.ts WHEN NOT MATCHED THEN INSERT (id, ts) VALUES (s.id, s.ts)",
        )
        .await
        .expect("MERGE must plan")
        .collect()
        .await
        .expect("MERGE must run");
    assert_eq!(
        instant_micros(&session).await,
        vec![(2, 1_780_725_966_000_000), (3, 1_814_944_027_000_000)],
        "the ANSI door reads a naive wall as UTC on MERGE into an instant column"
    );
}
