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

async fn nested_leaf(session: &ReparkSession, table: &str) -> i64 {
    let batches = session
        .sql(&format!("SELECT st.v FROM {table}"))
        .await
        .expect("leaf read")
        .collect()
        .await
        .expect("collect");
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>()
        .expect("microsecond leaf");
    column.value(0)
}

async fn ansi_nested_door_in(zone: &str) -> (ReparkSession, TempDir) {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .config("spark.sql.session.timeZone", zone)
        .build()
        .expect("session must build");
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .expect("catalog must register");
    session
        .sql(&format!(
            "CREATE SCHEMA ice.q7 WITH (location = '{warehouse}/q7')"
        ))
        .await
        .expect("CREATE SCHEMA must run");
    session
        .sql("CREATE TABLE ice.q7.src (id BIGINT, l TIMESTAMP(6) WITH TIME ZONE)")
        .await
        .expect("source table");
    session
        .sql("INSERT INTO ice.q7.src VALUES (1, TIMESTAMP '2026-01-02 03:04:05.123456+00:00')")
        .await
        .expect("source insert")
        .collect()
        .await
        .expect("source insert collect");
    (session, warehouse_dir)
}

async fn ansi_nested_door() -> (ReparkSession, TempDir) {
    ansi_nested_door_in("America/New_York").await
}

#[tokio::test]
async fn ansi_matched_update_of_a_nested_ntz_keeps_the_utc_wall() {
    let (session, _warehouse) = ansi_nested_door().await;
    session
        .sql("CREATE TABLE ice.q7.matched (id BIGINT, st STRUCT<v TIMESTAMP(6), n INT>, k BIGINT)")
        .await
        .expect("target");
    session
        .sql("INSERT INTO ice.q7.matched (id, st, k) VALUES (1, NULL, 0)")
        .await
        .expect("seed")
        .collect()
        .await
        .expect("seed collect");
    session
        .sql(
            "MERGE INTO ice.q7.matched AS t USING ice.q7.src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET st = named_struct('v', s.l, 'n', 1)",
        )
        .await
        .expect("merge")
        .collect()
        .await
        .expect("merge collect");
    assert_eq!(
        nested_leaf(&session, "ice.q7.matched").await,
        1_767_323_045_123_456,
        "ANSI WHEN MATCHED keeps the UTC wall for a naive source"
    );
}

#[tokio::test]
async fn ansi_unmatched_source_update_of_a_nested_ntz_keeps_the_utc_wall() {
    let (session, _warehouse) = ansi_nested_door().await;
    session
        .sql(
            "CREATE TABLE ice.q7.nmbs (id BIGINT, st STRUCT<v TIMESTAMP(6), n INT>, k BIGINT, \
             c TIMESTAMP(6) WITH TIME ZONE)",
        )
        .await
        .expect("target");
    session
        .sql(
            "INSERT INTO ice.q7.nmbs (id, st, k, c) VALUES (9, NULL, 0, TIMESTAMP '2026-01-02 \
             03:04:05.123456+00:00')",
        )
        .await
        .expect("seed")
        .collect()
        .await
        .expect("seed collect");
    session
        .sql(
            "MERGE INTO ice.q7.nmbs AS t USING ice.q7.src AS s ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE THEN UPDATE SET st = named_struct('v', t.c, 'n', 1)",
        )
        .await
        .expect("merge")
        .collect()
        .await
        .expect("merge collect");
    assert_eq!(
        nested_leaf(&session, "ice.q7.nmbs").await,
        1_767_323_045_123_456,
        "ANSI WHEN NOT MATCHED BY SOURCE keeps the UTC wall for a naive source"
    );
}

#[tokio::test]
async fn ansi_matched_update_with_a_wall_source_keeps_the_wall() {
    let (session, _warehouse) = ansi_nested_door().await;
    session
        .sql("CREATE TABLE ice.q7.wallsrc (id BIGINT, st STRUCT<v TIMESTAMP(6), n INT>, k BIGINT)")
        .await
        .expect("target");
    session
        .sql("INSERT INTO ice.q7.wallsrc (id, st, k) VALUES (1, NULL, 0)")
        .await
        .expect("seed")
        .collect()
        .await
        .expect("seed collect");
    session
        .sql(
            "MERGE INTO ice.q7.wallsrc AS t USING ice.q7.src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET st = named_struct('v', TIMESTAMP '2026-01-02 \
             03:04:05.123456', 'n', 1)",
        )
        .await
        .expect("merge")
        .collect()
        .await
        .expect("merge collect");
    assert_eq!(
        nested_leaf(&session, "ice.q7.wallsrc").await,
        1_767_323_045_123_456,
        "a wall source into a nested NTZ leaf keeps the wall"
    );
}

#[tokio::test]
async fn ansi_matched_update_in_utc_stores_the_utc_wall() {
    let (session, _warehouse) = ansi_nested_door_in("UTC").await;
    session
        .sql("CREATE TABLE ice.q7.utc (id BIGINT, st STRUCT<v TIMESTAMP(6), n INT>, k BIGINT)")
        .await
        .expect("target");
    session
        .sql("INSERT INTO ice.q7.utc (id, st, k) VALUES (1, NULL, 0)")
        .await
        .expect("seed")
        .collect()
        .await
        .expect("seed collect");
    session
        .sql(
            "MERGE INTO ice.q7.utc AS t USING ice.q7.src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET st = named_struct('v', s.l, 'n', 1)",
        )
        .await
        .expect("merge")
        .collect()
        .await
        .expect("merge collect");
    assert_eq!(
        nested_leaf(&session, "ice.q7.utc").await,
        1_767_323_045_123_456,
        "the UTC session wall of the instant is the UTC wall"
    );
}

async fn top_key(session: &ReparkSession, table: &str) -> i64 {
    let batches = session
        .sql(&format!("SELECT k FROM {table}"))
        .await
        .expect("key read")
        .collect()
        .await
        .expect("collect");
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 key");
    column.value(0)
}

#[tokio::test]
async fn ansi_matched_update_of_a_top_level_column_is_unchanged() {
    let (session, _warehouse) = ansi_nested_door().await;
    session
        .sql("CREATE TABLE ice.q7.top (id BIGINT, st STRUCT<v TIMESTAMP(6), n INT>, k BIGINT)")
        .await
        .expect("target");
    session
        .sql("INSERT INTO ice.q7.top (id, st, k) VALUES (1, NULL, 0)")
        .await
        .expect("seed")
        .collect()
        .await
        .expect("seed collect");
    session
        .sql(
            "MERGE INTO ice.q7.top AS t USING ice.q7.src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET k = s.id",
        )
        .await
        .expect("merge")
        .collect()
        .await
        .expect("merge collect");
    assert_eq!(
        top_key(&session, "ice.q7.top").await,
        1,
        "a top-level assignment keeps its value"
    );
}

#[tokio::test]
async fn ansi_matched_update_of_a_plain_table_stores_its_value() {
    let (session, _warehouse) = ansi_nested_door().await;
    session
        .sql("CREATE TABLE ice.q7.plain (id BIGINT, k BIGINT)")
        .await
        .expect("target");
    session
        .sql("INSERT INTO ice.q7.plain (id, k) VALUES (1, 0)")
        .await
        .expect("seed")
        .collect()
        .await
        .expect("seed collect");
    session
        .sql(
            "MERGE INTO ice.q7.plain AS t USING ice.q7.src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET k = s.id",
        )
        .await
        .expect("merge")
        .collect()
        .await
        .expect("merge collect");
    assert_eq!(
        top_key(&session, "ice.q7.plain").await,
        1,
        "a table without a nested NTZ leaf stores its value"
    );
}
