use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use repark_connect::{CompareOp, ConnectError, ScanRequest, ScanSource};

use super::live_pg::{Cell, LIVE, Reader, int32s};

const PID: &str = "SELECT pg_catalog.pg_backend_pid() AS pid";

pub(crate) fn texts(batches: &[RecordBatch]) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| batch.column(0).as_string::<i32>().iter())
        .map(|text| text.map(str::to_string))
        .collect()
}

pub(crate) async fn pid(reader: &Reader) -> i32 {
    let batches = reader.read_query(PID).await.expect(LIVE);
    let pids = int32s(&batches);
    assert_eq!(pids.len(), 1, "{pids:?}");
    pids[0].expect("a pid")
}

async fn poison(reader: &Reader, sql: &str, served_by: i32) {
    assert_eq!(
        pid(reader).await,
        served_by,
        "the connection before the poison"
    );
    reader.read_query(sql).await.expect(sql);
    assert_eq!(reader.pool.idle_count(), 1, "{sql}");
    assert_eq!(
        pid(reader).await,
        served_by,
        "the reset connection is reused"
    );
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_pooled_connection_is_reset_before_reuse() {
    let cell = Cell::open().await;
    let schema = cell.schema.clone();
    let lock_key = i64::from(u32::from_str_radix(&cell.app[7..15], 16).expect("tag"));
    cell.sql(&format!(
        "CREATE TABLE {schema}.iv (i interval, n int4); \
         INSERT INTO {schema}.iv VALUES ('1 day 02:00:00', 1); \
         CREATE TABLE {schema}.w (x int4); \
         CREATE FUNCTION {schema}.writer() RETURNS int4 LANGUAGE sql \
           AS 'INSERT INTO {schema}.w VALUES (1) RETURNING x'; \
         CREATE FUNCTION {schema}.never(int4, int4) RETURNS bool LANGUAGE sql \
           AS 'SELECT false'; \
         CREATE OPERATOR {schema}.= (LEFTARG = int4, RIGHTARG = int4, FUNCTION = {schema}.never)"
    ))
    .await;
    let reader = Reader::new(&cell.settings(&[("pool_max_size", "1")]));
    let iv = reader.resolve(cell.relation("iv")).await.expect(LIVE);
    let writer = format!("SELECT {schema}.writer() AS w");
    let read_only = Err(ConnectError::Server {
        sqlstate: "25006".to_string(),
        message: "cannot execute INSERT in a read-only transaction".to_string(),
    });
    assert_eq!(reader.read_query(&writer).await.map(|_| ()), read_only);
    let served_by = pid(&reader).await;

    poison(
        &reader,
        "SELECT pg_catalog.set_config('IntervalStyle', 'sql_standard', false) AS s",
        served_by,
    )
    .await;
    let interval = reader.read(ScanRequest::new(Arc::clone(&iv)).project(&[0]).expect("i"));
    let interval = texts(&interval.await.expect(LIVE));
    assert_eq!(interval, [Some("1 day 02:00:00".to_string())]);
    let pushed = ScanRequest::new(Arc::clone(&iv))
        .compare(0, CompareOp::Eq, "1 day 02:00:00".to_string())
        .expect("a pushed interval");
    assert_eq!(reader.read(pushed).await.expect(LIVE)[0].num_rows(), 1);

    poison(
        &reader,
        "SELECT pg_catalog.set_config('default_transaction_read_only', 'off', false) AS s",
        served_by,
    )
    .await;
    assert_eq!(reader.read_query(&writer).await.map(|_| ()), read_only);
    let served_by = pid(&reader).await;
    let written = cell
        .count(&format!("SELECT count(*) FROM {schema}.w"))
        .await;
    assert_eq!(written, 0, "nothing was written through the read path");

    poison(
        &reader,
        &format!("SELECT pg_catalog.set_config('search_path', '{schema}, pg_catalog', false) AS s"),
        served_by,
    )
    .await;
    let pushed = ScanRequest::new(Arc::clone(&iv))
        .compare(1, CompareOp::Eq, "1".to_string())
        .expect("a pushed int4");
    assert_eq!(reader.read(pushed).await.expect(LIVE)[0].num_rows(), 1);
    let shown = "SELECT setting AS s FROM pg_catalog.pg_settings WHERE name = 'search_path'";
    let shown = texts(&reader.read_query(shown).await.expect(LIVE));
    assert!(
        shown.iter().flatten().all(|path| !path.contains(&schema)),
        "{shown:?}"
    );

    let locked = format!("SELECT pg_catalog.pg_try_advisory_lock({lock_key}) AS s");
    poison(&reader, &locked, served_by).await;
    let held = format!(
        "SELECT count(*) FROM pg_catalog.pg_locks \
         WHERE locktype = 'advisory' AND objid = {lock_key} AND granted"
    );
    assert_eq!(cell.count(&held).await, 0, "the advisory lock is released");
    assert_eq!(pid(&reader).await, served_by);
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn user_types_resolve_after_a_reset() {
    let cell = Cell::open().await;
    let schema = cell.schema.clone();
    cell.sql(&format!(
        "CREATE TYPE {schema}.first AS ENUM ('a'); CREATE TYPE {schema}.second AS ENUM ('b')"
    ))
    .await;
    let reader = Reader::new(&cell.settings(&[("pool_max_size", "1")]));
    let served_by = pid(&reader).await;
    for (label, type_) in [("a", "first"), ("b", "second")] {
        let sql = format!("SELECT '{label}'::{schema}.{type_} AS e");
        let resolved = reader.resolve(ScanSource::query(&sql)).await.expect(&sql);
        let batches = reader.read(ScanRequest::new(resolved)).await.expect(&sql);
        assert_eq!(texts(&batches), [Some(label.to_string())]);
    }
    assert_eq!(pid(&reader).await, served_by);
    cell.close().await;
}
