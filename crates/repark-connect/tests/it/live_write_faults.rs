use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use repark_connect::{
    ConnectError, TimeoutSetting, WriteOptions, WritePath, WriteRefusal, WriteReport, WriteRequest,
};

use crate::live_pg::{Cell, LIVE};
use crate::live_write::{
    PATHS, Store, diverging, ids, no_backend_remains, rows_in, store_with, stream_batches,
};
use crate::write::batch_of;

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_write_dropped_mid_flight_poisons_the_writer_and_stores_nothing() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4, body text, big int8);
         CREATE TABLE {schema}.row (LIKE {schema}.bulk)"
    ))
    .await;
    let settings = cell.settings(&[]);
    let options = WriteOptions {
        rows_per_insert: 50,
        ..WriteOptions::from_settings(&settings)
    };
    let (batches, _) = stream_batches(&[100, 200_000, 100]);
    let interrupted = ConnectError::WriteRefused {
        refusal: WriteRefusal::Interrupted,
    };
    for (path, table) in PATHS {
        let store = Store::new(&settings);
        let resolved = store.target(&cell, table).await;
        let request = WriteRequest::new(&resolved).expect("a table takes rows");
        let mut writer = request.open(store.pool(), path, options).await.expect(LIVE);
        writer.write(&batches[0]).await.expect("a hundred rows");
        let cut = Duration::from_micros(500);
        let dropped = tokio::time::timeout(cut, writer.write(&batches[1])).await;
        assert!(dropped.is_err(), "{table}: the large write outlives 500 µs");
        assert_eq!(writer.write(&batches[2]).await, Err(interrupted.clone()));
        assert_eq!(writer.commit().await, Err(interrupted.clone()), "{table}");
        no_backend_remains(&cell).await;
        assert_eq!(rows_in(&cell, table).await, 0, "{table}");
        let report = store.store(&resolved, path, &batches[..1]).await;
        assert_eq!(report, Ok(WriteReport { path, rows: 100 }), "{table}");
        drop(store);
    }
    assert_eq!(
        interrupted.to_string(),
        "a write to a Postgres source refuses: an earlier call was dropped before it finished, \
         so the writer takes no more rows and commits nothing; drop it and write again"
    );
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn rows_arrive_in_input_order_on_both_paths() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4, body text, big int8, seq bigserial);
         CREATE TABLE {schema}.row (id int4, body text, big int8, seq bigserial)"
    ))
    .await;
    let settings = cell.settings(&[]);
    let store = Store::new(&settings);
    let defaults = WriteOptions::from_settings(&settings);
    let grouped = WriteOptions {
        copy_chunk_bytes: 4096,
        rows_per_insert: 7,
        ..defaults
    };
    let (batches, total) = stream_batches(&[1000, 0, 1, 255, 257, 2000]);
    for options in [defaults, grouped] {
        cell.sql(&format!("TRUNCATE {schema}.bulk, {schema}.row"))
            .await;
        for (path, table) in PATHS {
            let resolved = store.target(&cell, table).await;
            let request = WriteRequest::new(&resolved)
                .expect("a table takes rows")
                .columns(&[0, 1, 2])
                .expect("three of four columns");
            let mut writer = request.open(store.pool(), path, options).await.expect(LIVE);
            for batch in &batches {
                writer.write(batch).await.expect("a batch");
            }
            let rows = u64::try_from(total).expect("a row count");
            assert_eq!(writer.commit().await, Ok(WriteReport { path, rows }));
            let sql = format!("SELECT id FROM {schema}.{table} ORDER BY seq");
            let arrived = cell.admin.query(sql.as_str(), &[]).await.expect(&sql);
            let arrived: Vec<i32> = arrived.iter().map(|row| row.get(0)).collect();
            assert_eq!(
                arrived,
                (0..total).collect::<Vec<_>>(),
                "{table} {options:?}"
            );
        }
    }
    drop(store);
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_server_refusal_names_no_written_value() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TYPE {schema}.mood AS ENUM ('happy', 'sad');
         CREATE TABLE {schema}.bulk (id int4, mood {schema}.mood, short varchar(5));
         CREATE TABLE {schema}.row (LIKE {schema}.bulk);
         CREATE TABLE {schema}.gap (id int4, gap interval)"
    ))
    .await;
    let secret = "patient-ssn-123-45-6789";
    let texts = |values: [&str; 2]| -> ArrayRef { Arc::new(StringArray::from(values.to_vec())) };
    let moody = batch_of(&[
        ("id", ids(0..2)),
        ("mood", texts(["happy", secret])),
        ("short", texts(["ok", "ok"])),
    ]);
    let long = batch_of(&[
        ("id", ids(0..2)),
        ("mood", texts(["happy", "sad"])),
        ("short", texts(["ok", secret])),
    ]);
    let store = Store::new(&cell.settings(&[]));
    let refusal = |outcome: Result<WriteReport, ConnectError>| match outcome {
        Err(ConnectError::Server { sqlstate, message }) => (sqlstate, message),
        other => panic!("expected a server refusal, got {other:?}"),
    };
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let (sqlstate, message) = refusal(
            store
                .store(&resolved, path, std::slice::from_ref(&moody))
                .await,
        );
        assert_eq!(sqlstate, "22P02", "{table}");
        assert!(!message.contains("ssn"), "{message}");
        assert!(
            message.starts_with("invalid input value for enum"),
            "{message}"
        );
        assert!(message.ends_with("mood ***"), "{message}");
        let (sqlstate, message) = refusal(
            store
                .store(&resolved, path, std::slice::from_ref(&long))
                .await,
        );
        assert_eq!(sqlstate, "22001", "{table}");
        assert_eq!(message, "value too long for type character varying(5)");
        assert_eq!(rows_in(&cell, table).await, 0, "{table}");
    }
    let gaps = batch_of(&[("id", ids(0..2)), ("gap", texts(["1 day", secret]))]);
    let resolved = store.target(&cell, "gap").await;
    let outcome = store.store(&resolved, WritePath::Bulk, &[gaps]).await;
    let (sqlstate, message) = refusal(outcome);
    assert_eq!(sqlstate, "22007");
    assert_eq!(message, "invalid input syntax for type interval ***");
    drop(store);
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_row_write_idle_past_the_read_timeout_ends_as_that_timeout() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4, body text, big int8);
         CREATE TABLE {schema}.row (LIKE {schema}.bulk)"
    ))
    .await;
    let store = Store::new(&cell.settings(&[("read_timeout_ms", "700")]));
    let (batches, _) = stream_batches(&[300, 300, 256]);
    let idle = Duration::from_millis(2200);
    let timed_out = ConnectError::Timeout {
        which: TimeoutSetting::Read,
    };
    let resolved = store.target(&cell, "row").await;
    let request = WriteRequest::new(&resolved).expect("a table takes rows");
    for (first, write_again) in [(0, true), (0, false), (2, false)] {
        let opening = request
            .clone()
            .open(store.pool(), WritePath::Row, store.options);
        let mut writer = opening.await.expect(LIVE);
        writer
            .write(&batches[first])
            .await
            .expect("rows before the wait");
        tokio::time::sleep(idle).await;
        if write_again {
            assert_eq!(writer.write(&batches[1]).await, Err(timed_out.clone()));
        }
        assert_eq!(
            writer.commit().await,
            Err(timed_out.clone()),
            "{first} {write_again}"
        );
        assert_eq!(rows_in(&cell, "row").await, 0);
    }
    let resolved = store.target(&cell, "bulk").await;
    let request = WriteRequest::new(&resolved).expect("a table takes rows");
    let opening = request.open(store.pool(), WritePath::Bulk, store.options);
    let mut writer = opening.await.expect(LIVE);
    writer.write(&batches[0]).await.expect("three hundred rows");
    tokio::time::sleep(idle).await;
    writer.write(&batches[1]).await.expect("a COPY is not idle");
    let path = WritePath::Bulk;
    assert_eq!(writer.commit().await, Ok(WriteReport { path, rows: 600 }));
    drop(store);
    no_backend_remains(&cell).await;
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn other_arrow_encodings_store_the_same_table_on_both_paths() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4, body text, raw bytea, n int4);
         CREATE TABLE {schema}.row (LIKE {schema}.bulk);
         CREATE TABLE {schema}.plain (LIKE {schema}.bulk)"
    ))
    .await;
    let bodies: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some("é"),
        Some("a"),
    ]));
    let raws = cast(&bodies, &DataType::Binary).expect("binary");
    let counts: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), Some(7), None, Some(-1)]));
    let dictionary =
        |values: DataType| DataType::Dictionary(Box::new(DataType::Int8), Box::new(values));
    let recast = |array: &ArrayRef, to: DataType| cast(array, &to).expect("an encoding");
    let plain = batch_of(&[
        ("id", ids(0..4)),
        ("body", Arc::clone(&bodies)),
        ("raw", Arc::clone(&raws)),
        ("n", Arc::clone(&counts)),
    ]);
    let views = batch_of(&[
        ("id", recast(&ids(0..4), dictionary(DataType::Int32))),
        ("body", recast(&bodies, DataType::Utf8View)),
        ("raw", recast(&raws, DataType::BinaryView)),
        ("n", recast(&counts, dictionary(DataType::Int32))),
    ]);
    let large = batch_of(&[
        ("id", ids(4..8)),
        ("body", recast(&bodies, dictionary(DataType::LargeUtf8))),
        ("raw", recast(&raws, DataType::LargeBinary)),
        ("n", Arc::clone(&counts)),
    ]);
    let again = batch_of(&[
        ("id", ids(4..8)),
        ("body", Arc::clone(&bodies)),
        ("raw", raws),
        ("n", counts),
    ]);
    let store = Store::new(&cell.settings(&[]));
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let stream = [views.clone(), large.clone()];
        let report = store_with(store.pool(), &resolved, path, store.options, &stream).await;
        assert_eq!(report, Ok(WriteReport { path, rows: 8 }), "{table}");
    }
    let resolved = store.target(&cell, "plain").await;
    let report = store
        .store(&resolved, WritePath::Bulk, &[plain, again])
        .await;
    assert_eq!(report.map(|report| report.rows), Ok(8));
    let sends = [
        ("body", "textsend"),
        ("raw", "byteasend"),
        ("n", "int4send"),
    ];
    assert_eq!(diverging(&cell, "bulk", "row", &sends).await, [0_i32; 0]);
    assert_eq!(diverging(&cell, "bulk", "plain", &sends).await, [0_i32; 0]);
    drop(store);
    cell.close().await;
}
