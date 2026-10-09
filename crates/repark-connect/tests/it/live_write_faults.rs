use std::time::Duration;

use repark_connect::{ConnectError, WriteOptions, WriteRefusal, WriteReport, WriteRequest};

use crate::live_pg::{Cell, LIVE};
use crate::live_write::{PATHS, Store, no_backend_remains, rows_in, stream_batches};

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
