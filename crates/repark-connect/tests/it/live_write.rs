use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    ArrayRef, AsArray, Date32Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::compute::concat_batches;
use repark_connect::{
    ConnectError, MAX_INSERT_PARAMS, PostgresPool, PostgresSettings, Privilege, ResolvedSource,
    ScanRequest, WriteOptions, WritePath, WriteReport, WriteRequest, WriteValueRefusal,
};

use crate::live_pg::{Cell, LIVE, Reader, tag, url};
use crate::write::{ROWS, Sample, batch_of, sample_batch, samples};

pub(crate) const PATHS: [(WritePath, &str); 2] =
    [(WritePath::Bulk, "bulk"), (WritePath::Row, "row")];

pub(crate) struct Store {
    pub(crate) reader: Reader,
    pub(crate) options: WriteOptions,
}

impl Store {
    pub(crate) fn new(settings: &PostgresSettings) -> Store {
        Store {
            reader: Reader::new(settings),
            options: WriteOptions::from_settings(settings),
        }
    }

    pub(crate) fn pool(&self) -> &Arc<PostgresPool> {
        &self.reader.pool
    }

    pub(crate) async fn target(&self, cell: &Cell, table: &str) -> Arc<ResolvedSource> {
        self.reader.resolve(cell.relation(table)).await.expect(LIVE)
    }

    pub(crate) async fn store(
        &self,
        resolved: &ResolvedSource,
        path: WritePath,
        batches: &[RecordBatch],
    ) -> Result<WriteReport, ConnectError> {
        store_with(self.pool(), resolved, path, self.options, batches).await
    }

    pub(crate) async fn read_back(&self, resolved: Arc<ResolvedSource>) -> RecordBatch {
        let batches = self
            .reader
            .read(ScanRequest::new(resolved))
            .await
            .expect("the table reads back");
        concat_batches(&batches[0].schema(), &batches).expect("one batch")
    }
}

pub(crate) async fn store_with(
    pool: &Arc<PostgresPool>,
    resolved: &ResolvedSource,
    path: WritePath,
    options: WriteOptions,
    batches: &[RecordBatch],
) -> Result<WriteReport, ConnectError> {
    let request = WriteRequest::new(resolved)?;
    let mut writer = request.open(pool, path, options).await?;
    for batch in batches {
        writer.write(batch).await?;
    }
    writer.commit().await
}

pub(crate) fn ids(range: std::ops::Range<i32>) -> ArrayRef {
    Arc::new(Int32Array::from_iter_values(range))
}

pub(crate) async fn diverging(
    cell: &Cell,
    left: &str,
    right: &str,
    sends: &[(&str, &str)],
) -> Vec<i32> {
    let schema = &cell.schema;
    let differs: Vec<String> = sends
        .iter()
        .map(|(name, send)| {
            format!("pg_catalog.{send}(l.{name}) IS DISTINCT FROM pg_catalog.{send}(r.{name})")
        })
        .collect();
    let sql = format!(
        "SELECT COALESCE(l.id, r.id) FROM {schema}.{left} l FULL JOIN {schema}.{right} r \
         ON l.id = r.id WHERE l.id IS NULL OR r.id IS NULL OR {} ORDER BY 1",
        differs.join(" OR ")
    );
    let rows = cell.admin.query(sql.as_str(), &[]).await.expect(&sql);
    rows.iter().map(|row| row.get(0)).collect()
}

pub(crate) async fn rows_in(cell: &Cell, table: &str) -> i64 {
    cell.count(&format!("SELECT count(*) FROM {}.{table}", cell.schema))
        .await
}

pub(crate) async fn no_backend_remains(cell: &Cell) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while cell.backends().await > 0 {
        assert!(Instant::now() < deadline, "a backend outlived its writer");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(crate) fn sqlstate(outcome: Result<WriteReport, ConnectError>) -> String {
    match outcome {
        Err(ConnectError::Server { sqlstate, .. }) => sqlstate,
        other => panic!("expected a server refusal, got {other:?}"),
    }
}

async fn create_sample_tables(cell: &Cell, samples: &[Sample]) {
    let schema = &cell.schema;
    let columns: Vec<String> = samples
        .iter()
        .map(|sample| format!("{} {}", sample.name, sample.ddl.replace("{schema}", schema)))
        .collect();
    cell.sql(&format!(
        "CREATE TYPE {schema}.mood AS ENUM ('happy', 'sad', 'so so');
         CREATE DOMAIN {schema}.rating AS int4 CHECK (VALUE >= 0);
         CREATE TABLE {schema}.bulk (id int4, {columns});
         CREATE TABLE {schema}.row (LIKE {schema}.bulk)",
        columns = columns.join(", ")
    ))
    .await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn bulk_and_row_store_byte_identical_tables_for_every_declared_type() {
    let cell = Cell::open().await;
    let samples = samples();
    create_sample_tables(&cell, &samples).await;
    let store = Store::new(&cell.settings(&[]));
    let values = sample_batch(&samples);
    let mut columns = vec![("id", ids(0..8))];
    columns.extend(
        samples
            .iter()
            .map(|sample| (sample.name, Arc::clone(&sample.values))),
    );
    let batch = batch_of(&columns);
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let request = WriteRequest::new(&resolved).expect("a table takes rows");
        assert_eq!(request.path(WritePath::Bulk), WritePath::Bulk);
        let report = store
            .store(&resolved, path, std::slice::from_ref(&batch))
            .await;
        assert_eq!(report, Ok(WriteReport { path, rows: 8 }), "{table}");
        let back = store.read_back(resolved).await;
        assert_eq!(&back.columns()[1..], values.columns(), "{table} reads back");
    }
    let sends: Vec<(&str, &str)> = samples
        .iter()
        .map(|sample| (sample.name, sample.send))
        .collect();
    assert_eq!(diverging(&cell, "bulk", "row", &sends).await, [0_i32; 0]);
    assert_eq!(rows_in(&cell, "bulk").await, 8);
    drop(store);
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn an_interval_column_takes_the_row_path_whatever_was_asked() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4, gap interval, span interval(2), \
         ym interval year to month);
         CREATE TABLE {schema}.row (LIKE {schema}.bulk);
         CREATE TABLE {schema}.expect (LIKE {schema}.bulk)"
    ))
    .await;
    let texts = [
        Some("1 day"),
        Some("1 year 2 mons 3 days 04:05:06.789"),
        Some("P1Y2M3DT4H5M6.789S"),
        Some("-1 day +02:00:00"),
        Some("@ 1 minute ago"),
        Some("00:00:00"),
        Some("178000000 years"),
        None,
    ];
    assert_eq!(texts.len(), ROWS);
    let gaps: ArrayRef = Arc::new(StringArray::from(texts.to_vec()));
    let batch = batch_of(&[
        ("id", ids(0..8)),
        ("gap", Arc::clone(&gaps)),
        ("span", Arc::clone(&gaps)),
        ("ym", gaps),
    ]);
    let insert = format!(
        "INSERT INTO {schema}.expect VALUES \
         ($1, $2::text::interval, $2::text::interval, $2::text::interval)"
    );
    for (id, text) in (0_i32..).zip(texts) {
        cell.admin
            .execute(insert.as_str(), &[&id, &text])
            .await
            .expect("the server parses its own interval text");
    }
    let store = Store::new(&cell.settings(&[]));
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let request = WriteRequest::new(&resolved).expect("a table takes rows");
        assert_eq!(request.path(path), WritePath::Row);
        let report = store
            .store(&resolved, path, std::slice::from_ref(&batch))
            .await;
        let path = WritePath::Row;
        assert_eq!(report, Ok(WriteReport { path, rows: 8 }), "{table}");
    }
    let sends = [
        ("gap", "interval_send"),
        ("span", "interval_send"),
        ("ym", "interval_send"),
    ];
    assert_eq!(diverging(&cell, "bulk", "row", &sends).await, [0_i32; 0]);
    assert_eq!(diverging(&cell, "bulk", "expect", &sends).await, [0_i32; 0]);
    let literal = cell
        .count(&format!(
            "SELECT count(*) FROM {schema}.bulk WHERE id = 1 \
             AND gap = INTERVAL '1 year 2 mons 3 days 04:05:06.789' \
             AND span = INTERVAL '1 year 2 mons 3 days 04:05:06.79' \
             AND ym = INTERVAL '1 year 2 mons'"
        ))
        .await;
    assert_eq!(
        literal, 1,
        "the type modifier applies as an assignment does"
    );
    let nonsense = batch_of(&[
        ("id", ids(8..10)),
        (
            "gap",
            Arc::new(StringArray::from(vec!["1 day", "three-ish"])),
        ),
        ("span", Arc::new(StringArray::from(vec!["1 day", "1 day"]))),
        ("ym", Arc::new(StringArray::from(vec!["1 day", "1 day"]))),
    ]);
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let outcome = store
            .store(&resolved, path, std::slice::from_ref(&nonsense))
            .await;
        assert_eq!(sqlstate(outcome), "22007", "{table}");
        assert_eq!(rows_in(&cell, table).await, 8, "{table}");
    }
    drop(store);
    cell.close().await;
}

#[derive(Clone, Copy)]
struct Form {
    short: &'static str,
    fixed: &'static str,
    micros: i64,
    moment: &'static str,
    token: &'static str,
    tree: &'static str,
}

const FORMS: [Option<Form>; 5] = [
    Some(Form {
        short: "abc",
        fixed: "a",
        micros: 946_684_800_000_499,
        moment: "2000-01-01 00:00:00.000499",
        token: "{123E4567-E89B-12D3-A456-426614174000}",
        tree: "{ \"b\":1, \"a\":2, \"a\":3 }",
    }),
    Some(Form {
        short: "abcde",
        fixed: "abcd",
        micros: 946_684_800_000_500,
        moment: "2000-01-01 00:00:00.000500",
        token: "123e4567e89b12d3a456426614174000",
        tree: " [1,2 ] ",
    }),
    Some(Form {
        short: "abcde   ",
        fixed: "ab  ",
        micros: 946_684_800_501_500,
        moment: "2000-01-01 00:00:00.501500",
        token: "123e-4567-e89b-12d3-a456-4266-1417-4000",
        tree: "1.50",
    }),
    Some(Form {
        short: "",
        fixed: "",
        micros: -1,
        moment: "1969-12-31 23:59:59.999999",
        token: "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
        tree: "\"\\u00e9\"",
    }),
    None,
];

fn form_batch() -> RecordBatch {
    let text = |pick: fn(Form) -> &'static str| -> ArrayRef {
        Arc::new(
            FORMS
                .iter()
                .map(|form| form.map(pick))
                .collect::<StringArray>(),
        )
    };
    let micros: Vec<Option<i64>> = FORMS.iter().map(|form| form.map(|f| f.micros)).collect();
    batch_of(&[
        ("id", ids(0..5)),
        ("short", text(|form| form.short)),
        ("fixed", text(|form| form.fixed)),
        (
            "ms",
            Arc::new(TimestampMicrosecondArray::from(micros.clone())),
        ),
        (
            "tz",
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone("UTC")),
        ),
        ("token", text(|form| form.token)),
        ("tree", text(|form| form.tree)),
        ("doc", text(|form| form.tree)),
    ])
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn type_modifiers_and_text_forms_store_what_the_server_itself_parses() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4, short varchar(5), fixed char(4), \
         ms timestamp(3), tz timestamptz(0), token uuid, tree jsonb, doc json);
         CREATE TABLE {schema}.row (LIKE {schema}.bulk);
         CREATE TABLE {schema}.expect (LIKE {schema}.bulk)"
    ))
    .await;
    let insert = format!(
        "INSERT INTO {schema}.expect VALUES ($1, $2::text, $3::text, $4::text::timestamp, \
         ($4::text || '+00')::timestamptz, $5::text::uuid, $6::text::jsonb, $6::text::json)"
    );
    for (id, form) in (0_i32..).zip(FORMS) {
        let cell_of = |pick: fn(Form) -> &'static str| form.map(pick);
        let (short, fixed) = (cell_of(|form| form.short), cell_of(|form| form.fixed));
        let (moment, token) = (cell_of(|form| form.moment), cell_of(|form| form.token));
        let tree = cell_of(|form| form.tree);
        cell.admin
            .execute(
                insert.as_str(),
                &[&id, &short, &fixed, &moment, &token, &tree],
            )
            .await
            .expect("the server's own text input");
    }
    let batch = form_batch();
    let store = Store::new(&cell.settings(&[]));
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let report = store
            .store(&resolved, path, std::slice::from_ref(&batch))
            .await;
        assert_eq!(report, Ok(WriteReport { path, rows: 5 }), "{table}");
    }
    let sends = [
        ("short", "varcharsend"),
        ("fixed", "bpcharsend"),
        ("ms", "timestamp_send"),
        ("tz", "timestamptz_send"),
        ("token", "uuid_send"),
        ("tree", "jsonb_send"),
        ("doc", "json_send"),
    ];
    assert_eq!(diverging(&cell, "bulk", "row", &sends).await, [0_i32; 0]);
    assert_eq!(diverging(&cell, "bulk", "expect", &sends).await, [0_i32; 0]);
    let padded = cell
        .count(&format!(
            "SELECT count(*) FROM {schema}.bulk WHERE (id = 2 AND short = 'abcde' \
             AND pg_catalog.octet_length(fixed) = 4 AND ms = '2000-01-01 00:00:00.502' \
             AND tz = '2000-01-01 00:00:01+00' AND tree = '1.50') \
             OR (id = 0 AND ms = '2000-01-01 00:00:00' AND tree = '{{\"a\": 3, \"b\": 1}}')"
        ))
        .await;
    assert_eq!(padded, 2, "padding, trimming and rounding are the server's");
    drop(store);
    cell.close().await;
}

pub(crate) fn stream_batches(sizes: &[i32]) -> (Vec<RecordBatch>, i32) {
    let long = "y".repeat(300_000);
    let mut next = 0;
    let batches = sizes
        .iter()
        .map(|&size| {
            let range = next..next + size;
            next += size;
            let bodies = range.clone().map(|id| match id {
                id if id % 97 == 0 => None,
                1500 => Some(long.clone()),
                id => Some(format!("row {id}")),
            });
            let bigs = range
                .clone()
                .map(|id| (id % 5 != 0).then_some(i64::from(id) << 33));
            batch_of(&[
                ("id", ids(range)),
                ("body", Arc::new(bodies.collect::<StringArray>())),
                ("big", Arc::new(bigs.collect::<Int64Array>())),
            ])
        })
        .collect();
    (batches, next)
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn empty_and_multi_batch_streams_store_the_same_rows_in_the_same_order() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4, body text, big int8);
         CREATE TABLE {schema}.row (LIKE {schema}.bulk)"
    ))
    .await;
    let settings = cell.settings(&[]);
    let store = Store::new(&settings);
    let sends = [("body", "textsend"), ("big", "int8send")];
    let (empty, _) = stream_batches(&[0]);
    for batches in [&[][..], &empty[..]] {
        for (path, table) in PATHS {
            let resolved = store.target(&cell, table).await;
            let report = store.store(&resolved, path, batches).await;
            assert_eq!(report, Ok(WriteReport { path, rows: 0 }), "{table}");
            assert_eq!(rows_in(&cell, table).await, 0, "{table}");
        }
    }
    let (batches, total) = stream_batches(&[1000, 0, 1, 255, 257, 4096]);
    let defaults = WriteOptions::from_settings(&settings);
    let sized = |copy_chunk_bytes, rows_per_insert| WriteOptions {
        copy_chunk_bytes,
        rows_per_insert,
        ..defaults
    };
    let variants = [
        defaults,
        sized(64, 7),
        sized(1 << 20, 7),
        sized(1, 1),
        sized(1 << 20, 100_000),
    ];
    for options in variants {
        cell.sql(&format!("TRUNCATE {schema}.bulk, {schema}.row"))
            .await;
        for (path, table) in PATHS {
            let resolved = store.target(&cell, table).await;
            let report = store_with(store.pool(), &resolved, path, options, &batches).await;
            let rows = u64::try_from(total).expect("a row count");
            assert_eq!(
                report,
                Ok(WriteReport { path, rows }),
                "{table} {options:?}"
            );
        }
        assert_eq!(
            diverging(&cell, "bulk", "row", &sends).await,
            [0_i32; 0],
            "{options:?}"
        );
    }
    let resolved = store.target(&cell, "bulk").await;
    let back = store.read_back(resolved).await;
    let sent = concat_batches(&batches[0].schema(), &batches).expect("one batch");
    assert_eq!(back.columns(), sent.columns());
    drop(store);
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_server_refusal_is_the_same_on_both_paths_and_stores_nothing() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE DOMAIN {schema}.rating AS int4 CHECK (VALUE >= 0);
         CREATE TABLE {schema}.bulk (id int4 NOT NULL, short varchar(5), day date, \
         rating {schema}.rating, once int4 UNIQUE);
         CREATE TABLE {schema}.row (LIKE {schema}.bulk INCLUDING ALL)"
    ))
    .await;
    let rows = |id: Option<i32>, short: &str, day: i32, rating: i32, once: [i32; 2]| {
        let first = id.map(|id| id - 1);
        batch_of(&[
            ("id", Arc::new(Int32Array::from(vec![first, id]))),
            ("short", Arc::new(StringArray::from(vec!["ok", short]))),
            ("day", Arc::new(Date32Array::from(vec![0, day]))),
            ("rating", Arc::new(Int32Array::from(vec![0, rating]))),
            ("once", Arc::new(Int32Array::from(once.to_vec()))),
        ])
    };
    let good = rows(Some(1), "ok", 0, 1, [1, 2]);
    let cases = [
        ("23502", rows(None, "ok", 0, 1, [3, 4])),
        ("22001", rows(Some(3), "abcdef", 0, 1, [3, 4])),
        ("22008", rows(Some(3), "ok", i32::MAX, 1, [3, 4])),
        ("23514", rows(Some(3), "ok", 0, -1, [3, 4])),
        ("23505", rows(Some(3), "ok", 0, 1, [3, 3])),
        ("22021", rows(Some(3), "a\0b", 0, 1, [3, 4])),
    ];
    let store = Store::new(&cell.settings(&[]));
    let tail = rows(Some(9), "ok", 0, 1, [8, 9]);
    for (expected, bad) in cases {
        for (path, table) in PATHS {
            let resolved = store.target(&cell, table).await;
            let stream = [good.clone(), bad.clone(), tail.clone()];
            let outcome = store.store(&resolved, path, &stream).await;
            assert_eq!(sqlstate(outcome), expected, "{table}");
            assert_eq!(rows_in(&cell, table).await, 0, "{table} after {expected}");
        }
    }
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let report = store
            .store(&resolved, path, std::slice::from_ref(&good))
            .await;
        assert_eq!(report, Ok(WriteReport { path, rows: 2 }), "{table}");
    }
    drop(store);
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_refused_value_poisons_the_writer_and_stores_nothing() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4, token uuid);
         CREATE TABLE {schema}.row (LIKE {schema}.bulk)"
    ))
    .await;
    let nil = "00000000-0000-0000-0000-000000000000";
    let tokens = |range: std::ops::Range<i32>, last: &str| {
        let mut texts = vec![nil; range.len()];
        texts.pop();
        texts.push(last);
        batch_of(&[
            ("id", ids(range)),
            ("token", Arc::new(StringArray::from(texts))),
        ])
    };
    let (good, bad) = (tokens(0..300, nil), tokens(300..303, "not-a-uuid"));
    let store = Store::new(&cell.settings(&[]));
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let request = WriteRequest::new(&resolved).expect("a table takes rows");
        let mut writer = request
            .open(store.pool(), path, store.options)
            .await
            .expect(LIVE);
        assert_eq!(writer.path(), path);
        writer.write(&good).await.expect("three hundred rows");
        let refused = writer.write(&bad).await.expect_err("the last token");
        match &refused {
            ConnectError::UnwritableValue { index, reason, .. } => {
                assert_eq!((*index, *reason), (302, WriteValueRefusal::UuidSyntax));
            }
            other => panic!("expected a refused value, got {other:?}"),
        }
        assert_eq!(writer.write(&good).await, Err(refused.clone()), "{table}");
        assert_eq!(writer.commit().await, Err(refused), "{table}");
        assert_eq!(rows_in(&cell, table).await, 0, "{table}");
        let report = store
            .store(&resolved, path, std::slice::from_ref(&good))
            .await;
        assert_eq!(report, Ok(WriteReport { path, rows: 300 }), "{table}");
    }
    assert_eq!(
        diverging(&cell, "bulk", "row", &[("token", "uuid_send")]).await,
        [0_i32; 0]
    );
    drop(store);
    no_backend_remains(&cell).await;
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_writer_dropped_or_killed_before_commit_stores_nothing() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4, body text, big int8);
         CREATE TABLE {schema}.row (LIKE {schema}.bulk)"
    ))
    .await;
    let (batches, _) = stream_batches(&[2000, 2000, 2000]);
    for (path, table) in PATHS {
        let store = Store::new(&cell.settings(&[]));
        let resolved = store.target(&cell, table).await;
        let request = WriteRequest::new(&resolved).expect("a table takes rows");
        let mut writer = request
            .clone()
            .open(store.pool(), path, store.options)
            .await
            .expect(LIVE);
        writer.write(&batches[0]).await.expect("the first batch");
        assert_eq!(cell.backends().await, 1, "{table}");
        drop(writer);
        no_backend_remains(&cell).await;
        assert_eq!(
            store.pool().idle_count(),
            0,
            "a dropped writer is never pooled"
        );
        assert_eq!(rows_in(&cell, table).await, 0, "{table} after a drop");

        let mut writer = request
            .open(store.pool(), path, store.options)
            .await
            .expect(LIVE);
        writer.write(&batches[0]).await.expect("the first batch");
        let killed = cell
            .count(&format!(
                "SELECT count(pg_catalog.pg_terminate_backend(pid)) FROM pg_stat_activity \
                 WHERE application_name = '{}'",
                cell.app
            ))
            .await;
        assert_eq!(killed, 1, "{table}");
        no_backend_remains(&cell).await;
        let mut outcome = Ok(());
        for batch in &batches[1..] {
            outcome = outcome.and(writer.write(batch).await);
        }
        let outcome = match outcome {
            Ok(()) => writer.commit().await.map(|_| ()),
            Err(error) => {
                assert_eq!(writer.commit().await, Err(error.clone()), "{table}");
                Err(error)
            }
        };
        assert_eq!(outcome, Err(ConnectError::Disconnected), "{table}");
        assert_eq!(rows_in(&cell, table).await, 0, "{table} after a kill");
        let report = store.store(&resolved, path, &batches).await;
        assert_eq!(report, Ok(WriteReport { path, rows: 6000 }), "{table}");
        drop(store);
    }
    no_backend_remains(&cell).await;
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_commit_refused_is_definite_and_a_commit_unanswered_is_unknown() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4 UNIQUE DEFERRABLE INITIALLY DEFERRED);
         CREATE TABLE {schema}.row (id int4 UNIQUE DEFERRABLE INITIALLY DEFERRED);
         CREATE TABLE {schema}.slow_bulk (id int4);
         CREATE TABLE {schema}.slow_row (id int4);
         CREATE FUNCTION {schema}.stall() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN PERFORM pg_catalog.pg_sleep(3); RETURN NULL; END $$;
         CREATE CONSTRAINT TRIGGER stall AFTER INSERT ON {schema}.slow_bulk \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {schema}.stall();
         CREATE CONSTRAINT TRIGGER stall AFTER INSERT ON {schema}.slow_row \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {schema}.stall()"
    ))
    .await;
    let twins = batch_of(&[("id", Arc::new(Int32Array::from(vec![1, 1])))]);
    let one = batch_of(&[("id", ids(0..1))]);
    let store = Store::new(&cell.settings(&[("read_timeout_ms", "500")]));
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let outcome = store
            .store(&resolved, path, std::slice::from_ref(&twins))
            .await;
        assert_eq!(
            sqlstate(outcome),
            "23505",
            "{table}: the server answered COMMIT"
        );
        assert_eq!(rows_in(&cell, table).await, 0, "{table}");

        let slow = format!("slow_{table}");
        let resolved = store.target(&cell, &slow).await;
        let relation = WriteRequest::new(&resolved)
            .expect("a table takes rows")
            .relation()
            .clone();
        let started = Instant::now();
        let outcome = store
            .store(&resolved, path, std::slice::from_ref(&one))
            .await;
        assert_eq!(
            outcome,
            Err(ConnectError::CommitUnknown { relation }),
            "{table}"
        );
        assert!(started.elapsed() < Duration::from_millis(2500), "{table}");
        no_backend_remains(&cell).await;
    }
    drop(store);
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_role_without_insert_is_refused_naming_the_privilege() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    let role = format!("w_{}", &schema[3..]);
    let password = format!("p{}{}", tag(), tag());
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4); CREATE TABLE {schema}.row (id int4);
         CREATE ROLE {role} LOGIN PASSWORD '{password}';
         GRANT USAGE ON SCHEMA {schema} TO {role};
         GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role}"
    ))
    .await;
    let base = url();
    let (_, endpoint) = base.split_once('@').expect("a URL with userinfo");
    let restricted_url = format!("postgresql://{role}:{password}@{endpoint}");
    let store = Store::new(&cell.settings(&[("url", restricted_url.as_str())]));
    let one = batch_of(&[("id", ids(0..1))]);
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let relation = WriteRequest::new(&resolved)
            .expect("a table takes rows")
            .relation()
            .clone();
        let outcome = store
            .store(&resolved, path, std::slice::from_ref(&one))
            .await;
        let privilege = Privilege::Insert;
        let denied = ConnectError::PermissionDenied {
            relation,
            privilege,
        };
        assert_eq!(outcome, Err(denied), "{table}");
        assert_eq!(rows_in(&cell, table).await, 0, "{table}");
    }
    cell.sql(&format!(
        "GRANT INSERT ON {schema}.bulk, {schema}.row TO {role}"
    ))
    .await;
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let report = store
            .store(&resolved, path, std::slice::from_ref(&one))
            .await;
        assert_eq!(report, Ok(WriteReport { path, rows: 1 }), "{table}");
    }
    drop(store);
    no_backend_remains(&cell).await;
    cell.sql(&format!(
        "DROP OWNED BY {role}; DROP SCHEMA {schema} CASCADE; DROP ROLE {role}"
    ))
    .await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_write_returns_its_connection_clean_and_names_only_its_columns() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.bulk (id int4, body text DEFAULT 'filled', big int8, \
         gap interval DEFAULT '1 day');
         CREATE TABLE {schema}.row (LIKE {schema}.bulk INCLUDING ALL)"
    ))
    .await;
    let store = Store::new(&cell.settings(&[]));
    let batch = batch_of(&[
        (
            "big",
            Arc::new(Int64Array::from(vec![Some(7), None, Some(9)])),
        ),
        ("id", ids(0..3)),
    ]);
    for (path, table) in PATHS {
        let resolved = store.target(&cell, table).await;
        let request = WriteRequest::new(&resolved).expect("a table takes rows");
        assert_eq!(
            request.path(WritePath::Bulk),
            WritePath::Row,
            "all four columns"
        );
        let request = request.columns(&[2, 0]).expect("two of four columns");
        assert_eq!(
            request.path(WritePath::Bulk),
            WritePath::Bulk,
            "no interval named"
        );
        let mut writer = request
            .open(store.pool(), path, store.options)
            .await
            .expect(LIVE);
        assert_eq!(store.pool().idle_count(), 0);
        writer.write(&batch).await.expect("three rows");
        assert_eq!(
            rows_in(&cell, table).await,
            0,
            "nothing shows before COMMIT"
        );
        let report = writer.commit().await;
        assert_eq!(report, Ok(WriteReport { path, rows: 3 }), "{table}");
        assert_eq!(
            store.pool().idle_count(),
            1,
            "the connection is pooled again"
        );
        assert_eq!(cell.backends().await, 1, "and it is the one connection");
    }
    let sends = [
        ("body", "textsend"),
        ("big", "int8send"),
        ("gap", "interval_send"),
    ];
    assert_eq!(diverging(&cell, "bulk", "row", &sends).await, [0_i32; 0]);
    let defaults = cell
        .count(&format!(
            "SELECT count(*) FROM {schema}.bulk WHERE body = 'filled' AND gap = '1 day' \
             AND big IS NOT DISTINCT FROM (ARRAY[7, NULL, 9]::int8[])[id + 1]"
        ))
        .await;
    assert_eq!(defaults, 3, "the unnamed columns take their defaults");
    let pinned = store
        .reader
        .read_query(
            "SELECT pg_catalog.current_setting('default_transaction_read_only') AS pin, \
             pg_catalog.current_setting('transaction_read_only') AS now",
        )
        .await
        .expect("the pooled connection still reads");
    let pins: Vec<&str> = (0..2)
        .map(|column| pinned[0].column(column).as_string::<i32>().value(0))
        .collect();
    assert_eq!(
        pins,
        ["on", "on"],
        "the session stays read-only outside a write"
    );
    assert_eq!(cell.backends().await, 1);
    drop(store);
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_statement_trigger_fires_once_by_bulk_and_per_statement_by_row() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.fired (name text, statements int8);
         INSERT INTO {schema}.fired VALUES ('bulk', 0), ('row', 0);
         CREATE FUNCTION {schema}.bump() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN UPDATE {schema}.fired SET statements = statements + 1 \
         WHERE name = TG_TABLE_NAME; RETURN NULL; END $$;
         CREATE TABLE {schema}.bulk (id int4, body text, big int8);
         CREATE TABLE {schema}.row (LIKE {schema}.bulk);
         CREATE TRIGGER bump AFTER INSERT ON {schema}.bulk \
         FOR EACH STATEMENT EXECUTE FUNCTION {schema}.bump();
         CREATE TRIGGER bump AFTER INSERT ON {schema}.row \
         FOR EACH STATEMENT EXECUTE FUNCTION {schema}.bump()"
    ))
    .await;
    let settings = cell.settings(&[]);
    let store = Store::new(&settings);
    let defaults = WriteOptions::from_settings(&settings);
    let (batches, total) = stream_batches(&[10, 20, 300]);
    let rows = u64::try_from(total).expect("a row count");
    let cases = [
        (defaults, 1 + 1),
        (
            WriteOptions {
                rows_per_insert: 7,
                ..defaults
            },
            47 + 1,
        ),
        (
            WriteOptions {
                copy_chunk_bytes: 1,
                rows_per_insert: 7,
                ..defaults
            },
            330,
        ),
        (
            WriteOptions {
                rows_per_insert: 100_000,
                ..defaults
            },
            1,
        ),
    ];
    for (options, inserts) in cases {
        cell.sql(&format!("UPDATE {schema}.fired SET statements = 0"))
            .await;
        for (requested, table) in PATHS {
            let resolved = store.target(&cell, table).await;
            let report = store_with(store.pool(), &resolved, requested, options, &batches).await;
            let (path, statements) = match requested {
                WritePath::Bulk => (WritePath::Bulk, 1),
                WritePath::Row => (WritePath::Row, inserts),
            };
            assert_eq!(report, Ok(WriteReport { path, rows }), "{table}");
            let fired = cell
                .count(&format!(
                    "SELECT statements FROM {schema}.fired WHERE name = '{table}'"
                ))
                .await;
            assert_eq!(fired, statements, "{table} {options:?}");
        }
    }
    let widest = i32::try_from(MAX_INSERT_PARAMS / 3).expect("a row count");
    let (batches, total) = stream_batches(&[widest + 5]);
    let options = WriteOptions {
        rows_per_insert: 100_000,
        ..defaults
    };
    cell.sql(&format!(
        "TRUNCATE {schema}.row; UPDATE {schema}.fired SET statements = 0"
    ))
    .await;
    let resolved = store.target(&cell, "row").await;
    let report = store_with(store.pool(), &resolved, WritePath::Row, options, &batches).await;
    let rows = u64::try_from(total).expect("a row count");
    let path = WritePath::Row;
    assert_eq!(report, Ok(WriteReport { path, rows }));
    let fired = cell
        .count(&format!(
            "SELECT statements FROM {schema}.fired WHERE name = 'row'"
        ))
        .await;
    assert_eq!(
        fired,
        1 + 1,
        "one statement of 65 535 parameters, then one remainder statement"
    );
    drop(store);
    cell.close().await;
}
