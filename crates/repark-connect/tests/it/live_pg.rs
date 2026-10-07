use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{AsArray, Decimal128Array, Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{Date32Type, Int32Type};
use futures::{StreamExt, TryStreamExt};
use repark_connect::{
    CompareOp, ConnectError, PgIdent, PoolLimits, PostgresConnector, PostgresPool,
    PostgresSettings, Privilege, QualifiedRelation, QueryPool, ResolvedSource, ScanOptions,
    ScanRequest, ScanSource, SettingsDoor, TimeoutSetting, ValueRefusal, discover, scan,
};
use tokio_postgres::{Client, NoTls};

const LIVE: &str = "live: make pg-up, REPARK_PG_URL";
const SERIES: &str = "SELECT pg_catalog.generate_series(1, 200000000)::pg_catalog.int8 AS g";

static NEXT: AtomicU32 = AtomicU32::new(0);

fn url() -> String {
    std::env::var("REPARK_PG_URL").expect("live cells need REPARK_PG_URL: run make pg-up")
}

fn tag() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .subsec_nanos();
    let count = NEXT.fetch_add(1, Ordering::SeqCst);
    format!("{:08x}{count:02x}", nanos ^ std::process::id())
}

async fn admin() -> Client {
    let (client, connection) = tokio_postgres::connect(&url(), NoTls)
        .await
        .expect("the fixture connects");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

struct Cell {
    admin: Client,
    schema: String,
    app: String,
}

impl Cell {
    async fn open() -> Cell {
        let tag = tag();
        let cell = Cell {
            admin: admin().await,
            schema: format!("c2_{tag}"),
            app: format!("repark_{tag}"),
        };
        cell.sql(&format!("CREATE SCHEMA {}", cell.schema)).await;
        cell
    }

    async fn sql(&self, sql: &str) {
        self.admin.batch_execute(sql).await.expect(sql);
    }

    async fn count(&self, sql: &str) -> i64 {
        self.admin
            .query_one(sql, &[])
            .await
            .expect(sql)
            .get::<_, i64>(0)
    }

    async fn backends(&self) -> i64 {
        let sql = "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1";
        let row = self.admin.query_one(sql, &[&self.app]).await.expect(sql);
        row.get(0)
    }

    fn relation(&self, table: &str) -> ScanSource {
        let schema = PgIdent::new(self.schema.as_str()).expect("schema");
        let table = PgIdent::new(table).expect("table");
        ScanSource::Relation(QualifiedRelation::new(schema, table))
    }

    fn settings(&self, extra: &[(&str, &str)]) -> PostgresSettings {
        let mut props = BTreeMap::from([
            ("url".to_string(), url()),
            ("sslmode".to_string(), "disable".to_string()),
            ("application_name".to_string(), self.app.clone()),
        ]);
        for (key, value) in extra {
            props.insert((*key).to_string(), (*value).to_string());
        }
        props.retain(|_, value| !value.is_empty());
        PostgresSettings::from_props(&props, SettingsDoor::ReparkToml).expect("live settings")
    }

    async fn close(self) {
        self.sql(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await;
    }
}

fn pool(settings: &PostgresSettings) -> Arc<PostgresPool> {
    let connector = PostgresConnector::new(settings).expect("connector");
    QueryPool::new(connector, PoolLimits::from_settings(settings))
}

struct Reader {
    pool: Arc<PostgresPool>,
    options: ScanOptions,
}

impl Reader {
    fn new(settings: &PostgresSettings) -> Reader {
        Reader {
            pool: pool(settings),
            options: ScanOptions::from_settings(settings),
        }
    }

    async fn resolve(&self, source: ScanSource) -> Result<Arc<ResolvedSource>, ConnectError> {
        discover(&self.pool, &source, self.options.read_timeout)
            .await
            .map(Arc::new)
    }

    fn open(
        &self,
        request: ScanRequest,
    ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<RecordBatch, ConnectError>> + Send>>
    {
        Box::pin(scan(Arc::clone(&self.pool), request, self.options))
    }

    async fn read(&self, request: ScanRequest) -> Result<Vec<RecordBatch>, ConnectError> {
        self.open(request).try_collect().await
    }

    async fn read_query(&self, sql: &str) -> Result<Vec<RecordBatch>, ConnectError> {
        let resolved = self.resolve(ScanSource::query(sql)).await?;
        self.read(ScanRequest::new(resolved)).await
    }
}

fn int32s(batches: &[RecordBatch]) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| batch.column(0).as_primitive::<Int32Type>().iter())
        .collect()
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn backend_killed_mid_copy_is_disconnected() {
    let cell = Cell::open().await;
    let reader = Reader::new(&cell.settings(&[("batch_rows", "1000")]));
    let resolved = reader.resolve(ScanSource::query(SERIES)).await.expect(LIVE);
    assert_eq!(reader.pool.idle_count(), 1);
    let mut stream = reader.open(ScanRequest::new(resolved));
    stream
        .next()
        .await
        .expect("a batch")
        .expect("the first batch");
    assert_eq!(reader.pool.idle_count(), 0);
    let killed: i32 = cell
        .admin
        .query_one(
            "SELECT pid FROM pg_stat_activity WHERE application_name = $1",
            &[&cell.app],
        )
        .await
        .expect("the scan backend")
        .get(0);
    cell.sql(&format!("SELECT pg_terminate_backend({killed})"))
        .await;
    let outcome = loop {
        match stream.next().await {
            Some(Ok(_)) => {}
            Some(Err(error)) => break error,
            None => panic!("a killed backend must not end the stream cleanly"),
        }
    };
    assert_eq!(outcome, ConnectError::Disconnected);
    drop(stream);
    assert_eq!(
        reader.pool.idle_count(),
        0,
        "the dead client is never pooled"
    );
    let batches = reader
        .read_query("SELECT pg_catalog.pg_backend_pid() AS pid")
        .await
        .expect("the next scan");
    assert_ne!(
        int32s(&batches),
        [Some(killed)],
        "a fresh connection serves"
    );
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn stream_dropped_mid_copy_closes_the_backend() {
    let cell = Cell::open().await;
    let reader =
        Reader::new(&cell.settings(&[("batch_rows", "1000"), ("read_timeout_ms", "5000")]));
    let resolved = reader.resolve(ScanSource::query(SERIES)).await.expect(LIVE);
    let mut stream = reader.open(ScanRequest::new(resolved));
    stream
        .next()
        .await
        .expect("a batch")
        .expect("the first batch");
    assert_eq!(cell.backends().await, 1);
    drop(stream);
    let deadline = Instant::now() + reader.options.read_timeout;
    while cell.backends().await > 0 {
        assert!(
            Instant::now() < deadline,
            "the backend outlived the dropped stream"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        reader.pool.idle_count(),
        0,
        "a dropped scan is never pooled"
    );
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn idle_read_timeout_fires() {
    let cell = Cell::open().await;
    let reader = Reader::new(&cell.settings(&[("read_timeout_ms", "500")]));
    let started = Instant::now();
    let outcome = reader
        .read_query("SELECT 1 AS one FROM pg_catalog.pg_sleep(3)")
        .await;
    assert_eq!(
        outcome,
        Err(ConnectError::Timeout {
            which: TimeoutSetting::Read
        })
    );
    assert!(started.elapsed() < Duration::from_secs(3));
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn lock_timeout_fires() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.t (v int4); INSERT INTO {schema}.t VALUES (1)"
    ))
    .await;
    let reader =
        Reader::new(&cell.settings(&[("lock_timeout_ms", "300"), ("read_timeout_ms", "5000")]));
    let resolved = reader.resolve(cell.relation("t")).await.expect(LIVE);
    let holder = admin().await;
    holder
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {schema}.t IN ACCESS EXCLUSIVE MODE"
        ))
        .await
        .expect("the fixture holds the lock");
    let outcome = reader.read(ScanRequest::new(resolved)).await;
    assert_eq!(
        outcome,
        Err(ConnectError::Timeout {
            which: TimeoutSetting::Lock
        })
    );
    holder.batch_execute("ROLLBACK").await.expect("release");
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn pool_exhaustion_times_out() {
    let cell = Cell::open().await;
    let reader = Reader::new(&cell.settings(&[
        ("pool_max_size", "1"),
        ("pool_checkout_timeout_ms", "300"),
        ("batch_rows", "1000"),
    ]));
    let resolved = reader.resolve(ScanSource::query(SERIES)).await.expect(LIVE);
    let mut held = reader.open(ScanRequest::new(Arc::clone(&resolved)));
    held.next().await.expect("a batch").expect("the held scan");
    let mut second = reader.open(ScanRequest::new(resolved));
    let outcome = second.next().await.expect("an outcome");
    assert_eq!(
        outcome.map(|_| ()),
        Err(ConnectError::PoolExhausted {
            waited: Duration::from_millis(300)
        })
    );
    drop(held);
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn schema_drift_between_plan_and_scan_fails_loud_or_stays_typed() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.widened (v int4); INSERT INTO {schema}.widened VALUES (1), (2), (NULL);
         CREATE TABLE {schema}.retyped (v int4); INSERT INTO {schema}.retyped VALUES (7)"
    ))
    .await;
    let reader = Reader::new(&cell.settings(&[]));
    let widened = reader.resolve(cell.relation("widened")).await.expect(LIVE);
    let retyped = reader.resolve(cell.relation("retyped")).await.expect(LIVE);
    cell.sql(&format!(
        "ALTER TABLE {schema}.widened ALTER COLUMN v TYPE int8;
         ALTER TABLE {schema}.retyped ALTER COLUMN v TYPE text USING 'seven'"
    ))
    .await;
    let batches = reader
        .read(ScanRequest::new(widened))
        .await
        .expect("an int widened then cast back stays typed");
    assert_eq!(int32s(&batches), [Some(1), Some(2), None]);
    let outcome = reader.read(ScanRequest::new(retyped)).await;
    assert!(
        matches!(&outcome, Err(ConnectError::Server { sqlstate, .. }) if sqlstate == "22P02"),
        "{outcome:?}"
    );
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn scan_is_read_only_and_idempotent() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    cell.sql(&format!(
        "CREATE TABLE {schema}.t (id int4, amount numeric(8,3));
         INSERT INTO {schema}.t VALUES (1, 1.250), (2, 2.500), (3, 3.750);
         CREATE FUNCTION {schema}.wipe() RETURNS SETOF int4 LANGUAGE sql
           AS 'DELETE FROM {schema}.t RETURNING id'"
    ))
    .await;
    let reader = Reader::new(&cell.settings(&[]));
    let wipe = reader
        .read_query(&format!("SELECT w FROM {schema}.wipe() AS w"))
        .await;
    assert!(
        matches!(&wipe, Err(ConnectError::Server { sqlstate, .. }) if sqlstate == "25006"),
        "{wipe:?}"
    );
    let delete = reader
        .read_query(&format!("DELETE FROM {schema}.t RETURNING id"))
        .await;
    assert!(
        matches!(delete, Err(ConnectError::Server { .. })),
        "{delete:?}"
    );
    let rows = format!("SELECT count(*) FROM {schema}.t");
    assert_eq!(cell.count(&rows).await, 3);
    let deleted = format!(
        "SELECT coalesce(sum(n_tup_del), 0)::int8 FROM pg_stat_user_tables WHERE schemaname = '{schema}'"
    );
    assert_eq!(cell.count(&deleted).await, 0);
    let resolved = reader.resolve(cell.relation("t")).await.expect(LIVE);
    let pushed = || {
        ScanRequest::new(Arc::clone(&resolved))
            .compare(1, CompareOp::Gt, "2".to_string())
            .expect("a pushed value")
    };
    let first = reader.read(pushed()).await.expect("the first scan");
    let second = reader.read(pushed()).await.expect("the second scan");
    assert_eq!(first, second);
    assert_eq!(int32s(&first), [Some(2), Some(3)]);
    let amounts: Vec<i128> = first
        .iter()
        .flat_map(|batch| {
            let column = batch.column(1).as_any().downcast_ref::<Decimal128Array>();
            column
                .expect("decimal")
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(amounts, [2500, 3750]);
    assert_eq!(cell.count(&rows).await, 3);
    assert_eq!(
        reader.pool.idle_count(),
        1,
        "a clean scan returns its client"
    );
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn query_pool_connections_are_never_replication_connections() {
    let cell = Cell::open().await;
    let reader = Reader::new(&cell.settings(&[("pool_max_size", "2"), ("batch_rows", "1000")]));
    let resolved = reader.resolve(ScanSource::query(SERIES)).await.expect(LIVE);
    let mut first = reader.open(ScanRequest::new(Arc::clone(&resolved)));
    let mut second = reader.open(ScanRequest::new(resolved));
    first.next().await.expect("a batch").expect("first");
    second.next().await.expect("a batch").expect("second");
    let rows = cell
        .admin
        .query(
            "SELECT backend_type, application_name FROM pg_stat_activity \
             WHERE application_name = $1 OR backend_type = 'walsender'",
            &[&cell.app],
        )
        .await
        .expect("pg_stat_activity");
    let seen: Vec<(String, String)> = rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    let expected = (String::from("client backend"), cell.app.clone());
    assert_eq!(seen, [expected.clone(), expected]);
    drop((first, second));
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn missing_select_grant_names_the_privilege() {
    let cell = Cell::open().await;
    let schema = &cell.schema;
    let role = format!("r_{}", &schema[3..]);
    let password = format!("p{}{}", tag(), tag());
    cell.sql(&format!(
        "CREATE TABLE {schema}.t (v int4); CREATE SCHEMA {schema}_hidden;
         CREATE TABLE {schema}_hidden.t (v int4);
         CREATE ROLE {role} LOGIN PASSWORD '{password}'; GRANT USAGE ON SCHEMA {schema} TO {role}"
    ))
    .await;
    let base = url();
    let (_, endpoint) = base.split_once('@').expect("a URL with userinfo");
    let restricted_url = format!("postgresql://{role}:{password}@{endpoint}");
    let restricted = Reader::new(&cell.settings(&[("url", restricted_url.as_str())]));
    let owner = Reader::new(&cell.settings(&[]));
    let relation = QualifiedRelation::new(
        PgIdent::new(schema.as_str()).expect("schema"),
        PgIdent::new("t").expect("table"),
    );
    let denied = |privilege| ConnectError::PermissionDenied {
        relation: relation.clone(),
        privilege,
    };
    assert_eq!(
        restricted.resolve(cell.relation("t")).await,
        Err(denied(Privilege::Select))
    );
    let hidden = QualifiedRelation::new(
        PgIdent::new(format!("{schema}_hidden")).expect("schema"),
        PgIdent::new("t").expect("table"),
    );
    assert_eq!(
        restricted
            .resolve(ScanSource::Relation(hidden.clone()))
            .await,
        Err(ConnectError::PermissionDenied {
            relation: hidden,
            privilege: Privilege::Usage
        })
    );
    let resolved = owner.resolve(cell.relation("t")).await.expect(LIVE);
    assert_eq!(
        restricted.read(ScanRequest::new(resolved)).await,
        Err(denied(Privilege::Select)),
        "a grant revoked between plan and scan names the privilege"
    );
    let missing = restricted.resolve(cell.relation("absent")).await;
    assert!(
        matches!(missing, Err(ConnectError::RelationNotFound { .. })),
        "{missing:?}"
    );
    drop((restricted, owner));
    cell.sql(&format!("DROP SCHEMA {schema}_hidden CASCADE"))
        .await;
    cell.sql(&format!(
        "DROP OWNED BY {role}; DROP SCHEMA {schema} CASCADE; DROP ROLE {role}"
    ))
    .await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn plaintext_server_refuses_under_the_default() {
    let cell = Cell::open().await;
    let settings = cell.settings(&[("sslmode", "")]);
    let outcome = pool(&settings).checkout().await.map(|_| ());
    assert_eq!(outcome, Err(ConnectError::TlsRequired));
    assert!(ConnectError::TlsRequired.to_string().contains("sslmode"));
    cell.close().await;
}

const WIRE_ANCHORS: [(&str, &str); 11] = [
    ("'2000-01-01'::pg_catalog.date", "00000000"),
    ("'1970-01-01'::pg_catalog.date", "ffffd533"),
    ("'2024-03-10'::pg_catalog.date", "00002283"),
    ("'infinity'::pg_catalog.date", "7fffffff"),
    (
        "'1970-01-01 00:00:00'::pg_catalog.timestamp",
        "fffca2fec4c82000",
    ),
    (
        "'2024-03-10 12:00:00'::pg_catalog.timestamp",
        "0002b64beee1d000",
    ),
    (
        "'12345.678'::pg_catalog.numeric(8,3)",
        "0003000100000003000109291a7c",
    ),
    ("'-0.5'::pg_catalog.numeric(2,1)", "0001ffff400000011388"),
    ("'NaN'::pg_catalog.numeric", "00000000c0000000"),
    (
        "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::pg_catalog.uuid",
        "a0eebc999c0b4ef8bb6d6bb9bd380a11",
    ),
    ("'{\"a\": 1}'::pg_catalog.jsonb", "017b2261223a20317d"),
];

fn unhex(anchor: &str) -> Vec<u8> {
    (0..anchor.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&anchor[at..at + 2], 16).expect("hex"))
        .collect()
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn server_bytes_are_the_wire_anchors() {
    let cell = Cell::open().await;
    let pool = pool(&cell.settings(&[]));
    let pooled = pool.checkout().await.expect(LIVE);
    for (literal, anchor) in WIRE_ANCHORS {
        let sql = format!("COPY (SELECT {literal}) TO STDOUT (FORMAT BINARY)");
        let chunks: Vec<bytes::Bytes> = pooled
            .client()
            .copy_out(sql.as_str())
            .await
            .expect(literal)
            .try_collect()
            .await
            .expect(literal);
        let stream = chunks.concat();
        let length = i32::from_be_bytes(stream[21..25].try_into().expect("length"));
        let field = &stream[25..25 + usize::try_from(length).expect("not NULL")];
        assert_eq!(field, unhex(anchor), "{literal}");
    }
    pooled.release_clean();
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn mapped_types_round_trip_through_the_scan() {
    let cell = Cell::open().await;
    cell.sql(&format!(
        "CREATE TYPE {}.mood AS ENUM ('sad', 'ok')",
        cell.schema
    ))
    .await;
    let reader = Reader::new(&cell.settings(&[]));
    let batches = reader
        .read_query(&format!(
            "SELECT '2024-03-10'::pg_catalog.date AS d, \
             '2024-03-10 12:00:00'::pg_catalog.timestamp AS ts, \
             '2024-03-10 12:00:00+00'::pg_catalog.timestamptz AS tz, \
             '12345.678'::pg_catalog.numeric(8,3) AS n, \
             '-0.5'::pg_catalog.numeric(2,1) AS m, \
             '1 day 02:00:00'::pg_catalog.interval AS iv, \
             'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::pg_catalog.uuid AS u, \
             '{{\"a\": 1}}'::pg_catalog.jsonb AS jb, \
             '{{\"b\":  2}}'::pg_catalog.json AS j, \
             'ok'::{}.mood AS e, 7::pg_catalog.int8 AS big",
            cell.schema
        ))
        .await
        .expect(LIVE);
    let batch = &batches[0];
    let text = |index: usize| batch.column(index).as_string::<i32>().value(0).to_string();
    let date = batch.column(0).as_primitive::<Date32Type>().value(0);
    assert_eq!(date, 19_792);
    let micros = |index: usize| {
        let column = batch
            .column(index)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>();
        column.expect("timestamp").value(0)
    };
    assert_eq!(micros(1), 1_710_072_000_000_000);
    assert_eq!(micros(2), 1_710_072_000_000_000);
    let decimal = |index: usize| {
        let column = batch
            .column(index)
            .as_any()
            .downcast_ref::<Decimal128Array>();
        let column = column.expect("decimal");
        (column.value(0), column.precision(), column.scale())
    };
    assert_eq!(decimal(3), (12_345_678, 8, 3));
    assert_eq!(decimal(4), (-5, 2, 1));
    assert_eq!(text(5), "1 day 02:00:00");
    assert_eq!(text(6), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    assert_eq!(text(7), "{\"a\": 1}");
    assert_eq!(text(8), "{\"b\":  2}");
    assert_eq!(text(9), "ok");
    let big = batch.column(10).as_any().downcast_ref::<Int64Array>();
    assert_eq!(big.expect("int8").value(0), 7);
    let refusals = [
        ("'infinity'::pg_catalog.date", ValueRefusal::InfiniteDate),
        ("'NaN'::pg_catalog.numeric", ValueRefusal::NumericNaN),
    ];
    for (literal, refusal) in refusals {
        let outcome = reader.read_query(&format!("SELECT {literal} AS v")).await;
        assert!(
            matches!(&outcome, Err(ConnectError::UnrepresentableValue { reason, .. }) if *reason == refusal),
            "{literal}: {outcome:?}"
        );
    }
    cell.close().await;
}
