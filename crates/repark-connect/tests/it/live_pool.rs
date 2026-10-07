use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{AsArray, RecordBatch};
use futures::StreamExt;
use repark_connect::{
    CompareOp, ConnectError, PostgresSettings, ScanRequest, ScanSource, SettingsDoor,
    TimeoutSetting,
};
use tokio_postgres::{Client, NoTls};

use super::live_pg::{Cell, LIVE, Reader, int32s, url};

const PID: &str = "SELECT pg_catalog.pg_backend_pid() AS pid";
const PASSWORD: &str = "c2-login";

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

async fn identity(reader: &Reader) -> Vec<Option<String>> {
    let sql = "SELECT current_user::pg_catalog.text AS cu, \
               session_user::pg_catalog.text AS su, \
               pg_catalog.current_setting('role') AS r";
    let batches = reader.read_query(sql).await.expect(sql);
    let batch = batches.first().expect("one batch");
    (0..batch.num_columns())
        .map(|column| {
            let column = batch.column(column).as_string::<i32>();
            column.iter().next().flatten().map(str::to_string)
        })
        .collect()
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

    let login = cell.settings(&[]).user;
    let other = format!("{schema}_o");
    cell.sql(&format!("CREATE ROLE {other} NOLOGIN")).await;
    let mut identities = Vec::new();
    for setting in ["role", "session_authorization"] {
        let set = format!("SELECT pg_catalog.set_config('{setting}', '{other}', false) AS s");
        poison(&reader, &set, served_by).await;
        identities.push(identity(&reader).await);
    }
    cell.sql(&format!("DROP ROLE {other}")).await;
    let clean = [Some(login.clone()), Some(login), Some("none".to_string())];
    assert_eq!(
        identities,
        [clean.clone(), clean],
        "role, then session_authorization"
    );
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

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_pushed_scan_commits_before_its_connection_is_pooled() {
    let cell = Cell::open().await;
    let reader = Reader::new(&cell.settings(&[("pool_max_size", "1")]));
    let sql = "SELECT pg_catalog.pg_backend_pid() AS pid, \
               pg_catalog.pg_current_xact_id_if_assigned()::pg_catalog.text AS xid, 1 AS one";
    let resolved = reader.resolve(ScanSource::query(sql)).await.expect(LIVE);
    let mut served = Vec::new();
    for _ in 0..2 {
        let pushed = ScanRequest::new(Arc::clone(&resolved))
            .compare(2, CompareOp::Eq, "1".to_string())
            .expect("a pushed value");
        let batches = reader.read(pushed).await.expect(LIVE);
        let pid = int32s(&batches)[0].expect("a pid");
        assert!(batches[0].column(1).is_null(0), "no transaction id");
        assert_eq!(reader.pool.idle_count(), 1);
        let state = "SELECT state FROM pg_catalog.pg_stat_activity WHERE pid = $1";
        let state: Option<String> = cell
            .admin
            .query_opt(state, &[&pid])
            .await
            .expect(state)
            .map(|row| row.get(0));
        assert_eq!(
            state.as_deref(),
            Some("idle"),
            "pooled outside a transaction"
        );
        served.push(pid);
    }
    assert_eq!(served[0], served[1], "the committed connection is reused");
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_wrong_password_is_authentication_failed() {
    let cell = Cell::open().await;
    let settings = cell.settings(&[]);
    let wrong = url().replacen(
        &format!(":{}@", settings.password.as_deref().expect(LIVE)),
        ":not-the-password@",
        1,
    );
    let props = BTreeMap::from([
        ("url".to_string(), wrong),
        ("sslmode".to_string(), "disable".to_string()),
        ("application_name".to_string(), cell.app.clone()),
    ]);
    let wrong = PostgresSettings::from_props(&props, SettingsDoor::ReparkToml).expect(LIVE);
    let reader = Reader::new(&wrong);
    let refused = reader.resolve(ScanSource::query(PID)).await;
    assert_eq!(refused, Err(ConnectError::AuthenticationFailed));
    let message = refused.expect_err("refused").to_string();
    assert!(!message.contains("not-the-password"), "{message}");
    cell.close().await;
}

async fn ends_within(cell: &Cell, limit: Duration) {
    let busy = format!(
        "SELECT count(*) FROM pg_catalog.pg_stat_activity \
         WHERE application_name = '{}' AND state <> 'idle'",
        cell.app
    );
    let deadline = Instant::now() + limit;
    while cell.count(&busy).await > 0 {
        assert!(
            Instant::now() < deadline,
            "the server work outlived the scan"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_timeout_or_a_drop_ends_the_server_work() {
    let cell = Cell::open().await;
    let reader = Reader::new(&cell.settings(&[("read_timeout_ms", "300")]));
    let sleeps = "SELECT 1 AS one FROM pg_catalog.pg_sleep(20)";
    for _ in 0..3 {
        let timed_out = reader.read_query(sleeps).await.map(|_| ());
        let read = TimeoutSetting::Read;
        assert_eq!(timed_out, Err(ConnectError::Timeout { which: read }));
        ends_within(&cell, Duration::from_millis(400)).await;
    }

    let reader = Reader::new(&cell.settings(&[("batch_rows", "1000")]));
    let shown = "SELECT pg_catalog.current_setting('client_connection_check_interval') AS s";
    let shown = texts(&reader.read_query(shown).await.expect(LIVE));
    assert_eq!(shown, [Some("1s".to_string())]);
    let computes = "SELECT g::pg_catalog.int8 AS g \
                    FROM pg_catalog.generate_series(1, 10) g, pg_catalog.pg_sleep(20)";
    let resolved = reader
        .resolve(ScanSource::query(computes))
        .await
        .expect(LIVE);
    let mut stream = reader.open(ScanRequest::new(resolved));
    let first = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
    assert!(first.is_err(), "the server is still computing");
    let busy = format!(
        "SELECT count(*) FROM pg_catalog.pg_stat_activity \
         WHERE application_name = '{}' AND state = 'active'",
        cell.app
    );
    assert_eq!(cell.count(&busy).await, 1);
    drop(stream);
    ends_within(&cell, Duration::from_secs(3)).await;
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn query_mode_resolves_unqualified_names_through_the_role_search_path() {
    let cell = Cell::open().await;
    let table = format!("vq_{}", cell.schema);
    cell.sql(&format!(
        "CREATE TABLE public.{table} (x int4); INSERT INTO public.{table} VALUES (1), (2)"
    ))
    .await;
    let reader = Reader::new(&cell.settings(&[("pool_max_size", "1")]));
    let sql = format!("SELECT x FROM {table} WHERE x > 0");
    let unqualified = reader.read_query(&sql).await;
    let resolved = reader.resolve(ScanSource::query(&sql)).await;
    let pushed = resolved.map(|resolved| {
        ScanRequest::new(resolved)
            .compare(0, CompareOp::Eq, "2".to_string())
            .expect("a pushed value")
    });
    let pushed = match pushed {
        Ok(pushed) => reader.read(pushed).await,
        Err(error) => Err(error),
    };
    let shown = "SELECT pg_catalog.current_setting('search_path') AS s";
    let shown = texts(&reader.read_query(shown).await.expect(LIVE));
    let relation = reader.resolve(cell.relation("missing")).await;
    cell.sql(&format!("DROP TABLE public.{table}")).await;
    assert_eq!(int32s(&unqualified.expect(&sql)), [Some(1), Some(2)]);
    assert_eq!(int32s(&pushed.expect(&sql)), [Some(2)]);
    assert_eq!(shown, [Some("\"$user\", public".to_string())]);
    assert!(
        matches!(relation, Err(ConnectError::RelationNotFound { .. })),
        "{relation:?}"
    );
    cell.close().await;
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn a_user_operator_cannot_shadow_a_generated_compare() {
    let cell = Cell::open().await;
    let tag = &cell.schema;
    let objects = format!(
        "CREATE DOMAIN public.dom_{tag} AS int4; \
         CREATE FUNCTION public.never_{tag}(public.dom_{tag}, int4) RETURNS bool \
           LANGUAGE sql AS 'SELECT false'; \
         CREATE OPERATOR public.= (LEFTARG = public.dom_{tag}, RIGHTARG = int4, \
           FUNCTION = public.never_{tag}); \
         CREATE FUNCTION public.never_{tag}(int4, int4) RETURNS bool \
           LANGUAGE sql AS 'SELECT false'; \
         CREATE OPERATOR public.< (LEFTARG = int4, RIGHTARG = int4, \
           FUNCTION = public.never_{tag})"
    );
    cell.sql(&objects).await;
    let reader = Reader::new(&cell.settings(&[]));
    let sql = format!("SELECT 1::public.dom_{tag} AS d, 1 AS i");
    let resolved = reader.resolve(ScanSource::query(&sql)).await;
    let mut outcomes = Vec::new();
    for (column, op, value) in [(0, CompareOp::Eq, "1"), (1, CompareOp::Lt, "2")] {
        let pushed = match &resolved {
            Ok(resolved) => {
                let request = ScanRequest::new(Arc::clone(resolved));
                let request = request
                    .compare(column, op, value.to_string())
                    .expect("pushed");
                reader.read(request).await
            }
            Err(error) => Err(error.clone()),
        };
        outcomes
            .push(pushed.map(|batches| batches.iter().map(RecordBatch::num_rows).sum::<usize>()));
    }
    cell.sql(&format!(
        "DROP OPERATOR public.= (public.dom_{tag}, int4); DROP OPERATOR public.< (int4, int4); \
         DROP FUNCTION public.never_{tag}(public.dom_{tag}, int4); \
         DROP FUNCTION public.never_{tag}(int4, int4); DROP DOMAIN public.dom_{tag}"
    ))
    .await;
    assert_eq!(outcomes, [Ok(1), Ok(1)]);
    cell.close().await;
}

fn login(cell: &Cell, user: &str, password: &str, database: &str) -> PostgresSettings {
    let base = cell.settings(&[]);
    let props = BTreeMap::from([
        ("host".to_string(), base.host.clone()),
        ("port".to_string(), base.port.to_string()),
        ("database".to_string(), database.to_string()),
        ("user".to_string(), user.to_string()),
        ("password".to_string(), password.to_string()),
        ("sslmode".to_string(), "disable".to_string()),
        ("application_name".to_string(), cell.app.clone()),
    ]);
    PostgresSettings::from_props(&props, SettingsDoor::ReparkToml).expect("login settings")
}

async fn plain_session(settings: &PostgresSettings) -> Client {
    let mut config = tokio_postgres::Config::new();
    config
        .host(&settings.host)
        .port(settings.port)
        .user(&settings.user)
        .dbname(&settings.database)
        .application_name(&settings.application_name);
    if let Some(password) = &settings.password {
        config.password(password);
    }
    let (client, connection) = config.connect(NoTls).await.expect("a plain session");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn both_read(
    settings: &PostgresSettings,
) -> (Vec<Option<String>>, Vec<Option<String>>, usize) {
    let unqualified = "SELECT v, n FROM vt";
    let plain = plain_session(settings).await;
    let shown = plain.query(unqualified, &[]).await.expect(unqualified);
    let shown = shown.iter().map(|row| row.get(0)).collect();
    let reader = Reader::new(settings);
    let read = texts(&reader.read_query(unqualified).await.expect(unqualified));
    let resolved = reader.resolve(ScanSource::query(unqualified)).await;
    let pushed = ScanRequest::new(resolved.expect(unqualified))
        .compare(1, CompareOp::Eq, "1".to_string())
        .expect("a pushed int4");
    let pushed = reader.read(pushed).await.expect(unqualified);
    (shown, read, pushed.iter().map(RecordBatch::num_rows).sum())
}

#[tokio::test]
#[ignore = "live: make pg-up, REPARK_PG_URL"]
async fn query_mode_reads_the_configured_search_path_as_a_plain_session_does() {
    let cell = Cell::open().await;
    let role = format!("{}_r", cell.schema);
    let database = format!("{}_d", cell.schema);
    cell.sql(&format!("CREATE ROLE {role} LOGIN PASSWORD '{PASSWORD}'"))
        .await;
    cell.sql(&format!("CREATE DATABASE {database}")).await;
    let admin = cell.settings(&[]);
    let password = admin.password.clone().expect(LIVE);
    let as_admin = login(&cell, &admin.user, &password, &database);
    let as_role = login(&cell, &role, PASSWORD, &database);
    let owner = plain_session(&as_admin).await;
    let tables: Vec<String> = [
        ("app", "role"),
        ("db", "database"),
        ("roledb", "role in database"),
        ("public", "public"),
    ]
    .iter()
    .map(|(schema, label)| {
        format!(
            "CREATE SCHEMA IF NOT EXISTS {schema}; GRANT USAGE ON SCHEMA {schema} TO {role}; \
             CREATE TABLE {schema}.vt (v text, n int4); \
             INSERT INTO {schema}.vt VALUES ('{label}', 1), ('{label}', 2); \
             GRANT SELECT ON {schema}.vt TO {role}"
        )
    })
    .collect();
    owner
        .batch_execute(&tables.join("; "))
        .await
        .expect("the tables");
    cell.sql(&format!(
        "ALTER ROLE {role} SET search_path = app, public; \
         ALTER DATABASE {database} SET search_path = db, public"
    ))
    .await;
    let role_wins = both_read(&as_role).await;
    let database_only = both_read(&as_admin).await;
    cell.sql(&format!(
        "ALTER ROLE {role} IN DATABASE {database} SET search_path = roledb, public"
    ))
    .await;
    let role_in_database = both_read(&as_role).await;
    cell.sql(&format!(
        "ALTER ROLE {role} IN DATABASE {database} RESET search_path; \
         ALTER ROLE {role} RESET search_path; ALTER DATABASE {database} RESET search_path"
    ))
    .await;
    let built_in = both_read(&as_role).await;
    drop(owner);
    cell.sql(&format!("DROP DATABASE {database} WITH (FORCE)"))
        .await;
    cell.sql(&format!("DROP ROLE {role}")).await;
    for ((shown, read, pushed), expected) in [
        (role_wins, "role"),
        (database_only, "database"),
        (role_in_database, "role in database"),
        (built_in, "public"),
    ] {
        let expected = Some(expected.to_string());
        assert_eq!(
            shown,
            [expected.clone(), expected.clone()],
            "the plain session"
        );
        assert_eq!(read, shown, "query mode reads as the plain session");
        assert_eq!(pushed, 1, "{expected:?}");
    }
    cell.close().await;
}
