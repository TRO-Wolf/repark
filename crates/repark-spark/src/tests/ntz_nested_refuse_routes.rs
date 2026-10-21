use repark_functions::session_time_zone::SessionTimeZoneConfig;

use super::super::*;
use super::common::*;
use super::wap_branch::set_wap;

const NESTED: &str = "CREATE TABLE ice.sales.t (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT) \
                      USING iceberg TBLPROPERTIES ('format-version'='2', \
                      'write.wap.enabled'='true')";
const INSTANT: &str = "named_struct('v', TIMESTAMP '2026-01-02 03:04:05.123456+00:00', 'n', 1)";
const WALL: &str = "named_struct('v', TIMESTAMP_NTZ '2026-01-02 03:04:05.123456', 'n', 1)";
const REFUSED: &str = "Cannot write incompatible data for the table `ice`.`sales`.`t`: Cannot \
                       safely cast `st`.`v` to \"TIMESTAMP_NTZ\". A value that is not provably \
                       a TIMESTAMP_NTZ wall time is not written into a nested TIMESTAMP_NTZ \
                       field";

fn set_zone(ctx: &SessionContext, zone: &str) {
    let mut carrier = SessionTimeZoneConfig::default();
    carrier.set_zone(zone);
    let state_lock = ctx.state_ref();
    let mut state = state_lock.write();
    state.config_mut().options_mut().extensions.insert(carrier);
}

async fn seeded(wh: &TempDir, zone: &str) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    repark_functions::register_all(&ctx);
    run(&ctx, &catalogs, NESTED).await;
    let seed = "INSERT INTO ice.sales.t (id, k) VALUES (1, 0), (2, 0)";
    run(&ctx, &catalogs, seed).await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t CREATE BRANCH b1").await;
    set_zone(&ctx, zone);
    (ctx, catalogs)
}

async fn snapshots(catalogs: &CatalogRegistry) -> usize {
    let ident = TableIdent::from_strs(["sales", "t"]).unwrap();
    let loaded = catalogs["ice"].load_table(&ident).await.unwrap();
    loaded.metadata().snapshots().count()
}

async fn attempt(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<(), String> {
    match execute(ctx, catalogs, sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

type Route = (
    &'static str,
    Option<&'static str>,
    Option<&'static str>,
    String,
);

fn routes(value: &str) -> Vec<Route> {
    let branch = "ice.sales.t.branch_b1";
    vec![
        (
            "insert into a branch",
            None,
            None,
            format!("INSERT INTO {branch} SELECT 5, {value}, 0"),
        ),
        (
            "insert values into branch_main",
            None,
            None,
            format!("INSERT INTO ice.sales.t.branch_main VALUES (5, {value}, 0)"),
        ),
        (
            "update a branch",
            None,
            None,
            format!("UPDATE {branch} SET st = {value} WHERE id = 1"),
        ),
        (
            "merge into a branch",
            None,
            None,
            format!(
                "MERGE INTO {branch} t USING (SELECT 1 AS id, {value} AS st) s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET t.st = s.st \
                 WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.st)"
            ),
        ),
        (
            "insert under a wap id",
            None,
            Some("a1"),
            format!("INSERT INTO ice.sales.t SELECT 5, {value}, 0"),
        ),
        (
            "insert under a wap branch",
            Some("b1"),
            None,
            format!("INSERT INTO ice.sales.t (id, st) SELECT 5, {value}"),
        ),
        (
            "merge under a wap branch",
            Some("b1"),
            None,
            format!(
                "MERGE INTO ice.sales.t t USING (SELECT 1 AS id, {value} AS st) s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET st = s.st"
            ),
        ),
    ]
}

#[tokio::test]
async fn a_branch_or_wap_write_of_an_instant_refuses_and_writes_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, "America/New_York").await;
    let files = count_parquet_files(wh.path());
    let mut wrong = Vec::new();
    for (route, wap_branch, wap_id, write) in routes(INSTANT) {
        set_wap(&ctx, wap_branch, wap_id);
        let outcome = attempt(&ctx, &catalogs, &write).await;
        set_wap(&ctx, None, None);
        match outcome {
            Ok(()) => wrong.push(format!("{route}: stored")),
            Err(error) if !error.contains(REFUSED) => wrong.push(format!("{route}: {error}")),
            Err(_) => {}
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
    assert_eq!(count_parquet_files(wh.path()), files);
    assert_eq!(snapshots(&catalogs).await, 1);
}

#[tokio::test]
async fn a_branch_or_wap_write_of_a_wall_passes_the_gate() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, "America/New_York").await;
    for (route, wap_branch, wap_id, write) in routes(WALL) {
        set_wap(&ctx, wap_branch, wap_id);
        let outcome = attempt(&ctx, &catalogs, &write).await;
        set_wap(&ctx, None, None);
        match (route, outcome) {
            ("update a branch", Err(error)) => {
                assert!(
                    error.contains("arguments need to have the same data type"),
                    "{error}"
                );
            }
            (route, outcome) => assert_eq!(outcome, Ok(()), "{route}"),
        }
    }
}

#[tokio::test]
async fn a_frame_written_by_name_is_decided_by_the_column_of_that_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, "Asia/Kolkata").await;
    let by_name = crate::write_options::StatementWriteOptions {
        source_by_name: true,
        ..crate::write_options::StatementWriteOptions::empty()
    };
    let read_only = std::collections::HashSet::<String>::new();
    for (value, refused) in [(INSTANT, true), (WALL, false)] {
        let write = format!("INSERT INTO ice.sales.t SELECT {value} AS st, 5 AS id, NULL AS k");
        let outcome = execute_with_statement_options(&ctx, &catalogs, &write, &read_only, &by_name);
        let outcome = match outcome.await {
            Ok(frame) => frame.collect().await.map(|_| ()),
            Err(error) => Err(error),
        }
        .map_err(|error| error.to_string());
        let gated = outcome.as_ref().is_err_and(|error| error.contains(REFUSED));
        assert_eq!(gated, refused, "{value}: {outcome:?}");
    }
}

fn wrapped_writes() -> Vec<String> {
    let table = "ice.sales.t";
    let insert = format!("INSERT INTO {table} SELECT 5, {INSTANT}, 0");
    vec![
        format!("EXPLAIN ANALYZE {insert}"),
        format!("EXPLAIN ANALYZE VERBOSE {insert}"),
        format!("EXPLAIN ANALYZE UPDATE {table} SET st = {INSTANT} WHERE id = 1"),
        format!("EXPLAIN ANALYZE INSERT INTO {table}.branch_b1 SELECT 5, {INSTANT}, 0"),
        format!("PREPARE supplied AS {insert}"),
        format!("CREATE TABLE ice.sales.made USING iceberg AS {insert}"),
        format!("WITH c AS (SELECT 5 AS id) INSERT INTO {table} SELECT id, {INSTANT}, 0 FROM c"),
        format!("UPDATE {table} SET st = {INSTANT} WHERE ("),
    ]
}

#[tokio::test]
async fn a_statement_that_wraps_a_write_of_an_instant_is_decided_as_that_write() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, "America/New_York").await;
    let files = count_parquet_files(wh.path());
    let mut wrong = Vec::new();
    for write in wrapped_writes() {
        match attempt(&ctx, &catalogs, &write).await {
            Ok(()) => wrong.push(format!("{write}: ran")),
            Err(error) if !error.contains(REFUSED) => wrong.push(format!("{write}: {error}")),
            Err(_) => {}
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
    assert_eq!(count_parquet_files(wh.path()), files);
    assert_eq!(snapshots(&catalogs).await, 1);
    let planned = format!("EXPLAIN INSERT INTO ice.sales.t SELECT 5, {INSTANT}, 0");
    assert_eq!(attempt(&ctx, &catalogs, &planned).await, Ok(()));
}

#[tokio::test]
async fn a_prepared_write_of_an_instant_refuses_in_a_utc_session_too() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, "UTC").await;
    let zoned = format!("PREPARE zoned AS INSERT INTO ice.sales.t SELECT 5, {INSTANT}, 0");
    let refused = attempt(&ctx, &catalogs, &zoned).await.expect_err("refused");
    assert!(refused.contains(REFUSED), "{refused}");
    let wall = format!("PREPARE walled AS INSERT INTO ice.sales.t SELECT 6, {WALL}, 0");
    assert_eq!(attempt(&ctx, &catalogs, &wall).await, Ok(()));
    set_zone(&ctx, "America/New_York");
    assert_eq!(attempt(&ctx, &catalogs, "EXECUTE walled").await, Ok(()));
    assert_eq!(snapshots(&catalogs).await, 2);
}

#[tokio::test]
async fn only_a_utc_zone_lets_an_instant_through() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, "UTC").await;
    let write = format!("INSERT INTO ice.sales.t SELECT 5, {INSTANT}, 0");
    let mut stored = 1;
    for zone in [
        "UTC",
        "Z",
        "GMT",
        "UT",
        "+00:00",
        "-00:00",
        "GMT+0",
        "UTC+00:00",
        "Etc/UTC",
        "Etc/GMT",
        "Etc/Universal",
        "Etc/Zulu",
        "Zulu",
        "GMT0",
    ] {
        set_zone(&ctx, zone);
        assert_eq!(attempt(&ctx, &catalogs, &write).await, Ok(()), "{zone}");
        stored += 1;
        assert_eq!(snapshots(&catalogs).await, stored, "{zone}");
    }
    for zone in [
        "Africa/Abidjan",
        "Atlantic/Reykjavik",
        "Europe/London",
        "+01:00",
        "-05:00",
        "America/New_York",
    ] {
        set_zone(&ctx, zone);
        let refused = attempt(&ctx, &catalogs, &write).await.expect_err(zone);
        assert!(refused.contains(REFUSED), "{zone}: {refused}");
    }
    assert_eq!(snapshots(&catalogs).await, stored);
}
