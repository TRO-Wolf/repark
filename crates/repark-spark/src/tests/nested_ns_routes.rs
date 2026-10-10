use super::super::*;
use super::common::*;
use super::wap_branch::set_wap;

const NESTED: &str = "CREATE TABLE ice.sales.t (id INT, st STRUCT<v: timestamp_ns, n: INT>, k INT) \
                      USING iceberg TBLPROPERTIES ('format-version'='3', \
                      'write.wap.enabled'='true')";
const PLAIN: &str = "CREATE TABLE ice.sales.p (id INT, st STRUCT<v: TIMESTAMP, n: INT>, k INT) \
                     USING iceberg TBLPROPERTIES ('format-version'='3', \
                     'write.wap.enabled'='true')";
const VALUE: &str = "named_struct('v', TIMESTAMP '2026-01-02 03:04:05.123456+00:00', 'n', 1)";
const NOT_YET: &str = "Cannot write incompatible data for the table `ice`.`sales`.`t`: Cannot \
                       safely cast `st`.`v` to \"TIMESTAMP_NS\". A nested timestamp_ns leaf is not \
                       writable yet";

async fn seeded(wh: &TempDir, ddl: &str, table: &str) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup_allow_create_format_version_3(wh).await;
    run(&ctx, &catalogs, ddl).await;
    let seed = format!("INSERT INTO ice.sales.{table} (id, k) VALUES (1, 0), (2, 0)");
    run(&ctx, &catalogs, &seed).await;
    let branch = format!("ALTER TABLE ice.sales.{table} CREATE BRANCH b1");
    run(&ctx, &catalogs, &branch).await;
    (ctx, catalogs)
}

async fn snapshots(catalogs: &CatalogRegistry, table: &str) -> usize {
    let ident = TableIdent::from_strs(["sales", table]).unwrap();
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

fn row_level_routes() -> Vec<Route> {
    let branch = "ice.sales.t.branch_b1";
    vec![
        (
            "update a branch with a where",
            None,
            None,
            format!("UPDATE {branch} SET st = {VALUE} WHERE id = 1"),
        ),
        (
            "update a branch with no where",
            None,
            None,
            format!("UPDATE {branch} SET st = {VALUE}"),
        ),
        (
            "merge update into a branch",
            None,
            None,
            format!(
                "MERGE INTO {branch} t USING (SELECT 1 AS id, {VALUE} AS st) s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET st = s.st"
            ),
        ),
        (
            "merge insert into a branch",
            None,
            None,
            format!(
                "MERGE INTO {branch} t USING (SELECT 11 AS id, {VALUE} AS st) s ON t.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.st)"
            ),
        ),
        (
            "insert overwrite a branch",
            None,
            None,
            format!("INSERT OVERWRITE {branch} SELECT 5, {VALUE}, 0"),
        ),
        (
            "merge under a wap branch",
            Some("b1"),
            None,
            format!(
                "MERGE INTO ice.sales.t t USING (SELECT 1 AS id, {VALUE} AS st) s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET st = s.st"
            ),
        ),
    ]
}

fn routes() -> Vec<Route> {
    let branch = "ice.sales.t.branch_b1";
    vec![
        (
            "insert into a branch",
            None,
            None,
            format!("INSERT INTO {branch} SELECT 5, {VALUE}, 0"),
        ),
        (
            "insert values into a branch",
            None,
            None,
            format!("INSERT INTO {branch} VALUES (5, {VALUE}, 0)"),
        ),
        (
            "insert into branch_main",
            None,
            None,
            format!("INSERT INTO ice.sales.t.branch_main SELECT 5, {VALUE}, 0"),
        ),
        (
            "insert a column list into a branch",
            None,
            None,
            format!("INSERT INTO {branch} (id, st) SELECT 5, {VALUE}"),
        ),
        (
            "a frame appended to a branch",
            None,
            None,
            format!(
                "INSERT INTO {branch} (id, st, k) SELECT id, st, k FROM \
                 (SELECT 5 AS id, {VALUE} AS st, 0 AS k) AS frame"
            ),
        ),
        (
            "insert under a wap id",
            None,
            Some("a1"),
            format!("INSERT INTO ice.sales.t SELECT 5, {VALUE}, 0"),
        ),
        (
            "insert values under a wap id",
            None,
            Some("a2"),
            format!("INSERT INTO ice.sales.t VALUES (5, {VALUE}, 0)"),
        ),
        (
            "insert under a wap branch",
            Some("b1"),
            None,
            format!("INSERT INTO ice.sales.t SELECT 5, {VALUE}, 0"),
        ),
        (
            "insert under a new wap branch",
            Some("fresh"),
            None,
            format!("INSERT INTO ice.sales.t (id, st) SELECT 5, {VALUE}"),
        ),
    ]
}

#[tokio::test]
async fn a_branch_or_wap_write_of_a_nested_nanosecond_leaf_is_refused_and_writes_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, NESTED, "t").await;
    let files = count_parquet_files(wh.path());
    let mut wrong = Vec::new();
    for (route, wap_branch, wap_id, write) in routes().into_iter().chain(row_level_routes()) {
        set_wap(&ctx, wap_branch, wap_id);
        let outcome = attempt(&ctx, &catalogs, &write).await;
        set_wap(&ctx, None, None);
        match outcome {
            Ok(()) => wrong.push(format!("{route}: stored")),
            Err(error) if !error.contains(NOT_YET) => wrong.push(format!("{route}: {error}")),
            Err(_) => {}
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
    assert_eq!(count_parquet_files(wh.path()), files);
    assert_eq!(snapshots(&catalogs, "t").await, 1);
}

#[tokio::test]
async fn a_frame_written_by_name_is_refused_by_the_name_of_its_column() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, NESTED, "t").await;
    let by_name = crate::write_options::StatementWriteOptions {
        source_by_name: true,
        ..crate::write_options::StatementWriteOptions::empty()
    };
    let write = format!("INSERT INTO ice.sales.t SELECT 5 AS id, NULL AS k, {VALUE} AS st");
    let read_only = std::collections::HashSet::<String>::new();
    let outcome = execute_with_statement_options(&ctx, &catalogs, &write, &read_only, &by_name);
    let refused = match outcome.await {
        Ok(frame) => frame.collect().await.map(|_| ()),
        Err(error) => Err(error),
    };
    let refused = refused.expect_err("refused").to_string();
    assert!(refused.contains(NOT_YET), "{refused}");
    assert_eq!(snapshots(&catalogs, "t").await, 1);
}

const ALLOWED: [(Option<&str>, Option<&str>, &str); 13] = [
    (
        None,
        None,
        "INSERT INTO ice.sales.t.branch_b1 (id, k) VALUES (7, 0)",
    ),
    (
        None,
        None,
        "INSERT INTO ice.sales.t.branch_b1 VALUES (8, NULL, 0)",
    ),
    (
        None,
        None,
        "INSERT INTO ice.sales.t.branch_main (id, k) SELECT 9, 0",
    ),
    (
        None,
        None,
        "MERGE INTO ice.sales.t.branch_b1 t USING (SELECT 1 AS id) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET k = 9",
    ),
    (
        None,
        None,
        "UPDATE ice.sales.t.branch_b1 SET k = 5 WHERE id = 1",
    ),
    (None, None, "DELETE FROM ice.sales.t.branch_b1 WHERE id = 2"),
    (
        Some("b1"),
        None,
        "INSERT INTO ice.sales.t (id, k) VALUES (10, 0)",
    ),
    (
        Some("b1"),
        None,
        "INSERT INTO ice.sales.t VALUES (11, NULL, 0)",
    ),
    (
        Some("b1"),
        None,
        "MERGE INTO ice.sales.t t USING (SELECT 1 AS id) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET k = 3",
    ),
    (
        Some("b1"),
        None,
        "UPDATE ice.sales.t SET k = 6 WHERE id = 1",
    ),
    (Some("b1"), None, "DELETE FROM ice.sales.t WHERE id = 7"),
    (
        None,
        Some("a1"),
        "INSERT INTO ice.sales.t (id, k) VALUES (12, 0)",
    ),
    (
        None,
        Some("a2"),
        "INSERT INTO ice.sales.t VALUES (13, NULL, 0)",
    ),
];

#[tokio::test]
async fn a_branch_or_wap_statement_that_does_not_supply_the_column_still_runs() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, NESTED, "t").await;
    for (index, (wap_branch, wap_id, write)) in ALLOWED.into_iter().enumerate() {
        set_wap(&ctx, wap_branch, wap_id);
        let outcome = attempt(&ctx, &catalogs, write).await;
        set_wap(&ctx, None, None);
        assert_eq!(outcome, Ok(()), "{write}");
        assert_eq!(snapshots(&catalogs, "t").await, index + 2, "{write}");
    }
}

#[tokio::test]
async fn a_branch_or_wap_write_to_a_table_with_no_such_leaf_is_not_gated() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PLAIN, "p").await;
    let writes = [
        (
            None,
            None,
            format!("INSERT INTO ice.sales.p.branch_b1 SELECT 5, {VALUE}, 0"),
        ),
        (
            Some("b1"),
            None,
            format!("INSERT INTO ice.sales.p SELECT 6, {VALUE}, 0"),
        ),
        (
            None,
            Some("a1"),
            format!("INSERT INTO ice.sales.p SELECT 7, {VALUE}, 0"),
        ),
    ];
    for (index, (wap_branch, wap_id, write)) in writes.into_iter().enumerate() {
        set_wap(&ctx, wap_branch, wap_id);
        let outcome = attempt(&ctx, &catalogs, &write).await;
        set_wap(&ctx, None, None);
        assert_eq!(outcome, Ok(()), "{write}");
        assert_eq!(snapshots(&catalogs, "p").await, index + 2, "{write}");
    }
    let on_branch = "SELECT id FROM ice.sales.p.branch_b1";
    assert_eq!(rows(&ctx, &catalogs, on_branch).await, 4);
    assert_eq!(rows(&ctx, &catalogs, "SELECT id FROM ice.sales.p").await, 2);
}

#[tokio::test]
async fn a_statement_the_router_rewrites_later_is_still_decided_on_its_target() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, NESTED, "t").await;
    let ident = TableIdent::from_strs(["sales", "t"]).unwrap();
    let loaded = catalogs["ice"].load_table(&ident).await.unwrap();
    let first = loaded.metadata().current_snapshot_id().unwrap();
    let travelled = format!("FROM ice.sales.t VERSION AS OF {first}");
    let refused = [
        format!("INSERT INTO ice.sales.t SELECT id + 20, st, k {travelled}"),
        format!("INSERT INTO ice.sales.t.branch_b1 SELECT id + 20, st, k {travelled}"),
        format!(
            "MERGE WITH SCHEMA EVOLUTION INTO ice.sales.t t USING (SELECT 1 AS id, {VALUE} AS st) \
             s ON t.id = s.id WHEN MATCHED THEN UPDATE SET st = s.st"
        ),
        format!("INSERT INTO ice.sales.t SELECT count(*), {VALUE}, 0 FROM ice.sales.t.snapshots"),
    ];
    for write in &refused {
        let outcome = attempt(&ctx, &catalogs, write).await.expect_err(write);
        assert!(outcome.contains(NOT_YET), "{write}: {outcome}");
    }
    assert_eq!(snapshots(&catalogs, "t").await, 1);
    let allowed = [
        format!("INSERT INTO ice.sales.t (id, k) SELECT id + 20, k {travelled}"),
        "INSERT INTO ice.sales.t (id, k) SELECT count(*) + 40, 0 FROM ice.sales.t.snapshots"
            .to_string(),
        "MERGE WITH SCHEMA EVOLUTION INTO ice.sales.t t USING (SELECT 1 AS id) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET k = 4"
            .to_string(),
    ];
    for (index, write) in allowed.iter().enumerate() {
        assert_eq!(attempt(&ctx, &catalogs, write).await, Ok(()), "{write}");
        assert_eq!(snapshots(&catalogs, "t").await, index + 2, "{write}");
    }
}

#[tokio::test]
async fn a_statement_the_gate_cannot_read_is_refused_for_such_a_table_only() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, NESTED, "t").await;
    let unreadable = format!("INSERT INTO ice.sales.t SELECT 5, {VALUE}, 0 FROM (");
    let outcome = attempt(&ctx, &catalogs, &unreadable)
        .await
        .expect_err("refused");
    assert!(outcome.contains(NOT_YET), "{outcome}");
    let elsewhere = attempt(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.absent SELECT 5 FROM (",
    )
    .await;
    assert!(!elsewhere.expect_err("a parser error").contains(NOT_YET));
}

fn wrapped_writes() -> Vec<String> {
    let table = "ice.sales.t";
    let insert = format!("INSERT INTO {table} SELECT 5, {VALUE}, 0");
    vec![
        format!("EXPLAIN ANALYZE {insert}"),
        format!("EXPLAIN ANALYZE INSERT INTO {table} (id, st) SELECT 5, {VALUE}"),
        format!("EXPLAIN ANALYZE INSERT INTO {table} VALUES (5, {VALUE}, 0)"),
        format!("EXPLAIN ANALYZE VERBOSE {insert}"),
        format!("EXPLAIN ANALYZE INSERT OVERWRITE {table} SELECT 5, {VALUE}, 0"),
        format!("explain analyze insert into {table} select 5, {VALUE}, 0"),
        format!("EXPLAIN ANALYZE UPDATE {table} SET st = {VALUE} WHERE id = 1"),
        format!("EXPLAIN ANALYZE UPDATE {table} SET st = {VALUE}"),
        format!("EXPLAIN ANALYZE EXPLAIN ANALYZE {insert}"),
        format!("EXPLAIN ANALYZE INSERT INTO {table}.branch_b1 SELECT 5, {VALUE}, 0"),
        format!("EXPLAIN ANALYZE INSERT INTO {table} BY NAME SELECT {VALUE} AS st, 5 AS id"),
        format!("PREPARE supplied AS {insert}"),
        format!("CREATE TABLE ice.sales.made USING iceberg AS {insert}"),
        format!("CREATE OR REPLACE TABLE ice.sales.made USING iceberg AS {insert}"),
        format!("WITH c AS (SELECT 5 AS id) INSERT INTO {table} SELECT id, {VALUE}, 0 FROM c"),
        format!("UPDATE {table} SET st = {VALUE} WHERE ("),
    ]
}

#[tokio::test]
async fn a_statement_that_wraps_a_write_is_decided_as_the_write_it_runs() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, NESTED, "t").await;
    let files = count_parquet_files(wh.path());
    let mut wrong = Vec::new();
    for write in wrapped_writes() {
        match attempt(&ctx, &catalogs, &write).await {
            Ok(()) => wrong.push(format!("{write}: ran")),
            Err(error) if !error.contains(NOT_YET) => wrong.push(format!("{write}: {error}")),
            Err(_) => {}
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
    assert_eq!(count_parquet_files(wh.path()), files);
    assert_eq!(snapshots(&catalogs, "t").await, 1);
    let planned = [
        format!("EXPLAIN INSERT INTO ice.sales.t SELECT 5, {VALUE}, 0"),
        format!("EXPLAIN VERBOSE INSERT INTO ice.sales.t SELECT 5, {VALUE}, 0"),
        format!("EXPLAIN ANALYZE EXPLAIN INSERT INTO ice.sales.t SELECT 5, {VALUE}, 0"),
    ];
    for explain in &planned {
        assert_eq!(attempt(&ctx, &catalogs, explain).await, Ok(()), "{explain}");
    }
    assert_eq!(count_parquet_files(wh.path()), files);
    assert_eq!(snapshots(&catalogs, "t").await, 1);
}

#[tokio::test]
async fn a_wrapped_statement_that_does_not_supply_the_column_still_runs() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, NESTED, "t").await;
    let allowed = [
        "EXPLAIN ANALYZE INSERT INTO ice.sales.t (id, k) VALUES (7, 0)",
        "EXPLAIN ANALYZE INSERT INTO ice.sales.t VALUES (8, NULL, 0)",
        "EXPLAIN ANALYZE UPDATE ice.sales.t SET k = 3 WHERE id = 1",
        "EXPLAIN ANALYZE VERBOSE INSERT INTO ice.sales.t (id, k) SELECT 9, 0",
    ];
    for (index, write) in allowed.into_iter().enumerate() {
        assert_eq!(attempt(&ctx, &catalogs, write).await, Ok(()), "{write}");
        assert_eq!(snapshots(&catalogs, "t").await, index + 2, "{write}");
    }
    let prepared = "PREPARE omitted AS INSERT INTO ice.sales.t (id, k) VALUES (10, 0)";
    assert_eq!(attempt(&ctx, &catalogs, prepared).await, Ok(()));
    assert_eq!(attempt(&ctx, &catalogs, "EXECUTE omitted").await, Ok(()));
    assert_eq!(snapshots(&catalogs, "t").await, 6);
}

#[tokio::test]
async fn a_wrapped_write_to_a_table_with_no_such_leaf_is_not_gated() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh, PLAIN, "p").await;
    let writes = [
        format!("EXPLAIN ANALYZE INSERT INTO ice.sales.p SELECT 5, {VALUE}, 0"),
        format!("EXPLAIN ANALYZE INSERT INTO ice.sales.p (id, st) VALUES (6, {VALUE})"),
        "EXPLAIN ANALYZE UPDATE ice.sales.p SET k = 3 WHERE id = 1".to_string(),
    ];
    for (index, write) in writes.iter().enumerate() {
        assert_eq!(attempt(&ctx, &catalogs, write).await, Ok(()), "{write}");
        assert_eq!(snapshots(&catalogs, "p").await, index + 2, "{write}");
    }
    let prepared = format!("PREPARE plain AS INSERT INTO ice.sales.p SELECT 7, {VALUE}, 0");
    assert_eq!(attempt(&ctx, &catalogs, &prepared).await, Ok(()));
    assert_eq!(attempt(&ctx, &catalogs, "EXECUTE plain").await, Ok(()));
    assert_eq!(snapshots(&catalogs, "p").await, 5);
    let malformed = attempt(&ctx, &catalogs, "UPDATE ice.sales.p SET k = 1 WHERE (").await;
    assert!(
        !malformed
            .expect_err("a parser error")
            .contains("not writable yet")
    );
}
