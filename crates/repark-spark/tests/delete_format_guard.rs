use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::record_batch::RecordBatch;
use iceberg::TableIdent;
use iceberg::spec::Operation;
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const FORK_ROUTE_DELETE: &str = "DELETE FROM ice.ns.t WHERE id BETWEEN 1 AND 3";
const REPARK_ROUTE_DELETE: &str = "DELETE FROM ice.ns.t WHERE id >= 1 AND id < 4";
const FORK_ROUTE_UPDATE: &str = "UPDATE ice.ns.t SET id = id + 10 WHERE id BETWEEN 1 AND 3";
const REPARK_ROUTE_UPDATE: &str = "UPDATE ice.ns.t SET id = id + 10 WHERE id >= 1 AND id < 4";
const IN_LIST_DELETE: &str = "DELETE FROM ice.ns.t WHERE id IN (1, 2, 3)";
const MERGE_DELETE_AND_INSERT: &str = "MERGE INTO ice.ns.t t \
     USING (SELECT 2 AS id UNION ALL SELECT 9 AS id) s ON t.id = s.id \
     WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)";
const DELETE_FORMAT_PROPERTY: &str = "write.delete.format.default";

struct Snapshot {
    operation: Operation,
    properties: HashMap<String, String>,
}

impl Snapshot {
    fn total(&self, key: &str) -> u64 {
        self.properties
            .get(key)
            .map_or(0, |value| value.parse().expect("numeric summary value"))
    }
}

fn session() -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .build()
        .expect("session")
}

async fn attempt(session: &ReparkSession, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let frame = session.sql(sql).await.map_err(|error| error.to_string())?;
    frame.collect().await.map_err(|error| error.to_string())
}

async fn run(session: &ReparkSession, sql: &str) {
    attempt(session, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` must succeed: {error}"));
}

async fn seeded(columns: &str, properties: &str, rows: &str) -> (ReparkSession, TempDir) {
    let warehouse = TempDir::new().expect("warehouse");
    let session = session();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    run(
        &session,
        &format!(
            "CREATE TABLE ice.ns.t ({columns}) USING iceberg TBLPROPERTIES ('format-version'='2', \
             'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', \
             'write.merge.mode'='merge-on-read'{properties})"
        ),
    )
    .await;
    run(&session, &format!("INSERT INTO ice.ns.t VALUES {rows}")).await;
    (session, warehouse)
}

async fn seeded_table(properties: &str) -> (ReparkSession, TempDir) {
    seeded("id INT", properties, "(0), (1), (2), (3), (4)").await
}

fn data_format(format: &str) -> String {
    format!(", 'write.format.default'='{format}'")
}

async fn ids(session: &ReparkSession) -> Result<Vec<i32>, String> {
    let batches = attempt(session, "SELECT id FROM ice.ns.t ORDER BY id").await?;
    Ok(batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .to_vec()
        })
        .collect())
}

async fn current_snapshot(session: &ReparkSession) -> Snapshot {
    let catalogs = session.catalogs_snapshot();
    let catalog = catalogs.get("ice").expect("ice catalog");
    let table = catalog
        .load_table(&TableIdent::from_strs(["ns", "t"]).expect("ident"))
        .await
        .expect("load table");
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("current snapshot");
    Snapshot {
        operation: snapshot.summary().operation.clone(),
        properties: snapshot.summary().additional_properties.clone(),
    }
}

fn files_under(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).expect("listing") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found
}

fn position_delete_extensions(warehouse: &TempDir) -> Vec<String> {
    files_under(warehouse.path())
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains("pos-del"))
        })
        .map(|path| {
            path.extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

async fn assert_parquet_position_delete(session: &ReparkSession, warehouse: &TempDir, sql: &str) {
    assert_parquet_rewrite(session, warehouse, sql, &[0, 4], 3).await;
    assert_eq!(current_snapshot(session).await.operation, Operation::Delete);
}

async fn assert_parquet_rewrite(
    session: &ReparkSession,
    warehouse: &TempDir,
    sql: &str,
    survivors: &[i32],
    position_deletes: u64,
) {
    let outcome = attempt(session, sql).await;
    assert!(outcome.is_ok(), "`{sql}` must complete: {outcome:?}");
    assert_eq!(
        ids(session).await,
        Ok(survivors.to_vec()),
        "`{sql}` survivors"
    );
    let snapshot = current_snapshot(session).await;
    assert_eq!(snapshot.total("total-delete-files"), 1);
    assert_eq!(snapshot.total("total-position-deletes"), position_deletes);
    assert_eq!(position_delete_extensions(warehouse), vec!["parquet"]);
}

async fn assert_refused_unchanged(session: &ReparkSession, warehouse: &TempDir, sql: &str) {
    let error = assert_fails_unchanged(session, warehouse, sql).await;
    assert!(error.contains(DELETE_FORMAT_PROPERTY), "{error}");
    assert!(error.contains("'parquet'"), "{error}");
    assert!(error.contains("before any file is written"), "{error}");
}

async fn assert_fails_unchanged(session: &ReparkSession, warehouse: &TempDir, sql: &str) -> String {
    let files_before = files_under(warehouse.path()).len();
    let rows_before = ids(session).await;
    let error = attempt(session, sql)
        .await
        .expect_err("an unreadable delete format must refuse");
    assert_eq!(ids(session).await, rows_before);
    let snapshot = current_snapshot(session).await;
    assert_eq!(snapshot.operation, Operation::Append);
    assert_eq!(snapshot.total("total-delete-files"), 0);
    assert_eq!(
        files_under(warehouse.path()).len(),
        files_before,
        "`{sql}` wrote a file"
    );
    error
}

fn explicit_delete_format(data: &str, delete: &str) -> String {
    format!(
        "{}, '{DELETE_FORMAT_PROPERTY}'='{delete}'",
        data_format(data)
    )
}

#[tokio::test]
async fn inherited_avro_or_orc_repark_route_delete_writes_a_parquet_position_delete() {
    for format in ["avro", "orc"] {
        let (session, warehouse) = seeded_table(&data_format(format)).await;
        assert_parquet_position_delete(&session, &warehouse, REPARK_ROUTE_DELETE).await;
    }
}

#[tokio::test]
async fn inherited_avro_or_orc_repark_route_update_writes_a_parquet_position_delete() {
    for format in ["avro", "orc"] {
        let (session, warehouse) = seeded_table(&data_format(format)).await;
        let survivors = [0, 4, 11, 12, 13];
        assert_parquet_rewrite(&session, &warehouse, REPARK_ROUTE_UPDATE, &survivors, 3).await;
    }
}

#[tokio::test]
async fn inherited_avro_or_orc_fork_route_dml_refuses_unchanged() {
    for format in ["avro", "orc"] {
        for sql in [FORK_ROUTE_DELETE, IN_LIST_DELETE, FORK_ROUTE_UPDATE] {
            let (session, warehouse) = seeded_table(&data_format(format)).await;
            assert_refused_unchanged(&session, &warehouse, sql).await;
        }
    }
    for sql in [
        "DELETE FROM ice.ns.t WHERE s IS NULL",
        "DELETE FROM ice.ns.t WHERE s IS NULL OR id = 2",
    ] {
        let (session, warehouse) = seeded(
            "id INT, s STRUCT<a: INT>",
            &data_format("avro"),
            "(0, named_struct('a', 0)), (1, named_struct('a', 1)), (2, named_struct('a', 2)), \
             (3, named_struct('a', 3)), (4, named_struct('a', 4))",
        )
        .await;
        assert_refused_unchanged(&session, &warehouse, sql).await;
    }
}

#[tokio::test]
async fn inherited_avro_or_orc_merge_refuses_before_any_data_file() {
    for format in ["avro", "orc"] {
        let (session, warehouse) = seeded_table(&data_format(format)).await;
        assert_refused_unchanged(&session, &warehouse, MERGE_DELETE_AND_INSERT).await;
    }
}

#[tokio::test]
async fn explicit_avro_or_orc_delete_format_refuses_every_route() {
    for (data, delete) in [("parquet", "avro"), ("orc", "orc")] {
        for sql in [
            FORK_ROUTE_DELETE,
            REPARK_ROUTE_DELETE,
            FORK_ROUTE_UPDATE,
            REPARK_ROUTE_UPDATE,
            MERGE_DELETE_AND_INSERT,
        ] {
            let (session, warehouse) = seeded_table(&explicit_delete_format(data, delete)).await;
            assert_refused_unchanged(&session, &warehouse, sql).await;
        }
    }
}

#[tokio::test]
async fn explicit_parquet_delete_format_on_avro_data_is_readable() {
    for sql in [FORK_ROUTE_DELETE, REPARK_ROUTE_DELETE] {
        let (session, warehouse) = seeded_table(&explicit_delete_format("avro", "parquet")).await;
        assert_parquet_position_delete(&session, &warehouse, sql).await;
    }
}

#[tokio::test]
async fn parquet_control_is_unchanged() {
    for sql in [FORK_ROUTE_DELETE, REPARK_ROUTE_DELETE, IN_LIST_DELETE] {
        let (session, warehouse) = seeded_table(&data_format("parquet")).await;
        assert_parquet_position_delete(&session, &warehouse, sql).await;
    }
    let (session, warehouse) = seeded_table(&data_format("parquet")).await;
    assert_parquet_rewrite(
        &session,
        &warehouse,
        MERGE_DELETE_AND_INSERT,
        &[0, 1, 3, 4, 9],
        1,
    )
    .await;
}

const CASE_VARIANTS: [&str; 3] = ["ICE.NS.T", "ice.NS.t", "ice.ns.T"];
const BRANCH_TARGET: &str = "ice.ns.t.branch_b";
const BRANCH_SHAPES: [&str; 4] = [
    FORK_ROUTE_DELETE,
    REPARK_ROUTE_DELETE,
    FORK_ROUTE_UPDATE,
    REPARK_ROUTE_UPDATE,
];

fn on_target(sql: &str, target: &str) -> String {
    sql.replacen("ice.ns.t", target, 1)
}

async fn branch_head(session: &ReparkSession) -> Option<i64> {
    let catalogs = session.catalogs_snapshot();
    let catalog = catalogs.get("ice").expect("ice catalog");
    let table = catalog
        .load_table(&TableIdent::from_strs(["ns", "t"]).expect("ident"))
        .await
        .expect("load table");
    table
        .metadata()
        .snapshot_for_ref("b")
        .map(|snapshot| snapshot.snapshot_id())
}

fn set_session_write_codec(session: &ReparkSession) {
    let state = session.context().state_ref();
    let mut guard = state.write();
    assert!(repark_iceberg::write::apply_session_write_key(
        guard.config_mut().options_mut(),
        "spark.sql.iceberg.compression-codec",
        "zstd",
    ));
}

async fn branched_table(format: &str) -> (ReparkSession, TempDir, Option<i64>) {
    let (session, warehouse) = seeded_table(&data_format(format)).await;
    run(&session, "ALTER TABLE ice.ns.t CREATE BRANCH b").await;
    let head = branch_head(&session).await;
    assert!(head.is_some(), "branch b exists");
    (session, warehouse, head)
}

#[tokio::test]
async fn case_variant_dml_on_inherited_avro_or_orc_refuses_unchanged() {
    for format in ["avro", "orc"] {
        for target in CASE_VARIANTS {
            for sql in [FORK_ROUTE_DELETE, REPARK_ROUTE_DELETE, FORK_ROUTE_UPDATE] {
                let (session, warehouse) = seeded_table(&data_format(format)).await;
                assert_refused_unchanged(&session, &warehouse, &on_target(sql, target)).await;
            }
        }
    }
}

#[tokio::test]
async fn case_variant_explicit_avro_delete_format_refuses() {
    for target in CASE_VARIANTS {
        for sql in [FORK_ROUTE_DELETE, REPARK_ROUTE_DELETE, FORK_ROUTE_UPDATE] {
            let (session, warehouse) =
                seeded_table(&explicit_delete_format("parquet", "avro")).await;
            assert_refused_unchanged(&session, &warehouse, &on_target(sql, target)).await;
        }
    }
}

#[tokio::test]
async fn case_variant_merge_refuses_like_the_lowercase_spelling() {
    for target in CASE_VARIANTS {
        let (session, warehouse) = seeded_table(&data_format("avro")).await;
        let sql = on_target(MERGE_DELETE_AND_INSERT, target);
        assert_refused_unchanged(&session, &warehouse, &sql).await;
    }
}

#[tokio::test]
async fn branch_target_dml_on_inherited_avro_or_orc_refuses_with_the_branch_unchanged() {
    for format in ["avro", "orc"] {
        for sql in BRANCH_SHAPES {
            let (session, warehouse, head) = branched_table(format).await;
            assert_refused_unchanged(&session, &warehouse, &on_target(sql, BRANCH_TARGET)).await;
            assert_eq!(branch_head(&session).await, head, "`{sql}` moved branch b");
        }
    }
}

#[tokio::test]
async fn branch_target_dml_under_a_session_write_conf_refuses_with_the_branch_unchanged() {
    for sql in BRANCH_SHAPES {
        let (session, warehouse, head) = branched_table("avro").await;
        set_session_write_codec(&session);
        assert_refused_unchanged(&session, &warehouse, &on_target(sql, BRANCH_TARGET)).await;
        assert_eq!(branch_head(&session).await, head, "`{sql}` moved branch b");
    }
}

#[tokio::test]
async fn case_variant_branch_target_dml_fails_with_the_branch_unchanged() {
    for target in [
        "ICE.NS.T.branch_b",
        "ice.NS.t.branch_b",
        "ice.ns.T.branch_b",
    ] {
        for sql in BRANCH_SHAPES {
            let (session, warehouse, head) = branched_table("avro").await;
            assert_fails_unchanged(&session, &warehouse, &on_target(sql, target)).await;
            assert_eq!(branch_head(&session).await, head, "`{sql}` moved branch b");
        }
    }
}

#[tokio::test]
async fn branch_target_merge_on_inherited_avro_refuses_with_the_branch_unchanged() {
    let (session, warehouse, head) = branched_table("avro").await;
    let sql = on_target(MERGE_DELETE_AND_INSERT, BRANCH_TARGET);
    assert_refused_unchanged(&session, &warehouse, &sql).await;
    assert_eq!(branch_head(&session).await, head);
}

#[tokio::test]
async fn parquet_branch_and_case_variant_dml_is_unchanged() {
    for sql in [FORK_ROUTE_DELETE, REPARK_ROUTE_DELETE] {
        let (session, warehouse) = seeded_table(&data_format("parquet")).await;
        assert_parquet_position_delete(&session, &warehouse, &on_target(sql, "ICE.NS.T")).await;
    }
    let (session, _warehouse, head) = branched_table("parquet").await;
    run(&session, &on_target(FORK_ROUTE_DELETE, BRANCH_TARGET)).await;
    assert_ne!(
        branch_head(&session).await,
        head,
        "the branch delete commits"
    );
    let rows = attempt(&session, "SELECT id FROM ice.ns.t.branch_b ORDER BY id").await;
    assert!(rows.is_ok(), "the branch reads back: {rows:?}");
}

const COMMENT_PREFIXES: [&str; 2] = [concat!("/", "* c *", "/ "), "-- c\n"];
const BUG_001_TEXT: &str = "partition specs in history";

fn comment_prefixed(prefix: &str, sql: &str, target: &str) -> String {
    format!("{prefix}{}", on_target(sql, target))
}

async fn branched_with(properties: &str) -> (ReparkSession, TempDir, Option<i64>) {
    let (session, warehouse) = seeded_table(properties).await;
    run(&session, "ALTER TABLE ice.ns.t CREATE BRANCH b").await;
    let head = branch_head(&session).await;
    assert!(head.is_some(), "branch b exists");
    (session, warehouse, head)
}

fn set_session_wap_branch(session: &ReparkSession, branch: &str) {
    let state = session.context().state_ref();
    let mut guard = state.write();
    guard
        .config_mut()
        .options_mut()
        .extensions
        .insert(repark_spark::wap::WapSessionConfig {
            branch: Some(branch.to_string()),
            id: None,
        });
}

fn wap_enabled(format: &str) -> String {
    format!("{}, 'write.wap.enabled'='true'", data_format(format))
}

#[tokio::test]
async fn comment_prefixed_branch_dml_on_an_avro_delete_format_refuses_with_the_branch_unchanged() {
    for properties in [
        data_format("avro"),
        explicit_delete_format("parquet", "avro"),
    ] {
        for prefix in COMMENT_PREFIXES {
            for target in [BRANCH_TARGET, "t.branch_b"] {
                for sql in BRANCH_SHAPES {
                    let (session, warehouse, head) = branched_with(&properties).await;
                    if target == "t.branch_b" {
                        run(&session, "SET datafusion.catalog.default_catalog = 'ice'").await;
                        run(&session, "SET datafusion.catalog.default_schema = 'ns'").await;
                    }
                    let statement = comment_prefixed(prefix, sql, target);
                    assert_refused_unchanged(&session, &warehouse, &statement).await;
                    assert_eq!(
                        branch_head(&session).await,
                        head,
                        "`{statement}` moved branch b"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn comment_prefixed_dml_under_a_session_wap_branch_refuses_with_the_branch_unchanged() {
    for prefix in COMMENT_PREFIXES {
        for sql in BRANCH_SHAPES {
            let (session, warehouse, head) = branched_with(&wap_enabled("avro")).await;
            set_session_wap_branch(&session, "b");
            let statement = format!("{prefix}{sql}");
            assert_refused_unchanged(&session, &warehouse, &statement).await;
            assert_eq!(
                branch_head(&session).await,
                head,
                "`{statement}` moved branch b"
            );
        }
    }
}

#[tokio::test]
async fn refused_dml_under_a_session_wap_branch_creates_no_branch() {
    for prefix in ["", COMMENT_PREFIXES[0], COMMENT_PREFIXES[1]] {
        for sql in BRANCH_SHAPES {
            let (session, warehouse) = seeded_table(&wap_enabled("avro")).await;
            set_session_wap_branch(&session, "b");
            let statement = format!("{prefix}{sql}");
            assert_refused_unchanged(&session, &warehouse, &statement).await;
            assert_eq!(
                branch_head(&session).await,
                None,
                "`{statement}` created branch b"
            );
        }
    }
}

async fn multi_spec_unpartitioned_branched_table() -> (ReparkSession, TempDir, Option<i64>) {
    let warehouse = TempDir::new().expect("warehouse");
    let session = session();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id INT) USING iceberg PARTITIONED BY (bucket(4, id)) \
         TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read', \
         'write.update.mode'='merge-on-read')",
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.t VALUES (0), (1), (2), (3), (4)",
    )
    .await;
    run(
        &session,
        "ALTER TABLE ice.ns.t DROP PARTITION FIELD bucket(4, id)",
    )
    .await;
    run(&session, "ALTER TABLE ice.ns.t CREATE BRANCH b").await;
    let head = branch_head(&session).await;
    assert!(head.is_some(), "branch b exists");
    (session, warehouse, head)
}

#[tokio::test]
async fn comment_prefixed_branch_dml_on_a_multi_spec_unpartitioned_table_hits_bug_001() {
    for prefix in ["", COMMENT_PREFIXES[0], COMMENT_PREFIXES[1]] {
        for sql in BRANCH_SHAPES {
            let (session, warehouse, head) = multi_spec_unpartitioned_branched_table().await;
            let files_before = files_under(warehouse.path()).len();
            let statement = comment_prefixed(prefix, sql, BRANCH_TARGET);
            let outcome = attempt(&session, &statement).await;
            let Err(error) = outcome else {
                panic!("`{statement}` must hit the BUG-001 valve: {outcome:?}");
            };
            assert!(error.contains(BUG_001_TEXT), "`{statement}`: {error}");
            assert_eq!(
                branch_head(&session).await,
                head,
                "`{statement}` moved branch b"
            );
            assert_eq!(
                files_under(warehouse.path()).len(),
                files_before,
                "`{statement}` wrote a file"
            );
        }
    }
}

#[tokio::test]
async fn insert_into_a_missing_namespace_keeps_the_planner_not_found_error() {
    let (session, _warehouse) = seeded_table(&data_format("parquet")).await;
    let error = attempt(&session, "INSERT INTO ice.missing.t VALUES (1)")
        .await
        .expect_err("a missing namespace must fail");
    assert!(!error.contains("cannot load the write target"), "{error}");
    assert!(error.contains("table 'ice.missing.t' not found"), "{error}");
}

#[tokio::test]
async fn comment_prefixed_branch_delete_on_parquet_still_commits() {
    for prefix in COMMENT_PREFIXES {
        let (session, _warehouse, head) = branched_table("parquet").await;
        run(
            &session,
            &comment_prefixed(prefix, FORK_ROUTE_DELETE, BRANCH_TARGET),
        )
        .await;
        assert_ne!(
            branch_head(&session).await,
            head,
            "the branch delete commits"
        );
        let rows = attempt(&session, "SELECT id FROM ice.ns.t.branch_b ORDER BY id").await;
        assert!(rows.is_ok(), "the branch reads back: {rows:?}");
    }
}
