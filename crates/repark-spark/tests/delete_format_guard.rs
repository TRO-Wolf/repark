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
    let files_before = files_under(warehouse.path()).len();
    let rows_before = ids(session).await;
    let error = attempt(session, sql)
        .await
        .expect_err("an unreadable delete format must refuse");
    assert!(error.contains(DELETE_FORMAT_PROPERTY), "{error}");
    assert!(error.contains("'parquet'"), "{error}");
    assert!(error.contains("before any file is written"), "{error}");
    assert_eq!(ids(session).await, rows_before);
    let snapshot = current_snapshot(session).await;
    assert_eq!(snapshot.operation, Operation::Append);
    assert_eq!(snapshot.total("total-delete-files"), 0);
    assert_eq!(
        files_under(warehouse.path()).len(),
        files_before,
        "`{sql}` wrote a file"
    );
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
