use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::record_batch::RecordBatch;
use iceberg::TableIdent;
use iceberg::spec::Operation;
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const BATCH_ROWS_PROPERTY: &str = "write.delete.position-delete-batch-rows";
const MARCH_ROWS: i32 = 120;
const APRIL_IDS: std::ops::Range<i32> = 200..220;
const RANGE_DELETE: &str =
    "DELETE FROM ice.ns.t WHERE d BETWEEN DATE '2026-03-05' AND DATE '2026-03-14'";
const BINARY_OP_RANGE_DELETE: &str =
    "DELETE FROM ice.ns.t WHERE d >= DATE '2026-03-05' AND d < DATE '2026-03-15'";
const MONTH_DELETE: &str = "DELETE FROM ice.ns.t WHERE m = 4";

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

fn march_day(id: i32) -> i32 {
    id % 30 + 1
}

async fn seeded_table(batch_rows: &str) -> (ReparkSession, TempDir) {
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
            "CREATE TABLE ice.ns.t (id INT, d DATE, m INT) USING iceberg PARTITIONED BY (m, months(d)) \
             TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read', \
             '{BATCH_ROWS_PROPERTY}'='{batch_rows}')"
        ),
    )
    .await;
    let march = (0..MARCH_ROWS)
        .map(|id| format!("({id}, DATE '2026-03-{:02}', 3)", march_day(id)))
        .collect::<Vec<_>>()
        .join(", ");
    run(&session, &format!("INSERT INTO ice.ns.t VALUES {march}")).await;
    let april = APRIL_IDS
        .map(|id| format!("({id}, DATE '2026-04-{:02}', 4)", id % 20 + 1))
        .collect::<Vec<_>>()
        .join(", ");
    run(&session, &format!("INSERT INTO ice.ns.t VALUES {april}")).await;
    (session, warehouse)
}

async fn ids(session: &ReparkSession) -> Vec<i32> {
    attempt(session, "SELECT id FROM ice.ns.t ORDER BY id")
        .await
        .expect("read")
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .to_vec()
        })
        .collect()
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

fn all_ids() -> Vec<i32> {
    (0..MARCH_ROWS).chain(APRIL_IDS).collect()
}

fn range_survivors() -> Vec<i32> {
    (0..MARCH_ROWS)
        .filter(|id| !(5..15).contains(&march_day(*id)))
        .chain(APRIL_IDS)
        .collect()
}

fn assert_position_delete_live(snapshot: &Snapshot) {
    assert_eq!(snapshot.operation, Operation::Delete);
    assert!(snapshot.total("total-delete-files") >= 1);
    assert_eq!(snapshot.total("total-position-deletes"), 40);
    assert_eq!(snapshot.total("total-equality-deletes"), 0);
}

#[tokio::test]
async fn bulk_mor_delete_past_one_batch_completes_and_survivors_match() {
    let (session, _warehouse) = seeded_table("7").await;
    run(&session, RANGE_DELETE).await;
    let survivors = range_survivors();
    assert_eq!(all_ids().len() - survivors.len(), 40);
    assert_eq!(ids(&session).await, survivors);
    assert_position_delete_live(&current_snapshot(&session).await);
}

#[tokio::test]
async fn batch_rows_property_below_one_is_refused_through_the_door() {
    let (session, _warehouse) = seeded_table("0").await;
    let error = attempt(&session, RANGE_DELETE)
        .await
        .expect_err("a batch size below one must refuse the DELETE");
    assert!(error.contains(BATCH_ROWS_PROPERTY), "{error}");
    assert_eq!(ids(&session).await, all_ids());
    let snapshot = current_snapshot(&session).await;
    assert_eq!(snapshot.operation, Operation::Append);
    assert_eq!(snapshot.total("total-delete-files"), 0);
}

#[tokio::test]
async fn whole_partition_delete_still_takes_the_metadata_door() {
    let (session, _warehouse) = seeded_table("0").await;
    run(&session, MONTH_DELETE).await;
    assert_eq!(ids(&session).await, (0..MARCH_ROWS).collect::<Vec<_>>());
    let snapshot = current_snapshot(&session).await;
    assert_eq!(snapshot.operation, Operation::Delete);
    assert_eq!(snapshot.total("deleted-data-files"), 1);
    assert_eq!(snapshot.total("total-delete-files"), 0);
}

#[tokio::test]
async fn binary_op_range_delete_takes_the_repark_writer_not_the_fork() {
    let (session, _warehouse) = seeded_table("0").await;
    run(&session, BINARY_OP_RANGE_DELETE).await;
    assert_eq!(ids(&session).await, range_survivors());
    assert_position_delete_live(&current_snapshot(&session).await);
}
