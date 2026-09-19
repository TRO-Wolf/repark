use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::functions_aggregate::count::count_all;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergTableScan;
use repark_core::ReparkSession;
use tempfile::TempDir;

use crate::{SparkDialect, SparkExtension};

const MOR_V2: &str = "'format-version' = '2', 'write.delete.mode' = 'merge-on-read'";
const MOR_V3: &str = "'format-version' = '3', 'write.delete.mode' = 'merge-on-read'";
const FILES: i64 = 4;
const ROWS_PER_FILE: i64 = 3;
const TOTAL: i64 = FILES * ROWS_PER_FILE;

async fn spark_session(wh: &TempDir) -> ReparkSession {
    let warehouse = wh.path().to_str().unwrap().to_string();
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .build()
        .unwrap();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
    session
}

async fn run(session: &ReparkSession, sql: &str) {
    session.sql(sql).await.unwrap().collect().await.unwrap();
}

async fn seed(session: &ReparkSession, table: &str, props: &str) {
    let tblprops = if props.is_empty() {
        String::new()
    } else {
        format!(" TBLPROPERTIES ({props})")
    };
    run(
        session,
        &format!("CREATE TABLE ice.sales.{table} (id INT, name STRING) USING iceberg{tblprops}"),
    )
    .await;
    for file in 0..FILES {
        let base = file * ROWS_PER_FILE;
        run(
            session,
            &format!(
                "INSERT INTO ice.sales.{table} VALUES ({}, 'a'), ({}, 'b'), ({}, 'c')",
                base + 1,
                base + 2,
                base + 3
            ),
        )
        .await;
    }
}

fn scan_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let own = usize::from(plan.as_ref().downcast_ref::<IcebergTableScan>().is_some());
    own + plan.children().into_iter().map(scan_count).sum::<usize>()
}

async fn physical(session: &ReparkSession, sql: &str) -> Arc<dyn ExecutionPlan> {
    session
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

async fn count_value(session: &ReparkSession, sql: &str) -> i64 {
    let batches = session.sql(sql).await.unwrap().collect().await.unwrap();
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("one count row");
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Int64,
        "{sql}"
    );
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn assert_count(session: &ReparkSession, sql: &str, expected: i64, folded: bool) {
    let plan = physical(session, sql).await;
    let scans = scan_count(&plan);
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    if folded {
        assert_eq!(scans, 0, "expected a folded plan for {sql}:\n{text}");
    } else {
        assert_eq!(scans, 1, "expected a scanned plan for {sql}:\n{text}");
    }
    assert_eq!(count_value(session, sql).await, expected, "{sql}");
}

async fn current_snapshot(session: &ReparkSession, table: &str) -> i64 {
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), table.into());
    session.catalogs_snapshot()["ice"]
        .load_table(&ident)
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("a current snapshot")
}

#[tokio::test]
async fn count_star_folds_on_a_plain_table() {
    let wh = TempDir::new().unwrap();
    let session = spark_session(&wh).await;
    seed(&session, "t", "").await;
    assert_count(&session, "SELECT count(*) FROM ice.sales.t", TOTAL, true).await;
    assert_count(&session, "SELECT count(1) FROM ice.sales.t", TOTAL, true).await;
    assert_count(
        &session,
        "SELECT count(*) AS n FROM ice.sales.t LIMIT 1",
        TOTAL,
        true,
    )
    .await;
}

#[tokio::test]
async fn count_star_on_an_empty_table_folds_to_zero() {
    let wh = TempDir::new().unwrap();
    let session = spark_session(&wh).await;
    run(
        &session,
        "CREATE TABLE ice.sales.e (id INT, name STRING) USING iceberg",
    )
    .await;
    assert_eq!(
        count_value(&session, "SELECT count(*) FROM ice.sales.e").await,
        0
    );
}

#[tokio::test]
async fn dataframe_count_all_folds_on_a_plain_table() {
    let wh = TempDir::new().unwrap();
    let session = spark_session(&wh).await;
    seed(&session, "t", "").await;
    let frame = session
        .context()
        .table("ice.sales.t")
        .await
        .unwrap()
        .aggregate(vec![], vec![count_all()])
        .unwrap();
    let plan = frame.clone().create_physical_plan().await.unwrap();
    assert_eq!(scan_count(&plan), 0);
    let int32_frame = session
        .context()
        .table("ice.sales.t")
        .await
        .unwrap()
        .aggregate(
            vec![],
            vec![datafusion::functions_aggregate::count::count(
                datafusion::prelude::lit(1_i32),
            )],
        )
        .unwrap();
    let plan = int32_frame.clone().create_physical_plan().await.unwrap();
    assert_eq!(scan_count(&plan), 0);
    let batches = int32_frame.collect().await.unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "count(Int32(1))");
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        TOTAL
    );
    let counted = session
        .context()
        .table("ice.sales.t")
        .await
        .unwrap()
        .count()
        .await
        .unwrap();
    assert_eq!(i64::try_from(counted).unwrap(), TOTAL);
}

#[tokio::test]
async fn count_star_with_a_filter_scans_and_answers() {
    let wh = TempDir::new().unwrap();
    let session = spark_session(&wh).await;
    seed(&session, "t", "").await;
    assert_count(
        &session,
        "SELECT count(*) FROM ice.sales.t WHERE id < 5",
        4,
        false,
    )
    .await;
}

#[tokio::test]
async fn count_star_over_a_limit_subquery_scans_and_answers() {
    let wh = TempDir::new().unwrap();
    let session = spark_session(&wh).await;
    seed(&session, "t", "").await;
    assert_count(
        &session,
        "SELECT count(*) FROM (SELECT * FROM ice.sales.t LIMIT 5) AS s",
        5,
        false,
    )
    .await;
}

#[tokio::test]
async fn count_star_with_a_v2_position_delete_scans_and_answers() {
    let wh = TempDir::new().unwrap();
    let session = spark_session(&wh).await;
    seed(&session, "p", MOR_V2).await;
    run(&session, "DELETE FROM ice.sales.p WHERE id = 7").await;
    assert_count(
        &session,
        "SELECT count(*) FROM ice.sales.p",
        TOTAL - 1,
        false,
    )
    .await;
}

#[tokio::test]
async fn count_star_with_a_v3_deletion_vector_scans_and_answers() {
    let wh = TempDir::new().unwrap();
    let session = spark_session(&wh).await;
    seed(&session, "dv", MOR_V3).await;
    run(&session, "DELETE FROM ice.sales.dv WHERE id = 7").await;
    assert_count(
        &session,
        "SELECT count(*) FROM ice.sales.dv",
        TOTAL - 1,
        false,
    )
    .await;
}

#[tokio::test]
async fn count_star_after_a_copy_on_write_delete_folds_to_the_live_rows() {
    let wh = TempDir::new().unwrap();
    let session = spark_session(&wh).await;
    seed(&session, "c", "").await;
    run(&session, "DELETE FROM ice.sales.c WHERE id = 7").await;
    assert_count(
        &session,
        "SELECT count(*) FROM ice.sales.c",
        TOTAL - 1,
        true,
    )
    .await;
}

#[tokio::test]
async fn count_star_version_as_of_an_older_snapshot_folds_to_that_snapshot() {
    let wh = TempDir::new().unwrap();
    let session = spark_session(&wh).await;
    seed(&session, "v", "").await;
    let older = current_snapshot(&session, "v").await;
    run(
        &session,
        "INSERT INTO ice.sales.v VALUES (100, 'x'), (101, 'y')",
    )
    .await;
    assert_count(
        &session,
        &format!("SELECT count(*) FROM ice.sales.v VERSION AS OF {older}"),
        TOTAL,
        true,
    )
    .await;
    assert_count(
        &session,
        "SELECT count(*) FROM ice.sales.v",
        TOTAL + 2,
        true,
    )
    .await;
}
