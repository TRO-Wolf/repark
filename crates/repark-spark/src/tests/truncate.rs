//! pins: dml-c-truncate/C-001, C-002, C-005, C-006, C-007
use super::super::*;
use super::common::*;
use iceberg::spec::Operation;

async fn snapshot_count(catalogs: &CatalogRegistry, table: &str) -> usize {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .snapshots()
        .len()
}

async fn current_operation(catalogs: &CatalogRegistry, table: &str) -> Operation {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .current_snapshot()
        .expect("current snapshot")
        .summary()
        .operation
        .clone()
}

#[tokio::test]
async fn truncate_table_wipes_rows_stamps_delete_and_preserves_history() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let before = load_sales_table(&catalogs, "t").await;
    let pre_id = before
        .metadata()
        .current_snapshot_id()
        .expect("pre-truncate snapshot");
    let pre_count = before.metadata().snapshots().len();
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);

    run(&ctx, &catalogs, "TRUNCATE TABLE ice.sales.t").await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
    assert!(
        live_data_file_paths(&catalogs, "t").await.is_empty(),
        "truncate must leave zero live data files"
    );
    assert_eq!(snapshot_count(&catalogs, "t").await, pre_count + 1);
    assert_eq!(current_operation(&catalogs, "t").await, Operation::Delete);
    let summary = load_sales_table(&catalogs, "t")
        .await
        .metadata()
        .current_snapshot()
        .expect("truncate snapshot")
        .summary()
        .additional_properties
        .clone();
    assert_eq!(summary.get("total-records").map(String::as_str), Some("0"));
    assert_eq!(
        summary.get("total-data-files").map(String::as_str),
        Some("0")
    );
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.t VERSION AS OF {pre_id}")
        )
        .await,
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn empty_insert_overwrite_still_wipes_and_stamps_delete() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ow AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.ow SELECT * FROM ice.sales.ow WHERE false",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.ow").await, 0);
    assert_eq!(current_operation(&catalogs, "ow").await, Operation::Delete);
}

#[tokio::test]
async fn truncate_missing_table_is_table_or_view_not_found() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(&ctx, &catalogs, "TRUNCATE TABLE ice.sales.does_not_exist")
        .await
        .expect_err("missing table")
        .to_string();
    assert!(
        error.contains("TABLE_OR_VIEW_NOT_FOUND"),
        "spark class: {error}"
    );
}

#[tokio::test]
async fn truncate_view_is_expect_table_not_view() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW v_trunc AS SELECT * FROM ice.sales.t",
    )
    .await;
    let error = execute(&ctx, &catalogs, "TRUNCATE TABLE v_trunc")
        .await
        .expect_err("view")
        .to_string();
    assert!(
        error.contains("EXPECT_TABLE_NOT_VIEW"),
        "spark class: {error}"
    );
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);
}

#[tokio::test]
async fn truncate_partition_form_refuses_without_wiping() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.part USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "TRUNCATE TABLE ice.sales.part PARTITION (id = 1)",
    )
    .await
    .expect_err("partition form")
    .to_string();
    assert!(
        error.contains("INVALID_PARTITION_OPERATION") || error.contains("PARTITION"),
        "partition refuse: {error}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.part").await,
        3,
        "refused partition truncate must not full-table wipe"
    );
}

#[tokio::test]
async fn truncate_never_written_table_commits_delete_snapshot() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.empty (id INT, name STRING) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "TRUNCATE TABLE ice.sales.empty").await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.empty").await,
        0
    );
    assert_eq!(snapshot_count(&catalogs, "empty").await, 1);
    assert_eq!(
        current_operation(&catalogs, "empty").await,
        Operation::Delete
    );
}
