//! Partition-scoped INSERT OVERWRITE pins (DML-B).
use iceberg::spec::Operation;

use super::super::*;
use super::common::*;

/// pins: dml-b-insert-overwrite/C-002, C-004
#[tokio::test]
async fn empty_dynamic_partition_overwrite_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id) SELECT * FROM src WHERE false",
    )
    .await
    .expect_err("empty dynamic overwrite must refuse");
    assert!(
        error
            .to_string()
            .contains(repark_iceberg::write::EMPTY_DYNAMIC_OVERWRITE_NEEDLE),
        "got {error}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())],
        "empty dynamic overwrite must leave every row"
    );
}

/// pins: dml-b-insert-overwrite/C-002
#[tokio::test]
async fn dynamic_partition_overwrite_replaces_source_partitions_only() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id) SELECT 1 AS id, 'z' AS name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "z".into()), (2, "b".into()), (3, "c".into())],
        "dynamic overwrite must keep partitions absent from the source"
    );
    let table = load_sales_table(&catalogs, "t").await;
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("overwrite snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Overwrite);
    assert_eq!(
        snapshot
            .summary()
            .additional_properties
            .get("replace-partitions")
            .map(String::as_str),
        Some("true"),
        "dynamic overwrite stamps replace-partitions=true"
    );
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_partition_overwrite_stamps_overwrite_operation() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id = 2) SELECT 'y' AS name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (2, "y".into()), (3, "c".into())],
    );
    let table = load_sales_table(&catalogs, "t").await;
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("overwrite snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Overwrite);
    assert!(
        !snapshot
            .summary()
            .additional_properties
            .contains_key("replace-partitions"),
        "static overwrite must not stamp replace-partitions"
    );
}

/// pins: dml-b-insert-overwrite/C-001, C-004, C-005
#[tokio::test]
async fn empty_static_partition_overwrite_stamps_delete_operation() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id = 1) SELECT name FROM src WHERE false",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(2, "b".into()), (3, "c".into())],
        "empty static overwrite must drop only the named partition"
    );
    let table = load_sales_table(&catalogs, "t").await;
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("empty static snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Delete);
    assert!(
        !snapshot
            .summary()
            .additional_properties
            .contains_key("replace-partitions"),
        "empty static overwrite must not stamp replace-partitions"
    );
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_partition_overwrite_rejects_too_many_source_columns() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t PARTITION (id = 1) SELECT 1 AS id, 'z' AS name",
    )
    .await
    .expect_err("Hive static PARTITION injects id; SELECT must not also supply it");
    assert!(
        error.to_string().contains("TOO_MANY_DATA_COLUMNS"),
        "got {error}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())],
        "arity mismatch must not mutate the table"
    );
}
