use super::super::*;
use super::common::*;
use super::dyn_partition_overwrite::{run_static, seed, setup_dynamic};

async fn snapshot_count(catalogs: &CatalogRegistry) -> usize {
    load_sales_table(catalogs, "t")
        .await
        .metadata()
        .snapshots()
        .count()
}

#[tokio::test]
async fn dynamic_by_name_overwrite_replaces_touched_partition_only() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t BY NAME SELECT 'b' AS name, 20 AS id",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (3, "c".into()), (20, "b".into())],
    );
    let table = load_sales_table(&catalogs, "t").await;
    let summary = table
        .metadata()
        .current_snapshot()
        .expect("overwrite snapshot")
        .summary()
        .clone();
    assert_eq!(
        summary
            .additional_properties
            .get("replace-partitions")
            .map(String::as_str),
        Some("true"),
    );
}

#[tokio::test]
async fn dynamic_by_name_empty_overwrite_commits_nothing() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    let before = snapshot_count(&catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t BY NAME SELECT name, id FROM src WHERE false",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())],
    );
    assert_eq!(snapshot_count(&catalogs).await, before);
}

#[tokio::test]
async fn static_by_name_empty_overwrite_wipes_table() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t BY NAME SELECT name, id FROM src WHERE false",
    )
    .await;
    assert_eq!(table_rows(&ctx, &catalogs, "ice.sales.t").await, vec![]);
}

#[tokio::test]
async fn dynamic_column_list_overwrite_replaces_touched_partition_only() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t (name, id) SELECT 'b', 20",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (3, "c".into()), (20, "b".into())],
    );
}

#[tokio::test]
async fn static_entry_by_name_pins_whole_table_replace_under_dynamic_conf() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run_static(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t BY NAME SELECT 'b' AS name, 20 AS id",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(20, "b".into())],
    );
}

#[tokio::test]
async fn static_entry_by_name_empty_wipes_under_dynamic_conf() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run_static(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t BY NAME SELECT name, id FROM src WHERE false",
    )
    .await;
    assert_eq!(table_rows(&ctx, &catalogs, "ice.sales.t").await, vec![]);
}

#[tokio::test]
async fn dynamic_by_name_empty_overwrite_on_unpartitioned_table_commits_nothing() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    let before = snapshot_count(&catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t BY NAME SELECT name, id FROM src WHERE false",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())],
    );
    assert_eq!(snapshot_count(&catalogs).await, before);
}
