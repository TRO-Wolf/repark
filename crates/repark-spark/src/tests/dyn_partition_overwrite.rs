use iceberg::spec::Operation;

use super::super::*;
use super::common::*;

pub(super) async fn setup_dynamic(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    assert!(!repark_core::partition_overwrite_mode_from_ctx(&ctx).is_dynamic());
    let state = ctx.state_ref();
    let mut state = state.write();
    state
        .config_mut()
        .options_mut()
        .extensions
        .insert(repark_core::PartitionOverwriteModeConfig {
            mode: repark_core::PartitionOverwriteMode::Dynamic,
        });
    (ctx, catalogs)
}

pub(super) async fn seed(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg PARTITIONED BY (name)",
    )
    .await;
    run(
        ctx,
        catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
}

#[tokio::test]
async fn dynamic_partition_less_overwrite_replaces_touched_partition_only() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 20 AS id, 'b' AS name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (3, "c".into()), (20, "b".into())],
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
    );
}

#[tokio::test]
async fn dynamic_empty_overwrite_leaves_table_unchanged() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT id, name FROM src WHERE false",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())],
    );
}

#[tokio::test]
async fn static_partition_less_overwrite_replaces_whole_table() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 20 AS id, 'b' AS name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(20, "b".into())],
    );
}

#[tokio::test]
async fn dynamic_overwrite_on_unpartitioned_table_replaces_whole_table() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.u (id INT, name STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.u VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.u SELECT 20 AS id, 'b' AS name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.u").await,
        vec![(20, "b".into())],
    );
}

pub(super) async fn run_static(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) {
    crate::execute_static_overwrite(
        ctx,
        catalogs,
        sql,
        &std::collections::HashSet::<String>::new(),
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
}

#[tokio::test]
async fn static_entry_pins_whole_table_replace_under_dynamic_conf() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run_static(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 20 AS id, 'b' AS name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(20, "b".into())],
    );
}

#[tokio::test]
async fn dynamic_marker_inside_string_literal_stays_partition_scoped() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 20 AS id, '/*RSOW*/' AS name",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![
            (1, "a".into()),
            (2, "b".into()),
            (3, "c".into()),
            (20, "/*RSOW*/".into()),
        ],
    );
}

#[tokio::test]
async fn dynamic_trailing_line_comment_stays_partition_scoped() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t SELECT 20 AS id, 'b' AS name -- /*RSOW*/",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![(1, "a".into()), (3, "c".into()), (20, "b".into())],
    );
}

#[tokio::test]
async fn snapshot_race_replaces_concurrent_same_partition_append() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    seed(&ctx, &catalogs).await;
    let stale = load_sales_table(&catalogs, "t").await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (9, 'b'), (8, 'a')",
    )
    .await;
    let source = execute(&ctx, &catalogs, "SELECT 20 AS id, 'b' AS name")
        .await
        .unwrap();
    let stream = source.execute_stream().await.unwrap();
    let staged = repark_iceberg::write::write_overwrite_staged_files_from_stream(
        &stale,
        stream,
        Vec::new(),
        repark_iceberg::write::concurrency_from_ctx(&ctx),
    )
    .await
    .unwrap();
    let catalog = catalog_handle(&catalogs, "ice").unwrap();
    repark_iceberg::write::commit_replace_partitions_to(catalog, &stale, staged, None)
        .await
        .unwrap();
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.t").await,
        vec![
            (1, "a".into()),
            (3, "c".into()),
            (8, "a".into()),
            (20, "b".into()),
        ],
    );
}

#[tokio::test]
async fn serializable_race_refuses_concurrent_same_partition_append() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.s (id INT, name STRING) USING iceberg PARTITIONED BY (name) \
         TBLPROPERTIES ('write.overwrite.isolation-level'='serializable')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.s VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    let stale = load_sales_table(&catalogs, "s").await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.s VALUES (9, 'b')").await;
    let source = execute(&ctx, &catalogs, "SELECT 20 AS id, 'b' AS name")
        .await
        .unwrap();
    let stream = source.execute_stream().await.unwrap();
    let staged = repark_iceberg::write::write_overwrite_staged_files_from_stream(
        &stale,
        stream,
        Vec::new(),
        repark_iceberg::write::concurrency_from_ctx(&ctx),
    )
    .await
    .unwrap();
    let catalog = catalog_handle(&catalogs, "ice").unwrap();
    let error = repark_iceberg::write::commit_replace_partitions_to(catalog, &stale, staged, None)
        .await
        .expect_err("serializable overwrite must refuse the concurrent append");
    assert!(
        error.to_string().contains("conflicting files"),
        "conflict refusal must name the files: {error}"
    );
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.s").await,
        vec![
            (1, "a".into()),
            (2, "b".into()),
            (3, "c".into()),
            (9, "b".into())
        ],
    );
}

#[tokio::test]
async fn dynamic_empty_overwrite_on_unpartitioned_table_leaves_table_unchanged() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_dynamic(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.u (id INT, name STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.u VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.u SELECT id, name FROM src WHERE false",
    )
    .await;
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.u").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())],
    );
}
