/// `DROP TABLE` removes the table; `IF EXISTS` on a missing one is a no-op.
use super::super::*;
use super::common::*;

#[tokio::test]
async fn drop_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders").await,
        3
    );

    execute(&ctx, &catalogs, "DROP TABLE ice.sales.orders")
        .await
        .unwrap();
    // Gone — querying it now errors.
    assert!(
        execute(&ctx, &catalogs, "SELECT * FROM ice.sales.orders")
            .await
            .is_err()
    );
    // IF EXISTS on the now-missing table is a no-op.
    execute(&ctx, &catalogs, "DROP TABLE IF EXISTS ice.sales.orders")
        .await
        .unwrap();
}

#[tokio::test]
async fn drop_table_missing_is_table_or_view_not_found() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(&ctx, &catalogs, "DROP TABLE ice.sales.never_there")
        .await
        .expect_err("a missing DROP target refuses")
        .to_string();
    assert!(error.contains("[TABLE_OR_VIEW_NOT_FOUND]"), "got: {error}");
    assert!(error.contains("SQLSTATE: 42P01"), "got: {error}");
}

#[tokio::test]
async fn create_table_existing_is_table_or_view_already_exists() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.dup (id BIGINT) USING iceberg",
    )
    .await;

    for sql in [
        "CREATE TABLE ice.sales.dup (id BIGINT) USING iceberg",
        "CREATE TABLE ice.sales.dup AS SELECT * FROM src",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("an existing CREATE target refuses")
            .to_string();
        assert!(
            error.contains("[TABLE_OR_VIEW_ALREADY_EXISTS]"),
            "got: {error}"
        );
        assert!(error.contains("SQLSTATE: 42P07"), "got: {error}");
    }
}

#[test]
fn partition_management_unsupported_starts_with_condition_and_names_table() {
    let DataFusionError::Plan(message) =
        catalog_ops::partition_management_unsupported("`sc`.`ns`.`t`")
    else {
        panic!("partition_management_unsupported must be plan-class");
    };
    assert!(
        message.starts_with("[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED]"),
        "got: {message}"
    );
    assert!(message.contains("`sc`.`ns`.`t`"), "got: {message}");
    assert!(message.contains("SQLSTATE: 42601"), "got: {message}");
}

#[tokio::test]
async fn alter_add_hive_partition_refuses_with_partition_management_unsupported() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t ADD PARTITION (cat = 'q')",
    )
    .await
    .expect_err("hive ADD PARTITION must refuse");
    assert!(
        matches!(error, DataFusionError::Plan(_)),
        "hive ADD PARTITION must be plan-class, got: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED]"),
        "got: {message}"
    );
    assert!(message.contains("`ice`.`sales`.`t`"), "got: {message}");
    assert!(message.contains("SQLSTATE: 42601"), "got: {message}");
}
