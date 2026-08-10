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
