use super::super::*;
use super::common::*;

const TABLE_PASSWORD: &str = "TblPw1";
const VIEW_PASSWORD: &str = "ViewPw2";
const MASKED_URL: &str = "postgresql://u:***@db.example.com/sales";

async fn prepared() -> (TempDir, SessionContext, CatalogRegistry) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.t (id BIGINT) USING iceberg TBLPROPERTIES \
             ('conn' = 'postgresql://u:{TABLE_PASSWORD}@db.example.com/sales', 'plain' = 'p7')"
        ),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE VIEW ice.sales.v TBLPROPERTIES \
             ('conn' = 'postgresql://u:{VIEW_PASSWORD}@db.example.com/sales') \
             AS SELECT * FROM src"
        ),
    )
    .await;
    (warehouse, ctx, catalogs)
}

async fn rendered(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap();
    let mut cells = Vec::new();
    for batch in &batches {
        for column in batch.columns() {
            if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
                cells.extend(strings.iter().flatten().map(str::to_string));
            }
        }
    }
    cells.join("\n")
}

async fn assert_masked(sql: &str, password: &str) {
    let (_warehouse, ctx, catalogs) = prepared().await;
    let text = rendered(&ctx, &catalogs, sql).await.replace(", ", "");
    assert!(!text.contains(password), "{sql}: {text}");
    assert!(text.contains(MASKED_URL), "{sql}: {text}");
}

#[tokio::test]
async fn describe_table_extended_masks_a_property_password() {
    assert_masked("DESCRIBE TABLE EXTENDED ice.sales.t", TABLE_PASSWORD).await;
}

#[tokio::test]
async fn show_create_table_masks_a_property_password() {
    assert_masked("SHOW CREATE TABLE ice.sales.t", TABLE_PASSWORD).await;
}

#[tokio::test]
async fn show_table_extended_masks_a_property_password() {
    assert_masked("SHOW TABLE EXTENDED IN ice.sales LIKE 't'", TABLE_PASSWORD).await;
}

#[tokio::test]
async fn show_tblproperties_of_a_table_masks_a_property_password() {
    assert_masked("SHOW TBLPROPERTIES ice.sales.t", TABLE_PASSWORD).await;
    assert_masked("SHOW TBLPROPERTIES ice.sales.t ('conn')", TABLE_PASSWORD).await;
}

#[tokio::test]
async fn show_tblproperties_of_a_view_masks_a_property_password() {
    assert_masked("SHOW TBLPROPERTIES ice.sales.v", VIEW_PASSWORD).await;
    assert_masked("SHOW TBLPROPERTIES ice.sales.v ('conn')", VIEW_PASSWORD).await;
}

#[tokio::test]
async fn show_create_table_of_a_view_masks_a_property_password() {
    assert_masked("SHOW CREATE TABLE ice.sales.v", VIEW_PASSWORD).await;
}
