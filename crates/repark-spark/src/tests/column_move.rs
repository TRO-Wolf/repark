use super::super::*;
use super::common::*;
use tempfile::TempDir;

#[tokio::test]
async fn alter_column_move_first_and_after_reorder() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.moved AS SELECT * FROM src",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.moved ALTER COLUMN name FIRST",
    )
    .await
    .unwrap();
    let batches = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.moved")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, vec!["name".to_string(), "id".to_string()]);
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.moved ALTER COLUMN name AFTER id",
    )
    .await
    .unwrap();
    let back = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.moved")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let back_names: Vec<String> = back[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(back_names, vec!["id".to_string(), "name".to_string()]);
}
