use super::super::*;
use super::common::*;

async fn arity_door(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id INT, data STRING, cat STRING) USING iceberg",
    )
    .await;
    (ctx, catalogs)
}

async fn outcome(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    match execute(ctx, catalogs, sql).await {
        Ok(frame) => match frame.collect().await {
            Ok(_) => String::new(),
            Err(err) => err.to_string(),
        },
        Err(err) => err.to_string(),
    }
}

#[tokio::test]
async fn spark_short_values_insert_stamps_not_enough_data_columns() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = arity_door(&wh).await;
    let result = execute(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (9, 'z')").await;
    let DataFusionError::Plan(text) = result.unwrap_err() else {
        panic!("short VALUES must refuse plan-class");
    };
    assert!(
        text.contains("[INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS]"),
        "{text}"
    );
    assert!(text.contains("SQLSTATE: 21S01"), "{text}");
    assert!(
        text.contains("Table columns: `id`, `data`, `cat`."),
        "{text}"
    );
    assert!(text.contains("Data columns: `col1`, `col2`."), "{text}");
    assert!(text.contains("`ice`.`sales`.`t`"), "{text}");
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
}

#[tokio::test]
async fn spark_correct_arity_values_insert_succeeds() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = arity_door(&wh).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x')",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 1);
}

#[tokio::test]
async fn spark_named_column_list_insert_has_no_arity_condition() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = arity_door(&wh).await;
    let text = outcome(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t (id, data) VALUES (9, 'z')",
    )
    .await;
    assert!(!text.contains("NOT_ENOUGH_DATA_COLUMNS"), "{text}");
}

#[tokio::test]
async fn spark_short_insert_select_keeps_column_count_mismatch() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = arity_door(&wh).await;
    let text = outcome(&ctx, &catalogs, "INSERT INTO ice.sales.t SELECT 9, 'z'").await;
    assert!(text.contains("Column count doesn't match"), "{text}");
    assert!(!text.contains("NOT_ENOUGH_DATA_COLUMNS"), "{text}");
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
}

#[tokio::test]
async fn spark_wide_values_insert_has_no_arity_condition() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = arity_door(&wh).await;
    let text = outcome(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x', 'extra')",
    )
    .await;
    assert!(!text.contains("NOT_ENOUGH_DATA_COLUMNS"), "{text}");
}

#[tokio::test]
async fn spark_mixed_length_values_insert_has_no_arity_condition() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = arity_door(&wh).await;
    let text = outcome(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x'), (2, 'b')",
    )
    .await;
    assert!(!text.contains("NOT_ENOUGH_DATA_COLUMNS"), "{text}");
}

#[tokio::test]
async fn spark_short_overwrite_values_has_no_arity_condition() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = arity_door(&wh).await;
    let text = outcome(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t VALUES (9, 'z')",
    )
    .await;
    assert!(!text.contains("NOT_ENOUGH_DATA_COLUMNS"), "{text}");
}

#[tokio::test]
async fn spark_short_by_name_values_has_no_arity_condition() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = arity_door(&wh).await;
    let text = outcome(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t BY NAME VALUES (9, 'z')",
    )
    .await;
    assert!(!text.contains("NOT_ENOUGH_DATA_COLUMNS"), "{text}");
}

#[tokio::test]
async fn spark_missing_table_short_values_has_no_arity_condition() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let text = outcome(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.never_there VALUES (1, 'a')",
    )
    .await;
    assert!(!text.is_empty(), "missing-table INSERT must still refuse");
    assert!(!text.contains("NOT_ENOUGH_DATA_COLUMNS"), "{text}");
}
