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

#[tokio::test]
async fn alter_column_move_dotted_after_reference_keeps_bare_spark_parse_message() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.moved AS SELECT * FROM src",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.moved ALTER COLUMN name AFTER id.extra",
    )
    .await
    .expect_err("dotted AFTER reference must refuse");
    let mapped = repark_core::engine_err(error);
    let repark_common::Error::Parse(message) = &mapped else {
        panic!("expected a Parse error, got {mapped:?}");
    };
    assert_eq!(
        message,
        "[PARSE_SYNTAX_ERROR] Syntax error at or near '.'. SQLSTATE: 42601"
    );
    assert_eq!(mapped.to_string(), *message);
}
