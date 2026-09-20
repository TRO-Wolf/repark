use super::super::*;
use super::common::*;

async fn snapshot_operations(catalogs: &CatalogRegistry, table: &str) -> Vec<String> {
    let loaded = load_sales_table(catalogs, table).await;
    let metadata = loaded.metadata();
    metadata
        .history()
        .iter()
        .filter_map(|entry| metadata.snapshot_by_id(entry.snapshot_id))
        .map(|snapshot| snapshot.summary().operation.as_str().to_string())
        .collect()
}

#[tokio::test]
async fn replace_table_column_list_takes_the_column_def_replace_path() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rt AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "REPLACE TABLE ice.sales.rt (k INT, v STRING) USING iceberg",
    )
    .await;

    assert_eq!(snapshot_operations(&catalogs, "rt").await, ["append"]);
    let loaded = load_sales_table(&catalogs, "rt").await;
    assert!(
        loaded.metadata().current_snapshot().is_none(),
        "the column-def replace drops the main ref, as Spark's buildReplacement does"
    );
    assert_eq!(
        loaded
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>(),
        ["k", "v"]
    );
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rt").await, 0);
}

#[tokio::test]
async fn replace_table_as_select_records_an_overwrite() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rtas AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "REPLACE TABLE ice.sales.rtas USING iceberg AS SELECT 99 AS z",
    )
    .await;

    assert_eq!(
        snapshot_operations(&catalogs, "rtas").await,
        ["append", "overwrite"],
        "a rewrite to plain CREATE TABLE would record a second append"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rtas").await,
        1
    );
}

#[tokio::test]
async fn replace_table_on_a_missing_table_refuses_and_creates_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    for sql in [
        "REPLACE TABLE ice.sales.nope (k INT) USING iceberg",
        "REPLACE TABLE ice.sales.nope_rtas USING iceberg AS SELECT 1 AS i",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("REPLACE TABLE requires the table to exist")
            .to_string();
        assert!(
            error.contains("[TABLE_OR_VIEW_NOT_FOUND]"),
            "{sql}: {error}"
        );
        assert!(error.contains("SQLSTATE: 42P01"), "{sql}: {error}");
    }
    for name in ["nope", "nope_rtas"] {
        let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string());
        assert!(
            !catalogs["ice"].table_exists(&ident).await.unwrap(),
            "a refused REPLACE TABLE must not create `{name}`"
        );
    }
}

#[tokio::test]
async fn create_or_replace_table_still_creates_a_missing_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    run(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.fresh (k INT) USING iceberg",
    )
    .await;

    let ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "fresh".to_string(),
    );
    assert!(catalogs["ice"].table_exists(&ident).await.unwrap());
}
