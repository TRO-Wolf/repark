use super::super::*;
use super::common::{run, setup};
use tempfile::TempDir;

async fn door() -> (TempDir, SessionContext, CatalogRegistry) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id BIGINT, data STRING) USING iceberg",
    )
    .await;
    (warehouse, ctx, catalogs)
}

fn assert_cannot_safely_cast(err: &str) {
    assert!(
        err.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"),
        "{err}"
    );
    assert!(err.contains("SQLSTATE: KD000"), "{err}");
    assert!(err.contains("`id`"), "{err}");
    assert!(err.contains("\"STRING\""), "{err}");
    assert!(err.contains("\"BIGINT\""), "{err}");
}

#[tokio::test]
async fn update_string_literal_into_bigint_stamps_cannot_safely_cast() {
    let (_warehouse, ctx, catalogs) = door().await;
    let err = execute(&ctx, &catalogs, "UPDATE ice.sales.t SET id = 'notanumber'")
        .await
        .expect_err("string into BIGINT must refuse")
        .to_string();
    assert_cannot_safely_cast(&err);
}

#[tokio::test]
async fn update_int_literal_into_bigint_succeeds() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(&ctx, &catalogs, "UPDATE ice.sales.t SET id = 7").await;
}

#[tokio::test]
async fn update_string_literal_into_string_succeeds() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(&ctx, &catalogs, "UPDATE ice.sales.t SET data = 'z'").await;
}

#[tokio::test]
async fn update_string_column_into_bigint_stamps_cannot_safely_cast() {
    let (_warehouse, ctx, catalogs) = door().await;
    let err = execute(&ctx, &catalogs, "UPDATE ice.sales.t SET id = data")
        .await
        .expect_err("STRING column into BIGINT must refuse")
        .to_string();
    assert_cannot_safely_cast(&err);
}

#[tokio::test]
async fn update_cast_to_bigint_succeeds() {
    let (_warehouse, ctx, catalogs) = door().await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.t SET id = CAST('1' AS BIGINT)",
    )
    .await;
}

#[tokio::test]
async fn update_missing_table_keeps_its_own_error() {
    let (_warehouse, ctx, catalogs) = door().await;
    let err = execute(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.missing SET id = 'notanumber'",
    )
    .await
    .expect_err("missing table must still fail")
    .to_string();
    assert!(!err.contains("CANNOT_SAFELY_CAST"), "{err}");
}

#[tokio::test]
async fn insert_string_into_bigint_keeps_the_insert_path() {
    let (_warehouse, ctx, catalogs) = door().await;
    let text = match execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES ('notanumber', 'z')",
    )
    .await
    {
        Ok(frame) => match frame.collect().await {
            Ok(_) => String::new(),
            Err(err) => err.to_string(),
        },
        Err(err) => err.to_string(),
    };
    assert!(
        !text.contains("Cannot write incompatible data for the table"),
        "{text}"
    );
}
