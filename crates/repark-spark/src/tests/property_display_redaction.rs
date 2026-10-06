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

const KEYED: &str = "'password' = 'KeyPw1', 'aws.secret-access-key' = 'KeyPw2', \
     'client.token' = 'KeyPw3', 'jdbc_url' = 'KeyPw4', 'my_key' = 'KeyPw5', \
     'access_key' = 'KeyPw6', 'plain' = 'my password KeyPw7', 'note' = 'shown'";
const KEYED_SECRETS: [&str; 7] = [
    "KeyPw1", "KeyPw2", "KeyPw3", "KeyPw4", "KeyPw5", "KeyPw6", "KeyPw7",
];
const SPARK_REDACTED: &str = "*********(redacted)";

async fn keyed() -> (TempDir, SessionContext, CatalogRegistry) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!("CREATE TABLE ice.sales.k (id BIGINT) USING iceberg TBLPROPERTIES ({KEYED})"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("CREATE VIEW ice.sales.kv TBLPROPERTIES ({KEYED}) AS SELECT * FROM src"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE ice.keyed WITH DBPROPERTIES ({KEYED})"),
    )
    .await;
    (warehouse, ctx, catalogs)
}

async fn assert_key_rule(sql: &str) {
    let (_warehouse, ctx, catalogs) = keyed().await;
    let text = rendered(&ctx, &catalogs, sql).await.replace(", ", "");
    for secret in KEYED_SECRETS {
        assert!(!text.contains(secret), "{sql}: {text}");
    }
    assert!(text.contains(SPARK_REDACTED), "{sql}: {text}");
    assert!(text.contains("shown"), "{sql}: {text}");
}

#[tokio::test]
async fn show_tblproperties_of_a_table_redacts_secret_keys_like_spark() {
    assert_key_rule("SHOW TBLPROPERTIES ice.sales.k").await;
    let (_warehouse, ctx, catalogs) = keyed().await;
    let keyed = rendered(
        &ctx,
        &catalogs,
        "SHOW TBLPROPERTIES ice.sales.k ('password')",
    )
    .await;
    assert_eq!(keyed, format!("password\n{SPARK_REDACTED}"));
}

#[tokio::test]
async fn show_tblproperties_of_a_view_redacts_secret_keys() {
    assert_key_rule("SHOW TBLPROPERTIES ice.sales.kv").await;
    let (_warehouse, ctx, catalogs) = keyed().await;
    let keyed = rendered(
        &ctx,
        &catalogs,
        "SHOW TBLPROPERTIES ice.sales.kv ('password')",
    )
    .await;
    assert_eq!(keyed, format!("password\n{SPARK_REDACTED}"));
}

#[tokio::test]
async fn show_create_table_of_a_view_redacts_secret_keys_like_spark() {
    assert_key_rule("SHOW CREATE TABLE ice.sales.kv").await;
}

#[tokio::test]
async fn show_create_table_redacts_secret_keys_like_spark() {
    assert_key_rule("SHOW CREATE TABLE ice.sales.k").await;
}

#[tokio::test]
async fn describe_table_extended_redacts_secret_keys_like_spark() {
    assert_key_rule("DESCRIBE TABLE EXTENDED ice.sales.k").await;
}

#[tokio::test]
async fn show_table_extended_redacts_secret_keys_like_spark() {
    assert_key_rule("SHOW TABLE EXTENDED IN ice.sales LIKE 'k'").await;
}

#[tokio::test]
async fn describe_namespace_extended_redacts_secret_keys_like_spark() {
    assert_key_rule("DESCRIBE NAMESPACE EXTENDED ice.keyed").await;
}

#[tokio::test]
async fn a_credential_free_storage_location_is_shown_as_spark_shows_it() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE NAMESPACE ice.lake LOCATION 's3://lake/teams/data@corp.example.com/ns' \
         WITH DBPROPERTIES ('src' = 's3://lake/raw/user@example.com/p')",
    )
    .await;
    for sql in [
        "DESCRIBE NAMESPACE ice.lake",
        "DESCRIBE NAMESPACE EXTENDED ice.lake",
    ] {
        let text = rendered(&ctx, &catalogs, sql).await;
        assert!(
            text.contains("s3://lake/teams/data@corp.example.com/ns"),
            "{sql}: {text}"
        );
    }
    let text = rendered(&ctx, &catalogs, "DESCRIBE NAMESPACE EXTENDED ice.lake").await;
    assert!(text.contains("s3://lake/raw/user@example.com/p"), "{text}");
}
