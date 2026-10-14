use super::super::*;
use super::common::*;
use datafusion::sql::sqlparser::parser::ParserError;

fn parse_message(error: DataFusionError) -> String {
    let DataFusionError::SQL(inner, None) = error else {
        panic!("expected a ParseException-class SQL error, got {error:?}");
    };
    let ParserError::ParserError(message) = *inner else {
        panic!("expected ParserError::ParserError, got {inner:?}");
    };
    message
}

async fn seed_t(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    execute(ctx, catalogs, "DROP TABLE IF EXISTS ice.sales.t")
        .await
        .unwrap();
    execute(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert_eq!(rows(ctx, catalogs, "SELECT * FROM ice.sales.t").await, 3);
}

async fn seed_z(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    execute(
        ctx,
        catalogs,
        &format!("DROP TABLE IF EXISTS ice.sales.{table}"),
    )
    .await
    .unwrap();
    execute(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} AS SELECT * FROM src WHERE id = 1"),
    )
    .await
    .unwrap();
}

fn bodies() -> [(&'static str, &'static str); 4] {
    [
        ("INSERT INTO ice.sales.t VALUES (9, 'q')", "INSERT"),
        ("UPDATE ice.sales.t SET name = 'x' WHERE id = 1", "UPDATE"),
        ("DELETE FROM ice.sales.t WHERE id = 1", "DELETE"),
        (
            "MERGE INTO ice.sales.t USING (SELECT 1 AS id, 'm' AS name) s \
             ON ice.sales.t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name",
            "MERGE",
        ),
    ]
}

async fn assert_refusal(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    keyword: &str,
) {
    let error = execute(ctx, catalogs, sql).await.unwrap_err();
    assert_eq!(
        parse_message(error),
        format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{keyword}'. SQLSTATE: 42601"),
        "{sql}"
    );
    assert_eq!(
        rows(ctx, catalogs, "SELECT * FROM ice.sales.t").await,
        3,
        "{sql}"
    );
}

#[tokio::test]
async fn ctas_write_body_refuses_before_any_write() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_t(&ctx, &catalogs).await;
    for (body, keyword) in bodies() {
        let target = format!("z_create_{}", keyword.to_lowercase());
        execute(
            &ctx,
            &catalogs,
            &format!("DROP TABLE IF EXISTS ice.sales.{target}"),
        )
        .await
        .unwrap();
        assert_refusal(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE ice.sales.{target} AS {body}"),
            keyword,
        )
        .await;
        assert!(
            execute(
                &ctx,
                &catalogs,
                &format!("SELECT * FROM ice.sales.{target}")
            )
            .await
            .is_err(),
            "{body}"
        );
    }
}

#[tokio::test]
async fn ctas_or_replace_write_body_refuses_before_any_write() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_t(&ctx, &catalogs).await;
    for (body, keyword) in bodies() {
        let target = format!("z_replace_{}", keyword.to_lowercase());
        seed_z(&ctx, &catalogs, &target).await;
        assert_refusal(
            &ctx,
            &catalogs,
            &format!("CREATE OR REPLACE TABLE ice.sales.{target} AS {body}"),
            keyword,
        )
        .await;
        assert_eq!(
            rows(
                &ctx,
                &catalogs,
                &format!("SELECT * FROM ice.sales.{target}")
            )
            .await,
            1,
            "{body}"
        );
    }
}

#[tokio::test]
async fn replace_write_body_refuses_before_any_write() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_t(&ctx, &catalogs).await;
    for (body, keyword) in bodies() {
        let target = format!("z_bare_replace_{}", keyword.to_lowercase());
        seed_z(&ctx, &catalogs, &target).await;
        assert_refusal(
            &ctx,
            &catalogs,
            &format!("REPLACE TABLE ice.sales.{target} AS {body}"),
            keyword,
        )
        .await;
        assert_eq!(
            rows(
                &ctx,
                &catalogs,
                &format!("SELECT * FROM ice.sales.{target}")
            )
            .await,
            1,
            "{body}"
        );
    }
}

#[tokio::test]
async fn parenthesised_ctas_write_body_refuses_before_any_write() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_t(&ctx, &catalogs).await;
    for (body, keyword) in bodies() {
        assert_refusal(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE ice.sales.z_paren AS ({body})"),
            keyword,
        )
        .await;
    }
    assert!(
        execute(&ctx, &catalogs, "SELECT * FROM ice.sales.z_paren")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn ctas_if_not_exists_write_body_refuses_before_any_write() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_t(&ctx, &catalogs).await;
    for (body, keyword) in bodies() {
        let target = format!("z_if_not_exists_new_{}", keyword.to_lowercase());
        execute(
            &ctx,
            &catalogs,
            &format!("DROP TABLE IF EXISTS ice.sales.{target}"),
        )
        .await
        .unwrap();
        assert_refusal(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE IF NOT EXISTS ice.sales.{target} AS {body}"),
            keyword,
        )
        .await;
        assert!(
            execute(
                &ctx,
                &catalogs,
                &format!("SELECT * FROM ice.sales.{target}")
            )
            .await
            .is_err(),
            "{body}"
        );
    }
    for (body, keyword) in bodies() {
        let target = format!("z_if_not_exists_existing_{}", keyword.to_lowercase());
        seed_z(&ctx, &catalogs, &target).await;
        assert_refusal(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE IF NOT EXISTS ice.sales.{target} AS {body}"),
            keyword,
        )
        .await;
        assert_eq!(
            rows(
                &ctx,
                &catalogs,
                &format!("SELECT * FROM ice.sales.{target}")
            )
            .await,
            1,
            "{body}"
        );
    }
}

#[tokio::test]
async fn double_parenthesised_ctas_write_body_refuses_before_any_write() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_t(&ctx, &catalogs).await;
    for (body, keyword) in bodies() {
        assert_refusal(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE ice.sales.z_paren2 AS (({body}))"),
            keyword,
        )
        .await;
    }
    assert!(
        execute(&ctx, &catalogs, "SELECT * FROM ice.sales.z_paren2")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn with_prefixed_ctas_write_body_refuses_before_any_write() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_t(&ctx, &catalogs).await;
    assert_refusal(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.z_cte AS WITH c AS (SELECT 9 AS id, 'q' AS name) \
         INSERT INTO ice.sales.t SELECT * FROM c",
        "INSERT",
    )
    .await;
    assert!(
        execute(&ctx, &catalogs, "SELECT * FROM ice.sales.z_cte")
            .await
            .is_err()
    );
    assert_refusal(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.z_cte_paren AS \
         (WITH c AS (SELECT 9 AS id, 'q' AS name) INSERT INTO ice.sales.t SELECT * FROM c)",
        "INSERT",
    )
    .await;
    assert!(
        execute(&ctx, &catalogs, "SELECT * FROM ice.sales.z_cte_paren")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn replace_missing_write_body_refuses_before_any_write() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_t(&ctx, &catalogs).await;
    for (body, keyword) in bodies() {
        let target = format!("z_replace_missing_{}", keyword.to_lowercase());
        execute(
            &ctx,
            &catalogs,
            &format!("DROP TABLE IF EXISTS ice.sales.{target}"),
        )
        .await
        .unwrap();
        assert_refusal(
            &ctx,
            &catalogs,
            &format!("REPLACE TABLE ice.sales.{target} AS {body}"),
            keyword,
        )
        .await;
        assert!(
            execute(
                &ctx,
                &catalogs,
                &format!("SELECT * FROM ice.sales.{target}")
            )
            .await
            .is_err(),
            "{body}"
        );
    }
}

#[tokio::test]
async fn ctas_select_body_still_answers() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_t(&ctx, &catalogs).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.z_sel AS SELECT * FROM ice.sales.t",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.z_sel").await,
        3
    );
}
