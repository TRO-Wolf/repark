use super::super::*;
use super::common::*;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn options_stores_both_raw_and_prefixed_keys() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt_create (id BIGINT) USING iceberg OPTIONS ('k1'='v1')",
    )
    .await
    .expect("column-def CREATE with OPTIONS");
    let created = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "opt_create".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(
        created
            .metadata()
            .properties()
            .get("k1")
            .map(String::as_str),
        Some("v1")
    );
    assert_eq!(
        created
            .metadata()
            .properties()
            .get("option.k1")
            .map(String::as_str),
        Some("v1")
    );

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt_ctas USING iceberg OPTIONS ('k'='v') AS SELECT 1 AS i",
    )
    .await
    .expect("CTAS with OPTIONS");
    let ctas = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "opt_ctas".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(
        ctas.metadata().properties().get("k").map(String::as_str),
        Some("v")
    );
    assert_eq!(
        ctas.metadata()
            .properties()
            .get("option.k")
            .map(String::as_str),
        Some("v")
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT i FROM ice.sales.opt_ctas").await,
        1
    );

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt_mixed (id BIGINT) USING iceberg \
             OPTIONS (k1='v1', 'k2'='v2')",
    )
    .await
    .expect("mixed quoted and unquoted keys");
    let mixed = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "opt_mixed".to_string(),
        ))
        .await
        .unwrap();
    for (key, value) in [
        ("k1", "v1"),
        ("option.k1", "v1"),
        ("k2", "v2"),
        ("option.k2", "v2"),
    ] {
        assert_eq!(
            mixed.metadata().properties().get(key).map(String::as_str),
            Some(value),
            "key {key} must be stored"
        );
    }

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt_verbatim (id BIGINT) USING iceberg \
             OPTIONS ('a'='x,y', 'b'='p)q')",
    )
    .await
    .expect("values carrying commas and parens");
    let verbatim = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "opt_verbatim".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(
        verbatim
            .metadata()
            .properties()
            .get("a")
            .map(String::as_str),
        Some("x,y")
    );
    assert_eq!(
        verbatim
            .metadata()
            .properties()
            .get("b")
            .map(String::as_str),
        Some("p)q")
    );
}

#[tokio::test]
async fn options_near_misses_keep_their_refusals() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let with_err = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt_with (id BIGINT) USING iceberg WITH ('k'='v')",
    )
    .await
    .expect_err("WITH options must refuse");
    let message = with_err.to_string();
    assert!(
        message.contains("WITH/plain options are not supported")
            && message.contains("use TBLPROPERTIES for Iceberg table properties")
            && !message.contains("OPTIONS"),
        "got: {message}"
    );
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "opt_with".to_string(),
            ))
            .await
            .unwrap(),
        "a refused WITH CREATE must leave no table behind"
    );

    let plain_err = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt_plain (id BIGINT) USING iceberg ENGINE = InnoDB",
    )
    .await
    .expect_err("plain options must refuse");
    assert!(
        plain_err
            .to_string()
            .contains("WITH/plain options are not supported"),
        "got: {plain_err}"
    );

    for sql in [
        "CREATE TABLE ice.sales.opt_parquet (id BIGINT) USING parquet OPTIONS ('k'='v')",
        "CREATE TABLE ice.sales.opt_nous (id BIGINT) OPTIONS ('k'='v')",
    ] {
        let result = execute(&ctx, &catalogs, sql).await;
        assert!(result.is_err(), "{sql:?} must not silently serve");
    }
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "opt_parquet".to_string(),
            ))
            .await
            .unwrap(),
        "a non-iceberg OPTIONS CREATE must not reach the iceberg catalog"
    );
}

async fn stored_properties(catalogs: &CatalogRegistry, table: &str) -> HashMap<String, String> {
    catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            table.to_string(),
        ))
        .await
        .unwrap()
        .metadata()
        .properties()
        .clone()
}

async fn assert_no_sales_table(catalogs: &CatalogRegistry, table: &str) {
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.to_string(),
            ))
            .await
            .unwrap(),
        "a refused CREATE must leave no table behind: {table}"
    );
}

#[tokio::test]
async fn duplicate_options_clauses_refuse_with_no_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt_dup (id BIGINT) USING iceberg OPTIONS ('a'='b') \
         OPTIONS ('k'='v')",
    )
    .await
    .expect_err("two OPTIONS clauses must refuse");
    assert_no_sales_table(&catalogs, "opt_dup").await;
}

#[tokio::test]
async fn options_missing_equals_refuses_with_no_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt_noeq (id BIGINT) USING iceberg OPTIONS ('k' 'v')",
    )
    .await
    .expect_err("an OPTIONS pair without = must refuse");
    assert_no_sales_table(&catalogs, "opt_noeq").await;
}

#[tokio::test]
async fn options_trailing_comma_refuses_with_no_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt_trail (id BIGINT) USING iceberg OPTIONS ('k'='v',)",
    )
    .await
    .expect_err("an OPTIONS trailing comma must refuse");
    assert_no_sales_table(&catalogs, "opt_trail").await;
}

#[tokio::test]
async fn tblproperties_plus_options_refuses_with_no_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for (table, sql) in [
        (
            "opt_both_a",
            "CREATE TABLE ice.sales.opt_both_a (id BIGINT) USING iceberg \
             TBLPROPERTIES ('a'='b') OPTIONS ('k'='v')",
        ),
        (
            "opt_both_b",
            "CREATE TABLE ice.sales.opt_both_b (id BIGINT) USING iceberg \
             OPTIONS ('k'='v') TBLPROPERTIES ('a'='b')",
        ),
    ] {
        execute(&ctx, &catalogs, sql)
            .await
            .expect_err("TBLPROPERTIES together with OPTIONS must refuse");
        assert_no_sales_table(&catalogs, table).await;
    }
}

#[tokio::test]
async fn options_identifier_below_top_level_keeps_the_rewrite() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt_shadow (id BIGINT, options STRING) USING iceberg \
         PARTITIONED BY (options) OPTIONS ('k'='v')",
    )
    .await
    .expect("a column and partition field named options must not block the rewrite");
    let properties = stored_properties(&catalogs, "opt_shadow").await;
    assert_eq!(properties.get("k").map(String::as_str), Some("v"));
    assert_eq!(properties.get("option.k").map(String::as_str), Some("v"));
}
