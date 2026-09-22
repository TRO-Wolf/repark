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
