use super::super::*;
use super::common::*;

async fn props_of(catalogs: &CatalogRegistry, table: &str) -> HashMap<String, String> {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .properties()
        .clone()
}

#[tokio::test]
async fn unset_tblproperties_takes_only_the_if_exists_pair() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.p (id INT) USING iceberg TBLPROPERTIES ('k' = 'v', 'j' = 'w')",
    )
    .await;
    let before = props_of(&catalogs, "p").await;
    let missing_exists =
        "[PARSE_SYNTAX_ERROR] Syntax error at or near '(': missing 'EXISTS'. SQLSTATE: 42601";
    let cases = [
        ("IF ('nope')", missing_exists.to_string()),
        ("if ('k')", missing_exists.to_string()),
        (
            "EXISTS ('nope')",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXISTS': extra input 'EXISTS'. \
             SQLSTATE: 42601"
                .to_string(),
        ),
        (
            "exists IF ('k')",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'exists': extra input 'exists'. \
             SQLSTATE: 42601"
                .to_string(),
        ),
        (
            "IF",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601"
                .to_string(),
        ),
    ];
    for (tail, expected) in cases {
        let sql = format!("ALTER TABLE ice.sales.p UNSET TBLPROPERTIES {tail}");
        let error = execute(&ctx, &catalogs, &sql).await.expect_err(&sql);
        let mapped = repark_core::engine_err(error);
        let repark_common::Error::Parse(message) = &mapped else {
            panic!("{sql}: expected a Parse error, got {mapped:?}");
        };
        assert_eq!(message, &expected, "{sql}");
    }
    assert_eq!(props_of(&catalogs, "p").await, before);
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.p UNSET TBLPROPERTIES if exists ('k', 'nope')",
    )
    .await;
    let after = props_of(&catalogs, "p").await;
    assert!(!after.contains_key("k"));
    assert_eq!(after.get("j").map(String::as_str), Some("w"));
}
