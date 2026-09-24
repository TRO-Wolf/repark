use super::super::*;
use super::accept_any_refusals::{
    ACCEPT_ANY, PLAIN, SEED, assert_illegal_argument, assert_invalid_schema, base_plus, base_types,
    column_types, door, refusal, register_view, sorted_rows,
};
use super::common::*;

const SRC: &str = "SELECT CAST(1 AS BIGINT) AS id, 'a' AS data";

async fn src_door(
    wh: &TempDir,
    create: &str,
    merge_schema: bool,
) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = door(wh, create, merge_schema).await;
    register_view(&ctx, "hsrc", SRC).await;
    (ctx, catalogs)
}

async fn added_after(statements: &[&str]) -> Vec<(String, String)> {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = src_door(&wh, ACCEPT_ANY, true).await;
    for statement in statements {
        run(&ctx, &catalogs, statement).await;
    }
    column_types(&catalogs).await
}

#[tokio::test]
async fn an_added_column_is_named_per_item_from_the_statement() {
    let cases: [(&str, &[(&str, &str)]); 13] = [
        (
            "INSERT INTO ice.sales.t SELECT *, 'z' AS NewC FROM hsrc",
            &[("NewC", "string")],
        ),
        (
            "INSERT INTO ice.sales.t SELECT id, upper(data), 'z' AS NewC FROM hsrc",
            &[("upper(data)", "string"), ("NewC", "string")],
        ),
        (
            "INSERT INTO ice.sales.t BY NAME SELECT id, upper(data) FROM hsrc",
            &[("upper(data)", "string")],
        ),
        (
            "INSERT INTO ice.sales.t SELECT hsrc.*, upper(data) FROM hsrc",
            &[("upper(data)", "string")],
        ),
        (
            "INSERT INTO ice.sales.t SELECT 9 AS id, -1",
            &[("-1", "int")],
        ),
        (
            "INSERT INTO ice.sales.t SELECT * FROM VALUES (9, 'z')",
            &[("col1", "int"), ("col2", "string")],
        ),
        (
            "INSERT INTO ice.sales.t SELECT * FROM (SELECT 9 AS id, 'z' AS NewC)",
            &[("NewC", "string")],
        ),
        (
            "INSERT INTO ice.sales.t WITH c AS (SELECT 9 AS id, 'z' AS NewC) SELECT * FROM c",
            &[("NewC", "string")],
        ),
        (
            "INSERT INTO ice.sales.t SELECT *, upper(data) FROM (SELECT id, data FROM hsrc)",
            &[("upper(data)", "string")],
        ),
        (
            "INSERT INTO ice.sales.t WITH c(id, NewC) AS (SELECT 9, 'z') SELECT * FROM c",
            &[("NewC", "string")],
        ),
        (
            "INSERT INTO ice.sales.t SELECT * FROM (SELECT 9, 'z') AS v(id, NewC)",
            &[("NewC", "string")],
        ),
        (
            "INSERT INTO ice.sales.t SELECT * FROM VALUES (9, 'z') AS v(id, NewC)",
            &[("NewC", "string")],
        ),
        (
            "INSERT INTO ice.sales.t WITH c AS (SELECT 9 AS id, 'z' AS NewC) \
             SELECT * FROM c AS x(id, Other)",
            &[("Other", "string")],
        ),
    ];
    for (statement, added) in cases {
        assert_eq!(
            added_after(&[statement]).await,
            base_plus(added),
            "{statement}"
        );
    }
    let names: Vec<String> =
        added_after(&["INSERT INTO ice.sales.t VALUES (9, 'z') UNION ALL SELECT 8, 'y'"])
            .await
            .into_iter()
            .map(|(name, _)| name)
            .collect();
    assert_eq!(names, ["id", "data", "cat", "col1", "col2"]);
}

const ALIAS_LISTS: [(&str, &str); 4] = [
    (
        "INSERT INTO ice.sales.t WITH c(id, NewC) AS (SELECT 9, 'z') SELECT * FROM c",
        "NewC",
    ),
    (
        "INSERT INTO ice.sales.t SELECT * FROM (SELECT 9, 'z') AS v(id, NewC)",
        "NewC",
    ),
    (
        "INSERT INTO ice.sales.t SELECT * FROM VALUES (9, 'z') AS v(id, NewC)",
        "NewC",
    ),
    (
        "INSERT INTO ice.sales.t WITH c AS (SELECT 9 AS id, 'z' AS NewC) \
         SELECT * FROM c AS x(id, Other)",
        "Other",
    ),
];

#[tokio::test]
async fn a_column_alias_list_names_the_added_column_and_its_rows() {
    for (sql, added) in ALIAS_LISTS {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = src_door(&wh, ACCEPT_ANY, true).await;
        run(&ctx, &catalogs, sql).await;
        assert_eq!(
            column_types(&catalogs).await,
            base_plus(&[(added, "string")]),
            "{sql}"
        );
        assert_eq!(
            sorted_rows(&ctx, &catalogs).await,
            vec![format!(
                "| 9  |      |     | {:<width$} |",
                "z",
                width = added.len()
            )],
            "{sql}"
        );
    }
}

#[tokio::test]
async fn without_the_conf_a_column_alias_list_names_the_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = src_door(&wh, ACCEPT_ANY, false).await;
    for (sql, added) in ALIAS_LISTS {
        assert_illegal_argument(
            &ctx,
            &catalogs,
            sql,
            &format!("Field {added} not found in source schema"),
        )
        .await;
    }
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
    assert_eq!(column_types(&catalogs).await, base_types());
}

#[tokio::test]
async fn a_repeated_star_refuses_with_the_iceberg_duplicate_text() {
    for merge_schema in [false, true] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = src_door(&wh, ACCEPT_ANY, merge_schema).await;
        for sql in [
            "INSERT INTO ice.sales.t SELECT *, * FROM hsrc",
            "INSERT INTO ice.sales.t SELECT hsrc.*, hsrc.* FROM hsrc",
            "INSERT INTO ice.sales.t BY NAME SELECT *, * FROM hsrc",
            "INSERT OVERWRITE ice.sales.t SELECT *, * FROM hsrc",
        ] {
            assert_invalid_schema(
                &ctx,
                &catalogs,
                sql,
                "Invalid schema: multiple fields for name id: 0 and 2",
            )
            .await;
        }
        assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
        assert_eq!(column_types(&catalogs).await, base_types());
    }
}

#[tokio::test]
async fn a_typed_suffix_literal_refusal_never_quotes_the_internal_marker() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = src_door(&wh, ACCEPT_ANY, true).await;
    for (literal, quoted) in [
        ("9L", "CAST(9 AS BIGINT)"),
        ("1.5D", "CAST(1.5 AS DOUBLE)"),
        ("2BD", "CAST(2 AS DECIMAL(1,0))"),
    ] {
        let sql = format!("INSERT INTO ice.sales.t SELECT id, {literal} FROM hsrc");
        let mapped = refusal(&ctx, &catalogs, &sql).await;
        assert!(
            matches!(mapped, repark_common::Error::NotImplemented(ref message)
                if message.contains(&format!("`{quoted}`"))
                    && !message.contains("__repark")),
            "{sql}: got {mapped:?}"
        );
    }
    assert_eq!(column_types(&catalogs).await, base_types());
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
}

#[tokio::test]
async fn a_function_column_is_written_under_its_spark_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = src_door(&wh, ACCEPT_ANY, true).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT id, upper(data), 'z' AS NewC FROM hsrc",
    )
    .await;
    assert_eq!(
        sorted_rows(&ctx, &catalogs).await,
        vec!["| 1  |      |     | A           | z    |".to_string()]
    );
}

#[tokio::test]
async fn the_refusal_names_an_unaliased_expression_as_spark_does() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = src_door(&wh, ACCEPT_ANY, false).await;
    for (source, expected) in [
        ("SELECT id, upper(data) FROM hsrc", "upper(data)"),
        ("SELECT -1, 'z'", "-1"),
        ("VALUES (9, 'z') UNION ALL SELECT 8, 'y'", "col1"),
        ("SELECT * FROM VALUES (9, 'z')", "col1"),
        ("SELECT id, UPPER(data) FROM hsrc", "upper(data)"),
        ("SELECT id, upper(hsrc.DATA) FROM hsrc", "upper(DATA)"),
        ("SELECT s.id, upper(s.data) FROM hsrc s", "upper(data)"),
        ("SELECT id, ucase(data) FROM hsrc", "ucase(data)"),
        ("SELECT id, concat(data, 'x') FROM hsrc", "concat(data, x)"),
        (
            "SELECT id, data || 'x' || 'y' FROM hsrc",
            "concat(concat(data, x), y)",
        ),
        ("SELECT id, id * 2 + 1 FROM hsrc", "((id * 2) + 1)"),
        ("SELECT id, 1 + 2 * 3 FROM hsrc", "(1 + (2 * 3))"),
        ("SELECT id, -id FROM hsrc", "(- id)"),
        ("SELECT id, -(1) FROM hsrc", "(- 1)"),
        ("SELECT id, -(-1) FROM hsrc", "(- -1)"),
        ("SELECT id, +1 FROM hsrc", "(+ 1)"),
        ("SELECT id, abs(-id) FROM hsrc", "abs((- id))"),
        ("SELECT id, id <> 1 FROM hsrc", "(NOT (id = 1))"),
        (
            "SELECT id, id > 0 AND id < 3 FROM hsrc",
            "((id > 0) AND (id < 3))",
        ),
        (
            "SELECT id, id = 1 OR id = 2 FROM hsrc",
            "((id = 1) OR (id = 2))",
        ),
        ("SELECT id, NOT (id > 1) FROM hsrc", "(NOT (id > 1))"),
        (
            "SELECT id, data IS NOT NULL FROM hsrc",
            "(data IS NOT NULL)",
        ),
        (
            "SELECT id, coalesce(NULL, 'x') FROM hsrc",
            "coalesce(NULL, x)",
        ),
        (
            "SELECT id, length(data) + 1 FROM hsrc",
            "(length(data) + 1)",
        ),
        ("SELECT id, current_date() FROM hsrc", "current_date()"),
        ("SELECT id, -0.0 FROM hsrc", "0.0"),
        ("SELECT id, -0.50 FROM hsrc", "-0.50"),
        ("SELECT id, (1) FROM hsrc", "1"),
        ("SELECT id, TRUE FROM hsrc", "true"),
        ("SELECT id, NULL FROM hsrc", "NULL"),
        ("SELECT id, '' FROM hsrc", ""),
    ] {
        assert_illegal_argument(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.t {source}"),
            &format!("Field {expected} not found in source schema"),
        )
        .await;
    }
    assert_invalid_schema(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT id, CAST(id AS STRING) FROM hsrc",
        "Invalid schema: multiple fields for name id: 0 and 1",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
    assert_eq!(column_types(&catalogs).await, base_types());
}

#[tokio::test]
async fn an_expression_spark_names_otherwise_refuses_before_any_evolution() {
    for merge_schema in [true, false] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = src_door(&wh, ACCEPT_ANY, merge_schema).await;
        for sql in [
            "INSERT INTO ice.sales.t SELECT id, CASE WHEN id > 0 THEN 1 END FROM hsrc",
            "INSERT INTO ice.sales.t SELECT id, substr(data, -1) FROM hsrc",
            "INSERT INTO ice.sales.t BY NAME SELECT id, ceil(1.2) FROM hsrc",
            "INSERT OVERWRITE ice.sales.t SELECT id, 1e3 FROM hsrc",
        ] {
            let mapped = refusal(&ctx, &catalogs, sql).await;
            assert!(
                matches!(mapped, repark_common::Error::NotImplemented(ref message)
                    if message.contains("as Spark names it")
                        && !message.contains("datafusion")),
                "{sql}: got {mapped:?}"
            );
        }
        assert_eq!(column_types(&catalogs).await, base_types());
        assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
    }
}

#[tokio::test]
async fn a_plain_table_names_an_extra_expression_as_spark_does() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = src_door(&wh, PLAIN, false).await;
    let mapped = refusal(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t BY NAME SELECT id, upper(data) FROM hsrc",
    )
    .await;
    assert!(
        mapped.to_string().ends_with(
            "[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS] Cannot write incompatible data for the \
             table `ice`.`sales`.`t`: Cannot write extra columns `upper(data)`. SQLSTATE: KD000"
        ),
        "got {mapped}"
    );
}

#[tokio::test]
async fn an_empty_positional_overwrite_wipes_the_table() {
    for (create, merge_schema, sql, added) in [
        (
            ACCEPT_ANY,
            false,
            "INSERT OVERWRITE ice.sales.t SELECT 9 AS id WHERE false",
            &[][..],
        ),
        (
            ACCEPT_ANY,
            true,
            "INSERT OVERWRITE ice.sales.t SELECT 9 AS id, 'z' AS NewC WHERE false",
            &[("NewC", "string")][..],
        ),
        (
            ACCEPT_ANY,
            false,
            "INSERT OVERWRITE ice.sales.t BY NAME SELECT 9 AS id WHERE false",
            &[][..],
        ),
        (
            PLAIN,
            false,
            "INSERT OVERWRITE ice.sales.t BY NAME SELECT 9 AS id WHERE false",
            &[][..],
        ),
        (
            PLAIN,
            false,
            "INSERT OVERWRITE ice.sales.t BY NAME SELECT 9 AS id, NULL AS data, 'c' AS cat \
             WHERE false",
            &[][..],
        ),
    ] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = door(&wh, create, merge_schema).await;
        run(&ctx, &catalogs, SEED).await;
        run(&ctx, &catalogs, sql).await;
        assert_eq!(
            rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await,
            0,
            "{sql}"
        );
        assert_eq!(column_types(&catalogs).await, base_plus(added), "{sql}");
    }
}
