/// BUG-010: genuine multi-statement refuses as Parse (Spark `PARSE_SYNTAX_ERROR` class).
use super::super::*;
use super::common::*;

const UNCLOSED_BRACKETED_COMMENT_RENDERED_MESSAGE: &str = "SQL error: ParserError(\"[UNCLOSED_BRACKETED_COMMENT] Found an unclosed bracketed comment. Please, append */ at the end of the comment. SQLSTATE: 42601\")";

#[test]
fn planner_default_set_recognizer_pins_malformed_near_misses() {
    for sql in [
        "",
        "SE",
        "SELECT datafusion.catalog.default_catalog = 'ice'",
        "SETX datafusion.catalog.default_catalog = 'ice'",
        "SET datafusion.catalog.default_catalog_extra = 'ice'",
        "/* unclosed SET datafusion.catalog.default_catalog = 'ice'",
        "SETdatafusion.catalog.default_catalog = 'ice'",
    ] {
        assert!(
            crate::router::planner_default_set_side(sql).is_none(),
            "{sql}"
        );
    }
    assert!(matches!(
        crate::router::planner_default_set_side(
            "-- lead\nSeT/* comment */datafusion.catalog.default_catalog /* c */ = 'ice'"
        ),
        Some(crate::router::PlannerDefaultSide::Catalog)
    ));
    assert!(matches!(
        crate::router::planner_default_set_side(
            "set DataFusion.Catalog.Default_Schema --c\r= 'sales'"
        ),
        Some(crate::router::PlannerDefaultSide::Namespace)
    ));
}

async fn assert_single_value_answer(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    field: Field,
    column: Arc<dyn Array>,
) {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql:?} must answer one row: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql:?} must collect: {error}"));
    let schema = Arc::new(Schema::new(vec![field]));
    assert_eq!(batches.len(), 1, "{sql:?}");
    assert_eq!(batches[0].schema(), schema, "{sql:?}");
    let expected = RecordBatch::try_new(schema, vec![column])
        .unwrap_or_else(|error| panic!("{sql:?} expected batch: {error}"));
    assert_eq!(batches[0], expected, "{sql:?}");
}

#[tokio::test]
async fn semicolons_inside_literals_and_comments_do_not_trigger_multi_statement_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let cases: [(&str, Field, Arc<dyn Array>); 3] = [
        (
            "SELECT ';'",
            Field::new("Utf8(\";\")", DataType::Utf8, false),
            Arc::new(StringArray::from(vec![";"])),
        ),
        (
            "SELECT 1 /* ; */",
            Field::new("Int64(1)", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![1])),
        ),
        (
            "SELECT 1 -- ;\n",
            Field::new("Int64(1)", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![1])),
        ),
    ];
    for (sql, field, column) in cases {
        assert_single_value_answer(&ctx, &catalogs, sql, field, column).await;
    }
}

#[tokio::test]
async fn bug010_multi_statement_refuses_parse_class() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for sql in [
        "SELECT 1; SELECT 2",
        "select 1; select 2",
        "SELECT 1; SELECT 2;",
        "SELECT 1;\nSELECT 2",
        "SELECT 1; XYZZY 2",
        "SELECT 1; NOT_A_STATEMENT",
    ] {
        let err = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("multi-statement must refuse");
        assert!(matches!(&err, DataFusionError::SQL(_, _)));
        assert_eq!(
            err.to_string(),
            "SQL error: ParserError(\"[PARSE_SYNTAX_ERROR] Syntax error: multiple SQL statements in one call are not supported (Spark parity). Only a single statement is accepted; a trailing semicolon, whitespace, or comment after that statement is allowed. SQLSTATE: 42601\")",
            "{sql}"
        );
        let mapped = repark_core::engine_err(err);
        let repark_common::Error::Parse(message) = &mapped else {
            panic!("{sql}: expected a Parse error, got {mapped:?}");
        };
        assert_eq!(
            message,
            "[PARSE_SYNTAX_ERROR] Syntax error: multiple SQL statements in one call are not supported (Spark parity). Only a single statement is accepted; a trailing semicolon, whitespace, or comment after that statement is allowed. SQLSTATE: 42601",
            "{sql}"
        );
        assert_eq!(mapped.to_string(), *message, "{sql}");
    }
}

#[tokio::test]
async fn unclosed_bracketed_comments_use_spark_parser_contract() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for sql in [
        "SELECT 1; /* unclosed",
        "SELECT 1 /* c",
        "/* c",
        "SHOW TABLES IN sc.sales /* c",
        "SELECT 1 /* a /* b */",
        "SELECT 1 /*",
        "SELECT 1;; /* c",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("unclosed comment must refuse");
        assert!(
            matches!(&error, DataFusionError::SQL(_, _)),
            "{sql}: {error:?}"
        );
        assert_eq!(
            error.to_string(),
            UNCLOSED_BRACKETED_COMMENT_RENDERED_MESSAGE,
            "{sql}"
        );
    }
}

#[tokio::test]
async fn bracketed_comment_near_misses_keep_exact_single_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let cases: [(&str, Field, Arc<dyn Array>); 3] = [
        (
            "SELECT 1 /* a /* b */ */",
            Field::new("Int64(1)", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![1])),
        ),
        (
            "SELECT '/* x'",
            Field::new("Utf8(\"/* x\")", DataType::Utf8, false),
            Arc::new(StringArray::from(vec!["/* x"])),
        ),
        (
            "SELECT 1 -- /* x",
            Field::new("Int64(1)", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![1])),
        ),
    ];
    for (sql, field, column) in cases {
        assert_single_value_answer(&ctx, &catalogs, sql, field, column).await;
    }
}

#[tokio::test]
async fn quote_escape_and_carriage_return_near_misses_keep_exact_single_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let cases: [(&str, Field, Arc<dyn Array>); 4] = [
        (
            "SELECT 1 AS `/* x`",
            Field::new("/* x", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![1])),
        ),
        (
            "SELECT 'a\\'/* x'",
            Field::new("Utf8(\"a'/* x\")", DataType::Utf8, false),
            Arc::new(StringArray::from(vec!["a'/* x"])),
        ),
        (
            "SELECT \"a\\\"/* x\"",
            Field::new("Utf8(\"a\"/* x\")", DataType::Utf8, false),
            Arc::new(StringArray::from(vec!["a\"/* x"])),
        ),
        (
            "SELECT 1 --c\r/* x */",
            Field::new("Int64(1)", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![1])),
        ),
    ];
    for (sql, field, column) in cases {
        assert_single_value_answer(&ctx, &catalogs, sql, field, column).await;
    }
}

#[tokio::test]
async fn malformed_multi_statement_near_misses_keep_exact_outcomes() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let unclosed = execute(&ctx, &catalogs, "SELECT 1; /* unclosed")
        .await
        .expect_err("unclosed comment must refuse");
    assert!(matches!(&unclosed, DataFusionError::SQL(_, _)));
    assert_eq!(
        unclosed.to_string(),
        UNCLOSED_BRACKETED_COMMENT_RENDERED_MESSAGE
    );

    let mut actual = Vec::new();
    for sql in [
        "SELECT 'unterminated; SELECT 2",
        "SELECT /*+ BROADCAST(t) 1",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("malformed SQL must fail");
        assert!(
            matches!(&error, DataFusionError::SQL(_, _)),
            "{sql}: {error:?}"
        );
        actual.push(error.to_string());
    }
    assert_eq!(
        actual,
        vec![
            "SQL error: TokenizerError(\"Unterminated string literal at Line: 1, Column: 8\")"
                .to_string(),
            "SQL error: TokenizerError(\"Unexpected EOF while in a multi-line comment at Line: 1, Column: 26\")"
                .to_string(),
        ]
    );
}

/// BUG-010 oracle boundary: trailing `;` / whitespace / comments after a single statement OK.
#[tokio::test]
async fn bug010_trailing_semicolon_whitespace_comments_allowed() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let expected = execute(&ctx, &catalogs, "SELECT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for sql in [
        "SELECT 1;",
        "SELECT 1;  ",
        "SELECT 1; -- trailing comment",
        "SELECT 1 /* mid */; ",
        "SELECT 1;/*c*/",
        "SELECT 1;;",
        "SELECT 1;\n-- trailing comment\n",
        "  SELECT 1  ;  ",
        "SELECT 1 /* mid */; /* after */",
        "SELECT 1; /* only comment after */",
        "-- lead\nSELECT 1;",
    ] {
        let actual = execute(&ctx, &catalogs, sql)
            .await
            .unwrap_or_else(|err| panic!("single-stmt trailing form must pass: {sql:?}: {err}"))
            .collect()
            .await
            .unwrap_or_else(|err| panic!("collect failed for {sql:?}: {err}"));
        assert_eq!(actual, expected, "{sql}");
    }
}

/// A bare `INSERT INTO` applies its write even when the returned `DataFrame` is not collected.
#[tokio::test]
async fn bare_insert_applies_without_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    execute_without_collecting(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (10, 'x')").await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 4);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t WHERE id = 10").await,
        1
    );
}

/// C-2: lazy routing must not silently drop a bare `DELETE` when its `DataFrame` is not collected.
#[tokio::test]
async fn bare_delete_applies_without_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    execute_without_collecting(&ctx, &catalogs, "DELETE FROM ice.sales.t WHERE id = 2").await;

    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 2);
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t WHERE id = 2").await,
        0
    );
}

/// C-3: lazy routing must not silently drop a bare `UPDATE` when its `DataFrame` is not collected.
#[tokio::test]
async fn bare_update_applies_without_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    execute_without_collecting(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.t SET name = 'updated' WHERE id > 1",
    )
    .await;

    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.t WHERE name = 'updated'"
        )
        .await,
        2
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            "SELECT * FROM ice.sales.t WHERE id = 1 AND name = 'a'"
        )
        .await,
        1
    );
}

/// C-4 exactly-once: INSERT applies at `sql()`; collecting the `DataFrame` does not insert again.
#[tokio::test]
async fn insert_applies_exactly_once_across_a_later_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;

    let returned = execute(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (10, 'x')")
        .await
        .unwrap();
    // Eager: the row is already present before the returned DataFrame is collected.
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t WHERE id = 10").await,
        1,
        "the INSERT must be applied eagerly at execute() time"
    );

    // No double-apply: collecting the returned DataFrame must not insert a second copy.
    returned.collect().await.unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t WHERE id = 10").await,
        1,
        "collecting the returned DataFrame must not re-run the INSERT"
    );
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 4);
}

/// C-5 boundary: eager DML must NOT make a SELECT eager.
#[tokio::test]
async fn erroring_select_resolves_at_sql_and_errors_only_on_collect() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let dataframe = execute(&ctx, &catalogs, "SELECT CAST(name AS INT) AS n FROM src")
        .await
        .expect("a lazy SELECT resolves at sql() time without executing");
    assert!(
        dataframe.collect().await.is_err(),
        "the runtime CAST error must surface only on collect, not at sql()"
    );
}

/// Eager DML surfaces runtime failure at `sql()` time and commits nothing.
#[tokio::test]
async fn failing_dml_surfaces_its_runtime_error_at_sql_time() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nums AS SELECT id FROM src",
    )
    .await;

    // INSERT ...
    let result = execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.nums SELECT CAST(name AS INT) FROM src",
    )
    .await;
    assert!(
        result.is_err(),
        "an eagerly-applied DML must raise its runtime failure at execute()/sql() time"
    );
    // The failed write committed nothing new.
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.nums").await,
        3
    );
}
