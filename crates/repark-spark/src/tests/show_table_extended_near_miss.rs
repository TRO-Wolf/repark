use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::concat_batches;
use datafusion::sql::sqlparser::parser::ParserError;

use super::super::*;
use super::common::*;
use super::show_table_extended::*;

const UNCLOSED_BRACKETED_COMMENT_MESSAGE: &str = "[UNCLOSED_BRACKETED_COMMENT] Found an unclosed bracketed comment. Please, append */ at the end of the comment. SQLSTATE: 42601";

async fn answer_batch(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> RecordBatch {
    let frame = execute(ctx, catalogs, sql).await.expect(sql);
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let batches = frame.collect().await.expect(sql);
    concat_batches(&schema, &batches).expect(sql)
}

fn assert_diagnostic_parse_refusal(sql: &str, error: DataFusionError, expected: &str) {
    let DataFusionError::Diagnostic(diagnostic, inner) = error else {
        panic!("{sql}: expected a diagnostic parser error, got {error:?}");
    };
    assert_eq!(diagnostic.message, expected, "{sql}");
    let DataFusionError::SQL(parser_error, _) = inner.as_ref() else {
        panic!("{sql}: expected a SQL parser error inside the diagnostic");
    };
    let ParserError::ParserError(message) = parser_error.as_ref() else {
        panic!("{sql}: expected a parser message inside the diagnostic");
    };
    assert_eq!(message, expected, "{sql}");
}

#[tokio::test]
async fn show_table_extended_near_misses_keep_their_existing_paths() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pl (id BIGINT) USING iceberg TBLPROPERTIES ('k'='v')",
    )
    .await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    let show_tables = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("isTemporary", DataType::Boolean, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["sales"])),
            Arc::new(StringArray::from(vec!["pl"])),
            Arc::new(BooleanArray::from(vec![false])),
        ],
    )
    .unwrap();
    for sql in ["SHOW TABLES IN sales", "SHOW TABLES IN sales LIKE 'p*'"] {
        assert_eq!(
            answer_batch(&ctx, &catalogs, sql).await,
            show_tables,
            "{sql}"
        );
    }
    let tables_extended_sql = "SHOW TABLES EXTENDED IN sales LIKE '*'";
    let tables_extended = outcome(&ctx, &catalogs, tables_extended_sql)
        .await
        .expect_err("SHOW TABLES EXTENDED must keep its current parser refusal");
    assert_diagnostic_parse_refusal(
        tables_extended_sql,
        tables_extended,
        "Expected: end of statement, found: EXTENDED at Line: 1, Column: 13",
    );
    let properties = outcome(&ctx, &catalogs, "SHOW TBLPROPERTIES ice.sales.pl")
        .await
        .expect_err("SHOW TBLPROPERTIES must keep its current refusal");
    assert_analysis_refusal(
        "SHOW TBLPROPERTIES ice.sales.pl",
        properties,
        "SHOW [VARIABLE] is not supported unless information_schema is enabled",
    );
    for sql in ["SHOW TABLE", "SHOW TABLE ice.sales.pl"] {
        let error = outcome(&ctx, &catalogs, sql)
            .await
            .expect_err("SHOW TABLE must keep its current refusal");
        assert_analysis_refusal(
            sql,
            error,
            "SHOW [VARIABLE] is not supported unless information_schema is enabled",
        );
    }
    let create_statement = format!(
        "CREATE TABLE ice.sales.pl (\n  id BIGINT)\nUSING iceberg\nLOCATION '{}'\n\
         TBLPROPERTIES (\n  'current-snapshot-id' = 'none',\n  'format' = 'iceberg/parquet',\n  \
         'format-version' = '2',\n  'k' = 'v',\n  'write.parquet.compression-codec' = 'zstd')\n",
        table_location(&catalogs, "pl").await
    );
    let show_create = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "createtab_stmt",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec![create_statement]))],
    )
    .unwrap();
    assert_eq!(
        answer_batch(&ctx, &catalogs, "SHOW CREATE TABLE ice.sales.pl").await,
        show_create
    );
}

#[tokio::test]
async fn show_table_extended_skips_leading_and_inter_keyword_comments() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pc (id BIGINT) USING iceberg",
    )
    .await;
    let expected_columns = extended_schema();
    let properties = character_properties(&[
        ("current-snapshot-id", "none"),
        ("format", "iceberg/parquet"),
        ("format-version", "2"),
        ("write.parquet.compression-codec", "zstd"),
    ]);
    let location = table_location(&catalogs, "pc").await;
    let expected_rows = vec![managed_row(
        "pc",
        &location,
        &properties,
        None,
        "root\n |-- id: long (nullable = true)\n",
    )];
    let (columns, rows) = outcome(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc'",
    )
    .await
    .unwrap();
    assert_eq!(columns, expected_columns);
    assert_eq!(rows, expected_rows);
    for sql in [
        "/* c */ SHOW TABLE EXTENDED IN ice.sales LIKE 'pc'",
        "-- c\nSHOW TABLE EXTENDED IN ice.sales LIKE 'pc'",
        "SHOW/* c */TABLE /* d */ EXTENDED IN ice.sales LIKE 'pc'",
    ] {
        let (columns, rows) = outcome(&ctx, &catalogs, sql).await.unwrap();
        assert_eq!(columns, expected_columns, "{sql}");
        assert_eq!(rows, expected_rows, "{sql}");
    }
}

#[tokio::test]
async fn show_table_extended_refuses_unclosed_quotes_after_comments() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    for sql in [
        "/* c */ SHOW TABLE EXTENDED IN ice.sales LIKE 'pc",
        "-- c\nSHOW TABLE EXTENDED IN ice.sales LIKE 'pc",
        "SHOW /* c */ TABLE EXTENDED IN ice.sales LIKE 'pc",
        "/* it's */ SHOW TABLE EXTENDED IN ice.sales LIKE 'pc",
        "/* c */ SHOW TABLE EXTENDED IN ice.sales LIKE \"pc",
    ] {
        let error = outcome(&ctx, &catalogs, sql)
            .await
            .expect_err("an unclosed pattern quote must refuse");
        assert_parse_refusal(
            sql,
            error,
            if sql.ends_with("\"pc") {
                "[PARSE_SYNTAX_ERROR] Syntax error at or near '\"'. SQLSTATE: 42601"
            } else {
                "[PARSE_SYNTAX_ERROR] Syntax error at or near '''. SQLSTATE: 42601"
            },
        );
    }
}

#[tokio::test]
async fn show_table_extended_near_miss_probes_keep_their_exact_outcomes() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pc (id BIGINT) USING iceberg",
    )
    .await;
    let properties = character_properties(&[
        ("current-snapshot-id", "none"),
        ("format", "iceberg/parquet"),
        ("format-version", "2"),
        ("write.parquet.compression-codec", "zstd"),
    ]);
    let location = table_location(&catalogs, "pc").await;
    let expected = (
        extended_schema(),
        vec![managed_row(
            "pc",
            &location,
            &properties,
            None,
            "root\n |-- id: long (nullable = true)\n",
        )],
    );
    let single_semicolon = outcome(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc';",
    )
    .await
    .unwrap();
    let spaced_semicolons = outcome(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' ; ;",
    )
    .await
    .unwrap();
    assert_eq!(single_semicolon, expected);
    assert_eq!(spaced_semicolons, expected);

    let content_after_semicolons_sql = "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc';;x";
    let content_after_semicolons = outcome(&ctx, &catalogs, content_after_semicolons_sql)
        .await
        .expect_err("content after trailing semicolons must be refused");
    assert_parse_refusal(
        content_after_semicolons_sql,
        content_after_semicolons,
        "[PARSE_SYNTAX_ERROR] Syntax error: multiple SQL statements in one call are not supported (Spark parity). Only a single statement is accepted; a trailing semicolon, whitespace, or comment after that statement is allowed. SQLSTATE: 42601",
    );

    let unclosed_comment_sql = "/* c SHOW TABLE EXTENDED IN ice.sales LIKE 'pc";
    let unclosed_comment = outcome(&ctx, &catalogs, unclosed_comment_sql)
        .await
        .expect_err("the router front door must refuse an unclosed leading block comment");
    assert_parse_refusal(
        unclosed_comment_sql,
        unclosed_comment,
        UNCLOSED_BRACKETED_COMMENT_MESSAGE,
    );

    let inter_keyword_unclosed_comment_sql = "SHOW /* unclosed TABLE EXTENDED LIKE 'pc'";
    let inter_keyword_unclosed_comment =
        outcome(&ctx, &catalogs, inter_keyword_unclosed_comment_sql)
            .await
            .expect_err("the router front door must refuse an unclosed inter-keyword comment");
    assert_parse_refusal(
        inter_keyword_unclosed_comment_sql,
        inter_keyword_unclosed_comment,
        UNCLOSED_BRACKETED_COMMENT_MESSAGE,
    );

    let show_tables = outcome(
        &ctx,
        &catalogs,
        "/* c */ SHOW TABLES EXTENDED IN ice.sales LIKE 'pc'",
    )
    .await
    .expect_err("SHOW TABLES EXTENDED must keep its current parser outcome");
    assert_eq!(
        format!("{show_tables:?}"),
        "Diagnostic(Diagnostic { kind: Error, message: \"Expected: end of statement, found: EXTENDED at Line: 1, Column: 21\", span: Some(Span(Location(1,21)..Location(1,29))), notes: [], helps: [] }, SQL(ParserError(\"Expected: end of statement, found: EXTENDED at Line: 1, Column: 21\"), None))"
    );

    let extendedx = outcome(
        &ctx,
        &catalogs,
        "SHOW TABLE EXTENDEDX IN ice.sales LIKE 'pc'",
    )
    .await
    .expect_err("SHOW TABLE EXTENDEDX must keep its current planning outcome");
    assert_eq!(
        format!("{extendedx:?}"),
        "Plan(\"SHOW [VARIABLE] is not supported unless information_schema is enabled\")"
    );
}
