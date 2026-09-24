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

const NEAR_SINGLE_QUOTE: &str = "[PARSE_SYNTAX_ERROR] Syntax error at or near '''. SQLSTATE: 42601";
const NEAR_DOUBLE_QUOTE: &str =
    "[PARSE_SYNTAX_ERROR] Syntax error at or near '\"'. SQLSTATE: 42601";
const NEAR_BACKTICK: &str = "[PARSE_SYNTAX_ERROR] Syntax error at or near '`'. SQLSTATE: 42601";
const NEAR_LEFT_PAREN: &str = "[PARSE_SYNTAX_ERROR] Syntax error at or near '('. SQLSTATE: 42601";
const NEAR_END_OF_INPUT: &str =
    "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601";

fn assert_scanner_refusal(sql: &str, expected: &str) {
    let Some(Err(error)) = crate::show_table_extended::try_parse_show_table_extended(sql) else {
        panic!("{sql}: expected a SHOW TABLE EXTENDED parse refusal");
    };
    assert_parse_refusal(sql, error, expected);
}

fn assert_scanner_accepts(sql: &str, scope: &[&str], pattern: &str, has_partition: bool) {
    let Some(Ok(parsed)) = crate::show_table_extended::try_parse_show_table_extended(sql) else {
        panic!("{sql}: expected a SHOW TABLE EXTENDED parse");
    };
    assert_eq!(
        parsed,
        crate::show_table_extended::ShowTableExtended {
            scope: Some(scope.iter().map(|part| (*part).to_string()).collect()),
            pattern: pattern.to_string(),
            has_partition,
        },
        "{sql}"
    );
}

async fn assert_end_to_end_refusal(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    expected: &str,
) {
    let error = outcome(ctx, catalogs, sql)
        .await
        .expect_err("the statement must refuse");
    assert_parse_refusal(sql, error, expected);
    let mapped = repark_core::engine_err(
        outcome(ctx, catalogs, sql)
            .await
            .expect_err("the statement must refuse again"),
    );
    let repark_common::Error::Parse(message) = &mapped else {
        panic!("{sql}: expected a Parse error, got {mapped:?}");
    };
    assert_eq!(message, expected, "{sql}");
}

async fn seed_pc(ctx: &SessionContext, catalogs: &CatalogRegistry) -> (Schema, Vec<ExtendedRow>) {
    run(
        ctx,
        catalogs,
        "CREATE TABLE ice.sales.pc (id BIGINT) USING iceberg",
    )
    .await;
    let properties = character_properties(&[
        ("current-snapshot-id", "none"),
        ("format", "iceberg/parquet"),
        ("format-version", "2"),
        ("write.parquet.compression-codec", "zstd"),
    ]);
    let location = table_location(catalogs, "pc").await;
    (
        extended_schema(),
        vec![managed_row(
            "pc",
            &location,
            &properties,
            None,
            "root\n |-- id: long (nullable = true)\n",
        )],
    )
}

#[tokio::test]
async fn show_table_extended_refuses_doubled_delimiters_before_an_unclosed_quote() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    for (sql, expected) in [
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE 'p''c",
            NEAR_SINGLE_QUOTE,
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE \"p\"\"c",
            NEAR_DOUBLE_QUOTE,
        ),
        (
            "SHOW TABLE EXTENDED IN ice.`sa``les` LIKE 'pc",
            NEAR_SINGLE_QUOTE,
        ),
        (
            "SHOW TABLE EXTENDED IN ice.`sa``les LIKE 'pc'",
            NEAR_BACKTICK,
        ),
    ] {
        assert_scanner_refusal(sql, expected);
        assert_end_to_end_refusal(&ctx, &catalogs, sql, expected).await;
    }
}

#[tokio::test]
async fn show_table_extended_balanced_doubled_delimiters_keep_their_answers() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    for (sql, pattern) in [
        ("SHOW TABLE EXTENDED IN ice.sales LIKE 'p''c'", "p'c"),
        ("SHOW TABLE EXTENDED IN ice.sales LIKE \"p\"\"c\"", "p\"c"),
    ] {
        assert_scanner_accepts(sql, &["ice", "sales"], pattern, false);
        assert_eq!(
            outcome(&ctx, &catalogs, sql).await.unwrap(),
            (extended_schema(), Vec::new()),
            "{sql}"
        );
    }
    let backtick_sql = "SHOW TABLE EXTENDED IN ice.`sa``les` LIKE 'pc'";
    assert_scanner_accepts(backtick_sql, &["ice", "sa`les"], "pc", false);
    let missing_schema = outcome(&ctx, &catalogs, backtick_sql)
        .await
        .expect_err("the doubled-backtick namespace does not exist");
    assert_analysis_refusal(
        backtick_sql,
        missing_schema,
        "[SCHEMA_NOT_FOUND] The schema `ice`.`sa`les` cannot be found. Verify the spelling and \
         correctness of the schema and catalog. If you did not qualify the name with a catalog, \
         verify the current_schema() output, or qualify the name with the correct catalog. To \
         tolerate the error on drop use DROP SCHEMA IF EXISTS. SQLSTATE: 42704",
    );
}

#[tokio::test]
async fn show_table_extended_scanner_keeps_comment_markers_inside_open_quotes() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    for sql in [
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'p/*c",
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'p--c",
    ] {
        assert_scanner_refusal(sql, NEAR_SINGLE_QUOTE);
        assert_end_to_end_refusal(&ctx, &catalogs, sql, NEAR_SINGLE_QUOTE).await;
    }
    for (sql, pattern) in [
        ("SHOW TABLE EXTENDED IN ice.sales LIKE 'p/*c'", "p/*c"),
        ("SHOW TABLE EXTENDED IN ice.sales LIKE 'p--c'", "p--c"),
    ] {
        assert_scanner_accepts(sql, &["ice", "sales"], pattern, false);
        assert_eq!(
            outcome(&ctx, &catalogs, sql).await.unwrap(),
            (extended_schema(), Vec::new()),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn show_table_extended_scanner_skips_quotes_inside_comments() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let expected = seed_pc(&ctx, &catalogs).await;
    for sql in [
        "-- it's\nSHOW TABLE EXTENDED IN ice.sales LIKE \"pc",
        "/* a /* it's */ b */ SHOW TABLE EXTENDED IN ice.sales LIKE \"pc",
    ] {
        assert_scanner_refusal(sql, NEAR_DOUBLE_QUOTE);
        assert_end_to_end_refusal(&ctx, &catalogs, sql, NEAR_DOUBLE_QUOTE).await;
    }
    for sql in [
        "-- it's\nSHOW TABLE EXTENDED IN ice.sales LIKE \"pc\"",
        "/* a /* it's */ b */ SHOW TABLE EXTENDED IN ice.sales LIKE \"pc\"",
    ] {
        assert_scanner_accepts(sql, &["ice", "sales"], "pc", false);
        assert_eq!(
            outcome(&ctx, &catalogs, sql).await.unwrap(),
            expected,
            "{sql}"
        );
    }
}

#[tokio::test]
async fn show_table_extended_scanner_stops_at_an_unterminated_block_comment() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let expected = seed_pc(&ctx, &catalogs).await;
    let open_sql = "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' /* open";
    assert_scanner_refusal(open_sql, NEAR_END_OF_INPUT);
    assert_end_to_end_refusal(
        &ctx,
        &catalogs,
        open_sql,
        UNCLOSED_BRACKETED_COMMENT_MESSAGE,
    )
    .await;
    let closed_sql = "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' /* closed */";
    assert_scanner_accepts(closed_sql, &["ice", "sales"], "pc", false);
    assert_eq!(
        outcome(&ctx, &catalogs, closed_sql).await.unwrap(),
        expected
    );
}

#[tokio::test]
async fn show_table_extended_partition_keyword_without_parentheses_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    let sql = "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION";
    assert_scanner_refusal(sql, NEAR_END_OF_INPUT);
    assert_end_to_end_refusal(&ctx, &catalogs, sql, NEAR_END_OF_INPUT).await;
}

#[tokio::test]
async fn show_table_extended_partition_spec_refuses_a_nested_parenthesis() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    for sql in [
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (a=(1))",
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (a=(1)",
        "SHOW TABLE EXTENDED LIKE 'pc' PARTITION (a=(1))",
    ] {
        assert_scanner_refusal(sql, NEAR_LEFT_PAREN);
        assert_end_to_end_refusal(&ctx, &catalogs, sql, NEAR_LEFT_PAREN).await;
    }
}

#[tokio::test]
async fn show_table_extended_partition_spec_near_misses_keep_their_answers() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    for sql in [
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (cat='a')",
        "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (cat='(')",
    ] {
        assert_scanner_accepts(sql, &["ice", "sales"], "pc", true);
        let refused = outcome(&ctx, &catalogs, sql)
            .await
            .expect_err("a partition clause on an existing table refuses");
        assert_analysis_refusal(
            sql,
            refused,
            "[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED] The partition \
             command is invalid. Table `ice`.`sales`.`pc` does not support partition management. \
             SQLSTATE: 42601",
        );
    }
    let unclosed_sql = "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (cat='a'";
    assert_scanner_refusal(unclosed_sql, NEAR_END_OF_INPUT);
    assert_end_to_end_refusal(&ctx, &catalogs, unclosed_sql, NEAR_END_OF_INPUT).await;
}

#[tokio::test]
async fn show_table_extended_malformed_partition_specs_refuse_at_the_spark_token() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    for (spec, near) in [
        ("()", "')'"),
        ("(a=1,)", "')'"),
        ("(,a=1)", "','"),
        ("(=1)", "'='"),
        ("(a=)", "')'"),
        ("(1=1)", "'1'"),
        ("('a'=1)", "''a''"),
    ] {
        let sql = format!("SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION {spec}");
        let expected =
            format!("[PARSE_SYNTAX_ERROR] Syntax error at or near {near}. SQLSTATE: 42601");
        assert_scanner_refusal(&sql, &expected);
        assert_end_to_end_refusal(&ctx, &catalogs, &sql, &expected).await;
    }
}

#[tokio::test]
async fn show_table_extended_well_formed_partition_specs_keep_the_partition_refusal() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    for spec in ["(a=1, b='x')", "(a=-1)", "(a=DATE '2020-01-01')"] {
        let sql = format!("SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION {spec}");
        assert_scanner_accepts(&sql, &["ice", "sales"], "pc", true);
        let refused = outcome(&ctx, &catalogs, &sql)
            .await
            .expect_err("a partition clause on an existing table refuses");
        assert_analysis_refusal(
            &sql,
            refused,
            "[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED] The partition \
             command is invalid. Table `ice`.`sales`.`pc` does not support partition management. \
             SQLSTATE: 42601",
        );
    }
}

const PARTITION_MANAGEMENT_UNSUPPORTED: &str = "[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED] The partition command is invalid. Table `ice`.`sales`.`pc` does not support partition management. SQLSTATE: 42601";

#[tokio::test]
async fn show_table_extended_valueless_partition_key_refuses_with_spark_empty_partition_value() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    for (spec, key) in [
        ("(a)", "a"),
        ("(a, b)", "a"),
        ("(b=1, a)", "a"),
        ("(a, b=1)", "a"),
        ("(a, B)", "a"),
        ("(A)", "A"),
        ("(`A`)", "A"),
        ("(`x y`)", "x y"),
    ] {
        let sql = format!("SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION {spec}");
        let expected = format!(
            "[INVALID_SQL_SYNTAX.EMPTY_PARTITION_VALUE] Invalid SQL syntax: Partition key `{key}` \
             must set value. SQLSTATE: 42000"
        );
        assert_scanner_refusal(&sql, &expected);
        assert_end_to_end_refusal(&ctx, &catalogs, &sql, &expected).await;
    }
}

#[tokio::test]
async fn show_table_extended_trailing_token_outranks_a_valueless_partition_key() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    let sql = "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (a) garbage";
    let expected = "[PARSE_SYNTAX_ERROR] Syntax error at or near 'garbage': extra input 'garbage'. \
         SQLSTATE: 42601";
    assert_scanner_refusal(sql, expected);
    assert_end_to_end_refusal(&ctx, &catalogs, sql, expected).await;
}

#[tokio::test]
async fn show_table_extended_keyword_partition_key_keeps_the_partition_refusal() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    let sql = "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (select=1)";
    assert_scanner_accepts(sql, &["ice", "sales"], "pc", true);
    let refused = outcome(&ctx, &catalogs, sql)
        .await
        .expect_err("a partition clause on an existing table refuses");
    assert_analysis_refusal(sql, refused, PARTITION_MANAGEMENT_UNSUPPORTED);
}

#[tokio::test]
async fn show_table_extended_residue_r_u4_13_space_separated_values_keep_the_partition_refusal() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    let sql = "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (a=1 b=2)";
    assert_scanner_accepts(sql, &["ice", "sales"], "pc", true);
    let refused = outcome(&ctx, &catalogs, sql)
        .await
        .expect_err("a partition clause on an existing table refuses");
    assert_analysis_refusal(sql, refused, PARTITION_MANAGEMENT_UNSUPPORTED);
}

#[tokio::test]
async fn show_table_extended_residue_r_u4_14_stray_key_token_keeps_the_bare_head_line() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    let sql = "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (a b)";
    let expected = "[PARSE_SYNTAX_ERROR] Syntax error at or near 'b'. SQLSTATE: 42601";
    assert_scanner_refusal(sql, expected);
    assert_end_to_end_refusal(&ctx, &catalogs, sql, expected).await;
}

const SHOW_TABLE_EXTENDED_PC: &str = "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc'";

#[tokio::test]
async fn show_table_extended_scanner_arms_refuse_with_spark_measured_text() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    let partition = |spec: &str| format!("{SHOW_TABLE_EXTENDED_PC} PARTITION{spec}");
    for (sql, near) in [
        (partition(""), "end of input"),
        (partition(";"), "';'"),
        (partition(" 1"), "'1'"),
        (partition(" 'x'"), "''x''"),
        (partition(" )"), "')'"),
        (partition(" ("), "end of input"),
        (partition(" ()"), "')'"),
        (partition(" (1=1)"), "'1'"),
        (partition(" (,)"), "','"),
        (partition(" (a, =1)"), "'='"),
        (partition(" (a="), "end of input"),
        (partition(" (a=,b=1)"), "','"),
        (partition(" (a=)"), "')'"),
        (partition(" (a=(1))"), "'('"),
        (partition(" (a=1("), "'('"),
        (partition(" (a=1 (2))"), "'('"),
        (partition(" (a=1"), "end of input"),
        (partition(" (a=1,"), "end of input"),
        (partition(" (a"), "end of input"),
        ("SHOW TABLE EXTENDED".to_string(), "end of input"),
        ("SHOW TABLE EXTENDED IN".to_string(), "end of input"),
        ("SHOW TABLE EXTENDED IN ice.sales pc".to_string(), "'pc'"),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE".to_string(),
            "end of input",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE pc".to_string(),
            "'pc'",
        ),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc".to_string(),
            "'''",
        ),
        (
            format!("{SHOW_TABLE_EXTENDED_PC} extra"),
            "'extra': extra input 'extra'",
        ),
        (partition(" (a=1) extra"), "'extra': extra input 'extra'"),
    ] {
        let expected =
            format!("[PARSE_SYNTAX_ERROR] Syntax error at or near {near}. SQLSTATE: 42601");
        assert_scanner_refusal(&sql, &expected);
        assert_end_to_end_refusal(&ctx, &catalogs, &sql, &expected).await;
    }
    for (spec, key) in [(" (b=1, a)", "a"), (" (a, b)", "a")] {
        let sql = partition(spec);
        let expected = format!(
            "[INVALID_SQL_SYNTAX.EMPTY_PARTITION_VALUE] Invalid SQL syntax: Partition key `{key}` \
             must set value. SQLSTATE: 42000"
        );
        assert_scanner_refusal(&sql, &expected);
        assert_end_to_end_refusal(&ctx, &catalogs, &sql, &expected).await;
    }
}

#[tokio::test]
async fn show_table_extended_scanner_arms_accept_what_spark_parses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let expected = seed_pc(&ctx, &catalogs).await;
    for (sql, scope) in [
        (format!("{SHOW_TABLE_EXTENDED_PC};"), vec!["ice", "sales"]),
        (
            "SHOW TABLE EXTENDED IN ice.sales LIKE \"pc\"".to_string(),
            vec!["ice", "sales"],
        ),
        (
            "SHOW TABLE EXTENDED FROM ice.sales LIKE 'pc'".to_string(),
            vec!["ice", "sales"],
        ),
    ] {
        assert_scanner_accepts(&sql, &scope, "pc", false);
        assert_eq!(
            outcome(&ctx, &catalogs, &sql).await.unwrap(),
            expected,
            "{sql}"
        );
    }
    let two_values = format!("{SHOW_TABLE_EXTENDED_PC} PARTITION (a=1, b=2)");
    assert_scanner_accepts(&two_values, &["ice", "sales"], "pc", true);
    let refused = outcome(&ctx, &catalogs, &two_values)
        .await
        .expect_err("a partition clause on an existing table refuses");
    assert_analysis_refusal(&two_values, refused, PARTITION_MANAGEMENT_UNSUPPORTED);
    let ambient = "SHOW TABLE EXTENDED LIKE 'pc'";
    let Some(Ok(parsed)) = crate::show_table_extended::try_parse_show_table_extended(ambient)
    else {
        panic!("{ambient}: expected a SHOW TABLE EXTENDED parse");
    };
    assert_eq!(parsed.scope, None);
    assert_eq!(
        outcome(&ctx, &catalogs, ambient).await.unwrap(),
        (extended_schema(), Vec::new())
    );
}

#[tokio::test]
async fn show_table_extended_residue_a15_in_string_accepts_a_string_namespace() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    let sql = "SHOW TABLE EXTENDED IN 'x' LIKE 'pc'";
    assert_scanner_accepts(sql, &["x"], "pc", false);
    let refused = outcome(&ctx, &catalogs, sql)
        .await
        .expect_err("the string namespace does not exist");
    assert_analysis_refusal(
        sql,
        refused,
        "[SCHEMA_NOT_FOUND] The schema `spark_catalog`.`x` cannot be found. Verify the spelling \
         and correctness of the schema and catalog. If you did not qualify the name with a \
         catalog, verify the current_schema() output, or qualify the name with the correct \
         catalog. To tolerate the error on drop use DROP SCHEMA IF EXISTS. SQLSTATE: 42704",
    );
}

#[tokio::test]
async fn show_table_extended_residue_a15_in_like_keeps_the_bare_head_line() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    let sql = "SHOW TABLE EXTENDED IN LIKE 'pc'";
    let expected = "[PARSE_SYNTAX_ERROR] Syntax error at or near ''pc''. SQLSTATE: 42601";
    assert_scanner_refusal(sql, expected);
    assert_end_to_end_refusal(&ctx, &catalogs, sql, expected).await;
}

#[tokio::test]
async fn show_table_extended_residue_a15_extendedx_falls_through_to_show_variable() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    let sql = "SHOW TABLE EXTENDEDX IN ice.sales LIKE 'pc'";
    assert!(crate::show_table_extended::try_parse_show_table_extended(sql).is_none());
    let refused = outcome(&ctx, &catalogs, sql)
        .await
        .expect_err("SHOW TABLE EXTENDEDX falls through");
    assert_analysis_refusal(
        sql,
        refused,
        "SHOW [VARIABLE] is not supported unless information_schema is enabled",
    );
}

#[tokio::test]
async fn show_table_extended_partition_word_without_parenthesis_reports_the_missing_parenthesis() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    seed_pc(&ctx, &catalogs).await;
    for (spec, word) in [(" a=1", "a"), (" select", "select"), (" a", "a")] {
        let sql = format!("{SHOW_TABLE_EXTENDED_PC} PARTITION{spec}");
        let expected = format!(
            "[PARSE_SYNTAX_ERROR] Syntax error at or near '{word}': missing '('. SQLSTATE: 42601"
        );
        assert_scanner_refusal(&sql, &expected);
        assert_end_to_end_refusal(&ctx, &catalogs, &sql, &expected).await;
    }
}
