use super::super::*;
use super::common::*;

use datafusion::sql::sqlparser::parser::ParserError;
use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Type};

fn parse_error(near: &str) -> String {
    format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601")
}

fn extra_input_parse_error(near: &str) -> String {
    format!(
        "[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}': extra input '{near}'. SQLSTATE: 42601"
    )
}

fn end_of_input_parse_error() -> String {
    "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601".to_string()
}

fn partition_parse_error() -> String {
    "[UNSUPPORTED_FEATURE.DESC_TABLE_COLUMN_PARTITION] The feature is not supported: DESC TABLE \
     COLUMN for a specific partition. SQLSTATE: 0A000"
        .to_string()
}

fn table_not_found_error(parts: &[&str]) -> String {
    let table = parts
        .iter()
        .map(|part| format!("`{}`", part.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".");
    format!(
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view {table} cannot be found. Verify the spelling \
         and correctness of the schema and catalog. If you did not qualify the name with a schema, \
         verify the current_schema() output, or qualify the name with the correct schema and \
         catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. \
         SQLSTATE: 42P01"
    )
}

fn unresolved_column_error() -> String {
    "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name \
     `nope` cannot be resolved. Did you mean one of the following? [`id`, `s`, `we ird`]. \
     SQLSTATE: 42703"
        .to_string()
}

fn unresolved_column_without_suggestion_error(column: &str) -> String {
    let column = column
        .split('.')
        .map(|part| format!("`{}`", part.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".");
    format!(
        "[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function parameter with name {column} cannot be resolved.  SQLSTATE: 42703"
    )
}

fn nested_column_error() -> String {
    "[_LEGACY_ERROR_TEMP_1060] DESC TABLE COLUMN does not support nested column: s.a.".to_string()
}

fn deep_nested_column_error() -> String {
    "[INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from \"s.a\". Need a complex type \
     [STRUCT, ARRAY, MAP] but got \"INT\". SQLSTATE: 42000"
        .to_string()
}

fn five_part_table_error() -> String {
    "Unsupported compound identifier 'a.b.c.d.e'. Expected 1, 2 or 3 parts, got 5".to_string()
}

fn assert_parser_sql_error(sql: &str, expected: &str) {
    let Some(result) = crate::describe_show::try_parse_describe_table(sql) else {
        panic!("{sql} must take the describe-table path");
    };
    match result {
        Err(DataFusionError::SQL(error, _)) => match *error {
            ParserError::ParserError(message) => assert_eq!(message.as_str(), expected, "{sql}"),
            error => panic!("{sql} must use ParserError, got {error:?}"),
        },
        Err(error) => panic!("{sql} must return SQL parser error, got {error:?}"),
        Ok(_) => panic!("{sql} must return an error"),
    }
}

fn assert_parser_plan_error(sql: &str, expected: &str) {
    let Some(result) = crate::describe_show::try_parse_describe_table(sql) else {
        panic!("{sql} must take the describe-table path");
    };
    match result {
        Err(DataFusionError::Plan(message)) => assert_eq!(message.as_str(), expected, "{sql}"),
        Err(error) => panic!("{sql} must return plan error, got {error:?}"),
        Ok(_) => panic!("{sql} must return an error"),
    }
}

fn assert_parser_describe(
    sql: &str,
    expected_table: &str,
    expected_extended: bool,
    expected_column: Option<&[&str]>,
) {
    let Some(result) = crate::describe_show::try_parse_describe_table(sql) else {
        panic!("{sql} must take the describe-table path");
    };
    let describe = match result {
        Ok(describe) => describe,
        Err(error) => panic!("{sql} must parse, got {error:?}"),
    };
    assert_eq!(describe.catalog, "ice", "{sql}");
    assert_eq!(describe.namespace, "sales", "{sql}");
    assert_eq!(describe.table, expected_table, "{sql}");
    assert_eq!(describe.extended, expected_extended, "{sql}");
    assert_eq!(
        describe.column,
        expected_column.map(|column| column.iter().map(ToString::to_string).collect()),
        "{sql}"
    );
}

async fn assert_session_sql_error(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    expected: &str,
) {
    match execute(ctx, catalogs, sql).await {
        Err(DataFusionError::SQL(error, _)) => match *error {
            ParserError::ParserError(message) => assert_eq!(message.as_str(), expected, "{sql}"),
            error => panic!("{sql} must use ParserError, got {error:?}"),
        },
        Err(error) => panic!("{sql} must return SQL parser error, got {error:?}"),
        Ok(_) => panic!("{sql} must return an error"),
    }
}

async fn assert_session_plan_error(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    expected: &str,
) {
    match execute(ctx, catalogs, sql).await {
        Err(DataFusionError::Plan(message)) => assert_eq!(message.as_str(), expected, "{sql}"),
        Err(error) => panic!("{sql} must return plan error, got {error:?}"),
        Ok(_) => panic!("{sql} must return an error"),
    }
}

async fn create_describe_error_table(catalogs: &CatalogRegistry, warehouse: &str) {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "s",
                Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                    3,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                ))])),
            )),
            Arc::new(NestedField::optional(
                4,
                "we ird",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .unwrap();
    let location = format!("{warehouse}/sales/dc");
    std::fs::create_dir_all(&location).unwrap();
    catalogs["ice"]
        .create_table(
            &NamespaceIdent::new("sales".to_string()),
            iceberg::TableCreation::builder()
                .name("dc".to_string())
                .location(location)
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();
}

async fn describe_column_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<(String, String)> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                names.value(index).to_string(),
                values.value(index).to_string(),
            ));
        }
    }
    rows
}

macro_rules! parser_error_case {
    ($name:ident, $sql:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_parser_sql_error($sql, &$expected);
        }
    };
}

macro_rules! parser_describe_case {
    ($name:ident, $sql:expr, $table:expr, $extended:expr, $column:expr) => {
        #[test]
        fn $name() {
            assert_parser_describe($sql, $table, $extended, $column);
        }
    };
}

macro_rules! session_error_case {
    ($name:ident, $sql:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            let warehouse = TempDir::new().unwrap();
            let (ctx, catalogs) = setup(&warehouse).await;
            assert_session_sql_error(&ctx, &catalogs, $sql, &$expected).await;
        }
    };
}

parser_error_case!(
    parser_col_number,
    "DESCRIBE ice.sales.dc 1",
    extra_input_parse_error("1")
);
parser_error_case!(
    parser_col_paren,
    "DESCRIBE ice.sales.dc (",
    extra_input_parse_error("(")
);
parser_error_case!(
    parser_col_string,
    "DESCRIBE ice.sales.dc 'id'",
    extra_input_parse_error("'id'")
);
parser_error_case!(
    parser_col_dquote,
    "DESCRIBE ice.sales.dc \"id\"",
    parse_error("\"id\"")
);
parser_error_case!(
    parser_col_star,
    "DESCRIBE ice.sales.dc *",
    extra_input_parse_error("*")
);
parser_error_case!(
    parser_col_minus,
    "DESCRIBE ice.sales.dc -",
    extra_input_parse_error("-")
);
parser_error_case!(
    parser_col_trailing,
    "DESCRIBE ice.sales.dc id extra",
    extra_input_parse_error("extra")
);
parser_error_case!(
    parser_col_comma,
    "DESCRIBE ice.sales.dc id, s",
    parse_error(",")
);
parser_error_case!(
    parser_col_dot_trailing,
    "DESCRIBE ice.sales.dc s.",
    end_of_input_parse_error()
);
parser_error_case!(
    d_blk_lead_unclosed_quote,
    "/* c */ DESCRIBE sc.sales.pc 'x",
    parse_error("'")
);
parser_error_case!(
    d_line_lead_unclosed_quote,
    "-- c\nDESCRIBE sc.sales.pc 'x",
    parse_error("'")
);
parser_error_case!(
    d_blk_mid_unclosed_quote,
    "DESCRIBE /* c */ sc.sales.pc 'x",
    parse_error("'")
);
parser_error_case!(
    d_plain_unclosed_quote,
    "DESCRIBE sc.sales.pc 'x",
    parse_error("'")
);
parser_error_case!(
    d_blk_apos_unclosed_quote,
    "DESCRIBE sc.sales.pc /* it's */ 'x",
    parse_error("'")
);
parser_error_case!(
    d_blk_apos_lead_unclosed_quote,
    "/* it's */ DESCRIBE sc.sales.pc 'x",
    parse_error("'")
);
parser_error_case!(
    d_line_apos_unclosed_bt,
    "DESCRIBE sc.sales.pc -- it's\n`x",
    parse_error("`")
);
parser_error_case!(
    d_blk_lead_unclosed_dquote,
    "/* c */ DESC sc.sales.pc \"x",
    parse_error("\"")
);
parser_error_case!(
    parser_col_unclosed_quote,
    "DESCRIBE ice.sales.dc 'id",
    parse_error("'")
);
parser_error_case!(
    parser_col_unclosed_backtick,
    "DESCRIBE ice.sales.dc `id",
    parse_error("`")
);
parser_error_case!(
    parser_col_partition,
    "DESCRIBE ice.sales.dc PARTITION (id=1) id",
    partition_parse_error()
);
parser_error_case!(parser_tbl_bare, "DESCRIBE", end_of_input_parse_error());
parser_error_case!(parser_tbl_paren, "DESCRIBE (", end_of_input_parse_error());
parser_error_case!(
    parser_tbl_unclosed_quote,
    "DESCRIBE 'ice.sales.dc",
    parse_error("'")
);
parser_error_case!(
    parser_tbl_unclosed_backtick,
    "DESCRIBE `ice.sales.dc",
    parse_error("`")
);
parser_error_case!(
    parser_tbl_unclosed_dquote,
    "DESCRIBE \"ice.sales.dc",
    parse_error("\"")
);
parser_error_case!(
    parser_col_unclosed_dquote,
    "DESCRIBE ice.sales.dc \"id",
    parse_error("\"")
);
parser_error_case!(
    parser_tbl_string,
    "DESCRIBE 'ice.sales.dc'",
    parse_error("'ice.sales.dc'")
);
parser_error_case!(
    parser_tbl_dquote_part,
    "DESCRIBE \"ice\".sales.dc",
    parse_error("\"ice\"")
);
parser_error_case!(
    parser_tbl_dot_trailing,
    "DESCRIBE ice.sales.",
    extra_input_parse_error(".")
);

#[test]
fn parser_tbl_four_part_returns_table_not_found() {
    assert_parser_plan_error(
        "DESCRIBE ice.sales.dc.id",
        &table_not_found_error(&["ice", "sales", "dc", "id"]),
    );
}

#[test]
fn parser_tbl_five_part_falls_through() {
    assert!(crate::describe_show::try_parse_describe_table("DESCRIBE a.b.c.d.e").is_none());
}

parser_describe_case!(
    parser_col_backtick,
    "DESCRIBE ice.sales.dc `we ird`",
    "dc",
    false,
    Some(&["we ird"])
);
parser_describe_case!(
    parser_col_extended,
    "DESCRIBE EXTENDED ice.sales.dc id",
    "dc",
    true,
    Some(&["id"])
);
parser_describe_case!(
    parser_col_formatted,
    "DESCRIBE FORMATTED ice.sales.dc id",
    "dc",
    true,
    Some(&["id"])
);
parser_describe_case!(
    parser_col_case,
    "DESC ice.sales.dc ID",
    "dc",
    false,
    Some(&["ID"])
);
parser_describe_case!(
    parser_col_table_keyword,
    "DESCRIBE TABLE ice.sales.dc id",
    "dc",
    false,
    Some(&["id"])
);
parser_describe_case!(
    parser_col_semicolon,
    "DESCRIBE ice.sales.dc id;",
    "dc",
    false,
    Some(&["id"])
);
parser_describe_case!(
    parser_col_struct,
    "DESCRIBE ice.sales.dc s",
    "dc",
    false,
    Some(&["s"])
);
parser_describe_case!(
    parser_tbl_missing,
    "DESCRIBE ice.sales.nope",
    "nope",
    false,
    None
);
parser_describe_case!(
    parser_tbl_missing_column,
    "DESCRIBE ice.sales.nope id",
    "nope",
    false,
    Some(&["id"])
);
parser_describe_case!(
    parser_col_missing,
    "DESCRIBE ice.sales.dc nope",
    "dc",
    false,
    Some(&["nope"])
);
parser_describe_case!(
    parser_col_nested,
    "DESCRIBE ice.sales.dc s.a",
    "dc",
    false,
    Some(&["s", "a"])
);
parser_describe_case!(
    parser_col_nested_deep,
    "DESCRIBE ice.sales.dc s.a.b",
    "dc",
    false,
    Some(&["s", "a", "b"])
);
parser_describe_case!(
    parser_col_view,
    "DESCRIBE ice.sales.v id",
    "v",
    false,
    Some(&["id"])
);

#[test]
fn d_blk_ns_head_unclosed_falls_through() {
    assert!(
        crate::describe_show::try_parse_describe_table("/* c */ DESCRIBE NAMESPACE sc.sales 'x")
            .is_none()
    );
}

#[test]
fn d_unclosed_comment_falls_through() {
    assert!(
        crate::describe_show::try_parse_describe_table("/* c DESCRIBE sc.sales.pc 'x").is_none()
    );
}

#[test]
fn describe_keyword_and_quote_inside_comments_are_ignored() {
    for sql in [
        "/* ' */ DESCRIBE sc.sales.pc",
        "-- ' DESC sc.sales.pc\nDESCRIBE sc.sales.pc",
    ] {
        assert!(
            crate::describe_show::try_parse_describe_table(sql).is_some(),
            "{sql}"
        );
    }
}

#[test]
fn parser_near_misses_fall_through() {
    for sql in [
        "DESCRIBE NAMESPACE `x",
        "DESCRIBE FUNCTION 'x",
        "DESCRIBE QUERY SELECT 'x",
        "SELECT 'DESCRIBE t `x'",
        "DESCRIBED t",
        "DESCRIBE DATABASE ice.sales",
    ] {
        assert!(
            crate::describe_show::try_parse_describe_table(sql).is_none(),
            "{sql} must fall through"
        );
    }
}

session_error_case!(
    session_col_number,
    "DESCRIBE ice.sales.dc 1",
    extra_input_parse_error("1")
);
session_error_case!(
    session_col_paren,
    "DESCRIBE ice.sales.dc (",
    extra_input_parse_error("(")
);
session_error_case!(
    session_col_string,
    "DESCRIBE ice.sales.dc 'id'",
    extra_input_parse_error("'id'")
);
session_error_case!(
    session_col_dquote,
    "DESCRIBE ice.sales.dc \"id\"",
    parse_error("\"id\"")
);
session_error_case!(
    session_col_star,
    "DESCRIBE ice.sales.dc *",
    extra_input_parse_error("*")
);
session_error_case!(
    session_col_minus,
    "DESCRIBE ice.sales.dc -",
    extra_input_parse_error("-")
);
session_error_case!(
    session_col_trailing,
    "DESCRIBE ice.sales.dc id extra",
    extra_input_parse_error("extra")
);
session_error_case!(
    session_col_comma,
    "DESCRIBE ice.sales.dc id, s",
    parse_error(",")
);
session_error_case!(
    session_col_dot_trailing,
    "DESCRIBE ice.sales.dc s.",
    end_of_input_parse_error()
);
session_error_case!(
    session_col_unclosed_quote,
    "DESCRIBE ice.sales.dc 'id",
    parse_error("'")
);
session_error_case!(
    session_col_unclosed_backtick,
    "DESCRIBE ice.sales.dc `id",
    parse_error("`")
);
session_error_case!(
    session_col_partition,
    "DESCRIBE ice.sales.dc PARTITION (id=1) id",
    partition_parse_error()
);
session_error_case!(session_tbl_bare, "DESCRIBE", end_of_input_parse_error());
session_error_case!(session_tbl_paren, "DESCRIBE (", end_of_input_parse_error());
session_error_case!(
    session_tbl_unclosed_quote,
    "DESCRIBE 'ice.sales.dc",
    parse_error("'")
);
session_error_case!(
    session_tbl_unclosed_backtick,
    "DESCRIBE `ice.sales.dc",
    parse_error("`")
);
session_error_case!(
    session_tbl_unclosed_dquote,
    "DESCRIBE \"ice.sales.dc",
    parse_error("\"")
);
session_error_case!(
    session_col_unclosed_dquote,
    "DESCRIBE ice.sales.dc \"id",
    parse_error("\"")
);
session_error_case!(
    session_tbl_string,
    "DESCRIBE 'ice.sales.dc'",
    parse_error("'ice.sales.dc'")
);
session_error_case!(
    session_tbl_dquote_part,
    "DESCRIBE \"ice\".sales.dc",
    parse_error("\"ice\"")
);
session_error_case!(
    session_tbl_dot_trailing,
    "DESCRIBE ice.sales.",
    extra_input_parse_error(".")
);

#[tokio::test]
async fn session_tbl_four_part_returns_table_not_found() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    assert_session_plan_error(
        &ctx,
        &catalogs,
        "DESCRIBE ice.sales.dc.id",
        &table_not_found_error(&["ice", "sales", "dc", "id"]),
    )
    .await;
}

#[tokio::test]
async fn session_tbl_missing_returns_table_not_found() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    assert_session_plan_error(
        &ctx,
        &catalogs,
        "DESCRIBE ice.sales.nope",
        &table_not_found_error(&["ice", "sales", "nope"]),
    )
    .await;
}

#[tokio::test]
async fn session_tbl_missing_column_returns_table_not_found() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    assert_session_plan_error(
        &ctx,
        &catalogs,
        "DESCRIBE ice.sales.nope id",
        &table_not_found_error(&["ice", "sales", "nope"]),
    )
    .await;
}

#[tokio::test]
async fn session_col_missing_returns_spark_column_error() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    create_describe_error_table(&catalogs, warehouse.path().to_str().unwrap()).await;
    assert_session_plan_error(
        &ctx,
        &catalogs,
        "DESCRIBE ice.sales.dc nope",
        &unresolved_column_error(),
    )
    .await;
}

#[tokio::test]
async fn session_col_nested_returns_spark_legacy_error() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    create_describe_error_table(&catalogs, warehouse.path().to_str().unwrap()).await;
    assert_session_plan_error(
        &ctx,
        &catalogs,
        "DESCRIBE ice.sales.dc s.a",
        &nested_column_error(),
    )
    .await;
}

#[tokio::test]
async fn session_col_nested_deep_returns_spark_type_error() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    create_describe_error_table(&catalogs, warehouse.path().to_str().unwrap()).await;
    assert_session_plan_error(
        &ctx,
        &catalogs,
        "DESCRIBE ice.sales.dc s.a.b",
        &deep_nested_column_error(),
    )
    .await;
}

#[tokio::test]
async fn session_col_backtick_returns_exact_rows() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    create_describe_error_table(&catalogs, warehouse.path().to_str().unwrap()).await;
    assert_eq!(
        describe_column_rows(&ctx, &catalogs, "DESCRIBE ice.sales.dc `we ird`").await,
        vec![
            ("col_name".to_string(), "we ird".to_string()),
            ("data_type".to_string(), "string".to_string()),
            ("comment".to_string(), "NULL".to_string()),
        ]
    );
}

#[tokio::test]
async fn session_col_extended_returns_exact_rows() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    create_describe_error_table(&catalogs, warehouse.path().to_str().unwrap()).await;
    let expected = vec![
        ("col_name".to_string(), "id".to_string()),
        ("data_type".to_string(), "bigint".to_string()),
        ("comment".to_string(), "NULL".to_string()),
    ];
    assert_eq!(
        describe_column_rows(&ctx, &catalogs, "DESCRIBE EXTENDED ice.sales.dc id").await,
        expected
    );
}

#[tokio::test]
async fn session_col_formatted_returns_exact_rows() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    create_describe_error_table(&catalogs, warehouse.path().to_str().unwrap()).await;
    assert_eq!(
        describe_column_rows(&ctx, &catalogs, "DESCRIBE FORMATTED ice.sales.dc id").await,
        vec![
            ("col_name".to_string(), "id".to_string()),
            ("data_type".to_string(), "bigint".to_string()),
            ("comment".to_string(), "NULL".to_string()),
        ]
    );
}

#[tokio::test]
async fn session_col_case_returns_typed_name() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    create_describe_error_table(&catalogs, warehouse.path().to_str().unwrap()).await;
    assert_eq!(
        describe_column_rows(&ctx, &catalogs, "DESC ice.sales.dc ID").await,
        vec![
            ("col_name".to_string(), "ID".to_string()),
            ("data_type".to_string(), "bigint".to_string()),
            ("comment".to_string(), "NULL".to_string()),
        ]
    );
}

#[tokio::test]
async fn session_col_table_keyword_returns_exact_rows() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    create_describe_error_table(&catalogs, warehouse.path().to_str().unwrap()).await;
    assert_eq!(
        describe_column_rows(&ctx, &catalogs, "DESCRIBE TABLE ice.sales.dc id").await,
        vec![
            ("col_name".to_string(), "id".to_string()),
            ("data_type".to_string(), "bigint".to_string()),
            ("comment".to_string(), "NULL".to_string()),
        ]
    );
}

#[tokio::test]
async fn session_col_semicolon_returns_exact_rows() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    create_describe_error_table(&catalogs, warehouse.path().to_str().unwrap()).await;
    assert_eq!(
        describe_column_rows(&ctx, &catalogs, "DESCRIBE ice.sales.dc id;").await,
        vec![
            ("col_name".to_string(), "id".to_string()),
            ("data_type".to_string(), "bigint".to_string()),
            ("comment".to_string(), "NULL".to_string()),
        ]
    );
}

#[tokio::test]
async fn session_col_struct_returns_exact_rows() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    create_describe_error_table(&catalogs, warehouse.path().to_str().unwrap()).await;
    assert_eq!(
        describe_column_rows(&ctx, &catalogs, "DESCRIBE ice.sales.dc s").await,
        vec![
            ("col_name".to_string(), "s".to_string()),
            ("data_type".to_string(), "struct<a:int>".to_string()),
            ("comment".to_string(), "NULL".to_string()),
        ]
    );
}

#[tokio::test]
async fn session_tbl_five_part_pins_current_divergence() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    assert_session_plan_error(
        &ctx,
        &catalogs,
        "DESCRIBE a.b.c.d.e",
        &five_part_table_error(),
    )
    .await;
}

#[tokio::test]
async fn session_col_view_returns_spark_unresolved_column_error() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    execute(&ctx, &catalogs, "CREATE VIEW ice.sales.v AS SELECT 1 AS id")
        .await
        .unwrap();
    assert_session_plan_error(
        &ctx,
        &catalogs,
        "DESCRIBE ice.sales.v id",
        &unresolved_column_without_suggestion_error("id"),
    )
    .await;
}

#[tokio::test]
async fn session_col_view_missing_column_returns_spark_unresolved_column_error() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    execute(&ctx, &catalogs, "CREATE VIEW ice.sales.v AS SELECT 1 AS id")
        .await
        .unwrap();
    assert_session_plan_error(
        &ctx,
        &catalogs,
        "DESCRIBE ice.sales.v nope",
        &unresolved_column_without_suggestion_error("nope"),
    )
    .await;
}

#[tokio::test]
async fn session_describe_view_without_column_returns_rows() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    execute(&ctx, &catalogs, "CREATE VIEW ice.sales.v AS SELECT 1 AS id")
        .await
        .unwrap();
    assert_eq!(
        describe_column_rows(&ctx, &catalogs, "DESCRIBE ice.sales.v").await,
        vec![("id".to_string(), "int".to_string())]
    );
}
