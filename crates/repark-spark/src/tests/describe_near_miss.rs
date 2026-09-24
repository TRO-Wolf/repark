use super::super::*;
use super::common::*;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::sql::sqlparser::parser::ParserError;

type TableRow = (String, String, Option<String>);

const DC_TABLE: &str =
    "CREATE TABLE ice.sales.dc (id BIGINT, s STRUCT<a: INT>, `we ird` STRING) USING iceberg";

fn table_schema() -> Schema {
    Schema::new(vec![
        Field::new("col_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("comment", DataType::Utf8, true),
    ])
}

fn column_schema() -> Schema {
    Schema::new(vec![
        Field::new("info_name", DataType::Utf8, false),
        Field::new("info_value", DataType::Utf8, false),
    ])
}

fn strip_field_metadata(schema: &Schema) -> Schema {
    Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_metadata(HashMap::new()))
            .collect::<Vec<_>>(),
    )
}

async fn answer(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> (Schema, Vec<Vec<Option<String>>>) {
    let frame = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} must plan: {error}"));
    let schema = strip_field_metadata(frame.schema().as_arrow());
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql} must execute: {error}"));
    let mut rows = Vec::new();
    for batch in batches {
        let columns: Vec<&StringArray> = batch
            .columns()
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("DESCRIBE answers are strings")
            })
            .collect();
        for index in 0..batch.num_rows() {
            rows.push(
                columns
                    .iter()
                    .map(|column| (!column.is_null(index)).then(|| column.value(index).to_string()))
                    .collect(),
            );
        }
    }
    (schema, rows)
}

fn owned(rows: &[&[Option<&str>]]) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|cell| cell.map(ToString::to_string))
                .collect()
        })
        .collect()
}

fn table_rows(rows: &[TableRow]) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|(name, data_type, comment)| {
            vec![Some(name.clone()), Some(data_type.clone()), comment.clone()]
        })
        .collect()
}

fn dc_rows() -> Vec<Vec<Option<String>>> {
    table_rows(&[
        ("id".to_string(), "bigint".to_string(), None),
        ("s".to_string(), "struct<a:int>".to_string(), None),
        ("we ird".to_string(), "string".to_string(), None),
    ])
}

fn snapshots_rows() -> Vec<Vec<Option<String>>> {
    table_rows(&[
        ("committed_at".to_string(), "timestamp".to_string(), None),
        ("snapshot_id".to_string(), "bigint".to_string(), None),
        ("parent_id".to_string(), "bigint".to_string(), None),
        ("operation".to_string(), "string".to_string(), None),
        ("manifest_list".to_string(), "string".to_string(), None),
        (
            "summary".to_string(),
            "map<string,string>".to_string(),
            None,
        ),
    ])
}

fn column_rows(name: &str, data_type: &str) -> Vec<Vec<Option<String>>> {
    owned(&[
        &[Some("col_name"), Some(name)],
        &[Some("data_type"), Some(data_type)],
        &[Some("comment"), Some("NULL")],
    ])
}

fn table_not_found(parts: &[&str]) -> String {
    let table = parts
        .iter()
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".");
    format!(
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view {table} cannot be \
         found. Verify the spelling and correctness of the schema and catalog. If you did not \
         qualify the name with a schema, verify the current_schema() output, or qualify the name \
         with the correct schema and catalog. To tolerate the error on drop use DROP VIEW IF \
         EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01"
    )
}

fn unresolved_column(name: &str, suggestions: &str) -> String {
    format!(
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
         function parameter with name {name} cannot be resolved. Did you mean one of the \
         following? [{suggestions}]. SQLSTATE: 42703"
    )
}

fn parse_syntax(near: &str, extra_input: bool) -> String {
    if extra_input {
        format!(
            "[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}': extra input '{near}'. \
             SQLSTATE: 42601"
        )
    } else {
        format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601")
    }
}

async fn assert_parser_error(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    expected: &str,
) {
    let error = execute(ctx, catalogs, sql)
        .await
        .expect_err("the near miss must refuse");
    let DataFusionError::SQL(parser_error, None) = &error else {
        panic!("{sql} must answer a parser error, got {error:?}");
    };
    let ParserError::ParserError(message) = parser_error.as_ref() else {
        panic!("{sql} must answer ParserError, got {parser_error:?}");
    };
    assert_eq!(message, expected, "{sql}");
}

async fn dc_session(warehouse: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(warehouse).await;
    execute(&ctx, &catalogs, DC_TABLE)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    (ctx, catalogs)
}

async fn assert_error(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str, expected: &str) {
    let error = execute(ctx, catalogs, sql)
        .await
        .expect_err("the near miss must refuse");
    assert_eq!(error.to_string(), expected, "{sql}");
}

#[tokio::test]
async fn four_part_metadata_suffix_answers_metadata_rows() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = dc_session(&warehouse).await;
    for sql in [
        "DESCRIBE ice.sales.dc.snapshots",
        "DESCRIBE ice.sales.dc.SNAPSHOTS",
        "DESC ice.sales.dc.snapshots",
    ] {
        assert!(
            crate::describe_show::try_parse_describe_table(sql).is_none(),
            "{sql} must leave the metadata-table path to answer"
        );
    }
    for sql in [
        "DESCRIBE `ice`.`sales`.`dc`.`snapshots`",
        "DESCRIBE `ice`.`sales`.`dc`.`SNAPSHOTS`",
        "DESC `ice`.`sales`.`dc`.`snapshots`",
    ] {
        let (schema, rows) = answer(&ctx, &catalogs, sql).await;
        assert_eq!(schema, table_schema(), "{sql}");
        assert_eq!(rows, snapshots_rows(), "{sql}");
    }
}

#[tokio::test]
async fn four_part_missing_base_and_unknown_suffix_name_the_written_parts() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = dc_session(&warehouse).await;
    for (sql, parts) in [
        (
            "DESCRIBE ice.sales.missing.snapshots",
            ["ice", "sales", "missing", "snapshots"],
        ),
        ("DESCRIBE ice.sales.dc.nope", ["ice", "sales", "dc", "nope"]),
        (
            "DESCRIBE EXTENDED ice.sales.dc.nope",
            ["ice", "sales", "dc", "nope"],
        ),
        (
            "DESCRIBE ice.sales.dc.nope id",
            ["ice", "sales", "dc", "nope"],
        ),
    ] {
        assert_error(&ctx, &catalogs, sql, &table_not_found(&parts)).await;
    }
}

#[tokio::test]
async fn column_tail_comments_whitespace_and_case_keep_column_rows() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = dc_session(&warehouse).await;
    for sql in [
        "DESCRIBE ice.sales.dc /* c */ id",
        "DESCRIBE ice.sales.dc -- c\n id",
        "DESCRIBE ice.sales.dc id /* c */",
        "DESCRIBE ice.sales.dc id -- c",
        "DESCRIBE ice.sales.dc id ;",
        "DESCRIBE ice.sales.dc id\n",
        "DESCRIBE ice.sales.dc id;;",
        "describe ice.sales.dc id",
    ] {
        let (schema, rows) = answer(&ctx, &catalogs, sql).await;
        assert_eq!(schema, column_schema(), "{sql}");
        assert_eq!(rows, column_rows("id", "bigint"), "{sql}");
    }
    let sql = "DESC TABLE EXTENDED ice.sales.dc `we ird`";
    let (schema, rows) = answer(&ctx, &catalogs, sql).await;
    assert_eq!(schema, column_schema(), "{sql}");
    assert_eq!(rows, column_rows("we ird", "string"), "{sql}");
}

#[tokio::test]
async fn table_rows_survive_trailing_whitespace_and_quote_only_comments() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = dc_session(&warehouse).await;
    for sql in [
        "DESCRIBE ice.sales.dc\n",
        "DESCRIBE ice.sales.dc\t;",
        "DESCRIBE ice.sales.dc -- 'x",
    ] {
        let (schema, rows) = answer(&ctx, &catalogs, sql).await;
        assert_eq!(schema, table_schema(), "{sql}");
        assert_eq!(rows, dc_rows(), "{sql}");
    }
}

#[tokio::test]
async fn keyword_and_quoted_column_names_resolve_as_columns() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = dc_session(&warehouse).await;
    for (sql, name, suggestions) in [
        (
            "DESCRIBE ice.sales.dc select",
            "`select`",
            "`id`, `s`, `we ird`",
        ),
        (
            "DESCRIBE ice.sales.dc table",
            "`table`",
            "`id`, `s`, `we ird`",
        ),
        (
            "DESCRIBE ice.sales.dc version",
            "`version`",
            "`id`, `s`, `we ird`",
        ),
        (
            "DESCRIBE ice.sales.dc partition",
            "`partition`",
            "`id`, `we ird`, `s`",
        ),
        ("DESCRIBE ice.sales.dc for", "`for`", "`s`, `id`, `we ird`"),
        (
            "DESCRIBE ice.sales.dc `s.a`",
            "`s.a`",
            "`s`, `id`, `we ird`",
        ),
        ("DESCRIBE ice.sales.dc ``", "``", "`s`, `id`, `we ird`"),
    ] {
        assert_error(&ctx, &catalogs, sql, &unresolved_column(name, suggestions)).await;
    }
}

#[tokio::test]
async fn column_named_like_a_time_travel_keyword_is_not_time_travel() {
    for sql in [
        "DESCRIBE ice.sales.dc version",
        "DESC ice.sales.dc version",
        "/* c */ DESCRIBE ice.sales.dc version",
        "-- c\nDESC ice.sales.dc timestamp",
    ] {
        assert!(!crate::time_travel::sql_has_time_travel(sql), "{sql}");
    }
    assert!(crate::time_travel::sql_has_time_travel(
        "SELECT * FROM ice.sales.dc VERSION AS OF 1"
    ));
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = dc_session(&warehouse).await;
    assert_error(
        &ctx,
        &catalogs,
        "DESC ice.sales.dc version",
        &unresolved_column("`version`", "`id`, `s`, `we ird`"),
    )
    .await;
}

#[tokio::test]
async fn tokenizer_failures_keep_measured_table_path_messages() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = dc_session(&warehouse).await;
    for (sql, near, extra_input) in [
        ("DESCRIBE ice.sales.dc \\", "\\", true),
        ("DESCRIBE ice.sales.dc X'1", "'", false),
        ("DESCRIBE ice.sales.dc \"a'", "\"", false),
        ("DESCRIBE /* a /* b */ ' */ ice.sales.dc 'x", "'", false),
        ("DESCRIBE namespaces 'x", "'", false),
        ("DESCRIBE TABLE 'x", "'", false),
        ("DESCRIBE EXTENDED 'x", "'", false),
        ("DESCRIBE 'x", "'", false),
    ] {
        assert_parser_error(&ctx, &catalogs, sql, &parse_syntax(near, extra_input)).await;
    }
}

#[test]
fn tokenizer_failures_outside_the_table_form_fall_through() {
    for sql in [
        "/* c */ DESCRIBE NAMESPACE ice.sales 'x",
        "DESC/* c */NAMESPACE ice.sales 'x",
        "DESCRIBE DATABASE ice.sales 'x",
        "DESC SCHEMA ice.sales 'x",
        "DESCRIBE FUNCTION 'x",
        "DESCRIBE QUERY SELECT 'x",
        "DESCRIBEX ice.sales.dc 'x",
        "SELECT 'x",
        "/* c DESCRIBE ice.sales.dc 'x",
        "DESCRIBE /* c",
    ] {
        assert!(
            crate::describe_show::try_parse_describe_table(sql).is_none(),
            "{sql} must fall through"
        );
    }
}

#[tokio::test]
async fn unclosed_bracketed_comment_answers_the_front_door_refusal() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = dc_session(&warehouse).await;
    for sql in [
        "/* c DESCRIBE ice.sales.dc 'x",
        "DESCRIBE ice.sales.dc /* c",
    ] {
        assert_parser_error(
            &ctx,
            &catalogs,
            sql,
            "[UNCLOSED_BRACKETED_COMMENT] Found an unclosed bracketed comment. Please, append */ \
             at the end of the comment. SQLSTATE: 42601",
        )
        .await;
    }
}

#[tokio::test]
async fn describe_view_without_column_answers_full_table_schema() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    execute(&ctx, &catalogs, "CREATE VIEW ice.sales.v AS SELECT 1 AS id")
        .await
        .unwrap();
    let (schema, rows) = answer(&ctx, &catalogs, "DESCRIBE ice.sales.v").await;
    assert_eq!(schema, table_schema());
    assert_eq!(rows, owned(&[&[Some("id"), Some("int"), Some("")]]));
}

#[test]
fn unclosed_quote_scan_covers_each_branch() {
    for (sql, expected) in [
        ("DESCRIBE t 'x", Some('\'')),
        ("DESCRIBE t `x", Some('`')),
        ("DESCRIBE t \"x", Some('"')),
        ("DESCRIBE t 'a'", None),
        ("DESCRIBE t 'a' `x", Some('`')),
        ("DESCRIBE t \"a'", Some('"')),
        ("DESCRIBE t 'a''b", Some('\'')),
        ("DESCRIBE t /* ' */ `x", Some('`')),
        ("DESCRIBE t /* a /* ' */ \" */ `x", Some('`')),
        ("DESCRIBE t -- '\n`x", Some('`')),
        ("DESCRIBE t 'a/*'*/ `x", Some('`')),
        ("DESCRIBE t 'a--'\n`x", Some('`')),
        ("DESCRIBE t /* x", None),
    ] {
        assert_eq!(
            crate::describe_column::unclosed_describe_quote(sql),
            expected,
            "{sql:?}"
        );
    }
}

#[test]
fn table_describe_text_covers_each_branch() {
    for (sql, expected) in [
        ("", false),
        ("/* c", false),
        ("SELECT 'x", false),
        ("DESCRIBEX t 'x", false),
        ("DESCRIBE /* c", false),
        ("DESCRIBE NAMESPACE n 'x", false),
        ("DESCRIBE database n 'x", false),
        ("DESCRIBE Schema n 'x", false),
        ("DESCRIBE FUNCTION 'x", false),
        ("DESCRIBE QUERY SELECT 'x", false),
        ("DESC/* c */NAMESPACE n 'x", false),
        ("DESCRIBE 'x", true),
        ("DESCRIBE", true),
        ("DESC t 'x", true),
        ("describe t 'x", true),
        ("-- c\nDESCRIBE /* c */ t 'x", true),
        ("DESCRIBE namespaces 'x", true),
        ("DESCRIBE TABLE 'x", true),
    ] {
        assert_eq!(
            crate::describe_column::is_table_describe_text(sql),
            expected,
            "{sql:?}"
        );
    }
}
