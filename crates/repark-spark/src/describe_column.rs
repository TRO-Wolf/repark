use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use iceberg::table::Table;
use repark_common::spark_error;

use crate::catalog_ops::{iceberg_err, name_parts};
use crate::describe_show::DescribeTable;
use crate::namespace_ddl::consume_word;
use crate::spark_type_names::spark_ddl_type_name;

pub(crate) fn parse_describe_column_tail(
    parser: &mut Parser,
) -> Option<Result<Option<Vec<String>>>> {
    match parser.peek_token().token {
        Token::EOF | Token::SemiColon => Some(Ok(None)),
        _ => {
            let column_name = parser.parse_object_name(false).ok()?;
            if !column_name.0.iter().all(|part| {
                part.as_ident()
                    .is_some_and(|ident| ident.quote_style.is_none_or(|quote| quote == '`'))
            }) {
                return None;
            }
            let column = name_parts(&column_name);
            match column.as_slice() {
                [word]
                    if (word.eq_ignore_ascii_case("VERSION")
                        || word.eq_ignore_ascii_case("TIMESTAMP"))
                        && parser.parse_keyword(Keyword::AS)
                        && consume_word(parser, "OF") =>
                {
                    Some(Err(describe_column_parse_error("OF")))
                }
                [word] if word.eq_ignore_ascii_case("FOR") && consume_word(parser, "VERSION") => {
                    Some(Err(describe_column_parse_error("VERSION")))
                }
                _ if matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) => {
                    Some(Ok(Some(column)))
                }
                _ => None,
            }
        }
    }
}

fn describe_column_parse_error(near: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601"
    ))
}

pub(crate) fn execute_describe_column(
    ctx: &SessionContext,
    describe: &DescribeTable,
    table: &Table,
) -> Result<DataFrame> {
    ctx.read_batch(describe_column_batch(describe, table)?)
}

fn describe_column_batch(describe: &DescribeTable, table: &Table) -> Result<RecordBatch> {
    let column = describe.column.as_deref().ok_or_else(|| {
        DataFusionError::Internal("describe column execution needs a column path".to_string())
    })?;
    let [column_name] = column else {
        return Err(DataFusionError::Plan(format!(
            "[_LEGACY_ERROR_TEMP_1060] DESC TABLE COLUMN does not support nested column: {}.",
            column.join(".")
        )));
    };
    let iceberg_schema = table.metadata().current_schema();
    let fields = iceberg_schema.as_struct().fields();
    let Some((index, field)) = fields
        .iter()
        .enumerate()
        .find(|(_, field)| field.name.eq_ignore_ascii_case(column_name))
    else {
        return Err(unresolved_describe_column_error(
            column_name,
            fields.iter().map(|field| field.name.clone()),
        ));
    };
    let arrow_schema =
        iceberg::arrow::schema_to_arrow_schema(iceberg_schema).map_err(iceberg_err)?;
    let arrow_field = arrow_schema.fields().get(index).ok_or_else(|| {
        DataFusionError::Internal(
            "Iceberg and Arrow schemas have different field counts".to_string(),
        )
    })?;
    let comment = field.doc.clone().unwrap_or_else(|| "NULL".to_string());
    let schema = Arc::new(Schema::new(vec![
        Field::new("info_name", DataType::Utf8, false),
        Field::new("info_value", DataType::Utf8, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["col_name", "data_type", "comment"])),
            Arc::new(StringArray::from(vec![
                column_name.clone(),
                spark_ddl_type_name(arrow_field.data_type()),
                comment,
            ])),
        ],
    )?)
}

fn unresolved_describe_column_error(
    column_name: &str,
    field_names: impl Iterator<Item = String>,
) -> DataFusionError {
    let mut suggestions = field_names
        .map(|name| format!("`{}`", name.replace('`', "``")))
        .collect::<Vec<_>>();
    suggestions.sort_by_key(|candidate| {
        datafusion::common::utils::datafusion_strsim::levenshtein(candidate, column_name)
    });
    let column_name = format!("`{column_name}`");
    let suggestions = suggestions.join(", ");
    DataFusionError::Plan(spark_error::message(
        spark_error::UNRESOLVED_COLUMN_WITH_SUGGESTION,
        &[
            ("columnName", column_name.as_str()),
            ("suggestions", suggestions.as_str()),
        ],
    ))
}
