use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Token;
use iceberg::spec::{PartitionField, Schema as IcebergSchema, Transform};
use iceberg::table::Table;
use repark_common::spark_error;

use crate::catalog_ops::{iceberg_err, name_parts};
use crate::describe_show::DescribeTable;
use crate::namespace_ddl::consume_word;
use crate::show_create::{skip_sql_whitespace_and_comments, starts_with_sql_keywords};
use crate::spark_type_names::spark_ddl_type_name;

pub(crate) fn parse_describe_column_tail(
    parser: &mut Parser,
    sql: &str,
) -> Result<Option<Vec<String>>> {
    match parser.peek_token().token {
        Token::EOF | Token::SemiColon => Ok(None),
        _ => {
            let token = parser.peek_token().token.clone();
            let Ok(column_name) = parser.parse_object_name(false) else {
                return Err(describe_column_token_parse_error(&token, sql));
            };
            if !column_name.0.iter().all(|part| {
                part.as_ident()
                    .is_some_and(|ident| ident.quote_style.is_none_or(|quote| quote == '`'))
            }) {
                return Err(describe_token_parse_error(&token));
            }
            let column = name_parts(&column_name);
            match column.as_slice() {
                [word]
                    if word.eq_ignore_ascii_case("PARTITION")
                        && matches!(parser.peek_token().token, Token::LParen) =>
                {
                    Err(describe_partition_parse_error())
                }
                [word]
                    if (word.eq_ignore_ascii_case("VERSION")
                        || word.eq_ignore_ascii_case("TIMESTAMP"))
                        && parser.parse_keyword(Keyword::AS)
                        && consume_word(parser, "OF") =>
                {
                    Err(describe_parse_error("OF"))
                }
                [word] if word.eq_ignore_ascii_case("FOR") && consume_word(parser, "VERSION") => {
                    Err(describe_parse_error("VERSION"))
                }
                _ if matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) => {
                    Ok(Some(column))
                }
                _ => Err(describe_token_parse_error(&parser.peek_token().token)),
            }
        }
    }
}

pub(crate) fn describe_tokenizer_error(sql: &str) -> Option<Result<DescribeTable>> {
    if !is_table_describe_text(sql) {
        return None;
    }
    let error = match unclosed_describe_quote(sql) {
        Some(quote) => describe_parse_error(&quote.to_string()),
        None => describe_end_of_input_parse_error(),
    };
    Some(Err(error))
}

pub(crate) fn is_table_describe_text(sql: &str) -> bool {
    let Some(position) = skip_sql_whitespace_and_comments(sql, 0) else {
        return false;
    };
    let keyword = if starts_with_sql_keywords(sql, &["DESCRIBE"]) {
        "DESCRIBE"
    } else if starts_with_sql_keywords(sql, &["DESC"]) {
        "DESC"
    } else {
        return false;
    };
    let position = position + keyword.len();
    let Some(word_start) = skip_sql_whitespace_and_comments(sql, position) else {
        return false;
    };
    let word_end = sql[word_start..]
        .bytes()
        .position(|byte| !byte.is_ascii_alphanumeric() && byte != b'_')
        .map_or(sql.len(), |offset| word_start + offset);
    word_start == word_end || !is_non_table_describe_text_head(&sql[word_start..word_end])
}

fn is_non_table_describe_text_head(word: &str) -> bool {
    word.eq_ignore_ascii_case("namespace")
        || word.eq_ignore_ascii_case("database")
        || word.eq_ignore_ascii_case("schema")
        || word.eq_ignore_ascii_case("function")
        || word.eq_ignore_ascii_case("query")
}

pub(crate) fn unclosed_describe_quote(sql: &str) -> Option<char> {
    let mut quote: Option<char> = None;
    let mut position = 0;
    while position < sql.len() {
        let tail = sql.get(position..)?;
        if quote.is_none() && (tail.starts_with("/*") || tail.starts_with("--")) {
            position = skip_sql_whitespace_and_comments(sql, position)?;
            continue;
        }
        let character = tail.chars().next()?;
        let next_position = position + character.len_utf8();
        if let Some(open_quote) = quote {
            if character == open_quote {
                quote = None;
            }
        } else if matches!(character, '\'' | '`' | '"') {
            quote = Some(character);
        }
        position = next_position;
    }
    quote
}

pub(crate) fn describe_table_name_parse_error(token: &Token, sql: &str) -> DataFusionError {
    if sql.trim_end().ends_with('.') {
        return describe_parse_error_with_extra_input(".");
    }
    if matches!(token, Token::EOF | Token::LParen) {
        return describe_end_of_input_parse_error();
    }
    describe_token_parse_error(token)
}

pub(crate) fn describe_table_name_token_parse_error(token: &Token) -> DataFusionError {
    match token {
        Token::SingleQuotedString(_) | Token::DoubleQuotedString(_) => {
            describe_parse_error(&token.to_string())
        }
        Token::Word(word) if word.quote_style == Some('"') => {
            describe_parse_error(&token.to_string())
        }
        _ => describe_token_parse_error(token),
    }
}

pub(crate) fn describe_parse_error(near: &str) -> DataFusionError {
    describe_parse_error_with_detail(near, false)
}

pub(crate) fn describe_end_of_input_parse_error() -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601"
                .to_string(),
        )),
        None,
    )
}

pub(crate) fn describe_token_parse_error(token: &Token) -> DataFusionError {
    let near = token.to_string();
    let has_extra_input = !matches!(token, Token::Comma | Token::DoubleQuotedString(_))
        && !matches!(token, Token::Word(word) if word.quote_style == Some('"'));
    describe_parse_error_with_detail(&near, has_extra_input)
}

fn describe_column_token_parse_error(token: &Token, sql: &str) -> DataFusionError {
    if sql.trim_end().ends_with('.') {
        describe_end_of_input_parse_error()
    } else {
        describe_token_parse_error(token)
    }
}

fn describe_partition_parse_error() -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(
            "[UNSUPPORTED_FEATURE.DESC_TABLE_COLUMN_PARTITION] The feature is not supported: \
             DESC TABLE COLUMN for a specific partition. SQLSTATE: 0A000"
                .to_string(),
        )),
        None,
    )
}

pub(crate) fn describe_parse_error_with_extra_input(near: &str) -> DataFusionError {
    describe_parse_error_with_detail(near, true)
}

fn describe_parse_error_with_detail(near: &str, has_extra_input: bool) -> DataFusionError {
    let detail = if has_extra_input {
        format!("Syntax error at or near '{near}': extra input '{near}'.")
    } else {
        format!("Syntax error at or near '{near}'.")
    };
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(format!(
            "[PARSE_SYNTAX_ERROR] {detail} SQLSTATE: 42601"
        ))),
        None,
    )
}

pub(crate) fn execute_describe_column(
    ctx: &SessionContext,
    describe: &DescribeTable,
    table: &Table,
) -> Result<DataFrame> {
    ctx.read_batch(describe_column_batch(describe, table)?)
}

pub(crate) fn describe_partition_section(
    schema: &IcebergSchema,
    spec: &iceberg::spec::PartitionSpec,
) -> Result<Vec<(String, String, Option<String>)>> {
    let fields = spec.fields();
    if fields.is_empty() {
        return Ok(Vec::new());
    }
    if fields
        .iter()
        .all(|field| matches!(&field.transform, Transform::Identity))
    {
        let mut rows = vec![
            (
                "# Partition Information".to_string(),
                String::new(),
                Some(String::new()),
            ),
            (
                "# col_name".to_string(),
                "data_type".to_string(),
                Some("comment".to_string()),
            ),
        ];
        for field in fields {
            let source = schema.field_by_id(field.source_id).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "partition field `{}` refers to unknown source id {}",
                    field.name, field.source_id
                ))
            })?;
            let arrow_type =
                iceberg::arrow::type_to_arrow_type(&source.field_type).map_err(iceberg_err)?;
            rows.push((
                source.name.clone(),
                spark_ddl_type_name(&arrow_type),
                source.doc.clone(),
            ));
        }
        return Ok(rows);
    }
    let mut rows = vec![
        (String::new(), String::new(), Some(String::new())),
        (
            "# Partitioning".to_string(),
            String::new(),
            Some(String::new()),
        ),
    ];
    for (index, field) in fields.iter().enumerate() {
        rows.push((
            format!("Part {index}"),
            describe_partition_field(schema, field)?,
            Some(String::new()),
        ));
    }
    Ok(rows)
}

pub(crate) fn describe_partition_field(
    schema: &IcebergSchema,
    field: &PartitionField,
) -> Result<String> {
    let source = schema.field_by_id(field.source_id).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "partition field `{}` refers to unknown source id {}",
            field.name, field.source_id
        ))
    })?;
    Ok(match &field.transform {
        Transform::Identity => source.name.clone(),
        Transform::Year => format!("years({})", source.name),
        Transform::Month => format!("months({})", source.name),
        Transform::Day => format!("days({})", source.name),
        Transform::Hour => format!("hours({})", source.name),
        Transform::Bucket(width) => format!("bucket({width}, {})", source.name),
        Transform::Truncate(width) => format!("truncate({width}, {})", source.name),
        Transform::Void => format!("void({})", source.name),
        Transform::Unknown => format!("unknown({})", source.name),
    })
}

fn describe_column_batch(describe: &DescribeTable, table: &Table) -> Result<RecordBatch> {
    let column = describe.column.as_deref().ok_or_else(|| {
        DataFusionError::Internal("describe column execution needs a column path".to_string())
    })?;
    let iceberg_schema = table.metadata().current_schema();
    let column_name = match column {
        [column_name] => column_name,
        [_, _] => {
            return Err(DataFusionError::Plan(format!(
                "[_LEGACY_ERROR_TEMP_1060] DESC TABLE COLUMN does not support nested column: {}.",
                column.join(".")
            )));
        }
        _ => return Err(nested_describe_column_error(column, iceberg_schema)?),
    };
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

fn nested_describe_column_error(
    column: &[String],
    schema: &IcebergSchema,
) -> Result<DataFusionError> {
    let base_name = column[..column.len() - 1].join(".");
    let field = schema
        .field_by_name_case_insensitive(&base_name)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "[_LEGACY_ERROR_TEMP_1060] DESC TABLE COLUMN does not support nested column: {}.",
                column.join(".")
            ))
        })?;
    let arrow_type = iceberg::arrow::type_to_arrow_type(&field.field_type).map_err(iceberg_err)?;
    Ok(DataFusionError::Plan(format!(
        "[INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from \"{base_name}\". Need a \
         complex type [STRUCT, ARRAY, MAP] but got \"{}\". SQLSTATE: 42000",
        spark_ddl_type_name(&arrow_type).to_ascii_uppercase(),
    )))
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
