use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::DataType as SqlDataType;
use datafusion::sql::sqlparser::dialect::SparkSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use repark_core::CatalogRegistry;
use repark_functions::timestamp_type::{SparkTimestampType, spark_timestamp_type_from_options};
use repark_iceberg::write::alter::{ColumnPosition, starts_with_alter};
use repark_iceberg::write::nested_column::{
    ColumnPathChange, apply_column_path_changes, nested_add_refusal, nested_required_add_refusal,
};

use crate::alter::table_parts_to_ident;
use crate::create_table::sql_type_to_iceberg_with_timestamp_type;
use crate::{catalog_handle, iceberg_err, name_parts, reregister};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NestedAddColumn {
    path: Vec<String>,
    data_type: SqlDataType,
    required: bool,
    doc: Option<String>,
    position: Option<ColumnPosition>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum NestedColumnOperation {
    Add(Vec<NestedAddColumn>),
    Rename {
        from: Vec<String>,
        to: String,
    },
    Drop {
        paths: Vec<Vec<String>>,
        if_exists: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NestedColumnDdl {
    table_parts: Vec<String>,
    operation: NestedColumnOperation,
}

type ParseResult<T> = std::result::Result<T, ParserError>;

pub(crate) fn try_parse_nested_column_ddl(sql: &str) -> Option<Result<NestedColumnDdl>> {
    if !starts_with_alter(sql) {
        return None;
    }
    let dialect = SparkSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(sql).ok()?;
    if !parser.parse_keywords(&[Keyword::ALTER, Keyword::TABLE]) {
        return None;
    }
    let table_parts = name_parts(&parser.parse_object_name(false).ok()?);
    let operation = if parser.parse_keyword(Keyword::ADD) {
        if !(parser.parse_keyword(Keyword::COLUMN) || parser.parse_keyword(Keyword::COLUMNS)) {
            return None;
        }
        parse_add_columns(&mut parser)
    } else if parser.parse_keywords(&[Keyword::RENAME, Keyword::COLUMN]) {
        parse_rename_column(&mut parser)
    } else if parser.parse_keyword(Keyword::DROP) {
        if !(parser.parse_keyword(Keyword::COLUMN) || parser.parse_keyword(Keyword::COLUMNS)) {
            return None;
        }
        parse_drop_columns(&mut parser)
    } else {
        return None;
    };
    match operation {
        Ok(None) => None,
        Ok(Some(operation)) => Some(match first_double_quoted_word(sql) {
            Some(quoted) => Err(verbatim_parser_error(syntax_error_near(&quoted))),
            None => Ok(NestedColumnDdl {
                table_parts,
                operation,
            }),
        }),
        Err(error) => Some(Err(parser_error(error))),
    }
}

fn verbatim_parser_error(message: String) -> DataFusionError {
    DataFusionError::Context(
        message.clone(),
        Box::new(parser_error(ParserError::ParserError(message))),
    )
}

fn first_double_quoted_word(sql: &str) -> Option<Token> {
    Tokenizer::new(&SparkSqlDialect {}, sql)
        .tokenize()
        .ok()?
        .into_iter()
        .find(|token| match token {
            Token::Word(word) => word.quote_style == Some('"'),
            Token::DoubleQuotedString(_) => true,
            _ => false,
        })
}

fn syntax_error_near(token: &Token) -> String {
    format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{token}'. SQLSTATE: 42601")
}

fn parser_error(error: ParserError) -> DataFusionError {
    DataFusionError::SQL(Box::new(error), None)
}

fn parse_column_path(parser: &mut Parser<'_>) -> ParseResult<Vec<String>> {
    let mut path = vec![parser.parse_identifier()?.value];
    while parser.consume_token(&Token::Period) {
        path.push(parser.parse_identifier()?.value);
    }
    Ok(path)
}

fn expect_end(parser: &mut Parser<'_>) -> ParseResult<()> {
    let _ = parser.consume_token(&Token::SemiColon);
    if parser.peek_token().token == Token::EOF {
        Ok(())
    } else {
        Err(syntax_error_at(parser))
    }
}

fn parse_add_columns(parser: &mut Parser<'_>) -> ParseResult<Option<NestedColumnOperation>> {
    let parenthesized = parser.consume_token(&Token::LParen);
    let mut columns: Vec<NestedAddColumn> = Vec::new();
    let mut nested_seen = false;
    let outcome = loop {
        let path = match parse_column_path(parser) {
            Ok(path) => path,
            Err(error) => break Err(error),
        };
        nested_seen |= path.len() > 1;
        let column = parser
            .parse_data_type()
            .and_then(|data_type| parse_add_column_tail(parser, path, data_type));
        match column {
            Ok(column) => columns.push(column),
            Err(error) => break Err(error),
        }
        if !parser.consume_token(&Token::Comma) {
            break close_list(parser, parenthesized);
        }
    };
    if !nested_seen {
        return Ok(None);
    }
    outcome?;
    Ok(Some(NestedColumnOperation::Add(columns)))
}

fn close_list(parser: &mut Parser<'_>, parenthesized: bool) -> ParseResult<()> {
    if parenthesized && !parser.consume_token(&Token::RParen) {
        return Err(syntax_error_at(parser));
    }
    expect_end(parser)
}

fn syntax_error_at(parser: &Parser<'_>) -> ParserError {
    ParserError::ParserError(syntax_error_near(&parser.peek_token().token))
}

fn parse_add_column_tail(
    parser: &mut Parser<'_>,
    path: Vec<String>,
    data_type: SqlDataType,
) -> ParseResult<NestedAddColumn> {
    let mut column = NestedAddColumn {
        path,
        data_type,
        required: false,
        doc: None,
        position: None,
    };
    loop {
        if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            column.required = true;
        } else if parser.parse_keyword(Keyword::NULL) {
            column.required = false;
        } else if parser.parse_keyword(Keyword::COMMENT) {
            column.doc = Some(parser.parse_literal_string()?);
        } else if parser.parse_keyword(Keyword::FIRST) {
            column.position = Some(ColumnPosition::First);
        } else if parser.parse_keyword(Keyword::AFTER) {
            let reference = parser.parse_identifier()?.value;
            column.position = Some(ColumnPosition::After(reference));
        } else {
            return Ok(column);
        }
    }
}

fn parse_rename_column(parser: &mut Parser<'_>) -> ParseResult<Option<NestedColumnOperation>> {
    let Ok(from) = parse_column_path(parser) else {
        return Ok(None);
    };
    if from.len() < 2 {
        return Ok(None);
    }
    parser.expect_keyword(Keyword::TO)?;
    let to = parser.parse_identifier()?.value;
    expect_end(parser)?;
    Ok(Some(NestedColumnOperation::Rename { from, to }))
}

fn parse_drop_columns(parser: &mut Parser<'_>) -> ParseResult<Option<NestedColumnOperation>> {
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let parenthesized = parser.consume_token(&Token::LParen);
    let mut paths: Vec<Vec<String>> = Vec::new();
    let outcome = loop {
        match parse_column_path(parser) {
            Ok(path) => paths.push(path),
            Err(error) => break Err(error),
        }
        if !parser.consume_token(&Token::Comma) {
            break close_list(parser, parenthesized);
        }
    };
    if paths.iter().all(|path| path.len() < 2) {
        return Ok(None);
    }
    outcome?;
    Ok(Some(NestedColumnOperation::Drop { paths, if_exists }))
}

fn path_change_for_add(
    column: &NestedAddColumn,
    timestamp_type: SparkTimestampType,
) -> Result<ColumnPathChange> {
    let field_type = sql_type_to_iceberg_with_timestamp_type(&column.data_type, timestamp_type)?;
    let Some((leaf, parent)) = column.path.split_last() else {
        return Err(DataFusionError::Plan(
            "ALTER TABLE ADD COLUMN expects a column name".to_string(),
        ));
    };
    Ok(ColumnPathChange::Add {
        parent: (!parent.is_empty()).then(|| parent.join(".")),
        name: leaf.clone(),
        field_type,
        doc: column.doc.clone(),
        required: column.required,
        position: column.position.clone(),
    })
}

pub(crate) async fn execute_nested_column_ddl(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ddl: NestedColumnDdl,
) -> Result<DataFrame> {
    let (catalog_name, ident) = table_parts_to_ident(&ddl.table_parts)?;
    let handle = catalog_handle(catalogs, &catalog_name)?;
    let table = handle.load_table(&ident).await.map_err(iceberg_err)?;
    let changes = match &ddl.operation {
        NestedColumnOperation::Add(columns) => {
            let timestamp_type = spark_timestamp_type_from_options(ctx.copied_config().options());
            columns
                .iter()
                .map(|column| path_change_for_add(column, timestamp_type))
                .collect::<Result<Vec<_>>>()?
        }
        NestedColumnOperation::Rename { from, to } => vec![ColumnPathChange::Rename {
            path: from.join("."),
            to: to.clone(),
        }],
        NestedColumnOperation::Drop { paths, if_exists } => {
            let schema = table.metadata().current_schema();
            paths
                .iter()
                .map(|path| path.join("."))
                .filter(|name| !*if_exists || schema.field_by_name_case_insensitive(name).is_some())
                .map(|path| ColumnPathChange::Drop { path })
                .collect()
        }
    };
    if changes.is_empty() {
        return ctx.read_empty();
    }
    if let Some(message) = nested_add_refusal(table.metadata().current_schema(), &changes) {
        return Err(DataFusionError::Plan(message));
    }
    if let Some(message) = nested_required_add_refusal(&changes) {
        return Err(DataFusionError::Execution(message));
    }
    apply_column_path_changes(handle.as_ref(), &table, &changes)
        .await
        .map_err(iceberg_err)?;
    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, handle.clone(), &catalog_name, &namespace).await?;
    ctx.read_empty()
}
