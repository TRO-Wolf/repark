use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::{DataType, ObjectName};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use repark_core::EngineContext;
use repark_iceberg::write::alter::{ColumnPosition, starts_with_alter};
use repark_iceberg::write::nested_column::{ColumnPathChange, apply_column_path_changes};
use repark_iceberg::write::nested_type_sql::rewrite_nested_type_tokens;

use super::{FORM, invalidate};
use crate::create_table::{nested_type_parse_error, resolve_target, sql_type_to_iceberg};
use crate::schema_ddl::iceberg_err;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NestedAddColumn {
    path: Vec<String>,
    data_type: DataType,
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
    table: ObjectName,
    operation: NestedColumnOperation,
}

type ParseResult<T> = std::result::Result<T, ParserError>;

pub(crate) fn try_parse_nested_column_ddl(sql: &str) -> Option<Result<NestedColumnDdl>> {
    if !starts_with_alter(sql) {
        return None;
    }
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let tokens = match rewrite_nested_type_tokens(&tokens, true) {
        Ok(tokens) => tokens,
        Err(message) => return Some(Err(nested_type_parse_error(message))),
    };
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keywords(&[Keyword::ALTER, Keyword::TABLE]) {
        return None;
    }
    let table = parser.parse_object_name(false).ok()?;
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
        Ok(Some(operation)) => Some(Ok(NestedColumnDdl { table, operation })),
        Err(error) => Some(Err(DataFusionError::SQL(Box::new(error), None))),
    }
}

fn parse_column_path(parser: &mut Parser<'_>) -> ParseResult<Vec<String>> {
    let mut path = vec![parser.parse_identifier()?.value];
    while parser.consume_token(&Token::Period) {
        path.push(parser.parse_identifier()?.value);
    }
    Ok(path)
}

fn syntax_error_at(parser: &Parser<'_>) -> ParserError {
    ParserError::ParserError(format!(
        "[PARSE_SYNTAX_ERROR] Syntax error at or near '{}'. SQLSTATE: 42601",
        parser.peek_token().token
    ))
}

fn close_list(parser: &mut Parser<'_>, parenthesized: bool) -> ParseResult<()> {
    if parenthesized && !parser.consume_token(&Token::RParen) {
        return Err(syntax_error_at(parser));
    }
    let _ = parser.consume_token(&Token::SemiColon);
    if parser.peek_token().token == Token::EOF {
        Ok(())
    } else {
        Err(syntax_error_at(parser))
    }
}

fn parse_add_columns(parser: &mut Parser<'_>) -> ParseResult<Option<NestedColumnOperation>> {
    let parenthesized = parser.consume_token(&Token::LParen);
    let mut columns = Vec::new();
    let mut nested_seen = false;
    let outcome = loop {
        let path = match parse_column_path(parser) {
            Ok(path) => path,
            Err(error) => break Err(error),
        };
        nested_seen |= path.len() > 1;
        match parser
            .parse_data_type()
            .and_then(|data_type| parse_add_column_tail(parser, path, data_type))
        {
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

fn parse_add_column_tail(
    parser: &mut Parser<'_>,
    path: Vec<String>,
    data_type: DataType,
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
            column.position = Some(ColumnPosition::After(parser.parse_identifier()?.value));
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
    close_list(parser, false)?;
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

async fn path_change_for_add(
    cx: &EngineContext<'_>,
    column: &NestedAddColumn,
) -> Result<ColumnPathChange> {
    let field_type = sql_type_to_iceberg(cx.ctx, &column.data_type, FORM).await?;
    let Some((leaf, parent)) = column.path.split_last() else {
        return Err(DataFusionError::Plan(format!(
            "{FORM} ADD COLUMN expects a column name"
        )));
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
    cx: &EngineContext<'_>,
    ddl: NestedColumnDdl,
) -> Result<DataFrame> {
    let target = resolve_target(cx, &ddl.table, FORM)?;
    let table = target
        .catalog
        .load_table(&target.ident())
        .await
        .map_err(iceberg_err)?;
    let changes = match &ddl.operation {
        NestedColumnOperation::Add(columns) => {
            let mut changes = Vec::with_capacity(columns.len());
            for column in columns {
                changes.push(path_change_for_add(cx, column).await?);
            }
            changes
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
        return cx.ctx.read_empty();
    }
    apply_column_path_changes(target.catalog.as_ref(), &table, &changes)
        .await
        .map_err(iceberg_err)?;
    invalidate(cx, &target).await?;
    cx.ctx.read_empty()
}
