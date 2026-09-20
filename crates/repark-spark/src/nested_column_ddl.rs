use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{DataType as SqlDataType, Ident};
use datafusion::sql::sqlparser::dialect::SparkSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Token;
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
    let mut names = NameParser {
        parser,
        double_quoted: None,
    };
    let operation = if names.parser.parse_keyword(Keyword::ADD) {
        if !(names.parser.parse_keyword(Keyword::COLUMN)
            || names.parser.parse_keyword(Keyword::COLUMNS))
        {
            return None;
        }
        parse_add_columns(&mut names)
    } else if names
        .parser
        .parse_keywords(&[Keyword::RENAME, Keyword::COLUMN])
    {
        parse_rename_column(&mut names)
    } else if names.parser.parse_keyword(Keyword::DROP) {
        if !(names.parser.parse_keyword(Keyword::COLUMN)
            || names.parser.parse_keyword(Keyword::COLUMNS))
        {
            return None;
        }
        parse_drop_columns(&mut names)
    } else {
        return None;
    };
    match operation {
        Ok(None) => None,
        Ok(Some(operation)) => Some(match names.double_quoted {
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

struct NameParser<'a> {
    parser: Parser<'a>,
    double_quoted: Option<Ident>,
}

impl NameParser<'_> {
    fn name(&mut self) -> ParseResult<String> {
        let ident = self.parser.parse_identifier()?;
        if ident.quote_style == Some('"') && self.double_quoted.is_none() {
            self.double_quoted = Some(ident.clone());
        }
        Ok(ident.value)
    }

    fn column_path(&mut self) -> ParseResult<Vec<String>> {
        let mut path = vec![self.name()?];
        while self.parser.consume_token(&Token::Period) {
            path.push(self.name()?);
        }
        Ok(path)
    }
}

fn syntax_error_near(token: &impl std::fmt::Display) -> String {
    format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{token}'. SQLSTATE: 42601")
}

fn parser_error(error: ParserError) -> DataFusionError {
    DataFusionError::SQL(Box::new(error), None)
}

fn expect_end(parser: &mut Parser<'_>) -> ParseResult<()> {
    let _ = parser.consume_token(&Token::SemiColon);
    if parser.peek_token().token == Token::EOF {
        Ok(())
    } else {
        Err(syntax_error_at(parser))
    }
}

fn parse_add_columns(names: &mut NameParser<'_>) -> ParseResult<Option<NestedColumnOperation>> {
    let parenthesized = names.parser.consume_token(&Token::LParen);
    let mut columns: Vec<NestedAddColumn> = Vec::new();
    let mut nested_seen = false;
    let outcome = loop {
        let path = match names.column_path() {
            Ok(path) => path,
            Err(error) => break Err(error),
        };
        nested_seen |= path.len() > 1;
        let column = names
            .parser
            .parse_data_type()
            .and_then(|data_type| parse_add_column_tail(names, path, data_type));
        match column {
            Ok(column) => columns.push(column),
            Err(error) => break Err(error),
        }
        if !names.parser.consume_token(&Token::Comma) {
            break close_list(&mut names.parser, parenthesized);
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
    names: &mut NameParser<'_>,
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
        let parser = &mut names.parser;
        if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            column.required = true;
        } else if parser.parse_keyword(Keyword::NULL) {
            column.required = false;
        } else if parser.parse_keyword(Keyword::COMMENT) {
            column.doc = Some(parser.parse_literal_string()?);
        } else if parser.parse_keyword(Keyword::FIRST) {
            column.position = Some(ColumnPosition::First);
        } else if parser.parse_keyword(Keyword::AFTER) {
            let reference = names.name()?;
            column.position = Some(ColumnPosition::After(reference));
        } else {
            return Ok(column);
        }
    }
}

fn parse_rename_column(names: &mut NameParser<'_>) -> ParseResult<Option<NestedColumnOperation>> {
    let Ok(from) = names.column_path() else {
        return Ok(None);
    };
    if from.len() < 2 {
        return Ok(None);
    }
    names.parser.expect_keyword(Keyword::TO)?;
    let to = names.name()?;
    expect_end(&mut names.parser)?;
    Ok(Some(NestedColumnOperation::Rename { from, to }))
}

fn parse_drop_columns(names: &mut NameParser<'_>) -> ParseResult<Option<NestedColumnOperation>> {
    let if_exists = names.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let parenthesized = names.parser.consume_token(&Token::LParen);
    let mut paths: Vec<Vec<String>> = Vec::new();
    let outcome = loop {
        match names.column_path() {
            Ok(path) => paths.push(path),
            Err(error) => break Err(error),
        }
        if !names.parser.consume_token(&Token::Comma) {
            break close_list(&mut names.parser, parenthesized);
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
    let (catalog_name, ident) = table_parts_to_ident(catalogs, &ddl.table_parts)?;
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
