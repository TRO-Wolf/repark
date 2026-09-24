use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    AlterColumnOperation, DataType as SqlDataType, ExactNumberInfo, Ident, ObjectName, TimezoneInfo,
};
use datafusion::sql::sqlparser::dialect::SparkSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Token;
use iceberg::ErrorKind;
use repark_core::CatalogRegistry;
use repark_functions::timestamp_type::{SparkTimestampType, spark_timestamp_type_from_options};
use repark_iceberg::write::alter::{
    ColumnPosition, SchemaChange, apply_schema_changes_on_table, starts_with_alter,
};
use repark_iceberg::write::nested_column::{
    ColumnPathChange, NestedTypeRefusal, apply_column_path_changes, nested_add_refusal,
    nested_required_add_refusal, nested_spark_only_type_refusal, resolve_column_path,
    resolve_nested_type_change,
};

use crate::alter::table_parts_to_ident;
use crate::catalog_ops::table_or_view_not_found;
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
    AlterType {
        path: Vec<String>,
        data_type: SqlDataType,
    },
    Comment(Vec<(Vec<String>, String)>),
    MixedCommentList(Vec<String>),
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
    } else if names
        .parser
        .parse_keywords(&[Keyword::ALTER, Keyword::COLUMN])
    {
        parse_alter_column(&mut names, true)
    } else if names.parser.parse_keyword(Keyword::CHANGE)
        || names.parser.parse_keyword(Keyword::ALTER)
    {
        let _ = names.parser.parse_keyword(Keyword::COLUMN);
        parse_alter_column(&mut names, false)
    } else {
        return None;
    };
    match operation {
        Ok(None) => None,
        Ok(Some(operation)) => Some(match names.double_quoted {
            Some(quoted) => Err(verbatim_parser_error(syntax_error_near(&quoted))),
            None => match target_type_parse_refusal(&operation) {
                Some(message) => Err(verbatim_parser_error(message)),
                None => Ok(NestedColumnDdl {
                    table_parts,
                    operation,
                }),
            },
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

fn parse_alter_column(
    names: &mut NameParser<'_>,
    type_allowed: bool,
) -> ParseResult<Option<NestedColumnOperation>> {
    let path = match names.column_path() {
        Ok(path) => path,
        Err(error) if type_allowed => return Err(error),
        Err(_) => return Ok(None),
    };
    if names.parser.parse_keyword(Keyword::COMMENT) {
        return parse_comment_specs(names, path).map(Some);
    }
    if !type_allowed || !names.parser.parse_keyword(Keyword::TYPE) {
        return Ok(None);
    }
    if path.len() < 2 {
        return top_level_type_tail(&mut names.parser);
    }
    let data_type = names.parser.parse_data_type()?;
    let data_type = parse_unparameterized_type_parameters(&mut names.parser, data_type)?;
    expect_end(&mut names.parser)?;
    Ok(Some(NestedColumnOperation::AlterType { path, data_type }))
}

fn parse_comment_specs(
    names: &mut NameParser<'_>,
    first: Vec<String>,
) -> ParseResult<NestedColumnOperation> {
    let mut specs = vec![(first, comment_literal(&mut names.parser)?)];
    while names.parser.consume_token(&Token::Comma) {
        let path = names.column_path()?;
        if !names.parser.parse_keyword(Keyword::COMMENT) {
            return Ok(NestedColumnOperation::MixedCommentList(path));
        }
        specs.push((path, comment_literal(&mut names.parser)?));
    }
    let _ = names.parser.consume_token(&Token::SemiColon);
    match names.parser.peek_token().token {
        Token::EOF => Ok(NestedColumnOperation::Comment(specs)),
        extra => Err(ParserError::ParserError(format!(
            "[PARSE_SYNTAX_ERROR] Syntax error at or near '{extra}': extra input '{extra}'. \
             SQLSTATE: 42601"
        ))),
    }
}

fn comment_literal(parser: &mut Parser<'_>) -> ParseResult<String> {
    match parser.next_token().token {
        Token::SingleQuotedString(doc) | Token::DoubleQuotedString(doc) => Ok(doc),
        Token::EOF => Err(ParserError::ParserError(
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601".into(),
        )),
        _ => {
            parser.prev_token();
            Err(syntax_error_at(parser))
        }
    }
}

fn top_level_type_tail(parser: &mut Parser<'_>) -> ParseResult<Option<NestedColumnOperation>> {
    if parser.parse_data_type().is_err() {
        return Ok(None);
    }
    if parser.parse_keyword(Keyword::COMMENT) {
        parser.prev_token();
        return Err(syntax_error_at(parser));
    }
    Ok(None)
}

fn parse_unparameterized_type_parameters(
    parser: &mut Parser<'_>,
    data_type: SqlDataType,
) -> ParseResult<SqlDataType> {
    if !matches!(data_type, SqlDataType::Date | SqlDataType::Boolean)
        || !parser.consume_token(&Token::LParen)
    {
        return Ok(data_type);
    }
    let parameters = parser.parse_comma_separated(Parser::parse_literal_uint)?;
    parser.expect_token(&Token::RParen)?;
    Ok(SqlDataType::Custom(
        ObjectName::from(vec![Ident::new(data_type.to_string())]),
        parameters.iter().map(u64::to_string).collect(),
    ))
}

fn unparameterized_type_name(name: &ObjectName) -> bool {
    matches!(name.to_string().as_str(), "DATE" | "BOOLEAN")
}

fn target_type_parse_refusal(operation: &NestedColumnOperation) -> Option<String> {
    let NestedColumnOperation::AlterType { data_type, .. } = operation else {
        return None;
    };
    match data_type {
        SqlDataType::Varchar(None) | SqlDataType::Char(None) | SqlDataType::Character(None) => {
            Some(format!(
                "[DATATYPE_MISSING_SIZE] DataType \"{data_type}\" requires a length parameter, \
                 for example \"{data_type}\"(10). Please specify the length. SQLSTATE: 42K01"
            ))
        }
        SqlDataType::TinyInt(Some(_))
        | SqlDataType::SmallInt(Some(_))
        | SqlDataType::Int(Some(_))
        | SqlDataType::Integer(Some(_))
        | SqlDataType::BigInt(Some(_))
        | SqlDataType::Float(
            ExactNumberInfo::Precision(_) | ExactNumberInfo::PrecisionAndScale(..),
        )
        | SqlDataType::Double(
            ExactNumberInfo::Precision(_) | ExactNumberInfo::PrecisionAndScale(..),
        )
        | SqlDataType::String(Some(_))
        | SqlDataType::Binary(Some(_))
        | SqlDataType::TimestampNtz(Some(_))
        | SqlDataType::Timestamp(Some(_), TimezoneInfo::None) => {
            Some(unsupported_datatype(&data_type.to_string()))
        }
        SqlDataType::Custom(name, parameters) if unparameterized_type_name(name) => Some(
            unsupported_datatype(&format!("{name}({})", parameters.join(","))),
        ),
        _ => None,
    }
}

fn unsupported_datatype(spelling: &str) -> String {
    format!("[UNSUPPORTED_DATATYPE] Unsupported data type \"{spelling}\". SQLSTATE: 0A000")
}

fn spark_default_target_type(data_type: &SqlDataType) -> SqlDataType {
    match data_type {
        SqlDataType::Decimal(ExactNumberInfo::None)
        | SqlDataType::Numeric(ExactNumberInfo::None)
        | SqlDataType::Dec(ExactNumberInfo::None) => {
            SqlDataType::Decimal(ExactNumberInfo::PrecisionAndScale(10, 0))
        }
        other => other.clone(),
    }
}

fn spark_only_target_type(data_type: &SqlDataType) -> Option<String> {
    match data_type {
        SqlDataType::TinyInt(_) => Some("TINYINT".to_string()),
        SqlDataType::SmallInt(_) => Some("SMALLINT".to_string()),
        SqlDataType::Varchar(Some(length)) => Some(format!("VARCHAR({length})")),
        SqlDataType::Char(Some(length)) | SqlDataType::Character(Some(length)) => {
            Some(format!("CHAR({length})"))
        }
        _ => None,
    }
}

fn nested_type_error(refusal: NestedTypeRefusal) -> DataFusionError {
    match refusal {
        NestedTypeRefusal::Analysis(message) => DataFusionError::Plan(message),
        NestedTypeRefusal::Unsupported(message) => DataFusionError::Execution(message),
    }
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

fn column_doc_changes(
    schema: &iceberg::spec::Schema,
    specs: &[(Vec<String>, String)],
) -> Result<Vec<SchemaChange>> {
    specs
        .iter()
        .map(|(path, doc)| {
            resolve_column_path(schema, path).map(|name| SchemaChange::UpdateColumnDoc {
                name,
                doc: Some(doc.clone()),
            })
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(nested_type_error)
}

fn mixed_comment_list(path: &[String]) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "ALTER TABLE … ALTER COLUMN mixes COMMENT with another change for `{}` in one column \
         list; only a list of COMMENT changes is supported, so split the statement",
        path.join(".")
    ))
}

pub(crate) async fn execute_nested_column_ddl(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ddl: NestedColumnDdl,
) -> Result<DataFrame> {
    let (catalog_name, ident) = table_parts_to_ident(catalogs, &ddl.table_parts)?;
    let handle = catalog_handle(catalogs, &catalog_name)?;
    let namespace = crate::namespace_schema_name(ident.namespace());
    let table = handle.load_table(&ident).await.map_err(|error| {
        if matches!(
            error.kind(),
            ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
        ) {
            return table_or_view_not_found(&catalog_name, &namespace, ident.name());
        }
        iceberg_err(error)
    })?;
    match &ddl.operation {
        NestedColumnOperation::AlterType { path, data_type } => {
            let data_type = &spark_default_target_type(data_type);
            let table_name = crate::catalog_ops::quoted_table_display(&ddl.table_parts);
            let schema = table.metadata().current_schema();
            if let Some(to_type) = spark_only_target_type(data_type) {
                return Err(nested_type_error(nested_spark_only_type_refusal(
                    schema,
                    &table_name,
                    path,
                    &to_type,
                )));
            }
            let timestamp_type = spark_timestamp_type_from_options(ctx.copied_config().options());
            let to = sql_type_to_iceberg_with_timestamp_type(data_type, timestamp_type)?;
            let resolved = resolve_nested_type_change(schema, &table_name, path, &to)
                .map_err(nested_type_error)?;
            let operation = AlterColumnOperation::SetDataType {
                data_type: data_type.clone(),
                using: None,
                had_set: false,
            };
            let change: SchemaChange = crate::alter::schema_change_from_alter_column(
                &Ident::new(resolved),
                &operation,
                timestamp_type,
            )?;
            apply_schema_changes_on_table(handle.as_ref(), &table, &[change])
                .await
                .map_err(iceberg_err)?;
        }
        NestedColumnOperation::Comment(specs) => {
            let changes = column_doc_changes(table.metadata().current_schema(), specs)?;
            apply_schema_changes_on_table(handle.as_ref(), &table, &changes)
                .await
                .map_err(iceberg_err)?;
        }
        NestedColumnOperation::MixedCommentList(path) => {
            return Err(mixed_comment_list(path));
        }
        NestedColumnOperation::Add(columns) => {
            let timestamp_type = spark_timestamp_type_from_options(ctx.copied_config().options());
            let changes = columns
                .iter()
                .map(|column| path_change_for_add(column, timestamp_type))
                .collect::<Result<Vec<_>>>()?;
            if let Some(message) = nested_add_refusal(table.metadata().current_schema(), &changes) {
                return Err(DataFusionError::Plan(message));
            }
            if let Some(message) = nested_required_add_refusal(&changes) {
                return Err(DataFusionError::Execution(message));
            }
            apply_column_path_changes(handle.as_ref(), &table, &changes)
                .await
                .map_err(iceberg_err)?;
        }
        NestedColumnOperation::Rename { from, to } => {
            let changes = vec![ColumnPathChange::Rename {
                path: from.join("."),
                to: to.clone(),
            }];
            apply_column_path_changes(handle.as_ref(), &table, &changes)
                .await
                .map_err(iceberg_err)?;
        }
        NestedColumnOperation::Drop { paths, if_exists } => {
            let schema = table.metadata().current_schema();
            let changes = paths
                .iter()
                .map(|path| path.join("."))
                .filter(|name| !*if_exists || schema.field_by_name_case_insensitive(name).is_some())
                .map(|path| ColumnPathChange::Drop { path })
                .collect::<Vec<_>>();
            if changes.is_empty() {
                return ctx.read_empty();
            }
            apply_column_path_changes(handle.as_ref(), &table, &changes)
                .await
                .map_err(iceberg_err)?;
        }
    }
    reregister(ctx, handle.clone(), &catalog_name, &namespace).await?;
    ctx.read_empty()
}
