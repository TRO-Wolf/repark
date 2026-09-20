use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::DataType as SqlDataType;
use datafusion::sql::sqlparser::dialect::SparkSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::spec::{NullOrder, SortDirection, TableMetadata, Transform};
use iceberg::{Catalog, ErrorKind, TableIdent};
use repark_functions::timestamp_type::SparkTimestampType;
use repark_iceberg::write::alter::SchemaChange;
use repark_iceberg::write::nested_type_sql::rewrite_nested_type_tokens;

use crate::alter::IcebergAlterDdl;
use crate::create_table::sql_type_to_iceberg_with_timestamp_type;
use crate::iceberg_err;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReplaceColumnDef {
    pub(crate) name: String,
    pub(crate) data_type: SqlDataType,
    pub(crate) doc: Option<String>,
}

pub(crate) fn parse(sql: &str, table_parts: Vec<String>) -> Result<IcebergAlterDdl> {
    let dialect = SparkSqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|error| parse_error(error.to_string()))?;
    let tokens = rewrite_nested_type_tokens(&tokens, false).map_err(parse_error)?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keywords(&[Keyword::ALTER, Keyword::TABLE]) {
        return Err(parse_error(expected("ALTER TABLE", &parser)));
    }
    parser.parse_object_name(false).map_err(parser_error)?;
    if !parser.parse_keywords(&[Keyword::REPLACE, Keyword::COLUMNS]) {
        return Err(parse_error(expected("REPLACE COLUMNS", &parser)));
    }
    if !parser.consume_token(&Token::LParen) {
        return Err(parse_error(
            "ALTER TABLE REPLACE COLUMNS expects `(col TYPE, …)`".to_string(),
        ));
    }
    let mut columns = Vec::new();
    loop {
        columns.push(parse_column(&mut parser)?);
        if !parser.consume_token(&Token::Comma) {
            break;
        }
    }
    if !parser.consume_token(&Token::RParen) {
        return Err(parse_error(expected("`)`", &parser)));
    }
    let _ = parser.consume_token(&Token::SemiColon);
    if parser.peek_token().token != Token::EOF {
        return Err(parse_error(expected("end of statement", &parser)));
    }
    Ok(IcebergAlterDdl::ReplaceColumns {
        table_parts,
        columns,
    })
}

fn parse_column(parser: &mut Parser<'_>) -> Result<ReplaceColumnDef> {
    let name = parser.parse_identifier().map_err(parser_error)?.value;
    if parser.consume_token(&Token::Period) {
        return Err(hive_style_unsupported("Replacing with a nested column"));
    }
    let data_type = parser.parse_data_type().map_err(parser_error)?;
    let mut doc = None;
    loop {
        if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            return Err(hive_style_unsupported("NOT NULL"));
        }
        if parser.parse_keyword(Keyword::FIRST) || parser.parse_keyword(Keyword::AFTER) {
            return Err(hive_style_unsupported("Column position"));
        }
        if parser.parse_keyword(Keyword::COMMENT) {
            doc = Some(parser.parse_literal_string().map_err(parser_error)?);
            continue;
        }
        break;
    }
    Ok(ReplaceColumnDef {
        name,
        data_type,
        doc,
    })
}

pub(crate) async fn plan(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    columns: &[ReplaceColumnDef],
    timestamp_type: SparkTimestampType,
) -> Result<Vec<SchemaChange>> {
    refuse_duplicate_names(columns)?;
    let table = catalog.load_table(ident).await.map_err(iceberg_err)?;
    let metadata = table.metadata();
    refuse_lost_partition_source(metadata)?;
    refuse_lost_sort_source(metadata)?;
    let mut changes: Vec<SchemaChange> = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| SchemaChange::DropColumn {
            name: field.name.clone(),
        })
        .collect();
    for column in columns {
        changes.push(SchemaChange::AddColumn {
            name: column.name.clone(),
            field_type: sql_type_to_iceberg_with_timestamp_type(&column.data_type, timestamp_type)?,
            doc: column.doc.clone(),
            required: false,
            position: None,
        });
    }
    Ok(changes)
}

fn refuse_duplicate_names(columns: &[ReplaceColumnDef]) -> Result<()> {
    for (index, column) in columns.iter().enumerate() {
        if columns[..index]
            .iter()
            .any(|earlier| earlier.name.eq_ignore_ascii_case(&column.name))
        {
            return Err(DataFusionError::Plan(format!(
                "[COLUMN_ALREADY_EXISTS] The column `{}` already exists. Choose another name or \
                 rename the existing column. SQLSTATE: 42711",
                column.name
            )));
        }
    }
    Ok(())
}

fn refuse_lost_partition_source(metadata: &TableMetadata) -> Result<()> {
    for field in metadata.default_partition_spec().fields() {
        if field.transform == Transform::Void {
            continue;
        }
        return Err(validation_error(format!(
            "Cannot find source column for partition field: {}: {}: {}({})",
            field.field_id, field.name, field.transform, field.source_id
        )));
    }
    Ok(())
}

fn refuse_lost_sort_source(metadata: &TableMetadata) -> Result<()> {
    let Some(field) = metadata.default_sort_order().fields.first() else {
        return Ok(());
    };
    let direction = match field.direction {
        SortDirection::Ascending => "ASC",
        SortDirection::Descending => "DESC",
    };
    let nulls = match field.null_order {
        NullOrder::First => "NULLS FIRST",
        NullOrder::Last => "NULLS LAST",
    };
    Err(validation_error(format!(
        "Cannot find source column for sort field: {}({}) {direction} {nulls}",
        field.transform, field.source_id
    )))
}

fn validation_error(message: String) -> DataFusionError {
    iceberg_err(iceberg::Error::new(ErrorKind::DataInvalid, message))
}

fn hive_style_unsupported(operation: &str) -> DataFusionError {
    parse_error(format!(
        "{operation} is not supported in Hive-style REPLACE COLUMNS."
    ))
}

fn parse_error(message: String) -> DataFusionError {
    parser_error(ParserError::ParserError(message))
}

fn parser_error(error: ParserError) -> DataFusionError {
    DataFusionError::SQL(Box::new(error), None)
}

fn expected(what: &str, parser: &Parser<'_>) -> String {
    format!(
        "ALTER TABLE REPLACE COLUMNS expects {what}, found: {}",
        parser.peek_token().token
    )
}
