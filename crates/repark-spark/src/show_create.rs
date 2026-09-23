use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::spec::{NestedField, PrimitiveType, TableMetadata, Transform, Type as IcebergType};
use iceberg::{ErrorKind, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::catalog_ops::{
    catalog_handle, iceberg_err, name_parts, not_supported_command_for_v2_table,
    table_or_view_not_found,
};
use crate::describe_show::{describe_partition_field, quote_namespace_name_if_needed};
use crate::namespace_ddl::consume_word;
use crate::spark_type_names::spark_ddl_type_name;
use crate::table_props_view::spark_table_properties;
use crate::type_table::{SPARK_TYPE_NAME_DEPTH_FALLBACK, SPARK_TYPE_NAME_MAX_DEPTH};
use crate::write_options::StatementWriteOptions;

const TABLE_OPTION_PREFIX: &str = "option.";

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ShowCreateStatement {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) as_serde: bool,
}

impl ShowCreateStatement {
    fn complete_from_session(&mut self, catalogs: &CatalogRegistry) {
        if self.catalog.is_empty() || self.namespace.is_empty() {
            let (catalog, namespace) = crate::use_ddl::session_defaults(catalogs);
            if self.catalog.is_empty() {
                self.catalog = catalog;
            }
            if self.namespace.is_empty() {
                self.namespace = namespace;
            }
        }
    }

    fn ident(&self) -> TableIdent {
        TableIdent::new(
            NamespaceIdent::new(self.namespace.clone()),
            self.table.clone(),
        )
    }
}

pub(crate) fn try_parse_show_create(sql: &str) -> Option<Result<ShowCreateStatement>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keywords(&[Keyword::SHOW, Keyword::CREATE, Keyword::TABLE]) {
        return None;
    }
    if at_statement_end(&parser) {
        return Some(Err(parse_class_error(
            "[INVALID_STATEMENT_OR_CLAUSE] The statement or clause: SHOW CREATE TABLE is not \
             valid. SQLSTATE: 42601"
                .to_string(),
        )));
    }
    let Ok(name) = parser.parse_object_name(false) else {
        return Some(Err(syntax_error_at(&parser)));
    };
    let as_serde = if parser.parse_keyword(Keyword::AS) {
        if !consume_word(&mut parser, "SERDE") {
            return Some(Err(syntax_error_at(&parser)));
        }
        true
    } else {
        false
    };
    if !at_statement_end(&parser) {
        return Some(Err(syntax_error_at(&parser)));
    }
    let parts = name_parts(&name);
    let (catalog, namespace, table) = match parts.as_slice() {
        [catalog, namespace, table] => (catalog.clone(), namespace.clone(), table.clone()),
        [namespace, table] => (String::new(), namespace.clone(), table.clone()),
        [table] => (String::new(), String::new(), table.clone()),
        _ => return None,
    };
    Some(Ok(ShowCreateStatement {
        catalog,
        namespace,
        table,
        as_serde,
    }))
}

fn at_statement_end(parser: &Parser) -> bool {
    matches!(parser.peek_token().token, Token::EOF | Token::SemiColon)
}

fn syntax_error_at(parser: &Parser) -> DataFusionError {
    let near = match parser.peek_token().token {
        Token::EOF => "end of input".to_string(),
        token => format!("'{token}'"),
    };
    parse_class_error(format!(
        "[PARSE_SYNTAX_ERROR] Syntax error at or near {near}. SQLSTATE: 42601"
    ))
}

fn parse_class_error(message: String) -> DataFusionError {
    DataFusionError::SQL(Box::new(ParserError::ParserError(message)), None)
}

pub(crate) async fn try_show_create_intercept(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &StatementWriteOptions,
) -> Option<Result<DataFrame>> {
    let parsed = try_parse_show_create(sql)?;
    let mut statement = match parsed.and_then(|statement| {
        write_options
            .refuse_if_non_empty("SHOW CREATE TABLE")
            .map(|()| statement)
    }) {
        Ok(statement) => statement,
        Err(error) => return Some(Err(error)),
    };
    let shadowed = statement.catalog.is_empty()
        && statement.namespace.is_empty()
        && resolves_in_session(ctx, &statement.table).await;
    statement.complete_from_session(catalogs);
    if shadowed || catalogs.get(&statement.catalog).is_none() {
        return None;
    }
    match catalogs
        .is_view(&statement.catalog, &statement.ident())
        .await
    {
        Ok(true) => None,
        Ok(false) => Some(execute_show_create(ctx, catalogs, statement).await),
        Err(error) => Some(Err(error)),
    }
}

async fn resolves_in_session(ctx: &SessionContext, table: &str) -> bool {
    ctx.table_provider(TableReference::Bare {
        table: table.into(),
    })
    .await
    .is_ok()
}

pub(crate) async fn execute_show_create(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: ShowCreateStatement,
) -> Result<DataFrame> {
    let handle = catalog_handle(catalogs, &statement.catalog)?;
    let table = match handle.load_table(&statement.ident()).await {
        Ok(table) => table,
        Err(error) if error.kind() == ErrorKind::TableNotFound => {
            return Err(table_or_view_not_found(
                &statement.catalog,
                &statement.namespace,
                &statement.table,
            ));
        }
        Err(error) => return Err(iceberg_err(error)),
    };
    if statement.as_serde {
        return Err(not_supported_command_for_v2_table(
            "SHOW CREATE TABLE AS SERDE",
        ));
    }
    let text = render_create_table(&statement, table.metadata())?;
    ctx.read_batch(show_create_batch(text)?)
}

fn show_create_batch(text: String) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "createtab_stmt",
        DataType::Utf8,
        false,
    )]));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![text]))],
    )?)
}

fn render_create_table(
    statement: &ShowCreateStatement,
    metadata: &TableMetadata,
) -> Result<String> {
    let name = [&statement.catalog, &statement.namespace, &statement.table]
        .map(|part| quote_namespace_name_if_needed(part))
        .join(".");
    let schema = metadata.current_schema();
    let columns = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| column_ddl(field, " ", 0))
        .collect::<Result<Vec<String>>>()?;
    let mut text = format!(
        "CREATE TABLE {name} (\n  {})\nUSING iceberg\n",
        columns.join(",\n  ")
    );
    let (options, properties) = split_table_options(spark_table_properties(metadata));
    if !options.is_empty() {
        text.push_str("OPTIONS ");
        text.push_str(&concat_by_multi_lines(&options));
    }
    let partitioning = metadata
        .default_partition_spec()
        .fields()
        .iter()
        .filter(|field| field.transform != Transform::Void)
        .map(|field| describe_partition_field(schema, field))
        .collect::<Result<Vec<String>>>()?;
    if !partitioning.is_empty() {
        text.push_str(&format!("PARTITIONED BY ({})\n", partitioning.join(", ")));
    }
    if let Some(comment) = metadata.properties().get("comment") {
        text.push_str(&format!("COMMENT {}\n", spark_sql_string_literal(comment)));
    }
    text.push_str(&format!(
        "LOCATION {}\n",
        spark_sql_string_literal(metadata.location())
    ));
    text.push_str(&render_tblproperties_clause(&properties));
    Ok(text)
}

fn split_table_options(
    pairs: Vec<(String, String)>,
) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let options: Vec<(String, String)> = pairs
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(TABLE_OPTION_PREFIX)
                .map(|option| (option.to_string(), value.clone()))
        })
        .collect();
    let properties = pairs
        .into_iter()
        .filter(|(key, _)| {
            !key.starts_with(TABLE_OPTION_PREFIX)
                && !options.iter().any(|(option, _)| option == key)
        })
        .collect();
    (options, properties)
}

fn column_ddl(field: &NestedField, separator: &str, depth: usize) -> Result<String> {
    let mut ddl = format!(
        "{}{separator}{}",
        quote_namespace_name_if_needed(&field.name),
        spark_sql_type(&field.field_type, depth)?
    );
    if field.required {
        ddl.push_str(" NOT NULL");
    }
    if let Some(doc) = &field.doc {
        ddl.push_str(" COMMENT ");
        ddl.push_str(&spark_sql_string_literal(doc));
    }
    Ok(ddl)
}

fn spark_sql_type(field_type: &IcebergType, depth: usize) -> Result<String> {
    if depth >= SPARK_TYPE_NAME_MAX_DEPTH {
        return Ok(SPARK_TYPE_NAME_DEPTH_FALLBACK.to_string());
    }
    Ok(match field_type {
        IcebergType::Primitive(PrimitiveType::Uuid) => "STRING".to_string(),
        IcebergType::Primitive(PrimitiveType::Fixed(_)) => "BINARY".to_string(),
        IcebergType::Primitive(PrimitiveType::Unknown) => "VOID".to_string(),
        IcebergType::Primitive(_) => {
            let arrow = iceberg::arrow::type_to_arrow_type(field_type).map_err(iceberg_err)?;
            spark_ddl_type_name(&arrow).to_ascii_uppercase()
        }
        IcebergType::List(list) => format!(
            "ARRAY<{}>",
            spark_sql_type(&list.element_field.field_type, depth + 1)?
        ),
        IcebergType::Map(map) => format!(
            "MAP<{}, {}>",
            spark_sql_type(&map.key_field.field_type, depth + 1)?,
            spark_sql_type(&map.value_field.field_type, depth + 1)?
        ),
        IcebergType::Struct(structure) => {
            let fields = structure
                .fields()
                .iter()
                .map(|field| column_ddl(field, ": ", depth + 1))
                .collect::<Result<Vec<String>>>()?;
            format!("STRUCT<{}>", fields.join(", "))
        }
        IcebergType::Variant => "VARIANT".to_string(),
    })
}

pub(crate) fn render_tblproperties_clause(pairs: &[(String, String)]) -> String {
    if pairs.is_empty() {
        return String::new();
    }
    format!("TBLPROPERTIES {}", concat_by_multi_lines(pairs))
}

fn concat_by_multi_lines(pairs: &[(String, String)]) -> String {
    let lines: Vec<String> = pairs
        .iter()
        .map(|(key, value)| {
            format!(
                "{} = {}",
                spark_sql_string_literal(key),
                spark_sql_string_literal(value)
            )
        })
        .collect();
    format!("(\n  {})\n", lines.join(",\n  "))
}

pub(crate) fn spark_sql_string_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "\\'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn spark_sql_string_literal_escapes_single_quotes_only() {
        assert_eq!(spark_sql_string_literal("v"), "'v'");
        assert_eq!(spark_sql_string_literal("it's"), "'it\\'s'");
        assert_eq!(spark_sql_string_literal("a\\b"), "'a\\b'");
        assert_eq!(spark_sql_string_literal(""), "''");
    }

    #[test]
    fn render_tblproperties_clause_matches_spark_layout() {
        assert_eq!(
            render_tblproperties_clause(&pairs(&[("a.b", "it's"), ("k", "v")])),
            "TBLPROPERTIES (\n  'a.b' = 'it\\'s',\n  'k' = 'v')\n"
        );
        assert_eq!(
            render_tblproperties_clause(&pairs(&[("k", "v")])),
            "TBLPROPERTIES (\n  'k' = 'v')\n"
        );
        assert_eq!(render_tblproperties_clause(&[]), "");
    }

    #[test]
    fn split_table_options_moves_option_keys_and_drops_shadowed_properties() {
        let (options, properties) =
            split_table_options(pairs(&[("foo", "1"), ("k", "v"), ("option.foo", "2")]));
        assert_eq!(options, pairs(&[("foo", "2")]));
        assert_eq!(properties, pairs(&[("k", "v")]));
    }

    #[test]
    fn parse_accepts_one_two_and_three_part_names_and_as_serde() {
        let parsed = try_parse_show_create("show create table ice.sales.t")
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed,
            ShowCreateStatement {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "t".to_string(),
                as_serde: false,
            }
        );
        let parsed = try_parse_show_create("SHOW CREATE TABLE sales.t AS SERDE;")
            .unwrap()
            .unwrap();
        assert!(parsed.as_serde);
        assert!(parsed.catalog.is_empty());
        let parsed = try_parse_show_create("SHOW CREATE TABLE `we-ird`")
            .unwrap()
            .unwrap();
        assert_eq!(parsed.table, "we-ird");
    }

    #[test]
    fn parse_leaves_near_misses_alone() {
        for sql in [
            "SHOW CREATE VIEW v",
            "SHOW CREATE",
            "SHOW TABLES",
            "SHOW COLUMNS IN t",
            "SHOW TBLPROPERTIES t",
            "SHOW CREATE TABLE a.b.c.d",
            "SELECT 1",
        ] {
            assert!(try_parse_show_create(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn parse_refuses_malformed_forms_loudly() {
        let missing = try_parse_show_create("SHOW CREATE TABLE")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(
            missing.contains("[INVALID_STATEMENT_OR_CLAUSE]"),
            "{missing}"
        );
        let trailing = try_parse_show_create("SHOW CREATE TABLE t EXTRA")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(trailing.contains("[PARSE_SYNTAX_ERROR]"), "{trailing}");
        let as_json = try_parse_show_create("SHOW CREATE TABLE t AS JSON")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(as_json.contains("[PARSE_SYNTAX_ERROR]"), "{as_json}");
    }

    #[test]
    fn spark_sql_type_spells_nested_types_like_spark() {
        let structure = IcebergType::Struct(iceberg::spec::StructType::new(vec![
            Arc::new(NestedField::required(
                1,
                "x",
                IcebergType::Primitive(PrimitiveType::Int),
            )),
            Arc::new(
                NestedField::optional(2, "y", IcebergType::Primitive(PrimitiveType::String))
                    .with_doc("it's"),
            ),
        ]));
        assert_eq!(
            spark_sql_type(&structure, 0).unwrap(),
            "STRUCT<x: INT NOT NULL, y: STRING COMMENT 'it\\'s'>"
        );
        let map = IcebergType::Map(iceberg::spec::MapType::new(
            Arc::new(NestedField::map_key_element(
                3,
                IcebergType::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::map_value_element(
                4,
                IcebergType::Primitive(PrimitiveType::Decimal {
                    precision: 6,
                    scale: 2,
                }),
                false,
            )),
        ));
        assert_eq!(
            spark_sql_type(&map, 0).unwrap(),
            "MAP<STRING, DECIMAL(6,2)>"
        );
        for (primitive, expected) in [
            (PrimitiveType::Uuid, "STRING"),
            (PrimitiveType::Fixed(4), "BINARY"),
            (PrimitiveType::Timestamp, "TIMESTAMP_NTZ"),
            (PrimitiveType::Timestamptz, "TIMESTAMP"),
            (PrimitiveType::Long, "BIGINT"),
        ] {
            assert_eq!(
                spark_sql_type(&IcebergType::Primitive(primitive), 0).unwrap(),
                expected
            );
        }
    }
}
