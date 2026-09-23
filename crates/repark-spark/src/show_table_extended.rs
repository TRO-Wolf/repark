use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::spec::TableMetadata;
use iceberg::{ErrorKind, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::catalog_ops::{
    iceberg_err, name_parts, partition_management_unsupported, quoted_table_display,
    table_or_view_not_found,
};
use crate::describe_show::filter_pattern_matches;
use crate::spark_tree_string::spark_tree_string;
use crate::table_props_view::spark_table_properties;
use crate::write_options::StatementWriteOptions;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ShowTableExtended {
    pub(crate) scope: Option<Vec<String>>,
    pub(crate) pattern: String,
    pub(crate) has_partition: bool,
}

pub(crate) fn try_parse_show_table_extended(sql: &str) -> Option<Result<ShowTableExtended>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keywords(&[Keyword::SHOW, Keyword::TABLE, Keyword::EXTENDED]) {
        return None;
    }
    let scope = if parser.parse_keyword(Keyword::IN) || parser.parse_keyword(Keyword::FROM) {
        let name = match parser.parse_object_name(false) {
            Ok(name) => name,
            Err(_) => return Some(Err(syntax_error_at(&parser))),
        };
        Some(name_parts(&name))
    } else {
        None
    };
    if !parser.parse_keyword(Keyword::LIKE) {
        return Some(Err(syntax_error_at(&parser)));
    }
    let pattern = match parser.next_token().token {
        Token::SingleQuotedString(pattern) | Token::DoubleQuotedString(pattern) => pattern,
        _ => return Some(Err(syntax_error_at(&parser))),
    };
    let has_partition = if parser.parse_keyword(Keyword::PARTITION) {
        if !consume_parenthesized(&mut parser) {
            return Some(Err(syntax_error_at(&parser)));
        }
        true
    } else {
        false
    };
    if !at_statement_end(&parser) {
        return Some(Err(syntax_error_at(&parser)));
    }
    Some(Ok(ShowTableExtended {
        scope,
        pattern,
        has_partition,
    }))
}

pub(crate) async fn try_show_table_extended_intercept(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    write_options: &StatementWriteOptions,
) -> Option<Result<DataFrame>> {
    let statement = try_parse_show_table_extended(sql)?;
    let statement = match statement.and_then(|statement| {
        write_options
            .refuse_if_non_empty("SHOW TABLE EXTENDED")
            .map(|()| statement)
    }) {
        Ok(statement) => statement,
        Err(error) => return Some(Err(error)),
    };
    Some(execute_show_table_extended(ctx, catalogs, statement).await)
}

async fn execute_show_table_extended(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: ShowTableExtended,
) -> Result<DataFrame> {
    let ((catalog, namespace), ambient) =
        crate::use_ddl::resolve_show_tables_scope(catalogs, statement.scope).await?;
    let empty = show_table_extended_batch(Vec::new())?;
    let Some(handle) = catalogs.get(&catalog) else {
        return ctx.read_batch(empty);
    };
    if ambient
        && (namespace.is_empty()
            || !handle
                .namespace_exists(&NamespaceIdent::new(namespace.clone()))
                .await
                .map_err(iceberg_err)?)
    {
        return ctx.read_batch(empty);
    }
    let mut tables = repark_iceberg::catalog::list_table_names(handle.as_ref(), &namespace).await?;
    tables.sort();
    let matching_tables: Vec<String> = tables
        .into_iter()
        .filter(|table| filter_pattern_matches(table, &statement.pattern))
        .collect();
    if statement.has_partition {
        let Some(table) = matching_tables.first() else {
            return Err(table_or_view_not_found(
                &catalog,
                &namespace,
                &statement.pattern,
            ));
        };
        let display = quoted_table_display(&[catalog.clone(), namespace.clone(), table.clone()]);
        return Err(partition_management_unsupported(&display));
    }
    let mut rows = Vec::with_capacity(matching_tables.len());
    for table_name in matching_tables {
        let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table_name.clone());
        let table = match handle.load_table(&ident).await {
            Ok(table) => table,
            Err(error) if error.kind() == ErrorKind::TableNotFound => {
                return Err(table_or_view_not_found(&catalog, &namespace, &table_name));
            }
            Err(error) => return Err(iceberg_err(error)),
        };
        rows.push((
            namespace.clone(),
            table_name,
            false,
            show_table_information(&catalog, &namespace, ident.name(), table.metadata())?,
        ));
    }
    ctx.read_batch(show_table_extended_batch(rows)?)
}

fn show_table_extended_batch(rows: Vec<(String, String, bool, String)>) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("namespace", DataType::Utf8, false),
        Field::new("tableName", DataType::Utf8, false),
        Field::new("isTemporary", DataType::Boolean, false),
        Field::new("information", DataType::Utf8, false),
    ]));
    let mut namespaces = Vec::with_capacity(rows.len());
    let mut tables = Vec::with_capacity(rows.len());
    let mut temporary = Vec::with_capacity(rows.len());
    let mut information = Vec::with_capacity(rows.len());
    for (namespace, table, is_temporary, table_information) in rows {
        namespaces.push(namespace);
        tables.push(table);
        temporary.push(is_temporary);
        information.push(table_information);
    }
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(namespaces)),
            Arc::new(StringArray::from(tables)),
            Arc::new(BooleanArray::from(temporary)),
            Arc::new(StringArray::from(information)),
        ],
    )?)
}

fn show_table_information(
    catalog: &str,
    namespace: &str,
    table_name: &str,
    table: &TableMetadata,
) -> Result<String> {
    let mut lines = vec![
        format!("Catalog: {catalog}"),
        format!("Namespace: {namespace}"),
        format!("Table: {table_name}"),
    ];
    lines.push(if table.properties().contains_key("external") {
        "Type: EXTERNAL".to_string()
    } else {
        "Type: MANAGED".to_string()
    });
    if let Some(comment) = table.properties().get("comment") {
        lines.push(format!("Comment: {comment}"));
    }
    lines.push(format!("Location: {}", table.location()));
    lines.push("Provider: iceberg".to_string());
    if let Some(owner) = table.properties().get("owner") {
        lines.push(format!("Owner: {owner}"));
    }
    lines.push(format!(
        "Table Properties: {}",
        spark_character_properties(table)
    ));
    let arrow_schema =
        iceberg::arrow::schema_to_arrow_schema(table.current_schema()).map_err(iceberg_err)?;
    lines.push(format!(
        "Schema: {}",
        spark_tree_string(arrow_schema.as_ref())
    ));
    Ok(format!("{}\n", lines.join("\n")))
}

fn spark_character_properties(table: &TableMetadata) -> String {
    let properties = spark_table_properties(table)
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",");
    let bracketed = format!("[{properties}]");
    format!(
        "[{}]",
        bracketed
            .chars()
            .map(|character| character.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn consume_parenthesized(parser: &mut Parser) -> bool {
    if !parser.consume_token(&Token::LParen) {
        return false;
    }
    let mut depth = 1_usize;
    while depth > 0 {
        match parser.next_token().token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::EOF => return false,
            _ => {}
        }
    }
    true
}

fn at_statement_end(parser: &Parser) -> bool {
    matches!(parser.peek_token().token, Token::EOF | Token::SemiColon)
}

fn syntax_error_at(parser: &Parser) -> DataFusionError {
    let near = match parser.peek_token().token {
        Token::EOF => "end of input".to_string(),
        token => format!("'{token}'"),
    };
    DataFusionError::Plan(format!(
        "[PARSE_SYNTAX_ERROR] Syntax error at or near {near}. SQLSTATE: 42601"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_scope_like_partition_and_semicolon() {
        let parsed = try_parse_show_table_extended(
            "show table extended from `ice`.`sales` like 'p*' partition (cat = 'a');",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            parsed,
            ShowTableExtended {
                scope: Some(vec!["ice".to_string(), "sales".to_string()]),
                pattern: "p*".to_string(),
                has_partition: true,
            }
        );
    }

    #[test]
    fn parse_accepts_ambient_scope() {
        let parsed = try_parse_show_table_extended("SHOW TABLE EXTENDED LIKE 'pl'")
            .unwrap()
            .unwrap();
        assert_eq!(parsed.scope, None);
        assert_eq!(parsed.pattern, "pl");
        assert!(!parsed.has_partition);
    }

    #[test]
    fn parse_refuses_missing_like_at_the_end() {
        let error = try_parse_show_table_extended("SHOW TABLE EXTENDED IN ice.sales")
            .unwrap()
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. \
             SQLSTATE: 42601"
        );
    }

    #[test]
    fn parse_leaves_near_misses_alone() {
        for sql in [
            "SHOW TABLES IN sales",
            "SHOW TABLES EXTENDED IN sales LIKE '*'",
            "SHOW TBLPROPERTIES sales.pl",
            "SHOW CREATE TABLE sales.pl",
            "SHOW TABLE EXTENSION LIKE 'pl'",
        ] {
            assert!(try_parse_show_table_extended(sql).is_none(), "{sql}");
        }
    }
}
