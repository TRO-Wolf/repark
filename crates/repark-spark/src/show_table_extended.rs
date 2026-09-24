use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
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
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        if !crate::show_create::starts_with_sql_keywords(sql, &["SHOW", "TABLE", "EXTENDED"]) {
            return None;
        }
        let near = unbalanced_delimiter(sql).map_or_else(
            || "end of input".to_string(),
            |delimiter| format!("'{delimiter}'"),
        );
        return Some(Err(syntax_error_near(&near, None)));
    };
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keywords(&[Keyword::SHOW, Keyword::TABLE, Keyword::EXTENDED]) {
        return None;
    }
    let scope = if parser.parse_keyword(Keyword::IN) || parser.parse_keyword(Keyword::FROM) {
        let Ok(name) = parser.parse_object_name(false) else {
            return Some(Err(syntax_error_at(&parser)));
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
        token => return Some(Err(syntax_error_at_token(&token))),
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
        return Some(Err(trailing_syntax_error_at(&parser)));
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
    if statement.has_partition {
        let ident = TableIdent::new(
            NamespaceIdent::new(namespace.clone()),
            statement.pattern.clone(),
        );
        match handle.load_table(&ident).await {
            Ok(_) => {
                let display = quoted_table_display(&[
                    catalog.clone(),
                    namespace.clone(),
                    statement.pattern.clone(),
                ]);
                return Err(partition_management_unsupported(&display));
            }
            Err(error) if error.kind() == ErrorKind::TableNotFound => {
                return Err(table_or_view_not_found(
                    &catalog,
                    &namespace,
                    &statement.pattern,
                ));
            }
            Err(error) => return Err(iceberg_err(error)),
        };
    }
    let mut tables = repark_iceberg::catalog::list_table_names(handle.as_ref(), &namespace).await?;
    tables.sort();
    let matching_tables: Vec<String> = tables
        .into_iter()
        .filter(|table| filter_pattern_matches(table, &statement.pattern))
        .collect();
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
    syntax_error_at_token(&parser.peek_token().token)
}

fn syntax_error_at_token(token: &Token) -> DataFusionError {
    let near = token_near(token);
    syntax_error_near(&near, None)
}

fn trailing_syntax_error_at(parser: &Parser) -> DataFusionError {
    let near = token_near(&parser.peek_token().token);
    let detail = format!("extra input {near}");
    syntax_error_near(&near, Some(&detail))
}

fn token_near(token: &Token) -> String {
    match token {
        Token::EOF => "end of input".to_string(),
        token => format!("'{token}'"),
    }
}

fn syntax_error_near(near: &str, detail: Option<&str>) -> DataFusionError {
    let detail = detail.map_or_else(String::new, |detail| format!(": {detail}"));
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(format!(
            "[PARSE_SYNTAX_ERROR] Syntax error at or near {near}{detail}. SQLSTATE: 42601"
        ))),
        None,
    )
}

fn unbalanced_delimiter(sql: &str) -> Option<char> {
    let mut active_delimiter = None;
    let bytes = sql.as_bytes();
    let mut position = 0;
    while let Some(byte) = bytes.get(position) {
        if let Some(delimiter) = active_delimiter {
            if *byte == delimiter as u8 {
                active_delimiter = None;
            }
            position += 1;
        } else if bytes
            .get(position..)
            .is_some_and(|tail| tail.starts_with(b"--") || tail.starts_with(b"/*"))
        {
            position = crate::show_create::skip_sql_whitespace_and_comments(sql, position)?;
        } else if matches!(*byte, b'\'' | b'"' | b'`') {
            active_delimiter = Some(char::from(*byte));
            position += 1;
        } else {
            position += 1;
        }
    }
    active_delimiter
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_parse_refusal(sql: &str, expected: &str) {
        let Some(Err(error)) = try_parse_show_table_extended(sql) else {
            panic!("expected a parse refusal for {sql:?}");
        };
        let DataFusionError::SQL(parser_error, _) = error else {
            panic!("expected a SQL parser error for {sql:?}");
        };
        let ParserError::ParserError(message) = parser_error.as_ref() else {
            panic!("expected a parser message for {sql:?}");
        };
        assert_eq!(message, expected, "{sql}");
    }

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
    fn parse_refuses_required_syntax_shapes() {
        for (sql, expected) in [
            (
                "SHOW TABLE EXTENDED",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED IN ice.sales",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED IN ice.sales LIKE",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED IN ice.sales LIKE pc",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near 'pc'. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near '''. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED IN ice.sales LIKE \"pc",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near '\"'. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED IN `ice.sales LIKE 'pc'",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near '`'. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' extra",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near 'extra': extra input 'extra'. \
                 SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED IN ice.sales LIKE 'pc' PARTITION (cat='a'",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
            ),
        ] {
            assert_parse_refusal(sql, expected);
        }
    }

    #[test]
    fn parse_near_misses_keep_exact_answers() {
        for (sql, expected) in [
            (
                "SHOW TABLE EXTENDED LIKE",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED LIKE 'x",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near '''. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED LIKE 'x' PARTITION (a=1",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED LIKE 'x')",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near ')': extra input ')'. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED LIKE 'x' trailing",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near 'trailing': extra input 'trailing'. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED LIKE 'x' PARTITION (a=1) trailing",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near 'trailing': extra input 'trailing'. SQLSTATE: 42601",
            ),
            (
                "SHOW TABLE EXTENDED IN",
                "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
            ),
        ] {
            assert_parse_refusal(sql, expected);
        }
        assert!(
            try_parse_show_table_extended("SHOW /* unclosed TABLE EXTENDED LIKE 'x'").is_none()
        );
        assert_eq!(
            try_parse_show_table_extended("SHOW TABLE EXTENDED LIKE 'x' -- end")
                .unwrap()
                .unwrap(),
            ShowTableExtended {
                scope: None,
                pattern: "x".to_string(),
                has_partition: false,
            }
        );
        assert_eq!(
            try_parse_show_table_extended("SHOW TABLE EXTENDED LIKE ''")
                .unwrap()
                .unwrap(),
            ShowTableExtended {
                scope: None,
                pattern: String::new(),
                has_partition: false
            }
        );
        assert_eq!(
            try_parse_show_table_extended("show table extended like 'x'")
                .unwrap()
                .unwrap(),
            ShowTableExtended {
                scope: None,
                pattern: "x".to_string(),
                has_partition: false
            }
        );
        assert_eq!(
            try_parse_show_table_extended("SHOW TABLE EXTENDED LIKE 'x';;")
                .unwrap()
                .unwrap(),
            ShowTableExtended {
                scope: None,
                pattern: "x".to_string(),
                has_partition: false,
            }
        );
        assert!(
            try_parse_show_table_extended("SHOW TABLE EXTENDED IN ice.sales LIKE 'x'")
                .unwrap()
                .is_ok()
        );
        assert!(try_parse_show_table_extended("SHOW TABLE EXTENDEDX LIKE 'x'").is_none());
    }

    #[test]
    fn parse_leaves_near_misses_alone() {
        for sql in [
            "SHOW TABLES IN `x`",
            "SHOW TABLE EXTENDEDX IN a LIKE 'b'",
            "SELECT 'SHOW TABLE EXTENDED `x'",
            "SHOW TBLPROPERTIES `x`",
            "SHOW CREATE TABLE `x`",
        ] {
            assert!(try_parse_show_table_extended(sql).is_none(), "{sql}");
        }
    }
}
