//! Namespace and table catalog DDL handlers.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::ObjectName;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::{ErrorKind, NamespaceIdent, TableIdent};

use repark_core::CatalogRegistry;

use crate::catalog_ops::{
    catalog_handle, iceberg_err, name_parts, reregister, reregister_drop_namespace,
    resolve_namespace, sqlparser_err, table_or_view_not_found,
};

pub(crate) mod purge;

/// `DROP TABLE [IF EXISTS] catalog.namespace.table[, …]` → `catalog.drop_table`.
pub(crate) async fn execute_drop_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    names: &[ObjectName],
    if_exists: bool,
    purge: bool,
) -> Result<DataFrame> {
    for name in names {
        let parts = crate::use_ddl::complete_name(catalogs, &name_parts(name))?;
        let [catalog, namespace, table] = parts.as_slice() else {
            return Err(DataFusionError::Plan(format!(
                "DROP TABLE expects a three-part `catalog.namespace.table` name, got `{name}`"
            )));
        };
        let handle = catalog_handle(catalogs, catalog)?;
        let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
        if catalogs.is_view(catalog, &ident).await? {
            return Err(table_or_view_not_found(catalog, namespace, table));
        }
        if if_exists && !handle.table_exists(&ident).await.map_err(iceberg_err)? {
            continue;
        }
        if purge {
            let swept =
                purge::purge_table_files(handle.as_ref(), &ident, [catalog, namespace, table])
                    .await?;
            if !swept.delete_failures.is_empty() {
                tracing::warn!(
                    "DROP TABLE … PURGE on `{catalog}.{namespace}.{table}`: {} per-file deletes \
                     failed (log-only, suppressed as Java does)",
                    swept.delete_failures.len()
                );
            }
        }
        handle.drop_table(&ident).await.map_err(|error| {
            if !if_exists && error.kind() == ErrorKind::TableNotFound {
                return table_or_view_not_found(catalog, namespace, table);
            }
            iceberg_err(error)
        })?;
        reregister(ctx, handle.clone(), catalog, namespace).await?;
    }
    ctx.read_empty()
}

/// `DROP NAMESPACE|DATABASE [IF EXISTS] catalog.namespace` → `catalog.drop_namespace`.
pub(crate) async fn execute_drop_namespace(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    names: &[ObjectName],
    if_exists: bool,
) -> Result<DataFrame> {
    for name in names {
        let (catalog, namespace) = resolve_namespace(name)?;
        let handle = catalog_handle(catalogs, &catalog)?;
        let ident = NamespaceIdent::new(namespace.clone());
        if !handle.namespace_exists(&ident).await.map_err(iceberg_err)? {
            if if_exists {
                continue;
            }
            return Err(repark_iceberg::catalog::schema_not_found_on_drop(
                &catalog, &namespace,
            ));
        }
        repark_iceberg::catalog::refuse_non_empty_namespace_drop(
            handle.as_ref(),
            &ident,
            &namespace,
        )
        .await?;
        handle.drop_namespace(&ident).await.map_err(iceberg_err)?;
        reregister_drop_namespace(ctx, handle.clone(), &catalog, &namespace).await?;
    }
    ctx.read_empty()
}

/// A parsed Spark `CREATE {NAMESPACE|SCHEMA|DATABASE}`.
pub(crate) struct CreateNamespace {
    catalog: String,
    namespace: String,
    if_not_exists: bool,
    /// The namespace properties to create with.
    properties: HashMap<String, String>,
}

pub(crate) struct AlterNamespace {
    catalog: String,
    namespace: String,
    properties: HashMap<String, String>,
}

/// CREATE NAMESPACE with COMMENT, LOCATION, and WITH properties maps to `create_namespace`.
pub(crate) async fn execute_create_namespace(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    mut create: CreateNamespace,
) -> Result<DataFrame> {
    let handle = catalog_handle(catalogs, &create.catalog)?;
    let namespace = create.namespace.clone();
    let ident = NamespaceIdent::new(create.namespace);
    repark_iceberg::catalog::mirror_namespace_location_keys(&mut create.properties);
    if create.if_not_exists && handle.namespace_exists(&ident).await.map_err(iceberg_err)? {
        // G-6 Q1: IF NOT EXISTS must not silently adopt a contradictory LOCATION.
        let existing = handle.get_namespace(&ident).await.map_err(iceberg_err)?;
        repark_core::refuse_contradictory_namespace_location(
            &namespace,
            existing.properties(),
            &create.properties,
        )
        .map_err(DataFusionError::Plan)?;
        return ctx.read_empty();
    }
    handle
        .create_namespace(&ident, create.properties)
        .await
        .map_err(iceberg_err)?;
    reregister(ctx, handle.clone(), &create.catalog, &namespace).await?;
    ctx.read_empty()
}

pub(crate) async fn execute_alter_namespace(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    alter: AlterNamespace,
) -> Result<DataFrame> {
    let handle = catalog_handle(catalogs, &alter.catalog)?;
    let ident = NamespaceIdent::new(alter.namespace.clone());
    let missing = |error: iceberg::Error| {
        if error.kind() == ErrorKind::NamespaceNotFound {
            return repark_iceberg::catalog::schema_not_found(&format!("`{}`", alter.namespace));
        }
        iceberg_err(error)
    };
    let mut properties = handle
        .get_namespace(&ident)
        .await
        .map_err(missing)?
        .properties()
        .clone();
    properties.extend(alter.properties);
    handle
        .update_namespace(&ident, properties)
        .await
        .map_err(missing)?;
    reregister(ctx, handle.clone(), &alter.catalog, &alter.namespace).await?;
    ctx.read_empty()
}

pub(crate) fn try_parse_alter_namespace(sql: &str) -> Option<Result<AlterNamespace>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::ALTER) {
        return None;
    }
    let is_namespace = parser.parse_keyword(Keyword::SCHEMA)
        || parser.parse_keyword(Keyword::DATABASE)
        || consume_word(&mut parser, "NAMESPACE");
    if !is_namespace {
        return None;
    }
    let name = match parser.parse_object_name(false) {
        Ok(name) => name,
        Err(error) => return Some(Err(sqlparser_err(error))),
    };
    if !parser.parse_keyword(Keyword::SET)
        || !(consume_word(&mut parser, "DBPROPERTIES") || consume_word(&mut parser, "PROPERTIES"))
    {
        return None;
    }
    Some(parse_alter_namespace_body(&mut parser, &name))
}

fn parse_alter_namespace_body(parser: &mut Parser, name: &ObjectName) -> Result<AlterNamespace> {
    let (catalog, namespace) = resolve_namespace(name)?;
    let pairs = parse_alter_property_list(parser).map_err(alter_parse_error)?;
    refuse_alter_namespace_properties(&pairs).map_err(alter_parse_error)?;
    let properties = pairs
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect();
    Ok(AlterNamespace {
        catalog,
        namespace,
        properties,
    })
}

fn alter_parse_error(message: String) -> DataFusionError {
    DataFusionError::SQL(Box::new(ParserError::ParserError(message)), None)
}

fn alter_syntax_error(token: &Token) -> String {
    match token {
        Token::EOF => {
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601".to_string()
        }
        other => format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{other}'. SQLSTATE: 42601"),
    }
}

fn parse_alter_property_key(parser: &mut Parser) -> std::result::Result<String, String> {
    let token = parser.next_token().token;
    match token {
        Token::SingleQuotedString(key) | Token::DoubleQuotedString(key) => Ok(key),
        Token::Word(word) => {
            let mut key = word.value;
            while parser.consume_token(&Token::Period) {
                match parser.next_token().token {
                    Token::Word(part) => {
                        key.push('.');
                        key.push_str(&part.value);
                    }
                    other => return Err(alter_syntax_error(&other)),
                }
            }
            Ok(key)
        }
        other => Err(alter_syntax_error(&other)),
    }
}

fn parse_alter_property_value(parser: &mut Parser) -> std::result::Result<Option<String>, String> {
    let has_eq = parser.consume_token(&Token::Eq);
    let token = parser.peek_token().token;
    let value = match token {
        Token::Comma | Token::RParen if !has_eq => return Ok(None),
        Token::SingleQuotedString(value)
        | Token::DoubleQuotedString(value)
        | Token::Number(value, _) => value,
        Token::Word(word)
            if word.quote_style.is_none()
                && (word.value.eq_ignore_ascii_case("true")
                    || word.value.eq_ignore_ascii_case("false")) =>
        {
            word.value.to_ascii_lowercase()
        }
        other => return Err(alter_syntax_error(&other)),
    };
    parser.next_token();
    Ok(Some(value))
}

fn parse_alter_property_list(
    parser: &mut Parser,
) -> std::result::Result<Vec<(String, Option<String>)>, String> {
    if !parser.consume_token(&Token::LParen) {
        return Err(alter_syntax_error(&parser.peek_token().token));
    }
    let mut pairs = Vec::new();
    loop {
        let key = parse_alter_property_key(parser)?;
        let value = parse_alter_property_value(parser)?;
        pairs.push((key, value));
        if parser.consume_token(&Token::RParen) {
            break;
        }
        if !parser.consume_token(&Token::Comma) {
            return Err(alter_syntax_error(&parser.peek_token().token));
        }
    }
    let _ = parser.consume_token(&Token::SemiColon);
    match parser.peek_token().token {
        Token::EOF => Ok(pairs),
        trailing => Err(format!(
            "[PARSE_SYNTAX_ERROR] Syntax error at or near '{trailing}': extra input \
             '{trailing}'. SQLSTATE: 42601"
        )),
    }
}

fn refuse_alter_namespace_properties(
    pairs: &[(String, Option<String>)],
) -> std::result::Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    if let Some((key, _)) = pairs.iter().find(|(key, _)| !seen.insert(key.as_str())) {
        let rendered = key
            .split('.')
            .map(|part| format!("`{part}`"))
            .collect::<Vec<_>>()
            .join(".");
        return Err(format!(
            "[DUPLICATE_KEY] Found duplicate keys {rendered}. SQLSTATE: 23505"
        ));
    }
    let missing = pairs
        .iter()
        .filter(|(_, value)| value.is_none())
        .map(|(key, _)| key.as_str())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(format!(
            "Operation not allowed: Values must be specified for key(s): [{}].",
            missing.join(",")
        ));
    }
    let reserved = pairs.iter().find_map(|(key, _)| match key.as_str() {
        "location" => Some("location is a reserved namespace property, please use the LOCATION clause to specify it"),
        "owner" => Some("owner is a reserved namespace property, it will be set to the current user"),
        _ => None,
    });
    match reserved {
        Some(reason) => Err(format!(
            "[UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY] The feature is not supported: {reason}. \
             SQLSTATE: 0A000"
        )),
        None => Ok(()),
    }
}

/// Parse Spark CREATE NAMESPACE|SCHEMA|DATABASE with COMMENT, LOCATION, and WITH properties.
pub(crate) fn try_parse_create_namespace(sql: &str) -> Option<Result<CreateNamespace>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::CREATE) {
        return None;
    }
    let is_namespace = parser.parse_keyword(Keyword::SCHEMA)
        || parser.parse_keyword(Keyword::DATABASE)
        || consume_word(&mut parser, "NAMESPACE");
    if !is_namespace {
        return None;
    }
    Some(parse_create_namespace_body(&mut parser))
}

/// Parse the body after `CREATE {NAMESPACE|SCHEMA|DATABASE}`.
pub(crate) fn parse_create_namespace_body(parser: &mut Parser) -> Result<CreateNamespace> {
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = parser.parse_object_name(false).map_err(sqlparser_err)?;
    let (catalog, namespace) = resolve_namespace(&name)?;

    let mut properties = HashMap::new();
    loop {
        if parser.parse_keyword(Keyword::COMMENT) {
            properties.insert(
                "comment".to_string(),
                parse_namespace_property_string(parser, "CREATE NAMESPACE")?,
            );
        } else if parser.parse_keyword(Keyword::LOCATION) {
            properties.insert(
                "location".to_string(),
                parse_namespace_property_string(parser, "CREATE NAMESPACE")?,
            );
        } else if parser.parse_keyword(Keyword::WITH) {
            // Spark WITH DBPROPERTIES and Trino bare WITH carry the same key/value list.
            let _consumed_kind =
                consume_word(parser, "DBPROPERTIES") || consume_word(parser, "PROPERTIES");
            parse_namespace_property_list(parser, &mut properties, "CREATE NAMESPACE")?;
        } else {
            break;
        }
    }

    let trailing = parser.peek_token().token;
    if !matches!(trailing, Token::EOF | Token::SemiColon) {
        return Err(DataFusionError::Plan(format!(
            "unsupported CREATE NAMESPACE clause near `{trailing}` (supported: [IF NOT EXISTS] \
             catalog.namespace [COMMENT '…'] [LOCATION '…'] \
             [WITH [DBPROPERTIES|PROPERTIES] ('key' = 'value', …)])"
        )));
    }
    Ok(CreateNamespace {
        catalog,
        namespace,
        if_not_exists,
        properties,
    })
}

/// Consume the next token iff it is a `Word` whose value equals `word`.
pub(crate) fn consume_word(parser: &mut Parser, word: &str) -> bool {
    if let Token::Word(peeked) = &parser.peek_token().token
        && peeked.value.eq_ignore_ascii_case(word)
    {
        parser.next_token();
        return true;
    }
    false
}

/// Read one property key or value token.
pub(crate) fn parse_namespace_property_string(
    parser: &mut Parser,
    statement: &str,
) -> Result<String> {
    match parser.next_token().token {
        Token::Word(word) => Ok(word.value),
        Token::SingleQuotedString(value)
        | Token::DoubleQuotedString(value)
        | Token::Number(value, _) => Ok(value),
        other => Err(DataFusionError::Plan(format!(
            "{statement}: expected a property name or value, got `{other}`"
        ))),
    }
}

/// Parse a `( 'key' = 'value', … )` list (an empty `()` is allowed) into `properties`.
pub(crate) fn parse_namespace_property_list(
    parser: &mut Parser,
    properties: &mut HashMap<String, String>,
    statement: &str,
) -> Result<()> {
    parser.expect_token(&Token::LParen).map_err(sqlparser_err)?;
    if parser.consume_token(&Token::RParen) {
        return Ok(());
    }
    loop {
        let key = parse_namespace_property_string(parser, statement)?;
        parser.expect_token(&Token::Eq).map_err(sqlparser_err)?;
        let value = parse_namespace_property_string(parser, statement)?;
        properties.insert(key, value);
        if !parser.consume_token(&Token::Comma) {
            break;
        }
    }
    parser.expect_token(&Token::RParen).map_err(sqlparser_err)?;
    Ok(())
}
