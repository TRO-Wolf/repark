//! Namespace and table catalog DDL handlers.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::ObjectName;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::{NamespaceIdent, TableIdent};

use repark_core::CatalogRegistry;

use crate::catalog_ops::{
    catalog_handle, iceberg_err, name_parts, reregister, reregister_drop_namespace,
    resolve_namespace, sqlparser_err,
};

/// `DROP TABLE [IF EXISTS] catalog.namespace.table[, …]` → `catalog.drop_table`.
pub(crate) async fn execute_drop_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    names: &[ObjectName],
    if_exists: bool,
) -> Result<DataFrame> {
    for name in names {
        let parts = name_parts(name);
        let [catalog, namespace, table] = parts.as_slice() else {
            return Err(DataFusionError::Plan(format!(
                "DROP TABLE expects a three-part `catalog.namespace.table` name, got `{name}`"
            )));
        };
        let handle = catalog_handle(catalogs, catalog)?;
        let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
        if if_exists && !handle.table_exists(&ident).await.map_err(iceberg_err)? {
            continue;
        }
        handle.drop_table(&ident).await.map_err(iceberg_err)?;
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
        if if_exists && !handle.namespace_exists(&ident).await.map_err(iceberg_err)? {
            continue;
        }
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
                parse_namespace_property_string(parser)?,
            );
        } else if parser.parse_keyword(Keyword::LOCATION) {
            properties.insert(
                "location".to_string(),
                parse_namespace_property_string(parser)?,
            );
        } else if parser.parse_keyword(Keyword::WITH) {
            // Spark WITH DBPROPERTIES and Trino bare WITH carry the same key/value list.
            let _consumed_kind =
                consume_word(parser, "DBPROPERTIES") || consume_word(parser, "PROPERTIES");
            parse_namespace_property_list(parser, &mut properties)?;
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
pub(crate) fn parse_namespace_property_string(parser: &mut Parser) -> Result<String> {
    match parser.next_token().token {
        Token::Word(word) => Ok(word.value),
        Token::SingleQuotedString(value)
        | Token::DoubleQuotedString(value)
        | Token::Number(value, _) => Ok(value),
        other => Err(DataFusionError::Plan(format!(
            "CREATE NAMESPACE: expected a property name or value, got `{other}`"
        ))),
    }
}

/// Parse a `( 'key' = 'value', … )` list (an empty `()` is allowed) into `properties`.
pub(crate) fn parse_namespace_property_list(
    parser: &mut Parser,
    properties: &mut HashMap<String, String>,
) -> Result<()> {
    parser.expect_token(&Token::LParen).map_err(sqlparser_err)?;
    if parser.consume_token(&Token::RParen) {
        return Ok(());
    }
    loop {
        let key = parse_namespace_property_string(parser)?;
        parser.expect_token(&Token::Eq).map_err(sqlparser_err)?;
        let value = parse_namespace_property_string(parser)?;
        properties.insert(key, value);
        if !parser.consume_token(&Token::Comma) {
            break;
        }
    }
    parser.expect_token(&Token::RParen).map_err(sqlparser_err)?;
    Ok(())
}
