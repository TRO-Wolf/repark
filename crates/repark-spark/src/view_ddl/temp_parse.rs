use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{Ident, ObjectName};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Token;
use repark_common::spark_error;

use crate::catalog_ops::sqlparser_err;
use crate::namespace_ddl::consume_word;
use crate::normalize::multi_statement_parse_error;
use crate::view_ddl::parse::{
    consume_head_word, parse_view_clauses, split_view_statement, unquoted_head_words,
};

pub(crate) struct CreateTempViewStatement {
    pub(crate) or_replace: bool,
    pub(crate) name: Ident,
    pub(crate) aliases: Vec<(String, Option<String>)>,
    pub(crate) body_sql: String,
}

fn is_temp_create_view_head(sql: &str) -> bool {
    let words = unquoted_head_words(sql);
    let mut position = 0;
    if !consume_head_word(&words, &mut position, "CREATE") {
        return false;
    }
    if consume_head_word(&words, &mut position, "OR")
        && !consume_head_word(&words, &mut position, "REPLACE")
    {
        return false;
    }
    consume_head_word(&words, &mut position, "GLOBAL");
    if !consume_head_word(&words, &mut position, "TEMPORARY")
        && !consume_head_word(&words, &mut position, "TEMP")
    {
        return false;
    }
    consume_head_word(&words, &mut position, "VIEW")
}

pub(crate) fn try_parse_create_temp_view(sql: &str) -> Option<Result<CreateTempViewStatement>> {
    if !is_temp_create_view_head(sql) {
        return None;
    }
    Some(parse_create_temp_view_after_head(sql))
}

fn parse_create_temp_view_after_head(sql: &str) -> Result<CreateTempViewStatement> {
    let parts = split_view_statement(sql, "CREATE TEMPORARY VIEW")?;
    if parts.header.contains(&Token::SemiColon) {
        return Err(multi_statement_parse_error());
    }
    refuse_non_query_body(parts.body_head.as_ref())?;
    let mut parser = Parser::new(&DatabricksDialect {}).with_tokens(parts.header);
    parse_create_temp_view_header(&mut parser, parts.body_sql)
}

fn refuse_non_query_body(body_head: Option<&(Token, String)>) -> Result<()> {
    let Some((token, text)) = body_head else {
        return Ok(());
    };
    let starts_query = match token {
        Token::LParen => true,
        Token::Word(word) if word.quote_style.is_none() => {
            ["SELECT", "WITH", "VALUES", "FROM", "TABLE"]
                .iter()
                .any(|keyword| word.value.eq_ignore_ascii_case(keyword))
        }
        _ => false,
    };
    if starts_query {
        return Ok(());
    }
    let near = format!("'{text}'");
    Err(spark_parse_error(spark_error::message(
        spark_error::PARSE_SYNTAX_ERROR,
        &[("near", near.as_str())],
    )))
}

fn parse_create_temp_view_header(
    parser: &mut Parser,
    body_sql: String,
) -> Result<CreateTempViewStatement> {
    if !parser.parse_keyword(Keyword::CREATE) {
        return Err(DataFusionError::Plan(
            "could not parse `CREATE TEMPORARY VIEW`: expected CREATE".to_string(),
        ));
    }
    let or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    let global = consume_word(parser, "GLOBAL");
    if !consume_word(parser, "TEMPORARY") && !consume_word(parser, "TEMP") {
        return Err(DataFusionError::Plan(
            "could not parse `CREATE TEMPORARY VIEW`: expected TEMPORARY".to_string(),
        ));
    }
    if !consume_word(parser, "VIEW") {
        return Err(DataFusionError::Plan(
            "could not parse `CREATE TEMPORARY VIEW`: expected VIEW".to_string(),
        ));
    }
    if global {
        return Err(DataFusionError::NotImplemented(
            GLOBAL_TEMP_VIEW_REFUSAL.to_string(),
        ));
    }
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let object_name = parser.parse_object_name(false).map_err(sqlparser_err)?;
    let clauses = parse_view_clauses(parser, "CREATE TEMPORARY VIEW")?;
    if or_replace && if_not_exists {
        return Err(spark_parse_error(
            "CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed.".to_string(),
        ));
    }
    if clauses.properties.is_some() {
        return Err(spark_parse_error(
            "Operation not allowed: TBLPROPERTIES can't coexist with CREATE TEMPORARY VIEW."
                .to_string(),
        ));
    }
    if if_not_exists {
        return Err(spark_parse_error(
            "It is not allowed to define a TEMPORARY view with IF NOT EXISTS.".to_string(),
        ));
    }
    let name = single_part_temp_view_name(&object_name)?;
    Ok(CreateTempViewStatement {
        or_replace,
        name,
        aliases: clauses.aliases,
        body_sql,
    })
}

pub(crate) const GLOBAL_TEMP_VIEW_REFUSAL: &str = "CREATE GLOBAL TEMPORARY VIEW is not supported: \
     RePark has no cross-session `global_temp` schema, so a global temporary view is neither a \
     durable catalog view nor a session-local temporary view. Use CREATE TEMPORARY VIEW for a \
     session-local view or CREATE VIEW <catalog>.<namespace>.<view> for a durable one";

fn single_part_temp_view_name(object_name: &ObjectName) -> Result<Ident> {
    let idents = object_name
        .0
        .iter()
        .filter_map(|part| part.as_ident().cloned())
        .collect::<Vec<_>>();
    if idents.len() != object_name.0.len() {
        return Err(DataFusionError::Plan(format!(
            "could not parse `CREATE TEMPORARY VIEW`: expected a view name, got `{object_name}`"
        )));
    }
    let rendered = idents
        .iter()
        .map(|ident| format!("`{}`", ident.value.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".");
    match idents.as_slice() {
        [name] => Ok(name.clone()),
        [_, _] => Err(spark_parse_error(spark_error::message(
            spark_error::TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS,
            &[("actualName", rendered.as_str())],
        ))),
        [] => Err(DataFusionError::Plan(
            "could not parse `CREATE TEMPORARY VIEW`: expected a view name".to_string(),
        )),
        _ => Err(spark_parse_error(spark_error::message(
            spark_error::IDENTIFIER_TOO_MANY_NAME_PARTS,
            &[("identifier", rendered.as_str()), ("limit", "2")],
        ))),
    }
}

pub(crate) fn spark_parse_error(message: String) -> DataFusionError {
    DataFusionError::SQL(Box::new(ParserError::ParserError(message)), None)
}
