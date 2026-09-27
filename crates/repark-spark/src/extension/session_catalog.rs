use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use repark_core::{CatalogRegistry, ReparkSession};
use repark_functions::session_names::SessionDefaults;

const SESSION_CATALOG_NAME: &str = "spark_catalog";

const SESSION_NAME_FUNCTIONS: [&str; 3] = ["current_catalog", "current_database", "current_schema"];

const CURRENT_CATALOG_LISTINGS: [&str; 6] = [
    "NAMESPACES",
    "DATABASES",
    "SCHEMAS",
    "TABLES",
    "VIEWS",
    "FUNCTIONS",
];

pub fn apply_default_catalog(session: &ReparkSession, name: Option<&str>) {
    if let Some((catalog, namespace)) = session.catalogs_snapshot().apply_default_catalog(name) {
        crate::use_ddl::write_session_defaults_carrier(session.context(), &catalog, &namespace);
    }
}

pub(crate) fn with_configured_defaults(
    config: SessionConfig,
    conf: &HashMap<String, String>,
) -> SessionConfig {
    let catalog = CatalogRegistry::configured_default_catalog(conf)
        .unwrap_or_else(|| SESSION_CATALOG_NAME.to_string());
    let namespace = CatalogRegistry::default_namespace_for(&catalog).to_string();
    config.with_option_extension(SessionDefaults { catalog, namespace })
}

#[must_use]
pub fn to_datafusion_error(error: repark_core::Error) -> DataFusionError {
    match error {
        repark_core::Error::NotImplemented(message) => {
            repark_iceberg::write::unsupported_error(message)
        }
        repark_core::Error::IllegalArgument(message) => {
            repark_core::illegal_argument_error(message)
        }
        repark_core::Error::Analysis(message) => DataFusionError::Plan(message),
        other => DataFusionError::Execution(other.to_string()),
    }
}

fn is_name_word(word: &Word) -> bool {
    matches!(word.quote_style, None | Some('`'))
}

fn keyword_is(token: Option<&Token>, keywords: &[Keyword]) -> bool {
    matches!(token, Some(Token::Word(word)) if word.quote_style.is_none() && keywords.contains(&word.keyword))
}

fn names_catalog_operand(tokens: &[Token], index: usize, show: bool) -> bool {
    let previous = index.checked_sub(1).and_then(|at| tokens.get(at));
    let leads_name = matches!(tokens.get(index + 1), Some(Token::Period))
        && !matches!(previous, Some(Token::Period));
    let operand = keyword_is(previous, &[Keyword::USE])
        || (show && keyword_is(previous, &[Keyword::IN, Keyword::FROM]));
    leads_name || operand
}

fn calls_session_name(tokens: &[Token], index: usize, word: &Word) -> bool {
    word.quote_style.is_none()
        && matches!(tokens.get(index + 1), Some(Token::LParen))
        && SESSION_NAME_FUNCTIONS
            .iter()
            .any(|name| word.value.eq_ignore_ascii_case(name))
}

fn names_refused_operand(tokens: &[Token], index: usize, show: bool) -> bool {
    let previous = index.checked_sub(1).and_then(|at| tokens.get(at));
    if keyword_is(previous, &[Keyword::USE])
        || (show && keyword_is(previous, &[Keyword::IN, Keyword::FROM]))
    {
        return true;
    }
    matches!(tokens.get(index + 1), Some(Token::Period))
        && matches!(tokens.get(index + 2), Some(Token::Word(_)))
        && !matches!(previous, Some(Token::Period))
}

fn refuse_refused_catalog(catalogs: &CatalogRegistry, sql: &str) -> Result<()> {
    if !catalogs.has_refusals() {
        return Ok(());
    }
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return Ok(());
    };
    let tokens: Vec<Token> = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();
    let show = keyword_is(tokens.first(), &[Keyword::SHOW]);
    for (index, token) in tokens.iter().enumerate() {
        let Token::Word(word) = token else {
            continue;
        };
        if !is_name_word(word) {
            continue;
        }
        let Some(refusal) = catalogs.refusal(&word.value) else {
            continue;
        };
        if names_refused_operand(&tokens, index, show) {
            return Err(to_datafusion_error(refusal.error()));
        }
    }
    Ok(())
}

fn head_needs_current_catalog(tokens: &[Token]) -> bool {
    let word_at = |at: usize| match tokens.get(at) {
        Some(Token::Word(word)) if word.quote_style.is_none() => {
            Some(word.value.to_ascii_uppercase())
        }
        _ => None,
    };
    match word_at(0).as_deref() {
        Some("SHOW") => {
            let listing =
                word_at(1).is_some_and(|word| CURRENT_CATALOG_LISTINGS.contains(&word.as_str()));
            listing
                && !tokens
                    .iter()
                    .any(|token| keyword_is(Some(token), &[Keyword::IN, Keyword::FROM]))
        }
        Some("USE") => {
            let rest: Vec<&Token> = tokens
                .iter()
                .skip(1)
                .filter(|token| !matches!(token, Token::SemiColon | Token::EOF))
                .collect();
            matches!(rest.as_slice(), [Token::Word(_)])
        }
        _ => false,
    }
}

pub(crate) fn guard_statement(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<()> {
    crate::view_ddl::read::ensure_view_wrappers(ctx, catalogs)?;
    refuse_refused_catalog(catalogs, sql)?;
    let current_missing = catalogs.current_catalog_error().is_some();
    if !current_missing {
        return Ok(());
    }
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return Ok(());
    };
    let tokens: Vec<Token> = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();
    let (current, _) = catalogs.current_defaults();
    let show = keyword_is(tokens.first(), &[Keyword::SHOW]);
    let current_error = || {
        catalogs
            .current_catalog_error()
            .map_or(Ok(()), |error| Err(to_datafusion_error(error)))
    };
    for (index, token) in tokens.iter().enumerate() {
        let Token::Word(word) = token else {
            continue;
        };
        if !is_name_word(word) {
            continue;
        }
        let catalog_operand = names_catalog_operand(&tokens, index, show);
        if current_missing
            && ((catalog_operand && word.value == current)
                || calls_session_name(&tokens, index, word))
        {
            return current_error();
        }
    }
    if current_missing && head_needs_current_catalog(&tokens) {
        return current_error();
    }
    Ok(())
}
