use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use iceberg::table::Table;
use repark_core::CatalogRegistry;
use repark_core::time_travel::incremental::CHANGES_RELATION;
use repark_core::time_travel::next_temp_view_name;
use repark_iceberg::catalog::{ChangelogTableProvider, ChangelogWindow};

use crate::time_travel::PinnedViews;

struct ChangesSpan {
    start: usize,
    end: usize,
    table_parts: Vec<String>,
}

#[must_use]
pub fn sql_may_have_changes_relation(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains(CHANGES_RELATION)
}

#[allow(clippy::missing_errors_doc)]
pub async fn prepare_changes_sql(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    pinned: &mut PinnedViews,
) -> Result<Option<String>> {
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return Ok(None);
    };
    let spans = find_changes_spans(&tokens);
    if spans.is_empty() {
        return Ok(None);
    }
    let mut tokens = tokens;
    let mut rewritten = false;
    for span in spans.into_iter().rev() {
        let Some(table) = load_parent_table(catalogs, &span.table_parts).await else {
            continue;
        };
        let provider = ChangelogTableProvider::try_new(table, ChangelogWindow::default())?;
        let temp_name = next_temp_view_name();
        let _ = ctx.deregister_table(temp_name.as_str());
        pinned.record(temp_name.clone());
        ctx.register_table(temp_name.as_str(), Arc::new(provider))
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "failed to register changelog temp view {temp_name}: {error}"
                ))
            })?;
        let replacement = Token::Word(Word {
            value: temp_name,
            quote_style: None,
            keyword: Keyword::NoKeyword,
        });
        tokens.splice(span.start..span.end, std::iter::once(replacement));
        rewritten = true;
    }
    if !rewritten {
        return Ok(None);
    }
    Ok(Some(tokens.iter().map(ToString::to_string).collect()))
}

async fn load_parent_table(catalogs: &CatalogRegistry, parts: &[String]) -> Option<Table> {
    let [catalog_name, namespace, table] = parts else {
        return None;
    };
    let ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(namespace.clone()),
        table.clone(),
    );
    catalogs.get(catalog_name)?.load_table(&ident).await.ok()
}

fn find_changes_spans(tokens: &[Token]) -> Vec<ChangesSpan> {
    let significant: Vec<(usize, &Token)> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();
    let is_ident = |sig_index: usize| -> bool {
        matches!(
            significant.get(sig_index).map(|(_, token)| *token),
            Some(Token::Word(_) | Token::DoubleQuotedString(_))
        )
    };
    let is_period = |sig_index: usize| -> bool {
        matches!(
            significant.get(sig_index).map(|(_, token)| *token),
            Some(Token::Period)
        )
    };
    let mut spans = Vec::new();
    let mut sig_index = 0usize;
    while sig_index < significant.len() {
        if !is_ident(sig_index) {
            sig_index += 1;
            continue;
        }
        let start_sig = sig_index;
        let mut end_sig = sig_index + 1;
        while end_sig + 1 < significant.len() && is_period(end_sig) && is_ident(end_sig + 1) {
            end_sig += 2;
        }
        let parts = collect_parts(&significant[start_sig..end_sig]);
        if parts.len() == 4
            && parts[3].eq_ignore_ascii_case(CHANGES_RELATION)
            && follows_from_or_join(&significant, start_sig)
        {
            spans.push(ChangesSpan {
                start: significant[start_sig].0,
                end: significant[end_sig - 1].0 + 1,
                table_parts: parts[..3].to_vec(),
            });
        }
        sig_index = end_sig;
    }
    spans
}

fn collect_parts(significant_slice: &[(usize, &Token)]) -> Vec<String> {
    significant_slice
        .iter()
        .filter_map(|(_, token)| match token {
            Token::Word(word) => Some(word.value.clone()),
            Token::DoubleQuotedString(value) => Some(value.clone()),
            _ => None,
        })
        .collect()
}

fn follows_from_or_join(significant: &[(usize, &Token)], name_sig_start: usize) -> bool {
    let Some(previous) = name_sig_start.checked_sub(1) else {
        return false;
    };
    match significant.get(previous).map(|(_, token)| *token) {
        Some(Token::Word(word)) => {
            let keyword = word.value.to_ascii_uppercase();
            keyword == "FROM" || keyword == "JOIN"
        }
        Some(Token::Comma) => true,
        _ => false,
    }
}
