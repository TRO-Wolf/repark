//! Rewrite Spark Iceberg time-travel clauses to snapshot-pinned temporary providers.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergStaticTableProvider;
use repark_core::{
    CatalogRegistry, TimeTravelSpec, next_temp_view_name, parse_timestamp_to_ms,
    parse_version_value, resolve_snapshot_id,
};

use crate::catalog_ops::iceberg_err;

/// Whether `sql` contains a Spark Iceberg time-travel clause we must rewrite.
#[must_use]
pub fn sql_has_time_travel(sql: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return false;
    };
    !find_pinned_spans(&tokens).is_empty()
}

fn find_pinned_spans(tokens: &[Token]) -> Vec<TimeTravelSpan> {
    let mut spans = find_time_travel_spans(tokens);
    let claimed: Vec<(usize, usize)> = spans
        .iter()
        .map(|span| (span.table_start, span.clause_end))
        .collect();
    for span in find_ref_selector_spans(tokens) {
        let overlaps = claimed
            .iter()
            .any(|(start, end)| span.table_start < *end && *start < span.clause_end);
        if !overlaps {
            spans.push(span);
        }
    }
    spans.sort_by_key(|span| span.table_start);
    spans
}

/// One FROM/JOIN relation carrying an AS OF pin, with token indices for rewrite.
#[derive(Debug, Clone)]
struct TimeTravelSpan {
    /// Token index of the first table-name word.
    table_start: usize,
    /// Token index one past the last AS OF value token.
    clause_end: usize,
    table_parts: Vec<String>,
    spec: TimeTravelSpec,
}

/// Ephemeral names one statement registered, so the router can drop them after planning.
#[derive(Debug, Default)]
pub struct PinnedViews {
    names: Vec<String>,
}

impl PinnedViews {
    pub(crate) fn record(&mut self, name: String) {
        self.names.push(name);
    }

    /// Deregister every name this statement minted.
    pub fn release(&self, ctx: &SessionContext) {
        for name in &self.names {
            let _ = ctx.deregister_table(name.as_str());
        }
    }
}

/// Resolve time-travel clauses to snapshot-pinned providers and rewrite FROM to ephemeral views.
/// # Errors
/// Propagates parse, catalog, snapshot-resolution, and provider-build errors.
pub async fn prepare_time_travel_sql(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    pinned: &mut PinnedViews,
) -> Result<Option<String>> {
    let dialect = DatabricksDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return Ok(None);
    };
    let spans = find_pinned_spans(&tokens);
    if spans.is_empty() {
        return Ok(None);
    }

    // Resolve + register right-to-left so token indices stay valid for splicing.
    let mut tokens = tokens;
    for span in spans.into_iter().rev() {
        let snapshot_id = resolve_table_snapshot(catalogs, &span.table_parts, &span.spec).await?;
        let table = load_iceberg_table(catalogs, &span.table_parts).await?;
        let provider = IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
            .await
            .map_err(iceberg_err)?;
        // The SHARED minter in repark-core.
        let temp_name = next_temp_view_name();
        // KEPT after the unification, and not dead.
        let _ = ctx.deregister_table(temp_name.as_str());
        // Recorded BEFORE the registration attempt.
        pinned.names.push(temp_name.clone());
        ctx.register_table(temp_name.as_str(), Arc::new(provider))
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "failed to register time-travel temp view {temp_name}: {error}"
                ))
            })?;
        // Splice: table name + AS OF clause → single temp-view identifier.
        let replacement = Token::Word(Word {
            value: temp_name,
            quote_style: None,
            keyword: datafusion::sql::sqlparser::keywords::Keyword::NoKeyword,
        });
        tokens.splice(
            span.table_start..span.clause_end,
            std::iter::once(replacement),
        );
    }

    Ok(Some(tokens_to_sql(&tokens)))
}

async fn resolve_table_snapshot(
    catalogs: &CatalogRegistry,
    table_parts: &[String],
    spec: &TimeTravelSpec,
) -> Result<i64> {
    let table = load_iceberg_table(catalogs, table_parts).await?;
    resolve_snapshot_id(table.metadata(), spec)
}

async fn load_iceberg_table(
    catalogs: &CatalogRegistry,
    table_parts: &[String],
) -> Result<iceberg::table::Table> {
    let (catalog_name, ident) = three_part_ident(table_parts)?;
    let catalog = catalogs.get(&catalog_name).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "catalog '{catalog_name}' is not registered — cannot time-travel table {}",
            table_parts.join(".")
        ))
    })?;
    catalog.load_table(&ident).await.map_err(iceberg_err)
}

fn three_part_ident(parts: &[String]) -> Result<(String, TableIdent)> {
    match parts {
        [catalog, namespace, table] => {
            let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
            Ok((catalog.clone(), ident))
        }
        _ => Err(DataFusionError::Plan(format!(
            "time travel requires a three-part catalog.namespace.table identifier, got `{}`",
            parts.join(".")
        ))),
    }
}

/// Scan tokens for `… VERSION AS OF …` / `… TIMESTAMP AS OF …` (and `FOR SYSTEM_*` forms).
fn find_time_travel_spans(tokens: &[Token]) -> Vec<TimeTravelSpan> {
    let significant: Vec<(usize, &Token)> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();
    if significant.is_empty() {
        return Vec::new();
    }

    let word_at = |sig_index: usize| -> Option<&str> {
        match significant.get(sig_index).map(|(_, token)| *token) {
            Some(Token::Word(word)) => Some(word.value.as_str()),
            _ => None,
        }
    };
    let is_period = |sig_index: usize| -> bool {
        matches!(
            significant.get(sig_index).map(|(_, t)| *t),
            Some(Token::Period)
        )
    };

    let mut spans = Vec::new();
    let mut sig_index = 0usize;
    while sig_index < significant.len() {
        // Look for AS OF; then walk left for the version/time keyword + optional FOR + table name.
        let is_as = word_at(sig_index).is_some_and(|w| w.eq_ignore_ascii_case("AS"));
        let is_of = word_at(sig_index + 1).is_some_and(|w| w.eq_ignore_ascii_case("OF"));
        if !is_as || !is_of {
            sig_index += 1;
            continue;
        }

        // Keyword immediately before AS: VERSION | TIMESTAMP | SYSTEM_VERSION | SYSTEM_TIME
        let Some(kind_word) = word_at(sig_index.wrapping_sub(1)) else {
            sig_index += 1;
            continue;
        };
        let kind = match kind_word.to_ascii_uppercase().as_str() {
            "VERSION" | "SYSTEM_VERSION" => TimeTravelKind::Version,
            "TIMESTAMP" | "SYSTEM_TIME" => TimeTravelKind::Timestamp,
            _ => {
                sig_index += 1;
                continue;
            }
        };

        // Optional FOR before SYSTEM_* / after bare forms are also accepted without FOR.
        let mut clause_sig_start = sig_index - 1; // kind word
        if clause_sig_start > 0
            && word_at(clause_sig_start - 1).is_some_and(|w| w.eq_ignore_ascii_case("FOR"))
        {
            clause_sig_start -= 1;
        }

        // Table name is the multipart identifier immediately before the clause.
        if clause_sig_start == 0 {
            sig_index += 1;
            continue;
        }
        let name_sig_end = clause_sig_start; // exclusive end = start of clause
        let mut name_sig_start = name_sig_end - 1;
        // name_sig_start must be a word (or quoted string used as ident).
        if !is_ident_token(significant[name_sig_start].1) {
            sig_index += 1;
            continue;
        }
        while name_sig_start >= 2
            && is_period(name_sig_start - 1)
            && is_ident_token(significant[name_sig_start - 2].1)
        {
            name_sig_start -= 2;
        }

        // Value is the token(s) after OF.
        let value_sig = sig_index + 2;
        let Some((spec, value_tokens)) = parse_as_of_value(kind, &significant, value_sig) else {
            sig_index += 1;
            continue;
        };
        let value_end_sig = value_sig + value_tokens;

        let table_parts = collect_table_parts(&significant[name_sig_start..name_sig_end]);
        if table_parts.is_empty() {
            sig_index += 1;
            continue;
        }

        let table_start = significant[name_sig_start].0;
        // One past the last original token of the value.
        let last_value_token_index = significant[value_end_sig - 1].0;
        let clause_end = last_value_token_index + 1;

        spans.push(TimeTravelSpan {
            table_start,
            clause_end,
            table_parts,
            spec,
        });
        // Continue after the value.
        sig_index = value_end_sig;
    }

    spans
}

fn find_ref_selector_spans(tokens: &[Token]) -> Vec<TimeTravelSpan> {
    let significant: Vec<(usize, &Token)> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();
    let mut spans = Vec::new();
    let mut sig_index = 0usize;
    while sig_index < significant.len() {
        let opens_relation = matches!(
            significant.get(sig_index).map(|(_, token)| *token),
            Some(Token::Word(word))
                if word.value.eq_ignore_ascii_case("FROM")
                    || word.value.eq_ignore_ascii_case("JOIN")
                    || word.value.eq_ignore_ascii_case("USING")
        );
        if !opens_relation {
            sig_index += 1;
            continue;
        }
        let name_start = sig_index + 1;
        let Some(name_end) = dotted_name_end(&significant, name_start) else {
            sig_index += 1;
            continue;
        };
        let parts = collect_table_parts(&significant[name_start..name_end]);
        let Some(ref_name) = ref_selector_name(&parts) else {
            sig_index = name_end;
            continue;
        };
        spans.push(TimeTravelSpan {
            table_start: significant[name_start].0,
            clause_end: significant[name_end - 1].0 + 1,
            table_parts: parts[..parts.len() - 1].to_vec(),
            spec: TimeTravelSpec::VersionRef(ref_name),
        });
        sig_index = name_end;
    }
    spans
}

fn dotted_name_end(significant: &[(usize, &Token)], start: usize) -> Option<usize> {
    if !is_ident_token(significant.get(start)?.1) {
        return None;
    }
    let mut end = start + 1;
    while matches!(significant.get(end).map(|(_, t)| *t), Some(Token::Period))
        && significant
            .get(end + 1)
            .is_some_and(|(_, t)| is_ident_token(t))
    {
        end += 2;
    }
    Some(end)
}

fn ref_selector_name(parts: &[String]) -> Option<String> {
    if parts.len() < 4 {
        return None;
    }
    let last = parts.last()?;
    if crate::metadata_tables::is_metadata_table_name(last) {
        return None;
    }
    let lowered = last.to_ascii_lowercase();
    let rest = lowered
        .strip_prefix("branch_")
        .or_else(|| lowered.strip_prefix("tag_"))?;
    if rest.is_empty() {
        return None;
    }
    let prefix_len = last.len() - rest.len();
    Some(last[prefix_len..].to_string())
}

#[derive(Clone, Copy)]
enum TimeTravelKind {
    Version,
    Timestamp,
}

fn is_ident_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Word(_) | Token::DoubleQuotedString(_) | Token::SingleQuotedString(_)
    )
}

fn collect_table_parts(significant_slice: &[(usize, &Token)]) -> Vec<String> {
    let mut parts = Vec::new();
    for (_, token) in significant_slice {
        match token {
            Token::Word(word) => parts.push(word.value.clone()),
            Token::DoubleQuotedString(name) | Token::SingleQuotedString(name) => {
                parts.push(name.clone());
            }
            _ => {}
        }
    }
    parts
}

/// Returns `(spec, number_of_significant_tokens_consumed)`.
fn parse_as_of_value(
    kind: TimeTravelKind,
    significant: &[(usize, &Token)],
    value_sig: usize,
) -> Option<(TimeTravelSpec, usize)> {
    let token = significant.get(value_sig).map(|(_, t)| *t)?;

    // TIMESTAMP '…' form as the AS OF value (two tokens).
    if matches!(token, Token::Word(w) if w.value.eq_ignore_ascii_case("TIMESTAMP")) {
        let next = significant.get(value_sig + 1).map(|(_, t)| *t)?;
        let text = match next {
            Token::SingleQuotedString(text) | Token::DoubleQuotedString(text) => text.clone(),
            _ => return None,
        };
        return match kind {
            TimeTravelKind::Timestamp => parse_timestamp_to_ms(&text)
                .ok()
                .map(|ms| (TimeTravelSpec::TimestampMs(ms), 2)),
            TimeTravelKind::Version => None,
        };
    }

    // Unary minus + number: Iceberg snapshot ids are signed i64 and are often negative.
    if matches!(token, Token::Minus) {
        let next = significant.get(value_sig + 1).map(|(_, t)| *t)?;
        let Token::Number(text, _) = next else {
            return None;
        };
        let raw = format!("-{text}");
        return match kind {
            TimeTravelKind::Version => parse_version_value(&raw).ok().map(|spec| (spec, 2)),
            TimeTravelKind::Timestamp => parse_timestamp_to_ms(&raw)
                .ok()
                .map(|ms| (TimeTravelSpec::TimestampMs(ms), 2)),
        };
    }

    let raw = match token {
        Token::Number(text, _)
        | Token::SingleQuotedString(text)
        | Token::DoubleQuotedString(text) => text.clone(),
        Token::Word(word) => word.value.clone(),
        _ => return None,
    };

    match kind {
        TimeTravelKind::Version => parse_version_value(&raw).ok().map(|spec| (spec, 1)),
        TimeTravelKind::Timestamp => parse_timestamp_to_ms(&raw)
            .ok()
            .map(|ms| (TimeTravelSpec::TimestampMs(ms), 1)),
    }
}

fn tokens_to_sql(tokens: &[Token]) -> String {
    tokens.iter().map(ToString::to_string).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_spark_and_system_spellings() {
        assert!(sql_has_time_travel(
            "SELECT * FROM ice.sales.t VERSION AS OF 1"
        ));
        assert!(sql_has_time_travel(
            "SELECT * FROM ice.sales.t TIMESTAMP AS OF '2020-01-01'"
        ));
        assert!(sql_has_time_travel(
            "SELECT * FROM ice.sales.t FOR SYSTEM_VERSION AS OF 1"
        ));
        assert!(sql_has_time_travel(
            "SELECT * FROM ice.sales.t FOR SYSTEM_TIME AS OF '2020-01-01'"
        ));
        assert!(!sql_has_time_travel("SELECT * FROM ice.sales.t"));
        assert!(!sql_has_time_travel(
            "SELECT * FROM ice.sales.t WHERE version = 1"
        ));
    }

    #[test]
    fn find_spans_extracts_table_and_spec() {
        let sql = "SELECT * FROM ice.sales.t VERSION AS OF 42 WHERE id > 0";
        let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
            .tokenize()
            .unwrap();
        let spans = find_time_travel_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].table_parts, vec!["ice", "sales", "t"]);
        assert_eq!(spans[0].spec, TimeTravelSpec::SnapshotId(42));
    }

    #[test]
    fn find_spans_ref_name_and_system_time() {
        let sql = "SELECT * FROM ice.sales.t VERSION AS OF 'audit_branch'";
        let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
            .tokenize()
            .unwrap();
        let spans = find_time_travel_spans(&tokens);
        assert_eq!(
            spans[0].spec,
            TimeTravelSpec::VersionRef("audit_branch".into())
        );

        let sql = "SELECT * FROM ice.sales.t FOR SYSTEM_TIME AS OF '2020-06-01 00:00:00'";
        let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
            .tokenize()
            .unwrap();
        let spans = find_time_travel_spans(&tokens);
        assert!(matches!(spans[0].spec, TimeTravelSpec::TimestampMs(_)));
    }

    #[test]
    fn find_spans_negative_snapshot_id() {
        // Iceberg snapshot ids are signed; tokenizer splits unary minus from the digits.
        let sql = "SELECT * FROM ice.sales.t VERSION AS OF -9223372036854775807";
        let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
            .tokenize()
            .unwrap();
        let spans = find_time_travel_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].spec,
            TimeTravelSpec::SnapshotId(-9_223_372_036_854_775_807)
        );
        assert_eq!(spans[0].table_parts, vec!["ice", "sales", "t"]);
    }

    #[test]
    fn find_spans_multi_relation_join() {
        let sql = "SELECT * FROM ice.sales.a VERSION AS OF 1 \
                   JOIN ice.sales.b VERSION AS OF 2 ON true";
        let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
            .tokenize()
            .unwrap();
        let spans = find_time_travel_spans(&tokens);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].table_parts, vec!["ice", "sales", "a"]);
        assert_eq!(spans[0].spec, TimeTravelSpec::SnapshotId(1));
        assert_eq!(spans[1].table_parts, vec!["ice", "sales", "b"]);
        assert_eq!(spans[1].spec, TimeTravelSpec::SnapshotId(2));
    }

    #[test]
    fn comments_do_not_false_positive_time_travel() {
        assert!(!sql_has_time_travel(
            "SELECT * FROM ice.sales.t /* VERSION AS OF 1 */"
        ));
        assert!(!sql_has_time_travel(
            "SELECT * FROM ice.sales.t -- VERSION AS OF 1\nWHERE id > 0"
        ));
    }

    #[test]
    fn find_spans_double_quoted_table_parts() {
        let sql = r#"SELECT * FROM ice."sales"."t" VERSION AS OF 7"#;
        let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
            .tokenize()
            .unwrap();
        let spans = find_time_travel_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].table_parts, vec!["ice", "sales", "t"]);
        assert_eq!(spans[0].spec, TimeTravelSpec::SnapshotId(7));
    }

    #[test]
    fn system_version_string_ref_span() {
        let sql = "SELECT * FROM ice.sales.t FOR SYSTEM_VERSION AS OF 'main'";
        let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
            .tokenize()
            .unwrap();
        let spans = find_time_travel_spans(&tokens);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].spec, TimeTravelSpec::VersionRef("main".into()));
    }
}
