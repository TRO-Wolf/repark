//! Iceberg time-travel SQL rewrite: `VERSION AS OF` / `TIMESTAMP AS OF` (+ Spark's
//! `FOR SYSTEM_VERSION` / `FOR SYSTEM_TIME` spellings).
//!
//! sqlparser 0.59's `TableVersion` only models `FOR SYSTEM_TIME AS OF` and only when the dialect
//! opts into `supports_timestamp_versioning()` — Databricks dialect does not. Spark Iceberg's bare
//! `VERSION AS OF` / `TIMESTAMP AS OF` are unmodelled. This module therefore sniffs + rewrites at
//! the **token** level (same pattern as `USING` / `NAMESPACE` / `UNSET TBLPROPERTIES`), resolves a
//! snapshot id against table metadata, registers a fork
//! [`IcebergStaticTableProvider::try_new_from_table_snapshot`](iceberg_datafusion::IcebergStaticTableProvider)
//! (never a post-hoc filter), and rewrites the FROM/JOIN relation to an ephemeral temp view.
//!
//! The PIN half of v1's module — [`TimeTravelSpec`], its parsers, snapshot resolution, and the
//! `read_iceberg_table` reader-options path (`read_table_at`) — was hoisted MOVE-ONLY to
//! `repark_core::time_travel` in phase 1; this module keeps the SQL-TEXT half (the span scan +
//! FROM/JOIN splice) and imports the pin half from repark-core.
//!
//! Fork citations (pin `4723104b`):
//! - Static provider: `crates/integrations/datafusion/src/table/mod.rs:420`
//! - `snapshot_by_id` / `snapshot_for_ref` / `history`: `crates/iceberg/src/spec/table_metadata.rs:290-326`
//! - `snapshot_id_as_of_time` (`<=` semantics): `crates/iceberg/src/inspect/metadata_log_entries.rs:129-138`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergStaticTableProvider;
use repark_core::{
    CatalogRegistry, TimeTravelSpec, parse_timestamp_to_ms, parse_version_value,
    resolve_snapshot_id,
};

use crate::catalog_ops::iceberg_err;

/// Process-wide counter so ephemeral temp-view names never collide across concurrent sessions.
static TEMP_VIEW_SEQ: AtomicU64 = AtomicU64::new(1);

/// ===========================================================================================
/// Whether `sql` contains a Spark Iceberg time-travel clause we must rewrite.
/// ===========================================================================================
#[must_use]
pub fn sql_has_time_travel(sql: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return false;
    };
    !find_time_travel_spans(&tokens).is_empty()
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

/// ===========================================================================================
/// If `sql` has time-travel clauses: resolve each to a snapshot-pinned
/// [`IcebergStaticTableProvider`], register ephemeral temp views, rewrite FROM/JOIN relations,
/// and return the rewritten SQL. Returns `Ok(None)` when there is nothing to rewrite.
/// ===========================================================================================
///
/// # Errors
/// Propagates parse, catalog, snapshot-resolution, and provider-build errors.
pub async fn prepare_time_travel_sql(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<Option<String>> {
    let dialect = DatabricksDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return Ok(None);
    };
    let spans = find_time_travel_spans(&tokens);
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
        let temp_name = next_temp_view_name();
        // Replace any prior registration of the same ephemeral name (should never collide).
        let _ = ctx.deregister_table(temp_name.as_str());
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

fn next_temp_view_name() -> String {
    let sequence = TEMP_VIEW_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("__repark_tt_{sequence}")
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
        // Walk left over Ident (Period Ident)*.
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

        // Value is the token(s) after OF. Accept number, string, bare identifier, or
        // `TIMESTAMP '…'` (two significant tokens).
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

    // Unary minus + number: Iceberg snapshot ids are signed i64 and are often negative
    // (Java `ThreadLocalRandom.nextLong()`). sqlparser emits `Minus` + `Number` rather than a
    // single signed number token — without this arm, `VERSION AS OF -N` is not rewritten and
    // time travel silently fails open to a parse error (octo C2-L-001 / C2-Q-001).
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
