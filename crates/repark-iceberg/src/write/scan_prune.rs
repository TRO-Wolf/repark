//! MERGE target-scan pruning — residual join-key bounds on the Iceberg scan (R-PERF-MERGE-PRUNE).
//!
//! ## Analysis (2026-07-29) — what reaches the fork scan today
//!
//! [`crate::write::merge::TargetScanStream::execute`] opens:
//!
//! ```text
//! table.scan().snapshot_id(pin).select([data…, _file, _pos]).build()
//! ```
//!
//! Primary target may carry a residual join-key bounds filter (r24 PERF-04) when the shape is
//! safe; otherwise the scan is unfiltered. MoR/COW then join against that (possibly pruned)
//! primary; COW rewrite uses a separate file-scoped whole-file stream when conf allows.
//!
//! ## Seam — residual hazard + r24 PERF-04 partial lift
//!
//! `TableScanBuilder::with_filter(Predicate)` is public and **does** drive inclusive metrics
//! evaluation (file prune) **and** residual row filtering after a file is opened. Residual row
//! filtering is **incorrect for copy-on-write rewrite through the primary target**: survivors that
//! share a data file with a matched key must remain visible to the rewrite SQL, but a residual
//! `k ∈ [min,max]` filter drops them (reproduced: `merge_bucket_partitioned_routes_by_fork_hash`
//! lost `id=1` when source keys were `{2,8}`).
//!
//! **r24 PERF-04 (bounded equi-key):** residual **is** pushed onto the primary target when safe:
//!
//! - **`MoR`** — discovery / matched work / insert never need unmatched survivors through residual.
//! - **COW + `file_scoped_rewrite`** (default) — rewrite uses a separate whole-file allowlisted
//!   stream (`filter=None`); residual only scopes discovery + insert anti-join.
//! - **COW + file-scoped OFF** — residual stays **None** (full STOP).
//!
//! Bounded shapes only: bare equality ON, `Int32`/`Int64` keys, source min/max. General
//! multi-clause / non-equi ON is OUT (r25). Pins:
//! `cow_equi_key_residual_keeps_colocated_survivors`, `mor_equi_key_residual_upsert_correct`.
//!
//! **What the fork would need for a universal path:** a metrics-only / file-prune-only mode
//! (Java-style inclusive metrics evaluator without residual apply), or a public API to plan files
//! with a residual bound while still reading full files.
//!
//! ## Correctness of range residual
//!
//! For equality ON `target.k = source.k`, any matched target row must have `k` in
//! `[min(source.k), max(source.k)]`. Files whose column bounds fall entirely outside that range
//! can contain no MATCHED rows and need not be scanned for MATCHED work. Unmatched target rows
//! outside the range stay untouched by MoR/COW rewrite either way (they are not rewritten and
//! not position-deleted). Inserts never read those files for identity. Multi-column AND of bare
//! equalities: conjunctive bounds remain necessary for a match. Expression / NULL-safe (`<=>`)
//! conjuncts contribute no bounds; if no bare equality remains, pruning is skipped.

use datafusion::common::config::ConfigExtension;
use datafusion::common::extensions_options;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;

/// Session conf: `repark.merge.scan-pruning` — `true` (default) enables residual scan bounds;
/// `false` restores today's unfiltered target scan.
pub const SCAN_PRUNING_KEY: &str = "repark.merge.scan-pruning";

/// One bare-column equality extracted from ON: target column name (unqualified).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BareEquality {
    /// Target-side column (data column, not `_file`/`_pos`).
    pub target_column: String,
    /// Source-side column (same name in the common `t.id = s.id` shape; may differ).
    pub source_column: String,
}

/// ===========================================================================================
/// Extract bare-column equality conjuncts from an ON SQL string.
///
/// Accepts only forms (after whitespace normalize, case-insensitive keywords):
/// - `t.col = s.col` / `s.col = t.col` / `col = s.col` when aliases match the MERGE aliases
/// - AND-chained; each conjunct independent
///
/// Skips entirely (returns empty → no pruning) when ON contains `<=>`, OR, or any conjunct
/// that is not a bare two-side column equality (functions, arithmetic, literals).
/// ===========================================================================================
#[must_use]
pub fn bare_equalities_from_on(
    on_sql: &str,
    target_alias: &str,
    source_alias: &str,
) -> Vec<BareEquality> {
    let lowered = on_sql.to_ascii_lowercase();
    // NULL-safe equality or OR ⇒ skip pruning entirely (disclose).
    if lowered.contains("<=>") || contains_or_outside_parens(&lowered) {
        return Vec::new();
    }

    let mut out = Vec::new();
    for raw_conjunct in split_and_conjuncts(on_sql) {
        let trimmed = raw_conjunct.trim();
        if trimmed.is_empty() {
            continue;
        }
        // A non-bare conjunct contributes no bounds but does not invalidate the others
        // (slate refinement: bounds from the bare-equality subset remain necessary).
        if let Some(equality) = parse_bare_equality(trimmed, target_alias, source_alias) {
            out.push(equality);
        }
    }
    out
}

/// Split on top-level `AND` (case-insensitive), respecting parentheses.
fn split_and_conjuncts(sql: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let bytes = sql.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        let ch = bytes[index] as char;
        if ch == '(' {
            depth += 1;
            index += 1;
            continue;
        }
        if ch == ')' {
            depth = depth.saturating_sub(1);
            index += 1;
            continue;
        }
        if depth == 0 && matches_keyword_at(sql, index, "and") {
            parts.push(sql[start..index].to_string());
            index += 3;
            start = index;
            continue;
        }
        index += 1;
    }
    parts.push(sql[start..].to_string());
    parts
}

fn contains_or_outside_parens(lowered: &str) -> bool {
    let mut depth = 0i32;
    let bytes = lowered.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        let ch = bytes[index] as char;
        if ch == '(' {
            depth += 1;
            index += 1;
            continue;
        }
        if ch == ')' {
            depth = depth.saturating_sub(1);
            index += 1;
            continue;
        }
        if depth == 0 && matches_keyword_at(lowered, index, "or") {
            return true;
        }
        index += 1;
    }
    false
}

fn matches_keyword_at(sql: &str, index: usize, keyword: &str) -> bool {
    let rest = &sql[index..];
    let rest_lower = rest.to_ascii_lowercase();
    if !rest_lower.starts_with(keyword) {
        return false;
    }
    let after = index + keyword.len();
    let before_ok = index == 0
        || !sql
            .as_bytes()
            .get(index - 1)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_');
    let after_ok = after >= sql.len()
        || !sql
            .as_bytes()
            .get(after)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_');
    before_ok && after_ok
}

fn parse_bare_equality(
    conjunct: &str,
    target_alias: &str,
    source_alias: &str,
) -> Option<BareEquality> {
    // Only a single `=` (not `!=`, `<=`, `>=`, `<=>`).
    let eq_at = conjunct.find('=')?;
    if eq_at == 0 || eq_at + 1 >= conjunct.len() {
        return None;
    }
    let before = conjunct.as_bytes()[eq_at - 1] as char;
    let after = conjunct.as_bytes()[eq_at + 1] as char;
    if before == '<' || before == '>' || before == '!' || after == '=' {
        return None;
    }
    let left = conjunct[..eq_at].trim();
    let right = conjunct[eq_at + 1..].trim();
    let left_col = parse_column_ref(left, target_alias, source_alias)?;
    let right_col = parse_column_ref(right, target_alias, source_alias)?;
    // Need one target side and one source side.
    match (left_col.side, right_col.side) {
        (Side::Target, Side::Source) => Some(BareEquality {
            target_column: left_col.name,
            source_column: right_col.name,
        }),
        (Side::Source, Side::Target) => Some(BareEquality {
            target_column: right_col.name,
            source_column: left_col.name,
        }),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Target,
    Source,
}

struct ColRef {
    side: Side,
    name: String,
}

fn parse_column_ref(raw: &str, target_alias: &str, source_alias: &str) -> Option<ColRef> {
    // Reject anything with operators / functions / quotes / whitespace mid-token.
    let token = raw.trim();
    if token.is_empty()
        || token.contains('(')
        || token.contains(')')
        || token.contains('+')
        || token.contains('-')
        || token.contains('*')
        || token.contains('/')
        || token.contains('\'')
        || token.contains('"')
        || token.contains(' ')
    {
        return None;
    }
    let parts: Vec<&str> = token.split('.').collect();
    match parts.as_slice() {
        [alias, name] => {
            let alias_lower = alias.to_ascii_lowercase();
            let target_lower = target_alias.to_ascii_lowercase();
            let source_lower = source_alias.to_ascii_lowercase();
            if alias_lower == target_lower {
                Some(ColRef {
                    side: Side::Target,
                    name: (*name).to_string(),
                })
            } else if alias_lower == source_lower {
                Some(ColRef {
                    side: Side::Source,
                    name: (*name).to_string(),
                })
            } else {
                None
            }
        }
        // Bare name (ambiguous — not a proven bare equality under both aliases) or deeper
        // qualification: skip.
        _ => None,
    }
}

/// ===========================================================================================
/// Build an Iceberg residual predicate `col >= min AND col <= max` for one column.
/// ===========================================================================================
///
/// # Errors
/// Returns a plan error when the Datum cannot be constructed for the value type.
pub fn range_predicate_i64(column: &str, min: i64, max: i64) -> Result<Predicate> {
    let lower = Reference::new(column).greater_than_or_equal_to(Datum::long(min));
    let upper = Reference::new(column).less_than_or_equal_to(Datum::long(max));
    Ok(lower.and(upper))
}

/// ===========================================================================================
/// Build an Iceberg residual predicate for an `i32` column (`Datum::int`).
/// ===========================================================================================
///
/// # Errors
/// Returns a plan error when min/max do not fit `i32` (should not happen for Int32 arrays).
pub fn range_predicate_i32(column: &str, min: i32, max: i32) -> Result<Predicate> {
    let lower = Reference::new(column).greater_than_or_equal_to(Datum::int(min));
    let upper = Reference::new(column).less_than_or_equal_to(Datum::int(max));
    Ok(lower.and(upper))
}

/// Conjoin predicates; empty → None (no filter).
pub fn conjoin(predicates: Vec<Predicate>) -> Option<Predicate> {
    predicates.into_iter().reduce(Predicate::and)
}

/// Parse `repark.merge.scan-pruning` — missing → true; invalid → error.
///
/// # Errors
/// Non-boolean string.
pub fn parse_scan_pruning_enabled(raw: Option<&str>) -> Result<bool> {
    match raw {
        None => Ok(true),
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            other => Err(DataFusionError::Plan(format!(
                "config `{SCAN_PRUNING_KEY}` must be a boolean (got {other:?})"
            ))),
        },
    }
}

// DataFusion extension keys under `repark.merge.*` (underscore fields).
extensions_options! {
    /// MERGE execution knobs (session-scoped): scan pruning + file-scoped COW rewrite.
    pub struct ReparkMergeConfig {
        /// When true, residual join-key bounds are pushed to the target Iceberg scan.
        pub scan_pruning: bool, default = true
        /// When true, COW rewrite opens only affected data files (R-MERGE-FILE-SCAN).
        pub file_scoped_rewrite: bool, default = true
    }
}

impl ConfigExtension for ReparkMergeConfig {
    const PREFIX: &'static str = "repark.merge";
}

/// Attach merge scan-pruning config to a session.
///
/// Sets `file_scoped_rewrite` to its default (`true`). Call
/// [`with_merge_session_knobs`] when both knobs are known at build time.
#[must_use]
pub fn with_scan_pruning(config: SessionConfig, enabled: bool) -> SessionConfig {
    config.with_option_extension(ReparkMergeConfig {
        scan_pruning: enabled,
        file_scoped_rewrite: true,
    })
}

/// Attach both MERGE session knobs in one extension write (preferred at session build).
#[must_use]
pub fn with_merge_session_knobs(
    config: SessionConfig,
    scan_pruning: bool,
    file_scoped_rewrite: bool,
) -> SessionConfig {
    config.with_option_extension(ReparkMergeConfig {
        scan_pruning,
        file_scoped_rewrite,
    })
}

/// Attach file-scoped COW rewrite conf alone (`scan_pruning` left at default true).
#[must_use]
pub fn with_file_scoped_rewrite(config: SessionConfig, enabled: bool) -> SessionConfig {
    config.with_option_extension(ReparkMergeConfig {
        scan_pruning: true,
        file_scoped_rewrite: enabled,
    })
}

/// Resolve file-scoped rewrite from the live session (default true).
#[must_use]
pub fn file_scoped_rewrite_from_ctx(ctx: &SessionContext) -> bool {
    ctx.copied_config()
        .options()
        .extensions
        .get::<ReparkMergeConfig>()
        .is_none_or(|extension| extension.file_scoped_rewrite)
}

/// Resolve scan pruning from the live session (default true).
#[must_use]
pub fn scan_pruning_from_ctx(ctx: &SessionContext) -> bool {
    ctx.copied_config()
        .options()
        .extensions
        .get::<ReparkMergeConfig>()
        .is_none_or(|extension| extension.scan_pruning)
}

/// Pull `repark.merge.scan-pruning` from a builder conf map.
///
/// # Errors
/// Invalid boolean string.
pub fn scan_pruning_from_config_map<S>(
    config: &std::collections::HashMap<String, String, S>,
) -> Result<bool>
where
    S: std::hash::BuildHasher,
{
    let raw = config
        .get(SCAN_PRUNING_KEY)
        .or_else(|| config.get("repark.merge.scan_pruning"));
    parse_scan_pruning_enabled(raw.map(String::as_str))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_simple_equality() {
        let found = bare_equalities_from_on("t.id = s.id", "t", "s");
        assert_eq!(
            found,
            vec![BareEquality {
                target_column: "id".into(),
                source_column: "id".into(),
            }]
        );
    }

    #[test]
    fn extracts_and_chain_skips_expression() {
        let found = bare_equalities_from_on("t.id = s.id AND t.v = s.v + 1", "t", "s");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].target_column, "id");
    }

    #[test]
    fn null_safe_equality_skips_all() {
        assert!(bare_equalities_from_on("t.id <=> s.id", "t", "s").is_empty());
    }

    #[test]
    fn or_skips_all() {
        assert!(bare_equalities_from_on("t.id = s.id OR t.v = s.v", "t", "s").is_empty());
    }

    #[test]
    fn expression_only_yields_empty() {
        assert!(bare_equalities_from_on("t.id = s.id + 1", "t", "s").is_empty());
    }
}
