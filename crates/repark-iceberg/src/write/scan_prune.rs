//! MERGE target-scan pruning with residual join-key bounds.
//!
//! Bounds are pushed only for bare equality keys with matching `Int32`/`Int64` types. A residual
//! filter never reaches an unscoped COW rewrite, because it can hide unmatched survivors in an
//! affected file. `MoR` and file-scoped COW use the bounds for discovery and insert anti-joins;
//! file-scoped-off COW keeps the scan unfiltered. Unsupported shapes and probe failures skip
//! pruning, preserving the correct unfiltered path. Pins:
//! `cow_equi_key_residual_keeps_colocated_survivors`, `mor_equi_key_residual_upsert_correct`.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::common::config::ConfigExtension;
use datafusion::common::extensions_options;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;

use crate::write::idents::quote_ident_spark;
use crate::write::name_resolution::{CaseInsensitiveColumnIndex, SourceMatch};

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
///
/// Walks `char_indices()` so a non-ASCII byte in ON text never slices mid-UTF-8
/// (M5). ASCII keyword matching is unchanged.
fn split_and_conjuncts(sql: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut chars = sql.char_indices();
    while let Some((index, ch)) = chars.next() {
        if ch == '(' {
            depth += 1;
            continue;
        }
        if ch == ')' {
            depth = depth.saturating_sub(1);
            continue;
        }
        if depth == 0 && matches_keyword_at(sql, index, "and") {
            parts.push(sql[start..index].to_string());
            // `and` is three ASCII chars; this iteration already consumed the first.
            let _ = chars.next();
            let _ = chars.next();
            start = index + 3;
        }
    }
    parts.push(sql[start..].to_string());
    parts
}

fn contains_or_outside_parens(lowered: &str) -> bool {
    let mut depth = 0i32;
    for (index, ch) in lowered.char_indices() {
        if ch == '(' {
            depth += 1;
            continue;
        }
        if ch == ')' {
            depth = depth.saturating_sub(1);
            continue;
        }
        if depth == 0 && matches_keyword_at(lowered, index, "or") {
            return true;
        }
    }
    false
}

fn matches_keyword_at(sql: &str, index: usize, keyword: &str) -> bool {
    // Callers walk `char_indices()`, so `index` is a char boundary. Refuse any
    // other offset rather than slice mid-character (M5).
    if !sql.is_char_boundary(index) {
        return false;
    }
    let rest = &sql[index..];
    let rest_lower = rest.to_ascii_lowercase();
    if !rest_lower.starts_with(keyword) {
        return false;
    }
    let after = index + keyword.len();
    if after > sql.len() || !sql.is_char_boundary(after) {
        return false;
    }
    let before_ok = index == 0
        || sql[..index]
            .chars()
            .next_back()
            .is_none_or(|ch| !ch.is_ascii_alphanumeric() && ch != '_');
    let after_ok = after >= sql.len()
        || sql[after..]
            .chars()
            .next()
            .is_none_or(|ch| !ch.is_ascii_alphanumeric() && ch != '_');
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

/// Width of an identical Int32/Int64 join-key pair that may receive a residual bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IntKeyWidth {
    /// Target and source fields are both `DataType::Int32`.
    I32,
    /// Target and source fields are both `DataType::Int64`.
    I64,
}

/// ===========================================================================================
/// Skip-conjunct gate (M1): push Int32/Int64 bounds only when source and target Arrow types
/// are identical. Non-identical pairs and every other type (including Utf8=Utf8) skip.
/// ===========================================================================================
#[must_use]
pub(crate) fn identical_int_key_width(
    source_type: &DataType,
    target_type: &DataType,
) -> Option<IntKeyWidth> {
    if source_type != target_type {
        return None;
    }
    match target_type {
        DataType::Int32 => Some(IntKeyWidth::I32),
        DataType::Int64 => Some(IntKeyWidth::I64),
        _ => None,
    }
}

/// ===========================================================================================
/// Resolve `name` against `schema` case-insensitively (Spark `caseSensitive=false`).
/// Missing or ambiguous case-collision → None (M7: skip the conjunct).
/// ===========================================================================================
#[must_use]
pub(crate) fn unique_schema_field<'a>(schema: &'a ArrowSchema, name: &str) -> Option<&'a Field> {
    let names = schema.fields().iter().map(|field| field.name().as_str());
    let index = CaseInsensitiveColumnIndex::new(names);
    match index.resolve(name) {
        SourceMatch::Unique(position) => Some(schema.field(position)),
        SourceMatch::Missing | SourceMatch::Ambiguous(_) => None,
    }
}

/// Plan-only source schema (`SELECT * … LIMIT 0`). Any failure → None (M6: do not abort MERGE).
async fn source_schema_for_bounds(
    ctx: &SessionContext,
    source_from_sql: &str,
    source_alias: &str,
) -> Option<SchemaRef> {
    let probe = format!("SELECT * FROM {source_from_sql} AS {source_alias} LIMIT 0");
    let frame = ctx.sql(&probe).await.ok()?;
    Some(Arc::new(frame.schema().as_arrow().clone()))
}

fn strict_cast() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..CastOptions::default()
    }
}

/// Probe source min/max and build one residual range. Any failure → None (M6).
async fn try_int_key_range_predicate(
    ctx: &SessionContext,
    source_from_sql: &str,
    source_column: &str,
    target_column: &str,
    width: IntKeyWidth,
) -> Option<Predicate> {
    let source_quoted = quote_ident_spark(source_column);
    let bounds_sql = format!(
        "SELECT min({source_quoted}) AS __repark_min, max({source_quoted}) AS __repark_max \
         FROM {source_from_sql} AS __repark_src_bounds"
    );
    let batches = ctx.sql(&bounds_sql).await.ok()?.collect().await.ok()?;
    let batch = batches.first()?;
    if batch.num_rows() == 0 || batch.num_columns() < 2 {
        return None;
    }
    match width {
        IntKeyWidth::I32 => {
            let mins = cast_with_options(batch.column(0), &DataType::Int32, &strict_cast()).ok()?;
            let maxs = cast_with_options(batch.column(1), &DataType::Int32, &strict_cast()).ok()?;
            let min_array = mins.as_any().downcast_ref::<Int32Array>()?;
            let max_array = maxs.as_any().downcast_ref::<Int32Array>()?;
            if min_array.is_empty()
                || max_array.is_empty()
                || min_array.is_null(0)
                || max_array.is_null(0)
            {
                return None;
            }
            range_predicate_i32(target_column, min_array.value(0), max_array.value(0)).ok()
        }
        IntKeyWidth::I64 => {
            let mins = cast_with_options(batch.column(0), &DataType::Int64, &strict_cast()).ok()?;
            let maxs = cast_with_options(batch.column(1), &DataType::Int64, &strict_cast()).ok()?;
            let min_array = mins.as_any().downcast_ref::<Int64Array>()?;
            let max_array = maxs.as_any().downcast_ref::<Int64Array>()?;
            if min_array.is_empty()
                || max_array.is_empty()
                || min_array.is_null(0)
                || max_array.is_null(0)
            {
                return None;
            }
            range_predicate_i64(target_column, min_array.value(0), max_array.value(0)).ok()
        }
    }
}

/// ===========================================================================================
/// Residual Iceberg predicate for bare-equality join keys (M1/M6/M7).
///
/// Never errors: type mismatch, ambiguous/missing names, cast/schema/null-shape probe
/// failures skip that conjunct. Empty after skips → None (unfiltered scan).
/// ===========================================================================================
pub async fn residual_bounds_predicate(
    ctx: &SessionContext,
    source_from_sql: &str,
    source_alias: &str,
    write_schema: &ArrowSchema,
    equalities: &[BareEquality],
) -> Option<Predicate> {
    let source_schema = source_schema_for_bounds(ctx, source_from_sql, source_alias).await?;
    let mut predicates = Vec::new();
    for equality in equalities {
        let Some(target_field) = unique_schema_field(write_schema, &equality.target_column) else {
            continue;
        };
        let Some(source_field) =
            unique_schema_field(source_schema.as_ref(), &equality.source_column)
        else {
            continue;
        };
        let Some(width) =
            identical_int_key_width(source_field.data_type(), target_field.data_type())
        else {
            continue;
        };
        if let Some(predicate) = try_int_key_range_predicate(
            ctx,
            source_from_sql,
            source_field.name(),
            target_field.name(),
            width,
        )
        .await
        {
            predicates.push(predicate);
        }
    }
    conjoin(predicates)
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

    #[test]
    fn utf8_literal_in_on_does_not_panic_and_keeps_bare_equality() {
        let found = bare_equalities_from_on("t.id = s.id AND t.city = 'Zürich'", "t", "s");
        assert_eq!(
            found,
            vec![BareEquality {
                target_column: "id".into(),
                source_column: "id".into(),
            }]
        );
    }

    #[test]
    fn utf8_column_name_in_on_does_not_panic() {
        let found = bare_equalities_from_on("t.Zürich = s.Zürich AND t.id = s.id", "t", "s");
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].target_column, "Zürich");
        assert_eq!(found[0].source_column, "Zürich");
        assert_eq!(found[1].target_column, "id");
    }

    #[test]
    fn utf8_or_in_on_skips_all_without_panic() {
        assert!(bare_equalities_from_on("t.id = s.id OR t.city = 'Zürich'", "t", "s").is_empty());
    }

    #[test]
    fn identical_int32_and_int64_are_prunable() {
        assert_eq!(
            identical_int_key_width(&DataType::Int32, &DataType::Int32),
            Some(IntKeyWidth::I32)
        );
        assert_eq!(
            identical_int_key_width(&DataType::Int64, &DataType::Int64),
            Some(IntKeyWidth::I64)
        );
    }

    #[test]
    fn non_identical_and_non_int_pairs_skip_conjunct() {
        assert_eq!(
            identical_int_key_width(&DataType::Utf8, &DataType::Int32),
            None
        );
        assert_eq!(
            identical_int_key_width(&DataType::Int64, &DataType::Int32),
            None
        );
        assert_eq!(
            identical_int_key_width(&DataType::Utf8, &DataType::Utf8),
            None
        );
    }

    #[test]
    fn unique_schema_field_is_case_insensitive() {
        let schema = ArrowSchema::new(vec![Field::new("customerid", DataType::Int64, true)]);
        let field = unique_schema_field(&schema, "CustomerId").expect("resolves lowercase schema");
        assert_eq!(field.name(), "customerid");
    }

    #[test]
    fn unique_schema_field_skips_ambiguous_case_collision() {
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("ID", DataType::Int64, true),
        ]);
        assert!(unique_schema_field(&schema, "id").is_none());
        assert!(unique_schema_field(&schema, "ID").is_none());
    }

    fn register_mem_source(
        ctx: &SessionContext,
        name: &str,
        batch: datafusion::arrow::array::RecordBatch,
    ) {
        let table = datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]])
            .expect("source memtable");
        ctx.register_table(name, Arc::new(table))
            .expect("register source");
    }

    #[tokio::test]
    async fn residual_pushes_identical_int32_keys() {
        let ctx = SessionContext::new();
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("v", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![2, 8])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("int32 source batch");
        register_mem_source(&ctx, "src", batch);
        let write_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", DataType::Utf8, true),
        ]);
        let equalities = [BareEquality {
            target_column: "id".into(),
            source_column: "id".into(),
        }];
        let residual =
            residual_bounds_predicate(&ctx, "src", "s", &write_schema, &equalities).await;
        assert!(
            residual.is_some(),
            "identical Int32 keys must still produce a residual bound"
        );
    }

    #[tokio::test]
    async fn residual_skips_utf8_source_int32_target() {
        let ctx = SessionContext::new();
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Utf8, true),
                Field::new("v", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(datafusion::arrow::array::StringArray::from(vec!["9", "10"])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("utf8 source batch");
        register_mem_source(&ctx, "src", batch);
        let write_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", DataType::Utf8, true),
        ]);
        let equalities = [BareEquality {
            target_column: "id".into(),
            source_column: "id".into(),
        }];
        let residual =
            residual_bounds_predicate(&ctx, "src", "s", &write_schema, &equalities).await;
        assert!(
            residual.is_none(),
            "Utf8→Int32 must skip the conjunct (M1); a push here is the inverted-range hole"
        );
    }

    #[tokio::test]
    async fn residual_skips_int64_source_int32_target_without_abort() {
        let ctx = SessionContext::new();
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("v", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![5, 3_000_000_000])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec![
                    "a", "big",
                ])),
            ],
        )
        .expect("int64 source batch");
        register_mem_source(&ctx, "src", batch);
        let write_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", DataType::Utf8, true),
        ]);
        let equalities = [BareEquality {
            target_column: "id".into(),
            source_column: "id".into(),
        }];
        let residual =
            residual_bounds_predicate(&ctx, "src", "s", &write_schema, &equalities).await;
        assert!(
            residual.is_none(),
            "Int64→Int32 must skip (M1/M6) rather than abort the probe"
        );
    }

    #[tokio::test]
    async fn residual_resolves_source_column_case_insensitively() {
        let ctx = SessionContext::new();
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("customerid", DataType::Int64, true),
                Field::new("amt", DataType::Float64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(datafusion::arrow::array::Float64Array::from(vec![2.0])),
            ],
        )
        .expect("lowercase source batch");
        register_mem_source(&ctx, "src", batch);
        let write_schema = ArrowSchema::new(vec![
            Field::new("customerid", DataType::Int64, true),
            Field::new("amt", DataType::Float64, true),
        ]);
        let equalities = [BareEquality {
            target_column: "CustomerId".into(),
            source_column: "CustomerId".into(),
        }];
        let residual =
            residual_bounds_predicate(&ctx, "src", "s", &write_schema, &equalities).await;
        assert!(
            residual.is_some(),
            "M7: quoting unresolved CustomerId against lowercase schema must not skip a valid Int64 key"
        );
    }

    #[tokio::test]
    async fn residual_skips_ambiguous_source_case_collision() {
        let ctx = SessionContext::new();
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("ID", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .expect("colliding source batch");
        register_mem_source(&ctx, "src", batch);
        let write_schema = ArrowSchema::new(vec![Field::new("id", DataType::Int64, true)]);
        let equalities = [BareEquality {
            target_column: "id".into(),
            source_column: "id".into(),
        }];
        let residual =
            residual_bounds_predicate(&ctx, "src", "s", &write_schema, &equalities).await;
        assert!(
            residual.is_none(),
            "M7: ambiguous id/ID source collision must skip the conjunct"
        );
    }
}
