//! Case-insensitive by-name column resolution — Spark's default write/merge conform semantics.
//!
//! Spark resolves column names case-insensitively unless the user opts into strict matching
//! (`spark.sql.caseSensitive`, default **false** — `SqlApiConf.caseSensitiveAnalysis = false`).
//! The resolver itself is `caseInsensitiveResolution = (a, b) => a.equalsIgnoreCase(b)`
//! (`analysis/package.scala`), and by-name write conform runs it per target column in
//! `TableOutputResolver.reorderColumnsByName`:
//!
//! ```text
//! val matched = inputCols.filter(col => conf.resolver(col.name, expectedCol.name))
//! if (matched.isEmpty)      => missing-column error
//! else if (matched.length > 1) => incompatibleDataToTableAmbiguousColumnNameError(...)  // LOUD
//! else                      => matched.head                                             // use it
//! ```
//!
//! This module ports exactly that decision. It is shared by both by-name conform surfaces —
//! `append::conform_batch` (bulk append / partitioned CTAS) and `merge::expand_star_clauses`
//! (`MERGE … UPDATE SET * / INSERT *`) — so the two cannot drift. It is error-type agnostic: it
//! reports [`SourceMatch`] and each caller renders the surface-appropriate `DataFusionError`.

use std::collections::HashMap;

/// ===============================================================================================
/// The outcome of resolving one target column name against a set of source column names, mirroring
/// Spark's `reorderColumnsByName` decision (zero / one / many candidates).
/// ===============================================================================================
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SourceMatch {
    /// No source column resolves to the target (Spark's `matched.isEmpty`).
    Missing,
    /// Exactly one source column resolves to the target — its index into the source name list.
    Unique(usize),
    /// More than one source column resolves to the target (Spark's `matched.length > 1`); carries
    /// every colliding source column name so the caller's error can name them all.
    Ambiguous(Vec<String>),
}

/// ===============================================================================================
/// A source column-name set indexed for case-insensitive lookup. Built once per conform (O(source
/// columns), each name lowercased into the index); every target then resolves with a single
/// lowercased lookup (no O(cols²) scan). An exact-case name lands in its own lowercased bucket and
/// resolves uniquely — the common all-exact schema is behaviourally unchanged from the prior
/// exact-only path, though the implementation always lowercases (no exact-match short circuit).
/// Only a genuine case COLLISION (`id` and `ID`, or an exact duplicate `id`,`id`) produces a
/// multi-entry bucket, i.e. the ambiguity Spark rejects.
/// ===============================================================================================
pub(crate) struct CaseInsensitiveColumnIndex<'a> {
    source_names: Vec<&'a str>,
    indices_by_lowercase: HashMap<String, Vec<usize>>,
}

impl<'a> CaseInsensitiveColumnIndex<'a> {
    /// Index `source_names` (in their original order) by lowercased form. Full-Unicode
    /// `to_lowercase` matches Java `String.equalsIgnoreCase` on the ASCII identifiers that dominate
    /// real schemas; exotic non-ASCII case-folding differences are out of scope.
    pub(crate) fn new(source_names: impl IntoIterator<Item = &'a str>) -> Self {
        let source_names: Vec<&'a str> = source_names.into_iter().collect();
        let mut indices_by_lowercase: HashMap<String, Vec<usize>> =
            HashMap::with_capacity(source_names.len());
        for (index, name) in source_names.iter().enumerate() {
            indices_by_lowercase
                .entry(name.to_lowercase())
                .or_default()
                .push(index);
        }
        Self {
            source_names,
            indices_by_lowercase,
        }
    }

    /// Resolve one target column name against the indexed source columns (Spark's per-target
    /// `reorderColumnsByName` filter). Zero candidates → [`SourceMatch::Missing`]; more than one →
    /// [`SourceMatch::Ambiguous`] naming every colliding source column; exactly one →
    /// [`SourceMatch::Unique`] with its source index.
    pub(crate) fn resolve(&self, target_name: &str) -> SourceMatch {
        match self
            .indices_by_lowercase
            .get(&target_name.to_lowercase())
            .map(Vec::as_slice)
        {
            None | Some([]) => SourceMatch::Missing,
            Some(&[only]) => SourceMatch::Unique(only),
            Some(many) => SourceMatch::Ambiguous(
                many.iter()
                    .map(|&index| self.source_names[index].to_string())
                    .collect(),
            ),
        }
    }

    /// The original-cased source column name at `source_index` (as returned by [`Self::resolve`]).
    pub(crate) fn source_name(&self, source_index: usize) -> &'a str {
        self.source_names[source_index]
    }
}

/// ===============================================================================================
/// Shared resolver pins — the case/collision decision that both by-name conform surfaces depend on
/// (append `conform_batch` + MERGE `expand_star_clauses`). Kept here, on the semantics, so a
/// surface-level regression cannot silently weaken the resolver.
/// ===============================================================================================
#[cfg(test)]
mod tests {
    use super::*;

    /// PIN PL-8a — an EXACT-case name resolves uniquely. The resolver always lowercases the target
    /// (and indexes sources by lowercase at build), so there is **no** no-alloc exact-case short
    /// circuit — every resolve allocates the lowercased lookup key. Risk: a regression that only
    /// matched lowercased forms would still pass this, so PL-8b/PL-8c carry the case-insensitive
    /// contract.
    #[test]
    fn exact_case_name_resolves_uniquely() {
        let index = CaseInsensitiveColumnIndex::new(["key", "payload"]);
        assert_eq!(index.resolve("key"), SourceMatch::Unique(0));
        assert_eq!(index.resolve("payload"), SourceMatch::Unique(1));
        assert_eq!(index.source_name(0), "key");
    }

    /// PIN PL-8b — a differently-cased source name resolves to the target (Spark
    /// `equalsIgnoreCase`), and the resolved index points at the ORIGINAL-cased source name so the
    /// caller can emit `s."KEY"`. Risk: an exact-only resolver reports Missing here.
    #[test]
    fn differently_cased_name_resolves_case_insensitively() {
        let index = CaseInsensitiveColumnIndex::new(["KEY", "Payload"]);
        assert_eq!(index.resolve("key"), SourceMatch::Unique(0));
        assert_eq!(index.resolve("payload"), SourceMatch::Unique(1));
        assert_eq!(index.source_name(0), "KEY");
    }

    /// PIN PL-8c — two source columns colliding on one target (case-differing OR exact duplicate)
    /// is AMBIGUOUS, naming EVERY colliding source column (Spark `matched.length > 1`). Risk: a
    /// first-match-wins resolver silently drops one; the exact-dup case is the degenerate ambiguity.
    #[test]
    fn colliding_source_columns_are_ambiguous_naming_all() {
        let case_collision = CaseInsensitiveColumnIndex::new(["id", "ID"]);
        assert_eq!(
            case_collision.resolve("Id"),
            SourceMatch::Ambiguous(vec!["id".to_string(), "ID".to_string()])
        );

        let exact_duplicate = CaseInsensitiveColumnIndex::new(["id", "id"]);
        assert_eq!(
            exact_duplicate.resolve("id"),
            SourceMatch::Ambiguous(vec!["id".to_string(), "id".to_string()])
        );
    }

    /// PIN PL-8d — a target with no source column is Missing (Spark `matched.isEmpty`). Risk: a
    /// resolver that defaulted to index 0 would misroute an absent column's data.
    #[test]
    fn absent_target_column_is_missing() {
        let index = CaseInsensitiveColumnIndex::new(["key"]);
        assert_eq!(index.resolve("payload"), SourceMatch::Missing);
    }
}
