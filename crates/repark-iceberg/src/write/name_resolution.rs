//! Case-insensitive by-name column resolution shared by append and MERGE.

use std::collections::HashMap;

/// The outcome of resolving one target column name against a set of source column names, mirroring
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SourceMatch {
    /// No source column resolves to the target (Spark's `matched.isEmpty`).
    Missing,
    /// Exactly one source column resolves to the target — its index into the source name list.
    Unique(usize),
    /// More than one source column resolves to the target (Spark's `matched.length > 1`); carries
    Ambiguous(Vec<String>),
}

/// Index source names case-insensitively.
pub(crate) struct CaseInsensitiveColumnIndex<'a> {
    source_names: Vec<&'a str>,
    indices_by_lowercase: HashMap<String, Vec<usize>>,
}

impl<'a> CaseInsensitiveColumnIndex<'a> {
    /// Index `source_names` (in their original order) by lowercased form.
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

/// Shared resolver pins — the case/collision decision that both by-name conform surfaces depend on
#[cfg(test)]
mod tests {
    use super::*;

    /// PIN PL-8a — an EXACT-case name resolves uniquely.
    #[test]
    fn exact_case_name_resolves_uniquely() {
        let index = CaseInsensitiveColumnIndex::new(["key", "payload"]);
        assert_eq!(index.resolve("key"), SourceMatch::Unique(0));
        assert_eq!(index.resolve("payload"), SourceMatch::Unique(1));
        assert_eq!(index.source_name(0), "key");
    }

    /// PIN PL-8b — a differently-cased source name resolves to the target (Spark
    #[test]
    fn differently_cased_name_resolves_case_insensitively() {
        let index = CaseInsensitiveColumnIndex::new(["KEY", "Payload"]);
        assert_eq!(index.resolve("key"), SourceMatch::Unique(0));
        assert_eq!(index.resolve("payload"), SourceMatch::Unique(1));
        assert_eq!(index.source_name(0), "KEY");
    }

    /// PIN PL-8c — two source columns colliding on one target (case-differing OR exact duplicate)
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

    /// PIN PL-8d — a target with no source column is Missing (Spark `matched.isEmpty`).
    #[test]
    fn absent_target_column_is_missing() {
        let index = CaseInsensitiveColumnIndex::new(["key"]);
        assert_eq!(index.resolve("payload"), SourceMatch::Missing);
    }
}
