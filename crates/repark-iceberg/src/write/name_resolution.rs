//! Case-insensitive by-name column resolution shared by append and MERGE.

use std::collections::HashMap;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::error::{DataFusionError, Result};

/// Outcome of resolving one target column name against source names (Spark `reorderColumnsByName`).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SourceMatch {
    /// No source column resolves to the target (Spark's `matched.isEmpty`).
    Missing,
    /// Exactly one source column resolves to the target — its index into the source name list.
    Unique(usize),
    /// More than one source column resolves to the target.
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

    pub(crate) fn resolve_exact(&self, target_name: &str) -> SourceMatch {
        match self
            .source_names
            .iter()
            .position(|name| *name == target_name)
        {
            Some(index) => SourceMatch::Unique(index),
            None => SourceMatch::Missing,
        }
    }

    pub(crate) fn resolve_scoped(&self, target_name: &str, case_insensitive: bool) -> SourceMatch {
        if case_insensitive {
            self.resolve(target_name)
        } else {
            self.resolve_exact(target_name)
        }
    }

    /// Resolve one target column name against the indexed source columns.
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

pub(crate) fn resolve_arrow_field<'a>(
    schema: &'a ArrowSchema,
    name: &str,
    case_insensitive: bool,
) -> Option<&'a str> {
    if !case_insensitive {
        return schema
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .map(|field| field.name().as_str());
    }
    let mut found: Option<&'a str> = None;
    for field in schema.fields() {
        if field.name().eq_ignore_ascii_case(name) {
            if found.is_some() {
                return None;
            }
            found = Some(field.name().as_str());
        }
    }
    found
}

pub(crate) fn arrow_field_twins<'a>(schema: &'a ArrowSchema, name: &str) -> Vec<&'a str> {
    let twins = schema
        .fields()
        .iter()
        .filter(|field| field.name().eq_ignore_ascii_case(name))
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    if twins.len() > 1 { twins } else { Vec::new() }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn resolve_write_column(
    schema: &ArrowSchema,
    column: &str,
    case_insensitive: bool,
    missing: impl FnOnce() -> String,
) -> Result<String> {
    if let Some(canonical) = resolve_arrow_field(schema, column, case_insensitive) {
        return Ok(canonical.to_string());
    }
    let twins = arrow_field_twins(schema, column);
    if case_insensitive && twins.len() > 1 {
        return Err(DataFusionError::Plan(ambiguous_write_message(
            column, &twins,
        )));
    }
    Err(DataFusionError::Plan(missing()))
}

pub(crate) fn ambiguous_write_message(column: &str, twins: &[&str]) -> String {
    let options = twins
        .iter()
        .map(|_| format!("`{column}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "[AMBIGUOUS_REFERENCE] Reference `{column}` is ambiguous, could be: [{options}]. SQLSTATE: 42704"
    )
}

pub(crate) fn dedup_key(canonical: &str, case_insensitive: bool) -> String {
    if case_insensitive {
        canonical.to_ascii_lowercase()
    } else {
        canonical.to_string()
    }
}

/// Shared resolver pins for the case/collision decision both by-name conform surfaces depend on.
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

    /// PIN PL-8b.
    #[test]
    fn differently_cased_name_resolves_case_insensitively() {
        let index = CaseInsensitiveColumnIndex::new(["KEY", "Payload"]);
        assert_eq!(index.resolve("key"), SourceMatch::Unique(0));
        assert_eq!(index.resolve("payload"), SourceMatch::Unique(1));
        assert_eq!(index.source_name(0), "KEY");
    }

    /// PIN PL-8c.
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

    #[test]
    fn write_side_case_twins_are_ambiguous() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Schema::new(vec![
            Field::new("userId", DataType::Int64, true),
            Field::new("USERID", DataType::Int64, true),
        ]);
        assert_eq!(
            arrow_field_twins(&schema, "UserId"),
            vec!["userId", "USERID"]
        );
        assert_eq!(
            ambiguous_write_message("UserId", &arrow_field_twins(&schema, "UserId")),
            "[AMBIGUOUS_REFERENCE] Reference `UserId` is ambiguous, could be: [`UserId`, `UserId`]. SQLSTATE: 42704".to_string()
        );
        let single = Schema::new(vec![Field::new("userId", DataType::Int64, true)]);
        assert!(arrow_field_twins(&single, "UserId").is_empty());
    }
}
