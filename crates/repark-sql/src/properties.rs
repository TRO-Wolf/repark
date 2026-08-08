//! The `WITH ( … )` table-property vocabulary (design §2 Q1/Q2, grafts G4 + G9).
//!
//! Trino's spelling, deliberately: a **curated** set of bare keys the engine understands and
//! validates (`format`, `format_version`, `partitioning`, `location`), plus `sorted_by` held as a
//! reserved refusal, plus the functional escape hatch `extra_properties = MAP(ARRAY[…],
//! ARRAY[…])` carrying RAW Iceberg keys.
//!
//! The two halves solve opposite problems and both are needed:
//! * **Curated bare keys** give typo protection and validation. An unknown bare key refuses LOUD
//!   and lists the whole curated set, so `formatt` or `partition_by` is a one-line fix rather
//!   than a silently-ignored clause that produces a wrong table.
//! * **`extra_properties`** keeps every dotted Iceberg property reachable — `write.merge.mode`,
//!   `write.target-file-size-bytes`, … — without freezing dotted keys into this door's API.
//!   Merge-on-read table CREATION is therefore reachable in phase 2 (that is the concrete thing
//!   the hatch buys).
//!
//! `sorted_by` and the `ORC`/`AVRO` formats are the G9 **reserved refusals**: the spelling is
//! held so it cannot be re-used for something else, the refusal is loud, and the message names
//! the TRIGGER that would make us implement it. A refusal that names its trigger is a roadmap
//! entry; a refusal that does not is a wall.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SqlOption, Value,
};

use crate::partitioning::{PartitionTransform, parse_transform};

/// The curated bare keys, in the order the refusal message lists them.
const CURATED_KEYS: &[&str] = &[
    "format",
    "format_version",
    "partitioning",
    "location",
    "sorted_by",
    "extra_properties",
];

/// ===========================================================================================
/// A `WITH ( … )` clause, validated.
/// ===========================================================================================
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TableProperties {
    /// An explicit table location (`location = 's3://…'`). When absent the location is resolved
    /// from the namespace, exactly as the Spark door resolves it.
    pub(crate) location: Option<String>,
    /// The declared partition transforms, in clause order (empty = unpartitioned).
    pub(crate) partitioning: Vec<PartitionTransform>,
    /// Raw Iceberg table properties from `extra_properties`, applied at creation.
    pub(crate) extra_properties: HashMap<String, String>,
}

/// ===========================================================================================
/// Validate a parsed `WITH ( … )` option list into [`TableProperties`].
///
/// `form` names the statement in refusal messages (`CREATE TABLE` / `CREATE TABLE AS SELECT`).
/// ===========================================================================================
///
/// # Errors
/// An unknown bare key, a reserved key, an out-of-vocabulary `format`, a malformed
/// `partitioning` / `extra_properties` value, or an unsupported `format_version`.
pub(crate) fn parse_with_options(options: &[SqlOption], form: &str) -> Result<TableProperties> {
    let mut properties = TableProperties::default();
    let mut seen: Vec<String> = Vec::new();

    for option in options {
        let SqlOption::KeyValue { key, value } = option else {
            return Err(DataFusionError::NotImplemented(format!(
                "{form} WITH: only `key = value` properties are supported (got `{option}`); the \
                 supported keys are {}",
                curated_list()
            )));
        };
        let name = key.value.to_ascii_lowercase();
        if seen.contains(&name) {
            return Err(DataFusionError::Plan(format!(
                "{form} WITH: property `{name}` is specified more than once"
            )));
        }
        seen.push(name.clone());

        match name.as_str() {
            "format" => validate_format(&string_value(value, &name, form)?, form)?,
            "format_version" => validate_format_version(&scalar_value(value, &name, form)?, form)?,
            "location" => properties.location = Some(string_value(value, &name, form)?),
            "partitioning" => properties.partitioning = parse_partitioning(value, form)?,
            "sorted_by" => return Err(refuse_sorted_by(form)),
            "extra_properties" => {
                properties.extra_properties = parse_extra_properties(value, form)?;
            }
            _ => return Err(refuse_unknown_key(&key.value, form)),
        }
    }
    Ok(properties)
}

/// The curated key list, rendered for a message.
fn curated_list() -> String {
    CURATED_KEYS
        .iter()
        .map(|key| format!("`{key}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The typo guard (design §2 Q1): an unknown BARE key refuses loud and lists the whole curated
/// set, and points dotted Iceberg keys at the escape hatch that carries them.
fn refuse_unknown_key(key: &str, form: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "{form} WITH: unknown table property `{key}`. Supported properties are {}. Raw Iceberg \
         properties (dotted keys such as `write.merge.mode`) are set through the escape hatch: \
         WITH (extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['merge-on-read']))",
        curated_list()
    ))
}

/// G9 reserved refusal: hold the spelling, refuse loud, NAME THE TRIGGER.
fn refuse_sorted_by(form: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "{form} WITH: `sorted_by` is a reserved property that is not implemented yet — the \
         spelling is held so it cannot be reused for anything else. Iceberg sort orders are not \
         written by this engine today; a declared sort order would be silently ignored, which is \
         worse than this refusal. TRIGGER for implementing it: a write path that honours the sort \
         order (a sorted writer, or `rewrite_data_files` with a sort strategy). Until then, sort \
         in the SELECT that feeds the table."
    ))
}

/// `format` vocabulary: PARQUET only. ORC/AVRO are the second G9 reserved refusal — they are
/// real Iceberg formats, so the message must say we understood the request and why the answer is
/// no, plus what would change it.
fn validate_format(value: &str, form: &str) -> Result<()> {
    match value.trim().to_ascii_uppercase().as_str() {
        "PARQUET" => Ok(()),
        other @ ("ORC" | "AVRO") => Err(DataFusionError::NotImplemented(format!(
            "{form} WITH: format `{other}` is a valid Iceberg format but this engine writes \
             Parquet only — the writer stack (arrow-rs / parquet-rs) has no {other} writer. \
             TRIGGER for implementing it: an {other} writer in the fork's write path. Use \
             format = 'PARQUET' (the default)."
        ))),
        other => Err(DataFusionError::Plan(format!(
            "{form} WITH: unsupported format `{other}` (supported: 'PARQUET')"
        ))),
    }
}

/// `format_version`: the engine creates Iceberg format **v2** tables. Asking for 2 is satisfied
/// by consuming the key; anything else is a deterministic reject rather than a silently ignored
/// request — the whole point of accepting the key at all.
fn validate_format_version(value: &str, form: &str) -> Result<()> {
    match value.trim() {
        "2" => Ok(()),
        other => Err(DataFusionError::NotImplemented(format!(
            "{form} WITH: format_version = {other} is not supported — tables are created as \
             Iceberg format v2"
        ))),
    }
}

/// `partitioning = ARRAY['month(ts)', 'bucket(16, id)']` — the ONLY accepted spelling (Q2).
fn parse_partitioning(value: &Expr, form: &str) -> Result<Vec<PartitionTransform>> {
    let Expr::Array(array) = value else {
        return Err(DataFusionError::Plan(format!(
            "{form} WITH: `partitioning` must be an ARRAY of transform strings, e.g. \
             partitioning = ARRAY['month(ts)', 'bucket(16, id)'] (got `{value}`)"
        )));
    };
    let mut transforms = Vec::with_capacity(array.elem.len());
    for element in &array.elem {
        let spelling = literal_string(element).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{form} WITH: each `partitioning` element must be a string literal naming a \
                 transform (got `{element}`)"
            ))
        })?;
        transforms.push(parse_transform(&spelling, form)?);
    }
    Ok(transforms)
}

/// `extra_properties = MAP(ARRAY['k', …], ARRAY['v', …])` — the G4 escape hatch, in Trino's own
/// spelling so a Trino/dbt user's SQL transfers unchanged.
fn parse_extra_properties(value: &Expr, form: &str) -> Result<HashMap<String, String>> {
    let shape = || {
        DataFusionError::Plan(format!(
            "{form} WITH: `extra_properties` must be MAP(ARRAY['key', …], ARRAY['value', …]) \
             (got `{value}`)"
        ))
    };
    let Expr::Function(function) = value else {
        return Err(shape());
    };
    if !function.name.to_string().eq_ignore_ascii_case("map") {
        return Err(shape());
    }
    let FunctionArguments::List(list) = &function.args else {
        return Err(shape());
    };
    let [keys, values] = list.args.as_slice() else {
        return Err(shape());
    };
    let keys = array_of_strings(keys).ok_or_else(shape)?;
    let values = array_of_strings(values).ok_or_else(shape)?;
    if keys.len() != values.len() {
        return Err(DataFusionError::Plan(format!(
            "{form} WITH: `extra_properties` key/value arrays must be the same length (got {} \
             keys and {} values)",
            keys.len(),
            values.len()
        )));
    }

    let mut properties = HashMap::with_capacity(keys.len());
    for (key, value) in keys.into_iter().zip(values) {
        if key.trim().is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{form} WITH: `extra_properties` keys must not be empty"
            )));
        }
        // `format-version` is an Iceberg RESERVED property: iceberg-rust rejects it as a plain
        // table property at creation, so letting it through the hatch would fail deep inside the
        // write path with an opaque error. Refuse here, naming the curated key that DOES it.
        if key.trim().eq_ignore_ascii_case("format-version") {
            return Err(DataFusionError::Plan(format!(
                "{form} WITH: `format-version` is an Iceberg reserved property and cannot be set \
                 through `extra_properties` — use the curated property `format_version` instead"
            )));
        }
        if properties.insert(key.clone(), value).is_some() {
            return Err(DataFusionError::Plan(format!(
                "{form} WITH: `extra_properties` key `{key}` is specified more than once"
            )));
        }
    }
    Ok(properties)
}

/// A function argument that is `ARRAY['a', 'b']`, as owned strings.
fn array_of_strings(arg: &FunctionArg) -> Option<Vec<String>> {
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Array(array))) = arg else {
        return None;
    };
    array.elem.iter().map(literal_string).collect()
}

/// A single-quoted string literal's value.
fn literal_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => Some(text.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// A property value that must be a string literal.
fn string_value(value: &Expr, key: &str, form: &str) -> Result<String> {
    literal_string(value).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{form} WITH: property `{key}` must be a string literal (got `{value}`)"
        ))
    })
}

/// A property value that may be a string OR a bare number (`format_version = 2`).
fn scalar_value(value: &Expr, key: &str, form: &str) -> Result<String> {
    if let Some(text) = literal_string(value) {
        return Ok(text);
    }
    if let Expr::Value(inner) = value
        && let Value::Number(number, _) = &inner.value
    {
        return Ok(number.clone());
    }
    Err(DataFusionError::Plan(format!(
        "{form} WITH: property `{key}` must be a string or a number (got `{value}`)"
    )))
}

#[cfg(test)]
mod tests;
