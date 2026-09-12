use std::collections::BTreeMap;

use super::plan::{Column, SilverPlan, Source};
use super::policy::{
    ConflictingTie, EmptyInput, FormatId, IdenticalSourcePayloadTie, InputContract, InvalidValue,
    OnEmpty, OrderDirection, Publication, QualityRule, RatioPopulation, Selection, TargetType,
    TimestampUnit, Timezone, Transform, Validator,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SilverPlanIdentity {
    canonical_bytes: Vec<u8>,
}

impl SilverPlanIdentity {
    #[must_use]
    pub fn from_bytes(canonical_bytes: Vec<u8>) -> Self {
        Self { canonical_bytes }
    }

    #[must_use]
    pub fn canonical(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

enum Canonical {
    Bool(bool),
    Int(i64),
    Uint(u32),
    Float(f64),
    Str(String),
    Arr(Vec<Canonical>),
    Obj(BTreeMap<String, Canonical>),
}

pub(crate) fn canonical_bytes(plan: &SilverPlan) -> Vec<u8> {
    let mut out = Vec::new();
    plan_canonical(plan).append_to(&mut out);
    out
}

impl Canonical {
    fn append_to(&self, out: &mut Vec<u8>) {
        match self {
            Self::Bool(true) => out.extend_from_slice(b"true"),
            Self::Bool(false) => out.extend_from_slice(b"false"),
            Self::Int(value) => out.extend_from_slice(value.to_string().as_bytes()),
            Self::Uint(value) => out.extend_from_slice(value.to_string().as_bytes()),
            Self::Float(value) => out.extend_from_slice(value.to_string().as_bytes()),
            Self::Str(value) => append_string(out, value),
            Self::Arr(items) => {
                out.push(b'[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(b',');
                    }
                    item.append_to(out);
                }
                out.push(b']');
            }
            Self::Obj(fields) => {
                out.push(b'{');
                for (index, (key, value)) in fields.iter().enumerate() {
                    if index > 0 {
                        out.push(b',');
                    }
                    append_string(out, key);
                    out.push(b':');
                    value.append_to(out);
                }
                out.push(b'}');
            }
        }
    }
}

fn append_string(out: &mut Vec<u8>, value: &str) {
    out.push(b'"');
    for item in value.chars() {
        match item {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            ch if ch.is_control() => {
                let escaped = format!("\\u{:04x}", u32::from(ch));
                out.extend_from_slice(escaped.as_bytes());
            }
            ch => {
                let mut buffer = [0; 4];
                out.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
            }
        }
    }
    out.push(b'"');
}

fn obj(pairs: Vec<(&str, Canonical)>) -> Canonical {
    let mut map = BTreeMap::new();
    for (key, value) in pairs {
        map.insert(key.to_string(), value);
    }
    Canonical::Obj(map)
}

fn plan_canonical(plan: &SilverPlan) -> Canonical {
    obj(vec![
        (
            "columns",
            Canonical::Arr(plan.columns.iter().map(column_canonical).collect()),
        ),
        ("dataset_id", Canonical::Str(plan.dataset_id.clone())),
        ("input_contract", input_canonical(&plan.input_contract)),
        (
            "plan_format_version",
            Canonical::Int(plan.plan_format_version),
        ),
        ("publication", publication_canonical(&plan.publication)),
        (
            "quality",
            Canonical::Arr(plan.quality.iter().map(quality_canonical).collect()),
        ),
        ("selection", selection_canonical(&plan.selection)),
        ("source", source_canonical(&plan.source)),
    ])
}

fn source_canonical(source: &Source) -> Canonical {
    obj(vec![
        ("catalog", Canonical::Str(source.catalog.clone())),
        ("schema", Canonical::Str(source.schema.clone())),
        (
            "source_system_id",
            Canonical::Str(source.source_system_id.clone()),
        ),
        ("table", Canonical::Str(source.table.clone())),
    ])
}

fn input_canonical(input: &InputContract) -> Canonical {
    match input {
        InputContract::VersionedRowsWithoutDeletes {
            bronze_record_id_field,
        } => obj(vec![
            (
                "bronze_record_id_field",
                Canonical::Int(i64::from(*bronze_record_id_field)),
            ),
            (
                "kind",
                Canonical::Str("versioned_rows_without_deletes".to_string()),
            ),
        ]),
        InputContract::RowSnapshot {
            bronze_record_id_field,
        } => obj(vec![
            (
                "bronze_record_id_field",
                Canonical::Int(i64::from(*bronze_record_id_field)),
            ),
            ("kind", Canonical::Str("row_snapshot".to_string())),
        ]),
    }
}

fn column_canonical(column: &Column) -> Canonical {
    obj(vec![
        ("nullable", Canonical::Bool(column.nullable)),
        (
            "source_field_id",
            Canonical::Int(i64::from(column.source_field_id)),
        ),
        ("target_name", Canonical::Str(column.target_name.clone())),
        ("target_type", target_type_canonical(&column.target_type)),
        (
            "transforms",
            Canonical::Arr(column.transforms.iter().map(transform_canonical).collect()),
        ),
        (
            "validators",
            Canonical::Arr(column.validators.iter().map(validator_canonical).collect()),
        ),
    ])
}

fn target_type_canonical(target: &TargetType) -> Canonical {
    match target {
        TargetType::Int32 => obj(vec![("kind", Canonical::Str("int32".to_string()))]),
        TargetType::Int64 => obj(vec![("kind", Canonical::Str("int64".to_string()))]),
        TargetType::Utf8 => obj(vec![("kind", Canonical::Str("utf8".to_string()))]),
        TargetType::Boolean => obj(vec![("kind", Canonical::Str("boolean".to_string()))]),
        TargetType::Date => obj(vec![("kind", Canonical::Str("date".to_string()))]),
        TargetType::Timestamp { unit, timezone } => obj(vec![
            ("kind", Canonical::Str("timestamp".to_string())),
            ("timezone", Canonical::Str(timezone_name(*timezone))),
            ("unit", Canonical::Str(unit_name(unit))),
        ]),
        TargetType::Decimal { precision, scale } => obj(vec![
            ("kind", Canonical::Str("decimal".to_string())),
            ("precision", Canonical::Uint(*precision)),
            ("scale", Canonical::Uint(*scale)),
        ]),
    }
}

fn transform_canonical(transform: &Transform) -> Canonical {
    match transform {
        Transform::Trim { characters } => obj(vec![
            (
                "characters",
                Canonical::Arr(
                    characters
                        .iter()
                        .map(|item| Canonical::Str(item.clone()))
                        .collect(),
                ),
            ),
            ("kind", Canonical::Str("trim".to_string())),
        ]),
        Transform::EmptyToNull => obj(vec![("kind", Canonical::Str("empty_to_null".to_string()))]),
        Transform::ParseTimestamp {
            format_id,
            invalid_value,
        } => obj(vec![
            ("format_id", Canonical::Str(format_id_name(format_id))),
            (
                "invalid_value",
                Canonical::Str(invalid_value_name(invalid_value)),
            ),
            ("kind", Canonical::Str("parse_timestamp".to_string())),
        ]),
    }
}

fn validator_canonical(validator: &Validator) -> Canonical {
    match validator {
        Validator::RequireNonNull { rule_id } => obj(vec![
            ("kind", Canonical::Str("require_non_null".to_string())),
            ("rule_id", Canonical::Str(rule_id.clone())),
        ]),
        Validator::CheckAllowedValues { rule_id, values } => obj(vec![
            ("kind", Canonical::Str("check_allowed_values".to_string())),
            ("rule_id", Canonical::Str(rule_id.clone())),
            (
                "values",
                Canonical::Arr(
                    values
                        .iter()
                        .map(|item| Canonical::Str(item.clone()))
                        .collect(),
                ),
            ),
        ]),
    }
}

fn selection_canonical(selection: &Selection) -> Canonical {
    match selection {
        Selection::Disabled => obj(vec![("strategy", Canonical::Str("disabled".to_string()))]),
        Selection::LatestSourceVersion {
            key_fields,
            order_fields,
            conflicting_tie,
            identical_source_payload_tie,
        } => obj(vec![
            (
                "conflicting_tie",
                Canonical::Str(conflicting_tie_name(conflicting_tie)),
            ),
            (
                "identical_source_payload_tie",
                Canonical::Str(identical_tie_name(identical_source_payload_tie)),
            ),
            (
                "key_fields",
                Canonical::Arr(
                    key_fields
                        .iter()
                        .map(|value| Canonical::Int(i64::from(*value)))
                        .collect(),
                ),
            ),
            (
                "order_fields",
                Canonical::Arr(
                    order_fields
                        .iter()
                        .map(|field| {
                            obj(vec![
                                (
                                    "direction",
                                    Canonical::Str(direction_name(&field.direction)),
                                ),
                                (
                                    "source_field_id",
                                    Canonical::Int(i64::from(field.source_field_id)),
                                ),
                            ])
                        })
                        .collect(),
                ),
            ),
            (
                "strategy",
                Canonical::Str("latest_source_version".to_string()),
            ),
        ]),
    }
}

fn quality_canonical(rule: &QualityRule) -> Canonical {
    match rule {
        QualityRule::ExactDispositionAccounting { rule_id } => obj(vec![
            (
                "kind",
                Canonical::Str("exact_disposition_accounting".to_string()),
            ),
            ("rule_id", Canonical::Str(rule_id.clone())),
        ]),
        QualityRule::AcceptedKeyUniqueness { rule_id } => obj(vec![
            (
                "kind",
                Canonical::Str("accepted_key_uniqueness".to_string()),
            ),
            ("rule_id", Canonical::Str(rule_id.clone())),
        ]),
        QualityRule::RatioAtMost {
            rule_id,
            numerator,
            denominator,
            threshold,
            on_empty,
        } => obj(vec![
            ("denominator", Canonical::Str(population_name(denominator))),
            ("kind", Canonical::Str("ratio_at_most".to_string())),
            ("numerator", Canonical::Str(population_name(numerator))),
            ("on_empty", Canonical::Str(on_empty_name(on_empty))),
            ("rule_id", Canonical::Str(rule_id.clone())),
            ("threshold", Canonical::Float(*threshold)),
        ]),
    }
}

fn publication_canonical(publication: &Publication) -> Canonical {
    match publication {
        Publication::SingleClassifiedTable { empty_input } => obj(vec![
            ("empty_input", Canonical::Str(empty_input_name(empty_input))),
            (
                "strategy",
                Canonical::Str("single_classified_table".to_string()),
            ),
        ]),
    }
}

fn timezone_name(timezone: Timezone) -> String {
    match timezone {
        Timezone::Utc => "UTC".to_string(),
    }
}

fn unit_name(unit: &TimestampUnit) -> String {
    match unit {
        TimestampUnit::Second => "second".to_string(),
        TimestampUnit::Millisecond => "millisecond".to_string(),
        TimestampUnit::Microsecond => "microsecond".to_string(),
        TimestampUnit::Nanosecond => "nanosecond".to_string(),
    }
}

fn format_id_name(format_id: &FormatId) -> String {
    match format_id {
        FormatId::Iso8601SecondsOffsetV1 => "iso8601_seconds_offset_v1".to_string(),
    }
}

fn invalid_value_name(value: &InvalidValue) -> String {
    match value {
        InvalidValue::QuarantineRecord => "quarantine_record".to_string(),
    }
}

fn direction_name(direction: &OrderDirection) -> String {
    match direction {
        OrderDirection::Ascending => "ascending".to_string(),
        OrderDirection::Descending => "descending".to_string(),
    }
}

fn population_name(population: &RatioPopulation) -> String {
    match population {
        RatioPopulation::UnkeyedQuarantinedRows => "unkeyed_quarantined_rows".to_string(),
        RatioPopulation::InputRows => "input_rows".to_string(),
        RatioPopulation::QuarantinedKeyedWinners => "quarantined_keyed_winners".to_string(),
        RatioPopulation::KeyedWinners => "keyed_winners".to_string(),
    }
}

fn on_empty_name(on_empty: &OnEmpty) -> String {
    match on_empty {
        OnEmpty::Pass => "pass".to_string(),
        OnEmpty::Fail => "fail".to_string(),
    }
}

fn empty_input_name(empty_input: &EmptyInput) -> String {
    match empty_input {
        EmptyInput::RequireExplicitEmptyReplaceOption => {
            "require_explicit_empty_replace_option".to_string()
        }
    }
}

fn conflicting_tie_name(tie: &ConflictingTie) -> String {
    match tie {
        ConflictingTie::FailRun => "fail_run".to_string(),
    }
}

fn identical_tie_name(tie: &IdenticalSourcePayloadTie) -> String {
    match tie {
        IdenticalSourcePayloadTie::LowestBronzeRecordId => "lowest_bronze_record_id".to_string(),
    }
}
