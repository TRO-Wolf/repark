use std::io::Write;

use super::plan::{Column, SilverPlan, Source};
use super::policy::{
    ConflictingTie, EmptyInput, FormatId, IdenticalSourcePayloadTie, InputContract, InvalidValue,
    OnEmpty, OrderDirection, OrderField, Publication, QualityRule, RatioPopulation, Selection,
    TargetType, TimestampUnit, Timezone, Transform, Validator,
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

pub(crate) fn canonical_bytes(plan: &SilverPlan) -> Vec<u8> {
    let mut out = Vec::with_capacity(1024 + plan.columns.len() * 160);
    write_plan(&mut out, plan);
    out
}

fn write_plan(out: &mut Vec<u8>, plan: &SilverPlan) {
    out.push(b'{');
    write_static_key(out, "columns");
    write_columns(out, &plan.columns);
    out.push(b',');
    write_static_key(out, "dataset_id");
    append_string(out, &plan.dataset_id);
    out.push(b',');
    write_static_key(out, "input_contract");
    write_input(out, &plan.input_contract);
    out.push(b',');
    write_static_key(out, "plan_format_version");
    write_i64(out, plan.plan_format_version);
    out.push(b',');
    write_static_key(out, "publication");
    write_publication(out, &plan.publication);
    out.push(b',');
    write_static_key(out, "quality");
    write_quality_list(out, &plan.quality);
    out.push(b',');
    write_static_key(out, "selection");
    write_selection(out, &plan.selection);
    out.push(b',');
    write_static_key(out, "source");
    write_source(out, &plan.source);
    out.push(b'}');
}

fn write_source(out: &mut Vec<u8>, source: &Source) {
    out.push(b'{');
    write_static_key(out, "catalog");
    append_string(out, &source.catalog);
    out.push(b',');
    write_static_key(out, "schema");
    append_string(out, &source.schema);
    out.push(b',');
    write_static_key(out, "source_system_id");
    append_string(out, &source.source_system_id);
    out.push(b',');
    write_static_key(out, "table");
    append_string(out, &source.table);
    out.push(b'}');
}

fn write_input(out: &mut Vec<u8>, input: &InputContract) {
    let (kind, bronze_record_id_field) = match input {
        InputContract::VersionedRowsWithoutDeletes {
            bronze_record_id_field,
        } => ("versioned_rows_without_deletes", *bronze_record_id_field),
        InputContract::RowSnapshot {
            bronze_record_id_field,
        } => ("row_snapshot", *bronze_record_id_field),
    };
    out.push(b'{');
    write_static_key(out, "bronze_record_id_field");
    write_i64(out, i64::from(bronze_record_id_field));
    out.push(b',');
    write_static_key(out, "kind");
    append_string(out, kind);
    out.push(b'}');
}

fn write_columns(out: &mut Vec<u8>, columns: &[Column]) {
    out.push(b'[');
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        write_column(out, column);
    }
    out.push(b']');
}

fn write_column(out: &mut Vec<u8>, column: &Column) {
    out.push(b'{');
    write_static_key(out, "nullable");
    write_bool(out, column.nullable);
    out.push(b',');
    write_static_key(out, "source_field_id");
    write_i64(out, i64::from(column.source_field_id));
    out.push(b',');
    write_static_key(out, "target_name");
    append_string(out, &column.target_name);
    out.push(b',');
    write_static_key(out, "target_type");
    write_target_type(out, &column.target_type);
    out.push(b',');
    write_static_key(out, "transforms");
    write_transforms(out, &column.transforms);
    out.push(b',');
    write_static_key(out, "validators");
    write_validators(out, &column.validators);
    out.push(b'}');
}

fn write_target_type(out: &mut Vec<u8>, target: &TargetType) {
    out.push(b'{');
    match target {
        TargetType::Int32 => {
            write_static_key(out, "kind");
            append_string(out, "int32");
        }
        TargetType::Int64 => {
            write_static_key(out, "kind");
            append_string(out, "int64");
        }
        TargetType::Utf8 => {
            write_static_key(out, "kind");
            append_string(out, "utf8");
        }
        TargetType::Boolean => {
            write_static_key(out, "kind");
            append_string(out, "boolean");
        }
        TargetType::Date => {
            write_static_key(out, "kind");
            append_string(out, "date");
        }
        TargetType::Timestamp { unit, timezone } => {
            write_static_key(out, "kind");
            append_string(out, "timestamp");
            out.push(b',');
            write_static_key(out, "timezone");
            append_string(out, timezone_name(*timezone));
            out.push(b',');
            write_static_key(out, "unit");
            append_string(out, unit_name(unit));
        }
        TargetType::Decimal { precision, scale } => {
            write_static_key(out, "kind");
            append_string(out, "decimal");
            out.push(b',');
            write_static_key(out, "precision");
            write_u32(out, *precision);
            out.push(b',');
            write_static_key(out, "scale");
            write_u32(out, *scale);
        }
    }
    out.push(b'}');
}

fn write_transforms(out: &mut Vec<u8>, transforms: &[Transform]) {
    out.push(b'[');
    for (index, transform) in transforms.iter().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        write_transform(out, transform);
    }
    out.push(b']');
}

fn write_transform(out: &mut Vec<u8>, transform: &Transform) {
    out.push(b'{');
    match transform {
        Transform::Trim { characters } => {
            write_static_key(out, "characters");
            write_string_list(out, characters);
            out.push(b',');
            write_static_key(out, "kind");
            append_string(out, "trim");
        }
        Transform::EmptyToNull => {
            write_static_key(out, "kind");
            append_string(out, "empty_to_null");
        }
        Transform::ParseTimestamp {
            format_id,
            invalid_value,
        } => {
            write_static_key(out, "format_id");
            append_string(out, format_id_name(format_id));
            out.push(b',');
            write_static_key(out, "invalid_value");
            append_string(out, invalid_value_name(invalid_value));
            out.push(b',');
            write_static_key(out, "kind");
            append_string(out, "parse_timestamp");
        }
    }
    out.push(b'}');
}

fn write_validators(out: &mut Vec<u8>, validators: &[Validator]) {
    out.push(b'[');
    for (index, validator) in validators.iter().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        write_validator(out, validator);
    }
    out.push(b']');
}

fn write_validator(out: &mut Vec<u8>, validator: &Validator) {
    out.push(b'{');
    match validator {
        Validator::RequireNonNull { rule_id } => {
            write_static_key(out, "kind");
            append_string(out, "require_non_null");
            out.push(b',');
            write_static_key(out, "rule_id");
            append_string(out, rule_id);
        }
        Validator::CheckAllowedValues { rule_id, values } => {
            write_static_key(out, "kind");
            append_string(out, "check_allowed_values");
            out.push(b',');
            write_static_key(out, "rule_id");
            append_string(out, rule_id);
            out.push(b',');
            write_static_key(out, "values");
            write_string_list(out, values);
        }
    }
    out.push(b'}');
}

fn write_selection(out: &mut Vec<u8>, selection: &Selection) {
    out.push(b'{');
    match selection {
        Selection::Disabled => {
            write_static_key(out, "strategy");
            append_string(out, "disabled");
        }
        Selection::LatestSourceVersion {
            key_fields,
            order_fields,
            conflicting_tie,
            identical_source_payload_tie,
        } => {
            write_static_key(out, "conflicting_tie");
            append_string(out, conflicting_tie_name(conflicting_tie));
            out.push(b',');
            write_static_key(out, "identical_source_payload_tie");
            append_string(out, identical_tie_name(identical_source_payload_tie));
            out.push(b',');
            write_static_key(out, "key_fields");
            write_i32_list(out, key_fields);
            out.push(b',');
            write_static_key(out, "order_fields");
            write_order_fields(out, order_fields);
            out.push(b',');
            write_static_key(out, "strategy");
            append_string(out, "latest_source_version");
        }
    }
    out.push(b'}');
}

fn write_order_fields(out: &mut Vec<u8>, fields: &[OrderField]) {
    out.push(b'[');
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        out.push(b'{');
        write_static_key(out, "direction");
        append_string(out, direction_name(&field.direction));
        out.push(b',');
        write_static_key(out, "source_field_id");
        write_i64(out, i64::from(field.source_field_id));
        out.push(b'}');
    }
    out.push(b']');
}

fn write_quality_list(out: &mut Vec<u8>, quality: &[QualityRule]) {
    out.push(b'[');
    for (index, rule) in quality.iter().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        write_quality(out, rule);
    }
    out.push(b']');
}

fn write_quality(out: &mut Vec<u8>, rule: &QualityRule) {
    out.push(b'{');
    match rule {
        QualityRule::ExactDispositionAccounting { rule_id } => {
            write_static_key(out, "kind");
            append_string(out, "exact_disposition_accounting");
            out.push(b',');
            write_static_key(out, "rule_id");
            append_string(out, rule_id);
        }
        QualityRule::AcceptedKeyUniqueness { rule_id } => {
            write_static_key(out, "kind");
            append_string(out, "accepted_key_uniqueness");
            out.push(b',');
            write_static_key(out, "rule_id");
            append_string(out, rule_id);
        }
        QualityRule::RatioAtMost {
            rule_id,
            numerator,
            denominator,
            threshold,
            on_empty,
        } => {
            write_static_key(out, "denominator");
            append_string(out, population_name(denominator));
            out.push(b',');
            write_static_key(out, "kind");
            append_string(out, "ratio_at_most");
            out.push(b',');
            write_static_key(out, "numerator");
            append_string(out, population_name(numerator));
            out.push(b',');
            write_static_key(out, "on_empty");
            append_string(out, on_empty_name(on_empty));
            out.push(b',');
            write_static_key(out, "rule_id");
            append_string(out, rule_id);
            out.push(b',');
            write_static_key(out, "threshold");
            write_f64(out, *threshold);
        }
    }
    out.push(b'}');
}

fn write_publication(out: &mut Vec<u8>, publication: &Publication) {
    out.push(b'{');
    match publication {
        Publication::SingleClassifiedTable { empty_input } => {
            write_static_key(out, "empty_input");
            append_string(out, empty_input_name(empty_input));
            out.push(b',');
            write_static_key(out, "strategy");
            append_string(out, "single_classified_table");
        }
    }
    out.push(b'}');
}

fn write_string_list(out: &mut Vec<u8>, values: &[String]) {
    out.push(b'[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        append_string(out, value);
    }
    out.push(b']');
}

fn write_i32_list(out: &mut Vec<u8>, values: &[i32]) {
    out.push(b'[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        write_i64(out, i64::from(*value));
    }
    out.push(b']');
}

fn write_static_key(out: &mut Vec<u8>, key: &'static str) {
    append_string(out, key);
    out.push(b':');
}

fn write_bool(out: &mut Vec<u8>, value: bool) {
    if value {
        out.extend_from_slice(b"true");
    } else {
        out.extend_from_slice(b"false");
    }
}

fn write_i64(out: &mut Vec<u8>, value: i64) {
    let mut buffer = [0_u8; 20];
    let mut position = buffer.len();
    let mut remaining = value.unsigned_abs();
    if remaining == 0 {
        out.push(b'0');
        return;
    }
    while remaining > 0 {
        position -= 1;
        let digit = remaining % 10;
        remaining /= 10;
        buffer[position] = b'0' + u8::try_from(digit).unwrap_or_default();
    }
    if value < 0 {
        position -= 1;
        buffer[position] = b'-';
    }
    out.extend_from_slice(&buffer[position..]);
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    write_i64(out, i64::from(value));
}

fn write_f64(out: &mut Vec<u8>, value: f64) {
    let _ = write!(out, "{value}");
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

fn timezone_name(timezone: Timezone) -> &'static str {
    match timezone {
        Timezone::Utc => "UTC",
    }
}

fn unit_name(unit: &TimestampUnit) -> &'static str {
    match unit {
        TimestampUnit::Second => "second",
        TimestampUnit::Millisecond => "millisecond",
        TimestampUnit::Microsecond => "microsecond",
        TimestampUnit::Nanosecond => "nanosecond",
    }
}

fn format_id_name(format_id: &FormatId) -> &'static str {
    match format_id {
        FormatId::Iso8601SecondsOffsetV1 => "iso8601_seconds_offset_v1",
    }
}

fn invalid_value_name(value: &InvalidValue) -> &'static str {
    match value {
        InvalidValue::QuarantineRecord => "quarantine_record",
    }
}

fn direction_name(direction: &OrderDirection) -> &'static str {
    match direction {
        OrderDirection::Ascending => "ascending",
        OrderDirection::Descending => "descending",
    }
}

fn population_name(population: &RatioPopulation) -> &'static str {
    match population {
        RatioPopulation::UnkeyedQuarantinedRows => "unkeyed_quarantined_rows",
        RatioPopulation::InputRows => "input_rows",
        RatioPopulation::QuarantinedKeyedWinners => "quarantined_keyed_winners",
        RatioPopulation::KeyedWinners => "keyed_winners",
    }
}

fn on_empty_name(on_empty: &OnEmpty) -> &'static str {
    match on_empty {
        OnEmpty::Pass => "pass",
        OnEmpty::Fail => "fail",
    }
}

fn empty_input_name(empty_input: &EmptyInput) -> &'static str {
    match empty_input {
        EmptyInput::RequireExplicitEmptyReplaceOption => "require_explicit_empty_replace_option",
    }
}

fn conflicting_tie_name(tie: &ConflictingTie) -> &'static str {
    match tie {
        ConflictingTie::FailRun => "fail_run",
    }
}

fn identical_tie_name(tie: &IdenticalSourcePayloadTie) -> &'static str {
    match tie {
        IdenticalSourcePayloadTie::LowestBronzeRecordId => "lowest_bronze_record_id",
    }
}
