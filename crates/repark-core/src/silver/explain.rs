use std::fmt::Write as _;

use super::plan::{Column, SilverPlan, Source};
use super::policy::{
    ConflictingTie, EmptyInput, FormatId, IdenticalSourcePayloadTie, InputContract, InvalidValue,
    OnEmpty, OrderDirection, OrderField, Publication, QualityRule, RatioPopulation, Selection,
    TargetType, TimestampUnit, Timezone, Transform, Validator,
};

pub(crate) fn explain_plan(plan: &SilverPlan) -> String {
    let identity = plan.identity();
    explain_with_identity(plan, &identity)
}

pub(crate) fn explain_with_identity(
    plan: &SilverPlan,
    identity: &super::SilverPlanIdentity,
) -> String {
    let mut out = String::new();
    let canonical_text = std::str::from_utf8(identity.canonical()).unwrap_or("invalid-utf8");
    write_line(&mut out, format_args!("silver_plan"));
    write_line(&mut out, format_args!("canonical={canonical_text}"));
    write_line(
        &mut out,
        format_args!("plan_format_version={}", plan.plan_format_version),
    );
    write_line(&mut out, format_args!("dataset_id={}", plan.dataset_id));
    explain_source(&mut out, &plan.source);
    explain_input(&mut out, &plan.input_contract);
    for (index, column) in plan.columns.iter().enumerate() {
        explain_column(&mut out, index, column);
    }
    explain_selection(&mut out, &plan.selection);
    for (index, rule) in plan.quality.iter().enumerate() {
        explain_quality(&mut out, index, rule);
    }
    explain_publication(&mut out, &plan.publication);
    out
}

fn write_line(out: &mut String, args: std::fmt::Arguments<'_>) {
    let _ = out.write_fmt(args);
    out.push('\n');
}

fn explain_source(out: &mut String, source: &Source) {
    write_line(
        out,
        format_args!("source.source_system_id={}", source.source_system_id),
    );
    write_line(out, format_args!("source.catalog={}", source.catalog));
    write_line(out, format_args!("source.schema={}", source.schema));
    write_line(out, format_args!("source.table={}", source.table));
}

fn explain_input(out: &mut String, input: &InputContract) {
    match input {
        InputContract::VersionedRowsWithoutDeletes {
            bronze_record_id_field,
        } => {
            write_line(
                out,
                format_args!("input.kind=versioned_rows_without_deletes"),
            );
            write_line(
                out,
                format_args!("input.bronze_record_id_field={bronze_record_id_field}"),
            );
        }
        InputContract::RowSnapshot {
            bronze_record_id_field,
        } => {
            write_line(out, format_args!("input.kind=row_snapshot"));
            write_line(
                out,
                format_args!("input.bronze_record_id_field={bronze_record_id_field}"),
            );
        }
    }
}

fn explain_column(out: &mut String, index: usize, column: &Column) {
    write_line(
        out,
        format_args!("column[{index}].source_field_id={}", column.source_field_id),
    );
    write_line(
        out,
        format_args!("column[{index}].target_name={}", column.target_name),
    );
    write_line(
        out,
        format_args!(
            "column[{index}].target_type={}",
            target_type_text(&column.target_type)
        ),
    );
    write_line(
        out,
        format_args!("column[{index}].nullable={}", column.nullable),
    );
    write_line(
        out,
        format_args!(
            "column[{index}].transforms={}",
            transforms_text(&column.transforms)
        ),
    );
    write_line(
        out,
        format_args!(
            "column[{index}].validators={}",
            validators_text(&column.validators)
        ),
    );
}

fn target_type_text(target: &TargetType) -> String {
    match target {
        TargetType::Int32 => "int32".to_string(),
        TargetType::Int64 => "int64".to_string(),
        TargetType::Utf8 => "utf8".to_string(),
        TargetType::Boolean => "boolean".to_string(),
        TargetType::Date => "date".to_string(),
        TargetType::Timestamp { unit, timezone } => {
            format!(
                "timestamp(unit={}, timezone={})",
                timestamp_unit_text(unit),
                timezone_text(*timezone)
            )
        }
        TargetType::Decimal { precision, scale } => {
            format!("decimal(precision={precision}, scale={scale})")
        }
    }
}

fn transforms_text(transforms: &[Transform]) -> String {
    if transforms.is_empty() {
        return String::new();
    }
    transforms
        .iter()
        .map(transform_text)
        .collect::<Vec<_>>()
        .join("; ")
}

fn transform_text(transform: &Transform) -> String {
    match transform {
        Transform::Trim { characters } => {
            format!("trim(characters={})", list_text(characters))
        }
        Transform::EmptyToNull => "empty_to_null".to_string(),
        Transform::ParseTimestamp {
            format_id,
            invalid_value,
        } => format!(
            "parse_timestamp(format_id={}, invalid_value={})",
            format_id_text(format_id),
            invalid_value_text(invalid_value)
        ),
    }
}

fn validators_text(validators: &[Validator]) -> String {
    if validators.is_empty() {
        return String::new();
    }
    validators
        .iter()
        .map(validator_text)
        .collect::<Vec<_>>()
        .join("; ")
}

fn validator_text(validator: &Validator) -> String {
    match validator {
        Validator::RequireNonNull { rule_id } => {
            format!("require_non_null(rule_id={rule_id})")
        }
        Validator::CheckAllowedValues { rule_id, values } => {
            format!(
                "check_allowed_values(rule_id={rule_id}, values={})",
                list_text(values)
            )
        }
    }
}

fn explain_selection(out: &mut String, selection: &Selection) {
    match selection {
        Selection::Disabled => write_line(out, format_args!("selection.strategy=disabled")),
        Selection::LatestSourceVersion {
            key_fields,
            order_fields,
            conflicting_tie,
            identical_source_payload_tie,
        } => {
            write_line(
                out,
                format_args!("selection.strategy=latest_source_version"),
            );
            write_line(
                out,
                format_args!("selection.key_fields={}", int_list_text(key_fields)),
            );
            write_line(
                out,
                format_args!("selection.order_fields={}", order_fields_text(order_fields)),
            );
            write_line(
                out,
                format_args!(
                    "selection.conflicting_tie={}",
                    conflicting_tie_text(conflicting_tie)
                ),
            );
            write_line(
                out,
                format_args!(
                    "selection.identical_source_payload_tie={}",
                    identical_tie_text(identical_source_payload_tie)
                ),
            );
        }
    }
}

fn explain_quality(out: &mut String, index: usize, rule: &QualityRule) {
    match rule {
        QualityRule::ExactDispositionAccounting { rule_id } => {
            write_line(
                out,
                format_args!("quality[{index}]=exact_disposition_accounting(rule_id={rule_id})"),
            );
        }
        QualityRule::AcceptedKeyUniqueness { rule_id } => {
            write_line(
                out,
                format_args!("quality[{index}]=accepted_key_uniqueness(rule_id={rule_id})"),
            );
        }
        QualityRule::RatioAtMost {
            rule_id,
            numerator,
            denominator,
            threshold,
            on_empty,
        } => {
            write_line(
                out,
                format_args!(
                    "quality[{index}]=ratio_at_most(rule_id={rule_id}, numerator={}, denominator={}, threshold={threshold}, on_empty={})",
                    population_text(numerator),
                    population_text(denominator),
                    on_empty_text(on_empty)
                ),
            );
        }
    }
}

fn explain_publication(out: &mut String, publication: &Publication) {
    match publication {
        Publication::SingleClassifiedTable { empty_input } => {
            write_line(
                out,
                format_args!("publication.strategy=single_classified_table"),
            );
            write_line(
                out,
                format_args!("publication.empty_input={}", empty_input_text(empty_input)),
            );
        }
    }
}

fn list_text(values: &[String]) -> String {
    let inner = values
        .iter()
        .map(|value| format!("\"{value}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{inner}]")
}

fn int_list_text(values: &[i32]) -> String {
    let inner = values
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{inner}]")
}

fn order_fields_text(fields: &[OrderField]) -> String {
    let inner = fields
        .iter()
        .map(|field| {
            format!(
                "{} {}",
                field.source_field_id,
                direction_text(&field.direction)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{inner}]")
}

fn timestamp_unit_text(unit: &TimestampUnit) -> &'static str {
    match unit {
        TimestampUnit::Second => "second",
        TimestampUnit::Millisecond => "millisecond",
        TimestampUnit::Microsecond => "microsecond",
        TimestampUnit::Nanosecond => "nanosecond",
    }
}

fn timezone_text(timezone: Timezone) -> &'static str {
    match timezone {
        Timezone::Utc => "UTC",
    }
}

fn format_id_text(format_id: &FormatId) -> &'static str {
    match format_id {
        FormatId::Iso8601SecondsOffsetV1 => "iso8601_seconds_offset_v1",
    }
}

fn invalid_value_text(value: &InvalidValue) -> &'static str {
    match value {
        InvalidValue::QuarantineRecord => "quarantine_record",
    }
}

fn direction_text(direction: &OrderDirection) -> &'static str {
    match direction {
        OrderDirection::Ascending => "ascending",
        OrderDirection::Descending => "descending",
    }
}

fn conflicting_tie_text(tie: &ConflictingTie) -> &'static str {
    match tie {
        ConflictingTie::FailRun => "fail_run",
    }
}

fn identical_tie_text(tie: &IdenticalSourcePayloadTie) -> &'static str {
    match tie {
        IdenticalSourcePayloadTie::LowestBronzeRecordId => "lowest_bronze_record_id",
    }
}

fn population_text(population: &RatioPopulation) -> &'static str {
    match population {
        RatioPopulation::UnkeyedQuarantinedRows => "unkeyed_quarantined_rows",
        RatioPopulation::InputRows => "input_rows",
        RatioPopulation::QuarantinedKeyedWinners => "quarantined_keyed_winners",
        RatioPopulation::KeyedWinners => "keyed_winners",
    }
}

fn on_empty_text(on_empty: &OnEmpty) -> &'static str {
    match on_empty {
        OnEmpty::Pass => "pass",
        OnEmpty::Fail => "fail",
    }
}

fn empty_input_text(empty_input: &EmptyInput) -> &'static str {
    match empty_input {
        EmptyInput::RequireExplicitEmptyReplaceOption => "require_explicit_empty_replace_option",
    }
}
