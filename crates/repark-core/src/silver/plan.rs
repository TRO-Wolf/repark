use serde::de::DeserializeOwned;

use super::explain;
use super::identity::{self, SilverPlanIdentity};
use super::policy::{
    ConflictingTie, EmptyInput, FormatId, IdenticalSourcePayloadTie, InputContract, InvalidValue,
    OnEmpty, OrderDirection, OrderField, Publication, QualityRule, RatioPopulation, Selection,
    TargetType, TimestampUnit, Timezone, Transform, Validator,
};
use super::refusal::SilverRefusal;

const ROOT_KEYS: &[&str] = &[
    "columns",
    "dataset_id",
    "input_contract",
    "plan_format_version",
    "publication",
    "quality",
    "selection",
    "source",
];
const SOURCE_KEYS: &[&str] = &["catalog", "schema", "source_system_id", "table"];
const COLUMN_KEYS: &[&str] = &[
    "nullable",
    "source_field_id",
    "target_name",
    "target_type",
    "transforms",
    "validators",
];
const SUPPORTED_PLAN_FORMAT_VERSION: i64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub source_system_id: String,
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub source_field_id: i32,
    pub target_name: String,
    pub target_type: TargetType,
    pub nullable: bool,
    pub transforms: Vec<Transform>,
    pub validators: Vec<Validator>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SilverPlan {
    pub plan_format_version: i64,
    pub dataset_id: String,
    pub source: Source,
    pub input_contract: InputContract,
    pub columns: Vec<Column>,
    pub selection: Selection,
    pub quality: Vec<QualityRule>,
    pub publication: Publication,
}

impl SilverPlan {
    pub fn parse(text: &str) -> Result<Self, SilverRefusal> {
        let document =
            toml::from_str::<toml::Table>(text).map_err(|error| SilverRefusal::TomlSyntax {
                message: error.message().to_string(),
            })?;
        let plan = parse_plan(&document)?;
        plan.validate()?;
        Ok(plan)
    }

    #[must_use]
    pub fn canonical(&self) -> Vec<u8> {
        identity::canonical_bytes(self)
    }

    #[must_use]
    pub fn explain(&self) -> String {
        explain::explain_plan(self)
    }

    #[must_use]
    pub fn identity(&self) -> SilverPlanIdentity {
        SilverPlanIdentity::from_bytes(self.canonical())
    }

    fn validate(&self) -> Result<(), SilverRefusal> {
        if self.plan_format_version != SUPPORTED_PLAN_FORMAT_VERSION {
            return Err(SilverRefusal::UnsupportedPlanFormatVersion {
                version: self.plan_format_version,
            });
        }
        if self.columns.is_empty() {
            return Err(SilverRefusal::EmptyColumns);
        }
        check_unique_columns(&self.columns)?;
        check_rule_ids(self)?;
        for (index, column) in self.columns.iter().enumerate() {
            validate_column(index, column)?;
        }
        validate_selection(&self.columns, &self.selection)?;
        validate_quality(&self.quality)?;
        Ok(())
    }
}

fn parse_plan(document: &toml::Table) -> Result<SilverPlan, SilverRefusal> {
    deny_unknown("", document, ROOT_KEYS)?;
    let plan_format_version = required_i64("", document, "plan_format_version")?;
    let dataset_id = required_string("", document, "dataset_id")?;
    let source = parse_source("source", required_value("", document, "source")?)?;
    let input_contract = parse_input_contract(
        "input_contract",
        required_value("", document, "input_contract")?,
    )?;
    let columns = parse_columns("columns", required_value("", document, "columns")?)?;
    let selection = parse_selection("selection", required_value("", document, "selection")?)?;
    let quality = parse_quality("quality", required_value("", document, "quality")?)?;
    let publication =
        parse_publication("publication", required_value("", document, "publication")?)?;
    Ok(SilverPlan {
        plan_format_version,
        dataset_id,
        source,
        input_contract,
        columns,
        selection,
        quality,
        publication,
    })
}

fn parse_source(path: &str, value: &toml::Value) -> Result<Source, SilverRefusal> {
    let table = expect_table(path, value)?;
    deny_unknown(path, table, SOURCE_KEYS)?;
    Ok(Source {
        source_system_id: required_string(path, table, "source_system_id")?,
        catalog: required_string(path, table, "catalog")?,
        schema: required_string(path, table, "schema")?,
        table: required_string(path, table, "table")?,
    })
}

fn parse_input_contract(path: &str, value: &toml::Value) -> Result<InputContract, SilverRefusal> {
    let table = expect_table(path, value)?;
    deny_unknown(path, table, &["bronze_record_id_field", "kind"])?;
    let kind = required_string(path, table, "kind")?;
    let bronze_record_id_field = required_i32(path, table, "bronze_record_id_field")?;
    match kind.as_str() {
        "versioned_rows_without_deletes" => Ok(InputContract::VersionedRowsWithoutDeletes {
            bronze_record_id_field,
        }),
        "row_snapshot" => Ok(InputContract::RowSnapshot {
            bronze_record_id_field,
        }),
        other => Err(SilverRefusal::UnknownKind {
            path: join_path(path, "kind"),
            value: other.to_string(),
        }),
    }
}

fn parse_columns(path: &str, value: &toml::Value) -> Result<Vec<Column>, SilverRefusal> {
    let array = expect_array(path, value)?;
    array
        .iter()
        .enumerate()
        .map(|(index, item)| parse_column(&index_path(path, index), item))
        .collect()
}

fn parse_column(path: &str, value: &toml::Value) -> Result<Column, SilverRefusal> {
    let table = expect_table(path, value)?;
    deny_unknown(path, table, COLUMN_KEYS)?;
    let source_field_id = required_i32(path, table, "source_field_id")?;
    let target_name = required_string(path, table, "target_name")?;
    let target_type = parse_target_type(
        &join_path(path, "target_type"),
        required_value(path, table, "target_type")?,
    )?;
    let nullable = required_bool(path, table, "nullable")?;
    let transforms = parse_transforms(
        &join_path(path, "transforms"),
        required_value(path, table, "transforms")?,
    )?;
    let validators = parse_validators(
        &join_path(path, "validators"),
        required_value(path, table, "validators")?,
    )?;
    Ok(Column {
        source_field_id,
        target_name,
        target_type,
        nullable,
        transforms,
        validators,
    })
}

fn parse_target_type(path: &str, value: &toml::Value) -> Result<TargetType, SilverRefusal> {
    let table = expect_table(path, value)?;
    let kind = required_string(path, table, "kind")?;
    match kind.as_str() {
        "int32" => {
            deny_unknown(path, table, &["kind"])?;
            Ok(TargetType::Int32)
        }
        "int64" => {
            deny_unknown(path, table, &["kind"])?;
            Ok(TargetType::Int64)
        }
        "utf8" => {
            deny_unknown(path, table, &["kind"])?;
            Ok(TargetType::Utf8)
        }
        "boolean" => {
            deny_unknown(path, table, &["kind"])?;
            Ok(TargetType::Boolean)
        }
        "date" => {
            deny_unknown(path, table, &["kind"])?;
            Ok(TargetType::Date)
        }
        "timestamp" => parse_timestamp_type(path, table),
        "decimal" => parse_decimal_type(path, table),
        other => Err(SilverRefusal::UnknownKind {
            path: join_path(path, "kind"),
            value: other.to_string(),
        }),
    }
}

fn parse_timestamp_type(path: &str, table: &toml::Table) -> Result<TargetType, SilverRefusal> {
    deny_unknown(path, table, &["kind", "timezone", "unit"])?;
    let unit_text = required_string(path, table, "unit")?;
    let unit = decode_enum::<TimestampUnit>(&unit_text).map_err(|value| {
        SilverRefusal::UnsupportedTimestampUnit {
            path: join_path(path, "unit"),
            value,
        }
    })?;
    let timezone_text = required_string(path, table, "timezone")?;
    if timezone_text != "UTC" {
        return Err(SilverRefusal::UnsupportedTimezone {
            path: join_path(path, "timezone"),
            value: timezone_text,
        });
    }
    Ok(TargetType::Timestamp {
        unit,
        timezone: Timezone::Utc,
    })
}

fn parse_decimal_type(path: &str, table: &toml::Table) -> Result<TargetType, SilverRefusal> {
    deny_unknown(path, table, &["kind", "precision", "scale"])?;
    let precision = required_i64(path, table, "precision")?;
    let scale = required_i64(path, table, "scale")?;
    if precision < 1 || scale < 0 || scale > precision {
        return Err(SilverRefusal::DecimalScaleInvalid {
            path: path.to_string(),
            precision,
            scale,
        });
    }
    let precision = u32::try_from(precision).map_err(|_| SilverRefusal::DecimalScaleInvalid {
        path: path.to_string(),
        precision,
        scale,
    })?;
    let scale = u32::try_from(scale).map_err(|_| SilverRefusal::DecimalScaleInvalid {
        path: path.to_string(),
        precision: i64::from(precision),
        scale,
    })?;
    Ok(TargetType::Decimal { precision, scale })
}

fn parse_transforms(path: &str, value: &toml::Value) -> Result<Vec<Transform>, SilverRefusal> {
    let array = expect_array(path, value)?;
    array
        .iter()
        .enumerate()
        .map(|(index, item)| parse_transform(&index_path(path, index), item))
        .collect()
}

fn parse_transform(path: &str, value: &toml::Value) -> Result<Transform, SilverRefusal> {
    let table = expect_table(path, value)?;
    let kind = required_string(path, table, "kind")?;
    match kind.as_str() {
        "trim" => {
            deny_unknown(path, table, &["characters", "kind"])?;
            Ok(Transform::Trim {
                characters: required_string_array(path, table, "characters")?,
            })
        }
        "empty_to_null" => {
            deny_unknown(path, table, &["kind"])?;
            Ok(Transform::EmptyToNull)
        }
        "parse_timestamp" => parse_parse_timestamp(path, table),
        other => Err(SilverRefusal::UnknownKind {
            path: join_path(path, "kind"),
            value: other.to_string(),
        }),
    }
}

fn parse_parse_timestamp(path: &str, table: &toml::Table) -> Result<Transform, SilverRefusal> {
    deny_unknown(path, table, &["format_id", "invalid_value", "kind"])?;
    let format_text = required_string(path, table, "format_id")?;
    let format_id = decode_enum::<FormatId>(&format_text).map_err(|value| {
        SilverRefusal::UnsupportedFormatId {
            path: join_path(path, "format_id"),
            value,
        }
    })?;
    let invalid_text = required_string(path, table, "invalid_value")?;
    let invalid_value =
        decode_enum::<InvalidValue>(&invalid_text).map_err(|value| SilverRefusal::UnknownKind {
            path: join_path(path, "invalid_value"),
            value,
        })?;
    Ok(Transform::ParseTimestamp {
        format_id,
        invalid_value,
    })
}

fn parse_validators(path: &str, value: &toml::Value) -> Result<Vec<Validator>, SilverRefusal> {
    let array = expect_array(path, value)?;
    array
        .iter()
        .enumerate()
        .map(|(index, item)| parse_validator(&index_path(path, index), item))
        .collect()
}

fn parse_validator(path: &str, value: &toml::Value) -> Result<Validator, SilverRefusal> {
    let table = expect_table(path, value)?;
    let kind = required_string(path, table, "kind")?;
    let rule_id = required_string(path, table, "rule_id")?;
    match kind.as_str() {
        "require_non_null" => {
            deny_unknown(path, table, &["kind", "rule_id"])?;
            Ok(Validator::RequireNonNull { rule_id })
        }
        "check_allowed_values" => {
            deny_unknown(path, table, &["kind", "rule_id", "values"])?;
            Ok(Validator::CheckAllowedValues {
                rule_id,
                values: required_string_array(path, table, "values")?,
            })
        }
        other => Err(SilverRefusal::UnknownKind {
            path: join_path(path, "kind"),
            value: other.to_string(),
        }),
    }
}

fn parse_selection(path: &str, value: &toml::Value) -> Result<Selection, SilverRefusal> {
    let table = expect_table(path, value)?;
    let strategy = required_string(path, table, "strategy")?;
    match strategy.as_str() {
        "disabled" => {
            deny_unknown(path, table, &["strategy"])?;
            Ok(Selection::Disabled)
        }
        "latest_source_version" => parse_latest_source_version(path, table),
        other => Err(SilverRefusal::UnknownKind {
            path: join_path(path, "strategy"),
            value: other.to_string(),
        }),
    }
}

fn parse_latest_source_version(
    path: &str,
    table: &toml::Table,
) -> Result<Selection, SilverRefusal> {
    deny_unknown(
        path,
        table,
        &[
            "conflicting_tie",
            "identical_source_payload_tie",
            "key_fields",
            "order_fields",
            "strategy",
        ],
    )?;
    let key_fields = required_i32_array(path, table, "key_fields")?;
    let order_fields = parse_order_fields(
        &join_path(path, "order_fields"),
        required_value(path, table, "order_fields")?,
    )?;
    let conflicting_text = required_string(path, table, "conflicting_tie")?;
    let conflicting_tie = decode_enum::<ConflictingTie>(&conflicting_text).map_err(|value| {
        SilverRefusal::UnknownConflictingTie {
            path: join_path(path, "conflicting_tie"),
            value,
        }
    })?;
    let identical_text = required_string(path, table, "identical_source_payload_tie")?;
    let identical_source_payload_tie = decode_enum::<IdenticalSourcePayloadTie>(&identical_text)
        .map_err(|value| SilverRefusal::UnknownKind {
            path: join_path(path, "identical_source_payload_tie"),
            value,
        })?;
    Ok(Selection::LatestSourceVersion {
        key_fields,
        order_fields,
        conflicting_tie,
        identical_source_payload_tie,
    })
}

fn parse_order_fields(path: &str, value: &toml::Value) -> Result<Vec<OrderField>, SilverRefusal> {
    let array = expect_array(path, value)?;
    array
        .iter()
        .enumerate()
        .map(|(index, item)| parse_order_field(&index_path(path, index), item))
        .collect()
}

fn parse_order_field(path: &str, value: &toml::Value) -> Result<OrderField, SilverRefusal> {
    let table = expect_table(path, value)?;
    deny_unknown(path, table, &["direction", "source_field_id"])?;
    let source_field_id = required_i32(path, table, "source_field_id")?;
    let direction_text = required_string(path, table, "direction")?;
    let direction = decode_enum::<OrderDirection>(&direction_text).map_err(|value| {
        SilverRefusal::UnknownKind {
            path: join_path(path, "direction"),
            value,
        }
    })?;
    Ok(OrderField {
        source_field_id,
        direction,
    })
}

fn parse_quality(path: &str, value: &toml::Value) -> Result<Vec<QualityRule>, SilverRefusal> {
    let array = expect_array(path, value)?;
    array
        .iter()
        .enumerate()
        .map(|(index, item)| parse_quality_rule(&index_path(path, index), item))
        .collect()
}

fn parse_quality_rule(path: &str, value: &toml::Value) -> Result<QualityRule, SilverRefusal> {
    let table = expect_table(path, value)?;
    let kind = required_string(path, table, "kind")?;
    let rule_id = required_string(path, table, "rule_id")?;
    match kind.as_str() {
        "exact_disposition_accounting" => {
            deny_unknown(path, table, &["kind", "rule_id"])?;
            Ok(QualityRule::ExactDispositionAccounting { rule_id })
        }
        "accepted_key_uniqueness" => {
            deny_unknown(path, table, &["kind", "rule_id"])?;
            Ok(QualityRule::AcceptedKeyUniqueness { rule_id })
        }
        "ratio_at_most" => parse_ratio_at_most(path, table, rule_id),
        other => Err(SilverRefusal::UnknownKind {
            path: join_path(path, "kind"),
            value: other.to_string(),
        }),
    }
}

fn parse_ratio_at_most(
    path: &str,
    table: &toml::Table,
    rule_id: String,
) -> Result<QualityRule, SilverRefusal> {
    deny_unknown(
        path,
        table,
        &[
            "denominator",
            "kind",
            "numerator",
            "on_empty",
            "rule_id",
            "threshold",
        ],
    )?;
    let numerator_text = required_string(path, table, "numerator")?;
    let numerator = decode_enum::<RatioPopulation>(&numerator_text).map_err(|value| {
        SilverRefusal::UnknownRatioPopulation {
            path: join_path(path, "numerator"),
            value,
        }
    })?;
    let denominator_text = required_string(path, table, "denominator")?;
    let denominator = decode_enum::<RatioPopulation>(&denominator_text).map_err(|value| {
        SilverRefusal::UnknownRatioPopulation {
            path: join_path(path, "denominator"),
            value,
        }
    })?;
    let threshold = required_f64(path, table, "threshold")?;
    let on_empty_text = required_string(path, table, "on_empty")?;
    let on_empty =
        decode_enum::<OnEmpty>(&on_empty_text).map_err(|value| SilverRefusal::UnknownKind {
            path: join_path(path, "on_empty"),
            value,
        })?;
    Ok(QualityRule::RatioAtMost {
        rule_id,
        numerator,
        denominator,
        threshold,
        on_empty,
    })
}

fn parse_publication(path: &str, value: &toml::Value) -> Result<Publication, SilverRefusal> {
    let table = expect_table(path, value)?;
    deny_unknown(path, table, &["empty_input", "strategy"])?;
    let strategy = required_string(path, table, "strategy")?;
    if strategy != "single_classified_table" {
        return Err(SilverRefusal::UnknownKind {
            path: join_path(path, "strategy"),
            value: strategy,
        });
    }
    let empty_text = required_string(path, table, "empty_input")?;
    let empty_input = decode_enum::<EmptyInput>(&empty_text).map_err(|value| {
        SilverRefusal::UnknownEmptyInput {
            path: join_path(path, "empty_input"),
            value,
        }
    })?;
    Ok(Publication::SingleClassifiedTable { empty_input })
}

fn check_unique_columns(columns: &[Column]) -> Result<(), SilverRefusal> {
    let mut names = std::collections::BTreeSet::new();
    let mut source_ids = std::collections::BTreeSet::new();
    for column in columns {
        if !names.insert(&column.target_name) {
            return Err(SilverRefusal::DuplicateTargetName {
                name: column.target_name.clone(),
            });
        }
        if !source_ids.insert(column.source_field_id) {
            return Err(SilverRefusal::DuplicateSourceFieldId {
                source_field_id: column.source_field_id,
            });
        }
    }
    Ok(())
}

fn check_rule_ids(plan: &SilverPlan) -> Result<(), SilverRefusal> {
    let mut seen = std::collections::BTreeSet::new();
    for column in &plan.columns {
        for validator in &column.validators {
            if !seen.insert(validator.rule_id().to_string()) {
                return Err(SilverRefusal::DuplicateRuleId {
                    rule_id: validator.rule_id().to_string(),
                });
            }
        }
    }
    for rule in &plan.quality {
        if !seen.insert(rule.rule_id().to_string()) {
            return Err(SilverRefusal::DuplicateRuleId {
                rule_id: rule.rule_id().to_string(),
            });
        }
    }
    Ok(())
}

fn validate_column(index: usize, column: &Column) -> Result<(), SilverRefusal> {
    validate_type_position(index, column)?;
    if !column.nullable
        && !column
            .validators
            .iter()
            .any(|validator| matches!(validator, Validator::RequireNonNull { .. }))
    {
        return Err(SilverRefusal::NonNullableWithoutRequireNonNull {
            target_name: column.target_name.clone(),
        });
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Utf8,
    Int32,
    Int64,
    Boolean,
    Date,
    Timestamp,
    Decimal,
}

fn target_stage(target: &TargetType) -> Stage {
    match target {
        TargetType::Int32 => Stage::Int32,
        TargetType::Int64 => Stage::Int64,
        TargetType::Utf8 => Stage::Utf8,
        TargetType::Boolean => Stage::Boolean,
        TargetType::Date => Stage::Date,
        TargetType::Timestamp { .. } => Stage::Timestamp,
        TargetType::Decimal { .. } => Stage::Decimal,
    }
}

fn validate_type_position(index: usize, column: &Column) -> Result<(), SilverRefusal> {
    let mut stage: Option<Stage> = None;
    for (transform_index, transform) in column.transforms.iter().enumerate() {
        let path = format!("columns[{index}].transforms[{transform_index}]");
        let next = match transform {
            Transform::Trim { .. } | Transform::EmptyToNull => Stage::Utf8,
            Transform::ParseTimestamp { .. } => Stage::Timestamp,
        };
        let allowed = stage.is_none() || stage == Some(Stage::Utf8);
        if !allowed {
            return Err(SilverRefusal::IllegalTransformPosition { path });
        }
        stage = Some(next);
    }
    if let Some(stage) = stage
        && stage != target_stage(&column.target_type)
    {
        return Err(SilverRefusal::IllegalTransformPosition {
            path: format!("columns[{index}]"),
        });
    }
    Ok(())
}

fn validate_selection(columns: &[Column], selection: &Selection) -> Result<(), SilverRefusal> {
    let Selection::LatestSourceVersion {
        key_fields,
        order_fields,
        ..
    } = selection
    else {
        return Ok(());
    };
    let mapped: std::collections::BTreeSet<i32> = columns
        .iter()
        .map(|column| column.source_field_id)
        .collect();
    for source_field_id in key_fields {
        if !mapped.contains(source_field_id) {
            return Err(SilverRefusal::UnmappedSelectionKey {
                source_field_id: *source_field_id,
            });
        }
    }
    for field in order_fields {
        if !mapped.contains(&field.source_field_id) {
            return Err(SilverRefusal::UnmappedOrderField {
                source_field_id: field.source_field_id,
            });
        }
    }
    Ok(())
}

fn validate_quality(quality: &[QualityRule]) -> Result<(), SilverRefusal> {
    for (index, rule) in quality.iter().enumerate() {
        if let QualityRule::RatioAtMost { threshold, .. } = rule {
            let path = format!("quality[{index}].threshold");
            if threshold.is_nan() {
                return Err(SilverRefusal::ThresholdNan { path });
            }
            if !(0.0..=1.0).contains(threshold) {
                return Err(SilverRefusal::ThresholdOutOfRange { path });
            }
        }
    }
    Ok(())
}

fn decode_enum<T: DeserializeOwned>(text: &str) -> Result<T, String> {
    T::deserialize(toml::Value::String(text.to_string())).map_err(|_| text.to_string())
}

fn deny_unknown(path: &str, table: &toml::Table, known: &[&str]) -> Result<(), SilverRefusal> {
    for key in table.keys() {
        if !known.contains(&key.as_str()) {
            return Err(SilverRefusal::UnknownKey {
                path: join_path(path, key),
            });
        }
    }
    Ok(())
}

fn required_value<'a>(
    path: &str,
    table: &'a toml::Table,
    key: &str,
) -> Result<&'a toml::Value, SilverRefusal> {
    table.get(key).ok_or_else(|| SilverRefusal::MissingField {
        path: join_path(path, key),
    })
}

fn required_string(path: &str, table: &toml::Table, key: &str) -> Result<String, SilverRefusal> {
    let value = required_value(path, table, key)?;
    value
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| SilverRefusal::InvalidParameter {
            path: join_path(path, key),
            message: "expected string".to_string(),
        })
}

fn required_bool(path: &str, table: &toml::Table, key: &str) -> Result<bool, SilverRefusal> {
    let value = required_value(path, table, key)?;
    value
        .as_bool()
        .ok_or_else(|| SilverRefusal::InvalidParameter {
            path: join_path(path, key),
            message: "expected boolean".to_string(),
        })
}

fn required_i32(path: &str, table: &toml::Table, key: &str) -> Result<i32, SilverRefusal> {
    integer_i32(&join_path(path, key), required_value(path, table, key)?)
}

fn required_i64(path: &str, table: &toml::Table, key: &str) -> Result<i64, SilverRefusal> {
    let value = required_value(path, table, key)?;
    value
        .as_integer()
        .ok_or_else(|| SilverRefusal::InvalidParameter {
            path: join_path(path, key),
            message: "expected integer".to_string(),
        })
}

fn required_f64(path: &str, table: &toml::Table, key: &str) -> Result<f64, SilverRefusal> {
    let value_path = join_path(path, key);
    match required_value(path, table, key)? {
        toml::Value::Float(float) => Ok(*float),
        toml::Value::Integer(0) => Ok(0.0),
        toml::Value::Integer(1) => Ok(1.0),
        _ => Err(SilverRefusal::InvalidParameter {
            path: value_path,
            message: "expected number".to_string(),
        }),
    }
}

fn required_string_array(
    path: &str,
    table: &toml::Table,
    key: &str,
) -> Result<Vec<String>, SilverRefusal> {
    let array = expect_array(&join_path(path, key), required_value(path, table, key)?)?;
    array
        .iter()
        .enumerate()
        .map(|(index, item)| {
            item.as_str()
                .map(ToString::to_string)
                .ok_or_else(|| SilverRefusal::InvalidParameter {
                    path: index_path(&join_path(path, key), index),
                    message: "expected string".to_string(),
                })
        })
        .collect()
}

fn required_i32_array(
    path: &str,
    table: &toml::Table,
    key: &str,
) -> Result<Vec<i32>, SilverRefusal> {
    let array = expect_array(&join_path(path, key), required_value(path, table, key)?)?;
    array
        .iter()
        .enumerate()
        .map(|(index, item)| integer_i32(&index_path(&join_path(path, key), index), item))
        .collect()
}

fn integer_i32(path: &str, value: &toml::Value) -> Result<i32, SilverRefusal> {
    let Some(integer) = value.as_integer() else {
        return Err(SilverRefusal::InvalidParameter {
            path: path.to_string(),
            message: "expected integer".to_string(),
        });
    };
    i32::try_from(integer).map_err(|_| SilverRefusal::InvalidParameter {
        path: path.to_string(),
        message: format!("{integer} does not fit i32"),
    })
}

fn expect_table<'a>(path: &str, value: &'a toml::Value) -> Result<&'a toml::Table, SilverRefusal> {
    value
        .as_table()
        .ok_or_else(|| SilverRefusal::InvalidParameter {
            path: path.to_string(),
            message: "expected table".to_string(),
        })
}

fn expect_array<'a>(
    path: &str,
    value: &'a toml::Value,
) -> Result<&'a Vec<toml::Value>, SilverRefusal> {
    value
        .as_array()
        .ok_or_else(|| SilverRefusal::InvalidParameter {
            path: path.to_string(),
            message: "expected array".to_string(),
        })
}

fn join_path(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}.{key}")
    }
}

fn index_path(prefix: &str, index: usize) -> String {
    format!("{prefix}[{index}]")
}
