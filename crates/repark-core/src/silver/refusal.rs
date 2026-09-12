use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum SilverRefusal {
    TomlSyntax {
        message: String,
    },
    UnknownKey {
        path: String,
    },
    UnknownKind {
        path: String,
        value: String,
    },
    MissingField {
        path: String,
    },
    UnsupportedPlanFormatVersion {
        version: i64,
    },
    UnsupportedFormatId {
        path: String,
        value: String,
    },
    DuplicateTargetName {
        name: String,
    },
    DuplicateSourceFieldId {
        source_field_id: i32,
    },
    UnmappedSelectionKey {
        source_field_id: i32,
    },
    UnmappedOrderField {
        source_field_id: i32,
    },
    IllegalTransformPosition {
        path: String,
    },
    NonNullableWithoutRequireNonNull {
        target_name: String,
    },
    ThresholdOutOfRange {
        path: String,
    },
    ThresholdNan {
        path: String,
    },
    UnsupportedTimezone {
        path: String,
        value: String,
    },
    UnsupportedTimestampUnit {
        path: String,
        value: String,
    },
    DecimalScaleInvalid {
        path: String,
        precision: i64,
        scale: i64,
    },
    EmptyColumns,
    DuplicateRuleId {
        rule_id: String,
    },
    UnknownRatioPopulation {
        path: String,
        value: String,
    },
    UnknownConflictingTie {
        path: String,
        value: String,
    },
    UnknownEmptyInput {
        path: String,
        value: String,
    },
    InvalidParameter {
        path: String,
        message: String,
    },
}

impl fmt::Display for SilverRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TomlSyntax { message } => {
                write!(formatter, "TOML parse error: {message}")
            }
            Self::UnknownKey { path } => write!(formatter, "unknown key `{path}`"),
            Self::UnknownKind { path, value } => {
                write!(formatter, "unknown kind `{value}` at `{path}`")
            }
            Self::MissingField { path } => write!(formatter, "missing field `{path}`"),
            Self::UnsupportedPlanFormatVersion { version } => {
                write!(formatter, "unsupported plan_format_version {version}")
            }
            Self::UnsupportedFormatId { path, value } => {
                write!(formatter, "unsupported format_id `{value}` at `{path}`")
            }
            Self::DuplicateTargetName { name } => {
                write!(formatter, "duplicate target_name `{name}`")
            }
            Self::DuplicateSourceFieldId { source_field_id } => {
                write!(formatter, "duplicate source_field_id {source_field_id}")
            }
            Self::UnmappedSelectionKey { source_field_id } => {
                write!(formatter, "unmapped selection key {source_field_id}")
            }
            Self::UnmappedOrderField { source_field_id } => {
                write!(formatter, "unmapped order field {source_field_id}")
            }
            Self::IllegalTransformPosition { path } => {
                write!(formatter, "illegal transform position at `{path}`")
            }
            Self::NonNullableWithoutRequireNonNull { target_name } => {
                write!(
                    formatter,
                    "non-nullable target `{target_name}` has no require_non_null path"
                )
            }
            Self::ThresholdOutOfRange { path } => {
                write!(formatter, "threshold at `{path}` is outside [0, 1]")
            }
            Self::ThresholdNan { path } => write!(formatter, "threshold at `{path}` is NaN"),
            Self::UnsupportedTimezone { path, value } => {
                write!(formatter, "unsupported timezone `{value}` at `{path}`")
            }
            Self::UnsupportedTimestampUnit { path, value } => {
                write!(
                    formatter,
                    "unsupported timestamp unit `{value}` at `{path}`"
                )
            }
            Self::DecimalScaleInvalid {
                path,
                precision,
                scale,
            } => write!(
                formatter,
                "decimal precision {precision} scale {scale} is invalid at `{path}`"
            ),
            Self::EmptyColumns => write!(formatter, "columns must not be empty"),
            Self::DuplicateRuleId { rule_id } => {
                write!(formatter, "duplicate rule_id `{rule_id}`")
            }
            Self::UnknownRatioPopulation { path, value } => {
                write!(formatter, "unknown ratio population `{value}` at `{path}`")
            }
            Self::UnknownConflictingTie { path, value } => {
                write!(formatter, "unknown conflicting_tie `{value}` at `{path}`")
            }
            Self::UnknownEmptyInput { path, value } => {
                write!(formatter, "unknown empty_input `{value}` at `{path}`")
            }
            Self::InvalidParameter { path, message } => {
                write!(formatter, "invalid parameter at `{path}`: {message}")
            }
        }
    }
}

impl std::error::Error for SilverRefusal {}
