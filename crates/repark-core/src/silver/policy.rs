use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormatId {
    Iso8601SecondsOffsetV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidValue {
    QuarantineRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timezone {
    Utc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetType {
    Int32,
    Int64,
    Utf8,
    Boolean,
    Date,
    Timestamp {
        unit: TimestampUnit,
        timezone: Timezone,
    },
    Decimal {
        precision: u32,
        scale: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transform {
    Trim {
        characters: Vec<String>,
    },
    EmptyToNull,
    ParseTimestamp {
        format_id: FormatId,
        invalid_value: InvalidValue,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Validator {
    RequireNonNull {
        rule_id: String,
    },
    CheckAllowedValues {
        rule_id: String,
        values: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderField {
    pub source_field_id: i32,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictingTie {
    FailRun,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdenticalSourcePayloadTie {
    LowestBronzeRecordId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    Disabled,
    LatestSourceVersion {
        key_fields: Vec<i32>,
        order_fields: Vec<OrderField>,
        conflicting_tie: ConflictingTie,
        identical_source_payload_tie: IdenticalSourcePayloadTie,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RatioPopulation {
    UnkeyedQuarantinedRows,
    InputRows,
    QuarantinedKeyedWinners,
    KeyedWinners,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnEmpty {
    Pass,
    Fail,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QualityRule {
    ExactDispositionAccounting {
        rule_id: String,
    },
    AcceptedKeyUniqueness {
        rule_id: String,
    },
    RatioAtMost {
        rule_id: String,
        numerator: RatioPopulation,
        denominator: RatioPopulation,
        threshold: f64,
        on_empty: OnEmpty,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmptyInput {
    RequireExplicitEmptyReplaceOption,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Publication {
    SingleClassifiedTable { empty_input: EmptyInput },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputContract {
    VersionedRowsWithoutDeletes { bronze_record_id_field: i32 },
    RowSnapshot { bronze_record_id_field: i32 },
}

impl Validator {
    #[must_use]
    pub fn rule_id(&self) -> &str {
        match self {
            Self::RequireNonNull { rule_id } | Self::CheckAllowedValues { rule_id, .. } => rule_id,
        }
    }
}

impl QualityRule {
    #[must_use]
    pub fn rule_id(&self) -> &str {
        match self {
            Self::ExactDispositionAccounting { rule_id }
            | Self::AcceptedKeyUniqueness { rule_id }
            | Self::RatioAtMost { rule_id, .. } => rule_id,
        }
    }
}
