use std::fmt;
use std::sync::Arc;

use arrow::datatypes::DataType;
use repark_common::Error;

use crate::ident::IdentRefusal;
use crate::settings::{AUTH_METHOD_KEY, AuthMethod, DeclaredSetting, SpecRefusal, Spelling};

pub(crate) const REGISTRY: &str = "docs/spark-sql-iceberg-parity.md";

pub const UNMAPPED_ROW: &str = "CONNECT-DECL-pg-unmapped";

pub type Result<T> = std::result::Result<T, ConnectError>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConnectError {
    #[error(
        "invalid specification: `{AUTH_METHOD_KEY}` is `{value}`; expected one of: {}",
        AuthMethod::SPELLINGS.join(", ")
    )]
    InvalidAuthMethod { value: String },

    #[error(
        "auth method `{}` is declared but not supported yet: only `password` connects \
         (registry row {registry_row} in {REGISTRY})",
        method.spelling()
    )]
    DeclaredAuthMethod {
        method: AuthMethod,
        registry_row: &'static str,
    },

    #[error("invalid specification: {key} {reason}")]
    InvalidSpecification { key: Spelling, reason: SpecRefusal },

    #[error(
        "{key} is declared but not supported yet: {declared} (registry row {} in {REGISTRY})",
        declared.registry_row()
    )]
    DeclaredSetting {
        key: Spelling,
        declared: DeclaredSetting,
    },

    #[error("invalid specification: a Postgres identifier {reason}")]
    InvalidIdentifier { reason: IdentRefusal },

    #[error(
        "Postgres type `{postgres_name}` has no Arrow mapping: declared \
         (registry row {registry_row} in {REGISTRY})"
    )]
    Declared {
        postgres_name: &'static str,
        registry_row: &'static str,
    },

    #[error(
        "an Arrow {actual} array cannot encode as Postgres `{postgres_name}`, which maps to {expected}"
    )]
    ArrowType {
        postgres_name: &'static str,
        expected: DataType,
        actual: DataType,
    },

    #[error(
        "Postgres `{postgres_name}` wire value at row {index} is {actual} bytes; expected {expected}"
    )]
    WireLength {
        postgres_name: &'static str,
        index: usize,
        expected: usize,
        actual: usize,
    },

    #[error("Postgres `{postgres_name}` wire value at row {index} is not valid UTF-8")]
    InvalidUtf8 {
        postgres_name: &'static str,
        index: usize,
    },

    #[error(
        "column `{column}` has Postgres type `{postgres_type}`, which has no Arrow mapping \
         (registry row {row} in {REGISTRY}); select it through `query` with a cast, or a view"
    )]
    UnmappedType {
        column: Arc<str>,
        postgres_type: String,
        row: &'static str,
    },

    #[error(
        "column `{column}` (Postgres `{postgres_type}`) at row {index} holds {reason}, which \
         its Arrow type cannot hold exactly (registry row {} in {REGISTRY})",
        reason.registry_row()
    )]
    UnrepresentableValue {
        column: Arc<str>,
        postgres_type: &'static str,
        index: usize,
        reason: ValueRefusal,
    },

    #[error(
        "encoding Arrow values as Postgres `{postgres_name}` is not built: the write path (C-4) \
         adds it"
    )]
    EncodeNotBuilt { postgres_name: &'static str },

    #[error("malformed COPY BINARY stream: {violation}")]
    Protocol { violation: ProtocolViolation },

    #[error("the COPY BINARY stream ended before its trailer: the connection was lost")]
    Disconnected,

    #[error("an Arrow batch could not be built: {message}")]
    Arrow { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueRefusal {
    NumericNaN,
    NumericInfinity,
    NumericOutOfRange,
    InfiniteDate,
    InfiniteTimestamp,
    DateOutOfRange,
    TimestampOutOfRange,
}

impl ValueRefusal {
    #[must_use]
    pub fn registry_row(self) -> &'static str {
        match self {
            ValueRefusal::NumericNaN | ValueRefusal::NumericInfinity => {
                "CONNECT-DECL-pg-numeric-special"
            }
            ValueRefusal::InfiniteDate | ValueRefusal::InfiniteTimestamp => {
                "CONNECT-DECL-pg-infinite-datetime"
            }
            ValueRefusal::NumericOutOfRange
            | ValueRefusal::DateOutOfRange
            | ValueRefusal::TimestampOutOfRange => "CONNECT-DECL-pg-out-of-range",
        }
    }
}

impl fmt::Display for ValueRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ValueRefusal::NumericNaN => "`NaN`",
            ValueRefusal::NumericInfinity => "an infinite `numeric`",
            ValueRefusal::NumericOutOfRange => "a `numeric` beyond its decimal precision",
            ValueRefusal::InfiniteDate => "an infinite date",
            ValueRefusal::InfiniteTimestamp => "an infinite timestamp",
            ValueRefusal::DateOutOfRange => "a date beyond the 32-bit day count since 1970",
            ValueRefusal::TimestampOutOfRange => {
                "a timestamp after 294247-01-10, beyond microseconds since 1970 in 64 bits"
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolViolation {
    Signature,
    OidColumns,
    CriticalFlags { flags: u32 },
    HeaderExtension { length: i32 },
    FieldCount { expected: usize, actual: i16 },
    FieldLength { length: i32 },
    FieldTooLong { length: usize, max: usize },
    NullInNotNullColumn { column: usize },
    TrailingBytes,
    NumericSign { sign: u16 },
    NumericDigit { digit: i16 },
    JsonbVersion { found: Option<u8> },
}

impl fmt::Display for ProtocolViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolViolation::Signature => f.write_str("the header signature is not PGCOPY"),
            ProtocolViolation::OidColumns => {
                f.write_str("the header flags include OIDs (bit 16), which no scan requests")
            }
            ProtocolViolation::CriticalFlags { flags } => {
                write!(
                    f,
                    "the header sets reserved critical flag bits ({flags:#010x})"
                )
            }
            ProtocolViolation::HeaderExtension { length } => {
                write!(f, "the header extension length is negative ({length})")
            }
            ProtocolViolation::FieldCount { expected, actual } => {
                write!(
                    f,
                    "a tuple has {actual} fields; the scan planned {expected}"
                )
            }
            ProtocolViolation::FieldLength { length } => {
                write!(
                    f,
                    "a field length is negative ({length}) and not the NULL marker"
                )
            }
            ProtocolViolation::FieldTooLong { length, max } => {
                write!(
                    f,
                    "a field declares length {length}, beyond the {max}-byte maximum"
                )
            }
            ProtocolViolation::NullInNotNullColumn { column } => {
                write!(f, "a NULL arrived in planned NOT NULL column {column}")
            }
            ProtocolViolation::TrailingBytes => f.write_str("bytes follow the trailer"),
            ProtocolViolation::NumericSign { sign } => {
                write!(f, "a `numeric` sign word is {sign:#06x}")
            }
            ProtocolViolation::NumericDigit { digit } => {
                write!(f, "a `numeric` base-10000 digit is {digit}")
            }
            ProtocolViolation::JsonbVersion {
                found: Some(version),
            } => {
                write!(
                    f,
                    "a `jsonb` value has version {version}; only version 1 is known"
                )
            }
            ProtocolViolation::JsonbVersion { found: None } => {
                f.write_str("a `jsonb` value is empty, with no version byte")
            }
        }
    }
}

impl From<ConnectError> for Error {
    fn from(error: ConnectError) -> Self {
        match error {
            ConnectError::InvalidAuthMethod { .. }
            | ConnectError::InvalidSpecification { .. }
            | ConnectError::InvalidIdentifier { .. } => Error::Config(error.to_string()),
            ConnectError::DeclaredAuthMethod { .. }
            | ConnectError::DeclaredSetting { .. }
            | ConnectError::Declared { .. }
            | ConnectError::UnmappedType { .. }
            | ConnectError::UnrepresentableValue { .. }
            | ConnectError::EncodeNotBuilt { .. } => Error::NotImplemented(error.to_string()),
            ConnectError::ArrowType { .. }
            | ConnectError::WireLength { .. }
            | ConnectError::InvalidUtf8 { .. }
            | ConnectError::Protocol { .. }
            | ConnectError::Disconnected
            | ConnectError::Arrow { .. } => Error::DataFusion(error.to_string()),
        }
    }
}
