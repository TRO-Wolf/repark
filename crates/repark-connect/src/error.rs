use std::fmt;
use std::sync::Arc;
#[cfg(feature = "postgres")]
use std::{io, time::Duration};

use arrow::datatypes::DataType;
use repark_common::Error;

pub use crate::copy_binary::ProtocolViolation;
#[cfg(feature = "postgres")]
use crate::discover::{Privilege, SERVER_VERSION_ROW};
use crate::ident::IdentRefusal;
#[cfg(feature = "postgres")]
use crate::ident::QualifiedRelation;
use crate::partition::PartitionRefusal;
#[cfg(feature = "postgres")]
use crate::pool::TimeoutSetting;
use crate::settings::{AUTH_METHOD_KEY, AuthMethod, DeclaredSetting, SpecRefusal, Spelling};
#[cfg(feature = "postgres")]
use crate::tls::TlsFailure;

pub(crate) const REGISTRY: &str = "docs/spark-sql-iceberg-parity.md";

pub const UNMAPPED_ROW: &str = "CONNECT-DECL-pg-unmapped";

pub const DDL_ROW: &str = "CONNECT-DECL-pg-ddl";

pub const ZONE_ROW: &str = "CONNECT-DIV-pg-timestamp-zone";

#[must_use]
pub fn read_only_ddl(source: &str) -> String {
    format!(
        "database source `{source}` is read-only: DDL against it is not supported (registry \
         row {DDL_ROW} in {REGISTRY})"
    )
}

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

    #[error("{refusal}")]
    PartitionedRead { refusal: PartitionRefusal },

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

    #[error("a COPY BINARY field could not be buffered: the allocator refused its memory")]
    FieldBuffer,

    #[error("an Arrow batch could not be built: {message}")]
    Arrow { message: String },

    #[cfg(feature = "postgres")]
    #[error(
        "the Postgres server does not offer TLS and `sslmode` is `verify-full`; only \
         `sslmode=disable` connects in plaintext (registry row CONNECT-DIV-pg-sslmode in {REGISTRY})"
    )]
    TlsRequired,

    #[cfg(feature = "postgres")]
    #[error("TLS to the Postgres server failed: {kind}")]
    TlsHandshake { kind: TlsFailure },

    #[cfg(feature = "postgres")]
    #[error("the Postgres server is unreachable: {kind}")]
    Unreachable { kind: io::ErrorKind },

    #[cfg(feature = "postgres")]
    #[error("a Postgres wait exceeded `{which}`")]
    Timeout { which: TimeoutSetting },

    #[cfg(feature = "postgres")]
    #[error(
        "no pooled Postgres connection came free within `pool_checkout_timeout_ms` ({} ms); \
         raise it or `pool_max_size`",
        waited.as_millis()
    )]
    PoolExhausted { waited: Duration },

    #[cfg(feature = "postgres")]
    #[error("Postgres authentication failed: check `user`, `password` and `auth_method`")]
    AuthenticationFailed,

    #[cfg(feature = "postgres")]
    #[error("the Postgres server refused: {message} (SQLSTATE {sqlstate})")]
    Server { sqlstate: String, message: String },

    #[cfg(feature = "postgres")]
    #[error("the Postgres role lacks the {privilege} privilege that reading {relation} needs")]
    PermissionDenied {
        relation: QualifiedRelation,
        privilege: Privilege,
    },

    #[cfg(feature = "postgres")]
    #[error("Postgres relation {relation} does not exist (tables, views and foreign tables)")]
    RelationNotFound { relation: QualifiedRelation },

    #[cfg(feature = "postgres")]
    #[error(
        "Postgres server_version_num {server_version_num} < 140000: declared, {SERVER_VERSION_ROW}"
    )]
    DeclaredServerVersion { server_version_num: i32 },
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
    TimestampPastCalendar,
    WallClockGap,
    WallClockOverlap,
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
            | ValueRefusal::TimestampOutOfRange
            | ValueRefusal::TimestampPastCalendar => "CONNECT-DECL-pg-out-of-range",
            ValueRefusal::WallClockGap | ValueRefusal::WallClockOverlap => ZONE_ROW,
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
            ValueRefusal::TimestampPastCalendar => {
                "a wall clock whose instant in the session zone falls after \
                 +262142-12-31T23:59:59.999999 UTC, the end of the engine's calendar"
            }
            ValueRefusal::WallClockGap => {
                "a wall clock that a daylight-saving gap skips in the session zone; set \
                 `prefer_timestamp_ntz` to read the wall clock"
            }
            ValueRefusal::WallClockOverlap => {
                "a wall clock that a daylight-saving overlap repeats in the session zone; set \
                 `prefer_timestamp_ntz` to read the wall clock"
            }
        })
    }
}

impl From<ConnectError> for Error {
    fn from(error: ConnectError) -> Self {
        match error {
            ConnectError::InvalidAuthMethod { .. }
            | ConnectError::InvalidSpecification { .. }
            | ConnectError::InvalidIdentifier { .. } => Error::Config(error.to_string()),
            ConnectError::PartitionedRead { ref refusal } => match refusal {
                PartitionRefusal::DeclaredColumnType { .. }
                | PartitionRefusal::TooManyStrides { .. } => {
                    Error::NotImplemented(error.to_string())
                }
                PartitionRefusal::NotInteger { .. } => Error::NumberFormat(error.to_string()),
                PartitionRefusal::ColumnNotFound { .. }
                | PartitionRefusal::AmbiguousColumn { .. }
                | PartitionRefusal::ColumnType { .. } => Error::Analysis(error.to_string()),
                PartitionRefusal::Incomplete
                | PartitionRefusal::Reversed { .. }
                | PartitionRefusal::QueryOption
                | PartitionRefusal::Strides => Error::Config(error.to_string()),
            },
            ConnectError::DeclaredAuthMethod { .. }
            | ConnectError::DeclaredSetting { .. }
            | ConnectError::Declared { .. }
            | ConnectError::UnmappedType { .. }
            | ConnectError::UnrepresentableValue { .. }
            | ConnectError::EncodeNotBuilt { .. } => Error::NotImplemented(error.to_string()),
            #[cfg(feature = "postgres")]
            ConnectError::DeclaredServerVersion { .. } => Error::NotImplemented(error.to_string()),
            ConnectError::ArrowType { .. }
            | ConnectError::WireLength { .. }
            | ConnectError::InvalidUtf8 { .. }
            | ConnectError::Protocol { .. }
            | ConnectError::Disconnected
            | ConnectError::FieldBuffer
            | ConnectError::Arrow { .. } => Error::DataFusion(error.to_string()),
            #[cfg(feature = "postgres")]
            ConnectError::TlsRequired
            | ConnectError::TlsHandshake { .. }
            | ConnectError::Unreachable { .. }
            | ConnectError::Timeout { .. }
            | ConnectError::PoolExhausted { .. }
            | ConnectError::AuthenticationFailed
            | ConnectError::Server { .. }
            | ConnectError::PermissionDenied { .. }
            | ConnectError::RelationNotFound { .. } => Error::DataFusion(error.to_string()),
        }
    }
}
