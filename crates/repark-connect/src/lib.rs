mod copy_binary;
mod error;
mod ident;
mod settings;
mod types;

pub use copy_binary::{
    BatchLimits, COPY_SIGNATURE, CopyBinaryDecoder, DEFAULT_BATCH_BYTES, DEFAULT_BATCH_ROWS,
    MAX_BATCH_BYTES, MAX_FIELD_BYTES,
};
pub use error::{ConnectError, ProtocolViolation, Result, UNMAPPED_ROW, ValueRefusal};
pub use ident::{IdentRefusal, MAX_IDENT_BYTES, PgIdent, QualifiedRelation};
pub use settings::{
    AUTH_METHOD_KEY, AuthMethod, ConnectionSettings, DEFAULT_PORT, DeclaredSetting,
    POSTGRES_ALIASES, POSTGRES_DRIVER, POSTGRES_KEYS, PostgresSettings, SettingsDoor, SpecRefusal,
    Spelling, SslMode, UrlViolation, redact_source_prop,
};
pub use types::postgres;
