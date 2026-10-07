mod copy_binary;
mod error;
mod ident;
#[cfg(feature = "postgres")]
mod pool;
mod settings;
#[cfg(feature = "postgres")]
mod tls;
mod types;

pub use copy_binary::{
    BatchLimits, COPY_SIGNATURE, CopyBinaryDecoder, DEFAULT_BATCH_BYTES, DEFAULT_BATCH_ROWS,
    MAX_BATCH_BYTES, MAX_FIELD_BYTES,
};
pub use error::{ConnectError, ProtocolViolation, Result, UNMAPPED_ROW, ValueRefusal};
pub use ident::{IdentRefusal, MAX_IDENT_BYTES, PgIdent, QualifiedRelation};
#[cfg(feature = "postgres")]
pub use pool::{
    Connect, PgConnection, PoolConnection, PoolLimits, PooledClient, PostgresConnector,
    PostgresPool, QueryPool, TimeoutSetting, query_config, within,
};
pub use settings::{
    AUTH_METHOD_KEY, AuthMethod, ConnectionSettings, DEFAULT_PORT, DeclaredSetting,
    POSTGRES_ALIASES, POSTGRES_DRIVER, POSTGRES_KEYS, PostgresSettings, SettingsDoor, SpecRefusal,
    Spelling, SslMode, UrlViolation, redact_source_prop,
};
#[cfg(feature = "postgres")]
pub use tls::{TlsFailure, verify_full_config};
pub use types::postgres;
