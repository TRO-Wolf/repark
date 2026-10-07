mod copy_binary;
#[cfg(feature = "postgres")]
mod discover;
mod error;
mod ident;
#[cfg(feature = "postgres")]
mod pool;
#[cfg(feature = "postgres")]
mod read;
mod settings;
#[cfg(feature = "postgres")]
mod tls;
mod types;

pub use copy_binary::{
    BatchLimits, COPY_SIGNATURE, CopyBinaryDecoder, DEFAULT_BATCH_BYTES, DEFAULT_BATCH_ROWS,
    MAX_BATCH_BYTES, MAX_FIELD_BYTES,
};
#[cfg(feature = "postgres")]
pub use discover::{
    BEGIN_DISCOVERY, CastType, ColumnCollation, MIN_SERVER_VERSION_NUM, Privilege, QUERY_ALIAS,
    QUERY_SEARCH_PATH, ResolvedSource, SERVER_VERSION_ROW, ScanColumn, ScanSource,
    check_server_version, discover,
};
pub use error::{ConnectError, ProtocolViolation, Result, UNMAPPED_ROW, ValueRefusal};
pub use ident::{DEFAULT_SCHEMA, IdentRefusal, MAX_IDENT_BYTES, PgIdent, QualifiedRelation};
#[cfg(feature = "postgres")]
pub use pool::{
    CONNECTION_CHECK_INTERVAL, Canceller, Connect, PgConnection, PoolConnection, PoolLimits,
    PooledClient, PostgresConnector, PostgresPool, QueryPool, RESET_SESSION, TimeoutSetting,
    query_config, within,
};
#[cfg(feature = "postgres")]
pub use read::postgres::{
    BEGIN_SCAN, CompareOp, MAX_PARAM_SLOTS, ParamSlot, ScanOptions, ScanRequest, ScanStatement,
    scan,
};
pub use settings::{
    AUTH_METHOD_KEY, AuthMethod, ConnectionSettings, DEFAULT_PORT, DeclaredSetting,
    POSTGRES_ALIASES, POSTGRES_DRIVER, POSTGRES_KEYS, PostgresSettings, SettingsDoor, SpecRefusal,
    Spelling, SslMode, UrlViolation, redact_source_prop,
};
#[cfg(feature = "postgres")]
pub use tls::{TlsFailure, verify_full_config};
pub use types::postgres;
