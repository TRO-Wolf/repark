use std::error::Error as _;
use std::future::Future;
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use std::{fmt, io};

use rustls::pki_types::InvalidDnsNameError;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{AbortHandle, JoinHandle};
use tokio_postgres::config::SslMode as WireSslMode;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{CancelToken, Client, Config, NoTls, SimpleQueryMessage};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::error::{ConnectError, Result};
use crate::ident::PgIdent;
use crate::settings::{PostgresSettings, SslMode};
use crate::tls::{self, TlsFailure, TrackedTls};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutSetting {
    Connect,
    Read,
    Query,
    Lock,
}

impl TimeoutSetting {
    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            TimeoutSetting::Connect => "connect_timeout_ms",
            TimeoutSetting::Read => "read_timeout_ms",
            TimeoutSetting::Query => "query_timeout_ms",
            TimeoutSetting::Lock => "lock_timeout_ms",
        }
    }
}

impl fmt::Display for TimeoutSetting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.key())
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn within<T>(
    which: TimeoutSetting,
    limit: Duration,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(limit, work)
        .await
        .map_err(|_| ConnectError::Timeout { which })?
}

pub trait PoolConnection: Send + 'static {
    fn is_closed(&self) -> bool;

    fn abort_handle(&self) -> AbortHandle;

    fn reset(&self) -> impl Future<Output = bool> + Send {
        std::future::ready(true)
    }

    fn canceller(&self) -> Option<Canceller> {
        None
    }
}

#[derive(Clone)]
pub struct Canceller {
    token: CancelToken,
    tls: Option<MakeRustlsConnect>,
    limit: Duration,
}

impl Canceller {
    fn fire(self) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let Canceller { token, tls, limit } = self;
        let cancel = async move {
            match tls {
                Some(tls) => token.cancel_query(tls).await,
                None => token.cancel_query(NoTls).await,
            }
        };
        #[expect(
            clippy::disallowed_methods,
            reason = "the cancel request holds no client, lock or permit and is bounded by \
                      connect_timeout_ms; it outlives the abandoned lease so the server stops"
        )]
        let _cancel = tokio::spawn(async move {
            let _ = tokio::time::timeout(limit, cancel).await;
        });
    }
}

pub trait Connect: Send + Sync + 'static {
    type Connection: PoolConnection;

    fn connect(&self) -> impl Future<Output = Result<Self::Connection>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolLimits {
    pub max_size: usize,
    pub checkout_timeout: Duration,
    pub idle_timeout: Duration,
}

impl PoolLimits {
    #[must_use]
    pub fn from_settings(settings: &PostgresSettings) -> Self {
        Self {
            max_size: settings.pool_max_size,
            checkout_timeout: settings.pool_checkout_timeout,
            idle_timeout: settings.pool_idle_timeout,
        }
    }
}

struct Idle<T> {
    connection: T,
    since: Instant,
}

pub struct QueryPool<C: Connect> {
    connector: C,
    limits: PoolLimits,
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<Idle<C::Connection>>>,
}

impl<C: Connect> QueryPool<C> {
    #[must_use]
    pub fn new(connector: C, limits: PoolLimits) -> Arc<Self> {
        let permits = Semaphore::new(limits.max_size.min(Semaphore::MAX_PERMITS));
        Arc::new(Self {
            connector,
            limits,
            permits: Arc::new(permits),
            idle: Mutex::new(Vec::new()),
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn checkout(self: &Arc<Self>) -> Result<PooledClient<C>> {
        let waited = self.limits.checkout_timeout;
        let acquire = Arc::clone(&self.permits).acquire_owned();
        let Ok(Ok(permit)) = tokio::time::timeout(waited, acquire).await else {
            return Err(ConnectError::PoolExhausted { waited });
        };
        let connection = match self.take_idle() {
            Some(connection) => connection,
            None => self.connector.connect().await?,
        };
        Ok(PooledClient {
            lease: Lease {
                pool: Arc::clone(self),
                abort: Some(connection.abort_handle()),
                cancel: connection.canceller(),
                _permit: permit,
            },
            connection,
        })
    }

    #[must_use]
    pub fn idle_count(&self) -> usize {
        self.idle_slots().len()
    }

    fn idle_slots(&self) -> MutexGuard<'_, Vec<Idle<C::Connection>>> {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn take_idle(&self) -> Option<C::Connection> {
        let limit = self.limits.idle_timeout;
        let (reaped, reusable) = {
            let mut idle = self.idle_slots();
            let (live, reaped): (Vec<_>, Vec<_>) = idle
                .drain(..)
                .partition(|slot| slot.since.elapsed() < limit && !slot.connection.is_closed());
            *idle = live;
            (reaped, idle.pop())
        };
        drop(reaped);
        reusable.map(|slot| slot.connection)
    }

    fn put_idle(&self, connection: C::Connection) {
        if !connection.is_closed() {
            let since = Instant::now();
            self.idle_slots().push(Idle { connection, since });
        }
    }
}

struct Lease<C: Connect> {
    pool: Arc<QueryPool<C>>,
    abort: Option<AbortHandle>,
    cancel: Option<Canceller>,
    _permit: OwnedSemaphorePermit,
}

impl<C: Connect> Drop for Lease<C> {
    fn drop(&mut self) {
        if let Some(abort) = self.abort.take() {
            if let Some(cancel) = self.cancel.take() {
                cancel.fire();
            }
            abort.abort();
        }
    }
}

pub struct PooledClient<C: Connect> {
    lease: Lease<C>,
    connection: C::Connection,
}

impl<C: Connect> PooledClient<C> {
    pub async fn release_clean(self) {
        if !self.connection.reset().await {
            return;
        }
        let PooledClient {
            mut lease,
            connection,
        } = self;
        lease.abort = None;
        lease.cancel = None;
        lease.pool.put_idle(connection);
    }
}

impl<C: Connect> Deref for PooledClient<C> {
    type Target = C::Connection;

    fn deref(&self) -> &C::Connection {
        &self.connection
    }
}

pub const RESET_SESSION: &str = "CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ROLE; \
     RESET ALL; UNLISTEN *; SELECT pg_catalog.pg_advisory_unlock_all(); DISCARD PLANS; DISCARD TEMP; \
     DISCARD SEQUENCES";

const DRIVER_STATEMENT: &str = "^s[0-9]+$";

const FOREIGN_STATEMENTS: &str =
    "SELECT name FROM pg_catalog.pg_prepared_statements WHERE name OPERATOR(pg_catalog.!~) $1";

struct SessionPins {
    pins: Vec<(&'static str, String)>,
    login: String,
    check: String,
    timeout: Duration,
}

impl SessionPins {
    fn new(settings: &PostgresSettings) -> Self {
        let pins = session_pins(settings);
        let names: Vec<String> = pins.iter().map(|(key, _)| format!("'{key}'")).collect();
        let check = format!(
            "SELECT name, setting, \
             pg_catalog.statement_timestamp() = pg_catalog.transaction_timestamp(), \
             current_user::pg_catalog.text, session_user::pg_catalog.text, \
             (SELECT pg_catalog.count(*) FROM pg_catalog.pg_prepared_statements p \
              WHERE p.name OPERATOR(pg_catalog.!~) '{DRIVER_STATEMENT}') \
             FROM pg_catalog.pg_settings WHERE name IN ({})",
            names.join(", ")
        );
        Self {
            pins,
            login: settings.user.clone(),
            check,
            timeout: settings.read_timeout,
        }
    }

    fn hold(&self, shown: &[SimpleQueryMessage]) -> bool {
        let login = Some(self.login.as_str());
        let rows: Vec<(&str, &str, bool)> = shown
            .iter()
            .filter_map(|message| match message {
                SimpleQueryMessage::Row(row) => {
                    let cell = |index: usize| row.try_get(index).ok().flatten();
                    let settled = cell(2) == Some("t")
                        && cell(3) == login
                        && cell(4) == login
                        && cell(5) == Some("0");
                    Some((cell(0)?, cell(1)?, settled))
                }
                _ => None,
            })
            .collect();
        rows.len() == self.pins.len()
            && rows.iter().all(|(_, _, settled)| *settled)
            && self.pins.iter().all(|(key, value)| {
                rows.iter().any(|(name, setting, _)| {
                    name.eq_ignore_ascii_case(key) && as_set(key, setting) == value
                })
            })
    }
}

fn as_set<'a>(key: &str, setting: &'a str) -> &'a str {
    match (key, setting) {
        ("DateStyle", _) => setting.split(',').next().unwrap_or_default(),
        ("search_path", "\"\"") => "",
        _ => setting,
    }
}

pub struct PgConnection {
    client: Client,
    task: JoinHandle<()>,
    session: Arc<SessionPins>,
    cancel: Canceller,
}

impl PgConnection {
    fn spawn<S, T>(
        client: Client,
        connection: tokio_postgres::Connection<S, T>,
        session: Arc<SessionPins>,
        cancel: Canceller,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        #[expect(
            clippy::disallowed_methods,
            reason = "the connection task is tracked: PgConnection holds its handle, and a \
                      lease dropped before release_clean aborts it"
        )]
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        Self {
            client,
            task,
            session,
            cancel,
        }
    }

    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }
}

impl PoolConnection for PgConnection {
    fn is_closed(&self) -> bool {
        self.client.is_closed() || self.task.is_finished()
    }

    fn abort_handle(&self) -> AbortHandle {
        self.task.abort_handle()
    }

    async fn reset(&self) -> bool {
        let session = &self.session;
        let reset = async {
            self.client.batch_execute(RESET_SESSION).await?;
            let pattern: [(&(dyn ToSql + Sync), Type); 1] = [(&DRIVER_STATEMENT, Type::TEXT)];
            let foreign = self
                .client
                .query_typed(FOREIGN_STATEMENTS, &pattern)
                .await?;
            let deallocate: Vec<String> = foreign
                .iter()
                .filter_map(|row| PgIdent::new(row.try_get::<_, String>(0).ok()?).ok())
                .map(|name| format!("DEALLOCATE {name}"))
                .collect();
            if !deallocate.is_empty() {
                self.client.batch_execute(&deallocate.join("; ")).await?;
            }
            self.client.simple_query(&session.check).await
        };
        match tokio::time::timeout(session.timeout, reset).await {
            Ok(Ok(shown)) => session.hold(&shown),
            _ => false,
        }
    }

    fn canceller(&self) -> Option<Canceller> {
        Some(self.cancel.clone())
    }
}

pub const CONNECTION_CHECK_INTERVAL: Duration = Duration::from_secs(1);

fn session_pins(settings: &PostgresSettings) -> Vec<(&'static str, String)> {
    let millis = |limit: Duration| limit.as_millis().to_string();
    vec![
        ("client_encoding", "UTF8".to_string()),
        ("DateStyle", "ISO".to_string()),
        ("IntervalStyle", "postgres".to_string()),
        ("TimeZone", "UTC".to_string()),
        ("search_path", String::new()),
        ("default_transaction_read_only", "on".to_string()),
        ("lock_timeout", millis(settings.lock_timeout)),
        (
            "statement_timeout",
            millis(settings.query_timeout.unwrap_or_default()),
        ),
        (
            "idle_in_transaction_session_timeout",
            millis(settings.read_timeout),
        ),
        (
            "client_connection_check_interval",
            millis(CONNECTION_CHECK_INTERVAL),
        ),
    ]
}

#[must_use]
pub fn query_config(settings: &PostgresSettings) -> Config {
    let wire_sslmode = match settings.sslmode {
        SslMode::Disable => WireSslMode::Disable,
        _ => WireSslMode::Require,
    };
    let options: Vec<String> = session_pins(settings)
        .iter()
        .map(|(key, value)| format!("-c {key}={value}"))
        .collect();
    let mut config = Config::new();
    config
        .host(&settings.host)
        .port(settings.port)
        .user(&settings.user)
        .dbname(&settings.database)
        .application_name(&settings.application_name)
        .options(options.join(" "))
        .connect_timeout(settings.connect_timeout)
        .keepalives(true)
        .ssl_mode(wire_sslmode);
    if let Some(password) = &settings.password {
        config.password(password);
    }
    config
}

fn classify(error: &tokio_postgres::Error, tls_attempted: Option<bool>) -> ConnectError {
    if let Some(db) = error.as_db_error() {
        if db.code().code().starts_with("28") {
            return ConnectError::AuthenticationFailed;
        }
        return ConnectError::Server {
            sqlstate: db.code().code().to_string(),
            message: db.message().to_string(),
        };
    }
    let mut cause = error.source();
    while let Some(current) = cause {
        let rustls_error = current
            .downcast_ref::<io::Error>()
            .and_then(|io_error| io_error.get_ref())
            .and_then(|inner| inner.downcast_ref::<rustls::Error>())
            .or_else(|| current.downcast_ref::<rustls::Error>());
        if let Some(rustls_error) = rustls_error {
            let kind = tls::failure_of(rustls_error);
            return ConnectError::TlsHandshake { kind };
        }
        if current.is::<InvalidDnsNameError>() {
            let kind = TlsFailure::ServerName;
            return ConnectError::TlsHandshake { kind };
        }
        if let Some(io_error) = current.downcast_ref::<io::Error>() {
            return match io_error.kind() {
                io::ErrorKind::TimedOut => ConnectError::Timeout {
                    which: TimeoutSetting::Connect,
                },
                kind => ConnectError::Unreachable { kind },
            };
        }
        cause = current.source();
    }
    match tls_attempted {
        Some(false) => ConnectError::TlsRequired,
        _ if error.is_closed() => ConnectError::Unreachable {
            kind: io::ErrorKind::UnexpectedEof,
        },
        _ => ConnectError::AuthenticationFailed,
    }
}

pub struct PostgresConnector {
    config: Config,
    tls: Option<MakeRustlsConnect>,
    connect_timeout: Duration,
    session: Arc<SessionPins>,
}

impl PostgresConnector {
    #[allow(clippy::missing_errors_doc)]
    pub fn new(settings: &PostgresSettings) -> Result<Self> {
        let tls = if settings.sslmode == SslMode::Disable {
            None
        } else {
            let config = tls::verify_full_config(settings.sslrootcert.as_deref())?;
            Some(MakeRustlsConnect::new(config))
        };
        Ok(Self {
            config: query_config(settings),
            tls,
            connect_timeout: settings.connect_timeout,
            session: Arc::new(SessionPins::new(settings)),
        })
    }

    fn canceller(&self, client: &Client) -> Canceller {
        Canceller {
            token: client.cancel_token(),
            tls: self.tls.clone(),
            limit: self.connect_timeout,
        }
    }

    async fn open(&self) -> Result<PgConnection> {
        let Some(tls) = &self.tls else {
            let connecting = self.config.connect(NoTls).await;
            let (client, connection) = connecting.map_err(|error| classify(&error, None))?;
            let session = Arc::clone(&self.session);
            let cancel = self.canceller(&client);
            return Ok(PgConnection::spawn(client, connection, session, cancel));
        };
        let tracked = TrackedTls::new(tls.clone());
        let connecting = self.config.connect(tracked.clone()).await;
        let (client, connection) =
            connecting.map_err(|error| classify(&error, Some(tracked.handshake_attempted())))?;
        let session = Arc::clone(&self.session);
        let cancel = self.canceller(&client);
        Ok(PgConnection::spawn(client, connection, session, cancel))
    }
}

impl Connect for PostgresConnector {
    type Connection = PgConnection;

    async fn connect(&self) -> Result<PgConnection> {
        within(TimeoutSetting::Connect, self.connect_timeout, self.open()).await
    }
}

pub type PostgresPool = QueryPool<PostgresConnector>;
