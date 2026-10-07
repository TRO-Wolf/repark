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
use tokio_postgres::{Client, Config, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::error::{ConnectError, Result};
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
    _permit: OwnedSemaphorePermit,
}

impl<C: Connect> Drop for Lease<C> {
    fn drop(&mut self) {
        if let Some(abort) = self.abort.take() {
            abort.abort();
        }
    }
}

pub struct PooledClient<C: Connect> {
    lease: Lease<C>,
    connection: C::Connection,
}

impl<C: Connect> PooledClient<C> {
    pub fn release_clean(self) {
        let PooledClient {
            mut lease,
            connection,
        } = self;
        lease.abort = None;
        lease.pool.put_idle(connection);
    }
}

impl<C: Connect> Deref for PooledClient<C> {
    type Target = C::Connection;

    fn deref(&self) -> &C::Connection {
        &self.connection
    }
}

pub struct PgConnection {
    client: Client,
    task: JoinHandle<()>,
}

impl PgConnection {
    fn spawn<S, T>(client: Client, connection: tokio_postgres::Connection<S, T>) -> Self
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
        Self { client, task }
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
}

#[must_use]
pub fn query_config(settings: &PostgresSettings) -> Config {
    let wire_sslmode = match settings.sslmode {
        SslMode::Disable => WireSslMode::Disable,
        _ => WireSslMode::Require,
    };
    let options = format!(
        "-c client_encoding=UTF8 -c DateStyle=ISO -c IntervalStyle=postgres -c TimeZone=UTC \
         -c search_path= -c default_transaction_read_only=on -c lock_timeout={} \
         -c statement_timeout={} -c idle_in_transaction_session_timeout={}",
        settings.lock_timeout.as_millis(),
        settings
            .query_timeout
            .map_or(0, |timeout| timeout.as_millis()),
        settings.read_timeout.as_millis(),
    );
    let mut config = Config::new();
    config
        .host(&settings.host)
        .port(settings.port)
        .user(&settings.user)
        .dbname(&settings.database)
        .application_name(&settings.application_name)
        .options(options)
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
        })
    }

    async fn open(&self) -> Result<PgConnection> {
        let Some(tls) = &self.tls else {
            let connecting = self.config.connect(NoTls).await;
            let (client, connection) = connecting.map_err(|error| classify(&error, None))?;
            return Ok(PgConnection::spawn(client, connection));
        };
        let tracked = TrackedTls::new(tls.clone());
        let connecting = self.config.connect(tracked.clone()).await;
        let (client, connection) =
            connecting.map_err(|error| classify(&error, Some(tracked.handshake_attempted())))?;
        Ok(PgConnection::spawn(client, connection))
    }
}

impl Connect for PostgresConnector {
    type Connection = PgConnection;

    async fn connect(&self) -> Result<PgConnection> {
        within(TimeoutSetting::Connect, self.connect_timeout, self.open()).await
    }
}

pub type PostgresPool = QueryPool<PostgresConnector>;
