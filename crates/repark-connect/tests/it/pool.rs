use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use repark_common::{Error, ErrorClass};
use repark_connect::{
    Connect, ConnectError, PoolConnection, PoolLimits, PooledClient, PostgresConnector,
    PostgresSettings, QueryPool, SettingsDoor, TimeoutSetting, TlsFailure, query_config,
};
use rustls::ServerConnection;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::{AbortHandle, JoinHandle};
use tokio_postgres::config::{Host, SslMode as WireSslMode};

use crate::tls::{fixture, server_config};

struct Fake {
    closed: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl PoolConnection for Fake {
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn abort_handle(&self) -> AbortHandle {
        self.task.abort_handle()
    }
}

type Record = (Arc<AtomicBool>, AbortHandle);

#[derive(Clone, Default)]
struct Opened(Arc<Mutex<Vec<Record>>>);

impl Opened {
    fn count(&self) -> usize {
        self.0.lock().expect("opened").len()
    }

    fn close(&self, index: usize) {
        self.0.lock().expect("opened")[index]
            .0
            .store(true, Ordering::SeqCst);
    }

    fn finished(&self, index: usize) -> bool {
        self.0.lock().expect("opened")[index].1.is_finished()
    }
}

struct FakeConnector(Opened);

impl Connect for FakeConnector {
    type Connection = Fake;

    async fn connect(&self) -> repark_connect::Result<Fake> {
        let closed = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(std::future::pending::<()>());
        let record = (Arc::clone(&closed), task.abort_handle());
        self.0.0.lock().expect("opened").push(record);
        Ok(Fake { closed, task })
    }
}

fn fake_pool(
    max_size: usize,
    checkout_ms: u64,
    idle_ms: u64,
) -> (Arc<QueryPool<FakeConnector>>, Opened) {
    let opened = Opened::default();
    let limits = PoolLimits {
        max_size,
        checkout_timeout: Duration::from_millis(checkout_ms),
        idle_timeout: Duration::from_millis(idle_ms),
    };
    (
        QueryPool::new(FakeConnector(opened.clone()), limits),
        opened,
    )
}

async fn checkout_error<C: Connect>(pool: &Arc<QueryPool<C>>) -> ConnectError {
    match tokio::time::timeout(Duration::from_secs(3), pool.checkout()).await {
        Ok(Ok(_)) => panic!("the checkout succeeded"),
        Ok(Err(error)) => error,
        Err(elapsed) => panic!("the checkout hung past every timeout: {elapsed}"),
    }
}

#[tokio::test]
async fn a_clean_release_is_reused_by_the_next_checkout() {
    let (pool, opened) = fake_pool(2, 1000, 60_000);
    pool.checkout().await.expect("first").release_clean().await;
    assert_eq!(pool.idle_count(), 1);
    let again: PooledClient<FakeConnector> = pool.checkout().await.expect("reuse");
    assert_eq!(opened.count(), 1);
    assert_eq!(pool.idle_count(), 0);
    assert!(!again.is_closed());
    again.release_clean().await;
    tokio::task::yield_now().await;
    assert!(!opened.finished(0), "a clean release keeps the connection");
}

#[tokio::test]
async fn a_lease_dropped_before_release_aborts_its_connection() {
    let (pool, opened) = fake_pool(2, 1000, 60_000);
    drop(pool.checkout().await.expect("checkout"));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(opened.finished(0), "the connection task was aborted");
    assert_eq!(pool.idle_count(), 0);
    pool.checkout().await.expect("fresh").release_clean().await;
    assert_eq!(opened.count(), 2, "the dropped connection was not reused");
}

#[tokio::test]
async fn checkout_beyond_pool_max_size_is_pool_exhausted() {
    let (pool, _opened) = fake_pool(1, 100, 60_000);
    let held = pool.checkout().await.expect("the one permit");
    let started = Instant::now();
    let error = checkout_error(&pool).await;
    assert!(started.elapsed() >= Duration::from_millis(100));
    let waited = Duration::from_millis(100);
    assert_eq!(error, ConnectError::PoolExhausted { waited });
    assert!(
        error
            .to_string()
            .contains("`pool_checkout_timeout_ms` (100 ms)"),
        "{error}"
    );
    assert_eq!(Error::from(error).exception_class(), ErrorClass::Base);
    held.release_clean().await;
    pool.checkout()
        .await
        .expect("the permit came back")
        .release_clean()
        .await;
}

#[tokio::test]
async fn closed_and_idle_expired_connections_are_never_reused() {
    let (pool, opened) = fake_pool(2, 1000, 50);
    pool.checkout().await.expect("first").release_clean().await;
    opened.close(0);
    pool.checkout().await.expect("second").release_clean().await;
    assert_eq!(opened.count(), 2, "a closed connection is not reused");
    assert_eq!(pool.idle_count(), 1);
    tokio::time::sleep(Duration::from_millis(80)).await;
    let third = pool.checkout().await.expect("third");
    assert_eq!(opened.count(), 3, "an idle-expired connection is reaped");
    assert_eq!(pool.idle_count(), 0);
    opened.close(2);
    third.release_clean().await;
    assert_eq!(
        pool.idle_count(),
        0,
        "a closed connection is not pooled on release"
    );
}

fn settings(pairs: &[(&str, &str)]) -> PostgresSettings {
    let props: BTreeMap<String, String> = pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect();
    PostgresSettings::from_props(&props, SettingsDoor::ReparkToml).expect("the settings parse")
}

#[test]
fn query_config_pins_the_session_in_the_startup_packet() {
    let config = query_config(&settings(&[
        ("host", "db.internal"),
        ("port", "6543"),
        ("user", "app"),
        ("password", "pw"),
        ("database", "sales"),
        ("connect_timeout_ms", "3000"),
        ("read_timeout_ms", "7000"),
        ("query_timeout_ms", "9000"),
        ("lock_timeout_ms", "5000"),
        ("application_name", "nightly"),
    ]));
    assert_eq!(
        config.get_options(),
        Some(
            "-c client_encoding=UTF8 -c DateStyle=ISO -c IntervalStyle=postgres -c TimeZone=UTC \
             -c search_path= -c default_transaction_read_only=on -c lock_timeout=5000 \
             -c statement_timeout=9000 -c idle_in_transaction_session_timeout=7000 \
             -c client_connection_check_interval=1000"
        )
    );
    assert_eq!(config.get_hosts(), [Host::Tcp("db.internal".to_string())]);
    assert_eq!(config.get_ports(), [6543]);
    assert_eq!(config.get_user(), Some("app"));
    assert_eq!(config.get_password(), Some(&b"pw"[..]));
    assert_eq!(config.get_dbname(), Some("sales"));
    assert_eq!(config.get_application_name(), Some("nightly"));
    assert_eq!(config.get_connect_timeout(), Some(&Duration::from_secs(3)));
    assert!(config.get_keepalives());
    assert_eq!(config.get_ssl_mode(), WireSslMode::Require);

    let defaults = query_config(&settings(&[
        ("host", "h"),
        ("user", "app"),
        ("sslmode", "disable"),
    ]));
    assert_eq!(defaults.get_ssl_mode(), WireSslMode::Disable);
    assert_eq!(defaults.get_application_name(), Some("repark"));
    assert_eq!(defaults.get_dbname(), Some("app"));
    assert_eq!(defaults.get_password(), None);
    let options = defaults.get_options().expect("the session pins");
    assert!(
        options.ends_with(
            "-c lock_timeout=10000 -c statement_timeout=0 \
             -c idle_in_transaction_session_timeout=60000 \
             -c client_connection_check_interval=1000"
        ),
        "{options}"
    );
    assert!(!options.contains("replication"), "{options}");
}

async fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("address").port().to_string();
    (listener, port)
}

async fn connect_error(pairs: &[(&str, &str)]) -> ConnectError {
    let settings = settings(pairs);
    let connector = PostgresConnector::new(&settings).expect("the connector builds");
    checkout_error(&QueryPool::new(
        connector,
        PoolLimits::from_settings(&settings),
    ))
    .await
}

#[tokio::test]
async fn connect_timeout_bounds_a_server_that_never_answers() {
    let (listener, port) = listener().await;
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    let ca = fixture("ca.pem").to_string_lossy().into_owned();
    for sslmode in ["disable", "verify-full"] {
        let error = connect_error(&[
            ("host", "127.0.0.1"),
            ("port", &port),
            ("user", "app"),
            ("sslmode", sslmode),
            ("sslrootcert", &ca),
            ("connect_timeout_ms", "200"),
        ])
        .await;
        let which = TimeoutSetting::Connect;
        assert_eq!(error, ConnectError::Timeout { which }, "{sslmode}");
        assert!(
            error.to_string().contains("`connect_timeout_ms`"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn plaintext_server_refuses_under_verify_full() {
    let (listener, port) = listener().await;
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut request = [0_u8; 8];
        socket
            .read_exact(&mut request)
            .await
            .expect("the SSLRequest");
        socket.write_all(b"N").await.expect("refuse TLS");
        let mut rest = Vec::new();
        let _ = socket.read_to_end(&mut rest).await;
    });
    let ca = fixture("ca.pem").to_string_lossy().into_owned();
    let error = connect_error(&[
        ("host", "127.0.0.1"),
        ("port", &port),
        ("user", "app"),
        ("sslrootcert", &ca),
        ("connect_timeout_ms", "500"),
    ])
    .await;
    assert_eq!(error, ConnectError::TlsRequired);
    assert!(error.to_string().contains("`sslmode=disable`"), "{error}");
}

#[tokio::test]
async fn a_refused_port_is_unreachable() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|closed| closed.local_addr())
        .expect("a free port")
        .port()
        .to_string();
    let pairs = [
        ("host", "127.0.0.1"),
        ("port", &port),
        ("user", "app"),
        ("sslmode", "disable"),
    ];
    let error = connect_error(&pairs).await;
    let kind = std::io::ErrorKind::ConnectionRefused;
    assert_eq!(error, ConnectError::Unreachable { kind });
}

fn tls_server() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("address").port().to_string();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut tcp) = stream else { return };
            let mut request = [0_u8; 8];
            if tcp.read_exact(&mut request).is_ok() && tcp.write_all(b"S").is_ok() {
                let mut tls = ServerConnection::new(server_config()).expect("server");
                let _ = tls.complete_io(&mut tcp);
            }
        }
    });
    port
}

#[tokio::test]
async fn verify_full_refuses_an_untrusted_or_misnamed_server_certificate() {
    let port = tls_server();
    for (root, failure) in [
        ("other-ca.pem", TlsFailure::UntrustedCertificate),
        ("ca.pem", TlsFailure::HostNameMismatch),
    ] {
        let root = fixture(root).to_string_lossy().into_owned();
        let error = connect_error(&[
            ("host", "127.0.0.1"),
            ("port", &port),
            ("user", "app"),
            ("sslrootcert", &root),
            ("connect_timeout_ms", "2000"),
        ])
        .await;
        assert_eq!(
            error,
            ConnectError::TlsHandshake { kind: failure },
            "{root}"
        );
    }
}
