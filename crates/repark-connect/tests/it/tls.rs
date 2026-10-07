use std::path::{Path, PathBuf};
use std::sync::Arc;

use repark_common::{Error, ErrorClass};
use repark_connect::{ConnectError, TlsFailure, verify_full_config};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{CertificateError, ClientConnection, ServerConfig, ServerConnection};

pub(crate) fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/it/fixtures")
        .join(name)
}

pub(crate) fn server_config() -> Arc<ServerConfig> {
    let chain = CertificateDer::pem_file_iter(fixture("server.pem"))
        .expect("the leaf fixture opens")
        .collect::<Result<Vec<_>, _>>()
        .expect("the leaf fixture parses");
    let key = PrivateKeyDer::from_pem_file(fixture("server.key")).expect("the key fixture");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("the ring provider supports TLS 1.2 and 1.3")
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .expect("the server identity");
    Arc::new(config)
}

fn handshake(root: &str, name: &str) -> Result<(), rustls::Error> {
    let client_config = verify_full_config(Some(&fixture(root))).expect("the client config");
    let name = ServerName::try_from(name.to_string()).expect("a server name");
    let mut client = ClientConnection::new(Arc::new(client_config), name)?;
    let mut server = ServerConnection::new(server_config())?;
    while client.is_handshaking() || server.is_handshaking() {
        let mut to_server = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut to_server).expect("write to memory");
        }
        let mut flight = to_server.as_slice();
        while !flight.is_empty() {
            server.read_tls(&mut flight).expect("read from memory");
            server.process_new_packets()?;
        }
        let mut to_client = Vec::new();
        while server.wants_write() {
            server.write_tls(&mut to_client).expect("write to memory");
        }
        let mut flight = to_client.as_slice();
        while !flight.is_empty() {
            client.read_tls(&mut flight).expect("read from memory");
            client.process_new_packets()?;
        }
        if to_server.is_empty() && to_client.is_empty() {
            break;
        }
    }
    Ok(())
}

fn invalid_certificate(error: rustls::Error) -> CertificateError {
    match error {
        rustls::Error::InvalidCertificate(certificate) => certificate,
        other => panic!("expected a certificate refusal, got {other:?}"),
    }
}

#[test]
fn verify_full_trusts_sslrootcert_and_checks_the_host_name() {
    handshake("ca.pem", "localhost").expect("the CA in sslrootcert verifies the leaf");

    let wrong_name = handshake("ca.pem", "db.example.com").expect_err("a name the leaf lacks");
    assert!(
        matches!(
            invalid_certificate(wrong_name),
            CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. }
        ),
        "the host name is verified"
    );

    let unknown = handshake("other-ca.pem", "localhost").expect_err("a CA outside the store");
    assert_eq!(
        invalid_certificate(unknown),
        CertificateError::UnknownIssuer
    );
}

fn root_refusal(path: &Path) -> TlsFailure {
    let error = verify_full_config(Some(path)).expect_err("the root bundle refuses");
    let message = error.to_string();
    assert!(message.contains("`sslrootcert`"), "{message}");
    assert!(!message.contains(&*path.to_string_lossy()), "{message}");
    assert_eq!(
        Error::from(error.clone()).exception_class(),
        ErrorClass::Base
    );
    match error {
        ConnectError::TlsHandshake { kind } => kind,
        other => panic!("expected TlsHandshake, got {other:?}"),
    }
}

#[test]
fn sslrootcert_must_be_a_readable_pem_ca_bundle() {
    assert_eq!(
        root_refusal(&fixture("absent.pem")),
        TlsFailure::RootCertUnreadable {
            kind: std::io::ErrorKind::NotFound
        }
    );
    assert_eq!(
        root_refusal(&fixture("server.key")),
        TlsFailure::RootCertInvalid
    );

    let garbled = std::env::temp_dir().join(format!("repark-garbled-{}.pem", std::process::id()));
    std::fs::write(
        &garbled,
        "-----BEGIN CERTIFICATE-----\n@@@@\n-----END CERTIFICATE-----\n",
    )
    .expect("write the garbled bundle");
    let garbled_refusal = root_refusal(&garbled);
    std::fs::remove_file(&garbled).expect("remove the garbled bundle");
    assert_eq!(garbled_refusal, TlsFailure::RootCertInvalid);

    verify_full_config(Some(&fixture("ca.pem"))).expect("a CA bundle loads");
}

#[test]
fn a_bundle_with_one_unusable_certificate_refuses() {
    let ca = std::fs::read_to_string(fixture("ca.pem")).expect("the CA fixture");
    let unusable = "-----BEGIN CERTIFICATE-----\nAAAAAAAA\n-----END CERTIFICATE-----\n";
    let mixed = std::env::temp_dir().join(format!("repark-mixed-{}.pem", std::process::id()));
    std::fs::write(&mixed, format!("{ca}{unusable}")).expect("write the mixed bundle");
    let mixed_refusal = root_refusal(&mixed);
    std::fs::remove_file(&mixed).expect("remove the mixed bundle");
    assert_eq!(mixed_refusal, TlsFailure::RootCertInvalid);
}
