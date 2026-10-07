use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{fmt, fs, io};

use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use rustls::{CertificateError, ClientConfig, RootCertStore};
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::error::{ConnectError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsFailure {
    RootCertUnreadable { kind: io::ErrorKind },
    RootCertInvalid,
    NoTrustedRoots,
    ServerName,
    UntrustedCertificate,
    HostNameMismatch,
    CertificateExpired,
    Handshake,
}

impl fmt::Display for TlsFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlsFailure::RootCertUnreadable { kind } => {
                write!(f, "the `sslrootcert` file could not be read ({kind})")
            }
            TlsFailure::RootCertInvalid => {
                f.write_str("the `sslrootcert` file is not a PEM bundle of CA certificates")
            }
            TlsFailure::NoTrustedRoots => f.write_str(
                "no trusted root: the system store is empty; set `sslrootcert` to your CA bundle",
            ),
            TlsFailure::ServerName => {
                f.write_str("`host` is not a DNS name or IP address a certificate can name")
            }
            TlsFailure::UntrustedCertificate => f.write_str(
                "the server certificate does not chain to a trusted root; set `sslrootcert` to \
                 your CA bundle",
            ),
            TlsFailure::HostNameMismatch => {
                f.write_str("the server certificate does not name `host`")
            }
            TlsFailure::CertificateExpired => {
                f.write_str("the server certificate is expired or not yet valid")
            }
            TlsFailure::Handshake => f.write_str("the TLS handshake failed"),
        }
    }
}

fn refuse(kind: TlsFailure) -> ConnectError {
    ConnectError::TlsHandshake { kind }
}

pub(crate) fn failure_of(error: &rustls::Error) -> TlsFailure {
    let rustls::Error::InvalidCertificate(certificate) = error else {
        return TlsFailure::Handshake;
    };
    match certificate {
        CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. } => {
            TlsFailure::HostNameMismatch
        }
        CertificateError::Expired
        | CertificateError::ExpiredContext { .. }
        | CertificateError::NotValidYet
        | CertificateError::NotValidYetContext { .. } => TlsFailure::CertificateExpired,
        CertificateError::UnknownIssuer | CertificateError::BadSignature => {
            TlsFailure::UntrustedCertificate
        }
        _ => TlsFailure::Handshake,
    }
}

fn trusted_roots(sslrootcert: Option<&Path>) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
    if let Some(path) = sslrootcert {
        let pem = fs::read(path)
            .map_err(|error| refuse(TlsFailure::RootCertUnreadable { kind: error.kind() }))?;
        let certificates = CertificateDer::pem_slice_iter(&pem)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| refuse(TlsFailure::RootCertInvalid))?;
        let (added, ignored) = roots.add_parsable_certificates(certificates);
        if added == 0 || ignored > 0 {
            return Err(refuse(TlsFailure::RootCertInvalid));
        }
    }
    if roots.is_empty() {
        return Err(refuse(TlsFailure::NoTrustedRoots));
    }
    Ok(roots)
}

#[allow(clippy::missing_errors_doc)]
pub fn verify_full_config(sslrootcert: Option<&Path>) -> Result<ClientConfig> {
    let roots = trusted_roots(sslrootcert)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| refuse(TlsFailure::Handshake))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(config)
}

#[derive(Clone)]
pub(crate) struct TrackedTls {
    inner: MakeRustlsConnect,
    attempted: Arc<AtomicBool>,
}

impl TrackedTls {
    pub(crate) fn new(inner: MakeRustlsConnect) -> Self {
        Self {
            inner,
            attempted: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn handshake_attempted(&self) -> bool {
        self.attempted.load(Ordering::Acquire)
    }
}

impl<S> MakeTlsConnect<S> for TrackedTls
where
    MakeRustlsConnect: MakeTlsConnect<S>,
{
    type Stream = <MakeRustlsConnect as MakeTlsConnect<S>>::Stream;
    type TlsConnect = TrackedConnect<<MakeRustlsConnect as MakeTlsConnect<S>>::TlsConnect>;
    type Error = <MakeRustlsConnect as MakeTlsConnect<S>>::Error;

    fn make_tls_connect(
        &mut self,
        domain: &str,
    ) -> std::result::Result<Self::TlsConnect, Self::Error> {
        Ok(TrackedConnect {
            inner: self.inner.make_tls_connect(domain)?,
            attempted: Arc::clone(&self.attempted),
        })
    }
}

pub(crate) struct TrackedConnect<T> {
    inner: T,
    attempted: Arc<AtomicBool>,
}

impl<S, T: TlsConnect<S>> TlsConnect<S> for TrackedConnect<T> {
    type Stream = T::Stream;
    type Error = T::Error;
    type Future = T::Future;

    fn connect(self, stream: S) -> T::Future {
        self.attempted.store(true, Ordering::Release);
        self.inner.connect(stream)
    }
}
