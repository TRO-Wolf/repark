use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use repark_common::redaction;

use super::{AUTH_METHOD_KEY, AuthMethod, ConnectionSettings};
use crate::error::{ConnectError, Result};

pub const DEFAULT_PORT: u16 = 5432;
pub const POSTGRES_DRIVER: &str = "org.postgresql.Driver";
const DRIVER_KEY: &str = "driver";
const MAX_MILLIS: u64 = 2_147_483_647;

pub const POSTGRES_KEYS: [&str; 20] = [
    "url",
    "host",
    "port",
    "database",
    "user",
    "password",
    AUTH_METHOD_KEY,
    "sslmode",
    "sslrootcert",
    "connect_timeout_ms",
    "read_timeout_ms",
    "query_timeout_ms",
    "lock_timeout_ms",
    "batch_rows",
    "prefer_timestamp_ntz",
    "pushdown_predicate",
    "pool_max_size",
    "pool_checkout_timeout_ms",
    "pool_idle_timeout_ms",
    "application_name",
];

pub const POSTGRES_ALIASES: [(&str, &str); 12] = [
    ("url", "url"),
    ("user", "user"),
    ("password", "password"),
    ("sslmode", "sslmode"),
    ("sslrootcert", "sslrootcert"),
    ("connectTimeout", "connect_timeout_ms"),
    ("socketTimeout", "read_timeout_ms"),
    ("queryTimeout", "query_timeout_ms"),
    ("fetchsize", "batch_rows"),
    ("preferTimestampNTZ", "prefer_timestamp_ntz"),
    ("pushDownPredicate", "pushdown_predicate"),
    ("ApplicationName", "application_name"),
];

const SECONDS_ALIASES: [&str; 3] = ["connectTimeout", "socketTimeout", "queryTimeout"];

const DECLARED_KEYS: [(&str, DeclaredSetting); 10] = [
    ("sslcert", DeclaredSetting::ClientCert),
    ("sslkey", DeclaredSetting::ClientCert),
    ("sessionInitStatement", DeclaredSetting::SessionSql),
    ("customSchema", DeclaredSetting::SessionSql),
    ("options", DeclaredSetting::SessionSql),
    ("partitionColumn", DeclaredSetting::PartitionedRead),
    ("lowerBound", DeclaredSetting::PartitionedRead),
    ("upperBound", DeclaredSetting::PartitionedRead),
    ("numPartitions", DeclaredSetting::PartitionedRead),
    ("predicates", DeclaredSetting::PartitionedRead),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsDoor {
    ReparkToml,
    ReadPostgres,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

const SSLMODES: [(SslMode, &str); 6] = [
    (SslMode::Disable, "disable"),
    (SslMode::Allow, "allow"),
    (SslMode::Prefer, "prefer"),
    (SslMode::Require, "require"),
    (SslMode::VerifyCa, "verify-ca"),
    (SslMode::VerifyFull, "verify-full"),
];

impl SslMode {
    #[must_use]
    pub fn from_spelling(spelling: &str) -> Option<SslMode> {
        SSLMODES
            .iter()
            .find(|(_, known)| *known == spelling)
            .map(|(mode, _)| *mode)
    }

    #[must_use]
    pub fn spelling(self) -> &'static str {
        SSLMODES
            .iter()
            .find(|(mode, _)| *mode == self)
            .map_or("", |(_, spelling)| spelling)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclaredSetting {
    ClientCert,
    SessionSql,
    PartitionedRead,
    MultiHost,
    UnverifiedSslmode(SslMode),
}

impl DeclaredSetting {
    #[must_use]
    pub fn registry_row(self) -> &'static str {
        match self {
            DeclaredSetting::ClientCert => "CONNECT-DECL-pg-client-cert",
            DeclaredSetting::SessionSql => "CONNECT-DECL-pg-session-sql",
            DeclaredSetting::PartitionedRead => "CONNECT-DECL-pg-partitioned-read",
            DeclaredSetting::MultiHost => "CONNECT-DECL-pg-multi-host",
            DeclaredSetting::UnverifiedSslmode(_) => "CONNECT-DECL-sslmode-unverified",
        }
    }
}

impl fmt::Display for DeclaredSetting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verify_full = "use `verify-full` with `sslrootcert` pointing at your CA bundle";
        match self {
            DeclaredSetting::ClientCert => f.write_str("certificate auth; use a password"),
            DeclaredSetting::SessionSql => f.write_str(
                "SQL at connect, a schema override or raw startup options; select through \
                 `query` with casts instead",
            ),
            DeclaredSetting::PartitionedRead => f.write_str("a partitioned read (C-3)"),
            DeclaredSetting::MultiHost => f.write_str("a host list; give one host"),
            DeclaredSetting::UnverifiedSslmode(mode @ (SslMode::Allow | SslMode::Prefer)) => {
                write!(
                    f,
                    "`sslmode` `{}` falls back to plaintext silently; {verify_full}",
                    mode.spelling()
                )
            }
            DeclaredSetting::UnverifiedSslmode(mode) => write!(
                f,
                "`sslmode` `{}` encrypts without a host-name check; {verify_full}",
                mode.spelling()
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Spelling {
    Key(String),
    UrlQuery(String),
    UrlPart(&'static str),
}

impl fmt::Display for Spelling {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Spelling::Key(key) => write!(f, "`{key}`"),
            Spelling::UrlQuery(key) => write!(f, "`{key}` in the `url` query"),
            Spelling::UrlPart(part) => write!(f, "the {part} in `url`"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlViolation {
    Scheme,
    PercentEncoding,
    QueryPair,
    Ipv6Host,
    AmbiguousUserinfo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpecRefusal {
    UnknownKey { aliases: bool },
    Conflict { other: Spelling },
    Missing,
    Empty,
    Nul,
    Integer { min: u64, max: u64 },
    Boolean,
    Sslmode,
    Driver,
    ZeroReadTimeout,
    Url(UrlViolation),
}

impl fmt::Display for SpecRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpecRefusal::UnknownKey { aliases } => {
                let mut accepted: Vec<&str> = POSTGRES_KEYS.to_vec();
                if *aliases {
                    accepted.extend(
                        POSTGRES_ALIASES
                            .iter()
                            .map(|(alias, _)| *alias)
                            .filter(|alias| !POSTGRES_KEYS.contains(alias)),
                    );
                    accepted.push(DRIVER_KEY);
                }
                write!(
                    f,
                    "is not a Postgres source key; accepted keys: {}",
                    accepted.join(", ")
                )
            }
            SpecRefusal::Conflict { other } => {
                write!(f, "and {other} give the same setting; give it once")
            }
            SpecRefusal::Missing => f.write_str("is required"),
            SpecRefusal::Empty => f.write_str("is empty"),
            SpecRefusal::Nul => f.write_str("contains a NUL byte"),
            SpecRefusal::Integer { min, max } => write!(f, "must be an integer in {min}..={max}"),
            SpecRefusal::Boolean => f.write_str("must be `true` or `false`"),
            SpecRefusal::Sslmode => {
                let spellings: Vec<&str> = SSLMODES.iter().map(|(_, spelling)| *spelling).collect();
                write!(f, "must be one of: {}", spellings.join(", "))
            }
            SpecRefusal::Driver => write!(f, "accepts only `{POSTGRES_DRIVER}`"),
            SpecRefusal::ZeroReadTimeout => {
                f.write_str("is `0`, but every network wait needs a timeout (NS-7)")
            }
            SpecRefusal::Url(violation) => f.write_str(match violation {
                UrlViolation::Scheme => {
                    "must start with `postgresql://`, `postgres://` or `jdbc:postgresql://`"
                }
                UrlViolation::PercentEncoding => "has a `%` escape that is malformed or not UTF-8",
                UrlViolation::QueryPair => "has a query parameter without `=`",
                UrlViolation::Ipv6Host => "has a malformed bracketed IPv6 host",
                UrlViolation::AmbiguousUserinfo => {
                    "holds `?` or `=` in a URL without a path; percent-encode them \
                     (`%3F`, `%3D`) or add the `/database`"
                }
            }),
        }
    }
}

#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
pub struct PostgresSettings {
    pub auth_method: AuthMethod,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<String>,
    pub sslmode: SslMode,
    pub sslrootcert: Option<PathBuf>,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub query_timeout: Option<Duration>,
    pub lock_timeout: Duration,
    pub batch_rows: Option<NonZeroUsize>,
    pub prefer_timestamp_ntz: bool,
    pub pushdown_predicate: bool,
    pub pool_max_size: usize,
    pub pool_checkout_timeout: Duration,
    pub pool_idle_timeout: Duration,
    pub application_name: String,
}

impl fmt::Debug for PostgresSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresSettings")
            .field("auth_method", &self.auth_method)
            .field("sslmode", &self.sslmode)
            .finish_non_exhaustive()
    }
}

#[must_use]
pub fn redact_source_prop(key: &str, value: &str) -> String {
    let Some((scheme, _, rest)) = split_scheme(value) else {
        return redaction::redact_value(key, value);
    };
    let (userinfo, rest) = split_userinfo(rest);
    let mut masked = scheme.to_string();
    if let Some(userinfo) = userinfo {
        if let Some((user, _)) = userinfo.split_once(':') {
            masked.push_str(user);
            masked.push(':');
        }
        masked.push_str(redaction::REDACTED);
        masked.push('@');
    }
    let (before_query, query) = rest.split_once('?').unwrap_or((rest, ""));
    masked.push_str(before_query);
    if rest.len() > before_query.len() {
        let pairs: Vec<String> = query
            .split('&')
            .map(|pair| match pair.split_once('=') {
                Some((name, _)) if secret_query_name(name) => {
                    format!("{name}={}", redaction::REDACTED)
                }
                _ => pair.to_string(),
            })
            .collect();
        masked.push('?');
        masked.push_str(&pairs.join("&"));
    }
    redaction::redact_value(key, &masked)
}

fn secret_query_name(name: &str) -> bool {
    percent_decode(name, &Spelling::UrlPart("query key"))
        .map_or(true, |name| redaction::prop_key_is_secret(&name))
}

const URL_SCHEMES: [(&str, bool); 3] = [
    ("jdbc:postgresql://", true),
    ("postgresql://", false),
    ("postgres://", false),
];

fn split_scheme(url: &str) -> Option<(&'static str, bool, &str)> {
    URL_SCHEMES
        .iter()
        .find_map(|(scheme, jdbc)| Some((*scheme, *jdbc, url.strip_prefix(scheme)?)))
}

fn split_userinfo(rest: &str) -> (Option<&str>, &str) {
    let authority_end = rest.find('/').unwrap_or(rest.len());
    match rest.get(..authority_end).and_then(|head| head.rfind('@')) {
        Some(at) => (rest.get(..at), rest.get(at + 1..).unwrap_or_default()),
        None => (None, rest),
    }
}

type Givens = BTreeMap<&'static str, (Spelling, String)>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Position {
    Top(SettingsDoor),
    UrlQuery { jdbc: bool },
}

fn invalid(key: Spelling, reason: SpecRefusal) -> ConnectError {
    ConnectError::InvalidSpecification { key, reason }
}

fn give(given: &mut Givens, key: &'static str, spelling: Spelling, value: String) -> Result<()> {
    if let Some((first, _)) = given.get(key) {
        let reason = SpecRefusal::Conflict { other: spelling };
        return Err(invalid(first.clone(), reason));
    }
    given.insert(key, (spelling, value));
    Ok(())
}

fn take_entry(given: &mut Givens, name: &str, value: &str, position: Position) -> Result<()> {
    let top_spark = position == Position::Top(SettingsDoor::ReadPostgres);
    let spark = top_spark || position == Position::UrlQuery { jdbc: true };
    let in_url = matches!(position, Position::UrlQuery { .. });
    let spelling = if in_url {
        Spelling::UrlQuery(name.to_string())
    } else {
        Spelling::Key(name.to_string())
    };
    let matches = |key: &str| key == name || (spark && key.eq_ignore_ascii_case(name));
    let alias = POSTGRES_ALIASES
        .iter()
        .find(|(alias, _)| spark && matches(alias));
    let canonical = alias.map_or(name, |(_, canonical)| canonical);
    let known = POSTGRES_KEYS.iter().find(|key| **key == canonical);
    if let Some(key) = known.filter(|key| !in_url || (**key != "url" && **key != AUTH_METHOD_KEY)) {
        return give(given, key, spelling, value.to_string());
    }
    let declared = DECLARED_KEYS.iter().find(|(key, declared)| {
        matches(key) && (top_spark || *declared != DeclaredSetting::PartitionedRead)
    });
    let driver = top_spark && name.eq_ignore_ascii_case(DRIVER_KEY);
    match declared {
        Some((_, declared)) => Err(ConnectError::DeclaredSetting {
            key: spelling,
            declared: *declared,
        }),
        None if driver && value == POSTGRES_DRIVER => Ok(()),
        None if driver => Err(invalid(spelling, SpecRefusal::Driver)),
        None => {
            let spelling = if in_url {
                Spelling::UrlPart("query key")
            } else {
                spelling
            };
            Err(invalid(
                spelling,
                SpecRefusal::UnknownKey { aliases: spark },
            ))
        }
    }
}

fn percent_decode(text: &str, spelling: &Spelling) -> Result<String> {
    let reason = SpecRefusal::Url(UrlViolation::PercentEncoding);
    let refuse = || invalid(spelling.clone(), reason.clone());
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        if byte == b'%' {
            let pair = bytes
                .get(index + 1..index + 3)
                .filter(|pair| pair.iter().all(u8::is_ascii_hexdigit))
                .ok_or_else(refuse)?;
            let hex = std::str::from_utf8(pair).map_err(|_| refuse())?;
            out.push(u8::from_str_radix(hex, 16).map_err(|_| refuse())?);
            index += 3;
        } else {
            out.push(byte);
            index += 1;
        }
    }
    String::from_utf8(out).map_err(|_| refuse())
}

fn split_host_port(hostport: &str) -> Option<(&str, &str)> {
    let Some(bracketed) = hostport.strip_prefix('[') else {
        if hostport.contains(',') {
            return Some((hostport, ""));
        }
        return Some(hostport.split_once(':').unwrap_or((hostport, "")));
    };
    let (host, after) = bracketed.split_once(']')?;
    match after {
        "" => Some((host, "")),
        _ => Some((host, after.strip_prefix(':')?)),
    }
}

fn take_url(given: &mut Givens, spelling: &Spelling, url: &str) -> Result<()> {
    let refuse = |violation| invalid(spelling.clone(), SpecRefusal::Url(violation));
    let (_, jdbc, rest) = split_scheme(url).ok_or_else(|| refuse(UrlViolation::Scheme))?;
    let (userinfo, rest) = split_userinfo(rest);
    if !rest.contains('/') && userinfo.is_some_and(|userinfo| userinfo.contains(['?', '='])) {
        let reason = SpecRefusal::Url(UrlViolation::AmbiguousUserinfo);
        return Err(invalid(Spelling::UrlPart("userinfo"), reason));
    }
    let (before_query, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (hostport, path) = before_query.split_once('/').unwrap_or((before_query, ""));
    let (host, port) = split_host_port(hostport).ok_or_else(|| refuse(UrlViolation::Ipv6Host))?;
    let mut parts = vec![("host", host), ("port", port), ("database", path)];
    match userinfo.map(|userinfo| userinfo.split_once(':').ok_or(userinfo)) {
        Some(Ok((user, password))) => parts.extend([("user", user), ("password", password)]),
        Some(Err(user)) => parts.push(("user", user)),
        None => {}
    }
    for (part, raw) in parts {
        if raw.is_empty() && part != "password" {
            continue;
        }
        let value = percent_decode(raw, &Spelling::UrlPart(part))?;
        give(given, part, Spelling::UrlPart(part), value)?;
    }
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (name, value) = pair
            .split_once('=')
            .ok_or_else(|| refuse(UrlViolation::QueryPair))?;
        let name = percent_decode(name, spelling)?;
        let value = percent_decode(value, &Spelling::UrlPart("query"))?;
        take_entry(given, &name, &value, Position::UrlQuery { jdbc })?;
    }
    Ok(())
}

fn text(given: &mut Givens, key: &'static str) -> Result<Option<(Spelling, String)>> {
    match given.remove(key) {
        Some((spelling, value)) if value.contains('\0') => Err(invalid(spelling, SpecRefusal::Nul)),
        entry => Ok(entry),
    }
}

fn non_empty(given: &mut Givens, key: &'static str) -> Result<Option<String>> {
    match text(given, key)? {
        Some((spelling, value)) if value.is_empty() => Err(invalid(spelling, SpecRefusal::Empty)),
        entry => Ok(entry.map(|(_, value)| value)),
    }
}

fn required(given: &mut Givens, key: &'static str) -> Result<String> {
    non_empty(given, key)?
        .ok_or_else(|| invalid(Spelling::Key(key.to_string()), SpecRefusal::Missing))
}

fn integer(given: &mut Givens, key: &'static str, min: u64, max: u64) -> Result<Option<u64>> {
    let Some((spelling, value)) = given.remove(key) else {
        return Ok(None);
    };
    let seconds = match &spelling {
        Spelling::Key(name) | Spelling::UrlQuery(name) => SECONDS_ALIASES
            .iter()
            .any(|alias| alias.eq_ignore_ascii_case(name)),
        Spelling::UrlPart(_) => false,
    };
    let scale = if seconds { 1000 } else { 1 };
    let (min, max) = (min.div_ceil(scale), max / scale);
    let parsed = Some(value.as_str())
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| (min..=max).contains(value));
    match parsed {
        Some(0) if key == "read_timeout_ms" => Err(invalid(spelling, SpecRefusal::ZeroReadTimeout)),
        Some(value) => Ok(Some(value * scale)),
        None => Err(invalid(spelling, SpecRefusal::Integer { min, max })),
    }
}

fn millis(given: &mut Givens, key: &'static str, min: u64, default: u64) -> Result<Duration> {
    let value = integer(given, key, min, MAX_MILLIS)?;
    Ok(Duration::from_millis(value.unwrap_or(default)))
}

fn boolean(given: &mut Givens, key: &'static str, default: bool) -> Result<bool> {
    match given.remove(key) {
        None => Ok(default),
        Some((_, value)) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some((_, value)) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some((spelling, _)) => Err(invalid(spelling, SpecRefusal::Boolean)),
    }
}

fn sslmode(given: &mut Givens) -> Result<SslMode> {
    let Some((spelling, value)) = text(given, "sslmode")? else {
        return Ok(SslMode::VerifyFull);
    };
    match SslMode::from_spelling(&value) {
        Some(mode @ (SslMode::Disable | SslMode::VerifyFull)) => Ok(mode),
        Some(mode) => Err(ConnectError::DeclaredSetting {
            key: spelling,
            declared: DeclaredSetting::UnverifiedSslmode(mode),
        }),
        None => Err(invalid(spelling, SpecRefusal::Sslmode)),
    }
}

fn count(given: &mut Givens, key: &'static str, max: u64) -> Result<Option<NonZeroUsize>> {
    let value = integer(given, key, 1, max)?;
    Ok(value.and_then(|value| usize::try_from(value).ok().and_then(NonZeroUsize::new)))
}

fn batch_rows(given: &mut Givens) -> Result<Option<NonZeroUsize>> {
    let session_default = matches!(given.get("batch_rows"), Some((Spelling::Key(name), value))
        if name.eq_ignore_ascii_case("fetchsize") && !value.is_empty() && value.bytes().all(|byte| byte == b'0'));
    if session_default {
        given.remove("batch_rows");
        return Ok(None);
    }
    count(given, "batch_rows", MAX_MILLIS)
}

impl PostgresSettings {
    #[allow(clippy::missing_errors_doc)]
    pub fn from_props(props: &BTreeMap<String, String>, door: SettingsDoor) -> Result<Self> {
        let connection = ConnectionSettings::from_props(props)?;
        let mut given = Givens::new();
        for (name, value) in connection.props() {
            take_entry(&mut given, name, value, Position::Top(door))?;
        }
        if let Some((spelling, url)) = given.remove("url") {
            take_url(&mut given, &spelling, &url)?;
        }
        if let Some((spelling, _)) = given.get("host").filter(|(_, host)| host.contains(',')) {
            return Err(ConnectError::DeclaredSetting {
                key: spelling.clone(),
                declared: DeclaredSetting::MultiHost,
            });
        }
        let host = required(&mut given, "host")?;
        let port = integer(&mut given, "port", 1, u64::from(u16::MAX))?;
        let user = required(&mut given, "user")?;
        let database = non_empty(&mut given, "database")?;
        let query_timeout = millis(&mut given, "query_timeout_ms", 0, 0)?;
        Ok(Self {
            auth_method: connection.auth_method(),
            host,
            port: port
                .and_then(|port| u16::try_from(port).ok())
                .unwrap_or(DEFAULT_PORT),
            database: database.unwrap_or_else(|| user.clone()),
            user,
            password: text(&mut given, "password")?.map(|(_, value)| value),
            sslmode: sslmode(&mut given)?,
            sslrootcert: non_empty(&mut given, "sslrootcert")?.map(PathBuf::from),
            connect_timeout: millis(&mut given, "connect_timeout_ms", 1, 10_000)?,
            read_timeout: millis(&mut given, "read_timeout_ms", 0, 60_000)?,
            query_timeout: Some(query_timeout).filter(|timeout| !timeout.is_zero()),
            lock_timeout: millis(&mut given, "lock_timeout_ms", 0, 10_000)?,
            batch_rows: batch_rows(&mut given)?,
            prefer_timestamp_ntz: boolean(&mut given, "prefer_timestamp_ntz", false)?,
            pushdown_predicate: boolean(&mut given, "pushdown_predicate", true)?,
            pool_max_size: count(&mut given, "pool_max_size", 64)?.map_or(4, NonZeroUsize::get),
            pool_checkout_timeout: millis(&mut given, "pool_checkout_timeout_ms", 1, 30_000)?,
            pool_idle_timeout: millis(&mut given, "pool_idle_timeout_ms", 1, 300_000)?,
            application_name: text(&mut given, "application_name")?
                .map_or_else(|| "repark".to_string(), |(_, value)| value),
        })
    }
}
