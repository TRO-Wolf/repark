use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use repark_common::{Error, ErrorClass};
use repark_connect::{
    AUTH_METHOD_KEY, AuthMethod, ConnectError, ConnectionSettings, DeclaredSetting,
    POSTGRES_DRIVER, POSTGRES_KEYS, PostgresSettings, SettingsDoor, SpecRefusal, Spelling, SslMode,
    redact_source_prop,
};

fn props(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

#[test]
fn an_absent_auth_method_is_password() {
    let source = props(&[("url", "postgresql://localhost/db"), ("user", "app")]);
    let settings = ConnectionSettings::from_props(&source).expect("password by default");
    assert_eq!(settings.auth_method(), AuthMethod::Password);
    assert_eq!(settings.props(), &source);
}

#[test]
fn an_explicit_password_auth_method_is_accepted_and_carries_the_other_props() {
    let source = props(&[
        (AUTH_METHOD_KEY, "password"),
        ("url", "postgresql://localhost/db"),
        ("password", "s3cret"),
        ("sslmode", "require"),
    ]);
    let settings = ConnectionSettings::from_props(&source).expect("explicit password");
    assert_eq!(settings.auth_method(), AuthMethod::Password);
    let expected = props(&[
        ("url", "postgresql://localhost/db"),
        ("password", "s3cret"),
        ("sslmode", "require"),
    ]);
    assert_eq!(settings.props(), &expected);
}

#[test]
fn iam_token_is_a_declared_refusal() {
    let source = props(&[(AUTH_METHOD_KEY, "iam_token")]);
    let error = ConnectionSettings::from_props(&source).expect_err("iam_token refuses");
    assert_eq!(
        error,
        ConnectError::DeclaredAuthMethod {
            method: AuthMethod::IamToken,
            registry_row: "CONNECT-DECL-auth-iam_token",
        }
    );
    let message = error.to_string();
    assert!(message.contains("`iam_token`"), "{message}");
    assert!(message.contains("CONNECT-DECL-auth-iam_token"), "{message}");
    let common = Error::from(error);
    assert_eq!(common.exception_class(), ErrorClass::Unsupported);
}

#[test]
fn kerberos_is_a_declared_refusal() {
    let source = props(&[(AUTH_METHOD_KEY, "kerberos")]);
    let error = ConnectionSettings::from_props(&source).expect_err("kerberos refuses");
    assert_eq!(
        error,
        ConnectError::DeclaredAuthMethod {
            method: AuthMethod::Kerberos,
            registry_row: "CONNECT-DECL-auth-kerberos",
        }
    );
    assert!(
        error.to_string().contains("CONNECT-DECL-auth-kerberos"),
        "{error}"
    );
    let common = Error::from(error);
    assert_eq!(common.exception_class(), ErrorClass::Unsupported);
}

#[test]
fn any_other_auth_method_is_an_invalid_specification() {
    for value in [
        "",
        "Password",
        "iam-token",
        "IAM_TOKEN",
        " password",
        "ldap",
    ] {
        let source = props(&[(AUTH_METHOD_KEY, value)]);
        let error = ConnectionSettings::from_props(&source).expect_err("invalid value refuses");
        assert_eq!(
            error,
            ConnectError::InvalidAuthMethod {
                value: value.to_string(),
            }
        );
        let message = error.to_string();
        assert!(message.starts_with("invalid specification"), "{message}");
        assert!(
            message.contains("password, iam_token, kerberos"),
            "{message}"
        );
        let common = Error::from(error);
        assert_eq!(common.exception_class(), ErrorClass::IllegalArgument);
    }
}

#[test]
fn every_auth_method_spelling_round_trips() {
    for spelling in AuthMethod::SPELLINGS {
        let method = AuthMethod::from_spelling(spelling).expect("known spelling");
        assert_eq!(method.spelling(), *spelling);
    }
    assert_eq!(AuthMethod::Password.declared_refusal(), None);
}

#[test]
fn connection_settings_debug_never_renders_a_prop_value() {
    let secret = "SUPER_SECRET_VALUE_do_not_leak";
    let source = props(&[("url", "postgresql://localhost/db"), ("password", secret)]);
    let settings = ConnectionSettings::from_props(&source).expect("password settings");
    let rendered = format!("{settings:?}");
    assert!(!rendered.contains(secret), "leaked: {rendered}");
    assert!(rendered.contains("password"), "{rendered}");
    assert!(rendered.contains("Password"), "{rendered}");
}

const TOML: SettingsDoor = SettingsDoor::ReparkToml;
const SPARK: SettingsDoor = SettingsDoor::ReadPostgres;

fn parse(pairs: &[(&str, &str)], door: SettingsDoor) -> PostgresSettings {
    PostgresSettings::from_props(&props(pairs), door).expect("the settings parse")
}

fn refusal(pairs: &[(&str, &str)], door: SettingsDoor) -> ConnectError {
    PostgresSettings::from_props(&props(pairs), door).expect_err("the settings refuse")
}

fn one(name: &str, value: &str, door: SettingsDoor) -> PostgresSettings {
    parse(&[("host", "h"), ("user", "app"), (name, value)], door)
}

fn refuse_one(name: &str, value: &str, door: SettingsDoor) -> ConnectError {
    refusal(&[("host", "h"), ("user", "app"), (name, value)], door)
}

fn summary(s: &PostgresSettings) -> String {
    format!(
        "{}:{:?}@{}:{}/{} {:?} {:?} {:?} connect={:?} read={:?} query={:?} lock={:?} batch={:?} \
         ntz={} pushdown={} pool={} checkout={:?} idle={:?} app={}",
        s.user,
        s.password,
        s.host,
        s.port,
        s.database,
        s.auth_method,
        s.sslmode,
        s.sslrootcert,
        s.connect_timeout,
        s.read_timeout,
        s.query_timeout,
        s.lock_timeout,
        s.batch_rows,
        s.prefer_timestamp_ntz,
        s.pushdown_predicate,
        s.pool_max_size,
        s.pool_checkout_timeout,
        s.pool_idle_timeout,
        s.application_name,
    )
}

fn unknown() -> SpecRefusal {
    SpecRefusal::UnknownKey { aliases: false }
}

fn key(name: &str) -> Spelling {
    Spelling::Key(name.to_string())
}

fn query(name: &str) -> Spelling {
    Spelling::UrlQuery(name.to_string())
}

fn int(min: u64, max: u64) -> SpecRefusal {
    SpecRefusal::Integer { min, max }
}

fn conflict(other: Spelling) -> SpecRefusal {
    SpecRefusal::Conflict { other }
}

fn is_invalid(error: &ConnectError, key: Spelling, reason: SpecRefusal) {
    assert_eq!(error, &ConnectError::InvalidSpecification { key, reason });
    let class = Error::from(error.clone()).exception_class();
    assert_eq!(class, ErrorClass::IllegalArgument);
}

fn rejects(name: &str, value: &str, door: SettingsDoor, reason: SpecRefusal) {
    is_invalid(&refuse_one(name, value, door), key(name), reason);
}

fn is_declared(error: &ConnectError, key: Spelling, declared: DeclaredSetting) {
    assert_eq!(error, &ConnectError::DeclaredSetting { key, declared });
    let message = error.to_string();
    assert!(message.contains(declared.registry_row()), "{message}");
    let class = Error::from(error.clone()).exception_class();
    assert_eq!(class, ErrorClass::Unsupported);
}

fn values_refuse_naming_the_key() {
    let max = 2_147_483_647;
    rejects("port", "0", TOML, int(1, 65_535));
    rejects("port", "65536", TOML, int(1, 65_535));
    rejects("port", "-1", TOML, int(1, 65_535));
    rejects("pool_max_size", "65", TOML, int(1, 64));
    rejects("pool_max_size", "0", TOML, int(1, 64));
    rejects("batch_rows", "0", TOML, int(1, max));
    rejects("connect_timeout_ms", "0", TOML, int(1, max));
    rejects("lock_timeout_ms", "1.5", TOML, int(0, max));
    rejects("query_timeout_ms", "+5", TOML, int(0, max));
    rejects("pushdown_predicate", "yes", TOML, SpecRefusal::Boolean);
    is_invalid(
        &refusal(&[("user", "app")], TOML),
        key("host"),
        SpecRefusal::Missing,
    );
    is_invalid(
        &refusal(&[("host", "h")], TOML),
        key("user"),
        SpecRefusal::Missing,
    );
    rejects("user", "", TOML, SpecRefusal::Empty);
    rejects("application_name", "a\0b", TOML, SpecRefusal::Nul);
    assert_eq!(one("driver", POSTGRES_DRIVER, SPARK).host, "h");
    rejects("driver", "com.mysql.Driver", SPARK, SpecRefusal::Driver);
    for url in [
        "mysql://h/db",
        "jdbc:postgresql:db",
        "postgresql://h/db?flag",
        "postgresql://h/%zz",
    ] {
        let error = refusal(&[("url", url), ("user", "app")], TOML);
        let parsed = matches!(
            error,
            ConnectError::InvalidSpecification {
                reason: SpecRefusal::Url(_),
                ..
            }
        );
        assert!(parsed, "{url}: {error:?}");
    }
}

#[test]
fn every_endpoint_key_parses_and_unknown_keys_refuse() {
    let every_key = [
        ("host", "db.example.com"),
        ("port", "6543"),
        ("database", "sales"),
        ("user", "app"),
        ("password", "pw"),
        (AUTH_METHOD_KEY, "password"),
        ("sslmode", "disable"),
        ("sslrootcert", "/etc/ssl/ca.pem"),
        ("connect_timeout_ms", "1500"),
        ("read_timeout_ms", "2500"),
        ("query_timeout_ms", "3500"),
        ("lock_timeout_ms", "0"),
        ("batch_rows", "1000"),
        ("prefer_timestamp_ntz", "true"),
        ("pushdown_predicate", "false"),
        ("pool_max_size", "64"),
        ("pool_checkout_timeout_ms", "100"),
        ("pool_idle_timeout_ms", "200"),
        ("application_name", "etl"),
    ];
    let given: Vec<&str> = every_key.iter().map(|(key, _)| *key).collect();
    let untested: Vec<&&str> = POSTGRES_KEYS
        .iter()
        .filter(|key| !given.contains(key))
        .collect();
    assert_eq!(untested, [&"url"]);
    let every = "app:Some(\"pw\")@db.example.com:6543/sales Password Disable Some(\"/etc/ssl/ca.pem\") \
                 connect=1.5s read=2.5s query=Some(3.5s) lock=0ns batch=Some(1000) ntz=true \
                 pushdown=false pool=64 checkout=100ms idle=200ms app=etl";
    assert_eq!(summary(&parse(&every_key, TOML)), every);
    let defaults = "app:None@h:5432/app Password VerifyFull None connect=10s read=60s query=None \
                    lock=10s batch=None ntz=false pushdown=true pool=4 checkout=30s idle=300s \
                    app=repark";
    assert_eq!(
        summary(&parse(&[("host", "h"), ("user", "app")], TOML)),
        defaults
    );
    let url = "postgresql://al%40ice:p%3Aw@db.example.com:6543/sales?sslmode=disable&application_name=etl";
    let libpq = summary(&parse(&[("url", url)], TOML));
    assert!(libpq.starts_with("al@ice:Some(\"p:w\")@db.example.com:6543/sales Password Disable"));
    assert!(libpq.ends_with(" app=etl"), "{libpq}");
    let url = "jdbc:postgresql://[::1]:6543/sales?user=app&password=pw&ApplicationName=etl";
    let jdbc = summary(&parse(&[("url", url)], SPARK));
    assert!(jdbc.starts_with("app:Some(\"pw\")@::1:6543/sales Password VerifyFull"));
    assert!(jdbc.ends_with(" app=etl"), "{jdbc}");
    let short = summary(&parse(&[("url", "postgres://h"), ("user", "app")], TOML));
    assert!(short.starts_with("app:None@h:5432/app "), "{short}");

    let secret = "SECRET_VALUE_never_echoed";
    let error = refuse_one("sslmod", secret, TOML);
    is_invalid(&error, key("sslmod"), unknown());
    let message = error.to_string();
    assert!(
        message.starts_with("invalid specification: `sslmod`"),
        "{message}"
    );
    assert!(message.contains(&POSTGRES_KEYS.join(", ")), "{message}");
    assert!(!message.contains(secret), "{message}");
    let error = refuse_one("queryTimeout", "5", TOML);
    is_invalid(&error, key("queryTimeout"), unknown());
    let message = refuse_one("fetchSize2", secret, SPARK).to_string();
    assert!(
        message.contains("queryTimeout") && message.contains("driver"),
        "{message}"
    );
    assert!(!message.contains(secret), "{message}");
    let url = format!("postgresql://h/db?colour={secret}");
    let error = refusal(&[("url", &url), ("user", "app")], TOML);
    is_invalid(&error, query("colour"), unknown());
    assert!(!error.to_string().contains(secret), "{error}");

    values_refuse_naming_the_key();
}

#[test]
fn aliases_are_case_insensitive_and_conflicts_refuse() {
    for spelling in ["queryTimeout", "QUERYTIMEOUT", "querytimeout"] {
        let settings = one(spelling, "5", SPARK);
        assert_eq!(
            settings.query_timeout,
            Some(Duration::from_secs(5)),
            "{spelling}"
        );
    }
    assert_eq!(
        one("FetchSize", "100", SPARK).batch_rows,
        NonZeroUsize::new(100)
    );
    assert!(one("preferTimestampNtz", "TRUE", SPARK).prefer_timestamp_ntz);
    assert!(!one("PUSHDOWNPREDICATE", "false", SPARK).pushdown_predicate);
    assert_eq!(one("applicationname", "etl", SPARK).application_name, "etl");
    assert_eq!(one("SSLMODE", "disable", SPARK).sslmode, SslMode::Disable);
    assert_eq!(
        parse(&[("URL", "postgresql://h/db"), ("user", "app")], SPARK).host,
        "h"
    );

    let pairs = [
        ("host", "h"),
        ("user", "app"),
        ("queryTimeout", "5"),
        ("QUERYTIMEOUT", "6"),
    ];
    let error = refusal(&pairs, SPARK);
    is_invalid(&error, key("QUERYTIMEOUT"), conflict(key("queryTimeout")));
    let message = error.to_string();
    assert!(
        message.contains("`QUERYTIMEOUT` and `queryTimeout`"),
        "{message}"
    );
    let pairs = [
        ("host", "h"),
        ("user", "app"),
        ("query_timeout_ms", "5000"),
        ("queryTimeout", "5"),
    ];
    let error = refusal(&pairs, SPARK);
    is_invalid(
        &error,
        key("queryTimeout"),
        conflict(key("query_timeout_ms")),
    );

    let error = refusal(&[("url", "postgresql://alice@h/db"), ("user", "bob")], TOML);
    is_invalid(&error, key("user"), conflict(Spelling::UrlPart("user")));
    assert!(error.to_string().contains("the user in `url`"), "{error}");
    let error = refusal(
        &[
            ("url", "jdbc:postgresql://h/db?USER=alice"),
            ("user", "bob"),
        ],
        SPARK,
    );
    is_invalid(&error, key("user"), conflict(query("USER")));
    let error = refusal(
        &[("url", "postgresql://h:5432/db?port=5433"), ("user", "app")],
        TOML,
    );
    is_invalid(&error, Spelling::UrlPart("port"), conflict(query("port")));
    let error = refusal(&[("url", "postgresql://h/db?user=a&user=b")], TOML);
    is_invalid(&error, query("user"), conflict(query("user")));
    let error = refusal(
        &[
            ("url", "postgresql://h/db?ApplicationName=etl"),
            ("user", "app"),
        ],
        SPARK,
    );
    is_invalid(&error, query("ApplicationName"), unknown());
}

#[test]
fn alias_units_convert() {
    assert_eq!(
        one("socketTimeout", "5", SPARK).read_timeout.as_millis(),
        5000
    );
    assert_eq!(
        one("connectTimeout", "3", SPARK)
            .connect_timeout
            .as_millis(),
        3000
    );
    assert_eq!(one("queryTimeout", "0", SPARK).query_timeout, None);
    assert_eq!(
        one("read_timeout_ms", "5", SPARK).read_timeout.as_millis(),
        5
    );
    let url = "jdbc:postgresql://h/db?user=app&socketTimeout=7";
    assert_eq!(parse(&[("url", url)], SPARK).read_timeout.as_millis(), 7000);
    rejects("connectTimeout", "2147484", SPARK, int(1, 2_147_483));
}

#[test]
fn sslmode_default_is_verify_full() {
    assert_eq!(
        parse(&[("host", "h"), ("user", "app")], TOML).sslmode,
        SslMode::VerifyFull
    );
    for (spelling, mode) in [
        ("verify-full", SslMode::VerifyFull),
        ("disable", SslMode::Disable),
    ] {
        assert_eq!(one("sslmode", spelling, TOML).sslmode, mode);
        assert_eq!(SslMode::from_spelling(spelling), Some(mode));
        assert_eq!(mode.spelling(), spelling);
    }
}

#[test]
fn unverified_sslmodes_refuse() {
    for (spelling, mode) in [
        ("prefer", SslMode::Prefer),
        ("allow", SslMode::Allow),
        ("require", SslMode::Require),
        ("verify-ca", SslMode::VerifyCa),
    ] {
        let error = refuse_one("sslmode", spelling, TOML);
        is_declared(
            &error,
            key("sslmode"),
            DeclaredSetting::UnverifiedSslmode(mode),
        );
        let message = error.to_string();
        assert!(
            message.contains("`verify-full` with `sslrootcert`"),
            "{message}"
        );
    }
    let error = refusal(
        &[
            ("url", "postgresql://h/db?sslmode=require"),
            ("user", "app"),
        ],
        TOML,
    );
    let declared = DeclaredSetting::UnverifiedSslmode(SslMode::Require);
    is_declared(&error, query("sslmode"), declared);
    for spelling in ["VERIFY-FULL", "verify_full", ""] {
        let error = refuse_one("sslmode", spelling, TOML);
        is_invalid(&error, key("sslmode"), SpecRefusal::Sslmode);
    }
}

#[test]
fn declared_keys_refuse_naming_their_row() {
    let cases = [
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
    for (name, declared) in cases {
        is_declared(&refuse_one(name, "x", SPARK), key(name), declared);
    }
    for (name, declared) in &cases[..5] {
        is_declared(&refuse_one(name, "x", TOML), key(name), *declared);
    }
    rejects("numPartitions", "4", TOML, unknown());
    let url = "postgresql://h/db?options=-c%20search_path%3Dpublic";
    let error = refusal(&[("url", url), ("user", "app")], TOML);
    is_declared(&error, query("options"), DeclaredSetting::SessionSql);
    let error = refusal(&[("host", "a,b"), ("user", "app")], TOML);
    is_declared(&error, key("host"), DeclaredSetting::MultiHost);
    let error = refusal(
        &[("url", "postgresql://h1:5432,h2:5433/db"), ("user", "app")],
        TOML,
    );
    is_declared(
        &error,
        Spelling::UrlPart("host"),
        DeclaredSetting::MultiHost,
    );
}

#[test]
fn read_timeout_zero_refuses() {
    let error = refuse_one("read_timeout_ms", "0", TOML);
    is_invalid(&error, key("read_timeout_ms"), SpecRefusal::ZeroReadTimeout);
    assert!(error.to_string().contains("NS-7"), "{error}");
    rejects("socketTimeout", "0", SPARK, SpecRefusal::ZeroReadTimeout);
    let error = refusal(
        &[("url", "jdbc:postgresql://h/db?user=app&SOCKETTIMEOUT=0")],
        SPARK,
    );
    is_invalid(&error, query("SOCKETTIMEOUT"), SpecRefusal::ZeroReadTimeout);
}

#[test]
fn redact_source_prop_masks_url_credentials() {
    let secret = "s3cretPW";
    let url = format!("postgresql://u:{secret}@h/db");
    assert_eq!(redact_source_prop("url", &url), "postgresql://u:***@h/db");
    let url = format!("jdbc:postgresql://h:5432/db?user=app&password={secret}&sslmode=disable");
    let masked = "jdbc:postgresql://h:5432/db?user=app&password=***&sslmode=disable";
    assert_eq!(redact_source_prop("url", &url), masked);
    let url = format!("postgresql://{secret}@h/db");
    assert_eq!(redact_source_prop("url", &url), "postgresql://***@h/db");
    assert_eq!(redact_source_prop("password", secret), "***");
    for unchanged in [
        "postgresql://h:5432/db",
        "postgresql://h:5432/db?application_name=a@b",
        "jdbc:postgresql://h:5432/db?user=app&ApplicationName=x@y:z",
    ] {
        assert_eq!(redact_source_prop("url", unchanged), unchanged);
    }
}

#[test]
fn settings_debug_never_renders_a_value() {
    let secret = "SUPER_SECRET_VALUE_do_not_leak";
    let url = format!("postgresql://app:{secret}@db.internal:5432/sales");
    let rendered = format!("{:?}", parse(&[("url", &url)], TOML));
    for leak in [secret, "db.internal", "sales", "app"] {
        assert!(!rendered.contains(leak), "leaked {leak}: {rendered}");
    }
    assert!(rendered.contains("VerifyFull"), "{rendered}");
    let mut refusals: Vec<ConnectError> = [
        ("password", TOML),
        ("colour", TOML),
        ("port", TOML),
        ("pushdown_predicate", TOML),
        ("sslmode", TOML),
        ("sslcert", TOML),
        ("driver", SPARK),
    ]
    .into_iter()
    .map(|(name, door)| refusal(&[("url", &url), (name, secret)], door))
    .collect();
    for bad in [
        format!("{url}?x={secret}%zz"),
        format!("mysql://u:{secret}@h"),
        format!("postgresql://u:{secret}@h/db?{secret}"),
        format!("postgresql://u:{secret}@a,b/db"),
        format!("postgresql://u:{secret}@[::1/db"),
        format!("postgresql://u:{secret}@h:{secret}/db"),
    ] {
        refusals.push(refusal(&[("url", &bad)], TOML));
    }
    for error in refusals {
        let (display, debug) = (error.to_string(), format!("{error:?}"));
        assert!(!display.contains(secret), "leaked: {display}");
        assert!(!debug.contains(secret), "leaked: {debug}");
    }
}
