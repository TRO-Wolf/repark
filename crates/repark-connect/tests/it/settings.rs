use std::collections::BTreeMap;

use repark_common::{Error, ErrorClass};
use repark_connect::{AUTH_METHOD_KEY, AuthMethod, ConnectError, ConnectionSettings};

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
