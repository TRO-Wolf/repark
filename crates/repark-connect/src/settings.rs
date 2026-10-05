use std::collections::BTreeMap;
use std::fmt;

use repark_common::Error;

pub const AUTH_METHOD_KEY: &str = "auth_method";

const REGISTRY: &str = "docs/spark-sql-iceberg-parity.md";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    Password,
    IamToken,
    Kerberos,
}

impl AuthMethod {
    pub const SPELLINGS: &[&str] = &["password", "iam_token", "kerberos"];

    #[must_use]
    pub fn from_spelling(spelling: &str) -> Option<AuthMethod> {
        match spelling {
            "password" => Some(AuthMethod::Password),
            "iam_token" => Some(AuthMethod::IamToken),
            "kerberos" => Some(AuthMethod::Kerberos),
            _ => None,
        }
    }

    #[must_use]
    pub fn spelling(self) -> &'static str {
        match self {
            AuthMethod::Password => "password",
            AuthMethod::IamToken => "iam_token",
            AuthMethod::Kerberos => "kerberos",
        }
    }

    #[must_use]
    pub fn declared_refusal(self) -> Option<&'static str> {
        match self {
            AuthMethod::Password => None,
            AuthMethod::IamToken => Some("CONNECT-DECL-auth-iam_token"),
            AuthMethod::Kerberos => Some("CONNECT-DECL-auth-kerberos"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SettingsError {
    #[error(
        "invalid specification: `{AUTH_METHOD_KEY}` is `{value}`; expected one of: {}",
        AuthMethod::SPELLINGS.join(", ")
    )]
    InvalidAuthMethod { value: String },

    #[error(
        "auth method `{}` is declared but not supported yet: only `password` connects \
         (registry row {registry_row} in {REGISTRY})",
        method.spelling()
    )]
    DeclaredAuthMethod {
        method: AuthMethod,
        registry_row: &'static str,
    },
}

impl From<SettingsError> for Error {
    fn from(error: SettingsError) -> Self {
        match error {
            SettingsError::InvalidAuthMethod { .. } => Error::Config(error.to_string()),
            SettingsError::DeclaredAuthMethod { .. } => Error::NotImplemented(error.to_string()),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConnectionSettings {
    auth_method: AuthMethod,
    props: BTreeMap<String, String>,
}

impl ConnectionSettings {
    #[allow(clippy::missing_errors_doc)]
    pub fn from_props(props: &BTreeMap<String, String>) -> Result<Self, SettingsError> {
        let auth_method = match props.get(AUTH_METHOD_KEY) {
            None => AuthMethod::Password,
            Some(value) => AuthMethod::from_spelling(value).ok_or_else(|| {
                SettingsError::InvalidAuthMethod {
                    value: value.clone(),
                }
            })?,
        };
        if let Some(registry_row) = auth_method.declared_refusal() {
            return Err(SettingsError::DeclaredAuthMethod {
                method: auth_method,
                registry_row,
            });
        }
        let props = props
            .iter()
            .filter(|(key, _)| key.as_str() != AUTH_METHOD_KEY)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(Self { auth_method, props })
    }

    #[must_use]
    pub fn auth_method(&self) -> AuthMethod {
        self.auth_method
    }

    #[must_use]
    pub fn props(&self) -> &BTreeMap<String, String> {
        &self.props
    }
}

impl fmt::Debug for ConnectionSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionSettings")
            .field("auth_method", &self.auth_method)
            .field("prop_keys", &self.props.keys().collect::<Vec<_>>())
            .finish()
    }
}
