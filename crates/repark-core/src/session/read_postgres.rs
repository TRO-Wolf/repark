use std::collections::BTreeMap;
use std::sync::PoisonError;

use datafusion::prelude::DataFrame;
use repark_common::{Error, Result};

use super::ReparkSession;

pub const READ_POSTGRES_SOURCE: &str = "jdbc";

#[derive(Clone, PartialEq, Eq)]
pub enum PostgresTarget {
    Relation(String),
    Query(String),
}

#[derive(Clone, PartialEq, Eq)]
pub struct PostgresRead {
    pub url: String,
    pub target: PostgresTarget,
    pub properties: BTreeMap<String, String>,
    pub partitioning: Vec<&'static str>,
}

impl std::fmt::Debug for PostgresRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresRead")
            .field("partitioning", &self.partitioning)
            .finish_non_exhaustive()
    }
}

impl ReparkSession {
    pub(crate) fn note_postgres_catalog_names(&self, names: impl IntoIterator<Item = String>) {
        self.postgres_catalog_names
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(names);
    }
}

#[cfg(feature = "postgres")]
mod door {
    use std::sync::Arc;

    use repark_common::SourceIdentity;
    use repark_common::source::SourceKind;
    use repark_connect::{
        ConnectError, DeclaredSetting, PostgresSource, ScanSource, SettingsDoor, Spelling,
    };

    use super::{
        DataFrame, Error, PostgresRead, PostgresTarget, READ_POSTGRES_SOURCE, ReparkSession, Result,
    };
    use crate::engine_err;

    #[must_use]
    pub(crate) fn source_error(source: &str, error: ConnectError) -> Error {
        let message = format!("database source `{source}`: {error}");
        match Error::from(error) {
            Error::Config(_) => Error::Config(message),
            Error::NotImplemented(_) => Error::NotImplemented(message),
            _ => Error::DataFusion(message),
        }
    }

    impl ReparkSession {
        #[allow(clippy::missing_errors_doc)]
        pub async fn read_postgres(&self, read: PostgresRead) -> Result<DataFrame> {
            let refuse = |error| source_error(READ_POSTGRES_SOURCE, error);
            if let Some(first) = read.partitioning.first() {
                return Err(refuse(ConnectError::DeclaredSetting {
                    key: Spelling::Key((*first).to_string()),
                    declared: DeclaredSetting::PartitionedRead,
                }));
            }
            let target = match &read.target {
                PostgresTarget::Relation(dbtable) => {
                    ScanSource::from_dbtable(dbtable).map_err(refuse)?
                }
                PostgresTarget::Query(query) => ScanSource::query(query),
            };
            let mut props = read.properties;
            props.retain(|key, _| !key.eq_ignore_ascii_case("dbtable"));
            props.insert("url".to_string(), read.url);
            let identity =
                SourceIdentity::unassigned(READ_POSTGRES_SOURCE.to_string(), SourceKind::Postgres);
            let localiser = Arc::new(self.zone_localiser());
            let source =
                PostgresSource::new(&identity, props, SettingsDoor::ReadPostgres, localiser);
            let table = source.resolve(target).await.map_err(refuse)?;
            self.context()
                .read_table(Arc::new(table))
                .map_err(engine_err)
        }
    }
}

#[cfg(feature = "postgres")]
pub(crate) use door::source_error;

#[cfg(not(feature = "postgres"))]
impl ReparkSession {
    #[allow(clippy::missing_errors_doc, clippy::unused_async)]
    pub async fn read_postgres(&self, read: PostgresRead) -> Result<DataFrame> {
        let _ = read;
        Err(Error::NotImplemented(
            "the Postgres connector is not compiled into this build".to_string(),
        ))
    }
}
