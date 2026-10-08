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
    pub partition_column: Option<String>,
    pub lower_bound: Option<i64>,
    pub upper_bound: Option<i64>,
    pub num_partitions: Option<i64>,
    pub predicates: bool,
}

impl std::fmt::Debug for PostgresRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresRead")
            .field("partition_column", &self.partition_column)
            .field("num_partitions", &self.num_partitions)
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

    use datafusion::common::TableReference;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use repark_common::SourceIdentity;
    use repark_common::source::SourceKind;
    use repark_connect::{
        ConnectError, DeclaredSetting, PartitionOptions, PartitionRefusal, PostgresSource,
        ScanSource, SettingsDoor, Spelling,
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
            Error::Analysis(_) => Error::Analysis(message),
            Error::NumberFormat(_) => Error::NumberFormat(message),
            _ => Error::DataFusion(message),
        }
    }

    impl ReparkSession {
        #[allow(clippy::missing_errors_doc)]
        pub async fn read_postgres(&self, read: PostgresRead) -> Result<DataFrame> {
            let refuse = |error| source_error(READ_POSTGRES_SOURCE, error);
            if read.predicates {
                return Err(refuse(ConnectError::DeclaredSetting {
                    key: Spelling::Key("predicates".to_string()),
                    declared: DeclaredSetting::PartitionedRead,
                }));
            }
            let mut props = read.properties;
            props.retain(|key, _| !key.eq_ignore_ascii_case("dbtable"));
            let partitioning = PartitionOptions::of(
                read.partition_column,
                read.lower_bound,
                read.upper_bound,
                read.num_partitions,
            );
            let partitioning = partitioning
                .with_props(&mut props)
                .and_then(PartitionOptions::spec)
                .map_err(refuse)?;
            if partitioning.is_some() && matches!(read.target, PostgresTarget::Query(_)) {
                return Err(refuse(ConnectError::PartitionedRead {
                    refusal: PartitionRefusal::QueryOption,
                }));
            }
            let target = match &read.target {
                PostgresTarget::Relation(dbtable) => {
                    ScanSource::from_dbtable(dbtable).map_err(refuse)?
                }
                PostgresTarget::Query(query) => ScanSource::query(query),
            };
            props.insert("url".to_string(), read.url);
            let identity =
                SourceIdentity::unassigned(READ_POSTGRES_SOURCE.to_string(), SourceKind::Postgres);
            let localiser = Arc::new(self.zone_localiser());
            let source =
                PostgresSource::new(&identity, props, SettingsDoor::ReadPostgres, localiser);
            let name = match &target {
                ScanSource::Relation(relation) => {
                    TableReference::partial(relation.schema.as_str(), relation.table.as_str())
                }
                ScanSource::Query(_) => TableReference::bare(READ_POSTGRES_SOURCE),
            };
            let table = source.resolve(target).await.map_err(refuse)?;
            let table = match &partitioning {
                Some(partitioning) => table.partitioned(partitioning).map_err(refuse)?,
                None => table,
            };
            let plan = LogicalPlanBuilder::scan(name, provider_as_source(Arc::new(table)), None)
                .and_then(LogicalPlanBuilder::build)
                .map_err(engine_err)?;
            Ok(DataFrame::new(self.context().state(), plan))
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
