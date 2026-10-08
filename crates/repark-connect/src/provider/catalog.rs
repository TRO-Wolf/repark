use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use repark_common::SourceIdentity;

use super::scan::WallClockLocaliser;
use super::schema::PostgresSchemaProvider;
use super::table::PostgresTable;
use crate::discover::{ResolvedSource, ScanSource, discover};
use crate::error::Result;
use crate::pool::{PoolLimits, PostgresConnector, PostgresPool, QueryPool, TimeoutSetting, within};
use crate::read::postgres::request_error;
use crate::settings::{PostgresSettings, SettingsDoor};

pub const LISTING_ROW: &str = "CONNECT-DECL-pg-listing";

pub(crate) struct Mounted {
    pub(crate) settings: PostgresSettings,
    pub(crate) pool: Arc<PostgresPool>,
}

impl fmt::Debug for Mounted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mounted")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

pub struct PostgresSource {
    name: Arc<str>,
    door: SettingsDoor,
    props: BTreeMap<String, String>,
    localiser: Arc<dyn WallClockLocaliser>,
    mounted: OnceLock<Result<Arc<Mounted>>>,
}

impl fmt::Debug for PostgresSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresSource")
            .field("name", &self.name)
            .field("door", &self.door)
            .finish_non_exhaustive()
    }
}

impl PostgresSource {
    #[must_use]
    pub fn new(
        identity: &SourceIdentity,
        props: BTreeMap<String, String>,
        door: SettingsDoor,
        localiser: Arc<dyn WallClockLocaliser>,
    ) -> Arc<PostgresSource> {
        Arc::new(PostgresSource {
            name: identity.name.as_str().into(),
            door,
            props,
            localiser,
            mounted: OnceLock::new(),
        })
    }

    #[must_use]
    pub fn mount(
        identity: &SourceIdentity,
        props: BTreeMap<String, String>,
        localiser: Arc<dyn WallClockLocaliser>,
    ) -> Arc<dyn CatalogProvider> {
        let source = PostgresSource::new(identity, props, SettingsDoor::ReparkToml, localiser);
        Arc::new(PostgresCatalog { source })
    }

    #[must_use]
    pub fn name(&self) -> &Arc<str> {
        &self.name
    }

    pub(crate) fn localiser(&self) -> &Arc<dyn WallClockLocaliser> {
        &self.localiser
    }

    pub(crate) fn mounted(&self) -> Result<Arc<Mounted>> {
        self.mounted
            .get_or_init(|| {
                let settings = PostgresSettings::from_props(&self.props, self.door)?;
                let connector = PostgresConnector::new(&settings)?;
                let pool = QueryPool::new(connector, PoolLimits::from_settings(&settings));
                Ok(Arc::new(Mounted { settings, pool }))
            })
            .clone()
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn resolve(self: &Arc<Self>, source: ScanSource) -> Result<PostgresTable> {
        let mounted = self.mounted()?;
        let resolved = discover(&mounted.pool, &source, mounted.settings.read_timeout).await?;
        Ok(PostgresTable::new(Arc::clone(self), mounted, resolved))
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn ping(&self) -> Result<()> {
        let mounted = self.mounted()?;
        let pooled = mounted.pool.checkout().await?;
        let probe = async {
            pooled
                .client()
                .simple_query("SELECT 1")
                .await
                .map(drop)
                .map_err(|error| request_error(&error, None))
        };
        within(TimeoutSetting::Read, mounted.settings.read_timeout, probe).await?;
        pooled.release_clean().await;
        Ok(())
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn table(self: &Arc<Self>, resolved: ResolvedSource) -> Result<PostgresTable> {
        Ok(PostgresTable::new(
            Arc::clone(self),
            self.mounted()?,
            resolved,
        ))
    }
}

#[derive(Debug)]
pub struct PostgresCatalog {
    source: Arc<PostgresSource>,
}

impl PostgresCatalog {
    #[must_use]
    pub fn source(&self) -> &Arc<PostgresSource> {
        &self.source
    }
}

impl CatalogProvider for PostgresCatalog {
    fn schema_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schema = PostgresSchemaProvider::new(Arc::clone(&self.source), name);
        Some(Arc::new(schema))
    }
}
