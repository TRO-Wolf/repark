use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::SchemaReference;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use repark_common::{Error, Result};

use repark_common::SourceKind;
use repark_connect::{read_only_ddl, redact_source_prop};

use crate::catalog_state::{CatalogRegistry, SourceMount};
use crate::config_file::sources::SourceSpec;
use crate::engine_err;
use crate::extension::SessionExtension;
use crate::session::ReparkSession;

#[cfg(test)]
mod tests;

pub(crate) fn refuse_source_ddl(
    plan: &LogicalPlan,
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) -> std::result::Result<(), DataFusionError> {
    let LogicalPlan::Ddl(ddl) = plan else {
        return Ok(());
    };
    let config = ctx.copied_config();
    let default_catalog = config.options().catalog.default_catalog.as_str();
    let claimed = |name: Option<&str>| name.and_then(|name| catalogs.database_source(name));
    let spec = match ddl {
        DdlStatement::CreateExternalTable(create) => claimed(create.name.catalog()),
        DdlStatement::CreateMemoryTable(create) => claimed(create.name.catalog()),
        DdlStatement::CreateView(create) => claimed(create.name.catalog()),
        DdlStatement::CreateIndex(create) => claimed(create.table.catalog()),
        DdlStatement::DropTable(drop) => claimed(drop.name.catalog()),
        DdlStatement::DropView(drop) => claimed(drop.name.catalog()),
        DdlStatement::CreateCatalog(create) => claimed(Some(create.catalog_name.as_str()))
            .or_else(|| claimed(dotted_head(&create.catalog_name))),
        DdlStatement::CreateCatalogSchema(create) => claimed(Some(
            dotted_head(&create.schema_name).unwrap_or(default_catalog),
        )),
        DdlStatement::DropCatalogSchema(drop) => match &drop.name {
            SchemaReference::Full { catalog, .. } => claimed(Some(catalog.as_ref())),
            SchemaReference::Bare { .. } => claimed(Some(default_catalog)),
        },
        DdlStatement::CreateFunction(_) | DdlStatement::DropFunction(_) => None,
    };
    if let Some(spec) = spec {
        let message = match spec.identity.kind {
            SourceKind::Postgres => read_only_ddl(&spec.key_path()),
            SourceKind::SqlServer | SourceKind::Trino => source_refusal(spec),
        };
        return Err(DataFusionError::NotImplemented(message));
    }
    Ok(())
}

fn dotted_head(name: &str) -> Option<&str> {
    name.split_once('.').map(|(head, _)| head)
}

fn source_refusal(spec: &SourceSpec) -> String {
    if spec.identity.kind == SourceKind::Postgres {
        return format!(
            "database source `{}` (kind `postgres`) cannot be used: the Postgres connector is \
             not compiled into this build",
            spec.key_path()
        );
    }
    connector_pending_message(&spec.key_path(), spec.identity.kind.spelling())
}

fn connector_pending_message(key_path: &str, kind: &str) -> String {
    format!(
        "database source `{key_path}` (kind `{kind}`) is declared but cannot be used yet — \
         its connector arrives with roadmap 1.10 (Postgres, SQL Server, Trino)"
    )
}

#[derive(Debug)]
struct RefusingSourceSchemaProvider {
    message: String,
}

#[async_trait]
impl SchemaProvider for RefusingSourceSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        Vec::new()
    }

    async fn table(
        &self,
        _name: &str,
    ) -> std::result::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Err(DataFusionError::NotImplemented(self.message.clone()))
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> std::result::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Err(DataFusionError::NotImplemented(self.message.clone()))
    }

    fn deregister_table(
        &self,
        _name: &str,
    ) -> std::result::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Err(DataFusionError::NotImplemented(self.message.clone()))
    }

    fn table_exist(&self, _name: &str) -> bool {
        false
    }
}

#[derive(Debug)]
pub(crate) struct RefusingSourceCatalogProvider {
    schema: Arc<RefusingSourceSchemaProvider>,
}

impl RefusingSourceCatalogProvider {
    pub(crate) fn new(spec: &SourceSpec) -> Self {
        Self {
            schema: Arc::new(RefusingSourceSchemaProvider {
                message: source_refusal(spec),
            }),
        }
    }
}

impl CatalogProvider for RefusingSourceCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::clone(&self.schema) as Arc<dyn SchemaProvider>)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRow {
    pub name: String,
    pub kind: String,
    pub key_path: String,
    pub auto_register: bool,
    pub properties: BTreeMap<String, String>,
}

impl SourceRow {
    fn from_spec(spec: &SourceSpec) -> Self {
        Self {
            name: spec.identity.name.clone(),
            kind: spec.identity.kind.spelling().to_string(),
            key_path: spec.key_path(),
            auto_register: spec.auto_register,
            properties: spec
                .props
                .iter()
                .map(|(key, value)| (key.clone(), redact_source_prop(key, value)))
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NamedSource {
    name: String,
    kind: String,
    key_path: String,
    refusal: String,
    #[cfg(feature = "postgres")]
    postgres: Option<Arc<repark_connect::PostgresSource>>,
}

impl NamedSource {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    #[must_use]
    pub fn key_path(&self) -> &str {
        &self.key_path
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn ping(&self) -> Result<()> {
        #[cfg(feature = "postgres")]
        if let Some(source) = &self.postgres {
            return source.ping().await.map_err(|error| {
                crate::session::read_postgres::source_error(&self.key_path, error)
            });
        }
        Err(Error::NotImplemented(self.refusal.clone()))
    }
}

impl ReparkSession {
    fn mounted_source(&self, name: &str) -> Option<Arc<SourceSpec>> {
        self.catalogs
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .database_source(name)
            .cloned()
    }

    pub(crate) fn source_catalog_refusal(&self, name: &str) -> Option<Error> {
        let spec = self.mounted_source(name)?;
        Some(Error::NotImplemented(match spec.identity.kind {
            SourceKind::Postgres => read_only_ddl(&spec.key_path()),
            SourceKind::SqlServer | SourceKind::Trino => source_refusal(&spec),
        }))
    }

    pub(crate) async fn source_table_exists(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<Result<bool>> {
        self.mounted_source(catalog)?;
        let schema = self
            .context()
            .catalog(catalog)
            .and_then(|provider| provider.schema(namespace));
        let Some(schema) = schema else {
            return Some(Ok(false));
        };
        Some(
            schema
                .table(table)
                .await
                .map(|found| found.is_some())
                .map_err(engine_err),
        )
    }

    #[must_use]
    pub fn sources(&self) -> Vec<SourceRow> {
        self.source_specs
            .iter()
            .map(|spec| SourceRow::from_spec(spec))
            .collect()
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn source(&self, name: &str) -> Result<NamedSource> {
        self.source_specs
            .iter()
            .find(|spec| spec.identity.name == name)
            .map(|spec| self.named_source(spec))
            .ok_or_else(|| {
                let declared: Vec<&str> = self
                    .source_specs
                    .iter()
                    .map(|spec| spec.identity.name.as_str())
                    .collect();
                let list = if declared.is_empty() {
                    "none".to_string()
                } else {
                    declared.join(", ")
                };
                Error::DataFusion(format!(
                    "unknown database source '{name}' — declared sources: {list}"
                ))
            })
    }

    fn named_source(&self, spec: &SourceSpec) -> NamedSource {
        NamedSource {
            name: spec.identity.name.clone(),
            kind: spec.identity.kind.spelling().to_string(),
            key_path: spec.key_path(),
            refusal: source_refusal(spec),
            #[cfg(feature = "postgres")]
            postgres: SourceMount::mounts_postgres(spec).then(|| self.postgres_source(spec)),
        }
    }

    #[cfg(feature = "postgres")]
    fn postgres_source(&self, spec: &SourceSpec) -> Arc<repark_connect::PostgresSource> {
        let mounted = self
            .context()
            .catalog(&spec.identity.name)
            .and_then(|catalog| {
                catalog
                    .downcast_ref::<repark_connect::PostgresCatalog>()
                    .map(|catalog| Arc::clone(catalog.source()))
            });
        mounted.unwrap_or_else(|| {
            repark_connect::PostgresSource::new(
                &spec.identity,
                spec.props.clone(),
                repark_connect::SettingsDoor::ReparkToml,
                Arc::new(self.zone_localiser()),
            )
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn register_configured_sources(&self) -> Result<()> {
        let mount = SourceMount::new(Arc::clone(&self.source_specs), self.zone_localiser());
        let mut catalogs = self
            .catalogs
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        for spec in mount.mounted() {
            if catalogs.is_registered(&spec.identity.name)
                || self.context().catalog(&spec.identity.name).is_some()
            {
                return Err(Error::DataFusion(format!(
                    "catalog '{}' is already registered",
                    spec.identity.name
                )));
            }
        }
        mount.register(self.context()).map_err(engine_err)?;
        for spec in mount.mounted() {
            catalogs.insert_database_source(Arc::clone(spec));
        }
        drop(catalogs);
        self.note_postgres_catalog_names(
            mount
                .mounted()
                .filter(|spec| SourceMount::mounts_postgres(spec))
                .map(|spec| spec.identity.name.clone()),
        );
        Ok(())
    }
}
