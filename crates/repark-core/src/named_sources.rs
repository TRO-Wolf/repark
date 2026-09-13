use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use repark_common::{Error, Result};

use crate::config_file::redact::redact_value;
use crate::config_file::sources::SourceSpec;
use crate::session::ReparkSession;

#[cfg(test)]
mod tests;

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

    fn table_exist(&self, _name: &str) -> bool {
        false
    }
}

#[derive(Debug)]
struct RefusingSourceCatalogProvider {
    schema: Arc<RefusingSourceSchemaProvider>,
}

impl RefusingSourceCatalogProvider {
    fn new(spec: &SourceSpec) -> Self {
        Self {
            schema: Arc::new(RefusingSourceSchemaProvider {
                message: connector_pending_message(&spec.key_path(), spec.kind.spelling()),
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
            name: spec.name.clone(),
            kind: spec.kind.spelling().to_string(),
            key_path: spec.key_path(),
            auto_register: spec.auto_register,
            properties: spec
                .props
                .iter()
                .map(|(key, value)| (key.clone(), redact_value(key, value)))
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NamedSource {
    name: String,
    kind: String,
    key_path: String,
}

impl NamedSource {
    fn from_spec(spec: &SourceSpec) -> Self {
        Self {
            name: spec.name.clone(),
            kind: spec.kind.spelling().to_string(),
            key_path: spec.key_path(),
        }
    }

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

    /// # Errors
    /// Always [`Error::NotImplemented`] until the source's connector lands.
    #[allow(clippy::unnecessary_wraps)]
    pub fn ping(&self) -> Result<()> {
        Err(Error::NotImplemented(connector_pending_message(
            &self.key_path,
            &self.kind,
        )))
    }
}

impl ReparkSession {
    #[must_use]
    pub fn sources(&self) -> Vec<SourceRow> {
        self.source_specs.iter().map(SourceRow::from_spec).collect()
    }

    /// # Errors
    /// Returns [`Error::DataFusion`] naming the declared sources when `name` is undeclared.
    pub fn source(&self, name: &str) -> Result<NamedSource> {
        self.source_specs
            .iter()
            .find(|spec| spec.name == name)
            .map(NamedSource::from_spec)
            .ok_or_else(|| {
                let declared: Vec<&str> = self
                    .source_specs
                    .iter()
                    .map(|spec| spec.name.as_str())
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

    /// # Errors
    /// Returns [`Error::DataFusion`] when a source name collides with a registered name.
    pub fn register_configured_sources(&self) -> Result<()> {
        for spec in self.source_specs.iter() {
            if !spec.auto_register {
                continue;
            }
            let provider = RefusingSourceCatalogProvider::new(spec);
            let mut catalogs = self
                .catalogs
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            if catalogs.is_registered(&spec.name) || self.context().catalog(&spec.name).is_some() {
                return Err(Error::DataFusion(format!(
                    "catalog '{}' is already registered",
                    spec.name
                )));
            }
            self.context()
                .register_catalog(spec.name.clone(), Arc::new(provider));
            catalogs.insert_database_source(spec.clone());
        }
        Ok(())
    }
}
