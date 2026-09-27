use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::{PoisonError, RwLock};

use repark_common::Error;

use super::CatalogRegistry;

pub const SESSION_CATALOG_NAME: &str = "spark_catalog";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentCatalog {
    catalog: String,
    namespace: String,
    pinned: bool,
}

impl Default for CurrentCatalog {
    fn default() -> Self {
        Self::unpinned(SESSION_CATALOG_NAME)
    }
}

impl CurrentCatalog {
    fn unpinned(catalog: &str) -> Self {
        Self {
            catalog: catalog.to_string(),
            namespace: CatalogRegistry::default_namespace_for(catalog).to_string(),
            pinned: false,
        }
    }
}

#[must_use]
pub fn catalog_not_found_message(name: &str) -> String {
    format!(
        "[CATALOG_NOT_FOUND] The catalog `{name}` not found. Consider to set the SQL config \
         \"spark.sql.catalog.{name}\" to a catalog plugin. SQLSTATE: 42P08"
    )
}

impl CatalogRegistry {
    pub const DEFAULT_CATALOG_KEY: &'static str = "spark.sql.defaultCatalog";

    #[must_use]
    pub fn default_namespace_for(catalog: &str) -> &'static str {
        if catalog == SESSION_CATALOG_NAME {
            "default"
        } else {
            ""
        }
    }

    #[must_use]
    pub fn configured_default_catalog<S: BuildHasher>(
        config: &HashMap<String, String, S>,
    ) -> Option<String> {
        config
            .iter()
            .find(|(key, value)| {
                key.eq_ignore_ascii_case(Self::DEFAULT_CATALOG_KEY) && !value.is_empty()
            })
            .map(|(_, value)| value.clone())
    }

    #[must_use]
    pub fn with_session_catalogs<S: BuildHasher>(
        self,
        config: &HashMap<String, String, S>,
    ) -> Self {
        if let Some(name) = Self::configured_default_catalog(config) {
            *RwLock::write(&self.session_defaults).unwrap_or_else(PoisonError::into_inner) =
                CurrentCatalog::unpinned(&name);
        }
        self
    }

    #[must_use]
    pub fn current_defaults(&self) -> (String, String) {
        let current = RwLock::read(&self.session_defaults).unwrap_or_else(PoisonError::into_inner);
        (current.catalog.clone(), current.namespace.clone())
    }

    pub fn set_defaults(&self, catalog: &str, namespace: &str) {
        *RwLock::write(&self.session_defaults).unwrap_or_else(PoisonError::into_inner) =
            CurrentCatalog {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                pinned: true,
            };
    }

    #[must_use]
    pub fn apply_default_catalog(&self, name: Option<&str>) -> Option<(String, String)> {
        let mut current =
            RwLock::write(&self.session_defaults).unwrap_or_else(PoisonError::into_inner);
        if current.pinned {
            return None;
        }
        *current = CurrentCatalog::unpinned(name.unwrap_or(SESSION_CATALOG_NAME));
        Some((current.catalog.clone(), current.namespace.clone()))
    }

    #[must_use]
    pub fn current_catalog_error(&self) -> Option<Error> {
        let current = RwLock::read(&self.session_defaults)
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(refusal) = self.refusal(&current.catalog) {
            return Some(refusal.error());
        }
        let known = current.pinned
            || current.catalog == SESSION_CATALOG_NAME
            || self.is_registered(&current.catalog)
            || self.read_only_catalogs.contains(&current.catalog);
        (!known).then(|| Error::Analysis(catalog_not_found_message(&current.catalog)))
    }
}
