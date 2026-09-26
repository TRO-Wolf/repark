use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::{PoisonError, RwLock};

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
}
