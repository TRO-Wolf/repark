use std::sync::{Arc, PoisonError, RwLock};

use iceberg::Catalog;
use repark_common::Result;
use repark_iceberg::catalog::CatalogCaches;

use crate::catalog_state::CatalogRegistry;
use crate::engine_err;
use crate::session::ReparkSession;

pub(crate) fn caches_of(registry: &RwLock<CatalogRegistry>) -> Arc<CatalogCaches> {
    RwLock::read(registry)
        .unwrap_or_else(PoisonError::into_inner)
        .iceberg_caches()
}

impl ReparkSession {
    pub(crate) async fn memory_catalog_handle(&self, warehouse: &str) -> Result<Arc<dyn Catalog>> {
        let caches = caches_of(&self.catalogs);
        repark_iceberg::catalog::memory_catalog_cached(warehouse, &caches)
            .await
            .map_err(engine_err)
    }

    pub(crate) fn trim_iceberg_caches(&self) {
        caches_of(&self.catalogs).trim();
    }

    #[must_use]
    pub fn iceberg_metadata_cache_entries(&self) -> usize {
        caches_of(&self.catalogs).metadata_len()
    }

    #[must_use]
    pub fn iceberg_metadata_cache_stats(&self) -> Option<(u64, u64, u64)> {
        caches_of(&self.catalogs)
            .metadata_stats()
            .map(|stats| (stats.hits, stats.misses, stats.body_fetches))
    }
}
