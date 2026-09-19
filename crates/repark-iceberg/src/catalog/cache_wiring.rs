use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;

use iceberg::memory::MemoryCatalogBuilder;
use iceberg::{CacheScope, TableMetadataCache};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_s3tables::S3TablesCatalogBuilder;

use crate::catalog::caches::CatalogCaches;

pub(crate) trait CacheWiredBuilder: Sized {
    #[must_use]
    fn wire_metadata_cache(self, cache: Arc<TableMetadataCache>) -> Self;

    #[must_use]
    fn wire_manifest_cache_bytes(self, bytes: u64) -> Self;

    #[must_use]
    fn wire_credential_context(self, context: String) -> Self;
}

impl CacheWiredBuilder for MemoryCatalogBuilder {
    fn wire_metadata_cache(self, cache: Arc<TableMetadataCache>) -> Self {
        self.with_table_metadata_cache(cache)
    }

    fn wire_manifest_cache_bytes(self, bytes: u64) -> Self {
        self.with_shared_object_cache_bytes(bytes)
    }

    fn wire_credential_context(self, context: String) -> Self {
        self.with_cache_credential_context(context)
    }
}

impl CacheWiredBuilder for GlueCatalogBuilder {
    fn wire_metadata_cache(self, cache: Arc<TableMetadataCache>) -> Self {
        self.with_table_metadata_cache(cache)
    }

    fn wire_manifest_cache_bytes(self, bytes: u64) -> Self {
        self.with_shared_object_cache_bytes(bytes)
    }

    fn wire_credential_context(self, context: String) -> Self {
        self.with_cache_credential_context(context)
    }
}

impl CacheWiredBuilder for S3TablesCatalogBuilder {
    fn wire_metadata_cache(self, cache: Arc<TableMetadataCache>) -> Self {
        self.with_table_metadata_cache(cache)
    }

    fn wire_manifest_cache_bytes(self, bytes: u64) -> Self {
        self.with_shared_object_cache_bytes(bytes)
    }

    fn wire_credential_context(self, context: String) -> Self {
        self.with_cache_credential_context(context)
    }
}

#[must_use]
pub(crate) fn cache_credential_context<S: BuildHasher>(
    props: &HashMap<String, String, S>,
) -> Option<String> {
    let props: HashMap<String, String> =
        props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    CacheScope::credential_context_from_props(&props)
}

#[must_use]
pub(crate) fn wire_caches<B: CacheWiredBuilder, S: BuildHasher>(
    builder: B,
    caches: &CatalogCaches,
    props: &HashMap<String, String, S>,
) -> B {
    let Some(metadata) = caches.metadata_cache() else {
        return wire_manifest_cache(builder, caches);
    };
    let builder = wire_manifest_cache(builder.wire_metadata_cache(metadata), caches);
    match cache_credential_context(props) {
        Some(context) => builder.wire_credential_context(context),
        None => builder,
    }
}

fn wire_manifest_cache<B: CacheWiredBuilder>(builder: B, caches: &CatalogCaches) -> B {
    match caches.manifest_cache_bytes() {
        0 => builder,
        bytes => builder.wire_manifest_cache_bytes(bytes),
    }
}
