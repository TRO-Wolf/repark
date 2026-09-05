use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use iceberg::{TableMetadataCache, TableMetadataCacheStats};

pub const METADATA_CACHE_KEY: &str = "repark.iceberg.metadataCache";

pub const METADATA_CACHE_KEY_ALT: &str = "repark.iceberg.metadata_cache";

pub const METADATA_CACHE_ENTRIES_KEY: &str = "repark.iceberg.metadataCacheEntries";

pub const METADATA_CACHE_ENTRIES_KEY_ALT: &str = "repark.iceberg.metadata_cache_entries";

pub const DEFAULT_METADATA_CACHE_ENTRIES: usize = 512;

pub const MANIFEST_CACHE_BYTES_KEY: &str = "repark.iceberg.manifestCacheBytes";

pub const MANIFEST_CACHE_BYTES_KEY_ALT: &str = "repark.iceberg.manifest_cache_bytes";

pub const DEFAULT_MANIFEST_CACHE_BYTES: u64 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IcebergCacheSettings {
    pub metadata_cache: bool,
    pub metadata_cache_entries: usize,
    pub manifest_cache_bytes: u64,
}

impl Default for IcebergCacheSettings {
    fn default() -> Self {
        Self {
            metadata_cache: true,
            metadata_cache_entries: DEFAULT_METADATA_CACHE_ENTRIES,
            manifest_cache_bytes: DEFAULT_MANIFEST_CACHE_BYTES,
        }
    }
}

impl IcebergCacheSettings {
    /// # Errors
    /// Returns a plan error naming the key when a value is not a boolean / positive integer.
    pub fn from_config_map<S: BuildHasher>(config: &HashMap<String, String, S>) -> Result<Self> {
        let mut settings = Self::default();
        if let Some((raw, key)) = lookup(config, METADATA_CACHE_KEY, METADATA_CACHE_KEY_ALT) {
            settings.metadata_cache = parse_bool(raw, key, METADATA_CACHE_KEY)?;
        }
        if let Some((raw, key)) = lookup(
            config,
            METADATA_CACHE_ENTRIES_KEY,
            METADATA_CACHE_ENTRIES_KEY_ALT,
        ) {
            settings.metadata_cache_entries = parse_entries(raw, key, METADATA_CACHE_ENTRIES_KEY)?;
        }
        if let Some((raw, key)) = lookup(
            config,
            MANIFEST_CACHE_BYTES_KEY,
            MANIFEST_CACHE_BYTES_KEY_ALT,
        ) {
            settings.manifest_cache_bytes = parse_bytes(raw, key, MANIFEST_CACHE_BYTES_KEY)?;
        }
        Ok(settings)
    }
}

fn lookup<'a, 'k, S: BuildHasher>(
    config: &'a HashMap<String, String, S>,
    key: &'k str,
    alt: &'k str,
) -> Option<(&'a str, &'k str)> {
    if let Some(value) = config.get(key) {
        return Some((value.as_str(), key));
    }
    config.get(alt).map(|value| (value.as_str(), alt))
}

fn named(key: &str, canonical: &str) -> String {
    if key == canonical {
        format!("`{canonical}`")
    } else {
        format!("`{key}` (alias of `{canonical}`)")
    }
}

fn parse_bool(raw: &str, key: &str, canonical: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(DataFusionError::Plan(format!(
            "config {} must be `true` or `false` (got {other:?})",
            named(key, canonical)
        ))),
    }
}

fn parse_entries(raw: &str, key: &str, canonical: &str) -> Result<usize> {
    let value: usize = raw.trim().parse().map_err(|_| {
        DataFusionError::Plan(format!(
            "config {} must be a positive integer (got {raw:?})",
            named(key, canonical)
        ))
    })?;
    if value == 0 {
        return Err(DataFusionError::Plan(format!(
            "config {} must be >= 1 (got 0)",
            named(key, canonical)
        )));
    }
    Ok(value)
}

fn parse_bytes(raw: &str, key: &str, canonical: &str) -> Result<u64> {
    raw.trim().parse().map_err(|_| {
        DataFusionError::Plan(format!(
            "config {} must be an integer in [0, 2^64) (got {raw:?})",
            named(key, canonical)
        ))
    })
}

#[derive(Debug, Clone)]
pub struct CatalogCaches {
    metadata: Option<Arc<TableMetadataCache>>,
    metadata_entries: usize,
    manifest_bytes: u64,
}

impl Default for CatalogCaches {
    fn default() -> Self {
        Self::new(IcebergCacheSettings::default())
    }
}

impl CatalogCaches {
    #[must_use]
    pub fn new(settings: IcebergCacheSettings) -> Self {
        Self {
            metadata: settings
                .metadata_cache
                .then(|| Arc::new(TableMetadataCache::new())),
            metadata_entries: settings.metadata_cache_entries,
            manifest_bytes: settings.manifest_cache_bytes,
        }
    }

    #[must_use]
    pub fn disabled() -> Self {
        Self::new(IcebergCacheSettings {
            metadata_cache: false,
            manifest_cache_bytes: 0,
            ..IcebergCacheSettings::default()
        })
    }

    #[must_use]
    pub fn metadata_cache(&self) -> Option<Arc<TableMetadataCache>> {
        self.metadata.clone()
    }

    #[must_use]
    pub fn manifest_cache_bytes(&self) -> u64 {
        self.manifest_bytes
    }

    #[must_use]
    pub fn metadata_len(&self) -> usize {
        self.metadata.as_ref().map_or(0, |cache| cache.len())
    }

    #[must_use]
    pub fn metadata_stats(&self) -> Option<TableMetadataCacheStats> {
        self.metadata.as_ref().map(|cache| cache.stats())
    }

    pub fn trim(&self) {
        if let Some(cache) = self.metadata.as_ref()
            && cache.len() > self.metadata_entries
        {
            cache.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_of(key: &str, value: &str) -> HashMap<String, String> {
        HashMap::from([(key.to_string(), value.to_string())])
    }

    #[test]
    fn the_default_disables_the_shared_cache() {
        assert_eq!(IcebergCacheSettings::default().manifest_cache_bytes, 0);
        assert_eq!(
            CatalogCaches::default().manifest_cache_bytes(),
            DEFAULT_MANIFEST_CACHE_BYTES
        );
    }

    #[test]
    fn both_spellings_size_the_cache() {
        for key in [MANIFEST_CACHE_BYTES_KEY, MANIFEST_CACHE_BYTES_KEY_ALT] {
            let settings =
                IcebergCacheSettings::from_config_map(&config_of(key, "1048576")).unwrap();
            assert_eq!(settings.manifest_cache_bytes, 1_048_576);
            assert_eq!(
                CatalogCaches::new(settings).manifest_cache_bytes(),
                1_048_576
            );
        }
    }

    #[test]
    fn zero_disables_the_shared_cache() {
        let settings =
            IcebergCacheSettings::from_config_map(&config_of(MANIFEST_CACHE_BYTES_KEY, "0"))
                .unwrap();
        assert_eq!(settings.manifest_cache_bytes, 0);
        assert_eq!(CatalogCaches::disabled().manifest_cache_bytes(), 0);
    }

    #[test]
    fn a_bad_value_fails_loud_naming_the_key() {
        for value in ["many", "-1", ""] {
            let error =
                IcebergCacheSettings::from_config_map(&config_of(MANIFEST_CACHE_BYTES_KEY, value))
                    .unwrap_err()
                    .to_string();
            assert!(error.contains(MANIFEST_CACHE_BYTES_KEY), "got: {error}");
        }
    }

    #[test]
    fn a_bad_alias_names_the_key_set_and_the_canonical_one() {
        let error =
            IcebergCacheSettings::from_config_map(&config_of(MANIFEST_CACHE_BYTES_KEY_ALT, "many"))
                .unwrap_err()
                .to_string();
        assert!(error.contains(MANIFEST_CACHE_BYTES_KEY_ALT), "got: {error}");
        assert!(error.contains(MANIFEST_CACHE_BYTES_KEY), "got: {error}");
    }
}
