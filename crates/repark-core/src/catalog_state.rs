//! Engine-side Iceberg catalog handles and per-catalog staged-CTAS location policy.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use iceberg::Catalog;

/// How a registered catalog resolves a staged-CTAS location when the target namespace has none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocationPolicy {
    /// Glue: a table location must be resolvable from the namespace `location` property.
    RequireExplicitLocation,
    /// AWS S3 Tables assigns each table location.
    ServiceManagedLocation,
    /// Local memory catalog fallback root resolved once at registration.
    TempFallbackAllowed {
        /// The root a location-less staged CTAS resolves table locations under.
        root: PathBuf,
    },
}

/// Filesystem root a memory-catalog warehouse string contributes to temp-fallback policy.
#[must_use]
pub fn memory_warehouse_fallback_root(warehouse: &str) -> PathBuf {
    let trimmed = warehouse.trim();
    if let Some(rest) = strip_ascii_prefix_ci(trimmed, "file://") {
        let path = strip_ascii_prefix_ci(rest, "localhost").unwrap_or(rest);
        return absolute_local_from_file_rest(path);
    }
    if let Some(rest) = strip_ascii_prefix_ci(trimmed, "file:") {
        return absolute_local_from_file_rest(rest);
    }
    PathBuf::from(trimmed)
}

/// Iceberg `LocalFsStorage::normalize_path` treats `file://path` and `file:/path` as `/path`.
fn absolute_local_from_file_rest(rest: &str) -> PathBuf {
    if rest.starts_with('/') {
        PathBuf::from(rest)
    } else if rest.is_empty() {
        PathBuf::from("/")
    } else {
        PathBuf::from(format!("/{rest}"))
    }
}

fn strip_ascii_prefix_ci<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    let prefix_len = prefix.len();
    if value.len() >= prefix_len
        && value.is_char_boundary(prefix_len)
        && value[..prefix_len].eq_ignore_ascii_case(prefix)
    {
        Some(&value[prefix_len..])
    } else {
        None
    }
}

/// One registered catalog: the iceberg handle plus the location policy for staged CTAS.
#[derive(Clone)]
struct CatalogEntry {
    catalog: Arc<dyn Catalog>,
    location_policy: LocationPolicy,
}

/// Iceberg catalog handles keyed by DataFusion catalog name, each tagged with a location policy.
#[derive(Clone, Default)]
pub struct CatalogRegistry {
    entries: HashMap<String, CatalogEntry>,
    /// Read-only (postgres) catalog names for P11 DML routing.
    read_only_catalogs: std::collections::HashSet<String>,
    /// Local filesystem warehouse roots for SEC-02 grandfather (memory / `LocalFs` catalogs).
    local_warehouse_roots: Vec<String>,
}

impl CatalogRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `catalog` under `name` with its location `policy` (replacing any prior entry).
    pub fn insert(&mut self, name: String, catalog: Arc<dyn Catalog>, policy: LocationPolicy) {
        self.entries.insert(
            name,
            CatalogEntry {
                catalog,
                location_policy: policy,
            },
        );
    }

    /// Record a local warehouse root for SEC-02 grandfather.
    pub fn note_local_warehouse_root(&mut self, path: impl Into<String>) {
        let path = path.into();
        if path.is_empty() {
            return;
        }
        if !self
            .local_warehouse_roots
            .iter()
            .any(|existing| existing == &path)
        {
            self.local_warehouse_roots.push(path);
        }
    }

    /// Local warehouse roots registered for SEC-02 grandfather checks.
    #[must_use]
    pub fn local_warehouse_roots(&self) -> &[String] {
        &self.local_warehouse_roots
    }

    /// Attach the set of read-only catalog names (postgres) for this execute snapshot.
    pub fn set_read_only_catalogs(&mut self, names: std::collections::HashSet<String>) {
        self.read_only_catalogs = names;
    }

    /// Whether `name` is a known read-only (postgres) catalog for P11 routing.
    #[must_use]
    pub fn is_read_only_catalog(&self, name: &str) -> bool {
        self.read_only_catalogs.contains(name)
    }

    /// The iceberg handle registered under `name`, if any.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Arc<dyn Catalog>> {
        self.entries.get(name).map(|entry| &entry.catalog)
    }

    /// The [`LocationPolicy`] registered under `name`, if any.
    #[must_use]
    pub fn location_policy(&self, name: &str) -> Option<LocationPolicy> {
        self.entries
            .get(name)
            .map(|entry| entry.location_policy.clone())
    }
}

impl std::ops::Index<&str> for CatalogRegistry {
    type Output = Arc<dyn Catalog>;

    fn index(&self, name: &str) -> &Self::Output {
        &self.entries[name].catalog
    }
}

impl<const N: usize> From<[(String, Arc<dyn Catalog>); N]> for CatalogRegistry {
    /// Test/local convenience: register in-memory catalogs tagged `TempFallbackAllowed`.
    fn from(items: [(String, Arc<dyn Catalog>); N]) -> Self {
        let mut registry = Self::new();
        for (name, catalog) in items {
            registry.insert(
                name,
                catalog,
                LocationPolicy::TempFallbackAllowed {
                    root: std::env::temp_dir(),
                },
            );
        }
        registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_warehouse_fallback_root_uses_the_bare_warehouse_path() {
        let root = memory_warehouse_fallback_root("/var/repark-wh");
        assert_eq!(root, PathBuf::from("/var/repark-wh"));
        assert_ne!(root, std::env::temp_dir());
    }

    #[test]
    fn memory_warehouse_fallback_root_strips_a_file_scheme() {
        let root = memory_warehouse_fallback_root("file:///var/repark-wh");
        assert_eq!(root, PathBuf::from("/var/repark-wh"));
    }

    #[test]
    fn memory_warehouse_fallback_root_strips_file_scheme_case_insensitively() {
        let root = memory_warehouse_fallback_root("FILE:///var/repark-wh");
        assert_eq!(root, PathBuf::from("/var/repark-wh"));
    }

    #[test]
    fn memory_warehouse_fallback_root_strips_file_localhost() {
        let root = memory_warehouse_fallback_root("file://localhost/var/repark-wh");
        assert_eq!(root, PathBuf::from("/var/repark-wh"));
    }

    #[test]
    fn memory_warehouse_fallback_root_matches_fileio_single_slash_and_hostless() {
        assert_eq!(
            memory_warehouse_fallback_root("file:/var/repark-wh"),
            PathBuf::from("/var/repark-wh")
        );
        assert_eq!(
            memory_warehouse_fallback_root("file://var/repark-wh"),
            PathBuf::from("/var/repark-wh")
        );
    }

    #[test]
    fn memory_warehouse_fallback_root_does_not_panic_on_utf8_after_file_colon() {
        let root = memory_warehouse_fallback_root("file:/ü/scratch/repark-wh");
        assert_eq!(root, PathBuf::from("/ü/scratch/repark-wh"));
    }
}
