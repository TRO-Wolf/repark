//! Engine-side Iceberg catalog handles and per-catalog staged-CTAS location policy.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use datafusion::catalog::SchemaProvider;
use iceberg::Catalog;
use repark_iceberg::catalog::{CatalogCaches, IcebergCacheSettings};

use crate::config_file::maintenance::MaintenancePolicy;
use crate::config_file::sources::SourceSpec;

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
    table_props: HashMap<String, String>,
    warehouse_layout_root: Option<PathBuf>,
}

const TABLE_DEFAULT_PREFIX: &str = "table-default.";

const TABLE_OVERRIDE_PREFIX: &str = "table-override.";

fn merge_table_creation_properties(
    side: &HashMap<String, String>,
    user: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = HashMap::with_capacity(user.len() + side.len());
    for (key, value) in side {
        if let Some(stripped) = key.strip_prefix(TABLE_DEFAULT_PREFIX) {
            merged.insert(stripped.to_string(), value.clone());
        }
    }
    merged.extend(user.iter().map(|(key, value)| (key.clone(), value.clone())));
    for (key, value) in side {
        if let Some(stripped) = key.strip_prefix(TABLE_OVERRIDE_PREFIX) {
            merged.insert(stripped.to_string(), value.clone());
        }
    }
    merged
}

type ViewWrappedSchemas = HashMap<(String, String), Arc<dyn SchemaProvider>>;

/// Iceberg catalog handles keyed by DataFusion catalog name, each tagged with a location policy.
#[derive(Clone)]
pub struct CatalogRegistry {
    entries: HashMap<String, CatalogEntry>,
    database_sources: HashMap<String, Arc<SourceSpec>>,
    /// Read-only (postgres) catalog names for P11 DML routing.
    read_only_catalogs: std::collections::HashSet<String>,
    /// Local filesystem warehouse roots for SEC-02 grandfather (memory / `LocalFs` catalogs).
    local_warehouse_roots: Vec<String>,
    iceberg_caches: Arc<CatalogCaches>,
    maintenance_policy: Option<(String, Option<MaintenancePolicy>)>,
    session_defaults: Arc<RwLock<(String, String)>>,
    view_wrapped_schemas: Arc<std::sync::Mutex<ViewWrappedSchemas>>,
    view_expansion_depth: Arc<AtomicUsize>,
}

impl Default for CatalogRegistry {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            database_sources: HashMap::new(),
            read_only_catalogs: std::collections::HashSet::new(),
            local_warehouse_roots: Vec::new(),
            iceberg_caches: Arc::new(CatalogCaches::new(IcebergCacheSettings::default())),
            maintenance_policy: None,
            session_defaults: Arc::new(RwLock::new((
                "spark_catalog".to_string(),
                "default".to_string(),
            ))),
            view_wrapped_schemas: Arc::new(std::sync::Mutex::new(HashMap::new())),
            view_expansion_depth: Arc::new(AtomicUsize::new(0)),
        }
    }
}

pub struct ViewExpansionGuard {
    depth: Arc<AtomicUsize>,
    level: usize,
}

impl ViewExpansionGuard {
    #[must_use]
    pub fn level(&self) -> usize {
        self.level
    }
}

impl Drop for ViewExpansionGuard {
    fn drop(&mut self) {
        self.depth.fetch_sub(1, Ordering::SeqCst);
    }
}

impl CatalogRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::with_cache_settings(IcebergCacheSettings::default())
    }

    #[must_use]
    pub fn with_cache_settings(settings: IcebergCacheSettings) -> Self {
        Self {
            iceberg_caches: Arc::new(CatalogCaches::new(settings)),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn iceberg_caches(&self) -> Arc<CatalogCaches> {
        Arc::clone(&self.iceberg_caches)
    }

    /// Register `catalog` under `name` with its location `policy` (replacing any prior entry).
    pub fn insert(&mut self, name: String, catalog: Arc<dyn Catalog>, policy: LocationPolicy) {
        self.entries.insert(
            name,
            CatalogEntry {
                catalog,
                location_policy: policy,
                table_props: HashMap::new(),
                warehouse_layout_root: None,
            },
        );
    }

    pub fn set_warehouse_layout_root(&mut self, name: &str, root: PathBuf) {
        if let Some(entry) = self.entries.get_mut(name) {
            entry.warehouse_layout_root = Some(root);
        }
    }

    #[must_use]
    pub fn warehouse_layout_root(&self, name: &str) -> Option<PathBuf> {
        self.entries
            .get(name)
            .and_then(|entry| entry.warehouse_layout_root.clone())
    }

    pub fn merge_table_props(&mut self, name: &str, props: &HashMap<String, String>) {
        let Some(entry) = self.entries.get_mut(name) else {
            return;
        };
        for (key, value) in props {
            if key.starts_with(TABLE_DEFAULT_PREFIX)
                || key.starts_with(TABLE_OVERRIDE_PREFIX)
                || key == crate::catalog_config::WAREHOUSE_PROP
            {
                entry.table_props.insert(key.clone(), value.clone());
            }
        }
    }

    #[must_use]
    pub fn table_creation_properties(
        &self,
        name: &str,
        user: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let Some(entry) = self.entries.get(name) else {
            return user.clone();
        };
        merge_table_creation_properties(&entry.table_props, user)
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

    pub fn set_maintenance_policy(
        &mut self,
        profile_name: impl Into<String>,
        policy: Option<MaintenancePolicy>,
    ) {
        self.maintenance_policy = Some((profile_name.into(), policy));
    }

    #[must_use]
    pub fn maintenance_policy(&self) -> Option<(&str, Option<&MaintenancePolicy>)> {
        self.maintenance_policy
            .as_ref()
            .map(|(name, policy)| (name.as_str(), policy.as_ref()))
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

    pub(crate) fn insert_database_source(&mut self, spec: Arc<SourceSpec>) {
        self.database_sources.insert(spec.name.clone(), spec);
    }

    #[must_use]
    pub fn is_registered(&self, name: &str) -> bool {
        self.entries.contains_key(name) || self.database_sources.contains_key(name)
    }

    #[must_use]
    pub fn catalog_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.entries.keys().cloned().collect();
        names.extend(self.database_sources.keys().cloned());
        names.extend(self.read_only_catalogs.iter().cloned());
        names.sort();
        names.dedup();
        names
    }

    pub(crate) fn database_source(&self, name: &str) -> Option<&Arc<SourceSpec>> {
        self.database_sources.get(name)
    }

    /// The [`LocationPolicy`] registered under `name`, if any.
    #[must_use]
    pub fn location_policy(&self, name: &str) -> Option<LocationPolicy> {
        self.entries
            .get(name)
            .map(|entry| entry.location_policy.clone())
    }

    #[must_use]
    pub fn current_defaults(&self) -> (String, String) {
        RwLock::read(&self.session_defaults)
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn set_defaults(&self, catalog: &str, namespace: &str) {
        *RwLock::write(&self.session_defaults).unwrap_or_else(PoisonError::into_inner) =
            (catalog.to_string(), namespace.to_string());
    }

    #[must_use]
    pub fn registered_catalog_names(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn is_view(
        &self,
        catalog: &str,
        view: &iceberg::TableIdent,
    ) -> Result<bool, datafusion::error::DataFusionError> {
        let Some(handle) = self.get(catalog) else {
            return Ok(false);
        };
        match handle.view_exists(view).await {
            Ok(exists) => Ok(exists),
            Err(error)
                if error.kind() == iceberg::ErrorKind::FeatureUnsupported
                    || error.kind() == iceberg::ErrorKind::NamespaceNotFound =>
            {
                Ok(false)
            }
            Err(error) => Err(repark_iceberg::catalog::iceberg_to_datafusion(error)),
        }
    }

    #[must_use]
    pub fn view_wrapper_for(&self, catalog: &str, schema: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.view_wrapped_schemas
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(catalog.to_string(), schema.to_string()))
            .cloned()
    }

    pub fn note_view_wrapper(&self, catalog: &str, schema: &str, wrapped: Arc<dyn SchemaProvider>) {
        self.view_wrapped_schemas
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((catalog.to_string(), schema.to_string()), wrapped);
    }

    #[must_use]
    pub fn view_expansion_guard(&self) -> ViewExpansionGuard {
        let level = self.view_expansion_depth.fetch_add(1, Ordering::SeqCst) + 1;
        ViewExpansionGuard {
            depth: Arc::clone(&self.view_expansion_depth),
            level,
        }
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

    #[test]
    fn table_creation_properties_merge_override_user_default() {
        let side = HashMap::from([
            ("table-default.k1".to_string(), "d1".to_string()),
            ("table-default.k2".to_string(), "d2".to_string()),
            ("table-override.k2".to_string(), "o2".to_string()),
            ("warehouse".to_string(), "/tmp/wh".to_string()),
        ]);
        let merged = merge_table_creation_properties(
            &side,
            &HashMap::from([("k1".to_string(), "user".to_string())]),
        );
        assert_eq!(merged.get("k1").map(String::as_str), Some("user"));
        assert_eq!(merged.get("k2").map(String::as_str), Some("o2"));
        assert!(!merged.contains_key("warehouse"));
        let registry = CatalogRegistry::new();
        let passthrough = registry.table_creation_properties(
            "missing",
            &HashMap::from([("k".to_string(), "v".to_string())]),
        );
        assert_eq!(passthrough.len(), 1);
    }
}
