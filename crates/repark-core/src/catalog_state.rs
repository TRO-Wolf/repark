//! The engine-side catalog registry: iceberg `Catalog` handles + per-catalog location policy.
//!
//! Hoisted MOVE-ONLY from the v1 SQL crate's root (the phase-2 statement router consumes these
//! types but the SESSION owns them — the phase cut moves the types to their owner). Bodies are
//! byte-faithful to v1 at the port-source pin except forced-edit **E-4**:
//! [`LocationPolicy::TempFallbackAllowed`] gains `{ root: PathBuf }`, resolved once at
//! `register_memory_catalog` time — removing the phase-2 CTAS-time `std::env::temp_dir()`
//! env read before the type becomes public phase-1 API. (E-4 consequences: the enum loses
//! `Copy` — `PathBuf` is not `Copy` — and [`CatalogRegistry::location_policy`] clones.)

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use iceberg::Catalog;

/// ===========================================================================================
/// How a registered catalog resolves a table location for a **staged CTAS create** whose target
/// namespace carries no `location` property.
///
/// A real warehouse (Glue / S3 Tables, or any externally-supplied catalog) must never have its
/// data silently placed under a process-temp directory — that is the audit's BUG-002 / SEC-003
/// data-mis-placement hole. Such a catalog gets [`LocationPolicy::RequireExplicitLocation`], where
/// a missing namespace location is a loud, actionable error. The temp fallback survives only for
/// [`LocationPolicy::TempFallbackAllowed`] — the in-memory / local-filesystem catalog used offline.
/// ===========================================================================================
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocationPolicy {
    /// Glue (or any externally-supplied catalog): a table location must be resolvable
    /// from the namespace `location` property; a missing location fails loud, never a temp dir.
    RequireExplicitLocation,
    /// AWS S3 Tables: the SERVICE assigns each table's storage location at create time —
    /// namespaces carry no `location` property (structurally: there is nothing to set), and the
    /// fork's `S3TablesCatalog::create_table` REJECTS a caller-supplied location outright. A
    /// staged "pick location → write → register" CTAS is impossible here; the create arm instead
    /// routes create-first (create through the catalog to obtain the service location, stream the
    /// SELECT into it, commit ONE fast-append, drop the table on abort — Spark's non-staging
    /// `StagingTableCatalog` semantics: `BasicStagedTable.abortStagedChanges` → `dropTable`).
    ServiceManagedLocation,
    /// In-memory / local-filesystem catalog: fall back to the registration-time-resolved `root`
    /// when a namespace has no `location`, so offline development and tests run without a
    /// configured warehouse. **E-4:** `root` is resolved ONCE when the catalog is registered —
    /// the CTAS consumer reads `root` and never touches the process environment at query time.
    /// **A13:** `register_memory_catalog` sets `root` to the supplied warehouse, so two sessions
    /// with different warehouses no longer share `<temp>/repark_ctas/<catalog>/<ns>/<table>`.
    /// [`CatalogRegistry::from`] (test helper, no warehouse argument) still uses
    /// `std::env::temp_dir()`.
    TempFallbackAllowed {
        /// The root a location-less staged CTAS resolves table locations under.
        root: PathBuf,
    },
}

/// Filesystem root a memory-catalog warehouse string contributes to
/// [`LocationPolicy::TempFallbackAllowed`].
///
/// Bare absolute paths pass through. A `file://` URI drops the scheme (case-insensitive) and an
/// optional `localhost` host so the fallback composes as a filesystem path — the same local-dev
/// form `memory_catalog` already accepts, matching the Spark local-DDL `file://` rules.
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

/// Iceberg `LocalFsStorage::normalize_path` treats `file://path` and `file:/path` as absolute
/// `/path`. The refuse fence must use the same alias set or CALL `location` fail-opens.
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

/// One registered catalog: the iceberg handle plus the [`LocationPolicy`] that governs staged-CTAS
/// location resolution for it.
#[derive(Clone)]
struct CatalogEntry {
    catalog: Arc<dyn Catalog>,
    location_policy: LocationPolicy,
}

/// ===========================================================================================
/// iceberg `Catalog` handles keyed by their registered DataFusion catalog name, each tagged with
/// its [`LocationPolicy`]. The write path needs the iceberg-side handle (to create tables / run
/// transactions) alongside the registered provider; the policy is consulted only by the staged-
/// CTAS location resolution when a create hits a location-less namespace.
/// Local warehouse roots (memory catalog paths) grandfather SEC-02 local DDL under those trees.
/// ===========================================================================================
#[derive(Clone, Default)]
pub struct CatalogRegistry {
    entries: HashMap<String, CatalogEntry>,
    /// Read-only (postgres) catalog names for P11 DML routing. Stored on the registry so
    /// lookups stay correct across `.await` (no thread-local / worker-hop races).
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

    /// Record a local warehouse root for SEC-02 grandfather (`COPY TO` / `CREATE EXTERNAL` under
    /// this tree stay allowed when `repark.sql.allowLocalFilesystemDDL` is false).
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

    /// The [`LocationPolicy`] registered under `name`, if any. (E-4: clones — the policy now
    /// carries a `PathBuf` and is no longer `Copy`.)
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
    /// Test/local convenience: register in-memory catalogs, each tagged
    /// [`LocationPolicy::TempFallbackAllowed`]. Production registration threads the real policy via
    /// [`CatalogRegistry::insert`] (Glue gets [`LocationPolicy::RequireExplicitLocation`],
    /// S3 Tables gets [`LocationPolicy::ServiceManagedLocation`]). This helper has no warehouse
    /// argument, so `root` is `std::env::temp_dir()` — the construction-time analogue of the
    /// pre-A13 `register_memory_catalog` path. Product registration uses the warehouse.
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
