//! The `ReparkSession` equivalent.
//!
//! Constructs the DataFusion [`SessionContext`] (memory pool, batch size, partitions, the write
//! knobs as `ConfigExtension`s), runs the [`SessionExtension`] hooks at v1's inline registration
//! positions, holds the iceberg `Catalog` handles
//! ([`CatalogRegistry`]), and exposes the near-drop-in PySpark entrypoints: `sql`,
//! `register_iceberg_catalog` (+ the `register_memory_catalog` convenience), `create_namespace`,
//! `create_or_replace_temp_view` (batches) / `create_or_replace_temp_view_from` (a plan),
//! `drop_temp_view`, `table_exists`, `read_parquet`, `read_csv`, `read_json`.
//! All execution routes through the [`ExecutionBackend`] seam, so a future distributed coordinator
//! can slot in without reworking the write path (distribution is deferred — see
//! [`backend`](crate::ExecutionBackend)).
//!
//! `sql` routes through the session-default [`SqlDialect`] (phase-cut inversion, design §3 —
//! plain DataFusion in phase 1; the Spark door's statement router returns as a phase-2 dialect
//! impl on the same seam). Catalogs configure
//! two ways: directly (`register_iceberg_catalog` with a `repark_iceberg::catalog` builder —
//! memory, Glue, S3 Tables) or through Spark-style `spark.sql.catalog.<name>.*` config on the
//! builder ([`crate::parse_catalog_specs`] parses;
//! [`ReparkSession::register_configured_catalogs`] registers).
//! `read_parquet` routes `s3://`/`s3a://` paths through the [`crate::object_store_s3`]
//! registration, against the FINALIZE-resolved AWS SDK config (E-2).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use arrow::array::RecordBatch;
use aws_config::{BehaviorVersion, SdkConfig};
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionConfig, SessionContext};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_common::{Error, Result};

use crate::backend::{ExecutionBackend, SingleNodeBackend};
use crate::catalog_config::{self, CatalogKind, CatalogSpec};
use crate::catalog_state::{CatalogRegistry, LocationPolicy};
use crate::dialect::{DataFusionDialect, EngineContext, SqlDialect};
use crate::extension::{NoopSessionExtension, SessionExtension};
use crate::time_travel::{self, TimeTravelSpec};
// v1's two test-only re-exports, re-homed with the test module (they rode the v1 crate root,
// which the module split made this file's parent — `use super::*;` in `session/tests.rs`
// resolves through here).
#[cfg(test)]
pub(crate) use crate::error_map::{EngineErrorKind, classify_datafusion_error};
#[cfg(test)]
pub(crate) use crate::idents::reject_path_escape_segment;
use crate::{
    csv_read_options_from_map, csv_utf8_schema_from_path, engine_err, iceberg_err,
    json_read_options_from_map, object_store_s3, parse_table_identifier_segments,
    resolve_s3_region_override,
};

/// ===========================================================================================
/// Iceberg reader time-travel options (Spark `snapshot-id` / `as-of-timestamp` / `branch` / `tag`).
///
/// At most one of the four may be set; mutual exclusion fails loud naming both keys.
/// ===========================================================================================
#[derive(Debug, Clone, Default)]
pub struct TimeTravelOpts {
    /// Spark `snapshot-id` — pin to a concrete snapshot.
    pub snapshot_id: Option<i64>,
    /// Spark `as-of-timestamp` — epoch **milliseconds**.
    pub as_of_timestamp_ms: Option<i64>,
    /// Spark `branch` — pin to a branch ref.
    pub branch: Option<String>,
    /// Spark `tag` — pin to a tag ref.
    pub tag: Option<String>,
}

impl TimeTravelOpts {
    /// ===========================================================================================
    /// Convert to a [`TimeTravelSpec`], or `None` when no pin is set.
    /// ===========================================================================================
    ///
    /// # Errors
    /// Two or more pins set → [`Error::Analysis`] naming both option keys.
    pub fn into_spec(self) -> Result<Option<TimeTravelSpec>> {
        let mut set: Vec<(&str, TimeTravelSpec)> = Vec::new();
        if let Some(snapshot_id) = self.snapshot_id {
            set.push(("snapshot-id", TimeTravelSpec::SnapshotId(snapshot_id)));
        }
        if let Some(ms) = self.as_of_timestamp_ms {
            set.push(("as-of-timestamp", TimeTravelSpec::TimestampMs(ms)));
        }
        // Trim branch/tag (SQL VERSION AS OF already trims via parse_version_value). Whitespace
        // padding would otherwise fail as unknown ref `" b "` while the bare name exists —
        // octo C5-Q-001.
        if let Some(branch) = self.branch {
            let trimmed = branch.trim();
            if trimmed.is_empty() {
                return Err(Error::Analysis(
                    "Iceberg reader option branch requires a non-empty branch name".to_string(),
                ));
            }
            set.push(("branch", TimeTravelSpec::VersionRef(trimmed.to_string())));
        }
        if let Some(tag) = self.tag {
            let trimmed = tag.trim();
            if trimmed.is_empty() {
                return Err(Error::Analysis(
                    "Iceberg reader option tag requires a non-empty tag name".to_string(),
                ));
            }
            set.push(("tag", TimeTravelSpec::VersionRef(trimmed.to_string())));
        }
        match set.len() {
            0 => Ok(None),
            1 => Ok(Some(set.remove(0).1)),
            _ => {
                let names: Vec<&str> = set.iter().map(|(name, _)| *name).collect();
                Err(Error::Analysis(format!(
                    "Iceberg time-travel reader options are mutually exclusive; got {}",
                    names.join(" and ")
                )))
            }
        }
    }
}

const BYTES_PER_GB: usize = 1024 * 1024 * 1024;

/// The explicit AWS-use opt-in conf (E-2). A session with no AWS-backed catalog spec and no S3
/// region conf can still signal AWS use — e.g. plain `s3://` bronze reads on an otherwise-local
/// session — with `.config("repark.aws.enable", "true")`; `register_configured_catalogs()` then
/// resolves the AWS SDK chain once at finalize. Without any signal, finalize never touches the
/// chain (no IMDS probe for offline sessions).
pub const AWS_ENABLE_CONFIG_KEY: &str = "repark.aws.enable";

/// Smallest non-zero `memory_limit_bytes` accepted by [`ReparkSessionBuilder::build`].
/// Explicit `0` still opts out (unbounded). Pathological 1-byte budgets thrash the spill pool
/// without a clean config error (audit SAF-007 / 2026-07-25).
const MIN_MEMORY_LIMIT_BYTES: usize = 1024 * 1024;

/// Default `FairSpillPool` budget when the builder leaves memory unset (C1-Q-002 / C1-L-005).
///
/// Unbounded DataFusion defaults OOM on large sorts/aggregations/MERGE; 8 GiB is a conservative
/// single-node ceiling that still spills rather than hard-failing. Override with
/// [`ReparkSessionBuilder::memory_limit_gb`] / [`ReparkSessionBuilder::memory_limit_bytes`];
/// pass `memory_limit_bytes(0)` to opt out and keep DataFusion's unbounded pool.
const DEFAULT_MEMORY_LIMIT_BYTES: usize = 8 * BYTES_PER_GB;

/// ===========================================================================================
/// Builder for a [`ReparkSession`] — the PySpark `SparkSession.builder` analogue.
///
/// Every knob is optional. When `memory_limit_*` is **unset**, [`build`](Self::build) installs an
/// 8 GiB [`FairSpillPool`] that bounds **spillable operators only** (sort / hash-aggregate / join
/// reservations that ask the pool — C1-Q-002). Expression evaluation allocates Arrow buffers
/// outside the pool, so RSS can still exceed the budget (or the process can abort on allocation
/// failure) for large `array_repeat` / `repeat` / `sequence` / `collect_list` results. Plan-time
/// cardinality ceilings (`repark.sql.maxArrayElements`, default `10_000_000`) convert
/// planner-visible expansion bombs into catchable analysis errors; they do **not** make the pool
/// bound expression allocs. Re-size the **same** live pool via conf
/// `datafusion.runtime.memory_limit` (or set [`.memory_limit_gb(n)`](Self::memory_limit_gb) /
/// [`.memory_limit_bytes(n)`](Self::memory_limit_bytes) at build). `n = 0` opts out (unbounded
/// pool). Other unset knobs use DataFusion's defaults.
/// ===========================================================================================
#[derive(Clone, Default)]
pub struct ReparkSessionBuilder {
    memory_limit_bytes: Option<usize>,
    batch_size: Option<usize>,
    target_partitions: Option<usize>,
    /// The session-default [`SqlDialect`] (phase-cut seam slot). `None` → [`DataFusionDialect`].
    sql_dialect: Option<Arc<dyn SqlDialect>>,
    /// The build-time [`SessionExtension`] (phase-cut seam slot). `None` → no-op hooks (the
    /// pure-DataFusion baseline).
    extension: Option<Arc<dyn SessionExtension>>,
    /// The full Spark-style `.config(key, value)` map. Engine knobs above are set through the typed
    /// setters; this map additionally drives `spark.sql.catalog.<name>.*` catalog registration at
    /// session construction (see [`ReparkSession::register_configured_catalogs`]). Non-catalog keys
    /// are ignored here, matching PySpark's tolerance of unknown `.config` keys.
    config: HashMap<String, String>,
}

impl std::fmt::Debug for ReparkSessionBuilder {
    /// Manual, non-leaking `Debug` (the seam slots are `dyn` trait objects): knob fields only.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReparkSessionBuilder")
            .field("memory_limit_bytes", &self.memory_limit_bytes)
            .field("batch_size", &self.batch_size)
            .field("target_partitions", &self.target_partitions)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ReparkSessionBuilder {
    /// Install the session-default [`SqlDialect`] (phase-cut seam, design §3): every
    /// [`ReparkSession::sql`] call routes through it. Unset → [`DataFusionDialect`] (plain
    /// `SessionContext::sql`). A door with its own statement router installs its dialect here;
    /// [`ReparkSession::sql_with`] runs a one-off dialect without changing the session default.
    #[must_use]
    pub fn with_sql_dialect(mut self, dialect: Arc<dyn SqlDialect>) -> Self {
        self.sql_dialect = Some(dialect);
        self
    }

    /// Install the build-time [`SessionExtension`] (phase-cut seam, design §3): `build()` runs
    /// its two hooks at v1's inline registration positions — `configure` on the `SessionConfig`
    /// before the runtime/context are assembled, `register` on the freshly built
    /// `SessionContext`. Unset → the defaulted no-op hooks. Phase-2 repark-spark ships one
    /// extension holding exactly what v1 inlined (function registry + analyzer rules + TA UDFs
    /// + cardinality config).
    #[must_use]
    pub fn with_extension(mut self, extension: Arc<dyn SessionExtension>) -> Self {
        self.extension = Some(extension);
        self
    }

    /// Record a single Spark-style `.config(key, value)` pair (PySpark `.config`). Catalog keys
    /// (`spark.sql.catalog.<name>.*`) are consumed at [`build`](Self::build); other keys are kept
    /// for potential future use and otherwise ignored.
    #[must_use]
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }

    /// Record a whole config map at once (the facade collects `.config(...)` calls into one dict).
    #[must_use]
    pub fn configs(mut self, config: HashMap<String, String>) -> Self {
        self.config.extend(config);
        self
    }

    /// Cap engine memory at `gb` gigabytes via a `FairSpillPool` (spills instead of running out).
    ///
    /// Overrides the default 8 GiB budget applied when memory is left unset. Pass `0` (via
    /// [`Self::memory_limit_bytes`]) to opt out of a bounded pool entirely.
    ///
    /// Whole-GB budgets can never trip the [`MIN_MEMORY_LIMIT_BYTES`] floor: the smallest non-zero
    /// `gb` is `1` → 1 GiB, and the conversion `saturating_mul`s (it cannot wrap a huge `gb` back
    /// down into the refused `(0, 1 MiB)` gap). Pinned by
    /// `memory_limit_gb_never_lands_below_the_floor`; the gap is reachable only through
    /// [`Self::memory_limit_bytes`] (audit SAF-007).
    #[must_use]
    pub fn memory_limit_gb(mut self, gb: usize) -> Self {
        self.memory_limit_bytes = Some(gb.saturating_mul(BYTES_PER_GB));
        self
    }

    /// Cap engine memory at an explicit byte budget.
    ///
    /// Overrides the default 8 GiB budget. Pass `0` to opt out and keep DataFusion's unbounded
    /// memory pool (no `FairSpillPool`).
    #[must_use]
    pub fn memory_limit_bytes(mut self, bytes: usize) -> Self {
        self.memory_limit_bytes = Some(bytes);
        self
    }

    /// Rows per Arrow batch (DataFusion `batch_size`). Larger = fewer, bigger batches.
    ///
    /// Must be `>= 1` at [`Self::build`] — `0` is a config error (audit SAF-006). This is the
    /// ENGINE knob, not the Spark key: Spark documents
    /// `spark.sql.execution.arrow.maxRecordsPerBatch <= 0` as "no limit", and the Python facade
    /// translates that sentinel to "unset" (with a disclosure warning) before it reaches here —
    /// DataFusion has no unbounded-batch mode, so `0` stays a config error at this layer.
    #[must_use]
    pub fn batch_size(mut self, rows: usize) -> Self {
        self.batch_size = Some(rows);
        self
    }

    /// Degree of intra-query parallelism (DataFusion `target_partitions`).
    ///
    /// Must be `>= 1` at [`Self::build`] — `0` is a config error (audit SAF-006).
    #[must_use]
    pub fn target_partitions(mut self, partitions: usize) -> Self {
        self.target_partitions = Some(partitions);
        self
    }

    /// Build the session: configure the runtime + memory pool and register the Spark functions.
    ///
    /// The `spark.sql.catalog.<name>.*` config is parsed into [`CatalogSpec`]s here (fail-loud on a
    /// misconfiguration — synchronously, so a bad catalog block surfaces at build time) and stored
    /// on the session; the catalogs themselves are registered by the async
    /// [`ReparkSession::register_configured_catalogs`] (catalog registration is async — the PyO3
    /// constructor `block_on`s it). Existing sync callers that pass no catalog config get an empty
    /// spec list and never need the async step, so the sync `build()` contract is preserved.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if the DataFusion runtime fails to build, or [`Error::Config`]
    /// if a `spark.sql.catalog.*` block is malformed, `batch_size`/`target_partitions` is `0`, or a
    /// non-zero `memory_limit_bytes` is below 1 MiB.
    pub fn build(self) -> Result<ReparkSession> {
        if let Some(0) = self.batch_size {
            return Err(Error::Config("batch_size must be >= 1 (got 0)".to_string()));
        }
        if let Some(0) = self.target_partitions {
            return Err(Error::Config(
                "target_partitions must be >= 1 (got 0)".to_string(),
            ));
        }
        if let Some(bytes) = self.memory_limit_bytes
            && bytes > 0
            && bytes < MIN_MEMORY_LIMIT_BYTES
        {
            return Err(Error::Config(format!(
                "memory_limit_bytes must be 0 (unbounded) or >= {MIN_MEMORY_LIMIT_BYTES} \
                 (1 MiB); got {bytes}"
            )));
        }

        let catalog_specs = catalog_config::parse_catalog_specs(&self.config)?;
        // The optional `s3://`/`s3a://` read region override (else the aws-config chain resolves
        // it). Both spellings are accepted; identical values collapse, different values fail loud.
        let s3_region_override = resolve_s3_region_override(&self.config)?;
        // E-2: does this session SIGNAL AWS use? An AWS-backed catalog spec, the S3-region conf
        // (either spelling), or the explicit opt-in. Recorded at build; consumed by the finalize
        // (`register_configured_catalogs`), which resolves the AWS SDK chain once IF signaled —
        // offline sessions never pay the chain resolution / IMDS probe.
        let aws_signaled = catalog_specs
            .iter()
            .any(|spec| matches!(spec.kind, CatalogKind::Glue | CatalogKind::S3Tables))
            || s3_region_override.is_some()
            || self
                .config
                .get(AWS_ENABLE_CONFIG_KEY)
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"));
        // Write-path concurrency (session conf only — never a table property). Fail loud on a
        // non-integer or `< 1` value so a typo cannot silently fall back to serial or unbounded.
        let write_concurrency = repark_iceberg::write::concurrency_from_config_map(&self.config)
            .map_err(|error| Error::Config(error.to_string()))?;
        let scan_pruning =
            repark_iceberg::write::scan_prune::scan_pruning_from_config_map(&self.config)
                .map_err(|error| Error::Config(error.to_string()))?;
        let file_scoped_rewrite =
            repark_iceberg::write::file_scoped_rewrite_from_config_map(&self.config)
                .map_err(|error| Error::Config(error.to_string()))?;
        // MERGE target-scan file concurrency (session conf only). Unset = fork num_cpus default.
        let scan_concurrency =
            repark_iceberg::write::scan_concurrency_from_config_map(&self.config)
                .map_err(|error| Error::Config(error.to_string()))?;
        // The build-time extension (phase-cut inversion, design §3): its two hooks replace v1's
        // inline phase-2 registrations at the SAME positions in this construction order.
        let ext: Arc<dyn SessionExtension> = self
            .extension
            .clone()
            .unwrap_or_else(|| Arc::new(NoopSessionExtension));
        let mut config = SessionConfig::new();
        // DF 54.1 REGRESSION GUARD: the new default-on physical uncorrelated-scalar-subquery
        // path (`ScalarSubqueryExec` wrapping) drops the query's top-level Sort — `SELECT …
        // WHERE x < (SELECT …) ORDER BY …` returns unsorted rows (fuzzer repros fuzz-42-1/2,
        // 2026-08-01; minimal: no `SortExec` in the physical plan). Force the pre-54 rewrite
        // (`ScalarSubqueryToJoin`) until upstream fixes; re-enable is gated on the banked
        // repros passing WITH the flag on.
        config
            .options_mut()
            .optimizer
            .enable_physical_uncorrelated_scalar_subquery = false;
        config = repark_iceberg::write::with_merge_session_knobs(
            config,
            scan_pruning,
            file_scoped_rewrite,
        );
        config = repark_iceberg::write::with_scan_concurrency(config, scan_concurrency);
        if let Some(rows) = self.batch_size {
            config = config.with_batch_size(rows);
        }
        if let Some(partitions) = self.target_partitions {
            config = config.with_target_partitions(partitions);
        }
        config = repark_iceberg::write::with_write_concurrency(config, write_concurrency);
        // Extension hook 1 of 2 — CONFIGURE, at v1's inline position (after the engine knobs are
        // installed as ConfigExtensions, before the RuntimeEnv is assembled). v1 inlined the
        // cardinality/`repark.sql.*` `ConfigExtension` here (r24 SB1); the phase-2 Spark
        // extension re-homes it onto this hook, parsing the same builder config map.
        config = ext.configure(&self.config, config).map_err(engine_err)?;

        let mut runtime = RuntimeEnvBuilder::new();
        // Default 8 GiB FairSpillPool when unset (C1-Q-002). Explicit Some(0) opts out (unbounded).
        let pool_bytes = match self.memory_limit_bytes {
            None => Some(DEFAULT_MEMORY_LIMIT_BYTES),
            Some(0) => None,
            Some(bytes) => Some(bytes),
        };
        if let Some(bytes) = pool_bytes {
            runtime = runtime.with_memory_pool(Arc::new(FairSpillPool::new(bytes)));
        }
        // DataFusion caches directory listings by path on the RuntimeEnv object-list cache.
        // Path parquet overwrite stage-swaps into the *same* destination path; a warm listing
        // then makes same-session `read_parquet` return pre-overwrite rows while on-disk data
        // is already new (octo r4 Group I C1-Q-002). Limit 0 disables the cache so listings
        // always refresh after stage-swap / rmtree+rename.
        runtime = runtime.with_object_list_cache_limit(0);
        let runtime = runtime.build_arc().map_err(engine_err)?;

        let context = SessionContext::new_with_config_rt(config, runtime);
        // Extension hook 2 of 2 — REGISTER, at v1's inline position (immediately after context
        // creation). v1 inlined the Spark function registry, the expression-semantics analyzer
        // rules (appended after DataFusion's built-ins so they see type-coerced plans), and the
        // TA window UDFs here; the phase-2 Spark extension re-homes all three onto this hook.
        ext.register(&context).map_err(engine_err)?;

        Ok(ReparkSession {
            backend: Arc::new(SingleNodeBackend::new(context)),
            dialect: self
                .sql_dialect
                .unwrap_or_else(|| Arc::new(DataFusionDialect)),
            catalogs: Arc::new(RwLock::new(CatalogRegistry::new())),
            catalog_specs: Arc::new(catalog_specs),
            registered_s3_buckets: Arc::new(Mutex::new(HashSet::new())),
            s3_region_override: Arc::new(s3_region_override),
            postgres_catalog_names: Arc::new(RwLock::new(HashSet::new())),
            aws_signaled,
            aws_sdk_config: Arc::new(OnceLock::new()),
        })
    }
}

/// ===========================================================================================
/// `ReparkSession` — the near-drop-in entrypoint (`from repark import ReparkSession`).
///
/// Cheap to [`Clone`] (an `Arc` over the backend). The compute lives in DataFusion behind the
/// [`ExecutionBackend`] seam; this type is the thin Spark-shaped facade over it.
/// ===========================================================================================
#[derive(Clone)]
pub struct ReparkSession {
    backend: Arc<dyn ExecutionBackend>,
    /// The session-default [`SqlDialect`] every [`sql`](Self::sql) call routes through
    /// (phase-cut inversion, design §3). [`DataFusionDialect`] unless the builder installed one.
    dialect: Arc<dyn SqlDialect>,
    /// iceberg `Catalog` handles by registered name. Shared (the session is cheaply cloned) and
    /// interior-mutable so catalogs can be registered after construction. Read as a cheap clone per
    /// `sql` call so no lock is held across an `await`.
    catalogs: Arc<RwLock<CatalogRegistry>>,
    /// Names of registered postgres read catalogs (`SessionContext` only — not in `CatalogRegistry`).
    /// Threaded into the [`SqlDialect`] seam's `EngineContext::read_only` for P11 DML
    /// direction-notes (v1: the positional `execute_with_read_only` argument).
    postgres_catalog_names: Arc<RwLock<HashSet<String>>>,
    /// The catalogs configured via `spark.sql.catalog.<name>.*`, parsed at build time and registered
    /// (async) by [`register_configured_catalogs`](Self::register_configured_catalogs). Shared
    /// (`Arc`) so the session stays cheap to clone; empty when no catalog config was supplied.
    catalog_specs: Arc<Vec<CatalogSpec>>,
    /// The set of S3 buckets whose object store has already been registered on the `RuntimeEnv`, so
    /// `read_parquet` builds + registers each bucket's store at most once. Shared across session
    /// clones (they share one `RuntimeEnv`) and guarded by a brief `std::sync::Mutex` never held
    /// across an `.await` (the AWS store build happens outside the lock).
    registered_s3_buckets: Arc<Mutex<HashSet<String>>>,
    /// Optional explicit region for `s3`/`s3a` reads (the `spark.hadoop.fs.s3a.endpoint.region`
    /// config); `None` means the aws-config chain resolves the region. Shared (`Arc`) so the session
    /// stays cheap to clone.
    s3_region_override: Arc<Option<String>>,
    /// E-2: whether this session signaled AWS use at build time (an AWS-backed catalog spec, an
    /// S3-region conf, or the [`AWS_ENABLE_CONFIG_KEY`] opt-in). Consumed by the finalize pair,
    /// which resolves the AWS SDK chain only when this is set.
    aws_signaled: bool,
    /// E-2: the AWS SDK config resolved ONCE at finalize (`register_configured_catalogs` /
    /// `register_late_configured_catalogs`) — never lazily at S3 path-read time (v1's query-time
    /// env read, removed). Shared across session clones; an S3-path read on a session that never
    /// resolved fails loud naming the missing step.
    aws_sdk_config: Arc<OnceLock<SdkConfig>>,
}

impl ReparkSession {
    /// Start configuring a session (PySpark `SparkSession.builder`).
    #[must_use]
    pub fn builder() -> ReparkSessionBuilder {
        ReparkSessionBuilder::default()
    }

    /// Build a session with all defaults.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if the DataFusion runtime fails to build.
    pub fn new() -> Result<Self> {
        Self::builder().build()
    }

    /// The DataFusion context this session executes against (escape hatch for advanced wiring).
    #[must_use]
    pub fn context(&self) -> &SessionContext {
        self.backend.session_context()
    }

    /// Run a SQL string and return the resulting [`DataFrame`] (PySpark `spark.sql`).
    ///
    /// Routes through the session-default [`SqlDialect`] (the phase-cut inversion, design §3):
    /// [`DataFusionDialect`] unless the builder installed one. The phase-2 Spark door's dialect
    /// restores v1's statement interception (CTAS, MERGE INTO, ALTER, …) on this same seam.
    ///
    /// # Errors
    /// Returns the classified [`Error`]: [`Error::Parse`] on a syntax error, [`Error::Analysis`]
    /// on a planning failure (including iceberg not-found / already-exists),
    /// [`Error::NotImplemented`] on a deterministic scope gate, [`Error::Iceberg`] on another
    /// iceberg-origin failure (kind-first message), [`Error::DataFusion`] on execution failure.
    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        let dialect = Arc::clone(&self.dialect);
        self.sql_with(&dialect, query).await
    }

    /// Run a SQL string under an EXPLICIT dialect, leaving the session default untouched — two
    /// doors sharing one session (ADR-0002 "one test row per door"). The dialect receives the
    /// same per-call [`EngineContext`] snapshot `sql` builds.
    ///
    /// # Errors
    /// Identical classification to [`Self::sql`] — the [`engine_err`] fold is session-side, so
    /// every dialect gets the same error taxonomy.
    pub async fn sql_with(&self, dialect: &Arc<dyn SqlDialect>, query: &str) -> Result<DataFrame> {
        // Clone the registry (cheap — keys + `Arc`s) so no lock is held across the `await`.
        let catalogs = self.catalogs_snapshot();
        let read_only = self.postgres_catalog_names_snapshot();
        dialect
            .execute(
                EngineContext {
                    ctx: self.context(),
                    catalogs: &catalogs,
                    read_only: &read_only,
                },
                query,
            )
            .await
            .map_err(engine_err)
    }

    /// Register an iceberg [`Catalog`] under `name`: both as a DataFusion `CatalogProvider` (so
    /// `name.namespace.table` resolves in queries) and in the session's registry (so the write path
    /// can reach the iceberg handle). Create the catalog's namespaces before querying them.
    ///
    /// Re-registering the same `name` is rejected (same rule as [`Self::register_memory_catalog`])
    /// so an earlier handle is never silently orphaned.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if the name is already registered or the catalog's
    /// namespaces/schemas cannot be loaded.
    pub async fn register_iceberg_catalog(
        &self,
        name: &str,
        catalog: Arc<dyn Catalog>,
    ) -> Result<()> {
        // An externally-supplied catalog is treated as a real warehouse (Glue / S3 Tables register
        // through here too): a location-less namespace must fail loud at CTAS rather than fall back
        // to a temporary directory. The memory convenience below opts into the temp fallback.
        self.register_iceberg_catalog_with_policy(
            name,
            catalog,
            LocationPolicy::RequireExplicitLocation,
        )
        .await
    }

    /// Register an iceberg [`Catalog`] under `name` with an explicit [`LocationPolicy`] — the shared
    /// seam behind [`Self::register_iceberg_catalog`] and [`Self::register_memory_catalog`]. The
    /// policy is stored with the handle and governs how a staged CTAS resolves a namespace that has
    /// no `location` property (fail loud for real warehouses, temp fallback for the memory catalog).
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if the name is already registered or the catalog's
    /// namespaces/schemas cannot be loaded.
    async fn register_iceberg_catalog_with_policy(
        &self,
        name: &str,
        catalog: Arc<dyn Catalog>,
        location_policy: LocationPolicy,
    ) -> Result<()> {
        if self.catalog_handle(name).is_ok() {
            return Err(Error::DataFusion(format!(
                "catalog '{name}' is already registered — pick a different name or reuse the \
                 existing registration"
            )));
        }
        repark_iceberg::catalog::register_iceberg_catalog(self.context(), name, catalog.clone())
            .await
            .map_err(engine_err)?;
        self.catalogs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.to_string(), catalog, location_policy);
        Ok(())
    }

    /// Register every catalog configured through `spark.sql.catalog.<name>.*` (parsed at build
    /// time). This is the async completion of `build()` — the PyO3 constructor `block_on`s it, and
    /// Rust callers that supply catalog config call it once after `build()`. Each spec is dispatched
    /// to the matching `repark-catalog` builder: `memory` → the AWS-free local catalog, `glue` /
    /// `s3tables` → the AWS product surfaces. It is a no-op when no catalog config was supplied.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if a configured catalog cannot be built or registered (e.g. a
    /// required builder property is missing, or the catalog's namespaces cannot be loaded).
    pub async fn register_configured_catalogs(&self) -> Result<()> {
        // E-2: conditional finalize-time AWS resolution. Resolve the AWS SDK chain ONCE, here —
        // never lazily at S3 path-read time — and only when the session signaled AWS use at
        // build. Offline sessions skip this entirely (no IMDS probe).
        self.resolve_aws_sdk_config_if(self.aws_signaled).await;
        for spec in self.catalog_specs.iter() {
            self.register_catalog_spec(spec).await?;
        }
        Ok(())
    }

    /// E-2: resolve + store the session-held AWS SDK config when `signaled` (idempotent — the
    /// `OnceLock` keeps the first resolution). Catalog credentials are NOT this config: the
    /// fork's Glue / S3 Tables builders resolve their own chain at registration, per-session
    /// (v1 behavior, unchanged). This config serves the `s3://`/`s3a://` PATH-read stores.
    async fn resolve_aws_sdk_config_if(&self, signaled: bool) {
        if signaled && self.aws_sdk_config.get().is_none() {
            let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
            // A racing clone may have set it first; the first resolution wins (identical chain).
            let _ = self.aws_sdk_config.set(sdk_config);
        }
    }

    /// Test-only observability for the E-2 gate: whether the finalize step resolved the
    /// session-held AWS SDK config.
    #[cfg(test)]
    pub(crate) fn testing_aws_sdk_config_resolved(&self) -> bool {
        self.aws_sdk_config.get().is_some()
    }

    /// ===========================================================================================
    /// Register catalogs from a LATE `spark.sql.catalog.*` config map onto the LIVE session —
    /// the facade's `getOrCreate` reuse path (dogfood finding R-GETORCREATE). PySpark parity:
    /// Spark instantiates catalogs lazily per name, so a catalog configured by a LATER builder
    /// works against the already-active session; an already-registered name keeps its existing
    /// registration (reported to the caller in `skipped`, never silently re-registered — the
    /// live-Spark analogue is an already-instantiated catalog ignoring changed conf).
    /// Returns `(added, skipped_existing)` catalog-name lists, each sorted for determinism.
    /// ===========================================================================================
    ///
    /// # Errors
    /// Returns [`Error::Config`] if the `spark.sql.catalog.*` block is malformed, or
    /// [`Error::DataFusion`] if a NEW catalog fails to build/register (a failure mid-list leaves
    /// earlier additions registered — same additive semantics as the build-time pass).
    pub async fn register_late_configured_catalogs(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let specs = catalog_config::parse_catalog_specs(config)?;
        // E-2: the late config map can introduce the session's FIRST AWS signal (a late Glue /
        // S3 Tables catalog on a previously offline session) — same conditional resolution as
        // the build-time finalize, still never at path-read time.
        let late_aws_signaled = specs
            .iter()
            .any(|spec| matches!(spec.kind, CatalogKind::Glue | CatalogKind::S3Tables))
            || config
                .get(AWS_ENABLE_CONFIG_KEY)
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"));
        self.resolve_aws_sdk_config_if(self.aws_signaled || late_aws_signaled)
            .await;
        let mut added = Vec::new();
        let mut skipped = Vec::new();
        for spec in &specs {
            let already_iceberg = self.catalog_handle(&spec.name).is_ok();
            let already_postgres = self
                .postgres_catalog_names
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains(&spec.name);
            if already_iceberg || already_postgres {
                // Keep existing registration (ice or postgres); never silent re-register.
                skipped.push(spec.name.clone());
            } else {
                self.register_catalog_spec(spec).await?;
                added.push(spec.name.clone());
            }
        }
        added.sort();
        skipped.sort();
        Ok((added, skipped))
    }

    /// Build and register one parsed [`CatalogSpec`] via its matching `repark-catalog` builder.
    async fn register_catalog_spec(&self, spec: &CatalogSpec) -> Result<()> {
        match spec.kind {
            CatalogKind::Memory => {
                // Parsing guarantees a non-empty `warehouse` prop for the memory kind.
                let warehouse = spec
                    .props
                    .get(catalog_config::WAREHOUSE_PROP)
                    .map(String::as_str)
                    .unwrap_or_default();
                self.register_memory_catalog(&spec.name, warehouse).await
            }
            CatalogKind::Glue => {
                let catalog = repark_iceberg::catalog::glue_catalog(&spec.props)
                    .await
                    .map_err(engine_err)?;
                self.register_iceberg_catalog(&spec.name, catalog).await
            }
            CatalogKind::S3Tables => {
                let catalog = repark_iceberg::catalog::s3tables_catalog(&spec.props)
                    .await
                    .map_err(engine_err)?;
                // S3 Tables assigns each table's location at create (namespaces carry none), so
                // CTAS must route create-first — the policy the CTAS executor keys on.
                self.register_iceberg_catalog_with_policy(
                    &spec.name,
                    catalog,
                    LocationPolicy::ServiceManagedLocation,
                )
                .await
            }
            // Phase cut (design §2): the spec still PARSES (config fidelity — a v1 config map
            // builds unchanged), but registration fails loud until the postgres connector crate
            // (repark-connect) lands. The v1 registration body re-homes with that crate.
            CatalogKind::Postgres => Err(Error::NotImplemented(format!(
                "postgres catalog '{}' registration is not available in the phase-1 engine core \
                 — it returns with the postgres connector crate (the spec parsed; nothing was \
                 registered)",
                spec.name
            ))),
        }
    }

    fn postgres_catalog_names_snapshot(&self) -> HashSet<String> {
        self.postgres_catalog_names
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Create a namespace in a registered catalog (PySpark `CREATE NAMESPACE` / `CREATE DATABASE`),
    /// then re-register the provider so the new namespace is visible to queries.
    ///
    /// A `location` property is mirrored onto `location_uri` before the create
    /// (`repark_iceberg::catalog::mirror_namespace_location_keys` — unidirectional, never overwriting an
    /// explicitly-set key), so the canonical Glue `locationUri` field is set whichever property
    /// key the catalog implementation maps (the fork maps `location_uri`; Java's `GlueCatalog`
    /// maps `location` — audit BUG-001 / U2). This is the chokepoint for the PyO3
    /// `create_namespace` and the facade `spark.create_namespace(..., location=…)` paths.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if `catalog` is unknown; a create failure is classified by
    /// its iceberg kind ([`Error::Analysis`] for an already-existing namespace — Spark's
    /// `NamespaceAlreadyExistsException` is an `AnalysisException` — else [`Error::Iceberg`]).
    pub async fn create_namespace(
        &self,
        catalog: &str,
        namespace: &str,
        mut properties: HashMap<String, String>,
    ) -> Result<()> {
        let handle = self.catalog_handle(catalog)?;
        repark_iceberg::catalog::mirror_namespace_location_keys(&mut properties);
        handle
            .create_namespace(&NamespaceIdent::new(namespace.to_string()), properties)
            .await
            .map_err(iceberg_err)?;
        // The provider snapshots namespaces at construction — re-register to pick up the new one.
        repark_iceberg::catalog::register_iceberg_catalog(self.context(), catalog, handle)
            .await
            .map_err(engine_err)
    }

    /// A cheap clone of the catalog registry (keys + `Arc`s) for passing to the SQL layer.
    fn catalogs_snapshot(&self) -> CatalogRegistry {
        self.catalogs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The registered iceberg handle for `catalog`, or a targeted unknown-catalog error.
    fn catalog_handle(&self, catalog: &str) -> Result<Arc<dyn Catalog>> {
        self.catalogs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(catalog)
            .cloned()
            .ok_or_else(|| Error::DataFusion(format!("unknown catalog '{catalog}'")))
    }

    // === r21 T6: catalog-staleness ============================================================

    /// ===========================================================================================
    /// Live table names in `namespace` from the Iceberg catalog handle (list-on-access).
    ///
    /// Does **not** consult the DataFusion `IcebergCatalogProvider` name snapshot. Used by the
    /// Spark Catalog facade `listTables` so out-of-band creates/drops are visible without a
    /// provider rebuild (T6 / CQ-008 / BUG-007). Non-Iceberg catalogs (postgres) are not in the
    /// registry and fail with unknown-catalog.
    /// ===========================================================================================
    ///
    /// # Errors
    /// Unknown catalog → [`Error::DataFusion`]; list failure → classified iceberg error.
    pub async fn list_iceberg_table_names(
        &self,
        catalog: &str,
        namespace: &str,
    ) -> Result<Vec<String>> {
        let handle = self.catalog_handle(catalog)?;
        repark_iceberg::catalog::list_table_names(handle.as_ref(), namespace)
            .await
            .map_err(engine_err)
    }

    /// ===========================================================================================
    /// Rebuild the DataFusion catalog provider for `catalog` from the live Iceberg handle.
    ///
    /// Product SQL already re-registers after owned DDL. Call this after out-of-band mutations
    /// when free SQL / `information_schema` must see the new name directory. The facade
    /// `listTables` path does **not** require this (it lists live).
    /// ===========================================================================================
    ///
    /// # Errors
    /// Unknown catalog or provider rebuild failure → [`Error::DataFusion`].
    pub async fn refresh_catalog_provider(&self, catalog: &str) -> Result<()> {
        let handle = self.catalog_handle(catalog)?;
        // v1 call preserved: `reregister_catalog_provider` (the catalog crate's full-provider
        // rebuild wrapper, hoisted into `repark_iceberg::catalog::catalog_ops` in PR-B).
        repark_iceberg::catalog::reregister_catalog_provider(self.context(), handle, catalog)
            .await
            .map_err(engine_err)
    }

    /// ===========================================================================================
    /// Test-support only: create a table via the Catalog API **without** re-registering the DF
    /// provider — simulates an out-of-band create (another engine / session on the same handle).
    /// ===========================================================================================
    ///
    /// # Errors
    /// Unknown catalog, create failure, or invalid warehouse path → classified [`Error`].
    pub async fn testing_oob_create_table(
        &self,
        catalog_name: &str,
        namespace: &str,
        table: &str,
        warehouse_location: &str,
    ) -> Result<()> {
        use iceberg::TableCreation;
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        let handle = self.catalog_handle(catalog_name)?;
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .map_err(iceberg_err)?;
        let creation = TableCreation::builder()
            .name(table.to_string())
            .location(format!("{warehouse_location}/{table}"))
            .schema(schema)
            .properties(HashMap::new())
            .build();
        handle
            .create_table(&NamespaceIdent::new(namespace.to_string()), creation)
            .await
            .map_err(iceberg_err)?;
        Ok(())
    }

    /// ===========================================================================================
    /// Test-support only: drop a table via the Catalog API **without** re-registering the DF
    /// provider — simulates an out-of-band drop.
    /// ===========================================================================================
    ///
    /// # Errors
    /// Unknown catalog or drop failure → classified [`Error`].
    pub async fn testing_oob_drop_table(
        &self,
        catalog_name: &str,
        namespace: &str,
        table: &str,
    ) -> Result<()> {
        let handle = self.catalog_handle(catalog_name)?;
        let ident = TableIdent::new(
            NamespaceIdent::new(namespace.to_string()),
            table.to_string(),
        );
        handle.drop_table(&ident).await.map_err(iceberg_err)?;
        Ok(())
    }

    /// ===========================================================================================
    /// Session temp-view names from the default catalog/schema (no `information_schema` scan).
    ///
    /// DataFusion's `information_schema.tables` walks **every** catalog and calls
    /// `SchemaProvider::table_type` → `table()` load for each name. After an out-of-band drop of a
    /// DF-known Iceberg table, that load fails (`TableNotFound`) and aborts the whole query — so
    /// facade `listTables` must **not** use `information_schema` for temps (T6 F-T6-PHANTOM-A).
    /// `SchemaProvider::table_names` is a sync directory walk and never loads table metadata.
    /// ===========================================================================================
    ///
    /// # Errors
    /// Currently infallible (missing default catalog/schema → empty list). `Result` kept for
    /// API symmetry with other listing helpers.
    pub fn list_temp_view_names(&self) -> Result<Vec<String>> {
        let context = self.context();
        let session_config = context.copied_config();
        let catalog_name = session_config.options().catalog.default_catalog.clone();
        let schema_name = session_config.options().catalog.default_schema.clone();
        let Some(catalog) = context.catalog(&catalog_name) else {
            return Ok(Vec::new());
        };
        let Some(schema) = catalog.schema(&schema_name) else {
            return Ok(Vec::new());
        };
        Ok(schema.table_names())
    }

    /// ===========================================================================================
    /// DataFusion provider name directory for `catalog.schema` (no table load / no `info_schema`).
    ///
    /// Used by facade `listTables` for **non-Iceberg** permanent names so listing never materializes
    /// phantom Iceberg tables in other catalogs (same F-T6-PHANTOM-A root cause). For Iceberg,
    /// prefer [`list_iceberg_table_names`] (live Catalog API); this path is snapshot-stale.
    /// ===========================================================================================
    ///
    /// # Errors
    /// Currently infallible (unknown catalog/schema → empty list).
    pub fn list_df_schema_table_names(&self, catalog: &str, schema: &str) -> Result<Vec<String>> {
        let context = self.context();
        let Some(catalog_provider) = context.catalog(catalog) else {
            return Ok(Vec::new());
        };
        let Some(schema_provider) = catalog_provider.schema(schema) else {
            return Ok(Vec::new());
        };
        Ok(schema_provider.table_names())
    }

    /// Register `batches` as a replaceable in-memory view named `name` (PySpark
    /// `createOrReplaceTempView`). The schema is taken from the first batch.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if `batches` is empty (no schema to infer) or registration
    /// fails.
    pub fn create_or_replace_temp_view(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        let schema = batches
            .first()
            .ok_or_else(|| {
                Error::DataFusion(format!(
                    "cannot register temp view '{name}': no batches to infer a schema from"
                ))
            })?
            .schema();
        let table = MemTable::try_new(schema, vec![batches]).map_err(engine_err)?;
        self.replace_view(name, Arc::new(table))
    }

    /// Register a planned [`DataFrame`] as a replaceable temp view named `name` (PySpark
    /// `DataFrame.createOrReplaceTempView`). The view is lazy, like Spark's: each reference
    /// re-executes the plan (materialize with the batch overload above when that matters).
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if registration fails.
    pub fn create_or_replace_temp_view_from(&self, name: &str, frame: &DataFrame) -> Result<()> {
        self.replace_view(name, frame.clone().into_view())
    }

    /// ===========================================================================================
    /// Collect `frame` **once** and re-register as a [`MemTable`] temp view so subsequent scans
    /// are table scans, not re-execution of a VALUES (or other) body (R-PERF-VALUES).
    ///
    /// Empty results still register (schema from the plan, zero batches) so `.count()` / filters
    /// on an empty createDataFrame stay correct.
    ///
    /// **Contract (r23 CACHE1):** this entry point is the createDataFrame / VALUES path. Do **not**
    /// change its collect-once semantics for cache/persist — use
    /// [`Self::materialize_dataframe_as_cache_view`] for the cache path (caller-level branch).
    /// ===========================================================================================
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if collect or registration fails.
    pub async fn materialize_dataframe_as_temp_view(
        &self,
        name: &str,
        frame: DataFrame,
    ) -> Result<()> {
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let batches = frame.collect().await.map_err(engine_err)?;
        let partitions = if batches.is_empty() {
            // MemTable with known schema and no rows — empty createDataFrame still needs a view.
            vec![vec![]]
        } else {
            vec![batches]
        };
        let table = MemTable::try_new(schema, partitions).map_err(engine_err)?;
        self.replace_view(name, Arc::new(table))
    }

    // === r23 CACHE1: cache-honesty ===
    /// ===========================================================================================
    /// Cache-path materialize: collect once into a [`MemTable`] temp view with an optional
    /// `max_bytes` size guard (facade conf ``repark.cache.max_bytes``).
    ///
    /// **Caller-level branch (OTH-014 / CACHE1):** `cache()` / `persist()` use this entry point.
    /// createDataFrame / VALUES keep [`Self::materialize_dataframe_as_temp_view`] (byte-identical
    /// collect-once; no size threshold — data was already Python-resident).
    ///
    /// Single-node [`MemTable`] only — no disk spill despite Spark `MEMORY_AND_DISK*` names. When
    /// `max_bytes` is `Some(limit)` and the collected Arrow array memory exceeds `limit`, the
    /// batches are dropped and [`Error::Config`] names the conf key (loud memory contract).
    /// Size is measured **after** `collect` (refuses the pin; peak during collect is still
    /// O(result)). `None` or an absent conf = no size guard (`FairSpillPool` still bounds
    /// execution).
    /// ===========================================================================================
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if collect or registration fails; [`Error::Config`] when
    /// `max_bytes` is exceeded.
    pub async fn materialize_dataframe_as_cache_view(
        &self,
        name: &str,
        frame: DataFrame,
        max_bytes: Option<u64>,
    ) -> Result<()> {
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let batches = frame.collect().await.map_err(engine_err)?;
        if let Some(limit) = max_bytes {
            let total: u64 = batches
                .iter()
                .map(|batch| u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX))
                .fold(0_u64, u64::saturating_add);
            if total > limit {
                return Err(Error::Config(format!(
                    "cache materialize size {total} bytes exceeds repark.cache.max_bytes={limit}; \
                     raise the conf or avoid cache()/persist() on this plan (single-node MemTable \
                     pin; no disk spill)"
                )));
            }
        }
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let table = MemTable::try_new(schema, partitions).map_err(engine_err)?;
        self.replace_view(name, Arc::new(table))
    }

    /// ===========================================================================================
    /// Register pre-built Arrow [`RecordBatch`]es as a [`MemTable`] temp view (R-PERF-ARROW-CDF).
    ///
    /// Used by createDataFrame after Python builds a `pyarrow.Table` — skips VALUES SQL entirely.
    /// Empty `batches` still registers when `schema` is provided (zero-row frame).
    /// ===========================================================================================
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if `MemTable` construction or registration fails.
    pub fn register_record_batches_as_temp_view(
        &self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let table = MemTable::try_new(schema, partitions).map_err(engine_err)?;
        self.replace_view(name, Arc::new(table))
    }

    /// The shared "create OR REPLACE" registration: `register_table` errors on a name clash, so
    /// drop any existing view first (a no-op returning `Ok(None)` when absent).
    fn replace_view(
        &self,
        name: &str,
        provider: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Result<()> {
        self.context().deregister_table(name).map_err(engine_err)?;
        self.context()
            .register_table(name, provider)
            .map_err(engine_err)?;
        Ok(())
    }

    /// Drop a temp view (PySpark `spark.catalog.dropTempView`). Returns whether a view of that
    /// name existed — dropping a missing view is not an error, matching PySpark.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if the name cannot be resolved as a table reference.
    pub fn drop_temp_view(&self, name: &str) -> Result<bool> {
        Ok(self
            .context()
            .deregister_table(name)
            .map_err(engine_err)?
            .is_some())
    }

    /// Whether a table exists (PySpark `spark.catalog.tableExists`). A three-part
    /// `catalog.namespace.table` name asks the registered iceberg catalog (`false` when the
    /// namespace itself is absent, like PySpark); a one-part name checks the session's temp
    /// views. Names are split quote-aware (double-quote / backtick; dots allowed inside quotes —
    /// C2-L-006); two-part names need the default-catalog resolution that is a tracked follow-up.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] for a two-part name or an unregistered catalog; a catalog
    /// probe failure is classified by its iceberg kind (base [`Error::Iceberg`] for
    /// infrastructure faults).
    pub async fn table_exists(&self, name: &str) -> Result<bool> {
        // Quote-aware split (C2-L-006): matches the Python `_sql_table_ref` segment rules so
        // `catalog."db.with.dot".t` and backtick forms resolve the same way as `spark.table`.
        let parts = parse_table_identifier_segments(name).map_err(|message| {
            Error::DataFusion(format!("tableExists: invalid table identifier: {message}"))
        })?;
        match parts.as_slice() {
            [view] => self
                .context()
                .table_exist(view.as_str())
                .map_err(engine_err),
            [catalog, namespace, table] => {
                let handle = self.catalog_handle(catalog)?;
                let namespace = NamespaceIdent::new(namespace.clone());
                if !handle
                    .namespace_exists(&namespace)
                    .await
                    .map_err(iceberg_err)?
                {
                    return Ok(false);
                }
                handle
                    .table_exists(&TableIdent::new(namespace, table.clone()))
                    .await
                    .map_err(iceberg_err)
            }
            _ => Err(Error::DataFusion(format!(
                "tableExists supports `catalog.namespace.table` or a bare temp-view name, \
                 got '{name}' (default-catalog resolution is a tracked follow-up)"
            ))),
        }
    }

    /// Register the AWS-free in-memory Iceberg catalog (local-filesystem `warehouse`) under
    /// `name` — local development and tests. Glue / S3 Tables catalogs register through
    /// `spark.sql.catalog.*` / `repark.sql.catalog.*` config or [`Self::register_iceberg_catalog`]
    /// with a `repark-catalog` builder (see `repark-catalog`). Rejects a `name` that is already
    /// registered: the in-memory catalog keeps table METADATA in process memory, so silently
    /// replacing the handle would orphan every table committed through the old one.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if `name` is already registered or the catalog cannot be
    /// built or registered.
    pub async fn register_memory_catalog(&self, name: &str, warehouse: &str) -> Result<()> {
        if self.catalog_handle(name).is_ok() {
            return Err(Error::DataFusion(format!(
                "catalog '{name}' is already registered — re-registering an in-memory catalog \
                 would orphan its tables (their metadata lives in the replaced handle)"
            )));
        }
        let catalog = repark_iceberg::catalog::memory_catalog(warehouse)
            .await
            .map_err(engine_err)?;
        // The in-memory / LocalFs catalog keeps the offline temp-location fallback for CTAS into a
        // namespace with no `location` property (real warehouses fail loud instead). E-4: the
        // fallback ROOT is resolved here, once, at registration time — the CTAS consumer reads
        // the policy's `root` and never touches the process environment at query time.
        self.register_iceberg_catalog_with_policy(
            name,
            catalog,
            LocationPolicy::TempFallbackAllowed {
                root: std::env::temp_dir(),
            },
        )
        .await?;
        // SEC-02 grandfather: COPY TO / CREATE EXTERNAL under this warehouse stay allowed.
        self.catalogs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .note_local_warehouse_root(warehouse.to_string());
        Ok(())
    }

    /// ===========================================================================================
    /// Mark a local path as a trusted write root for the SEC-02 local-filesystem DDL gate.
    /// ===========================================================================================
    ///
    /// The SEC-02 gate exists to keep **untrusted free SQL** from reaching arbitrary local paths
    /// (`SECURITY.md` "Input surfaces": the gated rows are *Free SQL* `CREATE EXTERNAL TABLE` and
    /// *Free SQL* `COPY … TO`). The typed writer API is a different surface: `df.write.csv(path)`
    /// is the caller naming a destination in their own code, exactly like `spark.read.parquet`,
    /// which that same table lists as un-gated.
    ///
    /// The facade implements writes by generating `COPY … TO` and running it through the ordinary
    /// SQL path, so without this the gate would refuse every local `DataFrameWriter` call. The
    /// writer calls this with the destination the caller passed, which registers *that path only*
    /// — free SQL to any other local path still refuses.
    pub fn note_local_write_root(&self, path: &str) {
        self.catalogs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .note_local_warehouse_root(path.to_string());
    }

    /// Read a Parquet file or directory into a [`DataFrame`] (PySpark `spark.read.parquet`).
    ///
    /// An `s3://` (Iceberg warehouse) or `s3a://` (Spark bronze) path triggers a lazy, once-per-
    /// bucket registration of an authenticated S3 object store on the `RuntimeEnv` before planning
    /// (see [`object_store_s3`]); the scheme is preserved (both are registered), so no path rewrite
    /// is needed. Non-S3 paths (local, relative) are passed to DataFusion unchanged.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if the S3 store cannot be built (region/credentials), or if the
    /// path cannot be read or planned.
    pub async fn read_parquet(&self, path: &str) -> Result<DataFrame> {
        if let Some((_scheme, bucket)) = object_store_s3::parse_s3_bucket(path) {
            self.ensure_s3_bucket_registered(&bucket)?;
        }
        self.context()
            .read_parquet(path, ParquetReadOptions::default())
            .await
            .map_err(engine_err)
    }

    /// ===========================================================================================
    /// Read a CSV file or directory into a DataFusion [`DataFrame`] (PySpark `spark.read.csv`).
    ///
    /// Options are a case-insensitive string map (Spark reader option keys). The facade applies
    /// Spark defaults (`header=false`, etc.) before calling; unknown keys are ignored here (the
    /// facade fails loud on semantic keys repark does not honor).
    ///
    /// # Errors
    /// Returns [`Error::Analysis`] for malformed option values; [`Error::DataFusion`] on I/O/plan.
    /// ===========================================================================================
    pub async fn read_csv(
        &self,
        path: &str,
        options: &HashMap<String, String>,
    ) -> Result<DataFrame> {
        if let Some((_scheme, bucket)) = object_store_s3::parse_s3_bucket(path) {
            self.ensure_s3_bucket_registered(&bucket)?;
        }
        let mut csv_options = csv_read_options_from_map(options)?;
        // nullValue: force all-Utf8 schema so the scan path never type-parses null tokens
        // (DF null_regex is inference-only — octo R1-C1-001). Owned schema must outlive options.
        let utf8_schema = if options.contains_key("nullvalue") {
            csv_utf8_schema_from_path(path, csv_options.has_header, csv_options.delimiter)?
        } else {
            None
        };
        if let Some(ref schema) = utf8_schema {
            csv_options = csv_options.schema(schema);
        }
        self.context()
            .read_csv(path, csv_options)
            .await
            .map_err(engine_err)
    }

    /// ===========================================================================================
    /// Read a JSON file or directory into a DataFusion [`DataFrame`] (PySpark `spark.read.json`).
    ///
    /// Default is newline-delimited JSON (Spark `multiLine=false`). `multiLine=true` maps to
    /// DataFusion `newline_delimited=false` (JSON array / multi-line object file).
    ///
    /// # Errors
    /// Returns [`Error::Analysis`] for malformed option values; [`Error::DataFusion`] on I/O/plan.
    /// ===========================================================================================
    pub async fn read_json(
        &self,
        path: &str,
        options: &HashMap<String, String>,
    ) -> Result<DataFrame> {
        if let Some((_scheme, bucket)) = object_store_s3::parse_s3_bucket(path) {
            self.ensure_s3_bucket_registered(&bucket)?;
        }
        let json_options = json_read_options_from_map(options)?;
        self.context()
            .read_json(path, json_options)
            .await
            .map_err(engine_err)
    }

    /// ===========================================================================================
    /// Read an Iceberg catalog table, optionally pinned to a snapshot / ref / timestamp.
    ///
    /// With no pin (`opts` empty) this is `SELECT * FROM <ident>` against the current snapshot
    /// (same as [`Self::sql`] / the catalog provider). With a pin, builds a fork
    /// `IcebergStaticTableProvider` for that snapshot and returns a [`DataFrame`] over it — never a
    /// post-hoc filter (I1 / R-TIME-TRAVEL).
    ///
    /// # Errors
    /// Mutual-exclusion / parse / unknown-snapshot / catalog errors as classified [`Error`].
    /// ===========================================================================================
    pub async fn read_iceberg_table(
        &self,
        table_name: &str,
        opts: TimeTravelOpts,
    ) -> Result<DataFrame> {
        let spec = opts.into_spec()?;
        let parts = parse_table_identifier_segments(table_name).map_err(|message| {
            Error::Analysis(format!(
                "read_iceberg_table: invalid table identifier: {message}"
            ))
        })?;
        match spec {
            None => self.sql(&format!("SELECT * FROM {table_name}")).await,
            Some(spec) => {
                let catalogs = self.catalogs_snapshot();
                time_travel::read_table_at(self.context(), &catalogs, &parts, &spec)
                    .await
                    .map_err(engine_err)
            }
        }
    }

    /// ===========================================================================================
    /// Test-support only: create a branch or tag ref on an Iceberg table (`ManageSnapshots`).
    ///
    /// Product SQL `ALTER TABLE … CREATE BRANCH|TAG` / `CREATE BRANCH … IN` lands in I5; this
    /// seam **stays** for fixtures that prefer the programmatic path (I1 / metadata-table tests).
    /// ===========================================================================================
    ///
    /// # Errors
    /// Unknown catalog/table, unknown snapshot, or ref already exists → classified [`Error`].
    pub async fn testing_create_ref(
        &self,
        table_name: &str,
        kind: &str,
        ref_name: &str,
        snapshot_id: i64,
    ) -> Result<()> {
        let parts = parse_table_identifier_segments(table_name).map_err(|message| {
            Error::Analysis(format!(
                "_testing_create_ref: invalid table identifier: {message}"
            ))
        })?;
        let [catalog_name, namespace, table] = parts.as_slice() else {
            return Err(Error::Analysis(format!(
                "_testing_create_ref requires catalog.namespace.table, got '{table_name}'"
            )));
        };
        let kind = match kind.to_ascii_lowercase().as_str() {
            "branch" => repark_iceberg::write::SnapshotRefKind::Branch,
            "tag" => repark_iceberg::write::SnapshotRefKind::Tag,
            other => {
                return Err(Error::Analysis(format!(
                    "_testing_create_ref kind must be \"branch\" or \"tag\", got {other:?}"
                )));
            }
        };
        let handle = self.catalog_handle(catalog_name)?;
        let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
        repark_iceberg::write::testing_create_ref(
            handle.as_ref(),
            &ident,
            kind,
            ref_name,
            snapshot_id,
        )
        .await
        .map_err(iceberg_err)?;
        // Re-register so any catalog-provider snapshot of refs is refreshed (defensive; reads use
        // load_table + Static provider and do not require the catalog provider for refs).
        let catalogs = self.catalogs_snapshot();
        if let Some(catalog) = catalogs.get(catalog_name) {
            repark_iceberg::catalog::reregister_catalog_provider(
                self.context(),
                catalog.clone(),
                catalog_name,
            )
            .await
            .map_err(engine_err)?;
        }
        Ok(())
    }

    /// ===========================================================================================
    /// Test-support only: list snapshot `(snapshot_id, timestamp_ms)` pairs in history order.
    /// ===========================================================================================
    ///
    /// # Errors
    /// Unknown catalog/table → classified [`Error`].
    pub async fn testing_list_snapshots(&self, table_name: &str) -> Result<Vec<(i64, i64)>> {
        let parts = parse_table_identifier_segments(table_name).map_err(|message| {
            Error::Analysis(format!(
                "_testing_list_snapshots: invalid table identifier: {message}"
            ))
        })?;
        let [catalog_name, namespace, table] = parts.as_slice() else {
            return Err(Error::Analysis(format!(
                "_testing_list_snapshots requires catalog.namespace.table, got '{table_name}'"
            )));
        };
        let handle = self.catalog_handle(catalog_name)?;
        let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
        let table = handle.load_table(&ident).await.map_err(iceberg_err)?;
        Ok(table
            .metadata()
            .history()
            .iter()
            .map(|entry| (entry.snapshot_id, entry.timestamp_ms))
            .collect())
    }

    /// Ensure an authenticated S3 object store for `bucket` is registered on the `RuntimeEnv` under
    /// both the `s3://` and `s3a://` URL forms — building it (from the FINALIZE-resolved AWS SDK
    /// config, E-2) at most once per bucket for the session's lifetime.
    ///
    /// The store build runs OUTSIDE the `Mutex` (E-2 made it sync — the chain is pre-resolved, so
    /// nothing awaits here anymore), then a brief re-lock records the bucket and registers the
    /// store. Two racing callers each build a store,
    /// but `HashSet::insert` gates registration so exactly one wins and the loser's store is dropped
    /// — DataFusion registration is idempotent regardless.
    ///
    /// # Errors
    /// Returns [`Error::DataFusion`] if the session never resolved its SDK config (the E-2 gate,
    /// naming the finalize step) or the S3 store cannot be built or registered.
    fn ensure_s3_bucket_registered(&self, bucket: &str) -> Result<()> {
        if self
            .registered_s3_buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(bucket)
        {
            return Ok(());
        }
        // E-2 gate: the store build consumes the FINALIZE-resolved SDK config — an S3-path read
        // on a session that never resolved fails loud naming the missing step, never a silent
        // lazy chain resolution (v1's query-time env read, removed).
        let Some(sdk_config) = self.aws_sdk_config.get() else {
            return Err(Error::DataFusion(format!(
                "S3 read for bucket '{bucket}' refused: this session never resolved its AWS SDK \
                 config. Call register_configured_catalogs() after signaling AWS use — an \
                 AWS-backed catalog (spark.sql.catalog.*), the \
                 '{}' / '{}' region conf, or the explicit opt-in \
                 `{AWS_ENABLE_CONFIG_KEY}=true`.",
                object_store_s3::REPARK_S3A_REGION_CONFIG_KEY,
                object_store_s3::S3A_REGION_CONFIG_KEY,
            )));
        };
        let store = object_store_s3::build_amazon_s3_store(
            bucket,
            self.s3_region_override.as_deref(),
            sdk_config,
        )?;
        let mut registered = self
            .registered_s3_buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registered.insert(bucket.to_string()) {
            object_store_s3::register_bucket_store(self.context(), bucket, &store)?;
        }
        Ok(())
    }

    /// Register an object store for `bucket` under both S3 URL schemes (test seam for the AWS-free
    /// scheme-routing e2e: register an in-memory store and prove `read_parquet` routes to it).
    #[cfg(test)]
    fn register_s3_bucket_store_for_test(
        &self,
        bucket: &str,
        store: &Arc<dyn object_store::ObjectStore>,
    ) -> Result<()> {
        self.registered_s3_buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(bucket.to_string());
        object_store_s3::register_bucket_store(self.context(), bucket, store)
    }
}

impl std::fmt::Debug for ReparkSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReparkSession").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod aws_gate_tests;

#[cfg(test)]
mod tests;
