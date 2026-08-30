//! A session-centered DataFusion context with catalog, reader, temp-view, and SQL-door APIs.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock};

use aws_config::{BehaviorVersion, SdkConfig};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionConfig, SessionContext};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_common::{Error, Result};
use repark_iceberg::catalog::build_iceberg_catalog_provider;

use crate::backend::{ExecutionBackend, SingleNodeBackend};
use crate::catalog_config::{self, CatalogKind, CatalogSpec};
use crate::catalog_state::{CatalogRegistry, LocationPolicy, memory_warehouse_fallback_root};
use crate::dialect::{DataFusionDialect, EngineContext, SqlDialect};
use crate::extension::{NoopSessionExtension, SessionBuildConf, SessionExtension};
use crate::session_time_zone::{SessionTimeZone, resolve_session_time_zone};
use crate::temp_view::TempViewHome;
use crate::time_travel::{self, TimeTravelSpec};
// Test-only re-exports follow the production imports.
#[cfg(test)]
pub(crate) use crate::error_map::{EngineErrorKind, classify_datafusion_error};
#[cfg(test)]
pub(crate) use crate::idents::reject_path_escape_segment;
use crate::{
    csv_read_options_from_map, csv_utf8_schema_from_path, engine_err, iceberg_err,
    json_read_options_from_map, object_store_s3, parse_table_identifier_segments,
    resolve_s3_region_override,
};

mod df_guards;
mod spill;
mod temp_views;

use df_guards::{apply_df_54_1_config_guards, context_with_df_54_1_rule_guards};

pub(crate) use spill::BYTES_PER_GB;
pub use spill::REPARK_OWNED_DATAFUSION_PSEUDO_KEYS;
#[cfg(test)]
pub(crate) use spill::{
    DEFAULT_MEMORY_LIMIT_BYTES, MIN_MEMORY_LIMIT_BYTES, default_memory_limit_bytes,
};

/// Iceberg reader time-travel options (Spark `snapshot-id` / `as-of-timestamp` / `branch` / `tag`).
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
    /// Convert to a [`TimeTravelSpec`], or `None` when no pin is set.
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
        // Trim branch/tag (SQL VERSION AS OF already trims via parse_version_value).
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

/// The builder-config prefix that reaches DataFusion's own [`SessionConfig`] options
pub const DATAFUSION_CONFIG_PREFIX: &str = "datafusion.";

/// Session default for DataFusion `batch_size` (rows per Arrow batch).
pub const DEFAULT_BATCH_SIZE: usize = 65536;

/// Apply every `datafusion.*` key from the builder config map onto `config`.
fn apply_datafusion_config_keys(
    config: &mut SessionConfig,
    map: &HashMap<String, String>,
) -> Result<()> {
    let mut keys: Vec<&String> = map
        .keys()
        .filter(|key| {
            key.starts_with(DATAFUSION_CONFIG_PREFIX)
                && !REPARK_OWNED_DATAFUSION_PSEUDO_KEYS.contains(&key.as_str())
        })
        .collect();
    keys.sort();
    for key in keys {
        let value = &map[key];
        config.options_mut().set(key, value).map_err(|error| {
            Error::Config(format!(
                "invalid DataFusion session config '{key}' = '{value}': {error}"
            ))
        })?;
    }
    Ok(())
}

/// The explicit AWS-use opt-in conf (E-2).
pub const AWS_ENABLE_CONFIG_KEY: &str = "repark.aws.enable";

/// Builder for a [`ReparkSession`] with optional memory, batch, partition, dialect, extension, and
#[derive(Clone, Default)]
pub struct ReparkSessionBuilder {
    memory_limit_bytes: Option<usize>,
    batch_size: Option<usize>,
    target_partitions: Option<usize>,
    /// The session-default [`SqlDialect`].
    sql_dialect: Option<Arc<dyn SqlDialect>>,
    /// The build-time [`SessionExtension`].
    extension: Option<Arc<dyn SessionExtension>>,
    /// The full Spark-style `.config(key, value)` map.
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
    /// Install the session-default [`SqlDialect`].
    #[must_use]
    pub fn with_sql_dialect(mut self, dialect: Arc<dyn SqlDialect>) -> Self {
        self.sql_dialect = Some(dialect);
        self
    }

    /// Install the build-time [`SessionExtension`].
    #[must_use]
    pub fn with_extension(mut self, extension: Arc<dyn SessionExtension>) -> Self {
        self.extension = Some(extension);
        self
    }

    /// Record one Spark-style configuration pair.
    #[must_use]
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }

    /// Record a whole configuration map at once.
    #[must_use]
    pub fn configs(mut self, config: HashMap<String, String>) -> Self {
        self.config.extend(config);
        self
    }

    /// Cap engine memory at `gb` gigabytes with a `FairSpillPool`.
    #[must_use]
    pub fn memory_limit_gb(mut self, gb: usize) -> Self {
        self.memory_limit_bytes = Some(gb.saturating_mul(BYTES_PER_GB));
        self
    }

    /// Cap engine memory at an explicit byte budget.
    #[must_use]
    pub fn memory_limit_bytes(mut self, bytes: usize) -> Self {
        self.memory_limit_bytes = Some(bytes);
        self
    }

    /// Set rows per Arrow batch.
    #[must_use]
    pub fn batch_size(mut self, rows: usize) -> Self {
        self.batch_size = Some(rows);
        self
    }

    /// Set the degree of intra-query parallelism.
    #[must_use]
    pub fn target_partitions(mut self, partitions: usize) -> Self {
        self.target_partitions = Some(partitions);
        self
    }

    /// Build the session synchronously.
    /// # Errors
    /// Returns [`Error::DataFusion`] if the DataFusion runtime fails to build, or
    pub fn build(self) -> Result<ReparkSession> {
        if let Some(0) = self.batch_size {
            return Err(Error::Config("batch_size must be >= 1 (got 0)".to_string()));
        }
        if let Some(0) = self.target_partitions {
            return Err(Error::Config(
                "target_partitions must be >= 1 (got 0)".to_string(),
            ));
        }
        let pool_bytes =
            spill::resolve_build_time_pool_bytes(self.memory_limit_bytes, &self.config)?;

        let catalog_specs = catalog_config::parse_catalog_specs(&self.config)?;
        // The session timezone, resolved and VALIDATED here — once, at construction — so no
        let session_time_zone = resolve_session_time_zone(&self.config)?;
        // The optional `s3://`/`s3a://` read region override (else the aws-config chain resolves
        let s3_region_override = resolve_s3_region_override(&self.config)?;
        // E-2: does this session SIGNAL AWS use? An AWS-backed catalog spec, the S3-region conf
        let aws_signaled = catalog_specs
            .iter()
            .any(|spec| matches!(spec.kind, CatalogKind::Glue | CatalogKind::S3Tables))
            || s3_region_override.is_some()
            || self
                .config
                .get(AWS_ENABLE_CONFIG_KEY)
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"));
        // Write-path concurrency (session conf only — never a table property).
        let write_concurrency = repark_iceberg::write::concurrency_from_config_map(&self.config)
            .map_err(|error| Error::Config(error.to_string()))?;
        let scan_pruning =
            repark_iceberg::write::scan_prune::scan_pruning_from_config_map(&self.config)
                .map_err(|error| Error::Config(error.to_string()))?;
        let file_scoped_rewrite =
            repark_iceberg::write::file_scoped_rewrite_from_config_map(&self.config)
                .map_err(|error| Error::Config(error.to_string()))?;
        // MERGE target-scan file concurrency (session conf only).
        let scan_concurrency =
            repark_iceberg::write::scan_concurrency_from_config_map(&self.config)
                .map_err(|error| Error::Config(error.to_string()))?;
        // The extension hooks run at fixed positions in this construction order.
        let ext: Arc<dyn SessionExtension> = self
            .extension
            .clone()
            .unwrap_or_else(|| Arc::new(NoopSessionExtension));
        let mut config = SessionConfig::new();
        apply_df_54_1_config_guards(&mut config);
        config = repark_iceberg::write::with_merge_session_knobs(
            config,
            scan_pruning,
            file_scoped_rewrite,
        );
        config = repark_iceberg::write::with_scan_concurrency(config, scan_concurrency);
        // Typed settings use defaults unless an explicit configuration key overrides them below.
        config = config.with_batch_size(self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE));
        if let Some(partitions) = self.target_partitions {
            config = config.with_target_partitions(partitions);
        }
        config = repark_iceberg::write::with_write_concurrency(config, write_concurrency);
        // Explicit DataFusion keys override typed setters and defaults before extension configure.
        apply_datafusion_config_keys(&mut config, &self.config)?;
        // Configure runs after engine options and before runtime assembly.
        config = ext
            .configure(
                SessionBuildConf {
                    conf: &self.config,
                    session_time_zone: &session_time_zone,
                },
                config,
            )
            .map_err(engine_err)?;

        let mut runtime = RuntimeEnvBuilder::new();
        // FairSpillPool when set; explicit 0 / `'0'` opts out (unbounded).
        runtime = spill::with_memory_pool(runtime, pool_bytes);
        runtime = spill::with_temp_directory(runtime, &self.config)?;
        // DataFusion caches directory listings by path on the RuntimeEnv object-list cache.
        runtime = runtime.with_object_list_cache_limit(0);
        let runtime = runtime.build_arc().map_err(engine_err)?;

        // NOT `SessionContext::new_with_config_rt`: DF-54.1 regression guard 2 replaces ONE
        let context = context_with_df_54_1_rule_guards(config, runtime);
        // Capture the final build-time home and its provider identity once; calls re-check it.
        let temp_view_home = {
            let options = context.copied_config();
            let catalog_options = &options.options().catalog;
            let catalog_name = catalog_options.default_catalog.clone();
            let schema_name = catalog_options.default_schema.clone();
            let provider = context
                .catalog(&catalog_name)
                .and_then(|catalog| catalog.schema(&schema_name));
            TempViewHome {
                catalog: catalog_name,
                schema: schema_name,
                provider,
            }
        };
        // Register runs immediately after context creation.
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
            session_time_zone: Arc::new(session_time_zone),
            temp_view_home: Arc::new(temp_view_home),
            postgres_catalog_names: Arc::new(RwLock::new(HashSet::new())),
            aws_signaled,
            aws_sdk_config: Arc::new(OnceLock::new()),
        })
    }
}

/// `ReparkSession` — the near-drop-in entrypoint (`from repark import ReparkSession`).
#[derive(Clone)]
pub struct ReparkSession {
    backend: Arc<dyn ExecutionBackend>,
    /// The session-default [`SqlDialect`] every [`sql`](Self::sql) call routes through
    dialect: Arc<dyn SqlDialect>,
    /// iceberg `Catalog` handles by registered name.
    catalogs: Arc<RwLock<CatalogRegistry>>,
    /// Names of registered postgres read catalogs (`SessionContext` only — not in
    postgres_catalog_names: Arc<RwLock<HashSet<String>>>,
    /// The catalogs configured via `spark.sql.catalog.<name>.*`, parsed at build time and
    catalog_specs: Arc<Vec<CatalogSpec>>,
    /// The set of S3 buckets whose object store has already been registered on the `RuntimeEnv`,
    registered_s3_buckets: Arc<Mutex<HashSet<String>>>,
    /// Optional explicit region for `s3`/`s3a` reads (the `spark.hadoop.fs.s3a.endpoint.region`
    s3_region_override: Arc<Option<String>>,
    /// The session timezone (`spark.sql.session.timeZone`), parsed and validated ONCE at
    session_time_zone: Arc<SessionTimeZone>,
    /// R6-1: where this session's temp views live, captured ONCE at
    temp_view_home: Arc<TempViewHome>,
    /// E-2: whether this session signaled AWS use at build time (an AWS-backed catalog spec, an
    aws_signaled: bool,
    /// E-2: the AWS SDK config resolved ONCE at finalize (`register_configured_catalogs` /
    aws_sdk_config: Arc<OnceLock<SdkConfig>>,
}

impl ReparkSession {
    /// Start configuring a session (PySpark `SparkSession.builder`).
    #[must_use]
    pub fn builder() -> ReparkSessionBuilder {
        ReparkSessionBuilder::default()
    }

    /// Build a session with all defaults.
    /// # Errors
    /// Returns [`Error::DataFusion`] if the DataFusion runtime fails to build.
    pub fn new() -> Result<Self> {
        Self::builder().build()
    }

    /// Return the raw DataFusion context.
    #[must_use]
    pub fn context(&self) -> &SessionContext {
        self.backend.session_context()
    }

    /// Return the validated session timezone resolved during [`ReparkSessionBuilder::build`].
    #[must_use]
    pub fn session_time_zone(&self) -> &SessionTimeZone {
        &self.session_time_zone
    }

    /// Run a SQL string through the session-default [`SqlDialect`].
    /// # Errors
    /// Returns the classified [`Error`]: [`Error::Parse`] on a syntax error, [`Error::Analysis`]
    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        let dialect = Arc::clone(&self.dialect);
        self.sql_with(&dialect, query).await
    }

    /// Run a SQL string under an explicit dialect without changing the session default.
    /// # Errors
    /// Identical classification to [`Self::sql`] — the [`engine_err`] fold is session-side, so
    pub async fn sql_with(&self, dialect: &Arc<dyn SqlDialect>, query: &str) -> Result<DataFrame> {
        // Intercept SET datafusion.runtime.memory_limit (FairSpillPool swap) and refuse SET
        if let Some(frame) = spill::maybe_apply_runtime_set(self.context(), query)? {
            return Ok(frame);
        }
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

    /// Register an Iceberg [`Catalog`] as both a DataFusion provider and session write handle.
    /// # Errors
    /// Returns [`Error::DataFusion`] if the name is already registered or the catalog's
    pub async fn register_iceberg_catalog(
        &self,
        name: &str,
        catalog: Arc<dyn Catalog>,
    ) -> Result<()> {
        // An externally-supplied catalog is treated as a real warehouse (Glue / S3 Tables register
        self.register_iceberg_catalog_with_policy(
            name,
            catalog,
            LocationPolicy::RequireExplicitLocation,
        )
        .await
    }

    /// Register an iceberg [`Catalog`] with a staged-CTAS [`LocationPolicy`].
    /// # Errors
    /// Returns [`Error::DataFusion`] for a duplicate name or a namespace/schema load failure.
    async fn register_iceberg_catalog_with_policy(
        &self,
        name: &str,
        catalog: Arc<dyn Catalog>,
        location_policy: LocationPolicy,
    ) -> Result<()> {
        let duplicate = || Error::DataFusion(format!("catalog '{name}' is already registered"));
        let already_registered = {
            let catalogs = RwLock::read(&self.catalogs).unwrap_or_else(PoisonError::into_inner);
            catalogs.get(name).is_some()
        };
        if already_registered {
            return Err(duplicate());
        }
        let provider = build_iceberg_catalog_provider(catalog.clone())
            .await
            .map_err(engine_err)?;
        let mut catalogs = RwLock::write(&self.catalogs).unwrap_or_else(PoisonError::into_inner);
        if catalogs.get(name).is_some() {
            return Err(duplicate());
        }
        self.context().register_catalog(name, provider);
        catalogs.insert(name.to_string(), catalog, location_policy);
        Ok(())
    }

    /// Register catalog specifications parsed during `build()`.
    /// # Errors
    /// Returns [`Error::DataFusion`] if a configured catalog cannot be built or registered (e.g.
    pub async fn register_configured_catalogs(&self) -> Result<()> {
        // E-2: conditional finalize-time AWS resolution.
        self.resolve_aws_sdk_config_if(self.aws_signaled).await;
        for spec in self.catalog_specs.iter() {
            self.register_catalog_spec(spec).await?;
        }
        Ok(())
    }

    /// Resolve and store the session AWS SDK config when signaled.
    async fn resolve_aws_sdk_config_if(&self, signaled: bool) {
        if signaled && self.aws_sdk_config.get().is_none() {
            let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
            // A racing clone may have set it first; the first resolution wins (identical chain).
            let _ = self.aws_sdk_config.set(sdk_config);
        }
    }

    /// Test-only observability for the E-2 gate: whether the finalize step resolved the
    #[cfg(test)]
    pub(crate) fn testing_aws_sdk_config_resolved(&self) -> bool {
        self.aws_sdk_config.get().is_some()
    }

    /// Register catalogs from a late configuration map onto the live session.
    /// # Errors
    /// Returns [`Error::Config`] if the `spark.sql.catalog.*` block is malformed, or
    pub async fn register_late_configured_catalogs(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let specs = catalog_config::parse_catalog_specs(config)?;
        // Late configuration can introduce the first AWS signal for an offline session.
        let late_aws_signaled = specs
            .iter()
            .any(|spec| matches!(spec.kind, CatalogKind::Glue | CatalogKind::S3Tables))
            || resolve_s3_region_override(config)?.is_some()
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
                // Keep existing registrations; never replace them silently.
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
                self.register_iceberg_catalog_with_policy(
                    &spec.name,
                    catalog,
                    LocationPolicy::ServiceManagedLocation,
                )
                .await
            }
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

    /// Create a namespace and refresh its DataFusion provider.
    /// # Errors
    /// Returns [`Error::DataFusion`] if `catalog` is unknown; [`Error::Analysis`] if the namespace
    pub async fn create_namespace(
        &self,
        catalog: &str,
        namespace: &str,
        mut properties: HashMap<String, String>,
    ) -> Result<()> {
        let handle = self.catalog_handle(catalog)?;
        repark_iceberg::catalog::mirror_namespace_location_keys(&mut properties);
        let ident = NamespaceIdent::new(namespace.to_string());
        if handle.namespace_exists(&ident).await.map_err(iceberg_err)? {
            let existing = handle.get_namespace(&ident).await.map_err(iceberg_err)?;
            crate::refuse_contradictory_namespace_location(
                namespace,
                existing.properties(),
                &properties,
            )
            .map_err(Error::Analysis)?;
            return Ok(());
        }
        handle
            .create_namespace(&ident, properties)
            .await
            .map_err(iceberg_err)?;
        // The provider snapshots namespaces at construction — re-register to pick up the new one.
        repark_iceberg::catalog::register_iceberg_catalog(self.context(), catalog, handle)
            .await
            .map_err(engine_err)
    }

    /// A cheap clone of the catalog registry (keys + `Arc`s) for passing to the SQL layer.
    pub(crate) fn catalogs_snapshot(&self) -> CatalogRegistry {
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

    // === catalog-staleness

    /// Return live table names from an Iceberg catalog handle, without consulting its DataFusion
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

    /// Rebuild the DataFusion provider for a live Iceberg handle after an out-of-band mutation.
    /// # Errors
    /// Unknown catalog or provider rebuild failure → [`Error::DataFusion`].
    pub async fn refresh_catalog_provider(&self, catalog: &str) -> Result<()> {
        let handle = self.catalog_handle(catalog)?;
        // v1 call preserved: `reregister_catalog_provider` (the catalog crate's full-provider
        repark_iceberg::catalog::reregister_catalog_provider(self.context(), handle, catalog)
            .await
            .map_err(engine_err)
    }

    /// Test-support only: create a table via the Catalog API **without** re-registering the DF
    /// # Errors
    /// Unknown catalog, create failure, or invalid warehouse path → classified [`Error`].
    #[doc(hidden)]
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

    /// Test-support only: drop a table via the Catalog API **without** re-registering the DF
    /// # Errors
    /// Unknown catalog or drop failure → classified [`Error`].
    #[doc(hidden)]
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

    /// Return session temp-view names from the pinned home without loading table metadata.
    /// # Errors
    /// [`Error::Analysis`] when the build-time home provider was replaced; otherwise infallible.
    pub fn list_temp_view_names(&self) -> Result<Vec<String>> {
        // List the build-time home and refuse if its provider identity changed.
        crate::temp_view::assert_home_intact(self.context(), &self.temp_view_home)?;
        let Some(schema) = self.temp_view_home.provider.as_ref() else {
            return Ok(Vec::new());
        };
        Ok(schema.table_names())
    }

    /// Return DataFusion provider names for a catalog schema without loading tables.
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

    /// Whether a table exists (PySpark `spark.catalog.tableExists`).
    /// # Errors
    /// Returns [`Error::DataFusion`] for a two-part name or an unregistered catalog; a catalog
    pub async fn table_exists(&self, name: &str) -> Result<bool> {
        // Quote-aware split (C2-L-006): matches the Python `_sql_table_ref` segment rules so
        let parts = parse_table_identifier_segments(name).map_err(|message| {
            Error::DataFusion(format!("tableExists: invalid table identifier: {message}"))
        })?;
        match parts.as_slice() {
            // The one-part arm uses the pinned home and the already-parsed segment overload.
            [view] => {
                let quoted = name.trim().starts_with(['"', '`']);
                self.context()
                    .table_exist(self.temp_view_ref_from_segment(view, quoted)?)
                    .map_err(engine_err)
            }
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

    /// Register the AWS-free in-memory Iceberg catalog (local-filesystem `warehouse`) under `name`
    /// # Errors
    /// Returns [`Error::DataFusion`] if `name` is already registered or the catalog cannot be
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
        // The in-memory / LocalFs catalog keeps the offline fallback for CTAS into a namespace
        self.register_iceberg_catalog_with_policy(
            name,
            catalog,
            LocationPolicy::TempFallbackAllowed {
                root: memory_warehouse_fallback_root(warehouse),
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

    /// Mark a local path as a trusted write root for the SEC-02 local-filesystem DDL gate.
    pub fn note_local_write_root(&self, path: &str) {
        self.catalogs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .note_local_warehouse_root(path.to_string());
    }

    /// Read a Parquet file or directory into a [`DataFrame`].
    /// # Errors
    /// Returns [`Error::DataFusion`] if the S3 store cannot be built (region/credentials), or if
    pub async fn read_parquet(&self, path: &str) -> Result<DataFrame> {
        if let Some((_scheme, bucket)) = object_store_s3::parse_s3_bucket(path) {
            self.ensure_s3_bucket_registered(&bucket)?;
        }
        self.context()
            .read_parquet(path, ParquetReadOptions::default())
            .await
            .map_err(engine_err)
    }

    /// Read a CSV file or directory using a case-insensitive Spark option map.
    /// # Errors
    /// Returns [`Error::Analysis`] for malformed option values; [`Error::DataFusion`] on I/O/plan.
    pub async fn read_csv(
        &self,
        path: &str,
        options: &HashMap<String, String>,
    ) -> Result<DataFrame> {
        if let Some((_scheme, bucket)) = object_store_s3::parse_s3_bucket(path) {
            self.ensure_s3_bucket_registered(&bucket)?;
        }
        let mut csv_options = csv_read_options_from_map(options)?;
        // nullValue: force all-Utf8 schema so the scan path never type-parses null tokens (DF
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

    /// Read a JSON file or directory using Spark multiline semantics.
    /// # Errors
    /// Returns [`Error::Analysis`] for malformed option values; [`Error::DataFusion`] on I/O/plan.
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

    /// Read an Iceberg catalog table, optionally pinned to a snapshot / ref / timestamp.
    /// # Errors
    /// Mutual-exclusion / parse / unknown-snapshot / catalog errors as classified [`Error`].
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

    /// Test-support only: create a branch or tag ref on an Iceberg table (`ManageSnapshots`).
    /// # Errors
    /// Unknown catalog/table, unknown snapshot, or ref already exists → classified [`Error`].
    #[doc(hidden)]
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

    /// Test-support only: list snapshot `(snapshot_id, timestamp_ms)` pairs in history order.
    /// # Errors
    /// Unknown catalog/table → classified [`Error`].
    #[doc(hidden)]
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

    /// Ensure an authenticated S3 object store for `bucket` is registered on the `RuntimeEnv`
    /// # Errors
    /// Returns [`Error::DataFusion`] if the session never resolved its SDK config (the E-2 gate,
    fn ensure_s3_bucket_registered(&self, bucket: &str) -> Result<()> {
        if self
            .registered_s3_buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(bucket)
        {
            return Ok(());
        }
        // E-2 gate: the store build consumes the FINALIZE-resolved SDK config — an S3-path read on
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
mod tests;
