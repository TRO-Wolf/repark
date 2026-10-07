//! Synchronous Python wrapper over [`repark_core::ReparkSession`].

use std::collections::HashMap;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

use pyo3::prelude::*;
use repark_core::{EngineRuntime, ReparkSession, ReparkSessionBuilder};
use tokio::runtime::Runtime;

use crate::UnsupportedOperationException;
use crate::arrow_export::drain_arrow_c_stream;
use crate::dataframe::{PyDataFrame, with_stream_poll_no_detach};
use crate::deep_stack::{
    block_on_grown_if, block_on_grown_sized, build_shared_runtime, frame_drive_segment_cached,
};
use crate::fence::{fenced, fenced_span};
use crate::session_runtime::apply_session_knobs;
use crate::to_py_err;

/// Build the engine session, register catalogs, wrap in the Python handle.
fn finish_session(py: Python<'_>, builder: ReparkSessionBuilder) -> PyResult<PyReparkSession> {
    let session = builder.build().map_err(to_py_err)?;
    repark_functions::install_shared_analyzer_rules(session.context());
    let runtime = shared_runtime()?;
    py.detach(|| runtime.block_on(session.register_configured_catalogs()))
        .map_err(to_py_err)?;
    session.register_configured_sources().map_err(to_py_err)?;
    Ok(PyReparkSession {
        session,
        runtime,
        deep_view_levels: AtomicUsize::new(0),
    })
}

/// Process-wide Tokio runtime shared by sessions and their `DataFrames`.
static SHARED_RUNTIME: OnceLock<EngineRuntime> = OnceLock::new();

/// Return the process-wide multi-thread Tokio runtime, initializing it on first use.
/// # Errors
/// Returns `RuntimeError` if the Tokio runtime fails to build on the first call.
pub(crate) fn shared_runtime() -> PyResult<Arc<Runtime>> {
    if let Some(runtime) = SHARED_RUNTIME.get() {
        return Ok(Arc::clone(runtime.runtime()));
    }
    let runtime = build_shared_runtime().map_err(|err| {
        pyo3::exceptions::PyRuntimeError::new_err(crate::exceptions::mask_user_visible(format!(
            "failed to start the engine runtime: {err}"
        )))
    })?;
    let arc = Arc::new(runtime);
    // A losing initializer must use the installed runtime, not its rejected value.
    match SHARED_RUNTIME.set(EngineRuntime::new(Arc::clone(&arc))) {
        Ok(()) => Ok(arc),
        Err(_rejected) => SHARED_RUNTIME
            .get()
            .map(|installed| Arc::clone(installed.runtime()))
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(crate::exceptions::mask_user_visible(
                    "shared engine runtime race: set rejected but get returned empty",
                ))
            }),
    }
}

/// The Python-facing session handle and shared runtime.
#[pyclass(name = "PyReparkSession", module = "repark._native")]
pub struct PyReparkSession {
    pub(crate) session: ReparkSession,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) deep_view_levels: AtomicUsize,
}

impl PyReparkSession {
    pub(crate) fn note_view_depths(&self, depths: &crate::deep_stack::PlanDepths) {
        self.deep_view_levels
            .fetch_max(depths.plan.max(depths.expression), Ordering::Relaxed);
    }

    pub(crate) fn deep_view_levels(&self) -> usize {
        self.deep_view_levels.load(Ordering::Relaxed)
    }

    async fn plan_session_sql(
        session: &ReparkSession,
        query: &str,
    ) -> PyResult<datafusion::prelude::DataFrame> {
        Self::plan_session_sql_inner(session, query, false).await
    }

    async fn plan_session_sql_inner(
        session: &ReparkSession,
        query: &str,
        built: bool,
    ) -> PyResult<datafusion::prelude::DataFrame> {
        let prepared = crate::session_runtime::prepare_session_sql(query)?;
        if built {
            session.sql_built(&prepared).await.map_err(to_py_err)
        } else {
            session.sql(&prepared).await.map_err(to_py_err)
        }
    }
}

#[pymethods]
impl PyReparkSession {
    /// Build a session, applying the builder knobs the facade's `ReparkSession.Builder` collected.
    /// # Errors
    /// Returns `RuntimeError` if the DataFusion session or the Tokio runtime fails to build.
    #[new]
    #[pyo3(signature = (memory_limit_gb=None, batch_size=None, target_partitions=None, config=None, config_path=None))]
    pub fn new(
        py: Python<'_>,
        memory_limit_gb: Option<usize>,
        batch_size: Option<usize>,
        target_partitions: Option<usize>,
        config: Option<HashMap<String, String>>,
        config_path: Option<String>,
    ) -> PyResult<Self> {
        fenced_span!("py.session", "PyReparkSession.__new__", {
            let builder =
                apply_session_knobs(memory_limit_gb, batch_size, target_partitions, config)?;
            let builder = builder.from_config_file(config_path.map(std::path::PathBuf::from));
            let builder = builder
                .with_sql_dialect(Arc::new(repark_spark::SparkDialect))
                .with_extension(Arc::new(repark_spark::SparkExtension));
            finish_session(py, builder)
        })
    }

    /// Read a `repark.toml` file's translated pairs without building a session.
    /// # Errors
    /// Returns `RuntimeError` if discovery, the profile merge, interpolation, or translation fails.
    #[staticmethod]
    #[pyo3(signature = (config_path=None))]
    pub fn config_file_pairs(config_path: Option<String>) -> PyResult<HashMap<String, String>> {
        fenced_span!("py.session", "PyReparkSession.config_file_pairs", {
            repark_core::config_file_pairs(config_path.map(std::path::PathBuf::from))
                .map_err(to_py_err)
        })
    }

    /// Build a **native** (non-Spark) session for the ANSI-door callable `repark.sql()`.
    /// # Errors
    /// Same knob refusals as [`Self::new`].
    #[staticmethod]
    #[pyo3(signature = (memory_limit_gb=None, batch_size=None, target_partitions=None, config=None))]
    pub fn native(
        py: Python<'_>,
        memory_limit_gb: Option<usize>,
        batch_size: Option<usize>,
        target_partitions: Option<usize>,
        config: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        fenced_span!("py.session", "PyReparkSession.native", {
            let builder =
                apply_session_knobs(memory_limit_gb, batch_size, target_partitions, config)?;
            let handle = finish_session(py, builder)?;
            crate::session_runtime::register_native_door_functions(handle.session.context());
            Ok(handle)
        })
    }

    /// Run a Spark-SQL string, returning a [`PyDataFrame`] (PySpark `spark.sql`).
    /// # Errors
    /// Returns `RuntimeError` on parse, planning, iceberg, or execution failure.
    pub fn sql(&self, py: Python<'_>, query: &str) -> PyResult<PyDataFrame> {
        fenced_span!("py.sql", "PyReparkSession.sql", {
            let grown = crate::deep_stack::sql_drive_grown(query)
                || self.deep_view_levels() > crate::deep_stack::DEEP_NESTING_DEPTH;
            let df = py.detach(|| {
                let planned = Self::plan_session_sql(&self.session, query);
                block_on_grown_if(&self.runtime, planned, grown)
            })?;
            let mut depths = crate::deep_stack::plan_depths(df.logical_plan());
            depths.plan = depths.plan.max(self.deep_view_levels());
            depths.expression = depths.expression.max(self.deep_view_levels());
            Ok(PyDataFrame::new_with_depths(
                df,
                Arc::clone(&self.runtime),
                depths,
            ))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn sql_built(&self, py: Python<'_>, query: &str) -> PyResult<PyDataFrame> {
        fenced_span!("py.sql", "PyReparkSession.sql_built", {
            let grown = crate::deep_stack::sql_drive_grown(query)
                || self.deep_view_levels() > crate::deep_stack::DEEP_NESTING_DEPTH;
            let df = py.detach(|| {
                let planned = Self::plan_session_sql_inner(&self.session, query, true);
                block_on_grown_if(&self.runtime, planned, grown)
            })?;
            let mut depths = crate::deep_stack::plan_depths(df.logical_plan());
            depths.plan = depths.plan.max(self.deep_view_levels());
            depths.expression = depths.expression.max(self.deep_view_levels());
            Ok(PyDataFrame::new_with_depths(
                df,
                Arc::clone(&self.runtime),
                depths,
            ))
        })
    }

    /// Read a Parquet file or directory into a [`PyDataFrame`] (PySpark `spark.read.parquet`).
    /// # Errors
    /// Returns `RuntimeError` if the path cannot be read or planned.
    pub fn read_parquet(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        fenced_span!("py.read", "PyReparkSession.read_parquet", {
            let df = py
                .detach(|| self.runtime.block_on(self.session.read_parquet(path)))
                .map_err(to_py_err)?;
            Ok(PyDataFrame::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Read a CSV file or directory (PySpark `spark.read.csv` / `format("csv").load`).
    /// # Errors
    /// Maps engine analysis / I/O errors through the exception taxonomy.
    #[pyo3(signature = (path, options=None))]
    pub fn read_csv(
        &self,
        py: Python<'_>,
        path: &str,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<PyDataFrame> {
        fenced_span!("py.read", "PyReparkSession.read_csv", {
            let opts = options.unwrap_or_default();
            let df = py
                .detach(|| self.runtime.block_on(self.session.read_csv(path, &opts)))
                .map_err(to_py_err)?;
            Ok(PyDataFrame::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Read a JSON file or directory (PySpark `spark.read.json` / `format("json").load`).
    /// # Errors
    /// Maps engine analysis / I/O errors through the exception taxonomy.
    #[pyo3(signature = (path, options=None))]
    pub fn read_json(
        &self,
        py: Python<'_>,
        path: &str,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<PyDataFrame> {
        fenced_span!("py.read", "PyReparkSession.read_json", {
            let opts = options.unwrap_or_default();
            let df = py
                .detach(|| self.runtime.block_on(self.session.read_json(path, &opts)))
                .map_err(to_py_err)?;
            Ok(PyDataFrame::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Read one Excel sheet.
    /// # Errors
    /// Always `UnsupportedOperationException` — the reader is not in this build.
    #[pyo3(signature = (path, options=None))]
    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    pub fn read_excel(
        &self,
        py: Python<'_>,
        path: &str,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<PyDataFrame> {
        fenced_span!("py.read", "PyReparkSession.read_excel", {
            let _ = (py, path, options);
            Err(deferred_reader_error("spark.read.excel (read_excel)"))
        })
    }

    /// List Excel workbook sheet names in workbook order (`spark.read.sheet_names` helper).
    /// # Errors
    /// Always `UnsupportedOperationException` — the reader is not in this build.
    #[allow(clippy::unused_self)]
    pub fn excel_sheet_names(&self, py: Python<'_>, path: &str) -> PyResult<Vec<String>> {
        fenced_span!("py.read", "PyReparkSession.excel_sheet_names", {
            let _ = (py, path);
            Err(deferred_reader_error(
                "spark.read.sheet_names (excel_sheet_names)",
            ))
        })
    }

    #[pyo3(signature = (
        url,
        dbtable=None,
        query=None,
        properties=None,
        partition_column=None,
        lower_bound=None,
        upper_bound=None,
        num_partitions=None,
        predicates=None,
    ))]
    #[allow(
        clippy::too_many_arguments,
        clippy::needless_pass_by_value,
        clippy::missing_errors_doc
    )]
    pub fn read_postgres(
        &self,
        py: Python<'_>,
        url: &str,
        dbtable: Option<&str>,
        query: Option<&str>,
        properties: Option<HashMap<String, String>>,
        partition_column: Option<&str>,
        lower_bound: Option<i64>,
        upper_bound: Option<i64>,
        num_partitions: Option<usize>,
        predicates: Option<Vec<String>>,
    ) -> PyResult<PyDataFrame> {
        fenced_span!("py.read", "PyReparkSession.read_postgres", {
            let target = match (dbtable, query) {
                (Some(dbtable), None) => {
                    repark_core::PostgresTarget::Relation(String::from(dbtable))
                }
                (None, Some(query)) => repark_core::PostgresTarget::Query(String::from(query)),
                _ => {
                    return Err(to_py_err(repark_core::Error::Config(String::from(
                        "read_postgres takes exactly one of `dbtable` and `query`",
                    ))));
                }
            };
            let partitioning = [
                ("partitionColumn", partition_column.is_some()),
                ("lowerBound", lower_bound.is_some()),
                ("upperBound", upper_bound.is_some()),
                ("numPartitions", num_partitions.is_some()),
                ("predicates", predicates.is_some()),
            ]
            .into_iter()
            .filter_map(|(key, given)| given.then_some(key))
            .collect();
            let read = repark_core::PostgresRead {
                url: String::from(url),
                target,
                properties: properties.unwrap_or_default().into_iter().collect(),
                partitioning,
            };
            let frame = py
                .detach(|| self.runtime.block_on(self.session.read_postgres(read)))
                .map_err(to_py_err)?;
            Ok(PyDataFrame::new(frame, Arc::clone(&self.runtime)))
        })
    }

    /// Register `frame` as a replaceable lazy temp view named `name`.
    /// # Errors
    /// Returns `RuntimeError` if registration fails.
    pub fn create_or_replace_temp_view(&self, name: &str, frame: &PyDataFrame) -> PyResult<()> {
        fenced!("PyReparkSession.create_or_replace_temp_view", {
            let depths = frame.depths();
            let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
            crate::deep_stack::grown_sync(need, || {
                self.session
                    .create_or_replace_temp_view_from(name, frame.inner())
            })
            .map_err(to_py_err)?;
            self.note_view_depths(&depths);
            Ok(())
        })
    }

    /// Declare in-memory temp view `name` pre-sorted by `keys` so DataFusion elides window sorts.
    /// # Errors
    /// `AnalysisException` for unknown view/key, non-in-memory frames, unsorted data, or a NULL
    #[pyo3(signature = (name, keys, tighten_nulls=false))]
    pub fn declare_temp_view_sorted(
        &self,
        py: Python<'_>,
        name: &str,
        keys: Vec<String>,
        tighten_nulls: bool,
    ) -> PyResult<()> {
        fenced!("PyReparkSession.declare_temp_view_sorted", {
            py.detach(|| {
                let sorted = self
                    .session
                    .declare_temp_view_sorted(name, &keys, tighten_nulls);
                self.runtime.block_on(sorted)
            })
            .map_err(to_py_err)
        })
    }

    /// Collect `frame` once and register it as an in-memory temp view for reuse.
    /// # Errors
    /// Returns `RuntimeError` if collect or registration fails.
    pub fn materialize_as_temp_view(
        &self,
        py: Python<'_>,
        name: &str,
        frame: &PyDataFrame,
    ) -> PyResult<()> {
        fenced_span!("py.action", "PyReparkSession.materialize_as_temp_view", {
            let segment = frame_drive_segment_cached(&frame.depths())?;
            let frame = frame.executable()?;
            py.detach(|| {
                let materialized = self.session.materialize_dataframe_as_temp_view(name, frame);
                block_on_grown_sized(&self.runtime, materialized, segment)
            })
            .map_err(to_py_err)
        })
    }

    /// # Errors
    /// Returns a PySpark-shaped error if collect/registration fails or `max_bytes` is exceeded.
    #[pyo3(signature = (name, frame, budgets=(None, None)))]
    pub fn materialize_as_cache_view(
        &self,
        py: Python<'_>,
        name: &str,
        frame: &PyDataFrame,
        budgets: (Option<u64>, Option<u64>),
    ) -> PyResult<()> {
        fenced_span!("py.action", "PyReparkSession.materialize_as_cache_view", {
            let segment = frame_drive_segment_cached(&frame.depths())?;
            let frame = frame.executable()?;
            py.detach(|| {
                let materialized = self
                    .session
                    .materialize_dataframe_as_cache_view(name, frame, budgets);
                block_on_grown_sized(&self.runtime, materialized, segment)
            })
            .map_err(to_py_err)
        })
    }

    /// Register an Arrow IPC stream as a `MemTable` temp view.
    /// # Errors
    /// Returns `RuntimeError` if the IPC payload cannot be decoded or registration fails.
    pub fn register_ipc_stream_as_temp_view(
        &self,
        py: Python<'_>,
        name: &str,
        ipc_bytes: &[u8],
    ) -> PyResult<()> {
        fenced_span!(
            "py.action",
            "PyReparkSession.register_ipc_stream_as_temp_view",
            {
                use arrow::ipc::reader::StreamReader;
                use std::io::Cursor;

                let bytes = ipc_bytes.to_vec();
                py.detach(|| {
                    let cursor = Cursor::new(bytes);
                    let reader = StreamReader::try_new(cursor, None).map_err(|error| {
                        repark_core::Error::DataFusion(format!(
                            "createDataFrame Arrow IPC decode failed: {error}"
                        ))
                    })?;
                    let schema = reader.schema();
                    let mut batches = Vec::new();
                    for batch in reader {
                        let batch = batch.map_err(|error| {
                            repark_core::Error::DataFusion(format!(
                                "createDataFrame Arrow IPC batch failed: {error}"
                            ))
                        })?;
                        if batch.num_rows() > 0 {
                            batches.push(batch);
                        }
                    }
                    self.session
                        .register_record_batches_as_temp_view(name, schema, batches)
                })
                .map_err(to_py_err)
            }
        )
    }

    /// Register any Arrow C Stream exporter as a `MemTable` temp view.
    /// # Errors
    /// Missing exporters and non-capsule results return `TypeError`; exporter errors are preserved.
    pub fn register_arrow_stream_as_temp_view(
        &self,
        _py: Python<'_>,
        name: &str,
        obj: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        fenced_span!(
            "py.action",
            "PyReparkSession.register_arrow_stream_as_temp_view",
            {
                // Do not attach+detach on re-entrant `__arrow_c_stream__` or the process aborts.
                let (schema, batches) = with_stream_poll_no_detach(|| drain_arrow_c_stream(obj))?;
                self.session
                    .register_record_batches_as_temp_view(name, schema, batches)
                    .map_err(to_py_err)
            }
        )
    }

    /// Drop a temp view (PySpark `spark.catalog.dropTempView`); returns whether it existed.
    /// # Errors
    /// Returns `RuntimeError` if the name cannot be resolved as a table reference.
    pub fn drop_temp_view(&self, name: &str) -> PyResult<bool> {
        fenced!("PyReparkSession.drop_temp_view", {
            let held = self.deep_view_levels();
            let need = crate::deep_stack::clone_need_bytes(held, held);
            crate::deep_stack::grown_sync(need, || self.session.drop_temp_view(name))
                .map_err(to_py_err)
        })
    }

    /// Whether a table exists: three-part names ask Iceberg; one-part names check temp views.
    /// # Errors
    /// Returns `RuntimeError` for a two-part name, an unregistered catalog, or a probe failure.
    pub fn table_exists(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        fenced_span!("py.catalog", "PyReparkSession.table_exists", {
            py.detach(|| self.runtime.block_on(self.session.table_exists(name)))
                .map_err(to_py_err)
        })
    }

    /// This session's temp-view home as `[catalog, schema]`.
    /// # Errors
    /// Returns `RuntimeError` when a catalog took over the temp-view home, or engine lookup fails.
    pub fn temp_view_home(&self) -> PyResult<Vec<String>> {
        fenced!("PyReparkSession.temp_view_home", {
            self.session.temp_view_home().map_err(to_py_err)
        })
    }

    /// Home-qualified `[catalog, schema, table]` for a one-part temp-view name, or `None`.
    /// # Errors
    /// Returns `RuntimeError` when a catalog took over the temp-view home, or engine lookup fails.
    pub fn resolve_temp_view_home_ref(&self, name: &str) -> PyResult<Option<Vec<String>>> {
        fenced_span!(
            "py.catalog",
            "PyReparkSession.resolve_temp_view_home_ref",
            {
                self.session
                    .resolve_temp_view_home_ref(name)
                    .map_err(to_py_err)
            }
        )
    }

    /// Register the AWS-free in-memory Iceberg catalog under `name` — local development and tests.
    /// # Errors
    /// Returns `RuntimeError` if the catalog cannot be built or registered.
    pub fn register_memory_catalog(
        &self,
        py: Python<'_>,
        name: &str,
        warehouse: &str,
    ) -> PyResult<()> {
        fenced_span!("py.catalog", "PyReparkSession.register_memory_catalog", {
            py.detach(|| {
                let registered = self.session.register_memory_catalog(name, warehouse);
                self.runtime.block_on(registered)
            })
            .map_err(to_py_err)
        })
    }

    /// Mark a local destination as trusted for typed-writer generated SQL.
    pub fn note_local_write_root(&self, path: &str) {
        self.session.note_local_write_root(path);
    }

    /// Read an Iceberg catalog table with optional time-travel pins.
    /// # Errors
    /// Classified engine errors (analysis for unknown snapshot/ref/mutex; execution otherwise).
    #[pyo3(signature = (
        table_name, snapshot_id=None, as_of_timestamp_ms=None, branch=None, tag=None,
        version_as_of=None, timestamp_as_of=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn read_iceberg_table(
        &self,
        py: Python<'_>,
        table_name: &str,
        snapshot_id: Option<i64>,
        as_of_timestamp_ms: Option<i64>,
        branch: Option<String>,
        tag: Option<String>,
        version_as_of: Option<String>,
        timestamp_as_of: Option<String>,
    ) -> PyResult<PyDataFrame> {
        crate::session_sources::read_iceberg_table_pinned(
            py,
            self,
            table_name,
            snapshot_id,
            as_of_timestamp_ms,
            branch,
            tag,
            version_as_of,
            timestamp_as_of,
        )
    }

    /// Live Iceberg table names in `namespace` (list-on-access; no DF provider snapshot).
    /// # Errors
    /// Unknown catalog or list failure → classified engine error.
    pub fn list_iceberg_table_names(
        &self,
        py: Python<'_>,
        catalog: &str,
        namespace: &str,
    ) -> PyResult<Vec<String>> {
        fenced_span!("py.catalog", "PyReparkSession.list_iceberg_table_names", {
            py.detach(|| {
                let listed = self.session.list_iceberg_table_names(catalog, namespace);
                self.runtime.block_on(listed)
            })
            .map_err(to_py_err)
        })
    }

    /// Session temp-view names from the default catalog/schema (no `information_schema` scan).
    /// # Errors
    /// Classified engine error (currently infallible empty-or-list).
    pub fn list_temp_view_names(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        fenced_span!("py.catalog", "PyReparkSession.list_temp_view_names", {
            py.detach(|| self.session.list_temp_view_names())
                .map_err(to_py_err)
        })
    }

    /// DF provider name directory for `catalog.schema` (no table load; snapshot for Iceberg).
    /// # Errors
    /// Classified engine error (currently infallible empty-or-list).
    pub fn list_df_schema_table_names(
        &self,
        py: Python<'_>,
        catalog: &str,
        schema: &str,
    ) -> PyResult<Vec<String>> {
        fenced_span!(
            "py.catalog",
            "PyReparkSession.list_df_schema_table_names",
            {
                py.detach(|| self.session.list_df_schema_table_names(catalog, schema))
                    .map_err(to_py_err)
            }
        )
    }

    /// Rebuild the DataFusion catalog provider from the live Iceberg handle.
    /// # Errors
    /// Unknown catalog or rebuild failure → classified engine error.
    pub fn refresh_catalog_provider(&self, py: Python<'_>, catalog: &str) -> PyResult<()> {
        fenced_span!("py.catalog", "PyReparkSession.refresh_catalog_provider", {
            py.detach(|| {
                let refreshed = self.session.refresh_catalog_provider(catalog);
                self.runtime.block_on(refreshed)
            })
            .map_err(to_py_err)
        })
    }

    /// Test-support only: Catalog-API create without DF provider re-register (OOB create).
    /// # Errors
    /// Unknown catalog or create failure → classified engine error.
    pub fn testing_oob_create_table(
        &self,
        py: Python<'_>,
        catalog_name: &str,
        namespace: &str,
        table: &str,
        warehouse_location: &str,
    ) -> PyResult<()> {
        fenced!("PyReparkSession.testing_oob_create_table", {
            py.detach(|| {
                let created = self.session.testing_oob_create_table(
                    catalog_name,
                    namespace,
                    table,
                    warehouse_location,
                );
                self.runtime.block_on(created)
            })
            .map_err(to_py_err)
        })
    }

    /// Test-support only: Catalog-API drop without DF provider re-register (OOB drop).
    /// # Errors
    /// Unknown catalog or drop failure → classified engine error.
    pub fn testing_oob_drop_table(
        &self,
        py: Python<'_>,
        catalog_name: &str,
        namespace: &str,
        table: &str,
    ) -> PyResult<()> {
        fenced!("PyReparkSession.testing_oob_drop_table", {
            py.detach(|| {
                let dropped = self
                    .session
                    .testing_oob_drop_table(catalog_name, namespace, table);
                self.runtime.block_on(dropped)
            })
            .map_err(to_py_err)
        })
    }

    /// Test-support only: create a branch or tag ref (`ManageSnapshots`).
    /// # Errors
    /// Unknown table/snapshot or ref-already-exists → classified engine error.
    pub fn testing_create_ref(
        &self,
        py: Python<'_>,
        table_name: &str,
        kind: &str,
        ref_name: &str,
        snapshot_id: i64,
    ) -> PyResult<()> {
        fenced!("PyReparkSession.testing_create_ref", {
            py.detach(|| {
                let made = self
                    .session
                    .testing_create_ref(table_name, kind, ref_name, snapshot_id);
                self.runtime.block_on(made)
            })
            .map_err(to_py_err)
        })
    }

    /// Test-support only: list `(snapshot_id, timestamp_ms)` in history order.
    /// # Errors
    /// Unknown table → classified engine error.
    pub fn testing_list_snapshots(
        &self,
        py: Python<'_>,
        table_name: &str,
    ) -> PyResult<Vec<(i64, i64)>> {
        fenced!("PyReparkSession.testing_list_snapshots", {
            py.detach(|| {
                let listed = self.session.testing_list_snapshots(table_name);
                self.runtime.block_on(listed)
            })
            .map_err(to_py_err)
        })
    }

    /// Register catalogs from a LATE builder config onto this LIVE session.
    /// # Errors
    /// Raises when the `spark.sql.catalog.*` block is malformed or a new catalog fails to register.
    pub fn register_late_catalogs(
        &self,
        py: Python<'_>,
        config: HashMap<String, String>,
    ) -> PyResult<(Vec<String>, Vec<String>)> {
        fenced_span!("py.catalog", "PyReparkSession.register_late_catalogs", {
            py.detach(|| {
                let registered = self.session.register_late_configured_catalogs(&config);
                self.runtime.block_on(registered)
            })
            .map_err(to_py_err)
        })
    }

    /// Create a namespace in a registered catalog, optionally with a `location` property.
    /// # Errors
    /// Returns `RuntimeError` if the catalog is unknown or creation fails.
    #[pyo3(signature = (catalog, namespace, location=None))]
    pub fn create_namespace(
        &self,
        py: Python<'_>,
        catalog: &str,
        namespace: &str,
        location: Option<&str>,
    ) -> PyResult<()> {
        fenced_span!("py.catalog", "PyReparkSession.create_namespace", {
            let mut properties = HashMap::new();
            if let Some(location) = location {
                // Mirror the warehouse path onto the catalog's `location_uri` key.
                properties.insert("location".to_string(), location.to_string());
            }
            py.detach(|| {
                let created = self
                    .session
                    .create_namespace(catalog, namespace, properties);
                self.runtime.block_on(created)
            })
            .map_err(to_py_err)
        })
    }

    /// Test-only panic injection through a fenced Python method.
    #[cfg(test)]
    #[allow(clippy::unused_self)] // an instance method by design — it drives the pyclass boundary
    fn panic_probe(&self) -> PyResult<()> {
        fenced!("PyReparkSession.panic_probe", {
            panic!("SAF-007 injected panic (deterministic probe)")
        })
    }
}

impl PyReparkSession {
    /// Shared-runtime pointer equality helper for integration tests.
    #[cfg(test)]
    pub(crate) fn runtime_arc(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }
}

/// Build the named unsupported-operation error for deferred readers.
fn deferred_reader_error(surface: &str) -> PyErr {
    UnsupportedOperationException::new_err(crate::exceptions::mask_user_visible(format!(
        "{surface} is not available in this build: the repark-excel read connector is \
         scheduled post-milestone-one. See the \"Post-milestone-one (BACKLOG)\" \
         row in task/todo.md."
    )))
}
