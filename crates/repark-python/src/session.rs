//! Synchronous Python wrapper over [`repark_core::ReparkSession`].

use std::collections::HashMap;
use std::ffi::CStr;
use std::sync::{Arc, OnceLock};

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use repark_core::{EngineRuntime, ReparkSession, ReparkSessionBuilder};
use tokio::runtime::Runtime;

use crate::dataframe::{PyDataFrame, with_stream_poll_no_detach};
use crate::fence::{fenced, fenced_span};
use crate::{UnsupportedOperationException, to_py_err};

/// Arrow C Stream `PyCapsule` name — same constant as `dataframe.rs` export path.
const ARROW_STREAM_CAPSULE_NAME: &CStr = c"arrow_array_stream";

/// Apply the shared builder knobs used by both the Spark-door constructor and the native door.
fn apply_session_knobs(
    memory_limit_gb: Option<usize>,
    batch_size: Option<usize>,
    target_partitions: Option<usize>,
    config: Option<HashMap<String, String>>,
) -> PyResult<ReparkSessionBuilder> {
    let mut builder = ReparkSession::builder();
    // Zero explicitly opts out of the bounded pool; other values select the requested limit.
    match memory_limit_gb {
        None => {}
        Some(0) => builder = builder.memory_limit_bytes(0),
        Some(gb) => builder = builder.memory_limit_gb(gb),
    }
    // Zero is invalid for batch and partition counts; do not silently apply defaults.
    if let Some(0) = batch_size {
        return Err(to_py_err(repark_core::Error::Config(
            "batch_size must be >= 1 (got 0)".to_string(),
        )));
    }
    if let Some(0) = target_partitions {
        return Err(to_py_err(repark_core::Error::Config(
            "target_partitions must be >= 1 (got 0)".to_string(),
        )));
    }
    if let Some(rows) = batch_size {
        builder = builder.batch_size(rows);
    }
    if let Some(parts) = target_partitions {
        builder = builder.target_partitions(parts);
    }
    if let Some(config) = config {
        builder = builder.configs(config);
    }
    Ok(builder)
}

/// Build the engine session, register catalogs, wrap in the Python handle.
fn finish_session(py: Python<'_>, builder: ReparkSessionBuilder) -> PyResult<PyReparkSession> {
    let session = builder.build().map_err(to_py_err)?;
    repark_functions::install_shared_analyzer_rules(session.context());
    let runtime = shared_runtime()?;
    py.detach(|| runtime.block_on(session.register_configured_catalogs()))
        .map_err(to_py_err)?;
    Ok(PyReparkSession { session, runtime })
}

/// Process-wide Tokio runtime shared by sessions and their `DataFrames`.
static SHARED_RUNTIME: OnceLock<EngineRuntime> = OnceLock::new();

/// Return the process-wide multi-thread Tokio runtime, initializing it on first use.
/// # Errors
/// Returns `RuntimeError` if the Tokio runtime fails to build on the first call.
fn shared_runtime() -> PyResult<Arc<Runtime>> {
    if let Some(runtime) = SHARED_RUNTIME.get() {
        return Ok(Arc::clone(runtime.runtime()));
    }
    let runtime = Runtime::new().map_err(|err| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "failed to start the engine runtime: {err}"
        ))
    })?;
    let arc = Arc::new(runtime);
    // A losing initializer must use the installed runtime, not its rejected value.
    match SHARED_RUNTIME.set(EngineRuntime::new(Arc::clone(&arc))) {
        Ok(()) => Ok(arc),
        Err(_rejected) => SHARED_RUNTIME
            .get()
            .map(|installed| Arc::clone(installed.runtime()))
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "shared engine runtime race: set rejected but get returned empty",
                )
            }),
    }
}

/// The Python-facing session handle and shared runtime.
#[pyclass(name = "PyReparkSession", module = "repark._native")]
pub struct PyReparkSession {
    pub(crate) session: ReparkSession,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl PyReparkSession {
    /// Build a session, applying the builder knobs the facade's `ReparkSession.Builder` collected.
    /// # Errors
    /// Returns `RuntimeError` if the DataFusion session or the Tokio runtime fails to build.
    #[new]
    #[pyo3(signature = (memory_limit_gb=None, batch_size=None, target_partitions=None, config=None))]
    pub fn new(
        py: Python<'_>,
        memory_limit_gb: Option<usize>,
        batch_size: Option<usize>,
        target_partitions: Option<usize>,
        config: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        fenced_span!("py.session", "PyReparkSession.__new__", {
            let builder =
                apply_session_knobs(memory_limit_gb, batch_size, target_partitions, config)?;
            let builder = builder
                .with_sql_dialect(Arc::new(repark_spark::SparkDialect))
                .with_extension(Arc::new(repark_spark::SparkExtension));
            finish_session(py, builder)
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
            repark_functions::spark_log1p::register(handle.session.context());
            Ok(handle)
        })
    }

    /// Run a Spark-SQL string, returning a [`PyDataFrame`] (PySpark `spark.sql`).
    /// # Errors
    /// Returns `RuntimeError` on parse, planning, iceberg, or execution failure.
    pub fn sql(&self, py: Python<'_>, query: &str) -> PyResult<PyDataFrame> {
        fenced_span!("py.sql", "PyReparkSession.sql", {
            repark_spark::refuse_declared_function_in_sql(query)
                .map_err(crate::datafusion_to_py_err)?;
            let df = py
                .detach(|| self.runtime.block_on(self.session.sql(query)))
                .map_err(to_py_err)?;
            Ok(PyDataFrame::new(df, Arc::clone(&self.runtime)))
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

    /// Read PostgreSQL via the own-stack connector (PySpark `spark.read.jdbc` / format postgres).
    /// # Errors
    /// Always `UnsupportedOperationException` — the connector is not in this build.
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
        clippy::unused_self,
        clippy::needless_pass_by_value
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
        // Never log URL or properties because they may contain credentials.
        fenced_span!("py.read", "PyReparkSession.read_postgres", {
            // Bind and drop arguments without formatting credential-bearing values.
            let _ = (
                py,
                url,
                dbtable,
                query,
                properties,
                partition_column,
                lower_bound,
                upper_bound,
                num_partitions,
                predicates,
            );
            Err(deferred_reader_error("spark.read.jdbc (read_postgres)"))
        })
    }

    /// Register `frame` as a replaceable lazy temp view named `name`.
    /// # Errors
    /// Returns `RuntimeError` if registration fails.
    pub fn create_or_replace_temp_view(&self, name: &str, frame: &PyDataFrame) -> PyResult<()> {
        fenced!("PyReparkSession.create_or_replace_temp_view", {
            self.session
                .create_or_replace_temp_view_from(name, frame.inner())
                .map_err(to_py_err)
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
                self.runtime.block_on(self.session.declare_temp_view_sorted(
                    name,
                    &keys,
                    tighten_nulls,
                ))
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
            let frame = frame.inner().clone();
            py.detach(|| {
                self.runtime
                    .block_on(self.session.materialize_dataframe_as_temp_view(name, frame))
            })
            .map_err(to_py_err)
        })
    }

    /// Cache-path materialize with an optional `repark.cache.max_bytes` guard.
    /// # Errors
    /// Returns a PySpark-shaped error if collect/registration fails or `max_bytes` is exceeded.
    #[pyo3(signature = (name, frame, max_bytes=None))]
    pub fn materialize_as_cache_view(
        &self,
        py: Python<'_>,
        name: &str,
        frame: &PyDataFrame,
        max_bytes: Option<u64>,
    ) -> PyResult<()> {
        fenced_span!("py.action", "PyReparkSession.materialize_as_cache_view", {
            let frame = frame.inner().clone();
            py.detach(|| {
                self.runtime.block_on(
                    self.session
                        .materialize_dataframe_as_cache_view(name, frame, max_bytes),
                )
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
                use std::io::Cursor;

                use arrow::ipc::reader::StreamReader;

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
            self.session.drop_temp_view(name).map_err(to_py_err)
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
                self.runtime
                    .block_on(self.session.register_memory_catalog(name, warehouse))
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
        table_name,
        snapshot_id=None,
        as_of_timestamp_ms=None,
        branch=None,
        tag=None,
    ))]
    pub fn read_iceberg_table(
        &self,
        py: Python<'_>,
        table_name: &str,
        snapshot_id: Option<i64>,
        as_of_timestamp_ms: Option<i64>,
        branch: Option<String>,
        tag: Option<String>,
    ) -> PyResult<PyDataFrame> {
        fenced_span!("py.read", "PyReparkSession.read_iceberg_table", {
            let opts = repark_core::TimeTravelOpts {
                snapshot_id,
                as_of_timestamp_ms,
                branch,
                tag,
            };
            let df = py
                .detach(|| {
                    self.runtime
                        .block_on(self.session.read_iceberg_table(table_name, opts))
                })
                .map_err(to_py_err)?;
            Ok(PyDataFrame::new(df, Arc::clone(&self.runtime)))
        })
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
                self.runtime
                    .block_on(self.session.list_iceberg_table_names(catalog, namespace))
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
                self.runtime
                    .block_on(self.session.refresh_catalog_provider(catalog))
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
                self.runtime.block_on(self.session.testing_oob_create_table(
                    catalog_name,
                    namespace,
                    table,
                    warehouse_location,
                ))
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
                self.runtime.block_on(self.session.testing_oob_drop_table(
                    catalog_name,
                    namespace,
                    table,
                ))
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
                self.runtime.block_on(self.session.testing_create_ref(
                    table_name,
                    kind,
                    ref_name,
                    snapshot_id,
                ))
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
                self.runtime
                    .block_on(self.session.testing_list_snapshots(table_name))
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
                self.runtime
                    .block_on(self.session.register_late_configured_catalogs(&config))
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
                self.runtime.block_on(
                    self.session
                        .create_namespace(catalog, namespace, properties),
                )
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
    UnsupportedOperationException::new_err(format!(
        "{surface} is not available in this build: the repark-excel / repark-postgres read \
         connectors are scheduled post-milestone-one. See the \"Post-milestone-one (BACKLOG)\" \
         row in task/todo.md."
    ))
}

/// Resolve an Arrow C Stream capsule or exporter and drain non-empty batches.
/// # Errors
/// Returns `TypeError` for a missing exporter or non-capsule exporter result.
fn drain_arrow_c_stream(
    obj: &Bound<'_, PyAny>,
) -> PyResult<(arrow::datatypes::SchemaRef, Vec<RecordBatch>)> {
    // Keep the capsule alive for the import: `from_raw` moves the FFI stream and nulls release.
    let capsule_obj: Bound<'_, PyAny> = if obj.is_instance_of::<PyCapsule>() {
        obj.clone()
    } else {
        let exporter = obj.getattr("__arrow_c_stream__").map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(
                "register_arrow_stream_as_temp_view: object is not an Arrow C Stream exporter \
                 (missing __arrow_c_stream__) and is not an arrow_array_stream PyCapsule",
            )
        })?;
        // Call the optional-schema protocol without negotiation and preserve exporter errors.
        exporter.call0()?
    };
    let capsule = capsule_obj.cast::<PyCapsule>().map_err(|error| {
        pyo3::exceptions::PyTypeError::new_err(format!(
            "register_arrow_stream_as_temp_view: expected arrow_array_stream PyCapsule: {error}"
        ))
    })?;

    let pointer = capsule
        .pointer_checked(Some(ARROW_STREAM_CAPSULE_NAME))
        .map_err(|error| {
            repark_core::Error::DataFusion(format!(
                "register_arrow_stream_as_temp_view: invalid arrow_array_stream capsule: {error}"
            ))
        })
        .map_err(to_py_err)?
        .as_ptr()
        .cast::<FFI_ArrowArrayStream>();

    // SAFETY: `pointer_checked` verifies the capsule name and non-null pointer.
    let ffi_stream = unsafe { FFI_ArrowArrayStream::from_raw(pointer) };
    let mut reader = ArrowArrayStreamReader::try_new(ffi_stream)
        .map_err(|error| {
            repark_core::Error::DataFusion(format!(
                "register_arrow_stream_as_temp_view: open Arrow C Stream failed: {error}"
            ))
        })
        .map_err(to_py_err)?;

    let schema = reader.schema();
    let mut batches = Vec::new();
    // Python-backed streams re-enter the interpreter on every `get_next`.
    for batch_result in &mut reader {
        let batch = batch_result
            .map_err(|error| {
                repark_core::Error::DataFusion(format!(
                    "register_arrow_stream_as_temp_view: Arrow C Stream batch failed: {error}"
                ))
            })
            .map_err(to_py_err)?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    Ok((schema, batches))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// The Spark session must install both the Spark extension and dialect.
    #[test]
    fn spark_doored_session_resolves_spark_function_and_routes_spark_statement() {
        Python::attach(|py| {
            let session = PyReparkSession::new(py, None, None, None, None).expect("session builds");

            // (1) Spark function registry is installed: a Spark-only name resolves and evaluates.
            let frame = session
                .sql(py, "SELECT weekofyear(DATE '2021-01-01') AS w")
                .expect("a Spark-only function resolves — SparkExtension installed the registry");
            // Arrow path, not `show`: value AND type are the claim (docs/testing.md).
            let batches = frame
                .runtime_handle()
                .block_on(frame.inner().clone().collect())
                .expect("the Spark function evaluates");
            let weeks = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("weekofyear returns Int32");
            assert_eq!(
                weeks.value(0),
                53,
                "weekofyear must carry SPARK's ISO week-year semantics, not a DataFusion default"
            );

            // (2) Spark statement router is installed: a Spark-only statement reaches its refusal.
            let sql = "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE OUTPUT d.*";
            let Err(routed) = session.sql(py, sql) else {
                panic!("MERGE OUTPUT is a loud router refusal, not a plan")
            };
            let message = routed.to_string();
            assert!(
                message.contains("MERGE OUTPUT/RETURNING"),
                "the routed refusal names the MERGE OUTPUT gap; got: {message}"
            );
            assert!(
                routed.is_instance_of::<crate::UnsupportedOperationException>(py),
                "the router's NotImplemented folds to UnsupportedOperationException: {message}"
            );
        });
    }

    /// Native door: `PyReparkSession::native` must NOT install the Spark extension or dialect.
    #[test]
    fn native_session_is_not_spark_doored() {
        Python::attach(|py| {
            let session =
                PyReparkSession::native(py, None, None, None, None).expect("native session builds");

            let frame = session
                .sql(py, "SELECT CAST(5 AS INT) / CAST(2 AS INT) AS q")
                .expect("native integer division plans");
            let batches = frame
                .runtime_handle()
                .block_on(frame.inner().clone().collect())
                .expect("native integer division evaluates");
            let quotients = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("native INT/INT is Int32 (truncated), not Spark Float64");
            assert_eq!(
                quotients.value(0),
                2,
                "ANSI / DataFusion integer / truncates"
            );

            let Err(missing) = session.sql(py, "SELECT weekofyear(DATE '2021-01-01') AS w") else {
                panic!("weekofyear must not resolve on a native session");
            };
            let message = missing.to_string();
            assert!(
                message.to_ascii_lowercase().contains("weekofyear")
                    || message.to_ascii_lowercase().contains("invalid function"),
                "native session must lack the Spark function registry; got: {message}"
            );
        });
    }

    /// `read_excel` keeps its port-pin name, arity, and defaults and refuses loudly.
    #[test]
    fn read_excel_refuses_with_named_unsupported_operation() {
        Python::attach(|py| {
            let session = PyReparkSession::new(py, None, None, None, None).expect("session builds");
            // `PyDataFrame` is not `Debug`; pattern-match the error arm instead of `expect_err`.
            let Err(error) = session.read_excel(py, "/tmp/never-opened.xlsx", None) else {
                panic!(
                    "the excel reader is deferred post-milestone-one — it must not return a frame"
                )
            };
            assert!(
                error.is_instance_of::<crate::UnsupportedOperationException>(py),
                "a deferred surface raises UnsupportedOperationException"
            );
            assert!(
                error.is_instance_of::<crate::PySparkException>(py),
                "…which is still a PySparkException (hence a RuntimeError) — near-drop-in"
            );
            let message = error.to_string();
            assert!(
                message.contains("spark.read.excel"),
                "the message names the refused SURFACE: {message}"
            );
            assert!(
                message.contains("post-milestone-one"),
                "the message states the schedule: {message}"
            );
            assert!(
                message.contains("task/todo.md"),
                "the message points at the tracking row: {message}"
            );
        });
    }

    /// `excel_sheet_names` refuses with its own named surface.
    #[test]
    fn excel_sheet_names_refuses_with_named_unsupported_operation() {
        Python::attach(|py| {
            let session = PyReparkSession::new(py, None, None, None, None).expect("session builds");
            let error = session
                .excel_sheet_names(py, "/tmp/never-opened.xlsx")
                .expect_err("the excel reader is deferred post-milestone-one");
            assert!(error.is_instance_of::<crate::UnsupportedOperationException>(py));
            let message = error.to_string();
            assert!(
                message.contains("spark.read.sheet_names"),
                "the message names the refused SURFACE: {message}"
            );
            assert!(
                message.contains("post-milestone-one") && message.contains("task/todo.md"),
                "the message states the schedule and the tracking row: {message}"
            );
        });
    }

    /// The nine-argument JDBC refusal must not echo the connection URL or the properties map.
    #[test]
    fn read_postgres_refuses_with_named_unsupported_operation() {
        Python::attach(|py| {
            let session = PyReparkSession::new(py, None, None, None, None).expect("session builds");
            let Err(error) = session.read_postgres(
                py,
                "postgresql://user:sentinel-secret@host:5432/db",
                Some("public.t"),
                None,
                Some(HashMap::from([(
                    "password".to_owned(),
                    "sentinel-property-secret".to_owned(),
                )])),
                None,
                None,
                None,
                None,
                None,
            ) else {
                panic!(
                    "the postgres connector is deferred post-milestone-one — no frame is returned"
                )
            };
            assert!(error.is_instance_of::<crate::UnsupportedOperationException>(py));
            let message = error.to_string();
            assert!(
                message.contains("spark.read.jdbc"),
                "the message names the refused SURFACE: {message}"
            );
            assert!(
                message.contains("post-milestone-one") && message.contains("task/todo.md"),
                "the message states the schedule and the tracking row: {message}"
            );
            assert!(
                !message.contains("sentinel-secret") && !message.contains("postgresql://"),
                "a refusal must never echo the connection URL — it may carry credentials: \
                 {message}"
            );
            assert!(
                !message.contains("sentinel-property-secret") && !message.contains("password"),
                "a refusal must never echo the connection PROPERTIES — they may carry \
                 credentials: {message}"
            );
        });
    }

    /// A Rust panic through a fenced Python method surfaces as base `PySparkException`.
    #[test]
    fn fenced_panic_surfaces_as_pyspark_exception_and_leaves_session_usable() {
        Python::attach(|py| {
            let session = Py::new(
                py,
                PyReparkSession::new(py, None, None, None, None).expect("session builds"),
            )
            .expect("pyclass instantiates");

            // Drive the panic through real Python dispatch so PyO3's trampoline is in the loop.
            let error = session
                .call_method0(py, "panic_probe")
                .expect_err("the probe deterministically panics through the fence");
            assert!(
                error.is_instance_of::<crate::PySparkException>(py),
                "a fenced pymethod panic is the base PySparkException"
            );
            assert!(
                error.is_instance_of::<pyo3::exceptions::PyRuntimeError>(py),
                "PySparkException subclasses RuntimeError — `except RuntimeError` still catches it"
            );
            assert!(
                !error.is_instance_of::<pyo3::panic::PanicException>(py),
                "the fence must NOT let PyO3's raw PanicException (a BaseException) escape"
            );
            let message = error.to_string();
            assert!(
                message.contains("SAF-007 injected panic") && message.contains("internal error"),
                "the panic text is preserved under the internal-error framing: {message}"
            );

            // Interpreter alive + the SAME session still usable after the fenced panic.
            let frame = session
                .borrow(py)
                .sql(py, "SELECT 1 AS n")
                .expect("the session still plans and runs queries after a fenced panic");
            assert_eq!(
                frame.count(py).expect("count executes"),
                1,
                "the session remains usable after a fenced panic (nothing poisoned)"
            );
        });
    }

    #[test]
    fn sequential_sessions_share_one_tokio_runtime() {
        // Two sequential constructors must share one process-wide Tokio runtime.
        Python::attach(|py| {
            let first = PyReparkSession::new(py, None, None, None, None).expect("first session");
            let second = PyReparkSession::new(py, None, None, None, None).expect("second session");
            assert!(
                Arc::ptr_eq(&first.runtime_arc(), &second.runtime_arc()),
                "two PyReparkSession values must share the process-wide Tokio runtime Arc"
            );
        });
    }

    /// Collects the `family` field from a `py.entry` span.
    struct FamilyFieldVisitor<'a> {
        family: &'a mut String,
    }

    impl tracing::field::Visit for FamilyFieldVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "family" {
                let text = format!("{value:?}");
                *self.family = text
                    .strip_prefix('"')
                    .and_then(|s| s.strip_suffix('"'))
                    .unwrap_or(text.as_str())
                    .to_string();
            }
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "family" {
                *self.family = value.to_string();
            }
        }
    }

    struct FamilyRecorder {
        families: std::sync::Mutex<Vec<String>>,
    }

    struct FamilyLayer {
        recorder: Arc<FamilyRecorder>,
    }

    impl<S> tracing_subscriber::Layer<S> for FamilyLayer
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if attrs.metadata().name() != "py.entry" {
                return;
            }
            let mut family = String::new();
            attrs.record(&mut FamilyFieldVisitor {
                family: &mut family,
            });
            if !family.is_empty() {
                self.recorder
                    .families
                    .lock()
                    .expect("family lock")
                    .push(family);
            }
        }
    }

    /// Entry-point families emit `py.entry` spans with the family and operation fields.
    #[test]
    fn entry_point_families_emit_py_entry_spans() {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        use tracing_subscriber::layer::SubscriberExt;

        let recorder = Arc::new(FamilyRecorder {
            families: std::sync::Mutex::new(Vec::new()),
        });
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(FamilyLayer {
                recorder: Arc::clone(&recorder),
            }));

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let warehouse = std::env::temp_dir().join(format!("repark-obs1-family-{nanos}"));
        fs::create_dir_all(&warehouse).expect("temp warehouse dir");
        let warehouse_str = warehouse
            .to_str()
            .expect("utf-8 warehouse path")
            .to_string();

        Python::attach(|py| {
            let session = PyReparkSession::new(py, None, None, None, None).expect("session builds");
            let frame = session.sql(py, "SELECT 1 AS n").expect("sql plans");
            assert_eq!(frame.count(py).expect("count"), 1);
            // py.read: span opens before the body fails (missing path) — family still recorded.
            let _ = session.read_parquet(py, "/nonexistent/obs1-family-pin.parquet");
            // py.catalog: memory catalog registration (AWS-free).
            session
                .register_memory_catalog(py, "obs1_mem", &warehouse_str)
                .expect("memory catalog registers");
            let _ = session.table_exists(py, "no_such_temp_view");
        });

        let _ = fs::remove_dir_all(&warehouse);

        let families = recorder.families.lock().expect("family lock").clone();
        for expected in ["py.session", "py.sql", "py.action", "py.read", "py.catalog"] {
            assert!(
                families.iter().any(|family| family == expected),
                "expected family {expected}; recorded: {families:?}"
            );
        }
    }
}
