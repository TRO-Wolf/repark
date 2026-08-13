//! [`PyDataFrame`] — the Python-facing wrapper over a DataFusion [`DataFrame`].
//!
//! Exposes the eager actions PySpark users expect (`count`, `show`, `collect`) and the **zero-copy
//! Arrow handoff**: `__arrow_c_stream__` exports the rows through the Arrow `PyCapsule` interface, so
//! `pyarrow.table(df)` and `polars.from_arrow(df)` read the engine's Arrow buffers directly with no
//! serialization. The held [`DataFrame`] is cheap to [`Clone`]; the consuming engine actions
//! (`collect`/`count`) clone it so a [`PyDataFrame`] stays reusable.

use std::cell::Cell;
use std::collections::HashSet;
use std::ffi::CStr;
use std::sync::{Arc, OnceLock};

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::{DataType as ArrowDataType, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::util::pretty::pretty_format_batches;
use datafusion::common::{Column, JoinType};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::col;
use futures::StreamExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use tokio::runtime::Runtime;

use crate::column::PyColumn;
use crate::fence::{fence_stream_poll, fenced, fenced_span};
use crate::{datafusion_to_py_err, to_py_err};

/// The Arrow C stream interface mandates this exact capsule name (a NUL-terminated C string).
const ARROW_STREAM_CAPSULE_NAME: &CStr = c"arrow_array_stream";

/// Arrow C Data Interface schema capsule name (`PyCapsule` protocol / `pa.Schema._import_from_c_capsule`).
const ARROW_SCHEMA_CAPSULE_NAME: &CStr = c"arrow_schema";

// STREAM_POLL_NO_DETACH — see [`with_stream_poll_no_detach`]. (Doc comments cannot attach to
// `thread_local!` expansions under rustdoc.)
thread_local! {
    static STREAM_POLL_NO_DETACH: Cell<bool> = const { Cell::new(false) };
}

/// ===========================================================================================
/// Run `body` with `STREAM_POLL_NO_DETACH` set so nested repark-stream polls skip attach/detach.
///
/// Used by `register_arrow_stream_as_temp_view` while draining an Arrow C Stream that may re-enter
/// a repark `__arrow_c_stream__` through a Python generator (I4 / octo C1-SAF-001).
///
/// Why: the outer pymethod holds a PyO3 attach while an intermediate consumer (typical Cython
/// `RecordBatchReader.__next__` under `nogil`) may call `StreamingBatchReader::next` with the real
/// GIL already released. `Python::attach` then *assumes* attached (attach-count still >0) and
/// `detach`'s `PyEval_SaveThread` aborts with "GIL is released / thread state is NULL". Under the
/// flag we just `block_on` — process-safe; the outer drain owns the GIL story. abi3/limited-API
/// builds cannot use `PyGILState_Check`, so a thread-local flag is the portable contract.
///
/// Panic-safe (octo C2-SAF-001): a `Drop` guard restores the previous flag value even if `body`
/// panics (outer `fenced!` may catch the panic — without the guard the flag would stick `true`
/// and poison later export polls on this thread).
/// ===========================================================================================
pub(crate) fn with_stream_poll_no_detach<T>(body: impl FnOnce() -> T) -> T {
    struct RestoreNoDetach {
        previous: bool,
    }
    impl Drop for RestoreNoDetach {
        fn drop(&mut self) {
            STREAM_POLL_NO_DETACH.with(|flag| flag.set(self.previous));
        }
    }

    let previous = STREAM_POLL_NO_DETACH.with(|flag| flag.replace(true));
    let _restore = RestoreNoDetach { previous };
    body()
}

/// ===========================================================================================
/// Map a PySpark `how=` join keyword to a DataFusion [`JoinType`].
///
/// Supports the Spark join kinds the facade normalizes to (`inner` / `left` / `right` / `full`
/// plus the G4b semi family `leftsemi` / `leftanti`). An unrecognized keyword is a descriptive
/// error rather than a silent default (a silent `inner` would corrupt a left-join result set).
/// H1 (Group H) needs `right`/`full` for the Apache self-join / select-join-keys battery.
///
/// Every PySpark spelling is accepted here even though the facade normalizes before calling —
/// the binding is a public engine surface in its own right (`join_on_names` is called from the
/// Rust binding tests), so it must not depend on a caller-side normalization it cannot enforce.
/// ===========================================================================================
fn join_type_from_str(how: &str) -> PyResult<JoinType> {
    match how {
        "inner" => Ok(JoinType::Inner),
        "left" | "left_outer" | "leftouter" => Ok(JoinType::Left),
        "right" | "right_outer" | "rightouter" => Ok(JoinType::Right),
        "full" | "outer" | "fullouter" | "full_outer" => Ok(JoinType::Full),
        "semi" | "left_semi" | "leftsemi" => Ok(JoinType::LeftSemi),
        "anti" | "left_anti" | "leftanti" => Ok(JoinType::LeftAnti),
        other => Err(PyValueError::new_err(format!(
            "unsupported join type {other:?} (supported: 'inner', 'left', 'right', 'full', \
             'leftsemi', 'leftanti')"
        ))),
    }
}

/// ===========================================================================================
/// True when a join type's output schema is the LEFT input's schema alone.
///
/// `LeftSemi` / `LeftAnti` are filters expressed as joins: the right side decides which left
/// rows survive and contributes no columns. The Spark key-merge projection
/// ([`spark_join_projection`]) therefore must NOT run for them — there is no duplicate
/// right-hand key column to merge away, and building a projection over the joined schema would
/// be a no-op node at best and would mask a schema surprise at worst.
/// ===========================================================================================
fn join_keeps_only_left_columns(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti)
}

/// ===========================================================================================
/// Build the Spark-style output projection for an equi-join on named columns.
///
/// DataFusion's `join` keeps both the left and the right copy of each join key; PySpark keeps a
/// single merged key column. This projection keeps the first (left) occurrence of every join key
/// and every non-key column, dropping the duplicate right-side keys — reproducing Spark's
/// `df.join(other, on="k")` schema. Column collisions on *non-key* names are left to surface as
/// DataFusion's ambiguous-reference error, exactly as Spark raises on an ambiguous column.
/// ===========================================================================================
fn spark_join_projection(joined: &DataFrame, keys: &[String]) -> Vec<Expr> {
    let mut projection = Vec::new();
    let mut seen_keys = HashSet::new();
    for (qualifier, field) in joined.schema().iter() {
        let name = field.name();
        let is_key = keys.iter().any(|key| key == name);
        if is_key && !seen_keys.insert(name.clone()) {
            // The duplicate (right-side) copy of a join key — Spark merges it away.
            continue;
        }
        projection.push(Expr::Column(Column::new(qualifier.cloned(), name.clone())));
    }
    projection
}

/// ===========================================================================================
/// The Python-facing `DataFrame` (`repark._native.PyDataFrame`).
///
/// Holds the planned DataFusion [`DataFrame`] plus a handle to the session's Tokio [`Runtime`] so
/// it can drive its own async actions without re-entering the session.
///
/// # === r20 P2b: action/export ===
/// Plan handles are immutable: analyzed Arrow schema is cached once per handle (`OnceLock`) and
/// never invalidated. Interactive `columns` / `schema` / stream-export open no longer re-run
/// `analyze_eagerly` on every call.
/// ===========================================================================================
#[pyclass(name = "PyDataFrame", module = "repark._native")]
pub struct PyDataFrame {
    df: DataFrame,
    runtime: Arc<Runtime>,
    /// Cached post-`analyze_eagerly` Arrow schema for this plan handle (P2b scout #25).
    ///
    /// Filled on first [`Self::analyzed_arrow_schema_native`]; handles never mutate their plan, so
    /// the cache is never invalidated.
    analyzed_schema: OnceLock<SchemaRef>,
}

impl PyDataFrame {
    /// Wrap a planned [`DataFrame`]. Crate-internal: only [`crate::session::PyReparkSession`] mints
    /// these, threading through the runtime that owns the async engine.
    pub(crate) fn new(df: DataFrame, runtime: Arc<Runtime>) -> Self {
        Self {
            df,
            runtime,
            analyzed_schema: OnceLock::new(),
        }
    }

    /// The held plan — for session-side operations that take a `DataFrame` (temp-view
    /// registration) and for M3 ML fit streams (`ml` module).
    pub(crate) fn inner(&self) -> &DataFrame {
        &self.df
    }

    /// Shared Tokio runtime handle (M3 ML fit binder streams batches via `block_on`).
    pub(crate) fn runtime_handle(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }

    /// Post-analysis Arrow schema without executing the plan (metadata only).
    ///
    /// Runs [`repark_functions::analyze_eagerly`] so type-changing Spark rules (int `/` → float)
    /// are reflected — never pre-analysis labels. Exposed to Python as
    /// [`PyDataFrame::analyzed_arrow_schema`] (Arrow C schema capsule) for plan-only consumers
    /// such as the U7 `pandas_udf` pass-through type path (octo C6-Q-001).
    ///
    /// # === r20 P2b: action/export ===
    /// Result is cached on the handle (`OnceLock<SchemaRef>`). First call pays analysis; later
    /// metadata/export opens reuse the same `SchemaRef` (handles are immutable — never invalidate).
    fn analyzed_arrow_schema_native(&self) -> PyResult<SchemaRef> {
        if let Some(schema) = self.analyzed_schema.get() {
            return Ok(Arc::clone(schema));
        }
        let (state, plan) = self.df.clone().into_parts();
        let analyzed =
            repark_functions::analyze_eagerly(&state, plan).map_err(datafusion_to_py_err)?;
        let schema: SchemaRef = Arc::new(analyzed.schema().as_arrow().clone());
        // First writer wins under concurrent first-touch; losers drop their compute result.
        let _ = self.analyzed_schema.set(Arc::clone(&schema));
        Ok(self.analyzed_schema.get().map(Arc::clone).unwrap_or(schema))
    }
}

/// Max nesting depth for Arrow list/map type-key formatting (E2 / octo C3-CRATE-001).
///
/// `arrow_type_key` and [`spark_array_element_simple_string_at_depth`] recurse on nested
/// `List` / `LargeList` / `FixedSizeList` (and `Map` entry types). Without a bound, an
/// adversarial deep `List` schema can stack-overflow via `logical_schema_fields` / facade
/// `dtypes`. Past this depth the walk terminates with the fallback key
/// [`ARROW_TYPE_KEY_DEPTH_FALLBACK`].
const ARROW_TYPE_KEY_MAX_DEPTH: usize = 32;

/// Terminal token when [`ARROW_TYPE_KEY_MAX_DEPTH`] is exhausted.
const ARROW_TYPE_KEY_DEPTH_FALLBACK: &str = "...";

/// Map an Arrow data type onto a short repark facade type key for `StructType` construction.
fn arrow_type_key(data_type: &ArrowDataType) -> String {
    arrow_type_key_at_depth(data_type, 0)
}

/// Depth-bounded implementation of [`arrow_type_key`] (octo C3-CRATE-001).
fn arrow_type_key_at_depth(data_type: &ArrowDataType, depth: usize) -> String {
    if depth >= ARROW_TYPE_KEY_MAX_DEPTH {
        return ARROW_TYPE_KEY_DEPTH_FALLBACK.to_string();
    }
    match data_type {
        // Top-level width collapse is intentional (Spark IntegerType / DoubleType dtypes
        // surface): Int8/16/32 → "int", Float* → "double". Nested array elements use
        // [`spark_array_element_simple_string_at_depth`] so `array<tinyint>` / `array<float>`
        // match Spark (E2 ndarray lit).
        ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32 => "int".to_string(),
        ArrowDataType::Int64 | ArrowDataType::UInt64 => "long".to_string(),
        ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64 => {
            "double".to_string()
        }
        ArrowDataType::Boolean => "boolean".to_string(),
        ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8
        | ArrowDataType::Utf8View
        | ArrowDataType::Binary
        | ArrowDataType::LargeBinary
        | ArrowDataType::BinaryView => "string".to_string(),
        ArrowDataType::Date32 | ArrowDataType::Date64 => "date".to_string(),
        ArrowDataType::Timestamp(_, _) => "timestamp".to_string(),
        ArrowDataType::Decimal128(precision, scale)
        | ArrowDataType::Decimal256(precision, scale) => {
            format!("decimal({precision},{scale})")
        }
        // List / LargeList / FixedSizeList → Spark simpleString `array<element>` (E2).
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _) => {
            let element = spark_array_element_simple_string_at_depth(field.data_type(), depth + 1);
            format!("array<{element}>")
        }
        ArrowDataType::Map(entries, _) => {
            // Map entries are a struct of (key, value).
            if let ArrowDataType::Struct(fields) = entries.data_type()
                && fields.len() >= 2
            {
                let key =
                    spark_array_element_simple_string_at_depth(fields[0].data_type(), depth + 1);
                let value =
                    spark_array_element_simple_string_at_depth(fields[1].data_type(), depth + 1);
                return format!("map<{key},{value}>");
            }
            format!("{data_type:?}")
        }
        // Nested struct → Spark simpleString `struct<_1:bigint,_2:bigint>` (F2 nested
        // createDataFrame / printSchema). Debug `Struct([Field…])` is not a DDL key and
        // the facade previously collapsed it to StringType.
        ArrowDataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|field| {
                    let child =
                        spark_array_element_simple_string_at_depth(field.data_type(), depth + 1);
                    format!("{}:{child}", field.name())
                })
                .collect();
            format!("struct<{}>", parts.join(","))
        }
        other => format!("{other:?}"),
    }
}

/// Spark `simpleString` element token for nested array/map keys (E2 ndarray / array dtypes).
///
/// Depth-bounded (octo C3-CRATE-001) — see [`ARROW_TYPE_KEY_MAX_DEPTH`].
fn spark_array_element_simple_string_at_depth(data_type: &ArrowDataType, depth: usize) -> String {
    if depth >= ARROW_TYPE_KEY_MAX_DEPTH {
        return ARROW_TYPE_KEY_DEPTH_FALLBACK.to_string();
    }
    match data_type {
        ArrowDataType::Int8 => "tinyint".to_string(),
        ArrowDataType::Int16 => "smallint".to_string(),
        ArrowDataType::Int32
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32 => "int".to_string(),
        ArrowDataType::Int64 | ArrowDataType::UInt64 => "bigint".to_string(),
        ArrowDataType::Float16 | ArrowDataType::Float32 => "float".to_string(),
        ArrowDataType::Float64 => "double".to_string(),
        ArrowDataType::Boolean => "boolean".to_string(),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            "string".to_string()
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView => {
            "binary".to_string()
        }
        ArrowDataType::Date32 | ArrowDataType::Date64 => "date".to_string(),
        ArrowDataType::Timestamp(_, _) => "timestamp".to_string(),
        ArrowDataType::Decimal128(precision, scale)
        | ArrowDataType::Decimal256(precision, scale) => {
            format!("decimal({precision},{scale})")
        }
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _) => {
            format!(
                "array<{}>",
                spark_array_element_simple_string_at_depth(field.data_type(), depth + 1)
            )
        }
        // Pass depth through mutual recursion so Map/List alternation cannot reset the bound.
        other => arrow_type_key_at_depth(other, depth + 1),
    }
}

/// ===========================================================================================
/// A synchronous [`RecordBatchReader`] that lazily pulls ONE batch per `next()` from a DataFusion
/// [`SendableRecordBatchStream`], blocking the calling thread on the shared Tokio runtime for each
/// poll.
///
/// This is the producer half of the zero-copy Arrow export. [`FFI_ArrowArrayStream::new`] wraps it,
/// and the Arrow C stream consumer (`pyarrow.table(df)` / `polars.from_arrow(df)`) drives `next()`
/// through the FFI `get_next` callback. A batch is materialized only when the consumer asks for it,
/// so peak memory is O(one batch), not O(whole result) — the correction for the "stream export lie"
/// (audit SAF-003 / finding #14), replacing the prior full `collect()`-then-wrap.
///
/// The laziness guarantee is a **memory** bound — O(one batch) peak — not an *ordering* one. Over a
/// **sequential** stream this reader necessarily yields batch 1 before a later batch's error (the
/// reader pin `streaming_reader_yields_first_batch_before_a_later_error`); but the *end-to-end*
/// export ([`PyDataFrame::__arrow_c_stream__`]) runs a real, possibly parallel DataFusion plan whose
/// repartitioned execution may surface a later batch's error before batch 1 (audit F-BR-5). Callers
/// are promised O(batch) peak memory, never that "batch 1 arrives before any error".
///
/// Runtime safety: `block_on` runs on the *consumer's* calling thread (the Python thread for
/// pyarrow/polars; the test thread under `cargo test`), never a runtime worker thread, and is never
/// nested inside another `block_on` — so it cannot re-enter the process-wide `OnceLock` runtime
/// (SAF-008). The GIL is released for each poll ([`Python::detach`]) so other Python threads make
/// progress while the engine produces the next batch.
///
/// # `KeyboardInterrupt` / Ctrl-C (r23 PG2 / OTH-009 / Q15)
///
/// While this poll is parked in `block_on`, a SIGINT is **deferred** until the poll returns —
/// Python only raises `KeyboardInterrupt` when the main thread resumes bytecode. A
/// `python.check_signals()` call between batches was evaluated as the candidate abort seam and
/// **rejected for shipping**: the only channel out of `extern "C" get_next` is `ArrowError`, and
/// the facade re-raises mid-stream Arrow failures as `PySparkException`, which would **launder**
/// `KeyboardInterrupt` into the wrong type (see `test_applyinpandas` "must not be wrapped" and
/// `task/pg2-pg-runtime-ledger.md`). No runtime architecture change this unit. Residual: Ctrl-C
/// during a long single-batch poll waits for that poll; cooperative cancellation is parked.
/// ===========================================================================================
struct StreamingBatchReader {
    runtime: Arc<Runtime>,
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
}

impl Iterator for StreamingBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    /// Pull exactly one batch. A DataFusion error becomes [`ArrowError::ExternalError`] — whose
    /// `Display` preserves the engine text — which the FFI stream surfaces to the consumer.
    fn next(&mut self) -> Option<Self::Item> {
        // SAF-007: this poll is the body of `arrow`'s `extern "C" fn get_next` (the Arrow C-stream
        // callback); an escaping panic would unwind across `extern "C"` and ABORT the process (it is
        // NOT covered by PyO3's pymethod trampoline — see `fence.rs`). Fence the poll so a panic
        // becomes a terminal `Err(ArrowError)` on the stream's error channel instead.
        //
        // GIL model (I4 / octo C1-SAF-001): default path is `Python::attach` + `detach` so other
        // Python threads progress while the engine produces a batch. When
        // [`with_stream_poll_no_detach`] is active (C-stream *ingest* drain), skip attach/detach —
        // see that helper's docs for the process-abort footgun under nested repark-stream re-entry.
        let Self {
            runtime, stream, ..
        } = self;
        fence_stream_poll("PyDataFrame.__arrow_c_stream__.next", || {
            let no_detach = STREAM_POLL_NO_DETACH.with(Cell::get);
            let polled = if no_detach {
                runtime.block_on(stream.next())
            } else {
                Python::attach(|python| python.detach(|| runtime.block_on(stream.next())))
            };
            polled.map(|batch| batch.map_err(|error| ArrowError::ExternalError(Box::new(error))))
        })
    }
}

impl RecordBatchReader for StreamingBatchReader {
    /// The declared schema for the exported stream. The caller ([`PyDataFrame::__arrow_c_stream__`])
    /// hands it the analyzed LOGICAL schema (`analyze_eagerly`): the same TYPES the physical batches
    /// carry (so the consumer never bit-reinterprets — the F.expr class, `task/lessons.md`
    /// 2026-07-13), but Spark-style `nullable = true`, so the export matches Spark parity and the
    /// `columns`/`schema` metadata path rather than the physical `nullable = false`.
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// The transform methods return a fresh `PyDataFrame` the Python caller always consumes, and their
// pyclass args (`PyColumn`, `Vec<PyColumn>`, `Vec<String>`) arrive by value from Python.
#[allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::needless_pass_by_value
)]
#[pymethods]
impl PyDataFrame {
    /// Number of rows (PySpark `DataFrame.count`). Pushes the count into the engine; does not
    /// materialize rows.
    ///
    /// # Errors
    /// Returns `RuntimeError` if the engine fails to execute the count.
    pub fn count(&self, py: Python<'_>) -> PyResult<usize> {
        fenced_span!("py.action", "PyDataFrame.count", {
            py.detach(|| self.runtime.block_on(self.df.clone().count()))
                .map_err(datafusion_to_py_err)
        })
    }

    /// Column names from the **analyzed** logical schema — **no plan execution**.
    ///
    /// # Errors
    /// Returns `RuntimeError` if analysis fails.
    pub fn column_names(&self) -> PyResult<Vec<String>> {
        fenced!("PyDataFrame.column_names", {
            let schema = self.analyzed_arrow_schema_native()?;
            Ok(schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect())
        })
    }

    /// Logical schema metadata `(name, type_key, nullable)` after analysis — **no execution**.
    ///
    /// `type_key` is a short repark/facade token (`int`/`long`/`double`/`string`/…).
    ///
    /// # Errors
    /// Returns `RuntimeError` if analysis fails.
    pub fn logical_schema_fields(&self) -> PyResult<Vec<(String, String, bool)>> {
        fenced!("PyDataFrame.logical_schema_fields", {
            let schema = self.analyzed_arrow_schema_native()?;
            Ok(schema
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.name().clone(),
                        arrow_type_key(field.data_type()),
                        field.is_nullable(),
                    )
                })
                .collect())
        })
    }

    /// Post-analysis Arrow schema as an Arrow C Data Interface `PyCapsule` — **no plan execution**.
    ///
    /// Returns a capsule named `arrow_schema` suitable for `pyarrow.Schema._import_from_c_capsule`.
    /// Physical Arrow field types are preserved (float32, int16, binary, timestamp with tz, …) —
    /// unlike the collapsed type keys from [`Self::logical_schema_fields`]. Analysis only:
    /// no `limit`/`collect`/`to_arrow` and no row materialization (U7 octo C6-Q-001).
    ///
    /// # Errors
    /// Returns a classified engine exception if analysis fails, or `ValueError` if the schema
    /// cannot be exported to the C Data Interface.
    pub fn analyzed_arrow_schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        fenced!("PyDataFrame.analyzed_arrow_schema", {
            let schema = self.analyzed_arrow_schema_native()?;
            let ffi = FFI_ArrowSchema::try_from(schema.as_ref()).map_err(|error| {
                PyValueError::new_err(format!(
                    "failed to export analyzed Arrow schema to C Data Interface: {error}"
                ))
            })?;
            PyCapsule::new_with_value_and_destructor(
                py,
                ffi,
                ARROW_SCHEMA_CAPSULE_NAME,
                |ffi_schema, _ctx| drop(ffi_schema),
            )
        })
    }

    /// Limit rows engine-side (PySpark `DataFrame.limit`).
    ///
    /// # Errors
    /// Returns `RuntimeError` if the limit cannot be planned.
    pub fn limit(&self, n: usize) -> PyResult<Self> {
        fenced!("PyDataFrame.limit", {
            let df = self
                .df
                .clone()
                .limit(0, Some(n))
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Engine-side skip + fetch (DataFusion `Limit` with non-zero skip).
    ///
    /// Used by the facade display-style tail preview (`_preview_tail_rows`) so a head+tail
    /// `show()` path does not materialize the full result into an Arrow table. Public
    /// `DataFrame.tail` (separate unit) can share this later.
    ///
    /// # Errors
    /// Returns `RuntimeError` if the limit cannot be planned.
    pub fn limit_with_skip(&self, skip: usize, fetch: usize) -> PyResult<Self> {
        fenced!("PyDataFrame.limit_with_skip", {
            let df = self
                .df
                .clone()
                .limit(skip, Some(fetch))
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Render up to `n` rows as a text table (PySpark `DataFrame.show`).
    ///
    /// Applies engine-side `limit(n)` before collect so a large plan does not fully materialize.
    ///
    /// # Errors
    /// Returns `RuntimeError` if the engine fails to execute or the batches cannot be formatted.
    #[pyo3(signature = (n=20))]
    pub fn show(&self, py: Python<'_>, n: usize) -> PyResult<String> {
        fenced_span!("py.action", "PyDataFrame.show", {
            let limited = self
                .df
                .clone()
                .limit(0, Some(n))
                .map_err(datafusion_to_py_err)?;
            let batches = py.detach(|| {
                self.runtime
                    .block_on(limited.collect())
                    .map_err(datafusion_to_py_err)
            })?;
            pretty_format_batches(&batches)
                .map(|table| table.to_string())
                .map_err(|err| to_py_err(repark_core::Error::DataFusion(err.to_string())))
        })
    }

    /// Export the rows through the **Arrow `PyCapsule` interface** (zero-copy, **streaming**).
    ///
    /// Returns a single capsule named `arrow_array_stream` wrapping an [`FFI_ArrowArrayStream`] over
    /// a [`StreamingBatchReader`]. `pyarrow.table(df)` / `polars.from_arrow(df)` call this, then pull
    /// batches through the stream's C `get_next` callback — each pull runs one
    /// [`DataFrame::execute_stream`] poll on demand, so peak memory is O(one batch), not O(whole
    /// result) — a peak-**memory** bound, not a batch/error ordering guarantee (the engine plan may
    /// be parallel; see [`StreamingBatchReader`] and audit F-BR-5). pyo3 owns the
    /// [`FFI_ArrowArrayStream`] value inside the capsule; its destructor drops
    /// it — a no-op once the consumer has moved the stream out, since [`FFI_ArrowArrayStream`]'s
    /// `Drop` only fires the still-set `release` callback.
    ///
    /// `requested_schema` (schema-cast negotiation) is accepted for protocol compatibility but not
    /// honored — the engine always exports its native (analyzed) schema. This matches what
    /// pyarrow/polars do when they cannot satisfy a requested cast (they fall back to the producer's
    /// schema).
    ///
    /// # Errors
    /// Returns a classified engine exception if the physical plan cannot be built, or `RuntimeError`
    /// if the capsule cannot be allocated. A failure encountered mid-stream surfaces to the consumer
    /// as an Arrow stream error (see [`StreamingBatchReader::next`]).
    // `requested_schema` is part of the Arrow PyCapsule protocol signature; we accept but ignore it
    // (we always export the native schema), so it is intentionally passed by value and unused.
    #[allow(clippy::needless_pass_by_value)]
    #[pyo3(signature = (requested_schema=None))]
    pub fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        fenced_span!("py.action", "PyDataFrame.__arrow_c_stream__", {
            let _ = requested_schema;
            // Declared stream schema = the analyzed LOGICAL schema (the blessed `analyze_eagerly`
            // path): right TYPES and Spark-style `nullable = true`. NOT the physical
            // `stream.schema()`, which reports `nullable = false` for computed/literal columns and
            // would diverge from Spark parity (and the `columns`/`schema` metadata path). The
            // physical batches carry the SAME types (so no Arrow reinterpret — `task/lessons.md`
            // 2026-07-13), and their non-null data is validly read under a nullable-permissive
            // schema. Matches the pre-streaming export schema.
            // P2b: analyzed schema is cached on the handle (OnceLock) — open does not re-analyze.
            let schema: SchemaRef = self.analyzed_arrow_schema_native()?;
            // Open a lazy batch stream — the physical plan build runs with the GIL released.
            let stream = py
                .detach(|| self.runtime.block_on(self.df.clone().execute_stream()))
                .map_err(datafusion_to_py_err)?;
            let reader: Box<dyn RecordBatchReader + Send> = Box::new(StreamingBatchReader {
                runtime: Arc::clone(&self.runtime),
                stream,
                schema,
            });
            let ffi_stream = FFI_ArrowArrayStream::new(reader);

            // pyo3 0.29 renamed `new_with_destructor` → `new_with_value_and_destructor` and the
            // `name` parameter became `&'static CStr` (was `Option<CString>`); pass the const.
            PyCapsule::new_with_value_and_destructor(
                py,
                ffi_stream,
                ARROW_STREAM_CAPSULE_NAME,
                |ffi_stream, _ctx| drop(ffi_stream),
            )
        })
    }

    /// Add or replace a column (PySpark `DataFrame.withColumn`). Returns a new [`PyDataFrame`];
    /// the original plan is unchanged (Spark `DataFrame`s are immutable).
    ///
    /// # Errors
    /// Returns `RuntimeError` if the resulting plan cannot be built (e.g. the expression references
    /// an unknown column).
    pub fn with_column(&self, name: &str, column: PyColumn) -> PyResult<Self> {
        fenced!("PyDataFrame.with_column", {
            let df = self
                .df
                .clone()
                .with_column(name, column.expr())
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Keep only rows matching a [`PyColumn`] predicate (PySpark `DataFrame.filter` / `where` with a
    /// `Column`).
    ///
    /// # Errors
    /// Returns `RuntimeError` if the predicate cannot be planned.
    pub fn filter(&self, predicate: PyColumn) -> PyResult<Self> {
        fenced!("PyDataFrame.filter", {
            let df = self
                .df
                .clone()
                .filter(predicate.expr())
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Keep only rows matching a SQL-string predicate (PySpark `DataFrame.filter("a > 1")`).
    ///
    /// The string is parsed against *this* `DataFrame`'s schema on the real session, so session-
    /// registered functions and the frame's own columns resolve.
    ///
    /// # Errors
    /// Returns `RuntimeError` if the predicate does not parse or cannot be planned.
    pub fn filter_sql(&self, predicate: &str) -> PyResult<Self> {
        fenced!("PyDataFrame.filter_sql", {
            // G15: filter("col COLLATE name = …") never hits the statement router.
            repark_spark::refuse_collation_in_sql(predicate).map_err(datafusion_to_py_err)?;
            let expr = self
                .df
                .parse_sql_expr(predicate)
                .map_err(datafusion_to_py_err)?;
            let df = self.df.clone().filter(expr).map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Project to the given columns/expressions (PySpark `DataFrame.select`). The facade maps bare
    /// column names to `col(name)` before calling, so every argument arrives as a [`PyColumn`].
    ///
    /// # Errors
    /// Returns `RuntimeError` if the projection cannot be planned.
    pub fn select(&self, columns: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("PyDataFrame.select", {
            let expressions: Vec<Expr> = columns.iter().map(PyColumn::expr).collect();
            let df = self
                .df
                .clone()
                .select(expressions)
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Drop columns by name (PySpark `DataFrame.drop`). Dropping an absent column is a no-op, like
    /// Spark.
    ///
    /// # Errors
    /// Returns `RuntimeError` if the resulting plan cannot be built.
    pub fn drop(&self, names: Vec<String>) -> PyResult<Self> {
        fenced!("PyDataFrame.drop", {
            let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
            let df = self
                .df
                .clone()
                .drop_columns(&name_refs)
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Order rows by the given columns (PySpark `DataFrame.orderBy` / `sort`). The three vectors are
    /// parallel: `columns[i]` is sorted `ascending[i]` with `nulls_first[i]` — the facade derives
    /// the direction/null-ordering from each column's `asc()` / `desc()` and Spark's defaults
    /// (ascending → nulls first, descending → nulls last).
    ///
    /// # Errors
    /// Returns `RuntimeError` if the three vectors differ in length or the sort cannot be planned.
    pub fn sort(
        &self,
        columns: Vec<PyColumn>,
        ascending: Vec<bool>,
        nulls_first: Vec<bool>,
    ) -> PyResult<Self> {
        fenced!("PyDataFrame.sort", {
            if columns.len() != ascending.len() || columns.len() != nulls_first.len() {
                return Err(PyValueError::new_err(
                    "sort expects columns, ascending, and nulls_first vectors of equal length",
                ));
            }
            let sort_expressions = columns
                .iter()
                .zip(ascending)
                .zip(nulls_first)
                .map(|((column, is_ascending), nulls_first)| {
                    column.expr().sort(is_ascending, nulls_first)
                })
                .collect();
            let df = self
                .df
                .clone()
                .sort(sort_expressions)
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Equi-join on shared column names (PySpark `df.join(other, on=<name|list>, how=…)`).
    ///
    /// Reproduces Spark's single-merged-key-column output via [`spark_join_projection`] for the
    /// join types that carry both sides through. `how` accepts every keyword
    /// [`join_type_from_str`] maps.
    ///
    /// **Semi family (G4b).** `leftsemi` / `leftanti` output the LEFT side's columns only, so the
    /// key-merge projection is skipped ([`join_keeps_only_left_columns`]) and the join's own
    /// schema is the result schema. NULL keys never match (`NULL = NULL` is unknown), so a semi
    /// join drops NULL-keyed left rows and an anti join keeps them.
    ///
    /// # Errors
    /// Returns `RuntimeError` for an unsupported `how`, or if the join/projection cannot be planned.
    pub fn join_on_names(
        &self,
        right: PyRef<'_, PyDataFrame>,
        on: Vec<String>,
        how: &str,
    ) -> PyResult<Self> {
        fenced!("PyDataFrame.join_on_names", {
            let join_type = join_type_from_str(how)?;
            let on_refs: Vec<&str> = on.iter().map(String::as_str).collect();
            let joined = self
                .df
                .clone()
                .join(right.df.clone(), join_type, &on_refs, &on_refs, None)
                .map_err(datafusion_to_py_err)?;
            let df = if join_keeps_only_left_columns(join_type) {
                joined
            } else {
                let projection = spark_join_projection(&joined, &on);
                joined.select(projection).map_err(datafusion_to_py_err)?
            };
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Join on a boolean [`PyColumn`] condition (PySpark `df.join(other, on=<Column>, how=…)`).
    ///
    /// Keeps all columns from both sides (Spark does not merge columns for an expression join) —
    /// except for the semi family, where DataFusion's own `LeftSemi`/`LeftAnti` output schema is
    /// the left side alone, matching Spark.
    ///
    /// # Errors
    /// Returns `RuntimeError` for an unsupported `how`, or if the join cannot be planned.
    pub fn join_on_condition(
        &self,
        right: PyRef<'_, PyDataFrame>,
        condition: PyColumn,
        how: &str,
    ) -> PyResult<Self> {
        fenced!("PyDataFrame.join_on_condition", {
            let join_type = join_type_from_str(how)?;
            let joined = self
                .df
                .clone()
                .join_on(right.df.clone(), join_type, [condition.expr()])
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(joined, Arc::clone(&self.runtime)))
        })
    }

    /// Group + aggregate (PySpark `GroupedData.agg`; a global aggregate when `group_by` is empty).
    ///
    /// `group_by` are the grouping expressions (Spark places these columns first in the output);
    /// `aggregates` are the aggregate expressions, each already aliased facade-side to its PySpark
    /// output name (`sum(x)`, `count`, …). An empty `group_by` produces the global one-row
    /// aggregate — over an empty input that is a single row of NULLs (Spark), where a grouped
    /// aggregate over the same empty input is zero rows.
    ///
    /// # Errors
    /// Returns `RuntimeError` if the aggregate cannot be planned (e.g. an aggregate references an
    /// unknown column).
    pub fn aggregate(&self, group_by: Vec<PyColumn>, aggregates: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("PyDataFrame.aggregate", {
            let group_exprs: Vec<Expr> = group_by.iter().map(PyColumn::expr).collect();
            let aggregate_exprs: Vec<Expr> = aggregates.iter().map(PyColumn::expr).collect();
            let df = self
                .df
                .clone()
                .aggregate(group_exprs, aggregate_exprs)
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Set-union with `other` (PySpark `DataFrame.union` / `unionAll` / `unionByName`).
    ///
    /// `by_name = false` unions by **position**, keeping this frame's column names and coercing the
    /// two column types to a common super-type (Spark `union`). `by_name = true` resolves by
    /// **name**, filling any column present in only one side with NULL (Spark
    /// `unionByName(allowMissingColumns=True)` — the facade rejects a missing-column mismatch
    /// up front when `allowMissingColumns=False`). Neither form deduplicates (Spark parity: `union`
    /// is UNION ALL; use `distinct` to dedupe).
    ///
    /// # Errors
    /// Returns `RuntimeError` if the two frames cannot be unioned (e.g. a positional column-count
    /// mismatch, which Spark also raises).
    pub fn union(&self, other: PyRef<'_, PyDataFrame>, by_name: bool) -> PyResult<Self> {
        fenced!("PyDataFrame.union", {
            let unioned = if by_name {
                self.df.clone().union_by_name(other.df.clone())
            } else {
                self.df.clone().union(other.df.clone())
            }
            .map_err(datafusion_to_py_err)?;
            Ok(Self::new(unioned, Arc::clone(&self.runtime)))
        })
    }

    /// Distinct rows over **all** columns (PySpark `DataFrame.distinct` / `dropDuplicates()`).
    ///
    /// # Errors
    /// Returns `RuntimeError` if the distinct cannot be planned.
    pub fn distinct(&self) -> PyResult<Self> {
        fenced!("PyDataFrame.distinct", {
            let df = self.df.clone().distinct().map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Distinct rows keyed on a **subset** of columns (PySpark `dropDuplicates(subset)`).
    ///
    /// Keeps one row per distinct `subset` key and projects every original column, in order. Like
    /// Spark, which row survives per key is not specified when the non-key columns differ — parity
    /// fixtures pin a deterministic survivor (a unique key set, or identical non-key values).
    ///
    /// # Errors
    /// Returns `RuntimeError` if a subset column is unknown or the plan cannot be built.
    pub fn distinct_on(&self, subset: Vec<String>) -> PyResult<Self> {
        fenced!("PyDataFrame.distinct_on", {
            let on_exprs: Vec<Expr> = subset.iter().map(col).collect();
            let select_exprs: Vec<Expr> = self
                .df
                .schema()
                .iter()
                .map(|(_qualifier, field)| col(field.name()))
                .collect();
            let df = self
                .df
                .clone()
                .distinct_on(on_exprs, select_exprs, None)
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Rename a column (PySpark `DataFrame.withColumnRenamed`). Renaming a column that is absent is
    /// a silent no-op (Spark semantics — DataFusion's `with_column_renamed` returns the frame
    /// unchanged when the old name is not found).
    ///
    /// # Errors
    /// Returns `RuntimeError` if the projection cannot be rebuilt.
    pub fn with_column_renamed(&self, old_name: &str, new_name: &str) -> PyResult<Self> {
        fenced!("PyDataFrame.with_column_renamed", {
            let df = self
                .df
                .clone()
                .with_column_renamed(old_name, new_name)
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }
}

#[cfg(test)]
mod tests {
    //! Streaming Arrow-export pins, in two layers.
    //!
    //! **Reader-level** ([`StreamingBatchReader`]): driven through hand-scripted
    //! [`SendableRecordBatchStream`]s — the exact type `DataFrame::execute_stream` hands the reader —
    //! so the batch/error sequence is under the test's control (no engine, no wheel): multi-batch
    //! value+type fidelity, error surfacing, and — over a **sequential** stream — batch-1-before-a-
    //! later-error (a sequential-stream *ordering* property).
    //!
    //! **End-to-end** ([`PyDataFrame::__arrow_c_stream__`]):
    //! `arrow_c_stream_export_is_lazy_and_does_not_materialize_up_front` drives the *real* dunder over
    //! a real single-partition DataFusion plan whose source counts the batches it produces, proving
    //! the export does not collect up front — the pin that goes red on a collect-then-wrap revert
    //! (audit F-BR-4). The export's contract is O(one batch) **peak memory**, NOT batch/error
    //! ordering (audit F-BR-5). Every pin is deterministic — no RSS thresholds, no sleeps, no timing
    //! races (rule 12): the counter is a synchronous atomic read.

    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ffi_stream::ArrowArrayStreamReader;
    use datafusion::catalog::streaming::StreamingTable;
    use datafusion::error::DataFusionError;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::PartitionStream;
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// octo C2-SAF-001: panic inside `with_stream_poll_no_detach` must restore the TLS flag so a
    /// later export poll does not permanently skip attach/detach on this thread.
    #[test]
    fn stream_poll_no_detach_restores_flag_after_panic() {
        assert!(
            !STREAM_POLL_NO_DETACH.with(Cell::get),
            "precondition: flag starts false"
        );
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_stream_poll_no_detach(|| {
                assert!(
                    STREAM_POLL_NO_DETACH.with(Cell::get),
                    "flag true inside body"
                );
                panic!("injected panic for TLS restore pin");
            });
        }));
        assert!(panicked.is_err(), "body must panic");
        assert!(
            !STREAM_POLL_NO_DETACH.with(Cell::get),
            "flag must be false after panic unwind (Drop guard)"
        );
    }

    /// The single-column `Int64` schema (`v`, non-null) the reader fixtures stream.
    fn int64_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    /// A one-column `Int64` batch carrying `values` (non-null).
    fn int64_batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            int64_schema(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("int64 batch builds")
    }

    /// The `Int64` values of a batch's first column, in row order.
    fn int64_values(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("first column is Int64")
            .values()
            .to_vec()
    }

    /// Wrap a scripted sequence of batch results in a [`SendableRecordBatchStream`] — the reader is
    /// driven through its real code path, only the batch/error script is the test's.
    fn scripted_stream(
        schema: SchemaRef,
        items: Vec<Result<RecordBatch, DataFusionError>>,
    ) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(items),
        ))
    }

    /// A private multi-thread runtime for the reader to `block_on` — independent of the process
    /// `OnceLock` runtime (the reader only needs *a* runtime to drive its poll).
    fn reader_test_runtime() -> Arc<Runtime> {
        Arc::new(Runtime::new().expect("tokio runtime builds"))
    }

    /// A [`PartitionStream`] that counts how many batches it has actually **produced** (yielded on a
    /// poll). `futures::stream::iter(..).inspect(..)` fires the closure only as each item is polled
    /// through, so nothing is counted until a consumer drives the stream — an *un-consumed* export
    /// reads zero. This lets a pin prove `__arrow_c_stream__` pulls lazily instead of draining the
    /// whole result up front (audit F-BR-4).
    #[derive(Debug)]
    struct CountingPartitionStream {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        produced: Arc<AtomicUsize>,
    }

    impl PartitionStream for CountingPartitionStream {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            let produced = Arc::clone(&self.produced);
            let items: Vec<Result<RecordBatch, DataFusionError>> =
                self.batches.iter().cloned().map(Ok).collect();
            // `inspect` runs per yielded item, i.e. once per poll that produces a batch — this is
            // the "batches materialized so far" counter the laziness pin reads.
            let counted = futures::stream::iter(items).inspect(move |_batch| {
                produced.fetch_add(1, Ordering::SeqCst);
            });
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                counted,
            ))
        }
    }

    /// Move the `FFI_ArrowArrayStream` out of a capsule and wrap it as a reader — the consumer half
    /// of the Arrow C stream protocol (what pyarrow/polars do internally). Mirrors
    /// `tests/bindings.rs::import_stream`.
    fn import_capsule_stream(capsule: &Bound<'_, PyCapsule>) -> ArrowArrayStreamReader {
        let pointer = capsule
            .pointer_checked(Some(ARROW_STREAM_CAPSULE_NAME))
            .expect("capsule pointer is valid for the arrow stream name")
            .as_ptr()
            .cast::<FFI_ArrowArrayStream>();
        // SAFETY: `__arrow_c_stream__` placed a valid, initialized `FFI_ArrowArrayStream` here;
        // `from_raw` moves it out (nulling the producer's release callback so the capsule destructor
        // is a no-op) and we own the moved value for the reader's lifetime.
        let stream = unsafe { FFI_ArrowArrayStream::from_raw(pointer) };
        ArrowArrayStreamReader::try_new(stream).expect("stream is a valid Arrow C stream")
    }

    /// P2b: analyzed schema is cached per plan handle (`OnceLock`); repeated opens share one
    /// `SchemaRef` and never re-clone a fresh schema tree from analysis.
    #[test]
    fn analyzed_arrow_schema_native_caches_schema_ref_per_handle() {
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let dataframe = context
            .read_batch(int64_batch(&[1, 2, 3]))
            .expect("read_batch");
        let py_dataframe = PyDataFrame::new(dataframe, reader_test_runtime());
        let first = py_dataframe
            .analyzed_arrow_schema_native()
            .expect("first analyze");
        let second = py_dataframe
            .analyzed_arrow_schema_native()
            .expect("cached analyze");
        assert!(
            Arc::ptr_eq(&first, &second),
            "second analyzed_arrow_schema_native must return the same SchemaRef (OnceLock cache)"
        );
        // column_names also hits the cache path without building a second Schema tree.
        let names = py_dataframe.column_names().expect("column_names");
        assert_eq!(names, vec!["v".to_string()]);
        let third = py_dataframe
            .analyzed_arrow_schema_native()
            .expect("post-columns cache");
        assert!(Arc::ptr_eq(&first, &third));
    }

    #[test]
    fn arrow_c_stream_export_is_lazy_and_does_not_materialize_up_front() {
        // The END-TO-END laziness pin for `__arrow_c_stream__` itself (audit F-BR-4). The three
        // reader-level pins drive `StreamingBatchReader` directly and never call the dunder, and the
        // `bindings.rs` end-to-end pins assert value/type — so reverting `__arrow_c_stream__` to the
        // "stream export lie" (a full `collect()` then wrap) left the WHOLE suite green. This pin
        // drives the REAL dunder over a source that counts every batch it produces and proves NOTHING
        // is materialized until the consumer pulls.
        //
        // Deterministic by construction (rule 12 — no RSS thresholds, no sleeps, no timing races):
        // `DataFrame::execute_stream` returns the stream WITHOUT polling (`StreamingTableExec::execute`
        // hands back the un-polled partition stream), so a lazy export reads `produced == 0` while a
        // `collect()`-first export reads `produced == N`. `target_partitions = 1` forbids a
        // `RepartitionExec` that could pull the source on a background task.
        Python::attach(|python| {
            let produced = Arc::new(AtomicUsize::new(0));
            let batches = vec![
                int64_batch(&[1, 2]),
                int64_batch(&[3, 4]),
                int64_batch(&[5, 6]),
            ];
            let batch_count = batches.len();

            let context =
                SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
            let source = CountingPartitionStream {
                schema: int64_schema(),
                batches,
                produced: Arc::clone(&produced),
            };
            let provider = StreamingTable::try_new(int64_schema(), vec![Arc::new(source)])
                .expect("streaming table builds over the counting source");
            let dataframe = context
                .read_table(Arc::new(provider))
                .expect("read_table yields a DataFrame over the counting source");
            let py_dataframe = PyDataFrame::new(dataframe, reader_test_runtime());

            // Export the Arrow C stream. A LAZY dunder builds the stream and returns without polling;
            // the collect-then-wrap "lie" would drain all `batch_count` batches right here.
            let capsule = py_dataframe
                .__arrow_c_stream__(python, None)
                .expect("streaming export returns a capsule");
            assert_eq!(
                produced.load(Ordering::SeqCst),
                0,
                "LAZINESS: nothing is materialized at export time — a collect-then-wrap \
                 __arrow_c_stream__ would have drained all {batch_count} batches before returning"
            );

            // The exported stream is BOTH lazy and functional: draining it now yields every batch, in
            // order, with the right values — and only NOW is the source fully consumed.
            let reader = import_capsule_stream(&capsule);
            let drained: Vec<RecordBatch> =
                reader.map(|batch| batch.expect("batch decodes")).collect();
            let values: Vec<i64> = drained.iter().flat_map(int64_values).collect();
            assert_eq!(
                values,
                vec![1, 2, 3, 4, 5, 6],
                "every value crosses the streaming boundary, in order"
            );
            assert_eq!(
                produced.load(Ordering::SeqCst),
                batch_count,
                "the source is fully consumed only AFTER the consumer drains the stream"
            );
        });
    }

    #[test]
    fn streaming_reader_yields_first_batch_before_a_later_error() {
        // Laziness (the load-bearing streaming pin): a stream that ERRORS on its second poll must
        // still deliver batch 1. A reader that drained/collected the whole stream up front — the
        // "stream export lie" this replaces — would surface the batch-2 error before ANY batch.
        Python::attach(|_python| {
            let stream = scripted_stream(
                int64_schema(),
                vec![
                    Ok(int64_batch(&[1, 2])),
                    Err(DataFusionError::Execution("boom on batch 2".into())),
                ],
            );
            let mut reader = StreamingBatchReader {
                runtime: reader_test_runtime(),
                stream,
                schema: int64_schema(),
            };

            let first = reader
                .next()
                .expect("a first item is produced")
                .expect("batch 1 is Ok — delivered BEFORE the later error");
            assert_eq!(
                int64_values(&first),
                vec![1, 2],
                "batch 1 is yielded intact ahead of the batch-2 error"
            );

            let error = reader
                .next()
                .expect("a second item is produced")
                .expect_err("the second poll surfaces the stream error");
            assert!(
                error.to_string().contains("boom on batch 2"),
                "the DataFusion error text rides through ArrowError: {error}"
            );
        });
    }

    #[test]
    fn streaming_reader_preserves_multi_batch_values_and_schema() {
        // Correctness (value AND type, MULTI-batch): three batches stream out in order and
        // concatenate to the full expected values; the declared schema is the physical schema the
        // reader was given, and every batch keeps its Int64 type. The reader must not drop, merge,
        // reorder, or retype batches.
        Python::attach(|_python| {
            let stream = scripted_stream(
                int64_schema(),
                vec![
                    Ok(int64_batch(&[1, 2])),
                    Ok(int64_batch(&[3])),
                    Ok(int64_batch(&[4, 5, 6])),
                ],
            );
            let reader = StreamingBatchReader {
                runtime: reader_test_runtime(),
                stream,
                schema: int64_schema(),
            };

            assert_eq!(
                reader.schema(),
                int64_schema(),
                "declared schema is the physical schema (type surface)"
            );

            let batches: Vec<RecordBatch> = reader
                .map(|batch| batch.expect("each batch decodes"))
                .collect();
            assert_eq!(
                batches.len(),
                3,
                "all three batches stream through — none dropped or merged"
            );
            let concatenated: Vec<i64> = batches.iter().flat_map(int64_values).collect();
            assert_eq!(
                concatenated,
                vec![1, 2, 3, 4, 5, 6],
                "values preserved, in order, across batch boundaries"
            );
            for batch in &batches {
                assert_eq!(
                    batch.schema().field(0).data_type(),
                    &DataType::Int64,
                    "no retype across the streaming boundary"
                );
            }
        });
    }

    #[test]
    fn streaming_reader_surfaces_stream_error_with_message_preserved() {
        // Error surfacing: a DataFusion error becomes an `Err(ArrowError)` item (never swallowed to
        // `None`, never a panic), carrying the engine text, and the stream is exhausted after it.
        Python::attach(|_python| {
            let stream = scripted_stream(
                int64_schema(),
                vec![Err(DataFusionError::Execution("kaboom".into()))],
            );
            let mut reader = StreamingBatchReader {
                runtime: reader_test_runtime(),
                stream,
                schema: int64_schema(),
            };

            let error = reader
                .next()
                .expect("the error is delivered as an item, not None")
                .expect_err("a stream error maps to Err(ArrowError), never Ok");
            assert!(
                error.to_string().contains("kaboom"),
                "message preserved through ArrowError::ExternalError: {error}"
            );
            assert!(
                reader.next().is_none(),
                "the stream is exhausted after its single error"
            );
        });
    }

    /// A [`PartitionStream`] whose executed stream PANICS on its first poll — the SAF-007 injection
    /// for the FFI-callback abort path (the panicking analogue of [`CountingPartitionStream`]).
    #[derive(Debug)]
    struct PanicOnPollPartitionStream {
        schema: SchemaRef,
    }

    impl PartitionStream for PanicOnPollPartitionStream {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            // One item whose future panics when polled. The `Ok(..)` after the panic is unreachable
            // but pins the stream's item type to `Result<RecordBatch, DataFusionError>`.
            let panicking = futures::stream::once(async {
                panic!("SAF-007 injected stream-poll panic");
                #[allow(unreachable_code)]
                Ok(int64_batch(&[0]))
            });
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                panicking,
            ))
        }
    }

    /// The child half of the subprocess-isolated abort pin: export `__arrow_c_stream__` over a
    /// poll-panicking source and drain it through the REAL FFI reader (the `extern "C"` `get_next`
    /// callback). WITH `fence_stream_poll`, the panicking poll surfaces as a terminal
    /// `Err(ArrowError)` carrying the framed panic text and this returns normally (child exits 0).
    /// WITHOUT the fence, the poll panic unwinds across `extern "C"` and ABORTS this process.
    fn drive_panicking_stream_export_child() {
        Python::attach(|python| {
            let context =
                SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
            let source = PanicOnPollPartitionStream {
                schema: int64_schema(),
            };
            let provider = StreamingTable::try_new(int64_schema(), vec![Arc::new(source)])
                .expect("streaming table builds over the panicking source");
            let dataframe = context
                .read_table(Arc::new(provider))
                .expect("read_table yields a DataFrame over the panicking source");
            let py_dataframe = PyDataFrame::new(dataframe, reader_test_runtime());

            let capsule = py_dataframe
                .__arrow_c_stream__(python, None)
                .expect("export returns a capsule (execute_stream is lazy — no poll yet)");
            let mut reader = import_capsule_stream(&capsule);

            // The source panics on its FIRST poll, so the first item the FFI reader yields is the
            // fenced panic — a terminal `Err(ArrowError)`, never an abort.
            match reader.next() {
                Some(Ok(_)) => panic!("the panicking source must never yield a clean batch"),
                Some(Err(error)) => assert!(
                    error
                        .to_string()
                        .contains("SAF-007 injected stream-poll panic"),
                    "the fenced panic text rides the Arrow error channel: {error}"
                ),
                None => {
                    panic!("expected the fenced panic as an Arrow error item, got end-of-stream")
                }
            }
        });
    }

    #[test]
    fn arrow_stream_poll_panic_is_fenced_not_aborting_subprocess_isolated() {
        // PL-4 (SAF-007 / WG-3): a panic inside the Arrow C-stream `get_next` callback would unwind
        // across `extern "C"` and ABORT the interpreter (arrow-array-57.3.1 ffi_stream.rs has no
        // catch there, and PyO3's pymethod trampoline does not cover it — the pulls happen after
        // `__arrow_c_stream__` returned). `fence_stream_poll` turns that panic into a clean terminal
        // Arrow error. Because the mutation (removing the fence) genuinely ABORTS, the drain runs in
        // a re-exec of THIS test binary as a CHILD process: with the fence the child exits 0; with
        // the fence removed the child dies by SIGABRT and only the child dies — the parent survives
        // to observe RED. Deterministic (rule 12): no sleeps, no timing, no RSS.
        const CHILD_ENV: &str = "REPARK_SAF007_STREAM_CHILD";
        if std::env::var_os(CHILD_ENV).is_some() {
            drive_panicking_stream_export_child();
            return;
        }
        let exe = std::env::current_exe().expect("locate the current test binary");
        let output = std::process::Command::new(exe)
            .args([
                "--exact",
                "--nocapture",
                "dataframe::tests::arrow_stream_poll_panic_is_fenced_not_aborting_subprocess_isolated",
            ])
            .env(CHILD_ENV, "1")
            .output()
            .expect("spawn the isolated child process");
        assert!(
            output.status.success(),
            "with the fence, an FFI stream-poll panic becomes a clean Arrow error and the child \
             exits 0 (never aborts); got status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// E2 ndarray / array dtypes: shallow List element tokens stay Spark simpleString.
    #[test]
    fn arrow_type_key_list_element_simple_string_matches_spark() {
        let list_i8 = ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int8, true)));
        assert_eq!(arrow_type_key(&list_i8), "array<tinyint>");

        let nested = ArrowDataType::List(Arc::new(Field::new(
            "item",
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int32, true))),
            true,
        )));
        assert_eq!(arrow_type_key(&nested), "array<array<int>>");
    }

    /// octo C3-CRATE-001: deeply nested List must not stack-overflow; depth bound truncates.
    #[test]
    fn arrow_type_key_deep_list_nesting_is_depth_bounded() {
        // Far past ARROW_TYPE_KEY_MAX_DEPTH — without the bound this would blow the stack.
        let nest_levels = ARROW_TYPE_KEY_MAX_DEPTH.saturating_mul(4).max(128);
        let mut data_type = ArrowDataType::Int32;
        for _ in 0..nest_levels {
            data_type = ArrowDataType::List(Arc::new(Field::new("item", data_type, true)));
        }
        let key = arrow_type_key(&data_type);
        assert!(
            key.contains(ARROW_TYPE_KEY_DEPTH_FALLBACK),
            "deep List walk must hit the depth fallback, got {key:?}"
        );
        assert!(
            key.starts_with("array<"),
            "outer List still formats as array<…>, got {key:?}"
        );
        // Bound is finite: key length is O(max_depth), not O(nest_levels).
        assert!(
            key.len() < nest_levels * 8,
            "key must not grow linearly with adversarial depth (len={}, nest={nest_levels})",
            key.len()
        );
    }
}
