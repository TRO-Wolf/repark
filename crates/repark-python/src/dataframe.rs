//! Python-facing wrapper over a DataFusion [`DataFrame`].

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

/// Arrow C Data Interface schema capsule name used by the `PyCapsule` protocol.
const ARROW_SCHEMA_CAPSULE_NAME: &CStr = c"arrow_schema";

// See `with_stream_poll_no_detach`; this flag covers nested Python-backed stream polls.
thread_local! {
    static STREAM_POLL_NO_DETACH: Cell<bool> = const { Cell::new(false) };
}

/// Run `body` with nested polls skipping PyO3 attach/detach.
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

/// Map a PySpark join keyword to a DataFusion join type.
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

/// Whether a join returns only the left input columns.
fn join_keeps_only_left_columns(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti)
}

/// Build Spark's one-key-column projection for a named equi-join.
fn spark_join_projection(joined: &DataFrame, keys: &[String]) -> Vec<Expr> {
    let mut projection = Vec::new();
    let mut seen_keys = HashSet::new();
    for (qualifier, field) in joined.schema().iter() {
        let name = field.name();
        let is_key = keys.iter().any(|key| key == name);
        if is_key && !seen_keys.insert(name.clone()) {
            // Spark exposes one copy of each join key.
            continue;
        }
        projection.push(Expr::Column(Column::new(qualifier.cloned(), name.clone())));
    }
    projection
}

/// The Python-facing immutable `DataFrame` plan and its shared runtime.
#[pyclass(name = "PyDataFrame", module = "repark._native")]
pub struct PyDataFrame {
    df: DataFrame,
    runtime: Arc<Runtime>,
    /// Cached analyzed Arrow schema.
    analyzed_schema: OnceLock<SchemaRef>,
}

impl PyDataFrame {
    /// Bind a column's lambda variables to this frame schema before planning.
    fn bound(&self, column: &PyColumn) -> PyResult<Expr> {
        column
            .expr()
            .resolve_lambda_variables(self.df.schema())
            .map(|transformed| transformed.data)
            .map_err(datafusion_to_py_err)
    }

    /// Wrap a planned [`DataFrame`].
    pub(crate) fn new(df: DataFrame, runtime: Arc<Runtime>) -> Self {
        Self {
            df,
            runtime,
            analyzed_schema: OnceLock::new(),
        }
    }

    /// The held plan for session operations and ML streams.
    pub(crate) fn inner(&self) -> &DataFrame {
        &self.df
    }

    /// Shared Tokio runtime handle for asynchronous engine calls.
    pub(crate) fn runtime_handle(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }

    /// Post-analysis Arrow schema without executing the plan (metadata only).
    fn analyzed_arrow_schema_native(&self) -> PyResult<SchemaRef> {
        if let Some(schema) = self.analyzed_schema.get() {
            return Ok(Arc::clone(schema));
        }
        let (state, plan) = self.df.clone().into_parts();
        let analyzed =
            repark_functions::analyze_eagerly(&state, plan).map_err(datafusion_to_py_err)?;
        let schema: SchemaRef = repark_core::strip_tighten_export_metadata(Arc::new(
            analyzed.schema().as_arrow().clone(),
        ));
        // First writer wins under concurrent first-touch; losers drop their compute result.
        let _ = self.analyzed_schema.set(Arc::clone(&schema));
        Ok(self.analyzed_schema.get().map(Arc::clone).unwrap_or(schema))
    }
}

/// Max nesting depth for Arrow list/map type-key formatting.
const ARROW_TYPE_KEY_MAX_DEPTH: usize = 32;

/// Terminal token when [`ARROW_TYPE_KEY_MAX_DEPTH`] is exhausted.
const ARROW_TYPE_KEY_DEPTH_FALLBACK: &str = "...";

/// Map an Arrow data type onto a short repark facade type key for `StructType` construction.
fn arrow_type_key(data_type: &ArrowDataType) -> String {
    arrow_type_key_at_depth(data_type, 0)
}

/// Depth-bounded implementation of [`arrow_type_key`].
fn arrow_type_key_at_depth(data_type: &ArrowDataType, depth: usize) -> String {
    if depth >= ARROW_TYPE_KEY_MAX_DEPTH {
        return ARROW_TYPE_KEY_DEPTH_FALLBACK.to_string();
    }
    match data_type {
        // Collapse top-level integer and float widths to Spark's `int` and `double` keys.
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
        ArrowDataType::Timestamp(_, None) => "timestamp_ntz".to_string(),
        ArrowDataType::Timestamp(_, Some(_)) => "timestamp".to_string(),
        ArrowDataType::Decimal128(precision, scale)
        | ArrowDataType::Decimal256(precision, scale) => {
            format!("decimal({precision},{scale})")
        }
        // List variants use Spark's `array<element>` syntax.
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
        // Nested structs use Spark's field-name and child-type syntax, not Debug formatting.
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

/// Spark `simpleString` element token for nested array and map keys; depth-bounded.
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
        ArrowDataType::Timestamp(_, None) => "timestamp_ntz".to_string(),
        ArrowDataType::Timestamp(_, Some(_)) => "timestamp".to_string(),
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

/// A synchronous reader that polls one DataFusion batch per Arrow C Stream callback.
struct StreamingBatchReader {
    runtime: Arc<Runtime>,
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
}

impl Iterator for StreamingBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    /// Pull exactly one batch.
    fn next(&mut self) -> Option<Self::Item> {
        // The Arrow callback cannot unwind across extern "C"; fence the poll and report an error.
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
    /// The exported stream uses the analyzed logical types and Spark-style `nullable = true`.
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// Transform methods return a fresh `PyDataFrame`; pyclass args arrive by value from Python.
#[allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::needless_pass_by_value
)]
#[pymethods]
impl PyDataFrame {
    /// Number of rows (PySpark `DataFrame.count`).
    /// # Errors
    /// Returns `RuntimeError` if the engine fails to execute the count.
    pub fn count(&self, py: Python<'_>) -> PyResult<usize> {
        fenced_span!("py.action", "PyDataFrame.count", {
            py.detach(|| self.runtime.block_on(self.df.clone().count()))
                .map_err(datafusion_to_py_err)
        })
    }

    /// Column names from the **analyzed** logical schema — **no plan execution**.
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

    /// Post-analysis Arrow schema as an Arrow C Data Interface `PyCapsule`.
    /// # Errors
    /// Returns an engine exception on analysis failure, or `ValueError` if schema export fails.
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
    /// # Errors
    /// Returns a classified engine exception for physical-plan failures or `RuntimeError`.
    // `requested_schema` is part of the Arrow PyCapsule protocol signature.
    #[allow(clippy::needless_pass_by_value)]
    #[pyo3(signature = (requested_schema=None))]
    pub fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        fenced_span!("py.action", "PyDataFrame.__arrow_c_stream__", {
            let _ = requested_schema;
            // Use analyzed logical types with Spark nullability.
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

            // pyo3 0.29 renamed the destructor helper; pass a static `CStr` name.
            PyCapsule::new_with_value_and_destructor(
                py,
                ffi_stream,
                ARROW_STREAM_CAPSULE_NAME,
                |ffi_stream, _ctx| drop(ffi_stream),
            )
        })
    }

    /// Add or replace a column (PySpark `DataFrame.withColumn`).
    /// # Errors
    /// Returns `RuntimeError` if the resulting plan cannot be built (e.g.
    pub fn with_column(&self, name: &str, column: PyColumn) -> PyResult<Self> {
        fenced!("PyDataFrame.with_column", {
            let df = self
                .df
                .clone()
                .with_column(name, self.bound(&column)?)
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Keep rows matching a [`PyColumn`] predicate.
    /// # Errors
    /// Returns `RuntimeError` if the predicate cannot be planned.
    pub fn filter(&self, predicate: PyColumn) -> PyResult<Self> {
        fenced!("PyDataFrame.filter", {
            let df = self
                .df
                .clone()
                .filter(self.bound(&predicate)?)
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Keep only rows matching a SQL-string predicate (PySpark `DataFrame.filter("a > 1")`).
    /// # Errors
    /// Returns `RuntimeError` if the predicate does not parse or cannot be planned.
    pub fn filter_sql(&self, predicate: &str) -> PyResult<Self> {
        fenced!("PyDataFrame.filter_sql", {
            // This path bypasses the statement router, so apply its COLLATE refusal here.
            repark_spark::refuse_collation_in_sql(predicate).map_err(datafusion_to_py_err)?;
            let expr = self
                .df
                .parse_sql_expr(predicate)
                .map_err(datafusion_to_py_err)?;
            let df = self.df.clone().filter(expr).map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Project to the given columns/expressions (PySpark `DataFrame.select`).
    /// # Errors
    /// Returns `RuntimeError` if the projection cannot be planned.
    pub fn select(&self, columns: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("PyDataFrame.select", {
            let expressions: Vec<Expr> = columns
                .iter()
                .map(|column| self.bound(column))
                .collect::<PyResult<_>>()?;
            let df = self
                .df
                .clone()
                .select(expressions)
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Drop columns by name (PySpark `DataFrame.drop`).
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

    /// Order rows by the given columns (PySpark `DataFrame.orderBy` / `sort`).
    /// # Errors
    /// Returns `ValueError` on vector length mismatch; plan failures use the engine classifier.
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
                    Ok(self.bound(column)?.sort(is_ascending, nulls_first))
                })
                .collect::<PyResult<Vec<_>>>()?;
            let df = self
                .df
                .clone()
                .sort(sort_expressions)
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Equi-join on shared column names (PySpark `df.join(other, on=<name|list>, how=…)`).
    /// # Errors
    /// Returns `ValueError` for unsupported `how`; join failures use the engine classifier.
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
    /// # Errors
    /// Returns `ValueError` for unsupported `how`; binding failures use the engine classifier.
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
                .join_on(right.df.clone(), join_type, [self.bound(&condition)?])
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(joined, Arc::clone(&self.runtime)))
        })
    }

    /// Group + aggregate (PySpark `GroupedData.agg`; a global aggregate when `group_by` is empty).
    /// # Errors
    /// Returns `RuntimeError` if the aggregate cannot be planned (e.g.
    pub fn aggregate(&self, group_by: Vec<PyColumn>, aggregates: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("PyDataFrame.aggregate", {
            let group_exprs: Vec<Expr> = group_by
                .iter()
                .map(|column| self.bound(column))
                .collect::<PyResult<_>>()?;
            let aggregate_exprs: Vec<Expr> = aggregates
                .iter()
                .map(|column| self.bound(column))
                .collect::<PyResult<_>>()?;
            let df = self
                .df
                .clone()
                .aggregate(group_exprs, aggregate_exprs)
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Set-union with `other` (PySpark `DataFrame.union` / `unionAll` / `unionByName`).
    /// # Errors
    /// Returns `RuntimeError` if the two frames cannot be unioned (e.g.
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
    /// # Errors
    /// Returns `RuntimeError` if the distinct cannot be planned.
    pub fn distinct(&self) -> PyResult<Self> {
        fenced!("PyDataFrame.distinct", {
            let df = self.df.clone().distinct().map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    /// Distinct rows keyed on a **subset** of columns (PySpark `dropDuplicates(subset)`).
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

    /// Rename a column (PySpark `DataFrame.withColumnRenamed`).
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

    /// Recursively flatten nested structs (and optionally explode lists) — repark extra.
    /// # Errors
    /// Returns `AnalysisException` on name collision, empty-struct schema, or max-depth exhaustion.
    pub fn dynamic_flatten(
        &self,
        separator: String,
        explode_lists: bool,
        drop_null_lists: bool,
        empty_as_null: bool,
        max_depth: usize,
    ) -> PyResult<Self> {
        fenced!("PyDataFrame.dynamic_flatten", {
            let options = repark_core::DynamicFlattenOptions {
                separator,
                explode_lists,
                drop_null_lists,
                empty_as_null,
                max_depth,
            };
            let df = repark_core::dynamic_flatten(self.df.clone(), options).map_err(to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }
}

#[cfg(test)]
mod tests {
    //! Arrow export tests pin values, types, lazy execution, and errors.

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

    /// A panic in the nested-poll guard restores the TLS flag for later export polls.
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

    /// Wrap scripted batch results in a [`SendableRecordBatchStream`].
    fn scripted_stream(
        schema: SchemaRef,
        items: Vec<Result<RecordBatch, DataFusionError>>,
    ) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(items),
        ))
    }

    /// A private runtime used to drive reader polls in isolation.
    fn reader_test_runtime() -> Arc<Runtime> {
        Arc::new(Runtime::new().expect("tokio runtime builds"))
    }

    /// A partition stream that counts batches only when a consumer polls them.
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
            // `inspect` runs per yielded item, i.e.
            let counted = futures::stream::iter(items).inspect(move |_batch| {
                produced.fetch_add(1, Ordering::SeqCst);
            });
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                counted,
            ))
        }
    }

    /// Move the `FFI_ArrowArrayStream` out of a capsule and wrap it as a reader.
    fn import_capsule_stream(capsule: &Bound<'_, PyCapsule>) -> ArrowArrayStreamReader {
        let pointer = capsule
            .pointer_checked(Some(ARROW_STREAM_CAPSULE_NAME))
            .expect("capsule pointer is valid for the arrow stream name")
            .as_ptr()
            .cast::<FFI_ArrowArrayStream>();
        // SAFETY: `__arrow_c_stream__` placed a valid, initialized `FFI_ArrowArrayStream` here.
        let stream = unsafe { FFI_ArrowArrayStream::from_raw(pointer) };
        ArrowArrayStreamReader::try_new(stream).expect("stream is a valid Arrow C stream")
    }

    /// Analyzed schema is cached per plan handle.
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
        // The source counter proves that export opens lazily and polls only on consumption.
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

            // Export the Arrow C stream.
            let capsule = py_dataframe
                .__arrow_c_stream__(python, None)
                .expect("streaming export returns a capsule");
            assert_eq!(
                produced.load(Ordering::SeqCst),
                0,
                "LAZINESS: nothing is materialized at export time — a collect-then-wrap \
                 __arrow_c_stream__ would have drained all {batch_count} batches before returning"
            );

            // The exported stream is lazy: drain yields every batch, then the source is consumed.
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
        // Laziness: a stream that ERRORS on its second poll must still deliver batch 1.
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
        // Correctness: three batches stream in order and concatenate to expected values and types.
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
        // A DataFusion error becomes an `Err(ArrowError)` item, never swallowed and never a panic.
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

    /// A partition stream whose first poll panics, exercising the FFI callback fence.
    #[derive(Debug)]
    struct PanicOnPollPartitionStream {
        schema: SchemaRef,
    }

    impl PartitionStream for PanicOnPollPartitionStream {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            // The unreachable Ok value fixes the stream item type.
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

    /// Drive a poll-panicking export through the FFI reader and verify the fence returns an error.
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

            // The first poll returns the fenced panic as an Arrow error.
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
        // The callback cannot unwind across extern "C".
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

    /// Shallow list element tokens retain Spark `simpleString` formatting.
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

    /// Deeply nested List types stop at the configured depth bound.
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
