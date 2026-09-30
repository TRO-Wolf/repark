use std::cell::Cell;
use std::ffi::CStr;
use std::sync::{Arc, OnceLock};

use arrow::array::RecordBatchReader;
use arrow::datatypes::{DataType as ArrowDataType, SchemaRef};
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::util::pretty::pretty_format_batches;
use datafusion::common::JoinType;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::Expr;
use datafusion::prelude::col;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use tokio::runtime::Runtime;

use crate::arrow_export::StreamingBatchReader;
use crate::column::PyColumn;
use crate::deep_stack::{
    DEEP_NESTING_DEPTH, block_on_grown_sized, frame_drive_segment, refuse_expression_depth,
    run_grown_if, sql_drive_grown, stack_is_small,
};
use crate::fence::{fenced, fenced_span};
use crate::{datafusion_to_py_err, to_py_err};

const ARROW_STREAM_CAPSULE_NAME: &CStr = c"arrow_array_stream";

const ARROW_SCHEMA_CAPSULE_NAME: &CStr = c"arrow_schema";

thread_local! {
    pub(crate) static STREAM_POLL_NO_DETACH: Cell<bool> = const { Cell::new(false) };
}

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

#[pyclass(name = "PyDataFrame", module = "repark._native")]
pub struct PyDataFrame {
    pub(crate) df: DataFrame,
    pub(crate) runtime: Arc<Runtime>,
    analyzed_schema: OnceLock<SchemaRef>,
}

impl PyDataFrame {
    fn bound(&self, column: &PyColumn) -> PyResult<Expr> {
        crate::dataframe_names::bound_column(&self.df, column)
    }

    pub(crate) fn new(df: DataFrame, runtime: Arc<Runtime>) -> Self {
        Self {
            df,
            runtime,
            analyzed_schema: OnceLock::new(),
        }
    }

    pub(crate) fn inner(&self) -> &DataFrame {
        &self.df
    }

    pub(crate) fn runtime_handle(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) fn analyzed_arrow_schema_native(&self) -> PyResult<SchemaRef> {
        if let Some(schema) = self.analyzed_schema.get() {
            return Ok(Arc::clone(schema));
        }
        let segment = frame_drive_segment(&self.df)?;
        let (state, plan) = self.df.clone().into_parts();
        let analyzed = block_on_grown_sized(
            &self.runtime,
            async { repark_functions::analyze_eagerly(&state, plan) },
            segment,
        )
        .map_err(datafusion_to_py_err)?;
        let schema: SchemaRef = repark_core::strip_tighten_export_metadata(Arc::new(
            analyzed.schema().as_arrow().clone(),
        ));
        let _ = self.analyzed_schema.set(Arc::clone(&schema));
        Ok(self.analyzed_schema.get().map(Arc::clone).unwrap_or(schema))
    }
}

#[cfg(test)]
const ARROW_TYPE_KEY_MAX_DEPTH: usize = 32;

#[cfg(test)]
const ARROW_TYPE_KEY_DEPTH_FALLBACK: &str = "...";

fn arrow_type_key(data_type: &ArrowDataType) -> String {
    repark_spark::type_table::logical_type_key(data_type)
}

fn drive_columns<'a, T>(
    runtime: &Runtime,
    columns: impl IntoIterator<Item = &'a PyColumn>,
    build: impl FnOnce() -> PyResult<T>,
) -> PyResult<T> {
    let mut deepest = 0;
    for column in columns {
        deepest = deepest.max(column.expression_depth());
    }
    refuse_expression_depth(deepest)?;
    run_grown_if(
        runtime,
        deepest > DEEP_NESTING_DEPTH || stack_is_small(),
        build,
    )
}

#[allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::needless_pass_by_value
)]
#[pymethods]
impl PyDataFrame {
    #[allow(clippy::missing_errors_doc)]
    pub fn count(&self, py: Python<'_>) -> PyResult<usize> {
        fenced_span!("py.action", "PyDataFrame.count", {
            let segment = frame_drive_segment(&self.df)?;
            py.detach(|| block_on_grown_sized(&self.runtime, self.df.clone().count(), segment))
                .map_err(datafusion_to_py_err)
        })
    }

    #[allow(clippy::missing_errors_doc)]
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

    #[allow(clippy::missing_errors_doc)]
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

    #[allow(clippy::missing_errors_doc)]
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

    #[allow(clippy::missing_errors_doc)]
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

    #[allow(clippy::missing_errors_doc)]
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

    #[pyo3(signature = (n=20))]
    #[allow(clippy::missing_errors_doc)]
    pub fn show(&self, py: Python<'_>, n: usize) -> PyResult<String> {
        fenced_span!("py.action", "PyDataFrame.show", {
            let limited = self
                .df
                .clone()
                .limit(0, Some(n))
                .map_err(datafusion_to_py_err)?;
            let segment = frame_drive_segment(&limited)?;
            let batches = py.detach(|| {
                block_on_grown_sized(&self.runtime, limited.collect(), segment)
                    .map_err(datafusion_to_py_err)
            })?;
            pretty_format_batches(&batches)
                .map(|table| table.to_string())
                .map_err(|err| to_py_err(repark_core::Error::DataFusion(err.to_string())))
        })
    }

    #[allow(clippy::needless_pass_by_value)]
    #[pyo3(signature = (requested_schema=None))]
    #[allow(clippy::missing_errors_doc)]
    pub fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        fenced_span!("py.action", "PyDataFrame.__arrow_c_stream__", {
            let _ = requested_schema;
            let schema: SchemaRef = self.analyzed_arrow_schema_native()?;
            let schema = crate::arrow_export::coerced_export_schema(&schema);
            let segment = frame_drive_segment(&self.df)?;
            let grown = segment.is_some();
            let stream = py
                .detach(|| {
                    block_on_grown_sized(&self.runtime, self.df.clone().execute_stream(), segment)
                })
                .map_err(datafusion_to_py_err)?;
            let reader: Box<dyn RecordBatchReader + Send> = Box::new(
                StreamingBatchReader::new(Arc::clone(&self.runtime), stream, schema, grown)
                    .with_refusals(crate::arrow_export::refusal_log(&self.df)),
            );
            let ffi_stream = FFI_ArrowArrayStream::new(reader);

            PyCapsule::new_with_value_and_destructor(
                py,
                ffi_stream,
                ARROW_STREAM_CAPSULE_NAME,
                |ffi_stream, _ctx| drop(ffi_stream),
            )
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn with_column(&self, name: &str, column: PyColumn) -> PyResult<Self> {
        fenced!("PyDataFrame.with_column", {
            let df = drive_columns(&self.runtime, std::slice::from_ref(&column), || {
                self.df
                    .clone()
                    .with_column(name, self.bound(&column)?)
                    .map_err(datafusion_to_py_err)
            })?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn filter(&self, predicate: PyColumn) -> PyResult<Self> {
        fenced!("PyDataFrame.filter", {
            let df = drive_columns(&self.runtime, std::slice::from_ref(&predicate), || {
                self.df
                    .clone()
                    .filter(self.bound(&predicate)?)
                    .map_err(datafusion_to_py_err)
            })?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn filter_sql(&self, predicate: &str) -> PyResult<Self> {
        fenced!("PyDataFrame.filter_sql", {
            let grown = sql_drive_grown(predicate)?;
            let df = run_grown_if(&self.runtime, grown, || {
                crate::dataframe_names::filter_frame_with_sql(&self.df, predicate)
            })?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn select(&self, columns: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("PyDataFrame.select", {
            let df = drive_columns(&self.runtime, &columns, || {
                let expressions: Vec<Expr> = columns
                    .iter()
                    .map(|column| crate::dataframe_names::bound_projection(&self.df, column))
                    .collect::<PyResult<_>>()?;
                self.df
                    .clone()
                    .select(expressions)
                    .map_err(datafusion_to_py_err)
            })?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
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
            let df = drive_columns(&self.runtime, &columns, || {
                let sort_expressions = columns
                    .iter()
                    .zip(ascending)
                    .zip(nulls_first)
                    .map(|((column, is_ascending), nulls_first)| {
                        Ok(self.bound(column)?.sort(is_ascending, nulls_first))
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                self.df
                    .clone()
                    .sort(sort_expressions)
                    .map_err(datafusion_to_py_err)
            })?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn join_on_names(
        &self,
        right: PyRef<'_, PyDataFrame>,
        on: Vec<String>,
        how: &str,
    ) -> PyResult<Self> {
        fenced!("PyDataFrame.join_on_names", {
            let join_type = join_type_from_str(how)?;
            let df = crate::dataframe_names::join_on_keys(&self.df, &right.df, &on, join_type)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn join_on_condition(
        &self,
        right: PyRef<'_, PyDataFrame>,
        condition: PyColumn,
        how: &str,
    ) -> PyResult<Self> {
        fenced!("PyDataFrame.join_on_condition", {
            let join_type = join_type_from_str(how)?;
            let joined = drive_columns(&self.runtime, std::slice::from_ref(&condition), || {
                self.df
                    .clone()
                    .join_on(right.df.clone(), join_type, [self.bound(&condition)?])
                    .map_err(datafusion_to_py_err)
            })?;
            Ok(Self::new(joined, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn aggregate(&self, group_by: Vec<PyColumn>, aggregates: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("PyDataFrame.aggregate", {
            let columns = group_by.iter().chain(aggregates.iter());
            let df = drive_columns(&self.runtime, columns, || {
                let group_exprs: Vec<Expr> = group_by
                    .iter()
                    .map(|column| self.bound(column))
                    .collect::<PyResult<_>>()?;
                let aggregate_exprs: Vec<Expr> = aggregates
                    .iter()
                    .map(|column| self.bound(column))
                    .collect::<PyResult<_>>()?;
                self.df
                    .clone()
                    .aggregate(group_exprs, aggregate_exprs)
                    .map_err(datafusion_to_py_err)
            })?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn union(&self, other: PyRef<'_, PyDataFrame>, by_name: bool) -> PyResult<Self> {
        if by_name {
            return self.union_by_name(other, true);
        }
        fenced!("PyDataFrame.union", {
            let unioned = self
                .df
                .clone()
                .union(other.df.clone())
                .map_err(datafusion_to_py_err)?;
            Ok(Self::new(unioned, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn union_by_name(
        &self,
        other: PyRef<'_, PyDataFrame>,
        allow_missing: bool,
    ) -> PyResult<Self> {
        fenced!("PyDataFrame.union_by_name", {
            let unioned = crate::dataframe_names::union_frames(&self.df, &other.df, allow_missing)?;
            Ok(Self::new(unioned, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn distinct(&self) -> PyResult<Self> {
        fenced!("PyDataFrame.distinct", {
            let df = self.df.clone().distinct().map_err(datafusion_to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
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

    #[allow(clippy::missing_errors_doc)]
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

    #[allow(clippy::missing_errors_doc)]
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
mod tests;
