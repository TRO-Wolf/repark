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
use repark_core::frame_names::NameRule;
use tokio::runtime::Runtime;

use crate::arrow_export::StreamingBatchReader;
use crate::column::PyColumn;
use crate::column::expr_build::unresolved_sort_key;
use crate::deep_stack::{
    DEEP_NESTING_DEPTH, PlanDepths, block_on_grown_sized, clone_need_bytes, drive_segment_bytes,
    frame_drive_segment_cached, grown_clone_frame, grown_sync, max_depths, plan_depths,
    refuse_expression_depth, run_grown_if, sql_drive_grown, stack_is_small,
};
use crate::fence::{fenced, fenced_span};
use crate::frame_lineage::PyFrameNode;
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

pub(crate) fn join_type_from_str(how: &str) -> PyResult<JoinType> {
    match how {
        "inner" => Ok(JoinType::Inner),
        "left" | "left_outer" | "leftouter" => Ok(JoinType::Left),
        "right" | "right_outer" | "rightouter" => Ok(JoinType::Right),
        "full" | "outer" | "fullouter" | "full_outer" => Ok(JoinType::Full),
        "semi" | "left_semi" | "leftsemi" => Ok(JoinType::LeftSemi),
        "anti" | "left_anti" | "leftanti" => Ok(JoinType::LeftAnti),
        other => Err(PyValueError::new_err(crate::exceptions::mask_user_visible(
            format!(
                "unsupported join type {other:?} (supported: 'inner', 'left', 'right', 'full', \
             'leftsemi', 'leftanti')"
            ),
        ))),
    }
}

#[pyclass(name = "PyDataFrame", module = "repark._native")]
pub struct PyDataFrame {
    pub(crate) df: std::mem::ManuallyDrop<DataFrame>,
    pub(crate) runtime: Arc<Runtime>,
    analyzed_schema: OnceLock<SchemaRef>,
    executable: OnceLock<DataFrame>,
    rule: OnceLock<NameRule>,
    depths: PlanDepths,
}

impl Drop for PyDataFrame {
    fn drop(&mut self) {
        let need = clone_need_bytes(self.depths.plan, self.depths.expression);
        grown_sync(need, || {
            drop(self.executable.take());
            unsafe {
                std::mem::ManuallyDrop::drop(&mut self.df);
            }
        });
    }
}

impl PyDataFrame {
    fn bound(&self, column: &PyColumn) -> PyResult<(Expr, usize)> {
        crate::dataframe_names::bound_column(&self.df, self.rule(), column)
    }

    pub(crate) fn new(df: DataFrame, runtime: Arc<Runtime>) -> Self {
        let depths = crate::deep_stack::plan_depths(df.logical_plan());
        Self::new_with_depths(df, runtime, depths)
    }

    pub(crate) fn new_with_depths(
        df: DataFrame,
        runtime: Arc<Runtime>,
        depths: PlanDepths,
    ) -> Self {
        Self {
            df: std::mem::ManuallyDrop::new(df),
            runtime,
            analyzed_schema: OnceLock::new(),
            executable: OnceLock::new(),
            rule: OnceLock::new(),
            depths,
        }
    }

    pub(crate) fn derived(&self, df: DataFrame, depths: PlanDepths) -> Self {
        let child = Self::new_with_depths(df, Arc::clone(&self.runtime), depths);
        child.inherit_rule(self);
        child
    }

    pub(crate) fn inherit_rule(&self, parent: &Self) {
        if let Some(rule) = parent.rule.get() {
            debug_assert_eq!(*rule, crate::dataframe_names::frame_rule(&self.df));
            let _ = self.rule.set(*rule);
        }
    }

    pub(crate) fn rule(&self) -> NameRule {
        *self
            .rule
            .get_or_init(|| crate::dataframe_names::frame_rule(&self.df))
    }

    pub(crate) fn executable(&self) -> PyResult<DataFrame> {
        if let Some(twin) = self.executable.get() {
            return Ok(grown_clone_frame(twin, &self.depths));
        }
        let need = drive_segment_bytes(&self.depths).map_or_else(
            || clone_need_bytes(self.depths.plan, self.depths.expression),
            |bytes| bytes.max(clone_need_bytes(self.depths.plan, self.depths.expression)),
        );
        let (twin, handed) = grown_sync(need, || {
            let (state, plan) = grown_clone_frame(self.inner(), &self.depths).into_parts();
            repark_core::frame_names::strip_for_execution(plan).map(|stripped| {
                let twin = DataFrame::new(state, stripped);
                let handed = grown_clone_frame(&twin, &self.depths);
                (twin, handed)
            })
        })
        .map_err(datafusion_to_py_err)?;
        if let Err(lost) = self.executable.set(twin) {
            grown_sync(need, || drop(lost));
        }
        Ok(handed)
    }

    pub(crate) fn inner(&self) -> &DataFrame {
        &self.df
    }

    pub(crate) fn depths(&self) -> PlanDepths {
        self.depths
    }

    pub(crate) fn runtime_handle(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) fn analyzed_arrow_schema_native(&self) -> PyResult<SchemaRef> {
        if let Some(schema) = self.analyzed_schema.get() {
            return Ok(Arc::clone(schema));
        }
        let segment = frame_drive_segment_cached(&self.depths)?;
        let df = self.executable()?;
        let schema = block_on_grown_sized(
            &self.runtime,
            async {
                let (state, plan) = df.into_parts();
                let analyzed = repark_functions::analyze_eagerly(&state, plan);
                analyzed.map(|analyzed| {
                    repark_core::frame_names::strip_schema_ids(
                        repark_core::strip_tighten_export_metadata(Arc::new(
                            analyzed.schema().as_arrow().clone(),
                        )),
                    )
                })
            },
            segment,
        )
        .map_err(datafusion_to_py_err)?;
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
    frame: &PlanDepths,
    columns: impl IntoIterator<Item = &'a PyColumn>,
    build: impl FnOnce() -> PyResult<(T, usize)>,
) -> PyResult<(T, usize)> {
    let mut deepest_df = 0;
    let mut deepest_expr = 0;
    let mut deepest_plan = 0;
    for column in columns {
        deepest_df = deepest_df.max(column.df_depth());
        deepest_expr = deepest_expr.max(column.expression_depth());
        deepest_plan = deepest_plan.max(column.plan_depth());
    }
    refuse_expression_depth(deepest_df)?;
    run_grown_if(
        runtime,
        deepest_expr > DEEP_NESTING_DEPTH
            || deepest_plan > DEEP_NESTING_DEPTH
            || frame.plan > DEEP_NESTING_DEPTH
            || frame.expression > DEEP_NESTING_DEPTH
            || stack_is_small(),
        build,
    )
}

fn carries_subquery_plan<'a>(columns: impl IntoIterator<Item = &'a PyColumn>) -> bool {
    columns.into_iter().any(|column| column.plan_depth() > 0)
}

fn child_depths(
    frame: &PlanDepths,
    bound: usize,
    built: &DataFrame,
    carries_plan: bool,
) -> PlanDepths {
    if carries_plan {
        plan_depths(built.logical_plan())
    } else {
        grown_child(frame, bound)
    }
}

fn join_child_depths(
    left: &PlanDepths,
    right: &PlanDepths,
    bound: usize,
    built: &DataFrame,
    carries_plan: bool,
) -> PlanDepths {
    if carries_plan {
        plan_depths(built.logical_plan())
    } else {
        grown_join(left, right, bound)
    }
}

fn grown_child(frame: &PlanDepths, bound: usize) -> PlanDepths {
    PlanDepths {
        plan: frame.plan + 1,
        limited: frame.limited + 1,
        expression: frame.expression.max(bound),
    }
}

fn grown_join(left: &PlanDepths, right: &PlanDepths, bound: usize) -> PlanDepths {
    let frames = max_depths(left, right);
    PlanDepths {
        plan: frames.plan + 1,
        limited: frames.limited + 1,
        expression: frames.expression.max(bound),
    }
}

fn frame_clone_need(left: &PlanDepths, right: &PlanDepths) -> usize {
    let frames = max_depths(left, right);
    clone_need_bytes(frames.plan, frames.expression)
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
            crate::streaming_errors::refuse_streaming_action(py, self.inner())?;
            let segment = frame_drive_segment_cached(&self.depths)?;
            let twin = self.executable()?;
            py.detach(|| block_on_grown_sized(&self.runtime, twin.count(), segment))
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
                PyValueError::new_err(crate::exceptions::mask_user_visible(format!(
                    "failed to export analyzed Arrow schema to C Data Interface: {error}"
                )))
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
            let need = clone_need_bytes(self.depths.plan, self.depths.expression);
            let df = grown_sync(need, || {
                grown_clone_frame(self.inner(), &self.depths).limit(0, Some(n))
            })
            .map_err(datafusion_to_py_err)?;
            Ok(Self::new_with_depths(
                df,
                Arc::clone(&self.runtime),
                grown_child(&self.depths, 1),
            ))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn limit_with_skip(&self, skip: usize, fetch: Option<usize>) -> PyResult<Self> {
        fenced!("PyDataFrame.limit_with_skip", {
            let need = clone_need_bytes(self.depths.plan, self.depths.expression);
            let df = grown_sync(need, || {
                grown_clone_frame(self.inner(), &self.depths).limit(skip, fetch)
            })
            .map_err(datafusion_to_py_err)?;
            Ok(Self::new_with_depths(
                df,
                Arc::clone(&self.runtime),
                grown_child(&self.depths, 1),
            ))
        })
    }

    #[pyo3(signature = (n=20))]
    #[allow(clippy::missing_errors_doc)]
    pub fn show(&self, py: Python<'_>, n: usize) -> PyResult<String> {
        fenced_span!("py.action", "PyDataFrame.show", {
            crate::streaming_errors::refuse_streaming_action(py, self.inner())?;
            let need = clone_need_bytes(self.depths.plan, self.depths.expression);
            let twin = self.executable()?;
            let limited =
                grown_sync(need, || twin.limit(0, Some(n))).map_err(datafusion_to_py_err)?;
            let depths = grown_child(&self.depths, 1);
            let segment = frame_drive_segment_cached(&depths)?;
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
    #[pyo3(signature = (requested_schema=None, display_names=None))]
    #[allow(clippy::missing_errors_doc)]
    pub fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
        display_names: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        fenced_span!("py.action", "PyDataFrame.__arrow_c_stream__", {
            crate::streaming_errors::refuse_streaming_action(py, self.inner())?;
            let _ = requested_schema;
            let schema: SchemaRef = self.analyzed_arrow_schema_native()?;
            let schema = crate::arrow_export::coerced_export_schema(&schema);
            let (schema, rename) = match display_names {
                Some(names) if names.len() == schema.fields().len() => (
                    crate::arrow_export::renamed_export_schema(&schema, &names),
                    true,
                ),
                _ => (schema, false),
            };
            let segment = frame_drive_segment_cached(&self.depths)?;
            let grown = segment.is_some();
            let twin = self.executable()?;
            let stream = py
                .detach(|| block_on_grown_sized(&self.runtime, twin.execute_stream(), segment))
                .map_err(datafusion_to_py_err)?;
            let reader: Box<dyn RecordBatchReader + Send> = Box::new(
                StreamingBatchReader::new(Arc::clone(&self.runtime), stream, schema, grown)
                    .with_refusals(crate::arrow_export::refusal_log(&self.df))
                    .with_renamed_batches(rename),
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
            let carries_plan = carries_subquery_plan(std::slice::from_ref(&column));
            let (df, bound) = drive_columns(
                &self.runtime,
                &self.depths,
                std::slice::from_ref(&column),
                || {
                    let (bound, depth) = self.bound(&column)?;
                    let df = grown_clone_frame(self.inner(), &self.depths)
                        .with_column(name, bound)
                        .map_err(datafusion_to_py_err)?;
                    Ok((df, depth))
                },
            )?;
            let depths = child_depths(&self.depths, bound + 1, &df, carries_plan);
            Ok(self.derived(df, depths))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn filter(&self, predicate: PyColumn) -> PyResult<Self> {
        fenced!("PyDataFrame.filter", {
            let carries_plan = carries_subquery_plan(std::slice::from_ref(&predicate));
            let ((df, expanded), bound) = drive_columns(
                &self.runtime,
                &self.depths,
                std::slice::from_ref(&predicate),
                || {
                    let (bound, depth) = self.bound(&predicate)?;
                    let (df, expanded) =
                        crate::is_duplicated::filter_frame(self.inner(), &self.depths, bound)?;
                    Ok(((df, expanded), depth))
                },
            )?;
            let depths = child_depths(&self.depths, bound, &df, carries_plan || expanded);
            Ok(self.derived(df, depths))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn filter_sql(&self, predicate: &str) -> PyResult<Self> {
        fenced!("PyDataFrame.filter_sql", {
            let grown = sql_drive_grown(predicate)
                || self.depths.plan > DEEP_NESTING_DEPTH
                || self.depths.expression > DEEP_NESTING_DEPTH;
            let (df, bound, carries_plan) = run_grown_if(&self.runtime, grown, || {
                crate::dataframe_names::filter_frame_with_sql(self, predicate)
            })?;
            let depths = child_depths(&self.depths, bound, &df, carries_plan);
            Ok(self.derived(df, depths))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn select(&self, columns: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("PyDataFrame.select", {
            let carries_plan = carries_subquery_plan(&columns);
            let ((df, expanded), bound) =
                drive_columns(&self.runtime, &self.depths, &columns, || {
                    let rule = self.rule();
                    let mut deepest = 0;
                    let mut expressions = Vec::with_capacity(columns.len());
                    for column in &columns {
                        let (bound, depth) =
                            crate::dataframe_names::bound_projection(&self.df, rule, column)?;
                        deepest = deepest.max(depth);
                        expressions.push(bound);
                    }
                    let (df, expanded) = crate::is_duplicated::select_frame(
                        self.inner(),
                        &self.depths,
                        expressions,
                    )?;
                    Ok(((df, expanded), deepest))
                })?;
            let depths = child_depths(&self.depths, bound, &df, carries_plan || expanded);
            Ok(self.derived(df, depths))
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
                return Err(PyValueError::new_err(crate::exceptions::mask_user_visible(
                    "sort expects columns, ascending, and nulls_first vectors of equal length",
                )));
            }
            let carries_plan = carries_subquery_plan(&columns);
            let (df, bound) = drive_columns(&self.runtime, &self.depths, &columns, || {
                let bounds = columns
                    .iter()
                    .map(|column| self.bound(column))
                    .collect::<PyResult<Vec<_>>>()?;
                let deepest = bounds.iter().map(|(_, depth)| *depth).max().unwrap_or(0);
                let sort_expressions = bounds
                    .into_iter()
                    .zip(ascending)
                    .zip(nulls_first)
                    .map(|((bound, is_ascending), nulls_first)| {
                        bound.0.sort(is_ascending, nulls_first)
                    })
                    .collect::<Vec<_>>();
                let df = grown_clone_frame(self.inner(), &self.depths)
                    .sort(sort_expressions)
                    .map_err(|error| {
                        datafusion_to_py_err(unresolved_sort_key(error, self.df.schema()))
                    })?;
                Ok((df, deepest))
            })?;
            let depths = child_depths(&self.depths, bound, &df, carries_plan);
            Ok(self.derived(df, depths))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn join_on_names(
        &self,
        right: PyRef<'_, PyDataFrame>,
        on: Vec<String>,
        how: &str,
        left_node: &PyFrameNode,
        right_node: &PyFrameNode,
    ) -> PyResult<(Self, PyFrameNode)> {
        fenced!("PyDataFrame.join_on_names", {
            let join_type = join_type_from_str(how)?;
            let need = frame_clone_need(&self.depths, &right.depths());
            let (df, node) = grown_sync(need, || {
                crate::dataframe_names::join_on_keys(
                    self, &right, &on, join_type, left_node, right_node,
                )
            })?;
            Ok((Self::new(df, Arc::clone(&self.runtime)), node))
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
            let frames = max_depths(&self.depths, &right.depths());
            let carries_plan = carries_subquery_plan(std::slice::from_ref(&condition));
            let (joined, bound) = drive_columns(
                &self.runtime,
                &frames,
                std::slice::from_ref(&condition),
                || {
                    let (bound, depth) = self.bound(&condition)?;
                    let joined = grown_clone_frame(self.inner(), &self.depths)
                        .join_on(
                            grown_clone_frame(right.inner(), &right.depths()),
                            join_type,
                            [bound],
                        )
                        .map_err(datafusion_to_py_err)?;
                    Ok((joined, depth))
                },
            )?;
            let depths =
                join_child_depths(&self.depths, &right.depths(), bound, &joined, carries_plan);
            Ok(Self::new_with_depths(
                joined,
                Arc::clone(&self.runtime),
                depths,
            ))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn aggregate(&self, group_by: Vec<PyColumn>, aggregates: Vec<PyColumn>) -> PyResult<Self> {
        fenced!("PyDataFrame.aggregate", {
            let carries_plan = carries_subquery_plan(group_by.iter().chain(aggregates.iter()));
            let columns = group_by.iter().chain(aggregates.iter());
            let (df, bound) = drive_columns(&self.runtime, &self.depths, columns, || {
                let mut deepest = 0;
                let mut group_exprs = Vec::with_capacity(group_by.len());
                for column in &group_by {
                    let (bound, depth) = self.bound(column)?;
                    deepest = deepest.max(depth);
                    group_exprs.push(bound);
                }
                let mut aggregate_exprs = Vec::with_capacity(aggregates.len());
                for column in &aggregates {
                    let (bound, depth) = self.bound(column)?;
                    deepest = deepest.max(depth);
                    aggregate_exprs.push(bound);
                }
                let df = grown_clone_frame(self.inner(), &self.depths)
                    .aggregate(group_exprs, aggregate_exprs)
                    .map_err(datafusion_to_py_err)?;
                Ok((df, deepest))
            })?;
            let depths = child_depths(&self.depths, bound, &df, carries_plan);
            Ok(self.derived(df, depths))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn union(&self, other: PyRef<'_, PyDataFrame>, by_name: bool) -> PyResult<Self> {
        if by_name {
            return self.union_by_name(other, true);
        }
        fenced!("PyDataFrame.union", {
            let frames = max_depths(&self.depths, &other.depths());
            let need = clone_need_bytes(frames.plan, frames.expression);
            let unioned = grown_sync(need, || {
                grown_clone_frame(self.inner(), &self.depths)
                    .union(grown_clone_frame(other.inner(), &other.depths()))
            })
            .map_err(datafusion_to_py_err)?;
            Ok(Self::new_with_depths(
                unioned,
                Arc::clone(&self.runtime),
                PlanDepths {
                    plan: frames.plan + 1,
                    limited: frames.limited,
                    expression: frames.expression,
                },
            ))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn union_by_name(
        &self,
        other: PyRef<'_, PyDataFrame>,
        allow_missing: bool,
    ) -> PyResult<Self> {
        fenced!("PyDataFrame.union_by_name", {
            let need = frame_clone_need(&self.depths, &other.depths());
            let unioned = grown_sync(need, || {
                crate::dataframe_names::union_frames(self, &other, allow_missing)
            })?;
            Ok(Self::new(unioned, Arc::clone(&self.runtime)))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn distinct(&self) -> PyResult<Self> {
        fenced!("PyDataFrame.distinct", {
            let need = clone_need_bytes(self.depths.plan, self.depths.expression);
            let df = grown_sync(need, || {
                grown_clone_frame(self.inner(), &self.depths).distinct()
            })
            .map_err(datafusion_to_py_err)?;
            Ok(Self::new_with_depths(
                df,
                Arc::clone(&self.runtime),
                grown_child(&self.depths, 0),
            ))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn distinct_on(&self, subset: Vec<String>) -> PyResult<Self> {
        fenced!("PyDataFrame.distinct_on", {
            let need = clone_need_bytes(self.depths.plan, self.depths.expression);
            let df = grown_sync(need, || {
                let on_exprs: Vec<Expr> = subset.iter().map(col).collect();
                let select_exprs: Vec<Expr> = self
                    .df
                    .schema()
                    .iter()
                    .map(|(_qualifier, field)| col(field.name()))
                    .collect();
                grown_clone_frame(self.inner(), &self.depths).distinct_on(
                    on_exprs,
                    select_exprs,
                    None,
                )
            })
            .map_err(datafusion_to_py_err)?;
            Ok(Self::new_with_depths(
                df,
                Arc::clone(&self.runtime),
                grown_child(&self.depths, 1),
            ))
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn with_column_renamed(&self, old_name: &str, new_name: &str) -> PyResult<Self> {
        fenced!("PyDataFrame.with_column_renamed", {
            let need = clone_need_bytes(self.depths.plan, self.depths.expression);
            let df = grown_sync(need, || {
                grown_clone_frame(self.inner(), &self.depths)
                    .with_column_renamed(old_name, new_name)
            })
            .map_err(datafusion_to_py_err)?;
            Ok(Self::new_with_depths(
                df,
                Arc::clone(&self.runtime),
                grown_child(&self.depths, 2),
            ))
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
            let need = clone_need_bytes(self.depths.plan, self.depths.expression);
            let df = grown_sync(need, || {
                repark_core::dynamic_flatten(grown_clone_frame(self.inner(), &self.depths), options)
            })
            .map_err(to_py_err)?;
            Ok(Self::new(df, Arc::clone(&self.runtime)))
        })
    }
}
#[cfg(test)]
mod tests;
