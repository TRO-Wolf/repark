use std::sync::Arc;

use pyo3::prelude::*;
use repark_core::writer_layout::{
    TableWriteRequest, WriterAction, WriterLayout, WriterRefusal, missing_column_message,
    missing_column_name,
};

use crate::AnalysisException;
use crate::dataframe::PyDataFrame;
use crate::fence::fenced;
use crate::session::PyReparkSession;

type LayoutArgs = (Vec<String>, Option<i64>, Vec<String>, Vec<String>);

fn missing_column_error(py: Python<'_>, column: &str, tree: &str) -> PyErr {
    let raised = AnalysisException::new_err(crate::exceptions::mask_user_visible(
        missing_column_message(column, tree),
    ));
    let value = raised.value(py);
    let name = missing_column_name(column);
    let params =
        crate::exceptions::masked_message_params(py, &[("i", name.as_str()), ("schema", tree)]);
    if let Err(failure) = value.setattr("_spark_error_class", "_LEGACY_ERROR_TEMP_3060") {
        tracing::warn!(error = %failure, "writer plan condition setattr failed");
    }
    if let Err(failure) = value.setattr("_spark_message_parameters", params) {
        tracing::warn!(error = %failure, "writer plan params setattr failed");
    }
    raised
}

#[allow(clippy::missing_errors_doc)]
#[must_use]
#[pyfunction]
pub fn writer_target_is_table(target: &str, explicit_format: bool) -> bool {
    repark_core::writer_layout::save_target_names_table(target, explicit_format)
}

#[allow(clippy::missing_errors_doc, clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (session, action, target, qualified, mode, explicit_format, frame_schema, layout))]
pub fn writer_plan(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    action: &str,
    target: &str,
    qualified: Option<&str>,
    mode: &str,
    explicit_format: bool,
    frame_schema: &Bound<'_, PyAny>,
    layout: LayoutArgs,
) -> PyResult<&'static str> {
    fenced!("writer_layout.writer_plan", {
        let action = if action == "save" {
            WriterAction::Save
        } else {
            WriterAction::SaveAsTable
        };
        let schema = crate::cdf_infer::read_schema(frame_schema)?.ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(crate::exceptions::mask_user_visible(
                "writer plan needs a schema",
            ))
        })?;
        let frame_columns: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let (partition_columns, num_buckets, bucket_columns, sort_columns) = layout;
        let layout = WriterLayout {
            partition_columns,
            num_buckets,
            bucket_columns,
            sort_columns,
        };
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        let case_sensitive = repark_functions::case_sensitive::spark_case_sensitive_from_options(
            inner.context().copied_config().options(),
        );
        let request = TableWriteRequest {
            action,
            target,
            qualified,
            mode,
            explicit_format,
            layout: &layout,
            frame_columns: &frame_columns,
            case_sensitive,
        };
        let planned = py.detach(|| runtime.block_on(inner.plan_table_write(&request)));
        match planned {
            Ok(statement) => Ok(statement.as_str()),
            Err(WriterRefusal::MissingBucketColumn(column)) => Err(missing_column_error(
                py,
                &column,
                &repark_spark::spark_tree_string::spark_tree_string(&schema),
            )),
            Err(WriterRefusal::Spark(error)) => Err(crate::to_py_err(error)),
        }
    })
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
pub fn hive_partition_frame(frame: &PyDataFrame, columns: Vec<String>) -> PyResult<PyDataFrame> {
    fenced!("writer_layout.hive_partition_frame", {
        let depths = frame.depths();
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        let df = crate::deep_stack::grown_sync(need, || {
            repark_core::writer_layout::hive_partition_frame(
                crate::deep_stack::grown_clone_frame(frame.inner(), &frame.depths()),
                &columns,
            )
        })
        .map_err(crate::datafusion_to_py_err)?;
        Ok(PyDataFrame::new(df, frame.runtime_handle()))
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
pub fn respell_hive_partition_dirs(py: Python<'_>, root: &str, depth: usize) -> PyResult<()> {
    fenced!("writer_layout.respell_hive_partition_dirs", {
        py.detach(|| {
            repark_core::writer_layout::respell_local_partition_dirs(
                std::path::Path::new(root),
                depth,
            )
        })
        .map_err(crate::to_py_err)
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(writer_plan, module)?)?;
    module.add_function(wrap_pyfunction!(writer_target_is_table, module)?)?;
    module.add_function(wrap_pyfunction!(hive_partition_frame, module)?)?;
    module.add_function(wrap_pyfunction!(respell_hive_partition_dirs, module)?)?;
    Ok(())
}
