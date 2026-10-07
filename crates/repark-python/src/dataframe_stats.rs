use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::dataframe::PyDataFrame;
use crate::deep_stack::{block_on_grown_sized, frame_drive_segment_cached};
use crate::exceptions::AnalysisException;
use crate::fence::fenced;
use crate::to_py_err;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(freq_items, module)?)?;
    module.add_function(wrap_pyfunction!(transpose, module)?)?;
    Ok(())
}

#[pyfunction]
fn freq_items(
    frame: &PyDataFrame,
    column_names: Vec<String>,
    capacity: usize,
) -> PyResult<PyDataFrame> {
    fenced!("freq_items", {
        let depths = frame.depths();
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        let df = crate::deep_stack::grown_sync(need, || {
            repark_core::freq_items(
                crate::deep_stack::grown_clone_frame(frame.inner(), &frame.depths()),
                &column_names,
                capacity,
            )
        })
        .map_err(to_py_err)?;
        Ok(PyDataFrame::new(df, Arc::clone(&frame.runtime)))
    })
}

#[pyfunction]
pub(crate) fn transpose(
    frame: &PyDataFrame,
    py: Python<'_>,
    index_column: &str,
    key_names: Vec<String>,
    max_values: usize,
) -> PyResult<(PyDataFrame, Vec<String>)> {
    fenced!("transpose", {
        let segment = frame_drive_segment_cached(&frame.depths())?;
        let source = frame.executable()?;
        let outcome = py
            .detach(|| {
                block_on_grown_sized(
                    &frame.runtime,
                    repark_core::transpose_frame(source, index_column, key_names, max_values),
                    segment,
                )
            })
            .map_err(|error| match error {
                repark_core::TransposeError::Spark(spark) => {
                    let raised = AnalysisException::new_err(crate::exceptions::mask_user_visible(
                        spark.message,
                    ));
                    let pairs: Vec<(&str, &str)> = spark
                        .message_parameters
                        .iter()
                        .map(|pair| (pair.0.as_str(), pair.1.as_str()))
                        .collect();
                    let params = crate::exceptions::masked_message_params(py, &pairs);
                    for (name, value) in [
                        ("_spark_error_class", spark.error_class),
                        ("_spark_sql_state", spark.sql_state),
                    ] {
                        if let Err(failure) = raised.value(py).setattr(name, value) {
                            tracing::warn!(error = %failure, "transpose setattr failed");
                        }
                    }
                    if let Err(failure) = raised
                        .value(py)
                        .setattr("_spark_message_parameters", params)
                    {
                        tracing::warn!(error = %failure, "transpose params setattr failed");
                    }
                    raised
                }
                repark_core::TransposeError::Engine(err) => to_py_err(err),
            })?;
        Ok((
            PyDataFrame::new(outcome.frame, Arc::clone(&frame.runtime)),
            outcome.display_names,
        ))
    })
}
