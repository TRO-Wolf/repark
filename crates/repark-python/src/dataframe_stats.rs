use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::wrap_pyfunction;

use crate::dataframe::PyDataFrame;
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
        let df = repark_core::freq_items(frame.df.clone(), &column_names, capacity)
            .map_err(to_py_err)?;
        Ok(PyDataFrame::new(df, Arc::clone(&frame.runtime)))
    })
}

#[pyfunction]
fn transpose(
    frame: &PyDataFrame,
    py: Python<'_>,
    index_column: &str,
    key_names: Vec<String>,
    max_values: usize,
) -> PyResult<(PyDataFrame, Vec<String>)> {
    fenced!("transpose", {
        let source = frame.df.clone();
        let outcome = py
            .detach(|| {
                frame.runtime.block_on(repark_core::transpose_frame(
                    source,
                    index_column,
                    key_names,
                    max_values,
                ))
            })
            .map_err(|error| match error {
                repark_core::TransposeError::Spark(spark) => {
                    let raised = AnalysisException::new_err(spark.message);
                    let params = PyDict::new(py);
                    for (key, value) in spark.message_parameters {
                        if let Err(failure) = params.set_item(key, value) {
                            tracing::warn!(error = %failure, "transpose param set failed");
                        }
                    }
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
