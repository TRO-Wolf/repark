use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::dataframe::PyDataFrame;
use crate::fence::{fenced, fenced_span};
use crate::{datafusion_to_py_err, to_py_err};

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(input_files, module)?)?;
    module.add_function(wrap_pyfunction!(semantic_hash, module)?)?;
    Ok(())
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn input_files(py: Python<'_>, frame: &PyDataFrame) -> PyResult<Vec<String>> {
    fenced_span!("py.action", "plan_introspect.input_files", {
        let plan = py
            .detach(|| {
                frame
                    .runtime
                    .block_on(frame.df.clone().create_physical_plan())
            })
            .map_err(datafusion_to_py_err)?;
        Ok(repark_core::input_files(&plan))
    })
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn semantic_hash(frame: &PyDataFrame) -> PyResult<i64> {
    fenced!("plan_introspect.semantic_hash", {
        let (state, plan) = frame.df.clone().into_parts();
        repark_core::semantic_hash(&state, &plan).map_err(to_py_err)
    })
}
