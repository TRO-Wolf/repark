use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::dataframe::PyDataFrame;
use crate::fence::fenced;
use crate::to_py_err;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(stack_dataframe, module)?)?;
    Ok(())
}

#[pyfunction]
fn stack_dataframe(
    frame: &PyDataFrame,
    n: i64,
    passthrough_count: usize,
    output_names: Option<Vec<String>>,
) -> PyResult<PyDataFrame> {
    fenced!("stack_dataframe", {
        let df = repark_core::apply_stack(
            frame.df.clone(),
            n,
            passthrough_count,
            output_names.as_deref(),
        )
        .map_err(to_py_err)?;
        Ok(PyDataFrame::new(df, Arc::clone(&frame.runtime)))
    })
}
