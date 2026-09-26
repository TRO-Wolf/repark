use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::dataframe::PyDataFrame;
use crate::datafusion_to_py_err;
use crate::fence::fenced;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(drop_frame_columns, module)?)?;
    Ok(())
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
fn drop_frame_columns(
    frame: &PyDataFrame,
    names: Vec<String>,
    references: Vec<String>,
) -> PyResult<PyDataFrame> {
    fenced!("dataframe_names.drop_frame_columns", {
        let df = repark_core::frame_names::drop_named_columns(
            frame.inner().clone(),
            &names,
            &references,
        )
        .map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(df, frame.runtime_handle()))
    })
}
