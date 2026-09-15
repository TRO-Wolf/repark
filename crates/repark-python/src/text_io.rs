use std::path::Path;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::dataframe::PyDataFrame;
use crate::fence::fenced_span;
use crate::session::PyReparkSession;
use crate::to_py_err;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(read_text, module)?)?;
    module.add_function(wrap_pyfunction!(write_text_frame, module)?)?;
    module.add_function(wrap_pyfunction!(write_text_partitioned, module)?)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (session, path, wholetext=false, line_sep=None, user_schema=None, base_path=None))]
pub fn read_text(
    session: &PyReparkSession,
    path: &str,
    wholetext: bool,
    line_sep: Option<String>,
    user_schema: Option<Vec<(String, String)>>,
    base_path: Option<String>,
) -> PyResult<PyDataFrame> {
    fenced_span!("py.read", "read_text", {
        let dataframe = Python::attach(|py| {
            py.detach(|| {
                session.runtime.block_on(session.session.read_text(
                    path,
                    wholetext,
                    line_sep.as_deref(),
                    user_schema,
                    base_path.as_deref(),
                ))
            })
        })
        .map_err(to_py_err)?;
        Ok(PyDataFrame::new(dataframe, Arc::clone(&session.runtime)))
    })
}

#[pyfunction]
#[pyo3(signature = (frame, path, line_sep=None))]
pub fn write_text_frame(frame: &PyDataFrame, path: &str, line_sep: Option<String>) -> PyResult<()> {
    fenced_span!("py.write", "write_text_frame", {
        let separator = line_sep.unwrap_or_else(|| "\n".to_string());
        Python::attach(|py| {
            py.detach(|| {
                frame.runtime.block_on(repark_core::write_text_frame(
                    frame.inner(),
                    Path::new(path),
                    separator.as_str(),
                ))
            })
        })
        .map_err(to_py_err)?;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (frame, path, partition_columns, line_sep=None, session_zone="UTC"))]
pub fn write_text_partitioned(
    frame: &PyDataFrame,
    path: &str,
    partition_columns: Vec<String>,
    line_sep: Option<String>,
    session_zone: &str,
) -> PyResult<()> {
    fenced_span!("py.write", "write_text_partitioned", {
        let separator = line_sep.unwrap_or_else(|| "\n".to_string());
        Python::attach(|py| {
            py.detach(|| {
                frame.runtime.block_on(repark_core::write_text_partitioned(
                    frame.inner(),
                    Path::new(path),
                    separator.as_str(),
                    &partition_columns,
                    session_zone,
                ))
            })
        })
        .map_err(to_py_err)?;
        Ok(())
    })
}
