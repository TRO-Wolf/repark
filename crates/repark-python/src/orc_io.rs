use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use repark_core::OrcReadOptions;

use crate::dataframe::PyDataFrame;
use crate::fence::fenced_span;
use crate::session::PyReparkSession;
use crate::to_py_err;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(read_orc, module)?)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (session, path, merge_schema=false, path_glob_filter=None, recursive_file_lookup=false, modified_before=None, modified_after=None, base_path=None, ignore_corrupt_files=false, user_schema=None))]
#[allow(clippy::too_many_arguments)]
pub fn read_orc(
    session: &PyReparkSession,
    path: &str,
    merge_schema: bool,
    path_glob_filter: Option<String>,
    recursive_file_lookup: bool,
    modified_before: Option<String>,
    modified_after: Option<String>,
    base_path: Option<String>,
    ignore_corrupt_files: bool,
    user_schema: Option<Vec<(String, String)>>,
) -> PyResult<PyDataFrame> {
    fenced_span!("py.read", "read_orc", {
        let options = OrcReadOptions {
            merge_schema,
            path_glob_filter,
            recursive_file_lookup,
            modified_before,
            modified_after,
            base_path,
            ignore_corrupt_files,
            user_schema,
        };
        let dataframe = Python::attach(|py| {
            py.detach(|| {
                session
                    .runtime
                    .block_on(session.session.read_orc(path, options))
            })
        })
        .map_err(to_py_err)?;
        Ok(PyDataFrame::new(dataframe, Arc::clone(&session.runtime)))
    })
}
