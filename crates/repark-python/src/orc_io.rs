use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
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
#[pyo3(signature = (session, paths, merge_schema=false, path_glob_filter=None, recursive_file_lookup=false, modified_before=None, modified_after=None, base_path=None, ignore_corrupt_files=false, user_schema=None))]
#[allow(clippy::too_many_arguments)]
pub fn read_orc(
    session: &PyReparkSession,
    paths: Vec<String>,
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
                    .block_on(session.session.read_orc(&paths, options))
            })
            .map_err(|error| {
                let message = error.to_string();
                let raised = to_py_err(error);
                attach_orc_condition(py, &raised, &message);
                raised
            })
        })?;
        Ok(PyDataFrame::new(dataframe, Arc::clone(&session.runtime)))
    })
}

fn attach_orc_condition(py: Python<'_>, raised: &PyErr, message: &str) {
    let missing = "[PATH_NOT_FOUND] Path does not exist: ";
    if let Some(rest) = message.strip_prefix(missing) {
        let path = rest.strip_suffix(". SQLSTATE: 42K03").unwrap_or(rest);
        set_orc_condition(
            py,
            raised,
            "PATH_NOT_FOUND",
            Some("42K03"),
            Some(("path", path)),
        );
        return;
    }
    if message.starts_with("[UNABLE_TO_INFER_SCHEMA]") {
        set_orc_condition(
            py,
            raised,
            "UNABLE_TO_INFER_SCHEMA",
            Some("42KD9"),
            Some(("format", "ORC")),
        );
        return;
    }
    if message.starts_with("[FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER]") {
        set_orc_condition(
            py,
            raised,
            "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER",
            Some("KD001"),
            None,
        );
        return;
    }
    if message.starts_with("[CANNOT_MERGE_SCHEMAS]") {
        set_orc_condition(py, raised, "CANNOT_MERGE_SCHEMAS", None, None);
    }
}

fn set_orc_condition(
    py: Python<'_>,
    raised: &PyErr,
    error_class: &str,
    sql_state: Option<&str>,
    params: Option<(&str, &str)>,
) {
    let value = raised.value(py);
    let dict = params.map(|(key, item)| {
        let dict = PyDict::new(py);
        dict.set_item(key, item)
            .unwrap_or_else(|failure| tracing::warn!(error = %failure, "orc param set failed"));
        dict
    });
    if let Err(failure) = value.setattr("_spark_error_class", error_class) {
        tracing::warn!(error = %failure, "orc condition setattr failed");
    }
    if let Err(failure) = value.setattr("_spark_sql_state", sql_state) {
        tracing::warn!(error = %failure, "orc condition setattr failed");
    }
    if let Err(failure) = value.setattr("_spark_message_parameters", dict) {
        tracing::warn!(error = %failure, "orc condition setattr failed");
    }
}
