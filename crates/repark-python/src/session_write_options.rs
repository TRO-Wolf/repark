use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::fence::fenced_span;
use crate::session::PyReparkSession;

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (session, query, options, force_static_overwrite = false, force_dynamic_overwrite = false, source_by_name = false))]
pub fn session_sql_with_write_options(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    query: &str,
    options: HashMap<String, String>,
    force_static_overwrite: bool,
    force_dynamic_overwrite: bool,
    source_by_name: bool,
) -> PyResult<PyDataFrame> {
    let overwrite_intent = match (force_static_overwrite, force_dynamic_overwrite) {
        (false, false) => repark_core::OverwriteIntent::Session,
        (true, false) => repark_core::OverwriteIntent::Static,
        (false, true) => repark_core::OverwriteIntent::Dynamic,
        (true, true) => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "force_static_overwrite and force_dynamic_overwrite are mutually exclusive",
            ));
        }
    };
    fenced_span!("py.sql", "session_sql_with_write_options", {
        repark_spark::refuse_declared_function_in_sql(query)
            .map_err(crate::datafusion_to_py_err)?;
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        let df = py
            .detach(|| {
                runtime.block_on(inner.sql_built_with_write_options(
                    query,
                    &options,
                    overwrite_intent,
                    source_by_name,
                ))
            })
            .map_err(crate::to_py_err)?;
        Ok(PyDataFrame::new(df, runtime))
    })
}

#[allow(clippy::missing_errors_doc, clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (session, frame, url, format, mode, options, partition_by))]
pub fn session_write_path(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    frame: &PyDataFrame,
    url: &str,
    format: &str,
    mode: &str,
    options: HashMap<String, String>,
    partition_by: Vec<String>,
) -> PyResult<usize> {
    fenced_span!("py.write", "session_write_path", {
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        let frame = frame.inner().clone();
        py.detach(|| {
            runtime.block_on(inner.write_path(&frame, url, format, mode, &options, &partition_by))
        })
        .map_err(crate::to_py_err)
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (session, query))]
pub fn session_sql_built(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    query: &str,
) -> PyResult<PyDataFrame> {
    fenced_span!("py.sql", "session_sql_built", {
        let query = crate::session_runtime::prepare_session_sql(query)?;
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        let df = py
            .detach(|| runtime.block_on(inner.sql_built(&query)))
            .map_err(crate::to_py_err)?;
        Ok(PyDataFrame::new(df, runtime))
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (sql, keep_verbatim))]
pub fn built_sql_user_fragment(sql: &str, keep_verbatim: bool) -> PyResult<String> {
    fenced_span!("py.sql", "built_sql_user_fragment", {
        repark_spark::spark_literals::built_fragment::canonicalize_fragment_for_default_parse(
            sql,
            keep_verbatim,
        )
        .map(std::borrow::Cow::into_owned)
        .map_err(crate::datafusion_to_py_err)
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(session_sql_with_write_options, module)?)?;
    module.add_function(wrap_pyfunction!(session_write_path, module)?)?;
    module.add_function(wrap_pyfunction!(session_sql_built, module)?)?;
    module.add_function(wrap_pyfunction!(built_sql_user_fragment, module)?)?;
    Ok(())
}
