use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::fence::fenced_span;
use crate::session::PyReparkSession;

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (session, query, options, force_static_overwrite = false))]
pub fn session_sql_with_write_options(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    query: &str,
    options: HashMap<String, String>,
    force_static_overwrite: bool,
) -> PyResult<PyDataFrame> {
    fenced_span!("py.sql", "session_sql_with_write_options", {
        repark_spark::refuse_declared_function_in_sql(query)
            .map_err(crate::datafusion_to_py_err)?;
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        let df = py
            .detach(|| {
                runtime.block_on(inner.sql_with_write_options(
                    query,
                    &options,
                    force_static_overwrite,
                ))
            })
            .map_err(crate::to_py_err)?;
        Ok(PyDataFrame::new(df, runtime))
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(session_sql_with_write_options, module)?)?;
    Ok(())
}
