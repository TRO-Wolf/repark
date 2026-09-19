use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::fence::fenced_span;
use crate::session::PyReparkSession;

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (session, query, options, force_static_overwrite = false, force_dynamic_overwrite = false))]
pub fn session_sql_with_write_options(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    query: &str,
    options: HashMap<String, String>,
    force_static_overwrite: bool,
    force_dynamic_overwrite: bool,
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
                runtime.block_on(inner.sql_with_write_options(query, &options, overwrite_intent))
            })
            .map_err(crate::to_py_err)?;
        Ok(PyDataFrame::new(df, runtime))
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(session_sql_with_write_options, module)?)?;
    Ok(())
}
