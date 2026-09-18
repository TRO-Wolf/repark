use std::sync::Arc;

use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::fence::fenced_span;
use crate::session::PyReparkSession;
use crate::to_py_err;

#[pyfunction]
pub fn sql_static_overwrite(
    session: PyRef<'_, PyReparkSession>,
    py: Python<'_>,
    query: &str,
) -> PyResult<PyDataFrame> {
    fenced_span!("py.sql", "sql_static_overwrite", {
        repark_spark::refuse_declared_function_in_sql(query)
            .map_err(crate::datafusion_to_py_err)?;
        let inner = session.session.clone();
        let runtime = Arc::clone(&session.runtime);
        let df = py
            .detach(move || runtime.block_on(inner.sql_static_overwrite(query)))
            .map_err(to_py_err)?;
        Ok(PyDataFrame::new(df, Arc::clone(&session.runtime)))
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(sql_static_overwrite, module)?)?;
    Ok(())
}
