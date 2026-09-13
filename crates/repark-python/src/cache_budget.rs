use std::sync::Arc;

use pyo3::prelude::*;

use crate::fence::fenced_span;
use crate::session::PyReparkSession;
use crate::to_py_err;

#[pyfunction]
pub fn retained_cache_bytes(session: PyRef<'_, PyReparkSession>, py: Python<'_>) -> PyResult<u64> {
    fenced_span!("py.session", "PyReparkSession.retained_cache_bytes", {
        let inner = session.session.clone();
        let runtime = Arc::clone(&session.runtime);
        py.detach(move || runtime.block_on(inner.retained_cache_bytes()))
            .map_err(to_py_err)
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(retained_cache_bytes, module)?)?;
    Ok(())
}
