use pyo3::prelude::*;

use crate::fence::fenced;
use crate::session::PyReparkSession;

#[pyfunction]
pub fn iceberg_metadata_cache_census(
    session: PyRef<'_, PyReparkSession>,
) -> PyResult<(bool, u64, u64, u64, usize)> {
    fenced!("catalog_census.iceberg_metadata_cache_census", {
        let inner = &session.session;
        let entries = inner.iceberg_metadata_cache_entries();
        Ok(match inner.iceberg_metadata_cache_stats() {
            Some((hits, misses, body_fetches)) => (true, hits, misses, body_fetches, entries),
            None => (false, 0, 0, 0, entries),
        })
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(iceberg_metadata_cache_census, module)?)?;
    Ok(())
}
