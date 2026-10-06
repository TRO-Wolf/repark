use pyo3::prelude::*;

use crate::fence::fenced;
use crate::session::PyReparkSession;

#[pyfunction]
pub fn iceberg_metadata_cache_census(
    session: PyRef<'_, PyReparkSession>,
) -> PyResult<(bool, u64, u64, u64, usize)> {
    fenced!("catalog_census.iceberg_metadata_cache_census", {
        let inner = &session.session;
        let runtime = &session.runtime;
        session
            .py()
            .detach(|| runtime.block_on(inner.settle_iceberg_metadata_cache()));
        let entries = inner.iceberg_metadata_cache_entries();
        Ok(match inner.iceberg_metadata_cache_stats() {
            Some((hits, misses, body_fetches)) => (true, hits, misses, body_fetches, entries),
            None => (false, 0, 0, 0, entries),
        })
    })
}

#[pyfunction]
pub fn namespace_metadata(
    session: PyRef<'_, PyReparkSession>,
    catalog: &str,
    namespace: &str,
) -> PyResult<(Option<String>, Option<String>)> {
    fenced!("catalog_census.namespace_metadata", {
        let catalogs = session.session.catalogs_snapshot();
        let runtime = &session.runtime;
        session
            .py()
            .detach(|| {
                runtime.block_on(repark_spark::describe_show::namespace_metadata(
                    &catalogs, catalog, namespace,
                ))
            })
            .map_err(crate::datafusion_to_py_err)
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(iceberg_metadata_cache_census, module)?)?;
    module.add_function(wrap_pyfunction!(namespace_metadata, module)?)?;
    Ok(())
}
