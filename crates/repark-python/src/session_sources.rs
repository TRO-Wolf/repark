use std::collections::BTreeMap;

use pyo3::prelude::*;

use crate::fence::fenced;
use crate::session::PyReparkSession;
use crate::to_py_err;

type SourceRowTuple = (String, String, String, bool, BTreeMap<String, String>);

#[pyfunction]
pub fn session_sources(session: PyRef<'_, PyReparkSession>) -> PyResult<Vec<SourceRowTuple>> {
    fenced!("session_sources.session_sources", {
        Ok(session
            .session
            .sources()
            .into_iter()
            .map(|row| {
                (
                    row.name,
                    row.kind,
                    row.key_path,
                    row.auto_register,
                    row.properties,
                )
            })
            .collect())
    })
}

#[pyfunction]
pub fn session_source(
    session: PyRef<'_, PyReparkSession>,
    name: &str,
) -> PyResult<(String, String, String)> {
    fenced!("session_sources.session_source", {
        let source = session.session.source(name).map_err(to_py_err)?;
        Ok((
            source.name().to_string(),
            source.kind().to_string(),
            source.key_path().to_string(),
        ))
    })
}

#[pyfunction]
pub fn session_source_ping(session: PyRef<'_, PyReparkSession>, name: &str) -> PyResult<()> {
    fenced!("session_sources.session_source_ping", {
        session
            .session
            .source(name)
            .and_then(|source| source.ping())
            .map_err(to_py_err)
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(session_sources, module)?)?;
    module.add_function(wrap_pyfunction!(session_source, module)?)?;
    module.add_function(wrap_pyfunction!(session_source_ping, module)?)?;
    Ok(())
}
