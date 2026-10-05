use std::collections::BTreeMap;
use std::sync::Arc;

use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::fence::{fenced, fenced_span};
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

#[pyfunction]
pub fn read_iceberg_incremental(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    table_name: &str,
    window: BTreeMap<String, String>,
    travel: BTreeMap<String, String>,
) -> PyResult<PyDataFrame> {
    fenced_span!("py.read", "PyReparkSession.read_iceberg_incremental", {
        let inner: &PyReparkSession = &session;
        let df = py
            .detach(|| {
                inner
                    .runtime
                    .block_on(repark_core::time_travel::incremental::read_incremental(
                        &inner.session,
                        table_name,
                        &window,
                        &travel,
                    ))
            })
            .map_err(to_py_err)?;
        Ok(PyDataFrame::new(df, Arc::clone(&inner.runtime)))
    })
}

#[pyfunction]
pub fn read_iceberg_path(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    path: &str,
) -> PyResult<PyDataFrame> {
    fenced_span!("py.read", "PyReparkSession.read_iceberg_path", {
        let inner: &PyReparkSession = &session;
        let df = py
            .detach(|| {
                inner
                    .runtime
                    .block_on(inner.session.read_iceberg_path(path))
            })
            .map_err(to_py_err)?;
        Ok(PyDataFrame::new(df, Arc::clone(&inner.runtime)))
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(session_sources, module)?)?;
    module.add_function(wrap_pyfunction!(session_source, module)?)?;
    module.add_function(wrap_pyfunction!(session_source_ping, module)?)?;
    module.add_function(wrap_pyfunction!(read_iceberg_incremental, module)?)?;
    module.add_function(wrap_pyfunction!(read_iceberg_path, module)?)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn read_iceberg_table_pinned(
    py: Python<'_>,
    session: &PyReparkSession,
    table_name: &str,
    snapshot_id: Option<i64>,
    as_of_timestamp_ms: Option<i64>,
    branch: Option<String>,
    tag: Option<String>,
    version_as_of: Option<String>,
    timestamp_as_of: Option<String>,
) -> PyResult<PyDataFrame> {
    fenced_span!("py.read", "PyReparkSession.read_iceberg_table", {
        let opts = repark_core::time_travel::TimeTravelOpts {
            snapshot_id,
            as_of_timestamp_ms,
            branch,
            tag,
        };
        let df = py
            .detach(|| {
                session.runtime.block_on(session.session.read_iceberg_table(
                    table_name,
                    opts,
                    version_as_of,
                    timestamp_as_of,
                ))
            })
            .map_err(to_py_err)?;
        Ok(PyDataFrame::new(df, Arc::clone(&session.runtime)))
    })
}
