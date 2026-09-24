use std::sync::Arc;

use pyo3::prelude::*;

use crate::fence::fenced;
use crate::session::PyReparkSession;

#[allow(clippy::missing_errors_doc)]
#[allow(clippy::needless_pass_by_value)]
#[pyfunction]
#[pyo3(signature = (session, parts, partition_columns, num_buckets, bucket_columns, sort_columns))]
pub fn writer_check_layout(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    parts: Vec<String>,
    partition_columns: Vec<String>,
    num_buckets: Option<i64>,
    bucket_columns: Vec<String>,
    sort_columns: Vec<String>,
) -> PyResult<()> {
    fenced!("writer_layout.writer_check_layout", {
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        py.detach(|| {
            runtime.block_on(inner.check_writer_layout(
                &parts,
                partition_columns,
                num_buckets,
                bucket_columns,
                sort_columns,
            ))
        })
        .map_err(crate::to_py_err)
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (session, target, qualified, mode, explicit_format))]
pub fn writer_save_target(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    target: &str,
    qualified: Option<&str>,
    mode: &str,
    explicit_format: bool,
) -> PyResult<&'static str> {
    fenced!("writer_layout.writer_save_target", {
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        py.detach(|| {
            runtime.block_on(inner.writer_save_target(target, qualified, mode, explicit_format))
        })
        .map_err(crate::to_py_err)
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(writer_check_layout, module)?)?;
    module.add_function(wrap_pyfunction!(writer_save_target, module)?)?;
    Ok(())
}
