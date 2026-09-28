use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::fence::fenced;
use crate::session::PyReparkSession;
use crate::to_py_err;
use repark_core::frame_names::NameRule;
use repark_functions::case_sensitive::spark_case_sensitive_from_options;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(
        resolve_temp_view_home_ref_for_session,
        module
    )?)?;
    Ok(())
}

pub(crate) fn session_rule(session: &PyReparkSession) -> NameRule {
    let state = session.session.context().state_ref();
    let guard = state.read();
    NameRule::from_case_sensitive(spark_case_sensitive_from_options(guard.config().options()))
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn resolve_temp_view_home_ref_for_session(
    session: &PyReparkSession,
    name: &str,
) -> PyResult<Option<Vec<String>>> {
    fenced!("temp_view_names.resolve_temp_view_home_ref_for_session", {
        match session_rule(session) {
            NameRule::Exact => session.session.resolve_temp_view_home_ref_exact(name),
            NameRule::IgnoreCase => session.session.resolve_temp_view_home_ref(name),
        }
        .map_err(to_py_err)
    })
}
