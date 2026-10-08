use datafusion::dataframe::DataFrame;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::column::PyColumn;
use crate::dataframe::PyDataFrame;
use crate::datafusion_to_py_err;
use crate::deep_stack::grown_clone_frame;
use crate::fence::fenced;
use repark_core::frame_names::{
    NameRule, expose_hidden_keys, hidden_names_in, hidden_names_in_text, output_columns,
    rebind_key_name, shown_columns, using_hidden_keys,
};

#[allow(clippy::missing_errors_doc)]
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(using_hidden_key_fields, module)?)?;
    module.add_function(wrap_pyfunction!(expose_using_keys, module)?)?;
    module.add_function(wrap_pyfunction!(rebind_using_key, module)?)?;
    module.add_function(wrap_pyfunction!(hide_using_keys, module)?)?;
    Ok(())
}

fn widened(frame: &PyDataFrame, names: &[String]) -> PyResult<Option<PyDataFrame>> {
    let schema = frame.inner().schema();
    let missing = names
        .iter()
        .filter(|name| !schema.has_column_with_unqualified_name(name))
        .cloned()
        .collect::<Vec<_>>();
    let Some(plan) =
        expose_hidden_keys(frame.inner().logical_plan(), &missing).map_err(datafusion_to_py_err)?
    else {
        return Ok(None);
    };
    let (state, _) = grown_clone_frame(frame.inner(), &frame.depths()).into_parts();
    Ok(Some(PyDataFrame::new(
        DataFrame::new(state, plan),
        frame.runtime_handle(),
    )))
}

pub(crate) fn exposed_frame<'a>(
    frame: &PyDataFrame,
    columns: impl IntoIterator<Item = &'a PyColumn>,
) -> PyResult<Option<PyDataFrame>> {
    let exprs = columns.into_iter().map(PyColumn::expr).collect::<Vec<_>>();
    let names = hidden_names_in(&exprs);
    if names.is_empty() {
        return Ok(None);
    }
    widened(frame, &names)
}

pub(crate) fn exposed_for_text(frame: &PyDataFrame, text: &str) -> PyResult<Option<PyDataFrame>> {
    let names = hidden_names_in_text(text);
    if names.is_empty() {
        return Ok(None);
    }
    widened(frame, &names)
}

pub(crate) fn narrowed_frame(wide: &PyDataFrame, origin: &PyDataFrame) -> PyResult<PyDataFrame> {
    let columns = output_columns(origin.inner().schema());
    let narrowed = grown_clone_frame(wide.inner(), &wide.depths())
        .select(columns)
        .map_err(datafusion_to_py_err)?;
    Ok(PyDataFrame::new(narrowed, wide.runtime_handle()))
}

#[pyfunction]
fn using_hidden_key_fields(frame: &PyDataFrame) -> Vec<(bool, usize, String, String)> {
    using_hidden_keys(frame.inner().logical_plan())
        .into_iter()
        .map(|key| (key.right, key.side_position, key.alias, key.display))
        .collect()
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
fn expose_using_keys(frame: &PyDataFrame, names: Vec<String>) -> PyResult<Option<PyDataFrame>> {
    fenced!("using_keys.expose_using_keys", { widened(frame, &names) })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (column, qualifier, key, alias, exact, keep_name))]
fn rebind_using_key(
    column: &PyColumn,
    qualifier: Option<&str>,
    key: &str,
    alias: &str,
    exact: bool,
    keep_name: bool,
) -> PyResult<PyColumn> {
    fenced!("using_keys.rebind_using_key", {
        let rule = if exact {
            NameRule::Exact
        } else {
            NameRule::IgnoreCase
        };
        rebind_key_name(column.expr(), qualifier, key, alias, rule, keep_name)
            .map(PyColumn::from_expr)
            .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn hide_using_keys(frame: &PyDataFrame) -> PyResult<PyDataFrame> {
    fenced!("using_keys.hide_using_keys", {
        let shown = shown_columns(frame.inner().schema());
        let held = grown_clone_frame(frame.inner(), &frame.depths());
        if shown.len() == frame.inner().schema().fields().len() {
            return Ok(PyDataFrame::new(held, frame.runtime_handle()));
        }
        let narrowed = held.select(shown).map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(narrowed, frame.runtime_handle()))
    })
}
