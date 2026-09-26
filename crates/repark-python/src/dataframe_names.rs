use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::column::PyColumn;
use crate::dataframe::PyDataFrame;
use crate::datafusion_to_py_err;
use crate::fence::fenced;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(attribute_column, module)?)?;
    module.add_function(wrap_pyfunction!(attribute_copies, module)?)?;
    module.add_function(wrap_pyfunction!(attribute_copy_name, module)?)?;
    module.add_function(wrap_pyfunction!(drop_frame_columns, module)?)?;
    module.add_function(wrap_pyfunction!(refuse_ambiguous_join_condition, module)?)?;
    module.add_function(wrap_pyfunction!(requalify_join_sides, module)?)?;
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn attribute_column(name: &str) -> PyResult<PyColumn> {
    fenced!("dataframe_names.attribute_column", {
        Ok(PyColumn::from_expr(
            repark_core::frame_names::attribute_reference(name),
        ))
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn attribute_copies(frame: &PyDataFrame) -> PyResult<PyDataFrame> {
    fenced!("dataframe_names.attribute_copies", {
        let df = repark_core::frame_names::with_attribute_copies(frame.inner().clone())
            .map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(df, frame.runtime_handle()))
    })
}

#[pyfunction]
fn attribute_copy_name(frame: &PyDataFrame, name: &str) -> String {
    repark_core::frame_names::attribute_copy_name_in(frame.inner().schema(), name)
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
fn drop_frame_columns(
    frame: &PyDataFrame,
    names: Vec<String>,
    references: Vec<String>,
    attributes: Vec<String>,
) -> PyResult<PyDataFrame> {
    fenced!("dataframe_names.drop_frame_columns", {
        let df = repark_core::frame_names::drop_named_columns(
            frame.inner().clone(),
            &names,
            &references,
            &attributes,
        )
        .map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(df, frame.runtime_handle()))
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn refuse_ambiguous_join_condition(
    left: &PyDataFrame,
    right: &PyDataFrame,
    condition_sql: &str,
) -> PyResult<()> {
    fenced!("dataframe_names.refuse_ambiguous_join_condition", {
        repark_core::frame_names::refuse_ambiguous_condition(
            condition_sql,
            &[left.inner().schema(), right.inner().schema()],
        )
        .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn requalify_join_sides(
    joined: &PyDataFrame,
    left: &PyDataFrame,
    right: Option<&PyDataFrame>,
) -> PyResult<PyDataFrame> {
    fenced!("dataframe_names.requalify_join_sides", {
        let mut sides = vec![left.inner().schema()];
        sides.extend(right.map(|frame| frame.inner().schema()));
        let df = repark_core::frame_names::requalify_join_sides(joined.inner().clone(), &sides)
            .map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(df, joined.runtime_handle()))
    })
}
