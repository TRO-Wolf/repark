use pyo3::prelude::*;

use super::super::PyColumn;
use crate::fence::fenced;

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(grouping_id_column, module)?)?;
    module.add_function(wrap_pyfunction!(fragment_timestamp_ntz_sql, module)?)
}

#[pyfunction]
pub(crate) fn fragment_timestamp_ntz_sql(sql: &str) -> Option<String> {
    repark_spark::values_marker::fragment_timestamp_types_as_ntz(sql)
}

#[pyfunction]
pub(crate) fn grouping_id_column(args: Vec<PyColumn>) -> PyResult<PyColumn> {
    fenced!("grouping_id_column", {
        let exprs = args.iter().map(PyColumn::expr).collect::<Vec<_>>();
        Ok(PyColumn::combine_surveyed(
            PyColumn::grouping_id_call(exprs)?,
            &args,
        ))
    })
}
