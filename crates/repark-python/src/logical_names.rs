use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::fence::fenced;

#[pyfunction]
pub fn logical_column_names(frame: PyRef<'_, PyDataFrame>) -> PyResult<Vec<String>> {
    fenced!("logical_names.logical_column_names", {
        Ok(frame
            .inner()
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect())
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(logical_column_names, module)?)?;
    Ok(())
}
