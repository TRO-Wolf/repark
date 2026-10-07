use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::fence::fenced;

#[pyfunction]
pub fn logical_column_names(frame: PyRef<'_, PyDataFrame>) -> PyResult<Vec<String>> {
    fenced!("logical_names.logical_column_names", {
        let depths = frame.depths();
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        Ok(crate::deep_stack::grown_sync(need, || {
            frame
                .inner()
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect()
        }))
    })
}

#[pyfunction]
pub fn logical_column_qualifiers(frame: PyRef<'_, PyDataFrame>) -> PyResult<Vec<Option<String>>> {
    fenced!("logical_names.logical_column_qualifiers", {
        Ok(frame
            .inner()
            .schema()
            .iter()
            .map(|(qualifier, _)| qualifier.map(std::string::ToString::to_string))
            .collect())
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(logical_column_names, module)?)?;
    module.add_function(wrap_pyfunction!(logical_column_qualifiers, module)?)?;
    Ok(())
}
