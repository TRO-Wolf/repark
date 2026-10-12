use pyo3::prelude::*;

#[pyfunction]
pub fn _tsns_gate_counts() -> (u64, u64) {
    (
        repark_functions::timestamp_ns_coercion::gate_open_count(),
        repark_functions::timestamp_ns_coercion::fallback_count(),
    )
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(_tsns_gate_counts, module)?)?;
    Ok(())
}
