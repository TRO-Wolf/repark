use pyo3::exceptions::PyValueError;
use pyo3::prelude::{Bound, PyModule, PyModuleMethods, PyResult, pyfunction, wrap_pyfunction};

use crate::fence::fenced;

const PANIC_PROBE_TEXT: &str = "repark panic probe";

#[expect(
    clippy::panic,
    reason = "the panic probe raises a Rust panic on purpose so the suite can prove the fence is armed"
)]
fn raise_probe_panic() -> ! {
    std::panic::panic_any(PANIC_PROBE_TEXT)
}

#[pyfunction]
#[pyo3(name = "__panic_probe__")]
fn panic_probe(shape: &str) -> PyResult<()> {
    match shape {
        "method" => fenced!("PanicProbe.method", { raise_probe_panic() }),
        other => Err(PyValueError::new_err(format!(
            "unknown panic probe shape {other:?}"
        ))),
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(panic_probe, module)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PySparkException;
    use pyo3::Python;

    #[test]
    fn panic_probe_method_shape_is_a_fenced_pyspark_exception_without_the_flag() {
        Python::attach(|py| {
            let error = panic_probe("method").expect_err("the probe panics on purpose");
            assert!(error.is_instance_of::<PySparkException>(py));
            let message = error.to_string();
            assert!(
                message.contains("PanicProbe.method") && message.contains(PANIC_PROBE_TEXT),
                "the framing names the probe and keeps its text: {message}"
            );
        });
    }

    #[test]
    fn panic_probe_refuses_an_unknown_shape() {
        Python::attach(|py| {
            let error = panic_probe("other").expect_err("only the method shape exists");
            assert!(error.is_instance_of::<PyValueError>(py));
        });
    }
}
