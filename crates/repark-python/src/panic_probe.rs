use std::ffi::CStr;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::{
    Bound, Py, PyAny, PyModule, PyModuleMethods, PyResult, Python, pyclass, pyfunction, pymethods,
    wrap_pyfunction,
};
use pyo3::types::PyCapsule;

use crate::fence::{fence_stream_poll, fenced};

const PANIC_PROBE_TEXT: &str = "repark panic probe";

const PANIC_PROBE_STREAM_CAPSULE: &CStr = c"arrow_array_stream";

#[expect(
    clippy::panic,
    reason = "the panic probe raises a Rust panic on purpose so the suite can prove the fence is armed"
)]
fn raise_probe_panic() -> ! {
    std::panic::panic_any(PANIC_PROBE_TEXT)
}

#[pyclass(name = "PanicProbeStream", module = "repark._native")]
struct PanicProbeStream;

struct PanickingReader;

impl Iterator for PanickingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        fence_stream_poll("PanicProbe.stream.next", || raise_probe_panic())
    }
}

impl RecordBatchReader for PanickingReader {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "probe",
            DataType::Int64,
            false,
        )]))
    }
}

#[pymethods]
impl PanicProbeStream {
    #[allow(clippy::wrong_self_convention)]
    #[allow(clippy::needless_pass_by_value)]
    #[allow(clippy::unused_self)]
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(PanickingReader);
        PyCapsule::new_with_value_and_destructor(
            py,
            FFI_ArrowArrayStream::new(reader),
            PANIC_PROBE_STREAM_CAPSULE,
            |ffi_stream, _ctx| drop(ffi_stream),
        )
    }
}

#[pyfunction]
#[pyo3(name = "__panic_probe__")]
fn panic_probe(py: Python<'_>, shape: &str) -> PyResult<Py<PyAny>> {
    match shape {
        "method" => fenced!("PanicProbe.method", { raise_probe_panic() }),
        "stream" => Ok(Py::new(py, PanicProbeStream)?.into_any()),
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
    use pyo3::prelude::PyAnyMethods;

    #[test]
    fn panic_probe_method_shape_is_a_fenced_pyspark_exception_without_the_flag() {
        Python::attach(|py| {
            let error = panic_probe(py, "method").expect_err("the probe panics on purpose");
            assert!(error.is_instance_of::<PySparkException>(py));
            let message = error.to_string();
            assert!(
                message.contains("PanicProbe.method") && message.contains(PANIC_PROBE_TEXT),
                "the framing names the probe and keeps its text: {message}"
            );
        });
    }

    #[test]
    fn panic_probe_stream_shape_exports_a_capsule_and_its_poll_is_fenced() {
        Python::attach(|py| {
            let probe = panic_probe(py, "stream").expect("the stream shape returns an exporter");
            let capsule = probe
                .bind(py)
                .call_method0("__arrow_c_stream__")
                .expect("the exporter produces a capsule");
            assert!(capsule.is_instance_of::<PyCapsule>());
        });
        let mut reader = PanickingReader;
        assert_eq!(reader.schema().fields().len(), 1);
        let error = reader
            .next()
            .expect("a fenced panic yields one item")
            .expect_err("the item is the Err arm");
        let message = error.to_string();
        assert!(
            message.contains("PanicProbe.stream.next") && message.contains(PANIC_PROBE_TEXT),
            "the framing names the probe poll and keeps its text: {message}"
        );
    }

    #[test]
    fn panic_probe_refuses_an_unknown_shape() {
        Python::attach(|py| {
            let error = panic_probe(py, "other").expect_err("only two shapes exist");
            assert!(error.is_instance_of::<PyValueError>(py));
        });
    }
}
