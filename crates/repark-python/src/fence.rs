//! Shared PyO3 and Arrow C-stream panic fences.
//! Panics become Python or Arrow errors instead of unwinding across FFI or aborting the process.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use pyo3::prelude::PyErr;

use crate::PySparkException;

/// ===========================================================================================
/// Extract panic text and add the boundary operation to the internal-error message.
/// ===========================================================================================
fn describe_panic(operation: &str, payload: &(dyn Any + Send)) -> String {
    let detail = if let Some(text) = payload.downcast_ref::<&'static str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "non-string panic payload".to_string()
    };
    format!(
        "repark internal error in {operation}: {detail} (a Rust panic was caught at the Python \
         boundary; this is a bug — please report it)"
    )
}

/// ===========================================================================================
/// Run a `#[pymethods]` body under the panic fence.
///
/// Successful results pass through. A panic becomes [`crate::PySparkException`] with its text.
/// [`AssertUnwindSafe`] covers engine handles that are not statically `UnwindSafe`: a caught
/// panic returns an error and a fresh exception, so no partially-mutated state is observed.
/// ===========================================================================================
pub(crate) fn fence<T>(
    operation: &str,
    body: impl FnOnce() -> Result<T, PyErr>,
) -> Result<T, PyErr> {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(result) => result,
        Err(payload) => Err(PySparkException::new_err(describe_panic(
            operation,
            payload.as_ref(),
        ))),
    }
}

/// ===========================================================================================
/// Fence a method body and record its static entry-point family and operation.
/// ===========================================================================================
pub(crate) fn fence_with_span<T>(
    family: &'static str,
    operation: &'static str,
    body: impl FnOnce() -> Result<T, PyErr>,
) -> Result<T, PyErr> {
    let span = tracing::info_span!("py.entry", family, operation);
    let _guard = span.enter();
    fence(operation, body)
}

/// ===========================================================================================
/// A caught-panic error carried on the Arrow C-stream error channel.
///
/// Carry a framed panic through [`ArrowError::ExternalError`].
/// ===========================================================================================
#[derive(Debug)]
struct FencedStreamPanic(String);

impl std::fmt::Display for FencedStreamPanic {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for FencedStreamPanic {}

/// ===========================================================================================
/// Run one Arrow-C-stream batch poll under the panic fence.
///
/// Convert a poll panic into `Some(Err(ArrowError))` for that poll; other results pass through.
/// ===========================================================================================
pub(crate) fn fence_stream_poll(
    operation: &str,
    body: impl FnOnce() -> Option<Result<RecordBatch, ArrowError>>,
) -> Option<Result<RecordBatch, ArrowError>> {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(item) => item,
        Err(payload) => Some(Err(ArrowError::ExternalError(Box::new(FencedStreamPanic(
            describe_panic(operation, payload.as_ref()),
        ))))),
    }
}

/// Wrap a `#[pymethods]` body in the shared [`fence`].
macro_rules! fenced {
    ($operation:literal, $body:block) => {
        $crate::fence::fence($operation, move || $body)
    };
}

/// Wrap a body in [`fence`] and a static `py.entry` span.
macro_rules! fenced_span {
    ($family:literal, $operation:literal, $body:block) => {
        $crate::fence::fence_with_span($family, $operation, move || $body)
    };
}

pub(crate) use fenced;
pub(crate) use fenced_span;

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;
    use pyo3::exceptions::PyRuntimeError;

    /// A string panic becomes a catchable exception with preserved text.
    #[test]
    fn fence_converts_str_panic_to_pyspark_exception_with_message_preserved() {
        Python::attach(|py| {
            let result: Result<(), PyErr> = fence("Probe.op", || panic!("boom-str-payload"));
            let error = result.expect_err("a panicking body must produce an Err");
            assert!(
                error.is_instance_of::<PySparkException>(py),
                "a fenced panic is the base PySparkException"
            );
            assert!(
                error.is_instance_of::<PyRuntimeError>(py),
                "PySparkException subclasses RuntimeError (near-drop-in), never BaseException-only"
            );
            let message = error.to_string();
            assert!(
                message.contains("boom-str-payload"),
                "the panic payload text is preserved: {message}"
            );
            assert!(
                message.contains("Probe.op") && message.contains("internal error"),
                "the framing names the operation and marks it internal: {message}"
            );
        });
    }

    #[test]
    fn fence_converts_string_panic_payload() {
        Python::attach(|_py| {
            let detail = "dynamic-detail-42".to_string();
            let result: Result<(), PyErr> = fence("Probe.op", || panic!("{detail}"));
            let error = result.expect_err("panicking body produces Err");
            let message = error.to_string();
            assert!(
                message.contains("dynamic-detail-42"),
                "a String panic payload is preserved: {message}"
            );
        });
    }

    #[test]
    fn fence_passes_through_a_non_panicking_body() {
        Python::attach(|_py| {
            let result: Result<i64, PyErr> = fence("Probe.op", || Ok(7));
            assert_eq!(result.expect("no panic, value passes through"), 7);
        });
    }

    /// A non-string payload receives a stable label.
    #[test]
    fn fence_degrades_non_string_panic_payload_to_stable_label() {
        Python::attach(|py| {
            let result: Result<(), PyErr> = fence("Probe.op", || std::panic::panic_any(123_u32));
            let error = result.expect_err("panicking body produces Err");
            assert!(error.is_instance_of::<PySparkException>(py));
            let message = error.to_string();
            assert!(
                message.contains("non-string panic payload"),
                "non-string payloads get a stable label: {message}"
            );
        });
    }

    /// A stream-poll panic yields one Arrow error item.
    #[test]
    fn fence_stream_poll_converts_panic_to_terminal_arrow_error() {
        let item = fence_stream_poll("Stream.poll", || panic!("stream-poll-boom"));
        let error = item
            .expect("a fenced panic yields a terminal Some(item)")
            .expect_err("the item is the Err arm, never Ok");
        assert!(
            error.to_string().contains("stream-poll-boom"),
            "the panic text rides the Arrow error channel: {error}"
        );
        assert!(
            error.to_string().contains("Stream.poll"),
            "the framing names the poll operation: {error}"
        );
    }

    #[test]
    fn fence_stream_poll_passes_through_end_of_stream() {
        let item = fence_stream_poll("Stream.poll", || None);
        assert!(
            item.is_none(),
            "None (end of stream) passes through untouched"
        );
    }

    struct FamilyOperationVisitor<'a> {
        family: &'a mut String,
        operation: &'a mut String,
    }

    impl tracing::field::Visit for FamilyOperationVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            let text = format!("{value:?}");
            let unquoted = text
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(text.as_str())
                .to_string();
            match field.name() {
                "family" => *self.family = unquoted,
                "operation" => *self.operation = unquoted,
                _ => {}
            }
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            match field.name() {
                "family" => *self.family = value.to_string(),
                "operation" => *self.operation = value.to_string(),
                _ => {}
            }
        }
    }

    struct FieldCapture {
        records: std::sync::Mutex<Vec<(String, String, String)>>,
    }

    struct CaptureLayer {
        capture: std::sync::Arc<FieldCapture>,
    }

    impl<S> tracing_subscriber::Layer<S> for CaptureLayer
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if attrs.metadata().name() != "py.entry" {
                return;
            }
            let mut family = String::new();
            let mut operation = String::new();
            attrs.record(&mut FamilyOperationVisitor {
                family: &mut family,
                operation: &mut operation,
            });
            self.capture.records.lock().expect("capture lock").push((
                attrs.metadata().name().to_string(),
                family,
                operation,
            ));
        }
    }

    /// The span records family and operation while the body value passes through.
    #[test]
    fn fence_with_span_emits_py_entry_family_and_passes_through() {
        use tracing_subscriber::layer::SubscriberExt;

        let capture = std::sync::Arc::new(FieldCapture {
            records: std::sync::Mutex::new(Vec::new()),
        });
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(CaptureLayer {
                capture: std::sync::Arc::clone(&capture),
            }));

        let value: i64 = fence_with_span("py.action", "PyDataFrame.count", || Ok(42))
            .expect("body passes through");
        assert_eq!(value, 42);

        let records = capture.records.lock().expect("capture lock").clone();
        assert!(
            records.iter().any(|(name, family, operation)| {
                name == "py.entry" && family == "py.action" && operation == "PyDataFrame.count"
            }),
            "expected py.entry family=py.action operation=PyDataFrame.count; got {records:?}"
        );
    }

    /// Span fields remain static labels and never carry user secrets.
    #[test]
    fn fence_with_span_fields_are_static_labels_only() {
        use tracing_subscriber::layer::SubscriberExt;

        const SECRET: &str = "SUPER_SECRET_VALUE_do_not_leak_obs1";

        let capture = std::sync::Arc::new(FieldCapture {
            records: std::sync::Mutex::new(Vec::new()),
        });
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(CaptureLayer {
                capture: std::sync::Arc::clone(&capture),
            }));

        let result: Result<(), PyErr> = fence_with_span("py.sql", "PyReparkSession.sql", || Ok(()));
        assert!(result.is_ok());

        let records = capture.records.lock().expect("capture lock").clone();
        assert!(
            !records.is_empty(),
            "py.entry must fire so field values can be audited: {records:?}"
        );
        for (name, family, operation) in &records {
            assert!(
                !family.contains(SECRET) && !operation.contains(SECRET),
                "span {name} field leaked secret ({SECRET}): family={family:?} operation={operation:?}"
            );
            assert_eq!(family, "py.sql");
            assert_eq!(operation, "PyReparkSession.sql");
        }
    }
}
