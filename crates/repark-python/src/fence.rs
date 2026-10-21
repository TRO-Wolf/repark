//! Shared PyO3 and Arrow C-stream panic fences.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use pyo3::prelude::PyErr;

use crate::PySparkException;

pub(crate) const PANIC_FATAL_ENVIRONMENT_VARIABLE: &str = "REPARK_PANIC_FATAL";

/// Report whether a caught panic must end the run instead of becoming an error.
fn panics_are_fatal() -> bool {
    std::env::var_os(PANIC_FATAL_ENVIRONMENT_VARIABLE).is_some_and(|value| value == "1")
}

/// Extract panic text and add the boundary operation to the internal-error message.
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

/// Run a `#[pymethods]` body under the panic fence.
///
/// With `REPARK_PANIC_FATAL=1` a caught panic resumes unwinding instead of becoming an error.
pub(crate) fn fence<T>(
    operation: &str,
    body: impl FnOnce() -> Result<T, PyErr>,
) -> Result<T, PyErr> {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(result) => result,
        Err(payload) => {
            if panics_are_fatal() {
                std::panic::resume_unwind(payload);
            }
            Err(PySparkException::new_err(
                crate::exceptions::mask_user_visible(describe_panic(operation, payload.as_ref())),
            ))
        }
    }
}

/// Fence a method body and record its static entry-point family and operation.
pub(crate) fn fence_with_span<T>(
    family: &'static str,
    operation: &'static str,
    body: impl FnOnce() -> Result<T, PyErr>,
) -> Result<T, PyErr> {
    let span = tracing::info_span!("py.entry", family, operation);
    let _guard = span.enter();
    fence(operation, body)
}

/// A caught-panic error carried on the Arrow C-stream error channel.
#[derive(Debug)]
struct FencedStreamPanic(String);

impl std::fmt::Display for FencedStreamPanic {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for FencedStreamPanic {}

pub(crate) fn fenced_panic_detail(error: &ArrowError) -> Option<&str> {
    match error {
        ArrowError::ExternalError(inner) => inner
            .downcast_ref::<FencedStreamPanic>()
            .map(|caught| caught.0.as_str()),
        _ => None,
    }
}

/// Run one Arrow-C-stream batch poll under the panic fence.
///
/// With `REPARK_PANIC_FATAL=1` a caught panic resumes unwinding instead of becoming an error item.
pub(crate) fn fence_stream_poll(
    operation: &str,
    body: impl FnOnce() -> Option<Result<RecordBatch, ArrowError>>,
) -> Option<Result<RecordBatch, ArrowError>> {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(item) => item,
        Err(payload) => {
            if panics_are_fatal() {
                std::panic::resume_unwind(payload);
            }
            Some(Err(ArrowError::ExternalError(Box::new(FencedStreamPanic(
                describe_panic(operation, payload.as_ref()),
            )))))
        }
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

    const PROBE_CHILD_ENVIRONMENT_VARIABLE: &str = "REPARK_FENCE_PROBE_CHILD";
    const CONVERTING_PROBE: &str = "fence::tests::probe_child_converts_a_fenced_panic";
    const STREAM_PROBE: &str = "fence::tests::probe_child_converts_a_stream_panic";
    const FENCED_MESSAGE: &str = "repark internal error in Probe.op: probe-boom (a Rust panic was \
         caught at the Python boundary; this is a bug \u{2014} please report it)";

    fn run_probe_child(test_name: &str, fatal: bool) -> std::process::Output {
        let mut command =
            std::process::Command::new(std::env::current_exe().expect("test binary path"));
        command
            .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
            .env(PROBE_CHILD_ENVIRONMENT_VARIABLE, "1")
            .env_remove(PANIC_FATAL_ENVIRONMENT_VARIABLE);
        if fatal {
            command.env(PANIC_FATAL_ENVIRONMENT_VARIABLE, "1");
        }
        command.output().expect("the test binary starts")
    }

    fn combined_output(output: &std::process::Output) -> String {
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    #[test]
    fn probe_child_converts_a_fenced_panic() {
        if std::env::var_os(PROBE_CHILD_ENVIRONMENT_VARIABLE).is_none() {
            return;
        }
        Python::attach(|py| {
            let result: Result<(), PyErr> = fence("Probe.op", || panic!("probe-boom"));
            let error = result.expect_err("the flag is off, so the panic becomes an error");
            assert_eq!(error.value(py).to_string(), FENCED_MESSAGE);
        });
    }

    #[test]
    fn probe_child_converts_a_stream_panic() {
        if std::env::var_os(PROBE_CHILD_ENVIRONMENT_VARIABLE).is_none() {
            return;
        }
        let item = fence_stream_poll("Probe.op", || panic!("probe-boom"));
        let error = item
            .expect("a fenced panic yields an item")
            .expect_err("the flag is off, so the panic becomes an error item");
        assert!(error.to_string().contains(FENCED_MESSAGE), "{error}");
    }

    #[test]
    fn a_caught_panic_keeps_its_message_when_the_fatal_flag_is_off() {
        for probe in [CONVERTING_PROBE, STREAM_PROBE] {
            let output = run_probe_child(probe, false);
            let text = combined_output(&output);
            assert!(output.status.success(), "{probe}: {text}");
            assert!(text.contains("1 passed"), "{probe} did not run: {text}");
        }
    }

    #[test]
    fn a_caught_panic_fails_the_run_when_the_fatal_flag_is_on() {
        for probe in [CONVERTING_PROBE, STREAM_PROBE] {
            let output = run_probe_child(probe, true);
            let text = combined_output(&output);
            assert!(!output.status.success(), "{probe}: {text}");
            assert!(text.contains("probe-boom"), "{probe}: {text}");
            assert!(!text.contains("repark internal error"), "{probe}: {text}");
            assert!(text.contains("1 failed"), "{probe} did not run: {text}");
        }
    }

    #[test]
    fn only_the_value_one_turns_the_fatal_flag_on() {
        for (value, expected) in [("1", true), ("0", false), ("", false), ("true", false)] {
            let mut command =
                std::process::Command::new(std::env::current_exe().expect("test binary path"));
            command
                .args([
                    "--exact",
                    CONVERTING_PROBE,
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(PROBE_CHILD_ENVIRONMENT_VARIABLE, "1")
                .env(PANIC_FATAL_ENVIRONMENT_VARIABLE, value);
            let output = command.output().expect("the test binary starts");
            assert_eq!(!output.status.success(), expected, "value {value:?}");
        }
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
