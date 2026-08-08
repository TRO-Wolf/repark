//! The shared PyO3 panic fence (SAF-007).
//!
//! A Rust panic must never unwind across the Python / FFI boundary. Two boundary shapes exist in
//! this crate, and both route through the ONE catch-and-frame core here:
//!
//! 1. **`#[pymethods]` entry points** — [`fence`] wraps a method body returning [`PyResult<T>`].
//!    PyO3's own trampoline already `catch_unwind`s a pymethod panic, but it raises
//!    [`pyo3::panic::PanicException`], which derives from `BaseException`
//!    (`pyo3-0.29.0/src/panic.rs`) — so `except Exception` / `except RuntimeError` (repark's
//!    near-drop-in contract) do NOT catch it and a migrated program dies. [`fence`] catches the
//!    panic FIRST and re-raises it as the base [`crate::PySparkException`] (a `RuntimeError`), with
//!    the panic message preserved and an "internal error" framing.
//! 2. **The Arrow C-stream `get_next` callback** — [`fence_stream_poll`] wraps
//!    `StreamingBatchReader::next` (in [`crate::dataframe`]). That poll is invoked by
//!    `arrow`'s `unsafe extern "C" fn get_next` (`arrow-array-57.3.1/src/ffi_stream.rs`), which does
//!    NOT `catch_unwind`; an escaping panic unwinds across `extern "C"` → **process abort**. This is
//!    the genuine SAF-007 abort, and PyO3's trampoline does not cover it (the pulls happen after the
//!    `__arrow_c_stream__` pymethod returned the capsule). [`fence_stream_poll`] turns the panic into
//!    a terminal [`ArrowError`] on the stream's error channel, so the consumer sees a clean error and
//!    the process survives. The facade `DataFrame.to_arrow` maps that `ArrowException` back to the
//!    base [`crate::PySparkException`], so the two shapes converge on one Python-visible taxonomy.
//!
//! Message extraction mirrors PyO3's own `PanicException::from_panic_payload`: a panic payload is
//! almost always `&'static str` or `String`; anything else degrades to a stable label.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use pyo3::prelude::PyErr;

use crate::PySparkException;

/// ===========================================================================================
/// Extract a human-readable detail string from a caught panic payload, then wrap it in the shared
/// "internal error" framing that names the boundary operation.
///
/// Mirrors [`pyo3::panic::PanicException::from_panic_payload`]'s downcast order (`&str`, then
/// `String`) so the recovered text matches what a raw panic would have printed; a non-string payload
/// (rare — a `panic_any` with a custom type) degrades to a stable label rather than being lost.
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
/// On success the body's [`PyResult`] passes through unchanged. On a Rust panic the unwind is caught
/// here (never reaching PyO3's trampoline, so it is never a `BaseException`/`PanicException`) and
/// converted into a base [`crate::PySparkException`] (a `RuntimeError`) whose message preserves the
/// panic text under the shared "internal error" framing ([`describe_panic`]).
///
/// [`AssertUnwindSafe`] is required because the boundary closures capture non-[`UnwindSafe`] engine
/// handles (`DataFrame`, `ReparkSession`, `Bound<'_, PyAny>`); the assertion is sound because a
/// caught panic here does not observe any partially-mutated state — the fence returns an error and
/// the caller (Python) receives a fresh exception, never a torn value.
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
/// QUAL-05 / OBS1: panic-fence a `#[pymethods]` body under a named **entry-point family** span.
///
/// Span name is the family (`py.session`, `py.sql`, `py.read`, `py.action`, `py.catalog`, …);
/// `operation` is recorded as a field so a hang can be localized to Python-side vs engine without
/// a debugger. Additive only — same control flow as [`fence`]. Zero overhead when no subscriber.
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
/// [`ArrowError::ExternalError`] needs a `Box<dyn Error>`; this newtype carries the framed panic
/// message so a fenced stream-poll panic surfaces to the consumer exactly like any other engine
/// stream error (whose text the facade preserves), never as an abort.
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
/// The poll is invoked by `arrow`'s `extern "C"` `get_next` (see module docs / SAF-007): an
/// escaping panic would abort the process. On a panic this returns a single terminal
/// `Some(Err(ArrowError))` carrying the framed panic message, so the stream ends cleanly and the
/// interpreter survives; `None`/`Some(Ok(..))`/`Some(Err(..))` from the body pass through unchanged.
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

/// Wrap a `#[pymethods]` body in the shared [`fence`]. Keeps every call site to a single named
/// operation string + its existing body — the guard logic lives once, in [`fence`].
macro_rules! fenced {
    ($operation:literal, $body:block) => {
        $crate::fence::fence($operation, move || $body)
    };
}

/// QUAL-05 / OBS1: [`fenced!`] plus a family span (`py.entry` with `family` + `operation` fields).
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

    /// PL-1: a panicking closure with a `&str` payload becomes `Err(PySparkException)` — a
    /// `RuntimeError` (near-drop-in: `except RuntimeError` still catches), NOT a `BaseException` —
    /// and the panic text is preserved verbatim inside the framed message.
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

    /// PL-1: a `String` payload (the `format!`-style panic) is preserved too.
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

    /// PL-1: the happy path is a pure pass-through — no double-wrapping, value intact.
    #[test]
    fn fence_passes_through_a_non_panicking_body() {
        Python::attach(|_py| {
            let result: Result<i64, PyErr> = fence("Probe.op", || Ok(7));
            assert_eq!(result.expect("no panic, value passes through"), 7);
        });
    }

    /// PL-1: a non-string payload (`panic_any` with a custom type) degrades to a stable label
    /// instead of being lost, and is still a `PySparkException`.
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

    /// PL-4 (unit half): `fence_stream_poll` turns a panicking poll into a terminal
    /// `Some(Err(ArrowError))` carrying the framed panic text — never an unwind. (The
    /// subprocess-isolated end-to-end abort pin lives in `dataframe.rs`.)
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

    /// PL-4 (unit half): a non-panicking poll passes through unchanged (`None` = end of stream).
    #[test]
    fn fence_stream_poll_passes_through_end_of_stream() {
        let item = fence_stream_poll("Stream.poll", || None);
        assert!(
            item.is_none(),
            "None (end of stream) passes through untouched"
        );
    }

    /// Collects `family` / `operation` string fields from a `py.entry` span.
    struct FamilyOperationVisitor<'a> {
        family: &'a mut String,
        operation: &'a mut String,
    }

    impl tracing::field::Visit for FamilyOperationVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            let text = format!("{value:?}");
            // Debug of &str is quoted; strip outer quotes for stable asserts.
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

    /// QUAL-05 / OBS1: `fence_with_span` opens a `py.entry` span with family + operation fields,
    /// and still pass-throughs the body value (additive — no control-flow change).
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

    /// QUAL-05: family span fields are static labels only — never user secrets.
    ///
    /// Compile-time: `fenced_span!` requires literal family/operation. Runtime: capture the
    /// recorded field strings and assert a secret probe never appears in any field value.
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

        // SECRET is only a probe needle for field values — never passed into fence_with_span
        // (the API has no user-data field channel; family/operation are `'static` labels).
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
