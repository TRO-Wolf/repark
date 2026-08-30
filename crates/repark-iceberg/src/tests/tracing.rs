//! Shared test-only tracing harness with one global subscriber per test binary.

use std::cell::RefCell;
use std::sync::{Arc, Mutex, Once, OnceLock};

use tracing::span::{Attributes, Id};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// One recorded span field: `(field name, rendered value)`.
pub(crate) type SpanField = (String, String);
/// One recorded span: `(span name, fields)`.
pub(crate) type SpanEvent = (String, Vec<SpanField>);

/// Captures span names + recorded field strings for the thread that began capture.
pub(crate) struct SpanFieldCapture {
    events: Mutex<Vec<SpanEvent>>,
}

impl SpanFieldCapture {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
        })
    }

    /// Snapshot of every `catalog.*` span recorded on the capturing thread so far.
    pub(crate) fn snapshot(&self) -> Vec<SpanEvent> {
        self.events.lock().expect("span capture lock").clone()
    }
}

struct FieldCollector<'a> {
    fields: &'a mut Vec<SpanField>,
}

impl tracing::field::Visit for FieldCollector<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields
            .push((field.name().to_string(), format!("{value:?}")));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
}

thread_local! {
    /// Where `SpanFieldLayer` writes for the current test thread (`None` = not capturing).
    static CAPTURE_SLOT: RefCell<Option<Arc<SpanFieldCapture>>> =
        const { RefCell::new(None) };
}

/// Begin capturing `catalog.*` spans on this thread; installs the shared subscriber if needed.
pub(crate) fn begin_catalog_capture() -> Arc<SpanFieldCapture> {
    install();
    let capture = SpanFieldCapture::new();
    CAPTURE_SLOT.with(|slot| *slot.borrow_mut() = Some(Arc::clone(&capture)));
    capture
}

/// Clear this thread's capture slot (guard-drop path) so one test cannot leak into another.
pub(crate) fn clear_catalog_capture_slot() {
    CAPTURE_SLOT.with(|slot| *slot.borrow_mut() = None);
}

struct SpanFieldLayer;

impl<S> Layer<S> for SpanFieldLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        let name = attrs.metadata().name().to_string();
        if !name.starts_with("catalog.") {
            return;
        }
        CAPTURE_SLOT.with(|slot| {
            let Some(capture) = slot.borrow().as_ref().map(Arc::clone) else {
                return; // not a capturing thread — every other test lands here
            };
            let mut fields = Vec::new();
            attrs.record(&mut FieldCollector {
                fields: &mut fields,
            });
            capture
                .events
                .lock()
                .expect("span capture lock")
                .push((name, fields));
        });
    }
}

// -------------------------------------------------------------------------------------------

const TEST_ROOT: &str = "merge.trace_test_root";

/// Records creation order of `merge.*` spans that descend from [`TEST_ROOT`].
struct SpanNameRecorder {
    names: Arc<Mutex<Vec<String>>>,
}

impl<S> Layer<S> for SpanNameRecorder
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, ctx: Context<'_, S>) {
        let name = attrs.metadata().name();
        if !name.starts_with("merge.") || name == TEST_ROOT {
            return;
        }
        // Contextual parent (the common case) or an explicit one — either way the span must sit
        let under_root = |id: &Id| {
            ctx.span(id)
                .is_some_and(|span| span.scope().any(|s| s.name() == TEST_ROOT))
        };
        let descends = attrs.parent().map_or_else(
            || {
                ctx.lookup_current()
                    .is_some_and(|span| span.scope().any(|s| s.name() == TEST_ROOT))
            },
            under_root,
        );
        if descends {
            self.names
                .lock()
                .expect("span name lock")
                .push(name.to_string());
        }
    }
}

static MERGE_SPAN_NAMES: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();

fn merge_span_names_slot() -> &'static Arc<Mutex<Vec<String>>> {
    MERGE_SPAN_NAMES.get_or_init(|| Arc::new(Mutex::new(Vec::new())))
}

/// The shared record of root-descended `merge.*` span names; installs the subscriber if needed.
pub(crate) fn merge_span_names() -> Arc<Mutex<Vec<String>>> {
    install();
    Arc::clone(merge_span_names_slot())
}

// ------------------------------------------------------------------------------------------- The

/// Install the shared global subscriber (both layers) exactly once; repeat calls are no-ops, and a
pub(crate) fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let recorder = SpanNameRecorder {
            names: Arc::clone(merge_span_names_slot()),
        };
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(SpanFieldLayer)
                .with(recorder),
        );
    });
}
