use crate::session::*;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn spark_doored_session_resolves_spark_function_and_routes_spark_statement() {
    Python::attach(|py| {
        let session =
            PyReparkSession::new(py, None, None, None, None, None).expect("session builds");

        let frame = session
            .sql(py, "SELECT weekofyear(DATE '2021-01-01') AS w")
            .expect("a Spark-only function resolves — SparkExtension installed the registry");
        let batches = frame
            .runtime_handle()
            .block_on(frame.inner().clone().collect())
            .expect("the Spark function evaluates");
        let weeks = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("weekofyear returns Int32");
        assert_eq!(
            weeks.value(0),
            53,
            "weekofyear must carry SPARK's ISO week-year semantics, not a DataFusion default"
        );

        let sql = "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE OUTPUT d.*";
        let Err(routed) = session.sql(py, sql) else {
            panic!("MERGE OUTPUT is a loud router refusal, not a plan")
        };
        let message = routed.to_string();
        assert!(
            message.contains("MERGE OUTPUT/RETURNING"),
            "the routed refusal names the MERGE OUTPUT gap; got: {message}"
        );
        assert!(
            routed.is_instance_of::<crate::UnsupportedOperationException>(py),
            "the router's NotImplemented folds to UnsupportedOperationException: {message}"
        );
    });
}

#[test]
fn native_session_is_not_spark_doored() {
    Python::attach(|py| {
        let session =
            PyReparkSession::native(py, None, None, None, None).expect("native session builds");

        let frame = session
            .sql(py, "SELECT CAST(5 AS INT) / CAST(2 AS INT) AS q")
            .expect("native integer division plans");
        let batches = frame
            .runtime_handle()
            .block_on(frame.inner().clone().collect())
            .expect("native integer division evaluates");
        let quotients = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("native INT/INT is Int32 (truncated), not Spark Float64");
        assert_eq!(
            quotients.value(0),
            2,
            "ANSI / DataFusion integer / truncates"
        );

        let Err(missing) = session.sql(py, "SELECT weekofyear(DATE '2021-01-01') AS w") else {
            panic!("weekofyear must not resolve on a native session");
        };
        let message = missing.to_string();
        assert!(
            message.to_ascii_lowercase().contains("weekofyear")
                || message.to_ascii_lowercase().contains("invalid function"),
            "native session must lack the Spark function registry; got: {message}"
        );
    });
}

#[test]
fn read_excel_refuses_with_named_unsupported_operation() {
    Python::attach(|py| {
        let session =
            PyReparkSession::new(py, None, None, None, None, None).expect("session builds");
        let Err(error) = session.read_excel(py, "/tmp/never-opened.xlsx", None) else {
            panic!("the excel reader is deferred post-milestone-one — it must not return a frame")
        };
        assert!(
            error.is_instance_of::<crate::UnsupportedOperationException>(py),
            "a deferred surface raises UnsupportedOperationException"
        );
        assert!(
            error.is_instance_of::<crate::PySparkException>(py),
            "…which is still a PySparkException (hence a RuntimeError) — near-drop-in"
        );
        let message = error.to_string();
        assert!(
            message.contains("spark.read.excel"),
            "the message names the refused SURFACE: {message}"
        );
        assert!(
            message.contains("post-milestone-one"),
            "the message states the schedule: {message}"
        );
        assert!(
            message.contains("task/todo.md"),
            "the message points at the tracking row: {message}"
        );
    });
}

#[test]
fn excel_sheet_names_refuses_with_named_unsupported_operation() {
    Python::attach(|py| {
        let session =
            PyReparkSession::new(py, None, None, None, None, None).expect("session builds");
        let error = session
            .excel_sheet_names(py, "/tmp/never-opened.xlsx")
            .expect_err("the excel reader is deferred post-milestone-one");
        assert!(error.is_instance_of::<crate::UnsupportedOperationException>(py));
        let message = error.to_string();
        assert!(
            message.contains("spark.read.sheet_names"),
            "the message names the refused SURFACE: {message}"
        );
        assert!(
            message.contains("post-milestone-one") && message.contains("task/todo.md"),
            "the message states the schedule and the tracking row: {message}"
        );
    });
}

#[test]
fn read_postgres_refuses_with_named_unsupported_operation() {
    Python::attach(|py| {
        let session =
            PyReparkSession::new(py, None, None, None, None, None).expect("session builds");
        let Err(error) = session.read_postgres(
            py,
            "postgresql://user:sentinel-secret@host:5432/db",
            Some("public.t"),
            None,
            Some(HashMap::from([(
                "password".to_owned(),
                "sentinel-property-secret".to_owned(),
            )])),
            None,
            None,
            None,
            None,
            None,
        ) else {
            panic!("the postgres connector is deferred post-milestone-one — no frame is returned")
        };
        assert!(error.is_instance_of::<crate::UnsupportedOperationException>(py));
        let message = error.to_string();
        assert!(
            message.contains("spark.read.jdbc"),
            "the message names the refused SURFACE: {message}"
        );
        assert!(
            message.contains("post-milestone-one") && message.contains("task/todo.md"),
            "the message states the schedule and the tracking row: {message}"
        );
        assert!(
            !message.contains("sentinel-secret") && !message.contains("postgresql://"),
            "a refusal must never echo the connection URL — it may carry credentials: \
             {message}"
        );
        assert!(
            !message.contains("sentinel-property-secret") && !message.contains("password"),
            "a refusal must never echo the connection PROPERTIES — they may carry \
             credentials: {message}"
        );
    });
}

#[test]
fn fenced_panic_surfaces_as_pyspark_exception_and_leaves_session_usable() {
    Python::attach(|py| {
        let session = Py::new(
            py,
            PyReparkSession::new(py, None, None, None, None, None).expect("session builds"),
        )
        .expect("pyclass instantiates");

        let error = session
            .call_method0(py, "panic_probe")
            .expect_err("the probe deterministically panics through the fence");
        assert!(
            error.is_instance_of::<crate::PySparkException>(py),
            "a fenced pymethod panic is the base PySparkException"
        );
        assert!(
            error.is_instance_of::<pyo3::exceptions::PyRuntimeError>(py),
            "PySparkException subclasses RuntimeError — `except RuntimeError` still catches it"
        );
        assert!(
            !error.is_instance_of::<pyo3::panic::PanicException>(py),
            "the fence must NOT let PyO3's raw PanicException (a BaseException) escape"
        );
        let message = error.to_string();
        assert!(
            message.contains("SAF-007 injected panic") && message.contains("internal error"),
            "the panic text is preserved under the internal-error framing: {message}"
        );

        let frame = session
            .borrow(py)
            .sql(py, "SELECT 1 AS n")
            .expect("the session still plans and runs queries after a fenced panic");
        assert_eq!(
            frame.count(py).expect("count executes"),
            1,
            "the session remains usable after a fenced panic (nothing poisoned)"
        );
    });
}

#[test]
fn sequential_sessions_share_one_tokio_runtime() {
    Python::attach(|py| {
        let first = PyReparkSession::new(py, None, None, None, None, None).expect("first session");
        let second =
            PyReparkSession::new(py, None, None, None, None, None).expect("second session");
        assert!(
            Arc::ptr_eq(&first.runtime_arc(), &second.runtime_arc()),
            "two PyReparkSession values must share the process-wide Tokio runtime Arc"
        );
    });
}

struct FamilyFieldVisitor<'a> {
    family: &'a mut String,
}

impl tracing::field::Visit for FamilyFieldVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "family" {
            let text = format!("{value:?}");
            *self.family = text
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(text.as_str())
                .to_string();
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "family" {
            *self.family = value.to_string();
        }
    }
}

struct FamilyRecorder {
    families: std::sync::Mutex<Vec<String>>,
}

struct FamilyLayer {
    recorder: Arc<FamilyRecorder>,
}

impl<S> tracing_subscriber::Layer<S> for FamilyLayer
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
        attrs.record(&mut FamilyFieldVisitor {
            family: &mut family,
        });
        if !family.is_empty() {
            self.recorder
                .families
                .lock()
                .expect("family lock")
                .push(family);
        }
    }
}

#[test]
fn entry_point_families_emit_py_entry_spans() {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tracing_subscriber::layer::SubscriberExt;

    let recorder = Arc::new(FamilyRecorder {
        families: std::sync::Mutex::new(Vec::new()),
    });
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(FamilyLayer {
            recorder: Arc::clone(&recorder),
        }));

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let warehouse = std::env::temp_dir().join(format!("repark-obs1-family-{nanos}"));
    fs::create_dir_all(&warehouse).expect("temp warehouse dir");
    let warehouse_str = warehouse
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();

    Python::attach(|py| {
        let session =
            PyReparkSession::new(py, None, None, None, None, None).expect("session builds");
        let frame = session.sql(py, "SELECT 1 AS n").expect("sql plans");
        assert_eq!(frame.count(py).expect("count"), 1);
        let _ = session.read_parquet(py, "/nonexistent/obs1-family-pin.parquet");
        session
            .register_memory_catalog(py, "obs1_mem", &warehouse_str)
            .expect("memory catalog registers");
        let _ = session.table_exists(py, "no_such_temp_view");
    });

    let _ = fs::remove_dir_all(&warehouse);

    let families = recorder.families.lock().expect("family lock").clone();
    for expected in ["py.session", "py.sql", "py.action", "py.read", "py.catalog"] {
        assert!(
            families.iter().any(|family| family == expected),
            "expected family {expected}; recorded: {families:?}"
        );
    }
}
