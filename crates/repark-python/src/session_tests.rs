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

fn native_collect(
    session: &PyReparkSession,
    py: Python<'_>,
    query: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
    let frame = session
        .sql(py, query)
        .unwrap_or_else(|error| panic!("the native door plans {query}: {error}"));
    let collected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        frame
            .runtime_handle()
            .block_on(frame.inner().clone().collect())
    }));
    let answered = collected.unwrap_or_else(|_| panic!("the native door panicked on {query}"));
    answered.unwrap_or_else(|error| panic!("the native door answers {query}: {error}"))
}

fn map_entries(batch: &arrow::record_batch::RecordBatch, row: usize) -> Vec<(String, i64)> {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Int32Type, Int64Type};

    let map = batch.column(0).as_map();
    let entries = map.value(row);
    let keys = entries.column(0).as_string::<i32>();
    let values = entries.column(1);
    (0..entries.len())
        .map(|index| {
            let value = match values.data_type() {
                arrow::datatypes::DataType::Int32 => {
                    i64::from(values.as_primitive::<Int32Type>().value(index))
                }
                _ => values.as_primitive::<Int64Type>().value(index),
            };
            (keys.value(index).to_string(), value)
        })
        .collect()
}

#[test]
fn native_session_answers_a_map_over_a_column_instead_of_panicking() {
    Python::attach(|py| {
        let session =
            PyReparkSession::native(py, None, None, None, None).expect("native session builds");
        let batches = native_collect(&session, py, "SELECT map('a', id) AS m FROM range(1)");
        assert_eq!(
            batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        assert_eq!(map_entries(&batches[0], 0), vec![("a".to_string(), 0)]);
    });
}

#[test]
fn native_session_answers_a_per_row_map_with_two_pairs() {
    Python::attach(|py| {
        let session =
            PyReparkSession::native(py, None, None, None, None).expect("native session builds");
        let batches = native_collect(
            &session,
            py,
            "SELECT map('a', id, 'b', id + 1) AS m FROM range(3) ORDER BY id",
        );
        assert_eq!(
            batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            3
        );
        let rows: Vec<Vec<(String, i64)>> = batches
            .iter()
            .flat_map(|batch| (0..batch.num_rows()).map(|row| map_entries(batch, row)))
            .collect();
        for (id, entries) in (0_i64..).zip(&rows) {
            assert_eq!(
                entries,
                &vec![("a".to_string(), id), ("b".to_string(), id + 1)]
            );
        }
    });
}

#[test]
fn native_session_keeps_a_constant_map_on_the_datafusion_constructor() {
    Python::attach(|py| {
        let session =
            PyReparkSession::native(py, None, None, None, None).expect("native session builds");
        let batches = native_collect(&session, py, "SELECT map('a', 1) AS m");
        assert_eq!(map_entries(&batches[0], 0), vec![("a".to_string(), 1)]);
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
fn read_postgres_refusals_name_their_row_and_never_echo_credentials() {
    Python::attach(|py| {
        let session =
            PyReparkSession::new(py, None, None, None, None, None).expect("session builds");
        let properties =
            HashMap::from([("password".to_owned(), "sentinel-property-secret".to_owned())]);
        let Err(partitioned) = session.read_postgres(
            py,
            "postgresql://user:sentinel-secret@203.0.113.1:5432/db",
            Some("public.t"),
            None,
            Some(properties.clone()),
            None,
            None,
            None,
            None,
            Some(vec!["id > 0".to_owned()]),
        ) else {
            panic!("a `predicates` read stays declared")
        };
        let Err(incomplete) = session.read_postgres(
            py,
            "postgresql://user:sentinel-secret@203.0.113.1:5432/db",
            Some("public.t"),
            None,
            Some(properties.clone()),
            Some("id"),
            Some(0),
            None,
            Some(-1),
            None,
        ) else {
            panic!("three of the four options refuse before any connection")
        };
        assert!(incomplete.is_instance_of::<crate::IllegalArgumentException>(py));
        let incomplete = incomplete.to_string();
        assert!(
            incomplete.contains("users need to specify all or none"),
            "{incomplete}"
        );
        assert!(!incomplete.contains("sentinel"), "{incomplete}");
        assert!(partitioned.is_instance_of::<crate::UnsupportedOperationException>(py));
        let unknown_properties = HashMap::from([
            ("password".to_owned(), "sentinel-property-secret".to_owned()),
            ("bogusKey".to_owned(), "sentinel-value".to_owned()),
        ]);
        let Err(unknown) = session.read_postgres(
            py,
            "postgresql://user:sentinel-secret@203.0.113.1:5432/db",
            Some("public.t"),
            None,
            Some(unknown_properties),
            None,
            None,
            None,
            None,
            None,
        ) else {
            panic!("an unknown property refuses before any connection")
        };
        let partitioned = partitioned.to_string();
        let unknown = unknown.to_string();
        assert!(
            partitioned.contains("CONNECT-DECL-pg-partitioned-read")
                && partitioned.contains("`predicates`")
                && !partitioned.contains("id > 0"),
            "{partitioned}"
        );
        assert!(unknown.contains("bogusKey"), "{unknown}");
        for message in [partitioned, unknown] {
            assert!(message.contains("database source `jdbc`"), "{message}");
            for secret in [
                "sentinel-secret",
                "sentinel-property-secret",
                "sentinel-value",
                "postgresql://",
            ] {
                assert!(!message.contains(secret), "{message}");
            }
        }
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

const SEAT_EMPTY_DML: [&str; 6] = [
    "DELETE FROM seat_probe WHERE false",
    "DELETE FROM seat_probe WHERE 1 = 0",
    "DELETE FROM seat_probe WHERE id IS NULL",
    "UPDATE seat_probe SET flag = 'z' WHERE false",
    "UPDATE seat_probe SET flag = 'z' WHERE NULL",
    "UPDATE seat_probe SET flag = 'z' WHERE flag = NULL",
];

fn seat_probe_table() -> Arc<datafusion::datasource::MemTable> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("flag", DataType::Utf8, true),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("seat probe rows");
    Arc::new(
        datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
            .expect("seat probe table"),
    )
}

async fn seat_answer(
    context: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
    let frame = context
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

fn seat_column(batches: &[arrow::record_batch::RecordBatch]) -> Vec<String> {
    let mut values = Vec::new();
    for batch in batches {
        for index in 0..batch.num_rows() {
            let cell = arrow::util::display::array_value_to_string(batch.column(0), index)
                .expect("render");
            values.push(cell);
        }
    }
    values
}

pub(crate) fn assert_the_context_plans_an_empty_dml_as_a_no_op(
    context: &datafusion::prelude::SessionContext,
    label: &str,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        context
            .register_table("seat_probe", seat_probe_table())
            .expect("register seat probe");
        for sql in SEAT_EMPTY_DML {
            let counts = seat_column(&seat_answer(context, sql).await);
            assert_eq!(counts, vec!["0".to_string()], "{label}: {sql}");
        }
        let rows = seat_answer(context, "SELECT flag FROM seat_probe ORDER BY id").await;
        assert_eq!(
            seat_column(&rows),
            vec!["a", "b", "c"],
            "{label}: rows survive"
        );
        let selective = seat_answer(context, "DELETE FROM seat_probe WHERE id = 1").await;
        assert_eq!(
            seat_column(&selective),
            vec!["1".to_string()],
            "{label}: control"
        );
        let rows = seat_answer(context, "SELECT flag FROM seat_probe ORDER BY id").await;
        assert_eq!(
            seat_column(&rows),
            vec!["b", "c"],
            "{label}: control deleted one row"
        );
    });
}

#[test]
fn every_python_door_session_plans_an_empty_dml_as_a_no_op() {
    Python::attach(|py| {
        let facade =
            PyReparkSession::new(py, None, None, None, None, None).expect("facade session");
        assert_the_context_plans_an_empty_dml_as_a_no_op(facade.session.context(), "facade");
        let native = PyReparkSession::native(py, None, None, None, None).expect("native session");
        assert_the_context_plans_an_empty_dml_as_a_no_op(native.session.context(), "native");
    });
}

#[test]
fn the_native_door_answers_a_zero_count_for_an_empty_dml() {
    Python::attach(|py| {
        let session = PyReparkSession::native(py, None, None, None, None).expect("native session");
        session
            .session
            .context()
            .register_table("seat_probe", seat_probe_table())
            .expect("register seat probe");
        for sql in SEAT_EMPTY_DML {
            let frame = session.sql(py, sql).expect(sql);
            let batches = frame
                .runtime_handle()
                .block_on(frame.inner().clone().collect())
                .expect(sql);
            assert_eq!(seat_column(&batches), vec!["0".to_string()], "{sql}");
        }
        let frame = session
            .sql(py, "SELECT flag FROM seat_probe ORDER BY id")
            .expect("read");
        let batches = frame
            .runtime_handle()
            .block_on(frame.inner().clone().collect())
            .expect("read");
        assert_eq!(seat_column(&batches), vec!["a", "b", "c"]);
    });
}
