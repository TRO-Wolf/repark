use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;

#[test]
fn stream_poll_no_detach_restores_flag_after_panic() {
    assert!(
        !STREAM_POLL_NO_DETACH.with(Cell::get),
        "precondition: flag starts false"
    );
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_stream_poll_no_detach(|| {
            assert!(
                STREAM_POLL_NO_DETACH.with(Cell::get),
                "flag true inside body"
            );
            panic!("injected panic for TLS restore pin");
        });
    }));
    assert!(panicked.is_err(), "body must panic");
    assert!(
        !STREAM_POLL_NO_DETACH.with(Cell::get),
        "flag must be false after panic unwind (Drop guard)"
    );
}

fn int64_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn int64_batch(values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        int64_schema(),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .expect("int64 batch builds")
}

fn int64_values(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("first column is Int64")
        .values()
        .to_vec()
}

fn scripted_stream(
    schema: SchemaRef,
    items: Vec<Result<RecordBatch, DataFusionError>>,
) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(items),
    ))
}

fn reader_test_runtime() -> Arc<Runtime> {
    Arc::new(Runtime::new().expect("tokio runtime builds"))
}

#[derive(Debug)]
struct CountingPartitionStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    produced: Arc<AtomicUsize>,
}

impl PartitionStream for CountingPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let produced = Arc::clone(&self.produced);
        let items: Vec<Result<RecordBatch, DataFusionError>> =
            self.batches.iter().cloned().map(Ok).collect();
        let counted = futures::stream::iter(items).inspect(move |_batch| {
            produced.fetch_add(1, Ordering::SeqCst);
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            counted,
        ))
    }
}

fn import_capsule_stream(capsule: &Bound<'_, PyCapsule>) -> ArrowArrayStreamReader {
    let pointer = capsule
        .pointer_checked(Some(ARROW_STREAM_CAPSULE_NAME))
        .expect("capsule pointer is valid for the arrow stream name")
        .as_ptr()
        .cast::<FFI_ArrowArrayStream>();
    let stream = unsafe { FFI_ArrowArrayStream::from_raw(pointer) };
    ArrowArrayStreamReader::try_new(stream).expect("stream is a valid Arrow C stream")
}

#[test]
fn analyzed_arrow_schema_native_caches_schema_ref_per_handle() {
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let dataframe = context
        .read_batch(int64_batch(&[1, 2, 3]))
        .expect("read_batch");
    let py_dataframe = PyDataFrame::new(dataframe, reader_test_runtime());
    let first = py_dataframe
        .analyzed_arrow_schema_native()
        .expect("first analyze");
    let second = py_dataframe
        .analyzed_arrow_schema_native()
        .expect("cached analyze");
    assert!(
        Arc::ptr_eq(&first, &second),
        "second analyzed_arrow_schema_native must return the same SchemaRef (OnceLock cache)"
    );
    let names = py_dataframe.column_names().expect("column_names");
    assert_eq!(names, vec!["v".to_string()]);
    let third = py_dataframe
        .analyzed_arrow_schema_native()
        .expect("post-columns cache");
    assert!(Arc::ptr_eq(&first, &third));
}

#[test]
fn arrow_c_stream_export_is_lazy_and_does_not_materialize_up_front() {
    Python::attach(|python| {
        let produced = Arc::new(AtomicUsize::new(0));
        let batches = vec![
            int64_batch(&[1, 2]),
            int64_batch(&[3, 4]),
            int64_batch(&[5, 6]),
        ];
        let batch_count = batches.len();

        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let source = CountingPartitionStream {
            schema: int64_schema(),
            batches,
            produced: Arc::clone(&produced),
        };
        let provider = StreamingTable::try_new(int64_schema(), vec![Arc::new(source)])
            .expect("streaming table builds over the counting source");
        let dataframe = context
            .read_table(Arc::new(provider))
            .expect("read_table yields a DataFrame over the counting source");
        let py_dataframe = PyDataFrame::new(dataframe, reader_test_runtime());

        let capsule = py_dataframe
            .__arrow_c_stream__(python, None, None)
            .expect("streaming export returns a capsule");
        assert_eq!(
            produced.load(Ordering::SeqCst),
            0,
            "LAZINESS: nothing is materialized at export time — a collect-then-wrap \
                 __arrow_c_stream__ would have drained all {batch_count} batches before returning"
        );

        let reader = import_capsule_stream(&capsule);
        let drained: Vec<RecordBatch> = reader.map(|batch| batch.expect("batch decodes")).collect();
        let values: Vec<i64> = drained.iter().flat_map(int64_values).collect();
        assert_eq!(
            values,
            vec![1, 2, 3, 4, 5, 6],
            "every value crosses the streaming boundary, in order"
        );
        assert_eq!(
            produced.load(Ordering::SeqCst),
            batch_count,
            "the source is fully consumed only AFTER the consumer drains the stream"
        );
    });
}

#[test]
fn streaming_reader_yields_first_batch_before_a_later_error() {
    Python::attach(|_python| {
        let stream = scripted_stream(
            int64_schema(),
            vec![
                Ok(int64_batch(&[1, 2])),
                Err(DataFusionError::Execution("boom on batch 2".into())),
            ],
        );
        let mut reader =
            StreamingBatchReader::new(reader_test_runtime(), stream, int64_schema(), true);

        let first = reader
            .next()
            .expect("a first item is produced")
            .expect("batch 1 is Ok — delivered BEFORE the later error");
        assert_eq!(
            int64_values(&first),
            vec![1, 2],
            "batch 1 is yielded intact ahead of the batch-2 error"
        );

        let error = reader
            .next()
            .expect("a second item is produced")
            .expect_err("the second poll surfaces the stream error");
        assert!(
            error.to_string().contains("boom on batch 2"),
            "the DataFusion error text rides through ArrowError: {error}"
        );
    });
}

#[test]
fn streaming_reader_preserves_multi_batch_values_and_schema() {
    Python::attach(|_python| {
        let stream = scripted_stream(
            int64_schema(),
            vec![
                Ok(int64_batch(&[1, 2])),
                Ok(int64_batch(&[3])),
                Ok(int64_batch(&[4, 5, 6])),
            ],
        );
        let reader = StreamingBatchReader::new(reader_test_runtime(), stream, int64_schema(), true);

        assert_eq!(
            reader.schema(),
            int64_schema(),
            "declared schema is the physical schema (type surface)"
        );

        let batches: Vec<RecordBatch> = reader
            .map(|batch| batch.expect("each batch decodes"))
            .collect();
        assert_eq!(
            batches.len(),
            3,
            "all three batches stream through — none dropped or merged"
        );
        let concatenated: Vec<i64> = batches.iter().flat_map(int64_values).collect();
        assert_eq!(
            concatenated,
            vec![1, 2, 3, 4, 5, 6],
            "values preserved, in order, across batch boundaries"
        );
        for batch in &batches {
            assert_eq!(
                batch.schema().field(0).data_type(),
                &DataType::Int64,
                "no retype across the streaming boundary"
            );
        }
    });
}

#[test]
fn streaming_reader_surfaces_stream_error_with_message_preserved() {
    Python::attach(|_python| {
        let stream = scripted_stream(
            int64_schema(),
            vec![Err(DataFusionError::Execution("kaboom".into()))],
        );
        let mut reader =
            StreamingBatchReader::new(reader_test_runtime(), stream, int64_schema(), true);

        let error = reader
            .next()
            .expect("the error is delivered as an item, not None")
            .expect_err("a stream error maps to Err(ArrowError), never Ok");
        assert!(
            error.to_string().contains("kaboom"),
            "message preserved through ArrowError::ExternalError: {error}"
        );
        assert!(
            reader.next().is_none(),
            "the stream is exhausted after its single error"
        );
    });
}

#[derive(Debug)]
struct PanicOnPollPartitionStream {
    schema: SchemaRef,
}

impl PartitionStream for PanicOnPollPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let panicking = futures::stream::once(async {
            panic!("SAF-007 injected stream-poll panic");
            #[allow(unreachable_code)]
            Ok(int64_batch(&[0]))
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            panicking,
        ))
    }
}

fn drive_panicking_stream_export_child() {
    Python::attach(|python| {
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let source = PanicOnPollPartitionStream {
            schema: int64_schema(),
        };
        let provider = StreamingTable::try_new(int64_schema(), vec![Arc::new(source)])
            .expect("streaming table builds over the panicking source");
        let dataframe = context
            .read_table(Arc::new(provider))
            .expect("read_table yields a DataFrame over the panicking source");
        let py_dataframe = PyDataFrame::new(dataframe, reader_test_runtime());

        let capsule = py_dataframe
            .__arrow_c_stream__(python, None, None)
            .expect("export returns a capsule (execute_stream is lazy — no poll yet)");
        let mut reader = import_capsule_stream(&capsule);

        match reader.next() {
            Some(Ok(_)) => panic!("the panicking source must never yield a clean batch"),
            Some(Err(error)) => assert!(
                error
                    .to_string()
                    .contains("SAF-007 injected stream-poll panic"),
                "the fenced panic text rides the Arrow error channel: {error}"
            ),
            None => {
                panic!("expected the fenced panic as an Arrow error item, got end-of-stream")
            }
        }
    });
}

#[test]
fn arrow_stream_poll_panic_is_fenced_not_aborting_subprocess_isolated() {
    const CHILD_ENV: &str = "REPARK_SAF007_STREAM_CHILD";
    if std::env::var_os(CHILD_ENV).is_some() {
        drive_panicking_stream_export_child();
        return;
    }
    let exe = std::env::current_exe().expect("locate the current test binary");
    let output = std::process::Command::new(exe)
        .args([
            "--exact",
            "--nocapture",
            "dataframe::tests::arrow_stream_poll_panic_is_fenced_not_aborting_subprocess_isolated",
        ])
        .env(CHILD_ENV, "1")
        .output()
        .expect("spawn the isolated child process");
    assert!(
        output.status.success(),
        "with the fence, an FFI stream-poll panic becomes a clean Arrow error and the child \
             exits 0 (never aborts); got status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn arrow_type_key_list_element_simple_string_matches_spark() {
    let list_i8 = ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int8, true)));
    assert_eq!(arrow_type_key(&list_i8), "array<tinyint>");

    let nested = ArrowDataType::List(Arc::new(Field::new(
        "item",
        ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int32, true))),
        true,
    )));
    assert_eq!(arrow_type_key(&nested), "array<array<int>>");
}

#[test]
fn arrow_type_key_deep_list_nesting_is_depth_bounded() {
    let nest_levels = ARROW_TYPE_KEY_MAX_DEPTH.saturating_mul(4).max(128);
    let mut data_type = ArrowDataType::Int32;
    for _ in 0..nest_levels {
        data_type = ArrowDataType::List(Arc::new(Field::new("item", data_type, true)));
    }
    let key = arrow_type_key(&data_type);
    assert!(
        key.contains(ARROW_TYPE_KEY_DEPTH_FALLBACK),
        "deep List walk must hit the depth fallback, got {key:?}"
    );
    assert!(
        key.starts_with("array<"),
        "outer List still formats as array<…>, got {key:?}"
    );
    assert!(
        key.len() < nest_levels * 8,
        "key must not grow linearly with adversarial depth (len={}, nest={nest_levels})",
        key.len()
    );
}

fn battery_frame_on(table: &str) -> PyDataFrame {
    let runtime: Arc<Runtime> = Arc::new(Runtime::new().expect("a runtime builds"));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as _])
        .expect("a probe batch builds");
    let context = SessionContext::new();
    context.register_batch(table, batch).expect("register");
    let df = runtime
        .block_on(context.table(table))
        .expect("a table scans");
    PyDataFrame::new(df, runtime)
}

fn battery_frame() -> PyDataFrame {
    battery_frame_on("t")
}

fn assert_frame_depths(frame: &PyDataFrame, label: &str) {
    let walked = crate::deep_stack::plan_depths(frame.inner().logical_plan());
    let cached = frame.depths();
    assert_eq!(
        (cached.plan, cached.limited, cached.expression),
        (walked.plan, walked.limited, walked.expression),
        "{label}"
    );
}

fn key_join(py: Python<'_>, left: &Py<PyDataFrame>, right: &Py<PyDataFrame>) -> PyDataFrame {
    let stamped_left = crate::dataframe_names::stamp_attribute_ids(left.clone_ref(py))
        .expect("the left side stamps");
    let stamped_right = crate::dataframe_names::stamp_attribute_ids(right.clone_ref(py))
        .expect("the right side stamps");
    let left_node =
        crate::frame_lineage::frame_root(&stamped_left.borrow(py)).expect("a left node");
    let right_node =
        crate::frame_lineage::frame_root(&stamped_right.borrow(py)).expect("a right node");
    let (joined, _) = stamped_left
        .borrow(py)
        .join_on_names(
            stamped_right.borrow(py),
            vec!["id".to_string()],
            "inner",
            &left_node,
            &right_node,
        )
        .expect("a key join builds");
    joined
}

#[test]
fn cached_frame_levels_match_a_fresh_survey() {
    let id = || PyColumn::column("id").expect("a column builds");
    let base = battery_frame();
    assert_frame_depths(&base, "leaf");
    let mut chained = base
        .filter(id().is_not_null().expect("a predicate builds"))
        .expect("a filter builds");
    assert_frame_depths(&chained, "filter");
    for index in 0..20 {
        chained = chained
            .filter(id().is_not_null().expect("a predicate builds"))
            .expect("a chained filter builds");
        assert_frame_depths(&chained, &format!("filter chain {index}"));
    }
    let selected = base.select(vec![id()]).expect("a select builds");
    assert_frame_depths(&selected, "select");
    let extended = base.with_column("x", id()).expect("a with_column builds");
    assert_frame_depths(&extended, "with_column");
    let aliased_value = base
        .with_column("x", id().alias("y").expect("an alias builds"))
        .expect("a with_column over an alias builds");
    assert_frame_depths(&aliased_value, "with_column aliased");
    let replaced = base
        .with_column("id", id())
        .expect("a replacing with_column builds");
    assert_frame_depths(&replaced, "with_column replace");
    let sorted = base
        .sort(vec![id()], vec![true], vec![true])
        .expect("a sort builds");
    assert_frame_depths(&sorted, "sort");
    let aggregated = base
        .aggregate(
            vec![id()],
            vec![id().aggregate("sum", false).expect("a sum builds")],
        )
        .expect("an aggregate builds");
    assert_frame_depths(&aggregated, "aggregate");
    let limited = base.limit(2).expect("a limit builds");
    assert_frame_depths(&limited, "limit");
    let skipped = base.limit_with_skip(1, Some(2)).expect("a skip builds");
    assert_frame_depths(&skipped, "limit_with_skip");
    let distinct = base.distinct().expect("a distinct builds");
    assert_frame_depths(&distinct, "distinct");
    let distinct_on = base
        .distinct_on(vec!["id".to_string()])
        .expect("a distinct_on builds");
    assert_frame_depths(&distinct_on, "distinct_on");
    let renamed = base
        .with_column_renamed("id", "x")
        .expect("a rename builds");
    assert_frame_depths(&renamed, "renamed");
    let string_filtered = base.filter_sql("id > 1").expect("a sql filter builds");
    assert_frame_depths(&string_filtered, "filter_sql");
    Python::attach(|py| {
        let left = Py::new(py, battery_frame()).expect("a frame object builds");
        let right = Py::new(py, battery_frame_on("u")).expect("a frame object builds");
        let unioned = left
            .borrow(py)
            .union(right.borrow(py), false)
            .expect("a union builds");
        assert_frame_depths(&unioned, "union");
        let mut spine = left
            .borrow(py)
            .union(right.borrow(py), false)
            .expect("a union builds");
        for index in 0..10 {
            let next = Py::new(py, battery_frame()).expect("a frame object builds");
            spine = spine
                .union(next.borrow(py), false)
                .expect("a chained union builds");
            assert_frame_depths(&spine, &format!("union spine {index}"));
        }
        drop(spine);
        let named = left
            .borrow(py)
            .union_by_name(right.borrow(py), false)
            .expect("a union_by_name builds");
        assert_frame_depths(&named, "union_by_name");
        assert_frame_depths(&key_join(py, &left, &right), "join_on_names");
        let renamed = left
            .borrow(py)
            .with_column_renamed("id", "lid")
            .expect("a rename builds");
        let renamed = Py::new(py, renamed).expect("a frame object builds");
        let lid = PyColumn::column("lid").expect("a column builds");
        let conditional = renamed
            .borrow(py)
            .join_on_condition(
                right.borrow(py),
                lid.is_not_null().expect("a predicate builds"),
                "inner",
            )
            .expect("a conditional join builds");
        assert_frame_depths(&conditional, "join_on_condition");
    });
}

#[test]
fn name_rule_is_read_once_per_handle_and_children_inherit_their_parents() {
    Python::attach(|py| {
        let session = Py::new(
            py,
            crate::session::PyReparkSession::new(py, None, None, None, None, None)
                .expect("session"),
        )
        .expect("session object");
        let query = "SELECT 1 AS id, 2 AS v";
        let loose = session.borrow(py).sql(py, query).expect("a frame");
        assert!(loose.rule.get().is_none());
        assert_eq!(loose.rule(), NameRule::IgnoreCase);
        assert_eq!(loose.rule.get(), Some(&NameRule::IgnoreCase));
        crate::session_runtime::set_runtime_config(
            session.borrow(py),
            "spark.sql.caseSensitive",
            "true",
        )
        .expect("the conf flips");
        let exact = session.borrow(py).sql(py, query).expect("a later frame");
        assert_eq!(exact.rule(), NameRule::Exact);
        assert_eq!(loose.rule(), NameRule::IgnoreCase);
        for (parent, rule) in [(&loose, NameRule::IgnoreCase), (&exact, NameRule::Exact)] {
            let child = parent
                .select(vec![PyColumn::from_expr(col("id"))])
                .expect("a child");
            assert_eq!(child.rule.get(), Some(&rule));
            assert_eq!(crate::dataframe_names::frame_rule(child.inner()), rule);
            let filtered = child.filter_sql("id > 0").expect("a grandchild");
            assert_eq!(filtered.rule.get(), Some(&rule));
        }
        let unread = session.borrow(py).sql(py, query).expect("an unread frame");
        let child = unread.limit(1).expect("a child of an unread frame");
        assert!(child.rule.get().is_none());
        assert_eq!(child.rule(), NameRule::Exact);
    });
}
