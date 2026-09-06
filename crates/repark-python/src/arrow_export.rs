use std::cell::Cell;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, RecordBatchReader};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::error::ArrowError;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::DataFrame;
use futures::StreamExt;
use pyo3::Python;
use repark_core::PoolRefusalLog;
use tokio::runtime::Runtime;

use crate::dataframe::STREAM_POLL_NO_DETACH;
use crate::fence::{fence_stream_poll, fenced_panic_detail};

const MAX_NESTED_TYPE_DEPTH: usize = 32;

const CONTAINABLE_PANIC_PAYLOADS: &[&str] = &[
    "partition not used yet",
    "at least one spill reader should exist",
    "at least one receiver should exist",
    "right_data must be present",
    "left_stream must be set after spill future resolves",
    "left_schema must be set",
    "right bitmap should be available",
    "without Active spill state",
];

const REFUSAL_CONTAINMENT_NOTE: &str = concat!(
    "REPARK: the bounded memory pool refused this plan; the engine did not survive that ",
    "refusal, so repark reports the refusal itself."
);

#[derive(Debug)]
struct ContainedPoolRefusal(String);

impl std::fmt::Display for ContainedPoolRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ContainedPoolRefusal {}

/// A synchronous reader that polls one DataFusion batch per Arrow C Stream callback.
pub(crate) struct StreamingBatchReader {
    runtime: Arc<Runtime>,
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
    refusals: Option<Arc<PoolRefusalLog>>,
    refusals_before: u64,
}

impl StreamingBatchReader {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Self {
        Self {
            runtime,
            stream,
            schema,
            refusals: None,
            refusals_before: 0,
        }
    }

    pub(crate) fn with_refusals(mut self, refusals: Option<Arc<PoolRefusalLog>>) -> Self {
        self.refusals_before = refusals.as_ref().map_or(0, |log| log.refusals());
        self.refusals = refusals;
        self
    }

    fn as_pool_refusal(&self, error: ArrowError) -> ArrowError {
        let Some(detail) = fenced_panic_detail(&error).map(ToOwned::to_owned) else {
            return error;
        };
        if !CONTAINABLE_PANIC_PAYLOADS
            .iter()
            .any(|payload| detail.contains(payload))
        {
            return error;
        }
        let Some(log) = self.refusals.as_ref() else {
            return error;
        };
        if log.refusals() <= self.refusals_before {
            return error;
        }
        let Some(refusal) = log.last_refusal() else {
            return error;
        };
        tracing::warn!(
            target: "repark::spill",
            detail = detail.as_str(),
            "a bounded memory-pool refusal ended in an engine-internal failure"
        );
        ArrowError::ExternalError(Box::new(ContainedPoolRefusal(format!(
            "{refusal}\n{REFUSAL_CONTAINMENT_NOTE}"
        ))))
    }
}

impl Iterator for StreamingBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    /// Pull exactly one batch.
    fn next(&mut self) -> Option<Self::Item> {
        // The Arrow callback cannot unwind across extern "C"; fence the poll and report an error.
        let Self {
            runtime, stream, ..
        } = self;
        let schema = Arc::clone(&self.schema);
        let item = fence_stream_poll("PyDataFrame.__arrow_c_stream__.next", || {
            let no_detach = STREAM_POLL_NO_DETACH.with(Cell::get);
            let polled = if no_detach {
                runtime.block_on(stream.next())
            } else {
                Python::attach(|python| python.detach(|| runtime.block_on(stream.next())))
            };
            polled.map(|batch| {
                batch
                    .map_err(|error| ArrowError::ExternalError(Box::new(error)))
                    .and_then(|batch| coerce_batch_views(&batch, &schema))
            })
        });
        match item {
            Some(Err(error)) => Some(Err(self.as_pool_refusal(error))),
            other => other,
        }
    }
}

impl RecordBatchReader for StreamingBatchReader {
    /// The exported stream uses the analyzed logical types and Spark-style `nullable = true`.
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub(crate) fn refusal_log(frame: &DataFrame) -> Option<Arc<PoolRefusalLog>> {
    let runtime_env = frame.task_ctx().runtime_env();
    repark_core::pool_refusal_log(runtime_env.memory_pool.as_ref())
}

pub(crate) fn coerced_export_schema(schema: &SchemaRef) -> SchemaRef {
    if !schema_contains_views(schema) {
        return Arc::clone(schema);
    }
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| coerce_field_views(field, 0))
            .collect::<Vec<Field>>(),
        schema.metadata().clone(),
    ))
}

fn coerce_batch_views(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    let target = schema.fields();
    if batch.num_columns() == target.len()
        && batch
            .schema()
            .fields()
            .iter()
            .zip(target.iter())
            .all(|(current, want)| current.data_type() == want.data_type())
    {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .zip(target.iter())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column, field.data_type())
            }
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns)
}

fn schema_contains_views(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| type_contains_views(field.data_type(), 0))
}

fn type_contains_views(data_type: &DataType, depth: usize) -> bool {
    if depth > MAX_NESTED_TYPE_DEPTH {
        return false;
    }
    let child = depth + 1;
    match data_type {
        DataType::Utf8View | DataType::BinaryView => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| type_contains_views(field.data_type(), child)),
        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner)
        | DataType::FixedSizeList(inner, _) => type_contains_views(inner.data_type(), child),
        DataType::Map(entries, _) => type_contains_views(entries.data_type(), child),
        _ => false,
    }
}

fn coerce_field_views(field: &Field, depth: usize) -> Field {
    if depth > MAX_NESTED_TYPE_DEPTH {
        return field.clone();
    }
    field
        .clone()
        .with_data_type(coerce_type_views(field.data_type(), depth))
}

fn coerce_type_views(data_type: &DataType, depth: usize) -> DataType {
    let child = depth + 1;
    match data_type {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::Struct(fields) => DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|field| coerce_field_views(field, child))
                .collect::<Vec<Field>>(),
        )),
        DataType::List(inner) => DataType::List(Arc::new(coerce_field_views(inner, child))),
        DataType::LargeList(inner) => {
            DataType::LargeList(Arc::new(coerce_field_views(inner, child)))
        }
        DataType::FixedSizeList(inner, size) => {
            DataType::FixedSizeList(Arc::new(coerce_field_views(inner, child)), *size)
        }
        DataType::ListView(inner) => DataType::ListView(Arc::new(coerce_field_views(inner, child))),
        DataType::LargeListView(inner) => {
            DataType::LargeListView(Arc::new(coerce_field_views(inner, child)))
        }
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(pair) if pair.len() >= 2 => {
                let key = coerce_field_views(pair[0].as_ref(), child);
                let value = coerce_field_views(pair[1].as_ref(), child);
                let rebuilt = entries
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(Fields::from(vec![key, value])));
                DataType::Map(Arc::new(rebuilt), *sorted)
            }
            _ => data_type.clone(),
        },
        _ => data_type.clone(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{BinaryViewArray, StringViewArray};

    use super::*;

    fn view_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8View, true),
            Field::new("raw", DataType::BinaryView, false),
            Field::new("unit", DataType::Int32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(vec![Some("a"), None, Some("bb")])),
                Arc::new(BinaryViewArray::from(vec![
                    Some(b"x".as_slice()),
                    Some(b"yy".as_slice()),
                    Some(b"z".as_slice()),
                ])),
                Arc::new(arrow::array::Int32Array::from(vec![Some(1), None, Some(3)])),
            ],
        )
        .expect("view batch builds")
    }

    #[test]
    fn schema_without_views_is_shared_unchanged() {
        let schema = Arc::new(Schema::new(vec![Field::new("unit", DataType::Int32, true)]));
        let coerced = coerced_export_schema(&schema);
        assert!(Arc::ptr_eq(&schema, &coerced));
    }

    #[test]
    fn schema_with_views_coerces_flat_and_nested() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8View, true),
            Field::new(
                "top",
                DataType::Struct(Fields::from(vec![Field::new(
                    "inner",
                    DataType::Utf8View,
                    false,
                )])),
                true,
            ),
        ]));
        let coerced = coerced_export_schema(&schema);
        assert_eq!(
            coerced
                .field_with_name("id")
                .expect("id present")
                .data_type(),
            &DataType::Utf8
        );
        assert!(
            coerced
                .field_with_name("id")
                .expect("id present")
                .is_nullable()
        );
        let DataType::Struct(top) = coerced
            .field_with_name("top")
            .expect("top present")
            .data_type()
        else {
            panic!("top stays a struct");
        };
        assert_eq!(top[0].data_type(), &DataType::Utf8);
        assert!(!top[0].is_nullable());
    }

    #[test]
    fn batch_with_views_coerces_values_and_nulls() {
        let batch = view_batch();
        let schema = coerced_export_schema(&batch.schema());
        let coerced = coerce_batch_views(&batch, &schema).expect("coerce succeeds");
        assert_eq!(coerced.num_rows(), 3);
        assert_eq!(
            coerced
                .schema()
                .field_with_name("id")
                .expect("id present")
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            coerced
                .schema()
                .field_with_name("raw")
                .expect("raw present")
                .data_type(),
            &DataType::Binary
        );
        let ids = coerced
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("id column is StringArray");
        assert_eq!(ids.null_count(), 1);
        assert_eq!(
            ids.iter().collect::<Vec<_>>(),
            vec![Some("a"), None, Some("bb")]
        );
        let raws = coerced
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("raw column is BinaryArray");
        assert_eq!(
            raws.iter().collect::<Vec<_>>(),
            vec![
                Some(b"x".as_slice()),
                Some(b"yy".as_slice()),
                Some(b"z".as_slice())
            ]
        );
        let units = coerced
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("unit column passes through");
        assert_eq!(units.null_count(), 1);
    }

    #[test]
    fn batch_without_views_is_returned_unrebuilt() {
        let schema = Arc::new(Schema::new(vec![Field::new("unit", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int32Array::from(vec![
                Some(1),
                None,
            ]))],
        )
        .expect("plain batch builds");
        let coerced = coerce_batch_views(&batch, &schema).expect("fast path succeeds");
        assert!(Arc::ptr_eq(coerced.column(0), batch.column(0)));
        assert_eq!(coerced.schema().fields(), batch.schema().fields());
    }

    #[test]
    fn non_string_widening_mismatch_casts_losslessly() {
        let physical = Arc::new(Schema::new(vec![Field::new("unit", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(arrow::array::Int32Array::from(vec![
                Some(1),
                None,
                Some(3),
            ]))],
        )
        .expect("physical batch builds");
        let analyzed = Arc::new(Schema::new(vec![Field::new("unit", DataType::Int64, true)]));
        let coerced = coerce_batch_views(&batch, &analyzed).expect("widening casts");
        assert_eq!(coerced.schema().fields(), analyzed.fields());
        let values = coerced
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("unit column is Int64Array");
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(1), None, Some(3)]
        );
    }

    #[test]
    fn uncastable_mismatch_errors_loud() {
        let physical = Arc::new(Schema::new(vec![Field::new("unit", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(arrow::array::Int32Array::from(vec![Some(1)]))],
        )
        .expect("physical batch builds");
        let analyzed = Arc::new(Schema::new(vec![Field::new(
            "unit",
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int32, true)])),
            true,
        )]));
        let error = coerce_batch_views(&batch, &analyzed).expect_err("struct mismatch refuses");
        assert!(
            error.to_string().contains("not supported"),
            "loud cast refusal, got {error}"
        );
    }

    fn probe_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]))
    }

    fn panic_item() -> Result<RecordBatch, datafusion::error::DataFusionError> {
        panic!("partition not used yet")
    }

    fn probe_reader(
        stream: SendableRecordBatchStream,
        refusals: Option<Arc<PoolRefusalLog>>,
    ) -> StreamingBatchReader {
        let runtime = Arc::new(tokio::runtime::Runtime::new().expect("a tokio runtime builds"));
        StreamingBatchReader::new(runtime, stream, probe_schema()).with_refusals(refusals)
    }

    fn empty_reader(refusals: Option<Arc<PoolRefusalLog>>) -> StreamingBatchReader {
        let stream: SendableRecordBatchStream = Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                probe_schema(),
                futures::stream::empty(),
            ),
        );
        probe_reader(stream, refusals)
    }

    fn panicking_reader(refusals: Option<Arc<PoolRefusalLog>>) -> StreamingBatchReader {
        let stream: SendableRecordBatchStream = Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                probe_schema(),
                futures::stream::once(async { panic_item() }),
            ),
        );
        probe_reader(stream, refusals)
    }

    fn fenced_panic_with(payload: &'static str) -> ArrowError {
        crate::fence::fence_stream_poll("Probe.poll", || panic!("{payload}"))
            .expect("a fenced panic yields one item")
            .expect_err("the item is the Err arm")
    }

    fn fenced_panic() -> ArrowError {
        fenced_panic_with("partition not used yet")
    }

    fn refuse_once(log: &Arc<PoolRefusalLog>) {
        use datafusion::execution::memory_pool::{FairSpillPool, MemoryConsumer, MemoryPool};

        let pool: Arc<dyn MemoryPool> = Arc::new(repark_core::RefusalRecordingPool::new(
            Arc::new(FairSpillPool::new(1024)),
            Arc::clone(log),
        ));
        let reservation = MemoryConsumer::new("probe").register(&pool);
        reservation
            .try_grow(1024 * 1024)
            .expect_err("a 1 KiB pool refuses a 1 MiB reservation");
    }

    #[test]
    fn a_panic_that_follows_a_pool_refusal_is_reported_as_that_refusal() {
        let log = Arc::new(PoolRefusalLog::default());
        let reader = empty_reader(Some(Arc::clone(&log)));
        refuse_once(&log);
        let message = reader.as_pool_refusal(fenced_panic()).to_string();
        assert!(
            message.contains("fair("),
            "the pool names itself: {message}"
        );
        assert!(
            message.to_lowercase().contains("resources exhausted"),
            "the typed refusal survives: {message}"
        );
        assert!(
            !message.contains("a Rust panic was caught"),
            "the internal-error framing is gone: {message}"
        );
        assert!(
            !message.contains("partition not used yet"),
            "the engine's own panic payload is not the user's message: {message}"
        );
        assert!(
            message.contains(REFUSAL_CONTAINMENT_NOTE),
            "the containment is disclosed: {message}"
        );
    }

    #[test]
    fn an_unrelated_panic_after_a_pool_refusal_stays_the_bug_report() {
        let log = Arc::new(PoolRefusalLog::default());
        let reader = empty_reader(Some(Arc::clone(&log)));
        refuse_once(&log);
        let injected = fenced_panic_with("index out of bounds: the len is 3 but the index is 7");
        let message = reader.as_pool_refusal(injected).to_string();
        assert!(
            message.contains("a Rust panic was caught"),
            "a panic the pool refusal cannot explain is still a bug report: {message}"
        );
        assert!(
            !message.contains("fair("),
            "an unrelated panic is never dressed up as a pool refusal: {message}"
        );
    }

    #[test]
    fn every_allow_listed_payload_is_contained_after_a_refusal() {
        assert!(
            !CONTAINABLE_PANIC_PAYLOADS.is_empty(),
            "an empty allow-list would make this loop vacuous, so it is asserted first"
        );
        for payload in CONTAINABLE_PANIC_PAYLOADS {
            let log = Arc::new(PoolRefusalLog::default());
            let reader = empty_reader(Some(Arc::clone(&log)));
            refuse_once(&log);
            let framed = crate::fence::fence_stream_poll("Probe.poll", || {
                panic!("some frame: {payload} at some line")
            })
            .expect("a fenced panic yields one item")
            .expect_err("the item is the Err arm");
            let message = reader.as_pool_refusal(framed).to_string();
            assert!(
                message.contains("fair(") && !message.contains("a Rust panic was caught"),
                "allow-listed payload {payload:?} must be contained: {message}"
            );
        }
    }

    #[test]
    fn a_panic_with_no_pool_refusal_stays_the_internal_error() {
        let reader = empty_reader(Some(Arc::new(PoolRefusalLog::default())));
        let message = reader.as_pool_refusal(fenced_panic()).to_string();
        assert!(
            message.contains("a Rust panic was caught"),
            "a panic the pool did not cause is still reported as one: {message}"
        );
    }

    #[test]
    fn an_unbounded_session_has_no_log_and_leaves_the_internal_error_alone() {
        let reader = empty_reader(None);
        let message = reader.as_pool_refusal(fenced_panic()).to_string();
        assert!(
            message.contains("a Rust panic was caught"),
            "no pool, no rewrite: {message}"
        );
    }

    #[test]
    fn an_ordinary_stream_error_is_never_rewritten_as_a_refusal() {
        let log = Arc::new(PoolRefusalLog::default());
        let reader = empty_reader(Some(Arc::clone(&log)));
        refuse_once(&log);
        let ordinary = ArrowError::ExternalError(Box::new(
            datafusion::error::DataFusionError::Execution("kaboom".to_string()),
        ));
        let message = reader.as_pool_refusal(ordinary).to_string();
        assert!(
            message.contains("kaboom"),
            "only a fenced panic is rewritten: {message}"
        );
    }

    #[test]
    fn the_reader_delivers_the_typed_refusal_from_its_own_poll() {
        Python::attach(|_python| {
            let log = Arc::new(PoolRefusalLog::default());
            let mut reader = panicking_reader(Some(Arc::clone(&log)));
            refuse_once(&log);
            let error = reader
                .next()
                .expect("the panicking poll yields one item")
                .expect_err("the item is the Err arm");
            let message = error.to_string();
            assert!(message.contains("fair("), "{message}");
            assert!(
                !message.contains("a Rust panic was caught"),
                "the reader itself contains the panic: {message}"
            );
        });
    }
}
