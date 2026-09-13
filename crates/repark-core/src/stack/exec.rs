use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, new_null_array};
use arrow::compute::{CastOptions, cast, cast_with_options, interleave, take};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::format::DEFAULT_FORMAT_OPTIONS;
use datafusion::common::{Result, exec_datafusion_err, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;

use super::{StackLabels, UnpivotNode};

const STACK_UTF8_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: DEFAULT_FORMAT_OPTIONS,
};

#[derive(Debug)]
pub(crate) struct UnpivotExec {
    input: Arc<dyn ExecutionPlan>,
    n: usize,
    passthrough_count: usize,
    stack_count: usize,
    labels: Option<StackLabels>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl UnpivotExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, node: &UnpivotNode) -> Self {
        Self::create(
            input,
            node.n(),
            node.passthrough_count(),
            node.stack_count(),
            node.labels().cloned(),
            node.arrow_schema(),
        )
    }

    fn create(
        input: Arc<dyn ExecutionPlan>,
        n: usize,
        passthrough_count: usize,
        stack_count: usize,
        labels: Option<StackLabels>,
        schema: SchemaRef,
    ) -> Self {
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self {
            input,
            n,
            passthrough_count,
            stack_count,
            labels,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for UnpivotExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "UnpivotExec: stack(n={}, columns={})",
            self.n, self.stack_count
        )
    }
}

impl ExecutionPlan for UnpivotExec {
    fn name(&self) -> &'static str {
        "UnpivotExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .pop()
            .ok_or_else(|| exec_datafusion_err!("UnpivotExec requires one child"))?;
        if !children.is_empty() {
            return exec_err!("UnpivotExec requires exactly one child");
        }
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&self.schema)),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Arc::new(Self {
            input,
            n: self.n,
            passthrough_count: self.passthrough_count,
            stack_count: self.stack_count,
            labels: self.labels.clone(),
            schema: Arc::clone(&self.schema),
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(UnpivotStream {
            input: self.input.execute(partition, context)?,
            n: self.n,
            passthrough_count: self.passthrough_count,
            stack_count: self.stack_count,
            labels: self.labels.clone(),
            schema: Arc::clone(&self.schema),
            baseline: BaselineMetrics::new(&self.metrics, partition),
        }))
    }
}

struct UnpivotStream {
    input: SendableRecordBatchStream,
    n: usize,
    passthrough_count: usize,
    stack_count: usize,
    labels: Option<StackLabels>,
    schema: SchemaRef,
    baseline: BaselineMetrics,
}

impl Stream for UnpivotStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = match ready!(self.input.as_mut().poll_next(cx)) {
            Some(Ok(batch)) => {
                let _timer = self.baseline.elapsed_compute().timer();
                Poll::Ready(Some(match &self.labels {
                    Some(labels) => labeled_batch(&batch, labels, &self.schema),
                    None => unpivot_batch(
                        &batch,
                        self.n,
                        self.passthrough_count,
                        self.stack_count,
                        &self.schema,
                    ),
                }))
            }
            Some(Err(error)) => Poll::Ready(Some(Err(error))),
            None => Poll::Ready(None),
        };
        self.baseline.record_poll(poll)
    }
}

impl RecordBatchStream for UnpivotStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn unpivot_batch(
    batch: &RecordBatch,
    n: usize,
    passthrough_count: usize,
    stack_count: usize,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let input_rows = batch.num_rows();
    let n_cols = super::stack_column_count(stack_count, n);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(passthrough_count.saturating_add(n_cols));
    let repeat_indices = repeat_row_indices(input_rows, n)?;
    let interleave_indices = row_major_interleave_indices(n, input_rows);
    for index in 0..passthrough_count {
        arrays.push(take(batch.column(index).as_ref(), &repeat_indices, None)?);
    }
    for column_index in 0..n_cols {
        let output_type = output_schema
            .field(passthrough_count.saturating_add(column_index))
            .data_type();
        let mut pieces: Vec<ArrayRef> = Vec::with_capacity(n);
        for row_index in 0..n {
            let source = passthrough_count
                .saturating_add(row_index.saturating_mul(n_cols))
                .saturating_add(column_index);
            if source < passthrough_count.saturating_add(stack_count) {
                let column = batch.column(source);
                if column.data_type() == output_type {
                    pieces.push(Arc::clone(column));
                } else {
                    pieces.push(cast(column.as_ref(), output_type)?);
                }
            } else {
                pieces.push(new_null_array(output_type, input_rows));
            }
        }
        let refs: Vec<&dyn Array> = pieces.iter().map(AsRef::as_ref).collect();
        arrays.push(
            interleave(&refs, &interleave_indices)
                .map_err(|error| exec_datafusion_err!("{error}"))?,
        );
    }
    RecordBatch::try_new(Arc::clone(output_schema), arrays)
        .map_err(|error| exec_datafusion_err!("{error}"))
}

fn labeled_batch(
    batch: &RecordBatch,
    labels: &StackLabels,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let input_rows = batch.num_rows();
    let n = labels.names.len();
    if n == 0 {
        return exec_err!("stack labeled mode requires at least one label");
    }
    let n_cols = labels.cells.len() / n;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(n_cols.saturating_add(1));
    let mut label_values: Vec<&str> = Vec::with_capacity(input_rows.saturating_mul(n));
    for _ in 0..input_rows {
        label_values.extend(labels.names.iter().map(String::as_str));
    }
    arrays.push(Arc::new(StringArray::from(label_values)));
    let interleave_indices = row_major_interleave_indices(n, input_rows);
    let mut coerced: HashMap<usize, ArrayRef> = HashMap::with_capacity(labels.cells.len());
    for column_index in 0..n_cols {
        let mut pieces: Vec<ArrayRef> = Vec::with_capacity(n);
        for row_index in 0..n {
            let source = labels.cells[row_index
                .saturating_mul(n_cols)
                .saturating_add(column_index)];
            let piece = match coerced.entry(source) {
                Entry::Occupied(entry) => Arc::clone(entry.get()),
                Entry::Vacant(entry) => {
                    Arc::clone(entry.insert(coerce_stack_cell(batch.column(source))?))
                }
            };
            pieces.push(piece);
        }
        let refs: Vec<&dyn Array> = pieces.iter().map(AsRef::as_ref).collect();
        arrays.push(
            interleave(&refs, &interleave_indices)
                .map_err(|error| exec_datafusion_err!("{error}"))?,
        );
    }
    RecordBatch::try_new(Arc::clone(output_schema), arrays)
        .map_err(|error| exec_datafusion_err!("{error}"))
}

fn coerce_stack_cell(column: &ArrayRef) -> Result<ArrayRef> {
    if matches!(column.data_type(), DataType::Utf8) {
        return Ok(Arc::clone(column));
    }
    cast_with_options(column.as_ref(), &DataType::Utf8, &STACK_UTF8_CAST_OPTIONS)
        .map_err(|error| exec_datafusion_err!("{error}"))
}

fn repeat_row_indices(input_rows: usize, n: usize) -> Result<Int32Array> {
    let mut indices = Vec::with_capacity(input_rows.saturating_mul(n));
    for row in 0..input_rows {
        let row =
            i32::try_from(row).map_err(|_| exec_datafusion_err!("stack row index overflow"))?;
        for _ in 0..n {
            indices.push(row);
        }
    }
    Ok(Int32Array::from(indices))
}

fn row_major_interleave_indices(piece_count: usize, rows: usize) -> Vec<(usize, usize)> {
    let mut indices = Vec::with_capacity(rows.saturating_mul(piece_count));
    for row in 0..rows {
        for piece in 0..piece_count {
            indices.push((piece, row));
        }
    }
    indices
}

#[cfg(test)]
mod streaming_pin {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::limit::LocalLimitExec;
    use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        collect,
    };

    use super::UnpivotExec;

    #[derive(Debug)]
    struct CountingExec {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        produced: Arc<AtomicUsize>,
        cache: Arc<PlanProperties>,
    }

    impl CountingExec {
        fn new(batches: Vec<RecordBatch>, produced: Arc<AtomicUsize>) -> Self {
            let schema = batches
                .first()
                .map(RecordBatch::schema)
                .expect("counting exec needs at least one batch");
            let cache = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self {
                schema,
                batches,
                produced,
                cache,
            }
        }
    }

    impl DisplayAs for CountingExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "CountingExec")
        }
    }

    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &'static str {
            "CountingExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            Vec::new()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            if children.is_empty() {
                Ok(self)
            } else {
                datafusion::common::exec_err!("CountingExec has no children")
            }
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            let mut builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&self.schema), 1);
            let tx = builder.tx();
            let batches = self.batches.clone();
            let produced = Arc::clone(&self.produced);
            builder.spawn(async move {
                for batch in batches {
                    produced.fetch_add(1, Ordering::SeqCst);
                    if tx.send(Ok(batch)).await.is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            });
            Ok(builder.build())
        }
    }

    #[tokio::test]
    async fn unpivot_exec_emits_first_batch_before_input_is_exhausted() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let mut batches = Vec::with_capacity(8);
        for row in 0..8 {
            let a = Int64Array::from(vec![i64::from(row)]);
            let b = Int64Array::from(vec![i64::from(row) + 10]);
            batches.push(
                RecordBatch::try_new(Arc::clone(&input_schema), vec![Arc::new(a), Arc::new(b)])
                    .expect("input batch"),
            );
        }
        let produced = Arc::new(AtomicUsize::new(0));
        let counting = Arc::new(CountingExec::new(batches, Arc::clone(&produced)));
        let output_schema = Arc::new(Schema::new(vec![Field::new("col0", DataType::Int64, true)]));
        let unpivot = Arc::new(UnpivotExec::create(counting, 2, 0, 2, None, output_schema));
        let limited = Arc::new(LocalLimitExec::new(unpivot, 1));
        let context = SessionContext::new();
        let out = collect(limited, context.task_ctx())
            .await
            .expect("limit collect");
        let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1);
        let produced = produced.load(Ordering::SeqCst);
        assert!(
            produced < 8,
            "UnpivotExec must emit before the input stream is exhausted; produced={produced}"
        );
    }
}

#[cfg(test)]
mod labeled {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
        RecordBatch, StringArray, TimestampMillisecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::execution_plan::ExecutionPlan;

    use super::{StackLabels, UnpivotExec};

    fn cell_batch() -> (SchemaRef, RecordBatch) {
        let columns: Vec<(DataType, ArrayRef)> = vec![
            (DataType::Float64, Arc::new(Float64Array::from(vec![0.0]))),
            (DataType::Float64, Arc::new(Float64Array::from(vec![-0.0]))),
            (DataType::Float64, Arc::new(Float64Array::from(vec![1.0]))),
            (DataType::Float64, Arc::new(Float64Array::from(vec![1e16]))),
            (DataType::Float64, Arc::new(Float64Array::from(vec![1e-20]))),
            (
                DataType::Float64,
                Arc::new(Float64Array::from(vec![0.1 + 0.2])),
            ),
            (
                DataType::Float64,
                Arc::new(Float64Array::from(vec![f64::NAN])),
            ),
            (
                DataType::Float64,
                Arc::new(Float64Array::from(vec![f64::INFINITY])),
            ),
            (
                DataType::Float64,
                Arc::new(Float64Array::from(vec![f64::NEG_INFINITY])),
            ),
            (DataType::Float64, Arc::new(Float64Array::from(vec![None]))),
            (DataType::Int64, Arc::new(Int64Array::from(vec![-42]))),
            (
                DataType::Decimal128(10, 2),
                Arc::new(
                    Decimal128Array::from(vec![12345])
                        .with_precision_and_scale(10, 2)
                        .expect("decimal precision"),
                ),
            ),
            (DataType::Date32, Arc::new(Date32Array::from(vec![18262]))),
            (
                DataType::Timestamp(TimeUnit::Millisecond, None),
                Arc::new(TimestampMillisecondArray::from(vec![1_577_836_800_000])),
            ),
            (DataType::Boolean, Arc::new(BooleanArray::from(vec![true]))),
            (
                DataType::Utf8,
                Arc::new(StringArray::from(vec!["mixed CASE"])),
            ),
        ];
        let fields = columns
            .iter()
            .enumerate()
            .map(|(index, (data_type, _))| Field::new(format!("c{index}"), data_type.clone(), true))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let arrays = columns
            .into_iter()
            .map(|(_, array)| array)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).expect("cell batch");
        (schema, batch)
    }

    #[tokio::test]
    async fn labeled_stack_coerces_cells_like_engine_cast() {
        let (schema, batch) = cell_batch();
        let width = schema.fields().len();
        let context = SessionContext::new();
        let table =
            MemTable::try_new(Arc::clone(&schema), vec![vec![batch.clone()]]).expect("memtable");
        context
            .register_table("cells", Arc::new(table))
            .expect("register");
        let provider = context.table_provider("cells").await.expect("provider");
        let memory = provider
            .scan(&context.state(), None, &[], None)
            .await
            .expect("memtable scan");
        let names = vec!["first".to_string(), "second".to_string()];
        let mut cells = Vec::with_capacity(names.len() * width);
        for row in 0..names.len() {
            for column in 0..width {
                cells.push((column + row) % width);
            }
        }
        let labels = StackLabels {
            names: names.clone(),
            cells,
        };
        let mut out_fields = vec![Field::new("summary", DataType::Utf8, true)];
        for column in 0..width {
            out_fields.push(Field::new(format!("v{column}"), DataType::Utf8, true));
        }
        let exec: Arc<dyn ExecutionPlan> = Arc::new(UnpivotExec::create(
            memory,
            names.len(),
            0,
            labels.cells.len(),
            Some(labels),
            Arc::new(Schema::new(out_fields)),
        ));
        let output = collect(exec, context.task_ctx()).await.expect("collect");
        let result = output.first().expect("output batch");
        let cast_columns = (0..width)
            .map(|index| format!("CAST(\"c{index}\" AS STRING)"))
            .collect::<Vec<_>>()
            .join(", ");
        let expected = context
            .sql(&format!("SELECT {cast_columns} FROM cells"))
            .await
            .expect("oracle query")
            .collect()
            .await
            .expect("oracle collect");
        let oracle = expected.first().expect("oracle batch");
        let label_column = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("label column");
        for (row, name) in names.iter().enumerate() {
            assert_eq!(label_column.value(row), name.as_str());
            for column in 0..width {
                let actual = result
                    .column(column + 1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("cell column");
                let oracle_array = arrow::compute::cast(
                    oracle.column((column + row) % width).as_ref(),
                    &DataType::Utf8,
                )
                .expect("oracle utf8");
                let oracle_column = oracle_array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("oracle column");
                assert_eq!(
                    actual.is_null(row),
                    oracle_column.is_null(0),
                    "row {row} column {column} nullability"
                );
                if !actual.is_null(row) {
                    assert_eq!(
                        actual.value(row),
                        oracle_column.value(0),
                        "row {row} column {column} coerced string"
                    );
                }
            }
        }
    }
}
