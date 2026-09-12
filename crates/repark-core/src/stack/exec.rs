use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, new_null_array};
use arrow::compute::{cast, interleave, take};
use arrow::datatypes::SchemaRef;
use datafusion::common::{Result, exec_datafusion_err, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;

use super::UnpivotNode;

#[derive(Debug)]
pub(crate) struct UnpivotExec {
    input: Arc<dyn ExecutionPlan>,
    n: usize,
    passthrough_count: usize,
    stack_count: usize,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl UnpivotExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, node: &UnpivotNode) -> Self {
        Self::create(
            input,
            node.n(),
            node.passthrough_count(),
            node.stack_count(),
            node.arrow_schema(),
        )
    }

    fn create(
        input: Arc<dyn ExecutionPlan>,
        n: usize,
        passthrough_count: usize,
        stack_count: usize,
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
            schema,
            cache,
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
            schema: Arc::clone(&self.schema),
            cache,
        }))
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
            schema: Arc::clone(&self.schema),
        }))
    }
}

struct UnpivotStream {
    input: SendableRecordBatchStream,
    n: usize,
    passthrough_count: usize,
    stack_count: usize,
    schema: SchemaRef,
}

impl Stream for UnpivotStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.as_mut().poll_next(cx)) {
            Some(Ok(batch)) => Poll::Ready(Some(unpivot_batch(
                &batch,
                self.n,
                self.passthrough_count,
                self.stack_count,
                &self.schema,
            ))),
            Some(Err(error)) => Poll::Ready(Some(Err(error))),
            None => Poll::Ready(None),
        }
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
        let unpivot = Arc::new(UnpivotExec::create(counting, 2, 0, 2, output_schema));
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
