use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, new_null_array};
use arrow::compute::{cast, concat, take};
use arrow::datatypes::SchemaRef;
use datafusion::common::{Result, exec_datafusion_err, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

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
        let schema = node.arrow_schema();
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self {
            input,
            n: node.n(),
            passthrough_count: node.passthrough_count(),
            stack_count: node.stack_count(),
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
        let input = self.input.execute(partition, context)?;
        let schema = Arc::clone(&self.schema);
        let n = self.n;
        let passthrough_count = self.passthrough_count;
        let stack_count = self.stack_count;
        let mut builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), 2);
        let tx = builder.tx();
        builder.spawn(async move {
            let batches = collect(input).await?;
            for batch in batches {
                let stacked = unpivot_batch(&batch, n, passthrough_count, stack_count, &schema)?;
                if tx.send(Ok(stacked)).await.is_err() {
                    return Ok(());
                }
            }
            Ok(())
        });
        Ok(builder.build())
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
        arrays.push(interleave_row_major(&pieces)?);
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

fn interleave_row_major(pieces: &[ArrayRef]) -> Result<ArrayRef> {
    let n = pieces.len();
    let rows = pieces.first().map_or(0, Array::len);
    let refs: Vec<&dyn Array> = pieces.iter().map(AsRef::as_ref).collect();
    let concatenated = concat(&refs)?;
    let mut indices = Vec::with_capacity(rows.saturating_mul(n));
    for row in 0..rows {
        for piece in 0..n {
            let index = piece.saturating_mul(rows).saturating_add(row);
            let index = i32::try_from(index)
                .map_err(|_| exec_datafusion_err!("stack take index overflow"))?;
            indices.push(index);
        }
    }
    take(concatenated.as_ref(), &Int32Array::from(indices), None)
        .map_err(|error| exec_datafusion_err!("{error}"))
}
