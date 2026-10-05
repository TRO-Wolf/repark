use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::stats::{ColumnStatistics, Precision};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::window::WindowExpr;
use datafusion::physical_expr_common::sort_expr::OrderingRequirements;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{CardinalityEffect, PlanProperties};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::WindowAggExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::Semaphore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputOrder {
    Single,
    PartitionIndex,
}

#[derive(Debug)]
pub struct ParallelWindowExec {
    input: Arc<dyn ExecutionPlan>,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    groups: Vec<Vec<usize>>,
    input_order: InputOrder,
    required_ordering: Vec<Option<OrderingRequirements>>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

type GroupOutcome = Vec<(usize, Result<ArrayRef>)>;

impl ParallelWindowExec {
    #[must_use]
    pub fn from_window(window: &WindowAggExec, groups: Vec<Vec<usize>>) -> Self {
        Self {
            input: Arc::clone(window.input()),
            window_expr: window.window_expr().to_vec(),
            groups,
            input_order: InputOrder::Single,
            required_ordering: window.required_input_ordering(),
            properties: Arc::clone(window.properties()),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    #[must_use]
    pub fn from_unordered_coalesce(
        window: &WindowAggExec,
        source: Arc<dyn ExecutionPlan>,
        groups: Vec<Vec<usize>>,
    ) -> Self {
        Self {
            input: source,
            window_expr: window.window_expr().to_vec(),
            groups,
            input_order: InputOrder::PartitionIndex,
            required_ordering: vec![None],
            properties: Arc::clone(window.properties()),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn groups(&self) -> &[Vec<usize>] {
        &self.groups
    }

    #[cfg(test)]
    pub(super) fn input_order(&self) -> InputOrder {
        self.input_order
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(self.properties.eq_properties.schema())
    }
}

impl DisplayAs for ParallelWindowExec {
    fn fmt_as(&self, mode: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        match mode {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(formatter, "ParallelWindowExec: ")?;
                let listed: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|expr| {
                        format!(
                            "{}: {:?}, frame: {:?}",
                            expr.name().to_owned(),
                            expr.field(),
                            expr.get_window_frame()
                        )
                    })
                    .collect();
                write!(formatter, "wdw=[{}]", listed.join(", "))
            }
            DisplayFormatType::TreeRender => {
                let listed: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|expr| expr.name().to_owned())
                    .collect();
                writeln!(formatter, "select_list={}", listed.join(", "))
            }
        }
    }
}

impl ExecutionPlan for ParallelWindowExec {
    fn name(&self) -> &'static str {
        "ParallelWindowExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![self.input_order == InputOrder::Single]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.required_ordering.clone()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match self.input_order {
            InputOrder::Single => vec![Distribution::SinglePartition],
            InputOrder::PartitionIndex => vec![Distribution::UnspecifiedDistribution],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [child] = children.try_into().map_err(|children: Vec<_>| {
            DataFusionError::Internal(format!(
                "ParallelWindowExec takes one child, got {}",
                children.len()
            ))
        })?;
        let groups = self.groups.clone();
        Ok(Arc::new(match self.input_order {
            InputOrder::Single => {
                let window = WindowAggExec::try_new(self.window_expr.clone(), child, true)?;
                Self::from_window(&window, groups)
            }
            InputOrder::PartitionIndex => {
                let coalesce = Arc::new(CoalescePartitionsExec::new(Arc::clone(&child)));
                let window = WindowAggExec::try_new(self.window_expr.clone(), coalesce, true)?;
                Self::from_unordered_coalesce(&window, child, groups)
            }
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = match self.input_order {
            InputOrder::Single => self.input.execute(partition, Arc::clone(&context))?,
            InputOrder::PartitionIndex => partition_index_stream(&self.input, &context)?,
        };
        let permits = self
            .groups
            .len()
            .min(context.session_config().target_partitions())
            .max(1);
        let job = WindowJob {
            schema: self.output_schema(),
            window_expr: self.window_expr.clone(),
            groups: self.groups.clone(),
            permits,
            metrics: BaselineMetrics::new(&self.metrics, partition),
        };
        let stream = futures::stream::once(job.run(input))
            .filter_map(|outcome| futures::future::ready(outcome.transpose()));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let read = match self.input_order {
            InputOrder::Single => partition,
            InputOrder::PartitionIndex => None,
        };
        let input = Arc::unwrap_or_clone(self.input.partition_statistics(read)?);
        let mut column_statistics = input.column_statistics;
        column_statistics
            .extend((0..self.window_expr.len()).map(|_| ColumnStatistics::new_unknown()));
        Ok(Arc::new(Statistics {
            num_rows: input.num_rows,
            column_statistics,
            total_byte_size: Precision::Absent,
        }))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

fn partition_index_stream(
    input: &Arc<dyn ExecutionPlan>,
    context: &Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let tasks = (0..input.output_partitioning().partition_count())
        .map(|partition| {
            let stream = input.execute(partition, Arc::clone(context))?;
            Ok(SpawnedTask::spawn(stream.try_collect::<Vec<RecordBatch>>()))
        })
        .collect::<Result<Vec<_>>>()?;
    let ordered = futures::stream::iter(tasks)
        .then(|task| async move {
            task.join_unwind()
                .await
                .map_err(|error| DataFusionError::ExecutionJoin(Box::new(error)))?
        })
        .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
        .try_flatten();
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        input.schema(),
        ordered,
    )))
}

struct WindowJob {
    schema: SchemaRef,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    groups: Vec<Vec<usize>>,
    permits: usize,
    metrics: BaselineMetrics,
}

impl WindowJob {
    async fn run(self, input: SendableRecordBatchStream) -> Result<Option<RecordBatch>> {
        let input_schema = input.schema();
        let batches: Vec<RecordBatch> = input.try_collect().await?;
        let batch = concat_batches(&input_schema, &batches)?;
        drop(batches);
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let columns = {
            let _timer = self.metrics.elapsed_compute().timer();
            evaluate_groups(&self.window_expr, &self.groups, &batch, self.permits).await?
        };
        let mut output = batch.columns().to_vec();
        output.extend(columns);
        let result = RecordBatch::try_new(self.schema, output)?;
        self.metrics.record_output(result.num_rows());
        self.metrics.done();
        Ok(Some(result))
    }
}

async fn evaluate_groups(
    window_expr: &[Arc<dyn WindowExpr>],
    groups: &[Vec<usize>],
    batch: &RecordBatch,
    permits: usize,
) -> Result<Vec<ArrayRef>> {
    let semaphore = Arc::new(Semaphore::new(permits));
    let mut tasks = Vec::with_capacity(groups.len());
    for group in groups {
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let members: Vec<(usize, Arc<dyn WindowExpr>)> = group
            .iter()
            .map(|&index| (index, Arc::clone(&window_expr[index])))
            .collect();
        let batch = batch.clone();
        tasks.push(SpawnedTask::spawn_blocking(move || {
            let outcome = evaluate_group(&members, &batch);
            drop(permit);
            outcome
        }));
    }
    let mut slots: Vec<Option<ArrayRef>> = vec![None; window_expr.len()];
    let mut failure: Option<(usize, DataFusionError)> = None;
    for task in tasks {
        let outcome = task
            .join_unwind()
            .await
            .map_err(|error| DataFusionError::ExecutionJoin(Box::new(error)))?;
        for (index, result) in outcome {
            match result {
                Ok(column) => slots[index] = Some(column),
                Err(error) => {
                    if failure.as_ref().is_none_or(|(lowest, _)| index < *lowest) {
                        failure = Some((index, error));
                    }
                }
            }
        }
    }
    if let Some((_, error)) = failure {
        return Err(error);
    }
    slots
        .into_iter()
        .enumerate()
        .map(|(index, slot)| {
            slot.ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "ParallelWindowExec: window expression {index} produced no column"
                ))
            })
        })
        .collect()
}

fn evaluate_group(members: &[(usize, Arc<dyn WindowExpr>)], batch: &RecordBatch) -> GroupOutcome {
    let mut outcome = Vec::with_capacity(members.len());
    for (index, expr) in members {
        let result = expr.evaluate(batch);
        let failed = result.is_err();
        outcome.push((*index, result));
        if failed {
            break;
        }
    }
    outcome
}
