use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::common::config::ConfigOptions;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_common::physical_expr::{fmt_sql, is_volatile};
use datafusion::physical_expr_common::sort_expr::OrderingRequirements;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{CardinalityEffect, PlanProperties};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, SendableRecordBatchStream,
};
use futures::TryStreamExt;
use tokio::sync::Semaphore;

use super::parallel_single_partition_enabled;

pub(crate) const PARALLEL_PROJECTION_RULE: &str = "parallel_projection";

pub const PARALLEL_PROJECTION_MIN_ROWS: usize = 16_384;

#[derive(Debug, Default)]
pub struct ParallelProjectionRule;

impl PhysicalOptimizerRule for ParallelProjectionRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !parallel_single_partition_enabled(config) {
            return Ok(plan);
        }
        let plan = drop_round_robins(plan)?;
        plan.transform_up(|node| {
            let Some(projection) = node.downcast_ref::<ProjectionExec>() else {
                return Ok(Transformed::no(node));
            };
            if !qualifies(projection) {
                return Ok(Transformed::no(node));
            }
            let parallel: Arc<dyn ExecutionPlan> =
                Arc::new(ParallelProjectionExec::new(projection.clone()));
            Ok(Transformed::yes(parallel))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        PARALLEL_PROJECTION_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn has_volatile(projection: &ProjectionExec) -> bool {
    projection.expr().iter().any(|item| is_volatile(&item.expr))
}

fn computed_indices(exprs: &[ProjectionExpr]) -> Vec<usize> {
    exprs
        .iter()
        .enumerate()
        .filter(|(_, item)| item.expr.downcast_ref::<Column>().is_none())
        .map(|(index, _)| index)
        .collect()
}

fn qualifies(projection: &ProjectionExec) -> bool {
    projection.input().output_partitioning().partition_count() == 1
        && computed_indices(projection.expr()).len() >= 2
        && !has_volatile(projection)
}

fn drop_round_robins(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = plan
        .transform_up(|node| {
            if node.downcast_ref::<ProjectionExec>().is_some() {
                return Ok(Transformed::no(node));
            }
            let distributions = node.required_input_distribution();
            let orderings = node.required_input_ordering();
            let mut children = Vec::with_capacity(distributions.len());
            let mut changed = false;
            for (index, child) in node.children().into_iter().enumerate() {
                let rebuilt = match without_round_robin(child)? {
                    Some(rebuilt)
                        if parent_accepts(
                            distributions.get(index),
                            orderings.get(index).and_then(Option::as_ref),
                            &rebuilt,
                        )? =>
                    {
                        changed = true;
                        rebuilt
                    }
                    _ => Arc::clone(child),
                };
                children.push(rebuilt);
            }
            if !changed {
                return Ok(Transformed::no(node));
            }
            node.with_new_children(children).map(Transformed::yes)
        })
        .data()?;
    Ok(without_round_robin(&plan)?.unwrap_or(plan))
}

fn without_round_robin(child: &Arc<dyn ExecutionPlan>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let mut chain: Vec<&ProjectionExec> = Vec::new();
    let mut cursor = child;
    while let Some(projection) = cursor.downcast_ref::<ProjectionExec>() {
        if has_volatile(projection) {
            return Ok(None);
        }
        chain.push(projection);
        cursor = projection.input();
    }
    if chain.is_empty() {
        return Ok(None);
    }
    let Some(repartition) = cursor.downcast_ref::<RepartitionExec>() else {
        return Ok(None);
    };
    if !matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_))
        || repartition.input().output_partitioning().partition_count() != 1
    {
        return Ok(None);
    }
    let mut rebuilt = Arc::clone(repartition.input());
    for projection in chain.iter().rev() {
        rebuilt = Arc::new(ProjectionExec::try_new(
            projection.expr().to_vec(),
            rebuilt,
        )?);
    }
    Ok(Some(rebuilt))
}

fn parent_accepts(
    distribution: Option<&Distribution>,
    ordering: Option<&OrderingRequirements>,
    child: &Arc<dyn ExecutionPlan>,
) -> Result<bool> {
    if !matches!(
        distribution,
        None | Some(Distribution::UnspecifiedDistribution | Distribution::SinglePartition)
    ) {
        return Ok(false);
    }
    match ordering {
        None => Ok(true),
        Some(requirement) => child
            .equivalence_properties()
            .ordering_satisfy_requirement(requirement.clone().into_single()),
    }
}

#[derive(Debug)]
pub struct ParallelProjectionExec {
    projection: ProjectionExec,
    tasks: Vec<usize>,
    metrics: ExecutionPlanMetricsSet,
}

impl ParallelProjectionExec {
    #[must_use]
    pub fn new(projection: ProjectionExec) -> Self {
        let tasks = computed_indices(projection.expr());
        Self {
            projection,
            tasks,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    #[must_use]
    pub fn expr(&self) -> &[ProjectionExpr] {
        self.projection.expr()
    }
}

impl DisplayAs for ParallelProjectionExec {
    fn fmt_as(&self, mode: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        match mode {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let listed: Vec<String> = self
                    .expr()
                    .iter()
                    .map(|item| {
                        let text = item.expr.to_string();
                        if text == item.alias {
                            text
                        } else {
                            format!("{text} as {}", item.alias)
                        }
                    })
                    .collect();
                write!(
                    formatter,
                    "ParallelProjectionExec: expr=[{}]",
                    listed.join(", ")
                )
            }
            DisplayFormatType::TreeRender => {
                for (index, item) in self.expr().iter().enumerate() {
                    let sql = fmt_sql(item.expr.as_ref());
                    if item.expr.to_string() == item.alias {
                        writeln!(formatter, "expr{index}={sql}")?;
                    } else {
                        writeln!(formatter, "{}={sql}", item.alias)?;
                    }
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for ParallelProjectionExec {
    fn name(&self) -> &'static str {
        "ParallelProjectionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.projection.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.projection.input()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.projection.benefits_from_input_partitioning()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [child] = children.try_into().map_err(|children: Vec<_>| {
            DataFusionError::Internal(format!(
                "ParallelProjectionExec takes one child, got {}",
                children.len()
            ))
        })?;
        let projection = ProjectionExec::try_new(self.expr().to_vec(), child)?;
        Ok(Arc::new(Self::new(projection)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self
            .projection
            .input()
            .execute(partition, Arc::clone(&context))?;
        let schema = self.schema();
        let job = Arc::new(ProjectionJob {
            schema: Arc::clone(&schema),
            exprs: self.expr().to_vec(),
            tasks: self.tasks.clone(),
            permits: self
                .tasks
                .len()
                .min(context.session_config().target_partitions())
                .max(1),
            metrics: BaselineMetrics::new(&self.metrics, partition),
            parallel_batches: MetricBuilder::new(&self.metrics)
                .counter("parallel_batches", partition),
        });
        let stream = input.and_then(move |batch| {
            let job = Arc::clone(&job);
            async move { job.project(batch).await }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.projection.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

struct ProjectionJob {
    schema: SchemaRef,
    exprs: Vec<ProjectionExpr>,
    tasks: Vec<usize>,
    permits: usize,
    metrics: BaselineMetrics,
    parallel_batches: Count,
}

impl ProjectionJob {
    async fn project(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        let columns = {
            let _timer = self.metrics.elapsed_compute().timer();
            if rows < PARALLEL_PROJECTION_MIN_ROWS {
                evaluate_serial(&self.exprs, &batch)?
            } else {
                self.parallel_batches.add(1);
                evaluate_parallel(&self.exprs, &self.tasks, &batch, self.permits).await?
            }
        };
        let output = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        self.metrics.record_output(rows);
        Ok(output)
    }
}

fn evaluate_one(item: &ProjectionExpr, batch: &RecordBatch) -> Result<ArrayRef> {
    item.expr
        .evaluate(batch)
        .and_then(|value| value.into_array(batch.num_rows()))
}

fn evaluate_serial(exprs: &[ProjectionExpr], batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
    exprs.iter().map(|item| evaluate_one(item, batch)).collect()
}

async fn evaluate_parallel(
    exprs: &[ProjectionExpr],
    tasks: &[usize],
    batch: &RecordBatch,
    permits: usize,
) -> Result<Vec<ArrayRef>> {
    let semaphore = Arc::new(Semaphore::new(permits));
    let mut spawned = Vec::with_capacity(tasks.len());
    for &index in tasks {
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let item = exprs[index].clone();
        let batch = batch.clone();
        spawned.push((
            index,
            SpawnedTask::spawn_blocking(move || {
                let outcome = evaluate_one(&item, &batch);
                drop(permit);
                outcome
            }),
        ));
    }
    let mut slots: Vec<Option<Result<ArrayRef>>> = exprs
        .iter()
        .map(|item| {
            item.expr
                .downcast_ref::<Column>()
                .map(|_| evaluate_one(item, batch))
        })
        .collect();
    for (index, task) in spawned {
        let outcome = task
            .join_unwind()
            .await
            .map_err(|error| DataFusionError::ExecutionJoin(Box::new(error)))?;
        slots[index] = Some(outcome);
    }
    slots
        .into_iter()
        .enumerate()
        .map(|(index, slot)| {
            slot.unwrap_or_else(|| {
                Err(DataFusionError::Internal(format!(
                    "ParallelProjectionExec: expression {index} produced no column"
                )))
            })
        })
        .collect()
}
