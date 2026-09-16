use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{PlanProperties, reset_plan_states};
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};

#[derive(Debug)]
pub(crate) struct NljBuildSideExec {
    inner: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl NljBuildSideExec {
    fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        let source = inner.properties();
        let properties = PlanProperties::new(
            source.equivalence_properties().clone(),
            source.output_partitioning().clone(),
            source.emission_type,
            source.boundedness,
        )
        .with_evaluation_type(source.evaluation_type)
        .with_scheduling_type(source.scheduling_type);
        Self {
            inner,
            properties: Arc::new(properties),
        }
    }
}

impl std::fmt::Display for NljBuildSideExec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "NljBuildSideExec")
    }
}

impl DisplayAs for NljBuildSideExec {
    fn fmt_as(
        &self,
        _mode: DisplayFormatType,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(formatter, "NljBuildSideExec")
    }
}

#[allow(clippy::missing_errors_doc)]
impl ExecutionPlan for NljBuildSideExec {
    fn name(&self) -> &'static str {
        "NljBuildSideExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let [child] = children.try_into().map_err(|children: Vec<_>| {
            DataFusionError::Internal(format!(
                "NljBuildSideExec takes one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self::new(child)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let fresh = reset_plan_states(Arc::clone(&self.inner))?;
        fresh.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> DataFusionResult<Arc<datafusion::common::Statistics>> {
        self.inner.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.inner.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner
            .with_fetch(limit)
            .map(|inner| Arc::new(Self::new(inner)) as Arc<dyn ExecutionPlan>)
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }
}

#[derive(Debug)]
pub(crate) struct NljBuildSideReset;

impl PhysicalOptimizerRule for NljBuildSideReset {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            if plan.downcast_ref::<NestedLoopJoinExec>().is_none() {
                return Ok(Transformed::no(plan));
            }
            let children = plan.children().into_iter().cloned().collect::<Vec<_>>();
            let Ok([left, right]): Result<[Arc<dyn ExecutionPlan>; 2], _> = children.try_into()
            else {
                return Ok(Transformed::no(plan));
            };
            if left.downcast_ref::<NljBuildSideExec>().is_some() {
                return Ok(Transformed::no(plan));
            }
            let wrapped = Arc::new(NljBuildSideExec::new(left)) as Arc<dyn ExecutionPlan>;
            plan.with_new_children(vec![wrapped, right])
                .map(Transformed::yes)
        })
        .data()
    }

    fn name(&self) -> &str {
        "nlj_build_side_reset"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use futures::StreamExt;

    use super::*;

    async fn drain_rows(stream: SendableRecordBatchStream) -> usize {
        let mut stream = stream;
        let mut total = 0;
        while let Some(batch) = stream.next().await {
            total += batch.expect("a drained stream yields no error").num_rows();
        }
        total
    }

    fn empty_input() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        Arc::new(EmptyExec::new(schema))
    }

    fn two_partition_input() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        datafusion::physical_plan::test::TestMemoryExec::try_new_exec(
            &[vec![int_batch()], vec![int_batch()]],
            schema,
            None,
        )
        .expect("a two-partition memory input builds")
    }

    fn repartitioned(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(2))
                .expect("a repartition over an empty input builds"),
        )
    }

    fn tiny_join() -> Arc<dyn ExecutionPlan> {
        Arc::new(
            NestedLoopJoinExec::try_new(empty_input(), empty_input(), None, &JoinType::Inner, None)
                .expect("a filter-free inner nested-loop join builds"),
        )
    }

    fn int_batch() -> arrow::array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as _],
        )
        .expect("a two-row batch builds")
    }

    #[tokio::test]
    async fn a_bare_repartition_exec_runs_only_once() {
        let plan = repartitioned(two_partition_input());
        let context = Arc::new(TaskContext::default());
        let first = plan
            .execute(0, Arc::clone(&context))
            .expect("the first execute succeeds");
        drain_rows(first).await;
        let mut second = plan
            .execute(0, Arc::clone(&context))
            .expect("the second stream builds lazy");
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let waker = futures::task::noop_waker();
            let mut polling = std::task::Context::from_waker(&waker);
            let _ = second.as_mut().poll_next(&mut polling);
        }));
        let Err(payload) = panicked else {
            panic!("a twice-executed RepartitionExec must not poll clean");
        };
        let text = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&'static str>()
                    .map(ToString::to_string)
            })
            .unwrap_or_default();
        assert!(
            text.contains("partition not used yet"),
            "the second poll is the upstream one-shot failure, got: {text}"
        );
    }

    #[tokio::test]
    async fn the_wrapper_runs_a_one_shot_child_twice() {
        let plan = Arc::new(NljBuildSideExec::new(repartitioned(empty_input())));
        let context = Arc::new(TaskContext::default());
        for _ in 0..2 {
            let stream = plan
                .execute(0, Arc::clone(&context))
                .expect("every execute gets fresh state");
            drain_rows(stream).await;
        }
    }

    #[test]
    fn the_rule_wraps_only_the_build_side_of_a_nested_loop_join() {
        let rule = NljBuildSideReset;
        let config = ConfigOptions::new();
        let rewritten = rule
            .optimize(tiny_join(), &config)
            .expect("the rule applies");
        let join = rewritten
            .downcast_ref::<NestedLoopJoinExec>()
            .expect("the join node survives the rule");
        assert!(
            join.children()[0]
                .downcast_ref::<NljBuildSideExec>()
                .is_some(),
            "the left (build) child is wrapped"
        );
        assert!(
            join.children()[1]
                .downcast_ref::<NljBuildSideExec>()
                .is_none(),
            "the right (probe) child is untouched"
        );
        assert_eq!(
            rewritten.schema().fields().len(),
            tiny_join().schema().fields().len(),
            "the rewrite keeps the schema"
        );
    }

    #[test]
    fn the_rule_leaves_plans_without_a_nested_loop_join_alone() {
        let rule = NljBuildSideReset;
        let config = ConfigOptions::new();
        let bare = empty_input();
        let rewritten = rule
            .optimize(Arc::clone(&bare), &config)
            .expect("the rule applies");
        assert!(
            Arc::ptr_eq(&rewritten, &bare),
            "a plan with no nested-loop join keeps its identity"
        );
    }

    #[test]
    fn the_rule_is_idempotent() {
        let rule = NljBuildSideReset;
        let config = ConfigOptions::new();
        let once = rule
            .optimize(tiny_join(), &config)
            .expect("the first rewrite applies");
        let twice = rule
            .optimize(Arc::clone(&once), &config)
            .expect("the rule reapplies");
        assert!(
            Arc::ptr_eq(&once, &twice),
            "a wrapped build side is not wrapped again"
        );
    }

    #[test]
    fn the_wrapper_delegates_properties_and_limit_pushdown() {
        let inner = repartitioned(empty_input());
        let wrapped: Arc<dyn ExecutionPlan> = Arc::new(NljBuildSideExec::new(Arc::clone(&inner)));
        assert_eq!(
            wrapped.schema(),
            inner.schema(),
            "the wrapper keeps the schema"
        );
        assert_eq!(
            wrapped.output_partitioning(),
            inner.output_partitioning(),
            "the wrapper keeps the partitioning"
        );
        assert_eq!(
            wrapped.fetch(),
            inner.fetch(),
            "the wrapper keeps the fetch"
        );
        assert_eq!(
            wrapped.supports_limit_pushdown(),
            inner.supports_limit_pushdown(),
            "the wrapper keeps limit-pushdown support"
        );
        assert_eq!(wrapped.children().len(), 1, "the wrapper is unary");
    }

    #[tokio::test]
    async fn the_wrapper_replays_values_on_every_execute() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let single = Arc::new(
            RepartitionExec::try_new(
                datafusion::physical_plan::test::TestMemoryExec::try_new_exec(
                    &[vec![int_batch()]],
                    Arc::clone(&schema),
                    None,
                )
                .expect("a one-batch memory input builds"),
                Partitioning::RoundRobinBatch(1),
            )
            .expect("a repartition over memory builds"),
        );
        let wrapped = NljBuildSideExec::new(single);
        let context = Arc::new(TaskContext::default());
        for _ in 0..2 {
            let stream = wrapped
                .execute(0, Arc::clone(&context))
                .expect("every execute succeeds");
            assert_eq!(
                drain_rows(stream).await,
                2,
                "every execute replays the rows"
            );
        }
    }
}
