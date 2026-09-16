use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::JoinType;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{PlanProperties, reset_plan_states};
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
};

use crate::pool_refusals::{REFUSAL_CONTAINMENT_NOTE, pool_refusal_log};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuildSidePolicy {
    Spill,
    RefuseFallback,
}

impl BuildSidePolicy {
    pub(crate) fn for_join(join_type: JoinType, right_partitions: usize) -> Self {
        let left_family = matches!(
            join_type,
            JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark
        );
        if left_family && right_partitions > 1 {
            Self::RefuseFallback
        } else {
            Self::Spill
        }
    }
}

#[derive(Debug)]
pub(crate) struct NljBuildSideExec {
    inner: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    policy: BuildSidePolicy,
    executes: AtomicUsize,
}

impl NljBuildSideExec {
    fn new(inner: Arc<dyn ExecutionPlan>, policy: BuildSidePolicy) -> Self {
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
            policy,
            executes: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    pub(crate) fn policy(&self) -> BuildSidePolicy {
        self.policy
    }

    fn fallback_refusal(context: &TaskContext) -> Option<DataFusionError> {
        let recorded = pool_refusal_log(context.memory_pool().as_ref())?.last_refusal()?;
        Some(DataFusionError::ResourcesExhausted(format!(
            "{recorded}\n{REFUSAL_CONTAINMENT_NOTE}"
        )))
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
        Ok(Arc::new(Self::new(child, self.policy)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if self.executes.fetch_add(1, Ordering::AcqRel) > 0
            && self.policy == BuildSidePolicy::RefuseFallback
            && let Some(refusal) = Self::fallback_refusal(&context)
        {
            return Err(refusal);
        }
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
        let policy = self.policy;
        self.inner
            .with_fetch(limit)
            .map(|inner| Arc::new(Self::new(inner, policy)) as Arc<dyn ExecutionPlan>)
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
            let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() else {
                return Ok(Transformed::no(plan));
            };
            let policy = BuildSidePolicy::for_join(
                *join.join_type(),
                join.right().output_partitioning().partition_count(),
            );
            let children = plan.children().into_iter().cloned().collect::<Vec<_>>();
            let Ok([left, right]): Result<[Arc<dyn ExecutionPlan>; 2], _> = children.try_into()
            else {
                return Ok(Transformed::no(plan));
            };
            if left.downcast_ref::<NljBuildSideExec>().is_some() {
                return Ok(Transformed::no(plan));
            }
            let wrapped = Arc::new(NljBuildSideExec::new(left, policy)) as Arc<dyn ExecutionPlan>;
            plan.with_new_children(vec![wrapped, right])
                .map(Transformed::yes)
        })
        .data()
    }

    fn name(&self) -> &'static str {
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
    use datafusion::physical_plan::Partitioning;
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
        let plan = Arc::new(NljBuildSideExec::new(
            repartitioned(empty_input()),
            BuildSidePolicy::Spill,
        ));
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
        let wrapped = join.children()[0]
            .downcast_ref::<NljBuildSideExec>()
            .expect("the left (build) child is wrapped");
        assert_eq!(
            wrapped.policy(),
            BuildSidePolicy::Spill,
            "an inner join with one right partition spills"
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
        let wrapped: Arc<dyn ExecutionPlan> = Arc::new(NljBuildSideExec::new(
            Arc::clone(&inner),
            BuildSidePolicy::Spill,
        ));
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
        Arc::new(NljBuildSideExec::new(
            Arc::clone(&inner),
            BuildSidePolicy::Spill,
        ))
        .with_new_children(vec![])
        .expect_err("a child count other than one refuses");
    }

    fn join_with(join_type: JoinType, right_partitions: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let empty: Vec<Vec<arrow::array::RecordBatch>> = vec![Vec::new(); right_partitions];
        let right =
            datafusion::physical_plan::test::TestMemoryExec::try_new_exec(&empty, schema, None)
                .expect("a multi-partition memory right side builds");
        Arc::new(
            NestedLoopJoinExec::try_new(empty_input(), right, None, &join_type, None)
                .expect("a filter-free nested-loop join builds"),
        )
    }

    fn wrapped_policy(plan: Arc<dyn ExecutionPlan>) -> BuildSidePolicy {
        let rule = NljBuildSideReset;
        let rewritten = rule
            .optimize(plan, &ConfigOptions::new())
            .expect("the rule applies");
        let join = rewritten
            .downcast_ref::<NestedLoopJoinExec>()
            .expect("the join node survives the rule");
        join.children()[0]
            .downcast_ref::<NljBuildSideExec>()
            .expect("the build child is wrapped")
            .policy()
    }

    #[test]
    fn the_rule_refuses_only_the_left_family_past_one_right_partition() {
        use BuildSidePolicy::{RefuseFallback, Spill};
        let cases = [
            (JoinType::Inner, 1, Spill),
            (JoinType::Inner, 4, Spill),
            (JoinType::Left, 1, Spill),
            (JoinType::Left, 4, RefuseFallback),
            (JoinType::LeftSemi, 1, Spill),
            (JoinType::LeftSemi, 4, RefuseFallback),
            (JoinType::LeftAnti, 1, Spill),
            (JoinType::LeftAnti, 4, RefuseFallback),
            (JoinType::LeftMark, 1, Spill),
            (JoinType::LeftMark, 4, RefuseFallback),
            (JoinType::Right, 4, Spill),
            (JoinType::RightSemi, 4, Spill),
            (JoinType::RightAnti, 4, Spill),
            (JoinType::RightMark, 4, Spill),
            (JoinType::Full, 1, Spill),
            (JoinType::Full, 4, Spill),
        ];
        for (join_type, right_partitions, expected) in cases {
            assert_eq!(
                wrapped_policy(join_with(join_type, right_partitions)),
                expected,
                "the policy must split exactly on the documented set"
            );
        }
    }

    fn refusing_context() -> Arc<TaskContext> {
        use datafusion::execution::memory_pool::{FairSpillPool, MemoryConsumer, MemoryPool};
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;

        use crate::pool_refusals::{PoolRefusalLog, RefusalRecordingPool};
        let log = Arc::new(PoolRefusalLog::default());
        let pool: Arc<dyn MemoryPool> = Arc::new(RefusalRecordingPool::new(
            Arc::new(FairSpillPool::new(64 * 1024 * 1024)),
            Arc::clone(&log),
        ));
        MemoryConsumer::new("probe")
            .register(&pool)
            .try_grow(1024 * 1024 * 1024)
            .expect_err("one gigabyte does not fit sixty-four megabytes");
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .expect("a test runtime builds");
        Arc::new(TaskContext::default().with_runtime(runtime))
    }

    #[tokio::test]
    async fn the_second_execute_refuses_typed_when_the_fallback_is_unsafe() {
        let plan = Arc::new(NljBuildSideExec::new(
            repartitioned(two_partition_input()),
            BuildSidePolicy::RefuseFallback,
        ));
        let context = refusing_context();
        let first = plan
            .execute(0, Arc::clone(&context))
            .expect("the load execute succeeds");
        drain_rows(first).await;
        let Err(error) = plan.execute(0, Arc::clone(&context)) else {
            panic!("the fallback execute must not produce rows")
        };
        let message = error.to_string();
        assert!(
            message.to_lowercase().contains("resources exhausted") && message.contains("fair("),
            "the refusal is the genuine pool text, got: {message}"
        );
        assert!(
            message.contains("the bounded memory pool refused this plan"),
            "the refusal carries the containment disclosure, got: {message}"
        );
    }

    #[tokio::test]
    async fn the_second_execute_spills_when_no_refusal_was_recorded() {
        let plan = Arc::new(NljBuildSideExec::new(
            repartitioned(two_partition_input()),
            BuildSidePolicy::RefuseFallback,
        ));
        let context = Arc::new(TaskContext::default());
        for _ in 0..2 {
            let stream = plan
                .execute(0, Arc::clone(&context))
                .expect("no recorded refusal means nothing to refuse with");
            drain_rows(stream).await;
        }
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
        let wrapped = NljBuildSideExec::new(single, BuildSidePolicy::Spill);
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
