use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Statistics, internal_err};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

pub(crate) const ENFORCE_SORTING_RULE_NAME: &str = "EnforceSorting";

pub(crate) fn skip_safe_physical_optimizer_rules()
-> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    PhysicalOptimizer::new()
        .rules
        .into_iter()
        .map(|rule| {
            if rule.name() == ENFORCE_SORTING_RULE_NAME {
                Arc::new(SkipSafeEnforceSorting { inner: rule })
                    as Arc<dyn PhysicalOptimizerRule + Send + Sync>
            } else {
                rule
            }
        })
        .collect()
}

#[derive(Debug)]
struct SkipSafeEnforceSorting {
    inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
}

impl PhysicalOptimizerRule for SkipSafeEnforceSorting {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !plan.exists(|node| Ok(is_skipping_limit(node)))? {
            return self.inner.optimize(plan, config);
        }
        let sealed = plan
            .transform_up(|node| {
                if !is_skipping_limit(&node) {
                    return Ok(Transformed::no(node));
                }
                let inputs = node
                    .children()
                    .into_iter()
                    .map(|input| self.inner.optimize(Arc::clone(input), config))
                    .collect::<DataFusionResult<Vec<_>>>()?;
                let limit = node.with_new_children(inputs)?;
                Ok(Transformed::yes(
                    Arc::new(SealedSkippingLimitExec { limit }) as Arc<dyn ExecutionPlan>,
                ))
            })
            .data()?;
        self.inner
            .optimize(sealed, config)?
            .transform_down(|node| {
                let unsealed = node
                    .downcast_ref::<SealedSkippingLimitExec>()
                    .map(|seal| Arc::clone(&seal.limit));
                Ok(unsealed.map_or_else(|| Transformed::no(node), Transformed::yes))
            })
            .data()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema_check(&self) -> bool {
        self.inner.schema_check()
    }
}

#[cfg(test)]
pub(crate) fn sealed_for_test(limit: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(SealedSkippingLimitExec { limit })
}

fn is_skipping_limit(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.downcast_ref::<GlobalLimitExec>()
        .is_some_and(|limit| limit.skip() > 0)
}

#[derive(Debug)]
struct SealedSkippingLimitExec {
    limit: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for SealedSkippingLimitExec {
    fn fmt_as(
        &self,
        _mode: DisplayFormatType,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(formatter, "SealedSkippingLimitExec")
    }
}

#[allow(clippy::missing_errors_doc)]
impl ExecutionPlan for SealedSkippingLimitExec {
    fn name(&self) -> &'static str {
        "SealedSkippingLimitExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.limit.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!(
                "SealedSkippingLimitExec takes no child, got {}",
                children.len()
            )
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        internal_err!("SealedSkippingLimitExec is a planning placeholder and does not execute")
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Arc<Statistics>> {
        self.limit.partition_statistics(partition)
    }
}
