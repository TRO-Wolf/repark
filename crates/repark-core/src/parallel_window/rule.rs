use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result;
use datafusion::physical_expr::window::WindowExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::windows::WindowAggExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use super::exec::ParallelWindowExec;
use super::{PARALLEL_WINDOW_RULE, parallel_single_partition_enabled};

#[derive(Debug, Default)]
pub struct ParallelWindowRule;

impl PhysicalOptimizerRule for ParallelWindowRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !parallel_single_partition_enabled(config) {
            return Ok(plan);
        }
        plan.transform_up(|node| {
            let Some(window) = node.downcast_ref::<WindowAggExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(groups) = parallel_groups(window) else {
                return Ok(Transformed::no(node));
            };
            let parallel: Arc<dyn ExecutionPlan> =
                Arc::new(ParallelWindowExec::from_window(window, groups));
            Ok(Transformed::yes(parallel))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        PARALLEL_WINDOW_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn parallel_groups(window: &WindowAggExec) -> Option<Vec<Vec<usize>>> {
    let exprs = window.window_expr();
    if exprs.iter().any(|expr| !expr.partition_by().is_empty()) {
        return None;
    }
    if window.input().output_partitioning().partition_count() != 1 {
        return None;
    }
    let groups = argument_groups(exprs);
    (groups.len() >= 2).then_some(groups)
}

fn argument_groups(exprs: &[Arc<dyn WindowExpr>]) -> Vec<Vec<usize>> {
    let arguments: Vec<_> = exprs.iter().map(|expr| expr.expressions()).collect();
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (index, args) in arguments.iter().enumerate() {
        let shared = groups
            .iter_mut()
            .find(|group| group.first().is_some_and(|&lead| arguments[lead] == *args));
        match shared {
            Some(group) => group.push(index),
            None => groups.push(vec![index]),
        }
    }
    groups
}
