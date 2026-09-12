use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

use super::{UnpivotExec, UnpivotNode};

#[derive(Debug)]
pub struct StackQueryPlanner;

#[async_trait]
impl QueryPlanner for StackQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(StackExtensionPlanner)]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

#[derive(Debug)]
struct StackExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for StackExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(unpivot) = node.as_any().downcast_ref::<UnpivotNode>() else {
            return Ok(None);
        };
        let input = physical_inputs.first().cloned().ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "UnpivotExec requires one physical input".to_string(),
            )
        })?;
        Ok(Some(Arc::new(UnpivotExec::new(input, unpivot))))
    }
}
