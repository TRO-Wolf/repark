use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;

#[allow(clippy::missing_errors_doc)]
pub fn analyze_eagerly(
    state: &SessionState,
    plan: LogicalPlan,
) -> datafusion::error::Result<LogicalPlan> {
    state
        .analyzer()
        .execute_and_check(plan, state.config_options(), |_, _| {})
}
