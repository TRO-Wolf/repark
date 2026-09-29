use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;

/// Run Spark analyzer rules until schema changes reach the `TypeCoercion` fixpoint.
/// # Errors
/// Propagates analyzer-rule failures as [`datafusion::error::DataFusionError`].
pub fn analyze_eagerly(
    state: &SessionState,
    plan: LogicalPlan,
) -> datafusion::error::Result<LogicalPlan> {
    state
        .analyzer()
        .execute_and_check(plan, state.config_options(), |_, _| {})
}
