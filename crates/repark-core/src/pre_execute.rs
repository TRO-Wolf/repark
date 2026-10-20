//! Shared pre-execute sequencing for every SQL door: plan, guard, then execute.

use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, LogicalPlan, WriteOp};
use datafusion::prelude::{DataFrame, SessionContext};

use crate::catalog_state::CatalogRegistry;
use crate::dialect::EngineContext;
use crate::sorted_view::refuse_iceberg_create_of_tightened_ddl;

/// The per-statement belt: plan without executing, guard the plan, then execute it.
// No `Debug`: neither `SessionContext` nor `CatalogRegistry` implements it.
#[derive(Clone, Copy)]
pub struct PreExecute<'a> {
    ctx: &'a SessionContext,
    catalogs: &'a CatalogRegistry,
}

impl<'a> PreExecute<'a> {
    /// Build a belt over a context and the session's Iceberg catalog registry.
    #[must_use]
    pub fn new(ctx: &'a SessionContext, catalogs: &'a CatalogRegistry) -> Self {
        Self { ctx, catalogs }
    }

    /// Build a belt from the [`EngineContext`] a dialect already receives.
    #[must_use]
    pub fn from_engine_context(cx: &EngineContext<'a>) -> Self {
        Self::new(cx.ctx, cx.catalogs)
    }

    /// Plan a SQL string **without executing it**.
    /// # Errors
    /// Any parse/plan failure, verbatim; the ANSI door upgrades planner errors itself.
    pub async fn plan(&self, sql: &str) -> DfResult<LogicalPlan> {
        self.ctx.state().create_logical_plan(sql).await
    }

    /// Apply every pre-execute refusal to a planned statement before execution.
    /// # Errors
    /// [`DataFusionError::Plan`] carrying the refusal message.
    pub fn guard(&self, plan: &LogicalPlan) -> DfResult<()> {
        refuse_iceberg_create_of_tightened_ddl(plan, self.ctx, self.catalogs)
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        refuse_update_from_subquery(plan)?;
        crate::named_sources::refuse_source_ddl(plan, self.ctx, self.catalogs)
    }

    /// Execute an already-guarded plan.
    /// # Errors
    /// Any execution failure.
    pub async fn execute(&self, plan: LogicalPlan) -> DfResult<DataFrame> {
        self.ctx.execute_logical_plan(plan).await
    }

    /// The whole belt for a door with no door-specific plan-time work: plan → guard → execute.
    /// # Errors
    /// The plan error, the guard's [`DataFusionError::Plan`] refusal, or the execution error.
    pub async fn run(&self, sql: &str) -> DfResult<DataFrame> {
        let plan = self.plan(sql).await?;
        self.guard(&plan)?;
        self.execute(plan).await
    }
}

const UPDATE_SUBQUERY_REFUSAL: &str = "UPDATE with SET column = (SELECT ...) is not \
     supported on this statement: RePark cannot write the subquery value here, so it refuses \
     before any write.";

fn refuse_update_from_subquery(plan: &LogicalPlan) -> DfResult<()> {
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(());
    };
    if !matches!(dml.op, WriteOp::Update) {
        return Ok(());
    }
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return Ok(());
    };
    if projection.expr.iter().any(assigns_a_subquery) {
        return Err(DataFusionError::Plan(UPDATE_SUBQUERY_REFUSAL.to_string()));
    }
    Ok(())
}

fn assigns_a_subquery(expr: &Expr) -> bool {
    expr.contains_scalar_subquery()
}

#[cfg(test)]
mod tests;
