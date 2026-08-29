//! Shared pre-execute sequencing for every SQL door: plan, guard, then execute.
//!
//! `PREPARE` is intentionally outside the guard's stored-body walk. DataFusion 54.1 persists
//! nothing for prepared DDL and cannot execute its `CreateView` plan; the pinned behavior must be
//! revisited if that changes. Door-specific guards remain in their respective doors.

use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionContext};

use crate::catalog_state::CatalogRegistry;
use crate::dialect::EngineContext;
use crate::sorted_view::refuse_iceberg_create_of_tightened_ddl;

/// ===========================================================================================
/// The per-statement belt: plan without executing, guard the plan, then execute it.
///
/// Cheap to build (two borrows) — construct one per statement, exactly like [`EngineContext`].
/// ===========================================================================================
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
    ///
    /// Planning is side-effect free, so callers can guard the plan before any write.
    ///
    /// # Errors
    /// Any parse/plan failure, verbatim — callers that upgrade planner errors (the ANSI door's
    /// `sniff::upgrade_error`) map it themselves.
    pub async fn plan(&self, sql: &str) -> DfResult<LogicalPlan> {
        self.ctx.state().create_logical_plan(sql).await
    }

    /// Apply every pre-execute refusal to a planned statement before execution.
    ///
    /// # Errors
    /// [`DataFusionError::Plan`] carrying the refusal message.
    pub fn guard(&self, plan: &LogicalPlan) -> DfResult<()> {
        refuse_iceberg_create_of_tightened_ddl(plan, self.ctx, self.catalogs)
            .map_err(|error| DataFusionError::Plan(error.to_string()))
    }

    /// Execute an already-guarded plan.
    ///
    /// # Errors
    /// Any execution failure.
    pub async fn execute(&self, plan: LogicalPlan) -> DfResult<DataFrame> {
        self.ctx.execute_logical_plan(plan).await
    }

    /// The whole belt for a door with no door-specific plan-time work: plan → guard → execute.
    ///
    /// # Errors
    /// The plan error, the guard's [`DataFusionError::Plan`] refusal, or the execution error.
    pub async fn run(&self, sql: &str) -> DfResult<DataFrame> {
        let plan = self.plan(sql).await?;
        self.guard(&plan)?;
        self.execute(plan).await
    }
}

#[cfg(test)]
mod tests;
