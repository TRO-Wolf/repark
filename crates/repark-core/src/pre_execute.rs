//! The shared pre-execute belt: **the one place a planned statement is inspected before it
//! runs**, for every door.
//!
//! Why this module exists (SQM round 5, Z-2 — an ALTITUDE fix, not another patch). The
//! `tightenNulls` DDL-sink refuse was wired per door: round 4 added it to
//! `repark_spark::spark_ast::execute_passthrough` and `repark_sql::router::delegate`, and both
//! times the **native** door — `ReparkSession::sql` on the default `DataFusionDialect`, which
//! is plain `SessionContext::sql` — kept persisting `CREATE VIEW ice.ns.v AS …` /
//! `SELECT … INTO ice.ns.t` with `required: true` columns (MEASURED, round-5 ledger). Wiring a
//! guard at N call sites fails at exactly the rate a new call site is added.
//!
//! So the guard moved up: [`PreExecute`] owns the plan → guard → execute sequence, and **every**
//! door passes its planned statement through [`PreExecute::guard`]:
//!
//! | door | how it reaches the belt |
//! |---|---|
//! | native (`ReparkSession::sql`, `DataFusionDialect`) | [`PreExecute::run`] (the whole belt) |
//! | ANSI (`repark_sql::router::delegate`) | [`PreExecute::plan`] → door guards → `guard` → [`PreExecute::execute`] |
//! | Spark (`repark_spark::spark_ast::execute_passthrough`) | `guard` on the statement's plan |
//! | ANSI CTAS derivation (`repark_sql::create_table`) | `plan` → `guard` → `execute` (never `ctx.sql`, which executes eagerly — Z-3) |
//!
//! **What the belt does NOT see, measured (round 6, R6-5): `PREPARE`.** A `PREPARE p AS CREATE
//! VIEW <iceberg>.ns.v AS …` reaches [`PreExecute::guard`] as a `Statement::Prepare`, whose
//! stored body the guard does not descend into — [`refuse_iceberg_create_of_tightened_ddl`]
//! matches an EXECUTED `DdlStatement`, not a prepared one. That class is **inert today** and
//! measured so, not assumed: on DataFusion 54.1 the `PREPARE` itself persists nothing, and
//! `EXECUTE p` fails at collect with `NotImplemented: Unsupported logical plan: CreateView` —
//! a prepared DDL cannot run at all. Pinned by `prepare_of_a_tightened_ddl_sink_is_inert_today`
//! (`repark-core/tests/temp_view_doors.rs`), which goes red the day `EXECUTE` starts running
//! stored DDL — at which point the guard must descend into the prepared body.
//!
//! The belt deliberately does NOT absorb door-specific guards (`refuse_local_filesystem_plan`,
//! the Spark AST rewrites, the eager-command fold): those differ per door and belong to the
//! door. What it owns is the sequencing rule — *nothing executes before the planned statement
//! has been through `guard`* — and the guard body itself.

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
    /// This is the half that makes the belt honest: `SessionContext::sql` plans *and* executes
    /// DDL/DML eagerly, so a guard placed after it runs after the write (Z-3).
    ///
    /// # Errors
    /// Any parse/plan failure, verbatim — callers that upgrade planner errors (the ANSI door's
    /// `sniff::upgrade_error`) map it themselves.
    pub async fn plan(&self, sql: &str) -> DfResult<LogicalPlan> {
        self.ctx.state().create_logical_plan(sql).await
    }

    /// **The choke point.** Run every pre-execute refusal against a planned statement.
    ///
    /// Today that is the SE-1 `tightenNulls` DDL-sink refuse
    /// ([`refuse_iceberg_create_of_tightened_ddl`]) — `CREATE VIEW <iceberg>.ns.v AS …` and
    /// `SELECT … INTO <iceberg>.ns.t`, gated on the RESOLVED catalog (Z-1). New pre-execute
    /// refusals land HERE, never at a door.
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
