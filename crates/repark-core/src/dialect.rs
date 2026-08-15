//! The SQL dialect seam: how a statement front end plugs into [`ReparkSession::sql`].
//!
//! Phase-cut inversion (design §3 / forced-edit ledger #2): v1's `sql` made exactly one call into
//! the v1 SQL crate — `execute_with_read_only(ctx, catalogs, query, read_only)`. The phase-1
//! engine core cannot depend on the phase-2 statement router, so that call is inverted behind
//! [`SqlDialect`]: the session hands the dialect an [`EngineContext`] (the same field set v1
//! passed positionally, as a struct) and the dialect decides what the SQL string means.
//!
//! [`DataFusionDialect`] is the phase-1 default — plain `SessionContext::sql`. Reads and temp
//! views work as-is, and DELETE/UPDATE/INSERT already ride the fork `TableProvider` (ADR-0003).
//! The Spark statement interceptions (CTAS, MERGE INTO, ALTER, namespace DDL, time-travel SQL
//! rewriting, …) return as the phase-2 Spark door's `SqlDialect` impl, whose body is v1's
//! `execute_with_read_only` — the seam mirrors its signature for exactly that re-home.
//!
//! **UNSTABLE until phase 2:** this contract (field set, error surface, statement scope) is
//! documented provisional until the phase-2 doors land and exercise it. Seam growth happens by
//! adding [`EngineContext`] fields (`#[non_exhaustive]` keeps that non-breaking), never by
//! changing the `execute` signature.
//!
//! [`ReparkSession::sql`]: crate::ReparkSession::sql

use std::collections::HashSet;

use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};

use crate::catalog_state::CatalogRegistry;

/// ===========================================================================================
/// Everything a [`SqlDialect`] receives from the session for one statement execution.
///
/// Struct-extensible (the server-first graft): the field set mirrors v1
/// `execute_with_read_only(ctx, catalogs, query, read_only)` exactly, and new seam inputs land
/// as new fields — non-breaking for downstream matchers under `#[non_exhaustive]` — never as
/// signature changes.
/// ===========================================================================================
#[non_exhaustive]
pub struct EngineContext<'a> {
    /// The DataFusion context this statement plans and executes against.
    pub ctx: &'a SessionContext,
    /// Per-query snapshot of the session's Iceberg catalog registry (the session takes a cheap
    /// clone — keys + `Arc`s — so no lock is held across an `.await`).
    pub catalogs: &'a CatalogRegistry,
    /// Read-only (postgres) catalog names for the P11 DML direction-notes.
    pub read_only: &'a HashSet<String>,
}

impl<'a> EngineContext<'a> {
    /// Assemble a context from the three v1 positional arguments. `#[non_exhaustive]` forbids
    /// literal construction outside this crate, and door crates build a context in their own
    /// tests — this is the one sanctioned way (added phase-2 PR-2; new seam inputs land as
    /// defaulted builder-style setters beside it, never as `new` signature changes).
    #[must_use]
    pub fn new(
        ctx: &'a SessionContext,
        catalogs: &'a CatalogRegistry,
        read_only: &'a HashSet<String>,
    ) -> Self {
        Self {
            ctx,
            catalogs,
            read_only,
        }
    }
}

/// ===========================================================================================
/// A statement front end: parse, route, and execute ONE SQL string against an [`EngineContext`].
///
/// The session's [`sql`](crate::ReparkSession::sql) runs the session-default dialect;
/// [`sql_with`](crate::ReparkSession::sql_with) lets two doors share one session (ADR-0002
/// "one test row per door"). Implementations must be cheap to call per statement — the session
/// constructs a fresh [`EngineContext`] snapshot every call.
/// ===========================================================================================
// ?Send: rustc 1.96 HRTB rejects the default Send future once the iceberg Catalog
// object (inside CatalogRegistry) is in the crate graph after the AD-1 pin. The
// session awaits `execute` in place (`sql_with`); tokio::spawn is banned.
#[async_trait(?Send)]
pub trait SqlDialect: Send + Sync {
    /// Execute one SQL statement against the engine context.
    ///
    /// # Errors
    /// Any parse / plan / execution failure as a [`datafusion::error::DataFusionError`]; the
    /// session folds it into the crate [`Error`](crate::Error) taxonomy via
    /// [`engine_err`](crate::engine_err) — the fold stays session-side so every dialect gets the
    /// same classification.
    async fn execute(
        &self,
        cx: EngineContext<'_>,
        query: &str,
    ) -> datafusion::error::Result<DataFrame>;
}

/// ===========================================================================================
/// The phase-1 default dialect: plain `SessionContext::sql` (DataFusion semantics).
///
/// Consequence, stated plainly (design §3): the phase-1 native core has DataFusion semantics —
/// Spark semantics are the Spark door's dialect + extension by definition.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, Default)]
pub struct DataFusionDialect;

#[async_trait(?Send)]
impl SqlDialect for DataFusionDialect {
    async fn execute(
        &self,
        cx: EngineContext<'_>,
        query: &str,
    ) -> datafusion::error::Result<DataFrame> {
        cx.ctx.sql(query).await
    }
}

#[cfg(test)]
mod tests;
