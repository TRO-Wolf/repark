//! SQL dialect seam for plugging a statement front end into [`ReparkSession::sql`].

use std::collections::HashSet;

use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};

use crate::catalog_state::CatalogRegistry;

/// Everything a [`SqlDialect`] receives for one statement execution.
#[non_exhaustive]
pub struct EngineContext<'a> {
    /// The DataFusion context this statement plans and executes against.
    pub ctx: &'a SessionContext,
    /// Per-query snapshot of the session's Iceberg catalog registry (the session takes a cheap
    pub catalogs: &'a CatalogRegistry,
    /// Read-only (postgres) catalog names for the P11 DML direction-notes.
    pub read_only: &'a HashSet<String>,
}

impl<'a> EngineContext<'a> {
    /// Assemble a context from its session references.
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

/// A statement front end that parses, routes, and executes one SQL string.
// ?Send: rustc 1.96 HRTB rejects the default Send future once the iceberg Catalog object (inside
#[async_trait(?Send)]
pub trait SqlDialect: Send + Sync {
    /// Execute one SQL statement against the engine context.
    /// # Errors
    /// Any parse / plan / execution failure as a [`datafusion::error::DataFusionError`]; the
    async fn execute(
        &self,
        cx: EngineContext<'_>,
        query: &str,
    ) -> datafusion::error::Result<DataFrame>;
}

/// The default dialect, using plain `SessionContext::sql`.
#[derive(Debug, Clone, Copy, Default)]
pub struct DataFusionDialect;

#[async_trait(?Send)]
impl SqlDialect for DataFusionDialect {
    async fn execute(
        &self,
        cx: EngineContext<'_>,
        query: &str,
    ) -> datafusion::error::Result<DataFrame> {
        // Keep the native door's plan → guard → execute ordering.
        crate::PreExecute::from_engine_context(&cx).run(query).await
    }
}

#[cfg(test)]
mod tests;
