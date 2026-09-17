//! SQL dialect seam for plugging a statement front end into [`ReparkSession::sql`].

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};

use crate::catalog_state::CatalogRegistry;

/// Everything a [`SqlDialect`] receives for one statement execution.
#[non_exhaustive]
pub struct EngineContext<'a> {
    /// The DataFusion context this statement plans and executes against.
    pub ctx: &'a SessionContext,
    /// Per-query snapshot of the session's Iceberg catalog registry.
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
// ?Send: rustc 1.96 HRTB rejects the default Send future once CatalogRegistry is in the graph.
#[async_trait(?Send)]
pub trait SqlDialect: Send + Sync {
    /// Execute one SQL statement against the engine context.
    /// # Errors
    /// # Errors Any parse / plan / execution failure as a [`datafusion::error::DataFusionError`].
    async fn execute(
        &self,
        cx: EngineContext<'_>,
        query: &str,
    ) -> datafusion::error::Result<DataFrame>;

    #[allow(clippy::missing_errors_doc)]
    async fn execute_with_write_options(
        &self,
        cx: EngineContext<'_>,
        query: &str,
        options: &HashMap<String, String>,
    ) -> datafusion::error::Result<DataFrame> {
        if !options.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "write options are not supported on this SQL door ({} option(s) refused)",
                options.len()
            )));
        }
        self.execute(cx, query).await
    }

    /// Install dialect-owned hooks on the freshly built [`SessionContext`].
    fn on_session_built(&self, ctx: &SessionContext) {
        let _ = ctx;
    }
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
