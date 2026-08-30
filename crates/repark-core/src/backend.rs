//! Execution-backend seam for the local DataFusion context.

use datafusion::prelude::SessionContext;

/// Local execution-context holder and future backend boundary.
pub trait ExecutionBackend: Send + Sync {
    /// The DataFusion session this backend executes against.
    fn session_context(&self) -> &SessionContext;
}

/// The in-process DataFusion backend.
pub struct SingleNodeBackend {
    context: SessionContext,
}

impl SingleNodeBackend {
    /// Wrap an already-configured [`SessionContext`] (memory pool + functions registered).
    #[must_use]
    pub fn new(context: SessionContext) -> Self {
        Self { context }
    }
}

impl ExecutionBackend for SingleNodeBackend {
    fn session_context(&self) -> &SessionContext {
        &self.context
    }
}
