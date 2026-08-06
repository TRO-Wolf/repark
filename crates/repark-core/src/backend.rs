//! The execution-backend seam — where distribution is deferred.
//!
//! [`ExecutionBackend`] abstracts *where* a query executes. v1 ships only [`SingleNodeBackend`]
//! (in-process DataFusion), which is more than enough for the target workload (single-node
//! DataFusion handles ~1 TB). A future `repark-distributed` crate provides an alternative impl
//! (a custom coordinator — **not** Ballista, which cannot serialize Iceberg write/commit plan
//! nodes). Keeping the session behind this trait is the non-negotiable invariant that lets that
//! land later without reworking the write path.

use datafusion::prelude::SessionContext;

/// ===========================================================================================
/// The seam behind which distribution is deferred.
///
/// Intentionally minimal for v1: it exposes the DataFusion [`SessionContext`] the rest of the
/// engine plans and executes against. The trait — not its surface — is the load-bearing part;
/// the surface grows when a distributed backend actually arrives.
/// ===========================================================================================
pub trait ExecutionBackend: Send + Sync {
    /// The DataFusion session this backend executes against.
    fn session_context(&self) -> &SessionContext;
}

/// ===========================================================================================
/// `SingleNodeBackend` — in-process DataFusion. The v1 default and only backend.
/// ===========================================================================================
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
