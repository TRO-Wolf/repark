//! The execution-backend seam — a local execution-context holder, and the deliberately-minimal
//! extension point behind which distribution is deferred.
//!
//! [`ExecutionBackend`] names *where* a query executes. There is exactly one implementation,
//! [`SingleNodeBackend`] (in-process DataFusion), and the whole trait surface is a single method
//! that hands back a **concrete** DataFusion [`SessionContext`]. The *trait boundary* is the
//! load-bearing part — keeping the session behind it means a future distributed coordinator can
//! be introduced without reworking the write path — **not** its current surface.
//!
//! Read honestly, this seam is **not** evidence that distribution needs no wider change. Because
//! the method hands back a `SessionContext` by reference, callers today use single-node DataFusion
//! facilities directly; a real distributed backend (a custom coordinator — **not** Ballista,
//! which cannot serialize Iceberg write/commit plan nodes) would require widening this surface and
//! revisiting those call sites, not merely adding a second `impl`. Distribution is deferred by
//! decision (`docs/adr/0004-server-prep-disciplines.md`); single-node DataFusion is the target for
//! the first release and handles the intended workload. The honest prose lives in
//! `ARCHITECTURE.md`, "`ExecutionBackend` — what the seam is, honestly"; the current-state entry
//! is `STATUS.md` "Architectural risks".

use datafusion::prelude::SessionContext;

/// ===========================================================================================
/// The local execution-context holder — and the deliberately-minimal extension point behind
/// which distribution is deferred.
///
/// The surface is one method returning the **concrete** DataFusion [`SessionContext`] the rest of
/// the engine plans and executes against, so callers reach single-node DataFusion facilities
/// through it directly. The trait boundary — not this surface — is the load-bearing part: the
/// surface would have to widen (and its call sites be revisited) before a distributed backend
/// could exist, so a second `impl` alone is not what distribution would take. Module doc has the
/// full framing.
/// ===========================================================================================
pub trait ExecutionBackend: Send + Sync {
    /// The DataFusion session this backend executes against.
    fn session_context(&self) -> &SessionContext;
}

/// ===========================================================================================
/// `SingleNodeBackend` — in-process DataFusion. The default and, today, the only implementation:
/// every session plans and executes on one node.
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
