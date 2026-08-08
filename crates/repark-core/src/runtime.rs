//! [`EngineRuntime`] — the embedding's executor handle (EC-5; design §3, §4 Q7).
//!
//! The engine deliberately owns no runtime: every [`crate::ReparkSession`] entry point is
//! `async`, nothing in this crate blocks, and the **embedding** supplies the executor. The
//! phase-1 omissions ledger nevertheless resolved that the `EngineRuntime` *name* "becomes
//! engine API the day the binding ports, additively" — so the TYPE lives here and the
//! process-wide INSTANCE lives in the embedding (`repark-python`'s `OnceLock`, same lifetime
//! and behavior as at the port pin).
//!
//! **Core never constructs one.** This module has no `Runtime::new` call and no default: an
//! `EngineRuntime` can only be built by an embedder handing in a runtime it already owns. A
//! second embedding (a Flight SQL handler is the anticipated one) gets a named type to hold
//! rather than a convention to rediscover.

use std::sync::Arc;

use tokio::runtime::Runtime;

/// ===========================================================================================
/// The embedding's executor handle: a shared, cloneable pointer to a Tokio [`Runtime`] the
/// **embedder** built and owns.
///
/// Cloning is an `Arc` clone — every clone drives the same executor, which is what makes
/// "one runtime per process, shared by every session" expressible as a value instead of a
/// convention. `repark-core` never constructs, configures, or blocks on one; it only names the
/// handle so embedders (and a second embedding later) agree on the shape.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use repark_core::EngineRuntime;
///
/// // The EMBEDDING owns the runtime; core only holds the handle.
/// let runtime = EngineRuntime::new(Arc::new(tokio::runtime::Runtime::new().unwrap()));
/// let answer = runtime.block_on(async { 1 + 1 });
/// assert_eq!(answer, 2);
/// ```
/// ===========================================================================================
#[derive(Debug, Clone)]
pub struct EngineRuntime {
    runtime: Arc<Runtime>,
}

impl EngineRuntime {
    /// ===========================================================================================
    /// Adopt an embedder-owned Tokio runtime as this engine's executor handle.
    /// ===========================================================================================
    #[must_use]
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self { runtime }
    }

    /// ===========================================================================================
    /// Borrow the underlying shared runtime (for `Arc::clone` / `Arc::ptr_eq` by the embedder).
    /// ===========================================================================================
    #[must_use]
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    /// ===========================================================================================
    /// Drive `future` to completion on the handled runtime.
    ///
    /// This is the **embedding's** blocking boundary (a synchronous Python method, a request
    /// handler), never one core takes on its own behalf.
    /// ===========================================================================================
    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }
}

impl From<Arc<Runtime>> for EngineRuntime {
    fn from(runtime: Arc<Runtime>) -> Self {
        Self::new(runtime)
    }
}

#[cfg(test)]
mod tests;
