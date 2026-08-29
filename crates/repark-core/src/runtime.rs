//! [`EngineRuntime`] is an embedder-owned executor handle.
//!
//! Core never constructs or blocks on a runtime. The embedding owns runtime lifetime and may use
//! the handle's accessor or `block_on` boundary.

use std::sync::Arc;

use tokio::runtime::Runtime;

/// ===========================================================================================
/// Shared, cloneable pointer to an embedder-owned Tokio [`Runtime`].
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

#[cfg(test)]
mod tests;
