//! Register-only [`SessionExtension`] for the TA window UDFs.

use datafusion::prelude::SessionContext;
use repark_core::SessionExtension;

/// The TA door-neutral session extension.
#[derive(Debug, Clone, Copy, Default)]
pub struct TaExtension;

impl SessionExtension for TaExtension {
    /// Register the TA window UDFs on the session context.
    /// # Errors
    /// None — [`udf::register_all`](crate::udf::register_all) is infallible; the seam's `Result`
    fn register(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        crate::udf::register_all(ctx);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
