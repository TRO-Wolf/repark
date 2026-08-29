//! Register-only [`SessionExtension`] for the TA window UDFs.
//!
//! The extension forwards registration to [`udf::register_all`](crate::udf::register_all).
//! `configure` remains the trait default because TA installs no configuration extension.
//! Feature `datafusion` gates this module and the UDF layer.

use datafusion::prelude::SessionContext;
use repark_core::SessionExtension;

/// ===========================================================================================
/// The TA door-neutral session extension.
///
/// Install it with `ReparkSessionBuilder::with_extension` to register every `ta_*` UDF.
/// Extensions are session-scoped, so the UDFs are available through every door.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, Default)]
pub struct TaExtension;

impl SessionExtension for TaExtension {
    /// Register the TA window UDFs on the session context.
    ///
    /// # Errors
    /// None — [`udf::register_all`](crate::udf::register_all) is infallible; the seam's
    /// `Result` is kept because the trait spells one.
    fn register(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        crate::udf::register_all(ctx);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
