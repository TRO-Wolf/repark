//! `TaExtension` — the register-only [`SessionExtension`] for the TA window UDFs.
//!
//! Design SSOT: [`docs/design/sql-doors.md`] Q11 — the TA function set is owned by **neither**
//! SQL door. It ports as its own crate with a thin extension; the Spark door composes it (v1
//! parity — v1's `ReparkSessionBuilder::build()` called `repark_ta::udf::register_all` inline,
//! immediately after the Spark function registry + analyzer rules), and a native session opts in
//! by installing this extension itself.
//!
//! The wrapper is deliberately thin — it adds no behaviour over
//! [`udf::register_all`](crate::udf::register_all). The trait-wrapping both-sides audit applies:
//! [`configure`](SessionExtension::configure) is left at the trait default (TA registers no
//! `ConfigExtension` and reads no `repark.*` conf key — v1 did not either), and
//! [`register`](SessionExtension::register) forwards the whole UDF set.
//!
//! Feature-gated behind `datafusion` alongside [`crate::udf`]: the kernel core stays
//! dependency-light and independently publishable.

use datafusion::prelude::SessionContext;
use repark_core::SessionExtension;

/// ===========================================================================================
/// The TA door-neutral session extension: every `ta_*` window UDF, registered.
///
/// Install with `ReparkSessionBuilder::with_extension(Arc::new(TaExtension))` for a native
/// session; the Spark door composes it from `SparkExtension` so Spark-extended sessions keep
/// v1's behaviour without opting in. Extensions are session-scoped, not dialect-scoped — once
/// installed the UDFs are callable through every door.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, Default)]
pub struct TaExtension;

impl SessionExtension for TaExtension {
    /// v1 position: immediately after the Spark function registry + analyzer rules — the TA
    /// window UDFs (`ta_ema`, `ta_adx`, `ta_bbands_*`, …), SQL- and DataFrame-callable.
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
