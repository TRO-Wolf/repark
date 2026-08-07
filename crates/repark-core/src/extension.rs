//! The registration seam: what a door installs into the session at build time.
//!
//! Phase-cut inversion (design §3 / forced-edit ledger #3): v1's `build()` inlined the phase-2
//! registrations — the Spark function registry, the expression-semantics analyzer rules, the TA
//! window UDFs, and the cardinality/`repark.sql.*` `ConfigExtension`. The phase-1 engine core
//! cannot depend on those crates, so `build()` exposes the two positions as [`SessionExtension`]
//! hooks at the SAME points in v1's construction order: [`configure`](SessionExtension::configure)
//! after the write knobs are installed on the `SessionConfig` and BEFORE the `RuntimeEnv` is
//! assembled; [`register`](SessionExtension::register) immediately AFTER the `SessionContext`
//! is created. Phase-2 repark-spark ships one extension holding exactly what v1 inlined.
//!
//! Both hooks are defaulted (the trait-wrapping both-sides audit applies): a session built
//! without an extension gets pure DataFusion semantics.
//!
//! **UNSTABLE until phase 2:** the hook contract is documented provisional until the phase-2
//! doors land and exercise it.

use std::collections::HashMap;

use datafusion::prelude::{SessionConfig, SessionContext};

/// ===========================================================================================
/// Build-time session extension — two hooks at v1's inline registration positions.
///
/// Install with [`ReparkSessionBuilder::with_extension`](crate::ReparkSessionBuilder); `build()`
/// invokes [`configure`](Self::configure) then [`register`](Self::register) exactly once each.
/// ===========================================================================================
pub trait SessionExtension: Send + Sync {
    /// Amend the [`SessionConfig`] before the runtime and context are assembled (v1 position:
    /// the cardinality/`repark.sql.*` `ConfigExtension` install). `conf` is the builder's full
    /// Spark-style `.config(key, value)` map, so an extension parses its own keys the way v1's
    /// inline code did.
    ///
    /// # Errors
    /// A malformed conf value → [`datafusion::error::DataFusionError`]; `build()` folds it into
    /// the crate [`Error`](crate::Error) via [`engine_err`](crate::engine_err).
    fn configure(
        &self,
        conf: &HashMap<String, String>,
        config: SessionConfig,
    ) -> datafusion::error::Result<SessionConfig> {
        let _ = conf;
        Ok(config)
    }

    /// Register runtime objects on the freshly built [`SessionContext`] (v1 position: the Spark
    /// function registry + analyzer rules + TA window UDFs, straight after
    /// `SessionContext::new_with_config_rt`).
    ///
    /// # Errors
    /// A registration failure → [`datafusion::error::DataFusionError`]; `build()` folds it via
    /// [`engine_err`](crate::engine_err).
    fn register(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        let _ = ctx;
        Ok(())
    }
}

/// The defaulted no-op extension `build()` runs when no [`SessionExtension`] was supplied —
/// both hooks inherit the trait defaults, so the no-extension session is the pure-DataFusion
/// baseline by construction.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct NoopSessionExtension;

impl SessionExtension for NoopSessionExtension {}

#[cfg(test)]
mod tests;
