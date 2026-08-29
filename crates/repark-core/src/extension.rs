//! Build-time registration seam for SQL-door extensions.
//!
//! `configure` runs before runtime construction and `register` runs after context creation. The
//! default hooks preserve a pure-DataFusion session.

use std::collections::HashMap;

use datafusion::prelude::{SessionConfig, SessionContext};

use crate::session_time_zone::SessionTimeZone;

/// ===========================================================================================
/// Values already resolved by `build()` and passed to the configure hook.
/// ===========================================================================================
#[derive(Debug, Clone, Copy)]
pub struct SessionBuildConf<'a> {
    /// The builder's full Spark-style `.config(key, value)` map, as before.
    pub conf: &'a HashMap<String, String>,
    /// The validated session timezone (`spark.sql.session.timeZone`), resolved once in `build()`.
    pub session_time_zone: &'a SessionTimeZone,
}

/// ===========================================================================================
/// Build-time extension with configure-then-register hooks.
/// ===========================================================================================
pub trait SessionExtension: Send + Sync {
    /// Amend the [`SessionConfig`] before the runtime and context are assembled (v1 position:
    /// the cardinality/`repark.sql.*` `ConfigExtension` install). [`SessionBuildConf::conf`] is
    /// the builder's full Spark-style `.config(key, value)` map, so an extension parses its own
    /// keys the way v1's inline code did; [`SessionBuildConf::session_time_zone`] is the value
    /// `build()` already resolved, for the door that must carry it down to the function layer.
    ///
    /// # Errors
    /// A malformed conf value → [`datafusion::error::DataFusionError`]; `build()` folds it into
    /// the crate [`Error`](crate::Error) via [`engine_err`](crate::engine_err).
    fn configure(
        &self,
        session: SessionBuildConf<'_>,
        config: SessionConfig,
    ) -> datafusion::error::Result<SessionConfig> {
        let _ = session;
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
