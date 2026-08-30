//! Build-time registration seam for SQL-door extensions.

use std::collections::HashMap;

use datafusion::prelude::{SessionConfig, SessionContext};

use crate::session_time_zone::SessionTimeZone;

/// Values already resolved by `build()` and passed to the configure hook.
#[derive(Debug, Clone, Copy)]
pub struct SessionBuildConf<'a> {
    /// The builder's full Spark-style `.config(key, value)` map, as before.
    pub conf: &'a HashMap<String, String>,
    /// The validated session timezone (`spark.sql.session.timeZone`), resolved once in `build()`.
    pub session_time_zone: &'a SessionTimeZone,
}

/// Build-time extension with configure-then-register hooks.
pub trait SessionExtension: Send + Sync {
    /// Amend the [`SessionConfig`] before the runtime and context are assembled.
    /// # Errors
    /// # Errors A malformed conf value → [`datafusion::error::DataFusionError`].
    fn configure(
        &self,
        session: SessionBuildConf<'_>,
        config: SessionConfig,
    ) -> datafusion::error::Result<SessionConfig> {
        let _ = session;
        Ok(config)
    }

    /// Register runtime objects on the freshly built [`SessionContext`].
    /// # Errors
    /// A registration failure is a DataFusion error; `build()` folds it via [`engine_err`].
    fn register(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        let _ = ctx;
        Ok(())
    }
}

/// The defaulted no-op extension `build` runs when no [`SessionExtension`] was supplied.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct NoopSessionExtension;

impl SessionExtension for NoopSessionExtension {}

#[cfg(test)]
mod tests;
