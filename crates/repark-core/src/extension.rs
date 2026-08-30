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
    /// Amend the [`SessionConfig`] before the runtime and context are assembled (v1 position: the
    /// # Errors
    /// A malformed conf value → [`datafusion::error::DataFusionError`]; `build()` folds it into
    fn configure(
        &self,
        session: SessionBuildConf<'_>,
        config: SessionConfig,
    ) -> datafusion::error::Result<SessionConfig> {
        let _ = session;
        Ok(config)
    }

    /// Register runtime objects on the freshly built [`SessionContext`] (v1 position: the Spark
    /// # Errors
    /// A registration failure → [`datafusion::error::DataFusionError`]; `build()` folds it via
    fn register(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        let _ = ctx;
        Ok(())
    }
}

/// The defaulted no-op extension `build()` runs when no [`SessionExtension`] was supplied — both
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct NoopSessionExtension;

impl SessionExtension for NoopSessionExtension {}

#[cfg(test)]
mod tests;
