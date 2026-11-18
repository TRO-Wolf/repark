//! Build-time registration seam for SQL-door extensions.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};

use crate::session_time_zone::SessionTimeZone;

pub type FrameViewWrap =
    fn(&str, &DataFrame, &ConfigOptions, LiveOptions) -> Arc<dyn TableProvider>;

type ReadLive = dyn Fn() -> Option<Arc<ConfigOptions>> + Send + Sync;

#[derive(Clone, Default)]
pub struct LiveOptions(Option<Arc<ReadLive>>);

impl LiveOptions {
    #[must_use]
    pub fn new(read: Arc<ReadLive>) -> Self {
        Self(Some(read))
    }

    #[must_use]
    pub fn read(&self) -> Option<Arc<ConfigOptions>> {
        self.0.as_ref().and_then(|read| read())
    }
}

impl std::fmt::Debug for LiveOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("LiveOptions")
            .field(&self.0.is_some())
            .finish()
    }
}

#[derive(Clone)]
pub struct FrameViewSeat {
    wrap: FrameViewWrap,
    rule: Arc<dyn AnalyzerRule + Send + Sync>,
}

impl FrameViewSeat {
    #[must_use]
    pub fn new(wrap: FrameViewWrap, rule: Arc<dyn AnalyzerRule + Send + Sync>) -> Self {
        Self { wrap, rule }
    }

    pub(crate) fn rule(&self) -> Arc<dyn AnalyzerRule + Send + Sync> {
        Arc::clone(&self.rule)
    }

    pub(crate) fn wrap(
        &self,
        name: &str,
        frame: &DataFrame,
        live: &ConfigOptions,
        reader: LiveOptions,
    ) -> Arc<dyn TableProvider> {
        (self.wrap)(name, frame, live, reader)
    }
}

impl std::fmt::Debug for FrameViewSeat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrameViewSeat")
            .field("rule", &self.rule.name())
            .finish_non_exhaustive()
    }
}

#[must_use]
pub fn with_frame_view_seat(config: SessionConfig, seat: FrameViewSeat) -> SessionConfig {
    config.with_extension(Arc::new(seat))
}

/// Values already resolved by `build()` and passed to the configure hook.
#[derive(Debug, Clone, Copy)]
pub struct SessionBuildConf<'a> {
    /// The builder's full Spark-style `.config(key, value)` map, as before.
    pub conf: &'a HashMap<String, String>,
    /// The validated session timezone (`spark.sql.session.timeZone`), resolved once in `build()`.
    pub session_time_zone: &'a SessionTimeZone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignedZeroPolicy {
    pub literals: bool,
    pub keys: bool,
}

impl Default for SignedZeroPolicy {
    fn default() -> Self {
        Self {
            literals: true,
            keys: true,
        }
    }
}

#[must_use]
pub fn contains_normalize_zero(expr: &Expr) -> bool {
    expr.exists(|node| {
        Ok(matches!(node, Expr::ScalarFunction(call)
            if call.name() == repark_common::udf_names::NORMALIZE_ZERO_NAME))
    })
    .unwrap_or(true)
}

pub use repark_common::udf_names::NORMALIZE_ZERO_NAME;

#[must_use]
pub fn normalize_zero_call(expr: Expr) -> Expr {
    crate::session::normalize_zero_call(expr)
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

    #[expect(
        clippy::missing_errors_doc,
        reason = "The error contract is documented in map.md under the owner comment ban."
    )]
    fn configure_analyzer_rules(
        &self,
        rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> datafusion::error::Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
        Ok(rules)
    }

    fn type_planner(&self) -> Option<Arc<dyn TypePlanner>> {
        None
    }

    fn signed_zero_policy(&self) -> SignedZeroPolicy {
        SignedZeroPolicy::default()
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
