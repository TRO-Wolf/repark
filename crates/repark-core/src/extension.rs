//! Build-time registration seam for SQL-door extensions.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use datafusion::catalog::TableProvider;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DFSchemaRef, internal_datafusion_err};
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::optimizer::{Analyzer, AnalyzerRule, OptimizerRule};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use repark_iceberg::catalog::session_tables::HeldTable;

use crate::session_time_zone::SessionTimeZone;

pub type FrameViewWrap =
    fn(&str, &DataFrame, &ConfigOptions, LiveOptions) -> Arc<dyn TableProvider>;

type ReadLive = dyn Fn() -> Option<Arc<ConfigOptions>> + Send + Sync;

const LIVE_READ_WAIT: Duration = Duration::from_secs(1);
const READER_PARSE_NODE: &str = "ReaderParse";

struct LastRead {
    read: Arc<ReadLive>,
    last: Mutex<Arc<ConfigOptions>>,
}

#[derive(Clone, Default)]
pub struct LiveOptions(Option<Arc<LastRead>>);

impl LiveOptions {
    #[must_use]
    pub fn new(start: Arc<ConfigOptions>, read: Arc<ReadLive>) -> Self {
        Self(Some(Arc::new(LastRead {
            read,
            last: Mutex::new(start),
        })))
    }

    #[must_use]
    pub fn read(&self) -> Option<Arc<ConfigOptions>> {
        let live = self.0.as_ref()?;
        let fresh = (live.read)();
        let mut last = live.last.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(fresh) = fresh {
            *last = fresh;
        }
        Some(Arc::clone(&last))
    }

    #[must_use]
    pub fn of_session(context: &SessionContext) -> Self {
        let shared = context.state_ref();
        let start = Arc::clone(shared.read().config_options());
        let weak = Arc::downgrade(&shared);
        Self::new(
            start,
            Arc::new(move || {
                let state = weak.upgrade()?;
                state
                    .try_read_recursive_for(LIVE_READ_WAIT)
                    .map(|guard| Arc::clone(guard.config_options()))
            }),
        )
    }

    fn same_source(&self, other: &Self) -> bool {
        matches!((&self.0, &other.0), (Some(mine), Some(theirs)) if Arc::ptr_eq(mine, theirs))
    }
}

thread_local! {
    static READER_PARSE_MEMO: RefCell<Option<Arc<ReaderParseMemo>>> = const { RefCell::new(None) };
}

#[derive(Debug, Default)]
pub struct ReaderParseMemo {
    decided: Mutex<Vec<(LiveOptions, Arc<ConfigOptions>)>>,
    tables: Mutex<HashMap<String, HeldTable>>,
}

impl ReaderParseMemo {
    #[must_use]
    pub fn settled(&self, live: &LiveOptions) -> Option<Arc<ConfigOptions>> {
        let mut decided = self.decided.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((_, options)) = decided.iter().find(|(source, _)| source.same_source(live)) {
            return Some(Arc::clone(options));
        }
        let options = live.read()?;
        decided.push((live.clone(), Arc::clone(&options)));
        Some(options)
    }

    #[must_use]
    pub fn held_table(&self, key: &str) -> Option<HeldTable> {
        let tables = self.tables.lock().unwrap_or_else(PoisonError::into_inner);
        tables.get(key).cloned()
    }

    pub fn hold_table(&self, key: &str, table: HeldTable) {
        let mut tables = self.tables.lock().unwrap_or_else(PoisonError::into_inner);
        tables.entry(key.to_string()).or_insert(table);
    }

    #[must_use]
    pub fn enter(self: &Arc<Self>) -> Option<Arc<Self>> {
        READER_PARSE_MEMO.with(|slot| slot.replace(Some(Arc::clone(self))))
    }

    pub fn restore(previous: Option<Arc<Self>>) {
        READER_PARSE_MEMO.with(|slot| *slot.borrow_mut() = previous);
    }

    pub fn run<T>(self: &Arc<Self>, body: impl FnOnce() -> T) -> T {
        let _restore = MemoRestore(self.enter());
        body()
    }

    pub fn within<T>(memo: Option<Arc<Self>>, body: impl FnOnce() -> T) -> T {
        let _restore = MemoRestore(READER_PARSE_MEMO.with(|slot| slot.replace(memo)));
        body()
    }

    #[must_use]
    pub fn is_settled(&self) -> bool {
        !self
            .decided
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }

    pub fn adopt(&self, other: &Self) {
        let theirs = other
            .decided
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut mine = self.decided.lock().unwrap_or_else(PoisonError::into_inner);
        for (source, options) in theirs {
            if !mine.iter().any(|(held, _)| held.same_source(&source)) {
                mine.push((source, options));
            }
        }
    }
}

struct MemoRestore(Option<Arc<ReaderParseMemo>>);

impl Drop for MemoRestore {
    fn drop(&mut self) {
        ReaderParseMemo::restore(self.0.take());
    }
}

#[must_use]
pub fn reader_parse_memo() -> Option<Arc<ReaderParseMemo>> {
    READER_PARSE_MEMO.with(|slot| slot.borrow().clone())
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
    parse: Arc<dyn OptimizerRule + Send + Sync>,
    live: Arc<OnceLock<LiveOptions>>,
}

impl FrameViewSeat {
    #[must_use]
    pub fn new(
        wrap: FrameViewWrap,
        rule: Arc<dyn AnalyzerRule + Send + Sync>,
        parse: Arc<dyn OptimizerRule + Send + Sync>,
    ) -> Self {
        Self {
            wrap,
            rule,
            parse,
            live: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn rule(&self) -> Arc<dyn AnalyzerRule + Send + Sync> {
        Arc::clone(&self.rule)
    }

    pub(crate) fn parse_rule(&self) -> Arc<dyn OptimizerRule + Send + Sync> {
        Arc::clone(&self.parse)
    }

    pub(crate) fn bind(&self, context: &SessionContext) {
        let _ = self.live.set(LiveOptions::of_session(context));
    }

    #[must_use]
    pub fn live(&self) -> Option<LiveOptions> {
        self.live.get().cloned()
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
            .field("parse", &self.parse.name())
            .finish_non_exhaustive()
    }
}

#[must_use]
pub fn with_frame_view_seat(config: SessionConfig, seat: FrameViewSeat) -> SessionConfig {
    config.with_extension(Arc::new(seat))
}

#[derive(Debug, Clone)]
pub struct ReaderParse {
    input: LogicalPlan,
    copy: LogicalPlan,
    live: LiveOptions,
    analyzer: Arc<Analyzer>,
}

impl ReaderParse {
    #[must_use]
    pub fn new(plan: LogicalPlan, live: LiveOptions, analyzer: Arc<Analyzer>) -> Self {
        Self {
            input: plan.clone(),
            copy: plan,
            live,
            analyzer,
        }
    }

    #[must_use]
    pub fn of(plan: &LogicalPlan) -> Option<&Self> {
        match plan {
            LogicalPlan::Extension(extension) => extension.node.as_any().downcast_ref(),
            _ => None,
        }
    }

    #[must_use]
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    #[must_use]
    pub fn copy(&self) -> &LogicalPlan {
        &self.copy
    }

    #[must_use]
    pub fn live(&self) -> &LiveOptions {
        &self.live
    }

    #[must_use]
    pub fn analyzer(&self) -> &Analyzer {
        &self.analyzer
    }

    #[must_use]
    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl PartialEq for ReaderParse {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
    }
}

impl Eq for ReaderParse {}

impl Hash for ReaderParse {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
    }
}

impl PartialOrd for ReaderParse {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

impl UserDefinedLogicalNodeCore for ReaderParse {
    fn name(&self) -> &str {
        READER_PARSE_NODE
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "{READER_PARSE_NODE}")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        let input = inputs
            .pop()
            .ok_or_else(|| internal_datafusion_err!("{READER_PARSE_NODE} takes one input"))?;
        Ok(Self {
            input,
            copy: self.copy.clone(),
            live: self.live.clone(),
            analyzer: Arc::clone(&self.analyzer),
        })
    }
}

#[must_use]
pub fn reader_parse(frame: &DataFrame) -> Option<DataFrame> {
    let (state, plan) = frame.clone().into_parts();
    let live = state.config().get_extension::<FrameViewSeat>()?.live()?;
    let analyzer = Arc::new(state.analyzer().clone());
    let node = ReaderParse::new(plan, live, analyzer);
    Some(DataFrame::new(state, node.into_plan()))
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
