use std::any::Any;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::time::Duration;

use datafusion::prelude::DataFrame;
use futures::future::BoxFuture;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
use repark_iceberg::microbatch::offset::{Epoch, QueryId, RunId, SinkRecord, TableUuid};
use repark_iceberg::write::sink_offsets::read_resume_point;
use tokio::sync::watch;
use tokio::task::AbortHandle;

use crate::Session;
use crate::catalog_state::{CatalogRegistry, LocationPolicy};
use crate::idents::parse_table_identifier_segments;
use crate::microbatch::progress::{
    DEFAULT_RECENT_PROGRESS, Identity, ProgressLog, QueryStatus, StreamingQueryProgress,
    TriggerReport,
};
use crate::microbatch::relation::PlanTemplate;
use crate::microbatch::run::{Door, Run};
use crate::time_travel::microbatch_source::{MicroBatchSource, SourceOptions, WeakSessionState};

pub const DEFAULT_POLLING_DELAY: Duration = Duration::from_millis(10);

pub const DEFAULT_CATALOG_TIMEOUT: Duration = Duration::from_mins(1);

pub(crate) const LOAD_SINK: &str = "load the sink";

const WAIT_CHAIN_LIMIT: usize = 1024;

tokio::task_local! {
    static DRIVING: Weak<QueryShared>;
}

fn driving() -> Option<Arc<QueryShared>> {
    DRIVING.try_with(Weak::upgrade).ok().flatten()
}

struct WaitEdge(Arc<QueryShared>);

impl WaitEdge {
    fn enter(waiter: &Arc<QueryShared>, target: &Arc<QueryShared>) -> Option<WaitEdge> {
        *waiter.waiting_on() = Some(Arc::downgrade(target));
        let edge = WaitEdge(Arc::clone(waiter));
        let mut cursor = Arc::clone(target);
        for _ in 0..WAIT_CHAIN_LIMIT {
            let next = cursor.waiting_on().as_ref().and_then(Weak::upgrade);
            match next {
                Some(next) if Arc::ptr_eq(&next, waiter) => return None,
                Some(next) => cursor = next,
                None => return Some(edge),
            }
        }
        None
    }
}

impl Drop for WaitEdge {
    fn drop(&mut self) {
        *self.0.waiting_on() = None;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    ProcessingTime(Duration),
    AvailableNow,
    Once,
}

impl Default for Trigger {
    fn default() -> Self {
        Trigger::ProcessingTime(Duration::ZERO)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryState {
    #[default]
    Registered,
    Running,
    Draining,
    Stopped,
    Failed,
    RecoveryRequired,
}

impl QueryState {
    #[must_use]
    pub fn is_active(self) -> bool {
        matches!(self, QueryState::Running | QueryState::Draining)
    }

    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            QueryState::Stopped | QueryState::Failed | QueryState::RecoveryRequired
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownOutcome {
    Drained {
        durable: Option<SinkRecord>,
    },
    Stopped {
        durable: Option<SinkRecord>,
    },
    Failed {
        durable: Option<SinkRecord>,
        error: Arc<MicroBatchError>,
    },
    RecoveryRequired {
        durable: Option<SinkRecord>,
        reason: RecoveryReason,
    },
}

impl ShutdownOutcome {
    #[must_use]
    pub fn durable(&self) -> Option<&SinkRecord> {
        match self {
            ShutdownOutcome::Drained { durable }
            | ShutdownOutcome::Stopped { durable }
            | ShutdownOutcome::Failed { durable, .. }
            | ShutdownOutcome::RecoveryRequired { durable, .. } => durable.as_ref(),
        }
    }

    #[must_use]
    pub fn state(&self) -> QueryState {
        match self {
            ShutdownOutcome::Drained { .. } | ShutdownOutcome::Stopped { .. } => {
                QueryState::Stopped
            }
            ShutdownOutcome::Failed { .. } => QueryState::Failed,
            ShutdownOutcome::RecoveryRequired { .. } => QueryState::RecoveryRequired,
        }
    }
}

pub trait BatchBody: Send + Sync {
    fn run(&self, frame: DataFrame, epoch: Epoch) -> BoxFuture<'_, Result<(), MicroBatchError>>;
}

#[derive(Clone)]
pub enum SinkSpec {
    Table {
        sink: String,
    },
    ForeachBatch {
        sink: String,
        body: Arc<dyn BatchBody>,
    },
}

impl SinkSpec {
    #[must_use]
    pub fn sink(&self) -> &str {
        match self {
            SinkSpec::Table { sink } | SinkSpec::ForeachBatch { sink, .. } => sink,
        }
    }
}

impl fmt::Debug for SinkSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SinkSpec::Table { sink } => {
                formatter.debug_struct("Table").field("sink", sink).finish()
            }
            SinkSpec::ForeachBatch { sink, .. } => formatter
                .debug_struct("ForeachBatch")
                .field("sink", sink)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RecordedLocation(String);

impl RecordedLocation {
    #[must_use]
    pub fn new(location: impl Into<String>) -> Self {
        RecordedLocation(location.into())
    }
}

impl fmt::Debug for RecordedLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecordedLocation(<redacted>)")
    }
}

#[derive(Debug, Clone)]
pub struct StreamSpec {
    pub source: String,
    pub source_options: SourceOptions,
    pub plan: Option<PlanTemplate>,
    pub sink: SinkSpec,
    pub trigger: Trigger,
    pub query_name: Option<String>,
    pub checkpoint_location: Option<RecordedLocation>,
    pub stop_timeout: Option<Duration>,
    pub polling_delay: Duration,
    pub catalog_timeout: Duration,
    pub recent_progress_limit: usize,
}

impl StreamSpec {
    #[must_use]
    pub fn new(source: impl Into<String>, source_options: SourceOptions, sink: SinkSpec) -> Self {
        StreamSpec {
            source: source.into(),
            source_options,
            plan: None,
            sink,
            trigger: Trigger::default(),
            query_name: None,
            checkpoint_location: None,
            stop_timeout: None,
            polling_delay: DEFAULT_POLLING_DELAY,
            catalog_timeout: DEFAULT_CATALOG_TIMEOUT,
            recent_progress_limit: DEFAULT_RECENT_PROGRESS,
        }
    }
}

#[derive(Clone)]
pub(crate) struct TableTarget {
    pub(crate) name: String,
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) ident: TableIdent,
    pub(crate) local_catalog: Option<String>,
}

impl TableTarget {
    pub(crate) fn resolve(session: &Session, table: &str) -> Result<TableTarget, MicroBatchError> {
        let parts = parse_table_identifier_segments(table).map_err(|message| {
            MicroBatchError::Catalog(format!(
                "streaming sink table {table:?} is not a valid identifier: {message}"
            ))
        })?;
        let [catalog_name, namespace, table_name] = parts.as_slice() else {
            return Err(MicroBatchError::Catalog(format!(
                "streaming sink table {table:?} must be catalog.namespace.table"
            )));
        };
        let catalogs = session.catalogs_snapshot();
        let catalog = catalogs.get(catalog_name).cloned().ok_or_else(|| {
            MicroBatchError::Catalog(format!(
                "streaming sink table {table:?}: catalog {catalog_name:?} is not registered"
            ))
        })?;
        let local_catalog = matches!(
            catalogs.location_policy(catalog_name),
            Some(LocationPolicy::TempFallbackAllowed { .. })
        )
        .then(|| catalog_name.clone());
        Ok(TableTarget {
            name: table.to_string(),
            catalog,
            ident: TableIdent::new(NamespaceIdent::new(namespace.clone()), table_name.clone()),
            local_catalog,
        })
    }

    pub(crate) async fn load(&self) -> Result<Table, MicroBatchError> {
        self.catalog.load_table(&self.ident).await.map_err(|error| {
            MicroBatchError::Catalog(repark_common::redaction::mask_value_credentials(&format!(
                "streaming sink cannot load table {name:?}: {error}",
                name = self.name
            )))
        })
    }
}

#[derive(Default)]
pub struct StreamingQueryManager {
    queries: Mutex<Vec<QueryHandle>>,
}

impl Drop for StreamingQueryManager {
    fn drop(&mut self) {
        let queries = self
            .queries
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        for query in queries.iter() {
            query.shared.stop.send_replace(true);
        }
    }
}

pub(crate) async fn bounded<T>(
    limit: Duration,
    call: &'static str,
    work: impl Future<Output = Result<T, MicroBatchError>>,
) -> Result<T, MicroBatchError> {
    match tokio::time::timeout(limit, work).await {
        Ok(done) => done,
        Err(_) => Err(MicroBatchError::CatalogTimeout {
            call,
            waited: limit,
        }),
    }
}

impl fmt::Debug for StreamingQueryManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamingQueryManager")
            .field("queries", &self.registry().len())
            .finish()
    }
}

impl StreamingQueryManager {
    #[must_use]
    pub fn of(session: &Session) -> Arc<StreamingQueryManager> {
        let state = session.context().state_ref();
        let mut state = state.write();
        if let Some(manager) = state.config().get_extension::<StreamingQueryManager>() {
            return manager;
        }
        let manager = Arc::new(StreamingQueryManager::default());
        state.config_mut().set_extension(Arc::clone(&manager));
        manager
    }

    fn registry(&self) -> MutexGuard<'_, Vec<QueryHandle>> {
        self.queries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn register(
        self: &Arc<Self>,
        session: &Session,
        spec: StreamSpec,
    ) -> Result<QueryHandle, MicroBatchError> {
        if let Some(plan) = &spec.plan
            && plan.source() != spec.source
        {
            return Err(MicroBatchError::Catalog(format!(
                "the streaming frame reads {frame} but the query names source {source}",
                frame = plan.source(),
                source = spec.source
            )));
        }
        let sink = TableTarget::resolve(session, spec.sink.sink())?;
        let limit = spec.catalog_timeout;
        let table = bounded(limit, LOAD_SINK, sink.load()).await?;
        let sink_uuid = TableUuid::of(&table);
        let id = QueryId::derive(sink_uuid, spec.query_name.as_deref());
        let opening = MicroBatchSource::open(session, &spec.source, spec.source_options);
        let source = bounded(limit, "open the source", opening)
            .await?
            .with_catalog_timeout(limit);
        let door = match spec.sink {
            SinkSpec::Table { .. } => Door::Table,
            SinkSpec::ForeachBatch { body, .. } => Door::ForeachBatch(body),
        };
        let source_name = source.table_identifier();
        let (stop, _) = watch::channel(false);
        let (done, _) = watch::channel(false);
        Ok(QueryHandle {
            shared: Arc::new(QueryShared {
                id,
                run_id: RunId::fresh(),
                name: spec.query_name,
                sink,
                sink_uuid,
                trigger: spec.trigger,
                stop_timeout: spec.stop_timeout.filter(|limit| !limit.is_zero()),
                polling_delay: spec.polling_delay,
                catalog_timeout: spec.catalog_timeout,
                checkpoint_location: spec.checkpoint_location,
                manager: Arc::downgrade(self),
                session: Arc::downgrade(&session.catalogs),
                source_name,
                lifecycle: Mutex::new(Lifecycle {
                    pending: Some(Pending {
                        source,
                        plan: spec.plan,
                        context: WeakSessionState::of(session.context()),
                        door,
                    }),
                    ..Lifecycle::default()
                }),
                progress: Mutex::new(ProgressLog::new(spec.recent_progress_limit)),
                waiting_on: Mutex::new(None),
                stop,
                done,
            }),
        })
    }

    #[must_use]
    pub fn active(&self) -> Vec<QueryHandle> {
        let mut registry = self.registry();
        registry.retain(|query| !query.state().is_terminal());
        registry
            .iter()
            .filter(|query| query.state().is_active())
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn get(&self, id: QueryId) -> Option<QueryHandle> {
        self.active().into_iter().find(|query| query.id() == id)
    }

    pub async fn stop_all(&self) -> Vec<ShutdownOutcome> {
        let mut outcomes = Vec::new();
        for query in self.active() {
            outcomes.push(query.stop().await);
        }
        outcomes
    }

    fn admit(&self, query: &QueryHandle) -> Result<(), MicroBatchError> {
        let mut registry = self.registry();
        registry.retain(|entry| !entry.state().is_terminal());
        if registry.iter().any(|entry| entry.id() == query.id()) {
            return Err(MicroBatchError::Catalog(format!(
                "Cannot start query with id {id} as another query with same id is already active",
                id = query.id()
            )));
        }
        if registry
            .iter()
            .any(|entry| entry.shared.sink_uuid == query.shared.sink_uuid)
        {
            return Err(MicroBatchError::SinkBusy {
                sink: query.shared.sink.name.clone(),
            });
        }
        registry.push(query.clone());
        Ok(())
    }
}

pub(crate) struct Pending {
    pub(crate) source: MicroBatchSource,
    pub(crate) plan: Option<PlanTemplate>,
    pub(crate) context: WeakSessionState,
    pub(crate) door: Door,
}

#[derive(Default)]
struct Lifecycle {
    state: QueryState,
    durable: Option<SinkRecord>,
    in_flight: Option<Epoch>,
    outcome: Option<ShutdownOutcome>,
    exception: Option<Arc<MicroBatchError>>,
    abort: Option<AbortHandle>,
    pending: Option<Pending>,
}

pub(crate) struct QueryShared {
    pub(crate) id: QueryId,
    pub(crate) run_id: RunId,
    pub(crate) name: Option<String>,
    pub(crate) sink: TableTarget,
    sink_uuid: TableUuid,
    pub(crate) trigger: Trigger,
    stop_timeout: Option<Duration>,
    pub(crate) polling_delay: Duration,
    pub(crate) catalog_timeout: Duration,
    checkpoint_location: Option<RecordedLocation>,
    manager: Weak<StreamingQueryManager>,
    session: Weak<RwLock<CatalogRegistry>>,
    source_name: String,
    lifecycle: Mutex<Lifecycle>,
    progress: Mutex<ProgressLog>,
    waiting_on: Mutex<Option<Weak<QueryShared>>>,
    pub(crate) stop: watch::Sender<bool>,
    done: watch::Sender<bool>,
}

impl QueryShared {
    fn lifecycle(&self) -> MutexGuard<'_, Lifecycle> {
        self.lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn progress(&self) -> MutexGuard<'_, ProgressLog> {
        self.progress.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn waiting_on(&self) -> MutexGuard<'_, Option<Weak<QueryShared>>> {
        self.waiting_on
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn stopping(&self) -> ShutdownOutcome {
        ShutdownOutcome::Stopped {
            durable: self.durable(),
        }
    }

    pub(crate) fn report(&self, report: &TriggerReport) {
        let identity = Identity {
            id: self.id,
            run_id: self.run_id,
            name: self.name.as_deref(),
            source: &self.source_name,
            sink: &self.sink.name,
        };
        self.progress().record(&identity, report);
    }

    pub(crate) fn resumed(&self, durable: Option<SinkRecord>) {
        self.lifecycle().durable = durable;
    }

    pub(crate) fn begin_batch(&self, epoch: Epoch) {
        self.lifecycle().in_flight = Some(epoch);
    }

    pub(crate) fn end_batch(&self, committed: Option<SinkRecord>) {
        let mut lifecycle = self.lifecycle();
        lifecycle.in_flight = None;
        if let Some(record) = committed {
            lifecycle.durable = Some(record);
        }
    }

    pub(crate) fn durable(&self) -> Option<SinkRecord> {
        self.lifecycle().durable.clone()
    }

    pub(crate) fn finish(&self, ending: Result<Ending, MicroBatchError>) -> ShutdownOutcome {
        self.conclude(self.lifecycle(), ending)
    }

    pub(crate) fn panicked(&self, panic: &(dyn Any + Send)) -> MicroBatchError {
        let message = panic
            .downcast_ref::<&str>()
            .map(|text| (*text).to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| String::from("no message"));
        let lifecycle = self.lifecycle();
        let epoch = lifecycle
            .in_flight
            .or_else(|| lifecycle.durable.as_ref().map(|record| record.epoch.next()))
            .unwrap_or(Epoch::FIRST);
        MicroBatchError::DriverPanicked {
            epoch,
            message: repark_common::redaction::mask_value_credentials(&message),
        }
    }

    fn conclude(
        &self,
        mut lifecycle: MutexGuard<'_, Lifecycle>,
        ending: Result<Ending, MicroBatchError>,
    ) -> ShutdownOutcome {
        if let Some(outcome) = &lifecycle.outcome {
            return outcome.clone();
        }
        let durable = lifecycle.durable.clone();
        let (outcome, exception) = match ending {
            Ok(Ending::Drained) => (ShutdownOutcome::Drained { durable }, None),
            Ok(Ending::Stopped) => (ShutdownOutcome::Stopped { durable }, None),
            Err(MicroBatchError::RecoveryRequired {
                query,
                epoch,
                durable: reported,
                reason,
            }) => {
                let durable = reported.as_deref().cloned().or(durable);
                let error = MicroBatchError::RecoveryRequired {
                    query,
                    epoch,
                    durable: durable.clone().map(Box::new),
                    reason: reason.clone(),
                };
                (
                    ShutdownOutcome::RecoveryRequired { durable, reason },
                    Some(Arc::new(error)),
                )
            }
            Err(error) => {
                let error = Arc::new(error);
                (
                    ShutdownOutcome::Failed {
                        durable,
                        error: Arc::clone(&error),
                    },
                    Some(error),
                )
            }
        };
        lifecycle.state = outcome.state();
        lifecycle.in_flight = None;
        lifecycle.abort = None;
        lifecycle.pending = None;
        lifecycle.exception = exception;
        lifecycle.outcome = Some(outcome.clone());
        drop(lifecycle);
        self.progress().stopped();
        self.done.send_replace(true);
        outcome
    }

    pub(crate) fn stop_requested(&self) -> bool {
        *self.stop.borrow() || self.session_ended()
    }

    pub(crate) fn session_ended(&self) -> bool {
        self.session.strong_count() == 0
    }
}

pub(crate) enum Ending {
    Drained,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatalogCheck {
    Enforce,
    #[cfg(test)]
    Skip,
}

#[derive(Clone)]
pub struct QueryHandle {
    shared: Arc<QueryShared>,
}

impl fmt::Debug for QueryHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryHandle")
            .field("id", &self.shared.id)
            .field("run_id", &self.shared.run_id)
            .field("name", &self.shared.name)
            .field("sink", &self.shared.sink.name)
            .field("checkpoint_location", &self.shared.checkpoint_location)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl QueryHandle {
    #[must_use]
    pub fn id(&self) -> QueryId {
        self.shared.id
    }

    #[must_use]
    pub fn run_id(&self) -> RunId {
        self.shared.run_id
    }

    #[must_use]
    pub fn name(&self) -> Option<String> {
        self.shared.name.clone()
    }

    #[must_use]
    pub fn sink(&self) -> &str {
        &self.shared.sink.name
    }

    #[must_use]
    pub fn trigger(&self) -> Trigger {
        self.shared.trigger
    }

    #[must_use]
    pub fn state(&self) -> QueryState {
        self.shared.lifecycle().state
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state().is_active()
    }

    #[must_use]
    pub fn durable(&self) -> Option<SinkRecord> {
        self.shared.durable()
    }

    #[must_use]
    pub fn status(&self) -> QueryStatus {
        self.shared.progress().status()
    }

    #[must_use]
    pub fn last_progress(&self) -> Option<StreamingQueryProgress> {
        self.shared.progress().last()
    }

    #[must_use]
    pub fn recent_progress(&self) -> Vec<StreamingQueryProgress> {
        self.shared.progress().recent()
    }

    #[must_use]
    pub fn exception(&self) -> Option<Arc<MicroBatchError>> {
        self.shared.lifecycle().exception.clone()
    }

    #[allow(
        clippy::missing_errors_doc,
        clippy::unused_async,
        reason = "async as the sketch's signature: start is awaited inside the runtime that owns the driver task"
    )]
    pub async fn start(&self) -> Result<(), MicroBatchError> {
        self.launch(CatalogCheck::Enforce)
    }

    #[cfg(test)]
    pub(crate) fn start_below_catalog_check(&self) -> Result<(), MicroBatchError> {
        self.launch(CatalogCheck::Skip)
    }

    fn launch(&self, check: CatalogCheck) -> Result<(), MicroBatchError> {
        if check == CatalogCheck::Enforce
            && let Some(catalog) = &self.shared.sink.local_catalog
        {
            return Err(MicroBatchError::LocalCatalogRefused {
                catalog: catalog.clone(),
            });
        }
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(MicroBatchError::Catalog(format!(
                "query {id} must start inside a Tokio runtime",
                id = self.shared.id
            )));
        }
        let manager = self.shared.manager.upgrade().ok_or_else(|| {
            MicroBatchError::Catalog(format!(
                "query {id} outlived its session's streaming query manager",
                id = self.shared.id
            ))
        })?;
        let mut lifecycle = self.shared.lifecycle();
        let Some(work) = lifecycle.pending.take() else {
            return Err(MicroBatchError::Catalog(format!(
                "query {id} was already started; register a new query to run it again",
                id = self.shared.id
            )));
        };
        if let Err(error) = manager.admit(self) {
            lifecycle.pending = Some(work);
            return Err(error);
        }
        lifecycle.state = QueryState::Running;
        let run = Run::new(Arc::clone(&self.shared), work);
        #[expect(
            clippy::disallowed_methods,
            reason = "the one tracked driver task per query (sketch §3.5): its AbortHandle lives \
                      in the QueryHandle, stop() waits for it and aborts it on stopTimeout, and \
                      it ends on drain, stop or failure"
        )]
        let task = tokio::spawn(DRIVING.scope(Arc::downgrade(&self.shared), run.drive()));
        lifecycle.abort = Some(task.abort_handle());
        Ok(())
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn await_termination(
        &self,
        timeout: Option<Duration>,
    ) -> Result<bool, Arc<MicroBatchError>> {
        if driving().is_some_and(|driver| Arc::ptr_eq(&driver, &self.shared)) {
            return Err(Arc::new(MicroBatchError::AwaitFromDriver {
                query: self.shared.id,
            }));
        }
        if !self.wait_done(timeout).await {
            return Ok(false);
        }
        match self.exception() {
            Some(error) => Err(error),
            None => Ok(true),
        }
    }

    async fn wait_done(&self, timeout: Option<Duration>) -> bool {
        let mut done = self.shared.done.subscribe();
        let finished = async move { done.wait_for(|finished| *finished).await.is_ok() };
        match timeout {
            None => finished.await,
            Some(limit) => tokio::time::timeout(limit, finished).await.unwrap_or(false),
        }
    }

    pub async fn stop(&self) -> ShutdownOutcome {
        {
            let mut lifecycle = self.shared.lifecycle();
            if let Some(outcome) = &lifecycle.outcome {
                return outcome.clone();
            }
            if lifecycle.state == QueryState::Registered {
                return self.shared.conclude(lifecycle, Ok(Ending::Stopped));
            }
            if lifecycle.state == QueryState::Running {
                lifecycle.state = QueryState::Draining;
            }
        }
        self.shared.stop.send_replace(true);
        let driver = driving();
        let _edge = match &driver {
            Some(driver) => match WaitEdge::enter(driver, &self.shared) {
                Some(edge) => Some(edge),
                None => return self.shared.stopping(),
            },
            None => None,
        };
        let limit = self.shared.stop_timeout;
        if self.wait_done(limit).await {
            let finished = self.shared.lifecycle().outcome.clone();
            if let Some(outcome) = finished {
                return outcome;
            }
        }
        let (abort, epoch) = {
            let lifecycle = self.shared.lifecycle();
            (lifecycle.abort.clone(), lifecycle.in_flight)
        };
        if let Some(abort) = abort {
            abort.abort();
        }
        let durable = self.read_durable().await;
        self.shared.resumed(durable.clone());
        let epoch = epoch
            .or_else(|| durable.as_ref().map(|record| record.epoch.next()))
            .unwrap_or(Epoch::FIRST);
        self.shared.finish(Err(MicroBatchError::RecoveryRequired {
            query: self.shared.id,
            epoch,
            durable: durable.map(Box::new),
            reason: RecoveryReason::StopTimeout {
                waited: limit.unwrap_or_default(),
            },
        }))
    }

    async fn read_durable(&self) -> Option<SinkRecord> {
        let known = self.durable();
        let loading = self.shared.sink.load();
        let Ok(table) = bounded(self.shared.catalog_timeout, LOAD_SINK, loading).await else {
            return known;
        };
        match read_resume_point(&table, self.shared.id) {
            Ok(record) => record,
            Err(MicroBatchError::RecoveryRequired { durable, .. }) => durable.map(|record| *record),
            Err(_) => known,
        }
    }
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;
