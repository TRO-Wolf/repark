use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use datafusion::prelude::DataFrame;
use futures::future::BoxFuture;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
use repark_iceberg::microbatch::offset::{Epoch, QueryId, RunId, SinkRecord, TableUuid};

use crate::Session;
use crate::idents::parse_table_identifier_segments;
use crate::time_travel::microbatch_source::SourceOptions;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryState {
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
    pub sink: SinkSpec,
    pub trigger: Trigger,
    pub query_name: Option<String>,
    pub checkpoint_location: Option<RecordedLocation>,
    pub stop_timeout: Option<Duration>,
}

impl StreamSpec {
    #[must_use]
    pub fn new(source: impl Into<String>, source_options: SourceOptions, sink: SinkSpec) -> Self {
        StreamSpec {
            source: source.into(),
            source_options,
            sink,
            trigger: Trigger::default(),
            query_name: None,
            checkpoint_location: None,
            stop_timeout: None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct TableTarget {
    pub(crate) name: String,
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) ident: TableIdent,
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
        Ok(TableTarget {
            name: table.to_string(),
            catalog,
            ident: TableIdent::new(NamespaceIdent::new(namespace.clone()), table_name.clone()),
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
        &self,
        session: &Session,
        spec: StreamSpec,
    ) -> Result<QueryHandle, MicroBatchError> {
        let sink = TableTarget::resolve(session, spec.sink.sink())?;
        let table = sink.load().await?;
        let id = QueryId::derive(TableUuid::of(&table), spec.query_name.as_deref());
        Ok(QueryHandle {
            shared: Arc::new(QueryShared {
                id,
                run_id: RunId::fresh(),
                name: spec.query_name.clone(),
                sink,
                lifecycle: Mutex::new(Lifecycle {
                    state: QueryState::Registered,
                }),
                spec,
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
}

struct Lifecycle {
    state: QueryState,
}

struct QueryShared {
    id: QueryId,
    run_id: RunId,
    name: Option<String>,
    sink: TableTarget,
    lifecycle: Mutex<Lifecycle>,
    spec: StreamSpec,
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
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl QueryHandle {
    fn lifecycle(&self) -> MutexGuard<'_, Lifecycle> {
        self.shared
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

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
        self.shared.spec.trigger
    }

    #[must_use]
    pub fn state(&self) -> QueryState {
        self.lifecycle().state
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state().is_active()
    }
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;
