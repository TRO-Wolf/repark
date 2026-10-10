use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::spec::{SnapshotRef, TableMetadata};
use iceberg::table::Table;
use iceberg::view::{View, ViewCommit};
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent, ViewCreation,
};

use super::body_scope::{BodyScope, note_unknown_outcome, refuse_sink_commit, watches};
use super::{
    ClaimedStamp, epoch_check, latch_refusal, main_lineage, over_a_stray, stamped_by, stray_since,
};
use crate::microbatch::error::MicroBatchError;
use crate::microbatch::offset::{Epoch, QUERY_ID_KEY, QueryId, SinkRecord, SnapshotId};
use crate::microbatch::starting_mark::StartingMark;

#[derive(Debug)]
pub(super) struct AppendFence {
    inner: Arc<dyn Catalog>,
    rule: Rule,
}

#[derive(Debug)]
enum Rule {
    Stamp(ClaimedStamp),
    StartingMark(QueryId),
    BodySink(BodyScope),
}

enum Breach<'metadata> {
    ConcurrentStamp(&'metadata SnapshotRef),
    Stray(&'metadata SnapshotRef),
    BaseLeftMain,
}

impl AppendFence {
    pub(super) fn install(inner: &Arc<dyn Catalog>, claimed: &ClaimedStamp) -> Arc<dyn Catalog> {
        Arc::new(AppendFence {
            inner: Arc::clone(inner),
            rule: Rule::Stamp(claimed.clone()),
        })
    }

    pub(super) fn for_starting_mark(inner: &Arc<dyn Catalog>, query: QueryId) -> Arc<dyn Catalog> {
        Arc::new(AppendFence {
            inner: Arc::clone(inner),
            rule: Rule::StartingMark(query),
        })
    }

    pub(super) fn for_body(inner: &Arc<dyn Catalog>, scope: BodyScope) -> Arc<dyn Catalog> {
        Arc::new(AppendFence {
            inner: Arc::clone(inner),
            rule: Rule::BodySink(scope),
        })
    }

    fn refuse(&self, refreshed: &Table) -> Option<Error> {
        match &self.rule {
            Rule::Stamp(claimed) => Self::refuse_stamp(claimed, refreshed),
            Rule::StartingMark(query) => already_started(refreshed, *query),
            Rule::BodySink(scope) => refuse_sink_commit(scope, refreshed),
        }
    }

    fn body_scope_on(&self, table: &Table) -> Option<&BodyScope> {
        match &self.rule {
            Rule::BodySink(scope) if watches(scope, table) => Some(scope),
            _ => None,
        }
    }

    fn refuse_stamp(claimed: &ClaimedStamp, refreshed: &Table) -> Option<Error> {
        let metadata = refreshed.metadata();
        let (message, refusal) = if let Some(breach) = breach(metadata, claimed) {
            let message = Self::message(claimed, metadata, &breach);
            let refusal = Self::typed(claimed, refreshed, &breach, &message);
            (message, refusal)
        } else {
            let refusal = epoch_check(refreshed, &claimed.stamp).err()?;
            (refusal.to_string(), refusal)
        };
        latch_refusal(refreshed, &claimed.stamp, &refusal);
        Some(
            Error::new(ErrorKind::DataInvalid, message)
                .with_retryable(false)
                .with_source(refusal),
        )
    }

    fn message(claimed: &ClaimedStamp, metadata: &TableMetadata, breach: &Breach<'_>) -> String {
        let record = &claimed.stamp.record;
        let base = display_snapshot(claimed.base);
        let prefix = format!(
            "append fence: query {query} epoch {epoch} pinned base snapshot {base}",
            query = record.query,
            epoch = record.epoch
        );
        match breach {
            Breach::ConcurrentStamp(newer) => format!(
                "{prefix}; newer snapshot {newer} on main carries {QUERY_ID_KEY}={query}",
                newer = newer.snapshot_id(),
                query = record.query
            ),
            Breach::Stray(stray) => format!("{prefix}; {}", over_a_stray(stray, &claimed.stamp)),
            Breach::BaseLeftMain => format!(
                "{prefix}, which is no longer an ancestor of main (head {head}); nothing can be proven about {QUERY_ID_KEY}={query} above it",
                head = display_snapshot(metadata.current_snapshot_id().map(SnapshotId::new)),
                query = record.query
            ),
        }
    }

    fn typed(
        claimed: &ClaimedStamp,
        refreshed: &Table,
        breach: &Breach<'_>,
        message: &str,
    ) -> MicroBatchError {
        let stamp = &claimed.stamp;
        if let Err(durable) = epoch_check(refreshed, stamp) {
            return durable;
        }
        let record = &stamp.record;
        let winner = match breach {
            Breach::ConcurrentStamp(newer) => {
                SinkRecord::from_summary(&newer.summary().additional_properties)
                    .ok()
                    .flatten()
                    .map(|concurrent| concurrent.run)
                    .filter(|run| *run != record.run)
            }
            Breach::Stray(_) | Breach::BaseLeftMain => None,
        };
        match winner {
            Some(winner) => MicroBatchError::Fenced {
                query: record.query,
                epoch: record.epoch,
                winner,
            },
            None => MicroBatchError::Catalog(message.to_string()),
        }
    }
}

fn already_started(refreshed: &Table, query: QueryId) -> Option<Error> {
    let metadata = refreshed.metadata();
    let marked = metadata
        .properties()
        .contains_key(&StartingMark::property_key(query));
    let stamped = main_lineage(metadata)
        .any(|snapshot| stamped_by(&snapshot.summary().additional_properties, query));
    (marked || stamped).then(|| {
        let refusal = MicroBatchError::AlreadyCommitted {
            query,
            epoch: Epoch::FIRST,
        };
        Error::new(ErrorKind::DataInvalid, refusal.to_string())
            .with_retryable(false)
            .with_source(refusal)
    })
}

fn display_snapshot(snapshot: Option<SnapshotId>) -> String {
    snapshot.map_or_else(|| String::from("none"), |id| id.to_string())
}

fn breach<'metadata>(
    metadata: &'metadata TableMetadata,
    claimed: &ClaimedStamp,
) -> Option<Breach<'metadata>> {
    let query = claimed.stamp.record.query;
    let base = claimed.base.map(SnapshotId::get);
    let mut reached = base.is_none();
    for snapshot in main_lineage(metadata) {
        if Some(snapshot.snapshot_id()) == base {
            reached = true;
            break;
        }
        if stamped_by(&snapshot.summary().additional_properties, query) {
            return Some(Breach::ConcurrentStamp(snapshot));
        }
    }
    if !reached {
        return Some(Breach::BaseLeftMain);
    }
    stray_since(metadata, base, &claimed.stamp).map(Breach::Stray)
}

pub(super) fn refusal_of(error: &Error) -> Option<MicroBatchError> {
    std::error::Error::source(error)?
        .downcast_ref::<MicroBatchError>()
        .cloned()
}

#[async_trait]
impl Catalog for AppendFence {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn update_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        removals: HashSet<String>,
        updates: HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .update_namespace_properties(namespace, removals, updates)
            .await
    }

    async fn set_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        updates: HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .set_namespace_properties(namespace, updates)
            .await
    }

    async fn remove_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        removals: HashSet<String>,
    ) -> Result<()> {
        self.inner
            .remove_namespace_properties(namespace, removals)
            .await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let (refusal, watched) = if let Some(refreshed) = commit.base_table() {
            (self.refuse(refreshed), self.body_scope_on(refreshed))
        } else {
            let loaded = self.inner.load_table(commit.identifier()).await?;
            (self.refuse(&loaded), self.body_scope_on(&loaded))
        };
        if let Some(refusal) = refusal {
            return Err(refusal);
        }
        let committed = self.inner.update_table(commit).await;
        if let (Some(scope), Err(error)) = (watched, &committed)
            && error.kind() == ErrorKind::CommitStateUnknown
        {
            note_unknown_outcome(scope);
        }
        committed
    }

    async fn publish_create_table(&self, table: Table) -> Result<Table> {
        self.inner.publish_create_table(table).await
    }

    async fn publish_replace_table(
        &self,
        table: Table,
        expected_base_metadata_location: Option<String>,
    ) -> Result<Table> {
        if let Rule::BodySink(scope) = &self.rule
            && let Some(refusal) = refuse_sink_commit(scope, &table)
        {
            return Err(refusal);
        }
        self.inner
            .publish_replace_table(table, expected_base_metadata_location)
            .await
    }

    async fn list_views(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_views(namespace).await
    }

    async fn create_view(
        &self,
        namespace: &NamespaceIdent,
        creation: ViewCreation,
    ) -> Result<View> {
        self.inner.create_view(namespace, creation).await
    }

    async fn load_view(&self, view: &TableIdent) -> Result<View> {
        self.inner.load_view(view).await
    }

    async fn drop_view(&self, view: &TableIdent) -> Result<()> {
        self.inner.drop_view(view).await
    }

    async fn view_exists(&self, view: &TableIdent) -> Result<bool> {
        self.inner.view_exists(view).await
    }

    async fn rename_view(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_view(src, dest).await
    }

    async fn update_view(&self, commit: ViewCommit) -> Result<View> {
        self.inner.update_view(commit).await
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &HashMap<String, String> {
        self.inner.properties()
    }

    async fn invalidate_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.invalidate_table(table).await
    }

    async fn invalidate_view(&self, view: &TableIdent) -> Result<()> {
        self.inner.invalidate_view(view).await
    }
}
