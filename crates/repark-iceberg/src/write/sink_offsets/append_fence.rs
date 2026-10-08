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

use super::{ClaimedStamp, epoch_check, latch_refusal, main_lineage, stamped_by};
use crate::microbatch::error::MicroBatchError;
use crate::microbatch::offset::{QUERY_ID_KEY, QueryId, SinkRecord, SnapshotId};

#[derive(Debug)]
pub(super) struct AppendFence {
    inner: Arc<dyn Catalog>,
    claimed: ClaimedStamp,
}

enum Breach<'metadata> {
    ConcurrentStamp(&'metadata SnapshotRef),
    BaseLeftMain,
}

impl AppendFence {
    pub(super) fn install(inner: &Arc<dyn Catalog>, claimed: &ClaimedStamp) -> Arc<dyn Catalog> {
        Arc::new(AppendFence {
            inner: Arc::clone(inner),
            claimed: claimed.clone(),
        })
    }

    fn refuse(&self, refreshed: &Table) -> Option<Error> {
        let metadata = refreshed.metadata();
        let record = &self.claimed.stamp.record;
        let breach = breach(metadata, self.claimed.base, record.query)?;
        let message = self.message(metadata, &breach);
        let refusal = self.typed(refreshed, &breach, &message);
        latch_refusal(refreshed, &self.claimed.stamp, &refusal);
        Some(
            Error::new(ErrorKind::DataInvalid, message)
                .with_retryable(false)
                .with_source(refusal),
        )
    }

    fn message(&self, metadata: &TableMetadata, breach: &Breach<'_>) -> String {
        let record = &self.claimed.stamp.record;
        let base = display_snapshot(self.claimed.base);
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
            Breach::BaseLeftMain => format!(
                "{prefix}, which is no longer an ancestor of main (head {head}); nothing can be proven about {QUERY_ID_KEY}={query} above it",
                head = display_snapshot(metadata.current_snapshot_id().map(SnapshotId::new)),
                query = record.query
            ),
        }
    }

    fn typed(&self, refreshed: &Table, breach: &Breach<'_>, message: &str) -> MicroBatchError {
        let stamp = &self.claimed.stamp;
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
            Breach::BaseLeftMain => None,
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

fn display_snapshot(snapshot: Option<SnapshotId>) -> String {
    snapshot.map_or_else(|| String::from("none"), |id| id.to_string())
}

fn breach(
    metadata: &TableMetadata,
    base: Option<SnapshotId>,
    query: QueryId,
) -> Option<Breach<'_>> {
    let base = base.map(SnapshotId::get);
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
    (!reached).then_some(Breach::BaseLeftMain)
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
        let refusal = match commit.base_table() {
            Some(refreshed) => self.refuse(refreshed),
            None => self.refuse(&self.inner.load_table(commit.identifier()).await?),
        };
        match refusal {
            Some(refusal) => Err(refusal),
            None => self.inner.update_table(commit).await,
        }
    }

    async fn publish_create_table(&self, table: Table) -> Result<Table> {
        self.inner.publish_create_table(table).await
    }

    async fn publish_replace_table(
        &self,
        table: Table,
        expected_base_metadata_location: Option<String>,
    ) -> Result<Table> {
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
