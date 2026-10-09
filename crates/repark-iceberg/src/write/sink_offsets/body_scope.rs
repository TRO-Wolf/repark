use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::view::{View, ViewCommit};
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent, ViewCreation,
};

use super::{BatchScopeGuard, ScopeToken, scopes};
use crate::microbatch::error::MicroBatchError;
use crate::microbatch::offset::TableUuid;

thread_local! {
    static BODY: RefCell<Option<BodyScope>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone)]
struct BodyScope {
    sink: TableUuid,
    token: ScopeToken,
}

#[derive(Debug)]
struct BodySinkGuard {
    inner: Arc<dyn Catalog>,
    scope: BodyScope,
}

struct ScopedBody<Body> {
    scope: BodyScope,
    body: Pin<Box<Body>>,
}

struct Restore(Option<BodyScope>);

impl Drop for Restore {
    fn drop(&mut self) {
        BODY.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

impl<Body: Future> Future for ScopedBody<Body> {
    type Output = Body::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let outer = BODY.with(|slot| slot.borrow_mut().replace(self.scope.clone()));
        let _restore = Restore(outer);
        self.body.as_mut().poll(context)
    }
}

impl BatchScopeGuard {
    pub fn scope_body<Body: Future>(&self, body: Body) -> impl Future<Output = Body::Output> {
        ScopedBody {
            scope: BodyScope {
                sink: self.sink,
                token: self.token.clone(),
            },
            body: Box::pin(body),
        }
    }
}

fn ambient() -> Option<BodyScope> {
    BODY.with(|slot| slot.borrow().clone())
}

impl BodySinkGuard {
    fn is_sink(&self, table: &Table) -> bool {
        TableUuid::of(table) == self.scope.sink
    }
}

#[must_use]
pub fn in_body_scope() -> bool {
    BODY.with(|slot| slot.borrow().is_some())
}

pub(super) fn ambient_token(table: &Table) -> Option<ScopeToken> {
    ambient()
        .filter(|scope| scope.sink == TableUuid::of(table))
        .map(|scope| scope.token)
}

#[must_use]
pub fn guard_body_catalog(catalog: &Arc<dyn Catalog>) -> Arc<dyn Catalog> {
    match ambient() {
        Some(scope) => Arc::new(BodySinkGuard {
            inner: Arc::clone(catalog),
            scope,
        }),
        None => Arc::clone(catalog),
    }
}

#[must_use]
pub fn refuse_planned_sink_write(table: &Table) -> Option<MicroBatchError> {
    let scope = ambient().filter(|scope| scope.sink == TableUuid::of(table))?;
    let mut entries = scopes();
    let entry = entries
        .get_mut(&scope.sink)
        .filter(|entry| entry.token == scope.token)?;
    let refusal = MicroBatchError::UnstampedSinkWrite {
        sink: table.identifier().to_string(),
        epoch: entry.stamp.record.epoch,
    };
    entry.violation = Some(refusal.clone());
    Some(refusal)
}

fn admit_sink_commit(scope: &BodyScope, sink: &TableIdent) -> Option<MicroBatchError> {
    let mut entries = scopes();
    let entry = entries
        .get_mut(&scope.sink)
        .filter(|entry| entry.token == scope.token)?;
    if entry.claimed && entry.committed.is_none() {
        return None;
    }
    let refusal = MicroBatchError::UnstampedSinkWrite {
        sink: sink.to_string(),
        epoch: entry.stamp.record.epoch,
    };
    entry.violation = Some(refusal.clone());
    Some(refusal)
}

fn refused(refusal: MicroBatchError) -> Error {
    Error::new(ErrorKind::DataInvalid, refusal.to_string())
        .with_retryable(false)
        .with_source(refusal)
}

fn note_unknown_outcome(scope: &BodyScope) {
    if let Some(entry) = scopes()
        .get_mut(&scope.sink)
        .filter(|entry| entry.token == scope.token)
    {
        entry.outcome_unknown = true;
    }
}

#[async_trait]
impl Catalog for BodySinkGuard {
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
        let on_sink = match commit.base_table() {
            Some(base) => self.is_sink(base),
            None => self.is_sink(&self.inner.load_table(commit.identifier()).await?),
        };
        if !on_sink {
            return self.inner.update_table(commit).await;
        }
        if let Some(refusal) = admit_sink_commit(&self.scope, commit.identifier()) {
            return Err(refused(refusal));
        }
        let committed = self.inner.update_table(commit).await;
        if let Err(error) = &committed
            && error.kind() == ErrorKind::CommitStateUnknown
        {
            note_unknown_outcome(&self.scope);
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
        if self.is_sink(&table)
            && let Some(refusal) = admit_sink_commit(&self.scope, table.identifier())
        {
            return Err(refused(refusal));
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
