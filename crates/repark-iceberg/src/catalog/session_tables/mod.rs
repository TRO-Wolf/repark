use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::internal_datafusion_err;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use iceberg::table::Table;
use iceberg::view::{View, ViewCommit};
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent, ViewCreation,
};
use iceberg_datafusion::IcebergTableProvider;

type TableKey = (String, TableIdent);
type Slot = Arc<Mutex<Option<Resolved>>>;
type BoxedResolve<'a> =
    Pin<Box<dyn Future<Output = DataFusionResult<Option<Arc<dyn TableProvider>>>> + Send + 'a>>;
type BoxedLoad<'a> = Pin<Box<dyn Future<Output = Result<Table>> + Send + 'a>>;

const SWEEP_FLOOR: usize = 64;
const CONFORM_NODE: &str = "HeldIcebergConformExec";

thread_local! {
    static RESOLVING: RefCell<Option<Slot>> = const { RefCell::new(None) };
    static REFRESHING: Cell<bool> = const { Cell::new(false) };
}

static BINDINGS: LazyLock<Mutex<Bindings>> = LazyLock::new(|| {
    Mutex::new(Bindings {
        by_provider: HashMap::new(),
        sweep_at: SWEEP_FLOOR,
    })
});

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[must_use]
pub fn same_state(seen: &Table, table: &Table) -> bool {
    seen.metadata_location().is_some()
        && seen.metadata_location() == table.metadata_location()
        && seen.metadata().uuid() == table.metadata().uuid()
        && seen.metadata().last_updated_ms() == table.metadata().last_updated_ms()
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum TableState {
    Live(Table),
    Dropped,
}

impl TableState {
    fn same_as(&self, table: &Table) -> bool {
        match self {
            Self::Live(seen) => same_state(seen, table),
            Self::Dropped => false,
        }
    }
}

#[derive(Debug, Clone)]
struct Entry {
    epoch: u64,
    state: TableState,
}

#[derive(Debug, Default)]
pub struct SessionTables {
    seen: Mutex<HashMap<TableKey, Entry>>,
    clock: AtomicU64,
}

impl SessionTables {
    #[must_use]
    pub fn epoch(&self, catalog: &str, ident: &TableIdent) -> u64 {
        locked(&self.seen)
            .get(&(catalog.to_string(), ident.clone()))
            .map_or(0, |entry| entry.epoch)
    }

    #[must_use]
    pub fn current(&self, catalog: &str, ident: &TableIdent) -> Option<TableState> {
        locked(&self.seen)
            .get(&(catalog.to_string(), ident.clone()))
            .map(|entry| entry.state.clone())
    }

    #[must_use]
    pub fn tracked(&self) -> usize {
        locked(&self.seen).len()
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn observe(
        &self,
        catalog: &str,
        ident: &TableIdent,
        table: &Table,
        before: u64,
        refresh: bool,
    ) {
        let key = (catalog.to_string(), ident.clone());
        let mut seen = locked(&self.seen);
        if let Some(entry) = seen.get(&key)
            && (entry.epoch != before || (!refresh && entry.state.same_as(table)))
        {
            return;
        }
        let epoch = self.tick();
        seen.insert(
            key,
            Entry {
                epoch,
                state: TableState::Live(table.clone()),
            },
        );
    }

    fn record(&self, catalog: &str, ident: &TableIdent, state: TableState) {
        let epoch = self.tick();
        locked(&self.seen).insert((catalog.to_string(), ident.clone()), Entry { epoch, state });
    }

    fn forget(&self, catalog: &str, ident: &TableIdent) {
        locked(&self.seen).remove(&(catalog.to_string(), ident.clone()));
    }
}

#[must_use]
pub fn table_epoch(config: &SessionConfig, catalog: &str, ident: &TableIdent) -> Option<u64> {
    config
        .get_extension::<SessionTables>()
        .map(|tables| tables.epoch(catalog, ident))
}

#[must_use]
pub fn with_session_tables(config: SessionConfig) -> SessionConfig {
    config.with_extension(Arc::new(SessionTables::default()))
}

#[allow(clippy::missing_errors_doc)]
pub async fn session_catalog_provider(
    context: &SessionContext,
    name: &str,
    catalog: Arc<dyn Catalog>,
) -> DataFusionResult<(Arc<dyn Catalog>, Arc<dyn CatalogProvider>)> {
    let tables = context
        .state_ref()
        .read()
        .config()
        .get_extension::<SessionTables>();
    let catalog = match tables {
        Some(tables) => Arc::new(SessionTablesCatalog {
            inner: catalog,
            name: name.to_string(),
            tables,
        }) as Arc<dyn Catalog>,
        None => catalog,
    };
    let provider = super::build_iceberg_catalog_provider(Arc::clone(&catalog)).await?;
    Ok((catalog, provider))
}

#[derive(Debug, Clone)]
struct Resolved {
    tables: Weak<SessionTables>,
    catalog: String,
    ident: TableIdent,
    table: Table,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindMode {
    Query,
    View,
    Command,
}

#[derive(Debug, Clone)]
pub struct Binding {
    resolved: Resolved,
    mode: BindMode,
}

impl Binding {
    #[must_use]
    pub fn table(&self) -> &Table {
        &self.resolved.table
    }

    #[must_use]
    pub fn mode(&self) -> BindMode {
        self.mode
    }

    #[must_use]
    pub fn ident(&self) -> &TableIdent {
        &self.resolved.ident
    }

    #[must_use]
    pub fn catalog(&self) -> &str {
        &self.resolved.catalog
    }

    #[must_use]
    pub fn current(&self) -> Option<TableState> {
        self.resolved
            .tables
            .upgrade()?
            .current(&self.resolved.catalog, &self.resolved.ident)
    }

    #[must_use]
    pub fn memo_key(&self) -> String {
        format!(
            "{:p}/{}/{}",
            self.resolved.tables.as_ptr(),
            self.resolved.catalog,
            self.resolved.ident
        )
    }
}

struct Bindings {
    by_provider: HashMap<usize, (Weak<dyn TableProvider>, Binding)>,
    sweep_at: usize,
}

fn is_iceberg(provider: &Arc<dyn TableProvider>) -> bool {
    let any: &dyn Any = provider.as_ref();
    any.is::<IcebergTableProvider>()
}

fn provider_key(provider: &Arc<dyn TableProvider>) -> usize {
    Arc::as_ptr(provider).cast::<()>() as usize
}

fn note(provider: &Arc<dyn TableProvider>, binding: Binding) {
    let mut bindings = locked(&BINDINGS);
    if bindings.by_provider.len() >= bindings.sweep_at {
        bindings
            .by_provider
            .retain(|_, (held, _)| held.strong_count() > 0);
        bindings.sweep_at = (bindings.by_provider.len() * 2).max(SWEEP_FLOOR);
    }
    bindings
        .by_provider
        .insert(provider_key(provider), (Arc::downgrade(provider), binding));
}

#[must_use]
pub fn binding(provider: &Arc<dyn TableProvider>) -> Option<Binding> {
    let bindings = locked(&BINDINGS);
    let (held, binding) = bindings.by_provider.get(&provider_key(provider))?;
    let alive = held.upgrade()?;
    Arc::ptr_eq(&alive, provider).then(|| binding.clone())
}

#[must_use]
pub fn rebound_clone(
    provider: &Arc<dyn TableProvider>,
    mode: BindMode,
) -> Option<Arc<dyn TableProvider>> {
    let found = binding(provider)?;
    let any: &dyn Any = provider.as_ref();
    let iceberg = any.downcast_ref::<IcebergTableProvider>()?;
    let clone: Arc<dyn TableProvider> = Arc::new(iceberg.clone());
    let mode = if found.mode == BindMode::View {
        BindMode::View
    } else {
        mode
    };
    note(
        &clone,
        Binding {
            resolved: found.resolved,
            mode,
        },
    );
    Some(clone)
}

#[allow(clippy::missing_errors_doc)]
pub fn rebound_provider(table: &Table) -> Result<IcebergTableProvider> {
    let ident = table.identifier().clone();
    let shim: Arc<dyn Catalog> = Arc::new(OneTableCatalog {
        table: table.clone(),
    });
    let built = IcebergTableProvider::try_new(shim, ident.namespace().clone(), ident.name());
    let mut built = pin!(built);
    match built.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(provider) => provider.map(|provider| provider.with_uuid_as_string(true)),
        Poll::Pending => Err(Error::new(
            ErrorKind::Unexpected,
            "a held Iceberg scan's table could not be bound without a catalog call",
        )),
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn refresh_load(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Result<Table> {
    Refreshing {
        inner: catalog.load_table(ident),
    }
    .await
}

struct Refreshing<'a> {
    inner: BoxedLoad<'a>,
}

impl Future for Refreshing<'_> {
    type Output = Result<Table>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let previous = REFRESHING.with(|flag| flag.replace(true));
        let polled = self.inner.as_mut().poll(context);
        REFRESHING.with(|flag| flag.set(previous));
        polled
    }
}

struct Capturing<'a> {
    inner: BoxedResolve<'a>,
    slot: Slot,
}

impl Future for Capturing<'_> {
    type Output = DataFusionResult<Option<Arc<dyn TableProvider>>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let slot = Arc::clone(&self.slot);
        let previous = RESOLVING.with(|current| current.replace(Some(slot)));
        let polled = self.inner.as_mut().poll(context);
        RESOLVING.with(|current| current.replace(previous));
        polled
    }
}

#[must_use]
pub fn capturing_schema(inner: Arc<dyn SchemaProvider>) -> Arc<dyn SchemaProvider> {
    Arc::new(CapturingSchema { inner })
}

#[derive(Debug)]
struct CapturingSchema {
    inner: Arc<dyn SchemaProvider>,
}

#[async_trait]
impl SchemaProvider for CapturingSchema {
    fn owner_name(&self) -> Option<&str> {
        self.inner.owner_name()
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let slot: Slot = Arc::default();
        let found = Capturing {
            inner: self.inner.table(name),
            slot: Arc::clone(&slot),
        }
        .await?;
        if let Some(provider) = &found
            && is_iceberg(provider)
            && let Some(resolved) = locked(&slot).take()
        {
            note(
                provider,
                Binding {
                    resolved,
                    mode: BindMode::Query,
                },
            );
        }
        Ok(found)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

#[derive(Debug)]
struct OneTableCatalog {
    table: Table,
}

fn outside_one_table() -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        "a held Iceberg scan reads only its decided table",
    )
}

#[async_trait]
impl Catalog for OneTableCatalog {
    async fn list_namespaces(&self, _: Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>> {
        Err(outside_one_table())
    }

    async fn create_namespace(
        &self,
        _: &NamespaceIdent,
        _: HashMap<String, String>,
    ) -> Result<Namespace> {
        Err(outside_one_table())
    }

    async fn get_namespace(&self, _: &NamespaceIdent) -> Result<Namespace> {
        Err(outside_one_table())
    }

    async fn namespace_exists(&self, _: &NamespaceIdent) -> Result<bool> {
        Err(outside_one_table())
    }

    async fn update_namespace(&self, _: &NamespaceIdent, _: HashMap<String, String>) -> Result<()> {
        Err(outside_one_table())
    }

    async fn drop_namespace(&self, _: &NamespaceIdent) -> Result<()> {
        Err(outside_one_table())
    }

    async fn list_tables(&self, _: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Err(outside_one_table())
    }

    async fn create_table(&self, _: &NamespaceIdent, _: TableCreation) -> Result<Table> {
        Err(outside_one_table())
    }

    async fn load_table(&self, ident: &TableIdent) -> Result<Table> {
        if ident == self.table.identifier() {
            Ok(self.table.clone())
        } else {
            Err(outside_one_table())
        }
    }

    async fn drop_table(&self, _: &TableIdent) -> Result<()> {
        Err(outside_one_table())
    }

    async fn table_exists(&self, ident: &TableIdent) -> Result<bool> {
        Ok(ident == self.table.identifier())
    }

    async fn rename_table(&self, _: &TableIdent, _: &TableIdent) -> Result<()> {
        Err(outside_one_table())
    }

    async fn register_table(&self, _: &TableIdent, _: String) -> Result<Table> {
        Err(outside_one_table())
    }

    async fn update_table(&self, _: TableCommit) -> Result<Table> {
        Err(outside_one_table())
    }
}

struct SessionTablesCatalog {
    inner: Arc<dyn Catalog>,
    name: String,
    tables: Arc<SessionTables>,
}

impl fmt::Debug for SessionTablesCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

impl SessionTablesCatalog {
    fn recorded(&self, table: Table) -> Table {
        self.tables.record(
            &self.name,
            table.identifier(),
            TableState::Live(table.clone()),
        );
        table
    }
}

#[async_trait]
impl Catalog for SessionTablesCatalog {
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
        let table = self.inner.create_table(namespace, creation).await?;
        Ok(self.recorded(table))
    }

    async fn load_table(&self, ident: &TableIdent) -> Result<Table> {
        let refresh = REFRESHING.with(Cell::get);
        let before = self.tables.epoch(&self.name, ident);
        let table = self.inner.load_table(ident).await?;
        self.tables
            .observe(&self.name, ident, &table, before, refresh);
        RESOLVING.with(|current| {
            if let Some(slot) = current.borrow().as_ref() {
                let mut slot = locked(slot);
                if slot.is_none() {
                    *slot = Some(Resolved {
                        tables: Arc::downgrade(&self.tables),
                        catalog: self.name.clone(),
                        ident: ident.clone(),
                        table: table.clone(),
                    });
                }
            }
        });
        Ok(table)
    }

    async fn drop_table(&self, ident: &TableIdent) -> Result<()> {
        self.inner.drop_table(ident).await?;
        self.tables.record(&self.name, ident, TableState::Dropped);
        Ok(())
    }

    async fn table_exists(&self, ident: &TableIdent) -> Result<bool> {
        self.inner.table_exists(ident).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(src, dest).await?;
        self.tables.record(&self.name, src, TableState::Dropped);
        self.tables.forget(&self.name, dest);
        Ok(())
    }

    async fn register_table(&self, ident: &TableIdent, metadata_location: String) -> Result<Table> {
        let table = self.inner.register_table(ident, metadata_location).await?;
        Ok(self.recorded(table))
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let table = self.inner.update_table(commit).await?;
        Ok(self.recorded(table))
    }

    async fn publish_create_table(&self, table: Table) -> Result<Table> {
        let table = self.inner.publish_create_table(table).await?;
        Ok(self.recorded(table))
    }

    async fn publish_replace_table(
        &self,
        table: Table,
        expected_base_metadata_location: Option<String>,
    ) -> Result<Table> {
        let table = self
            .inner
            .publish_replace_table(table, expected_base_metadata_location)
            .await?;
        Ok(self.recorded(table))
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

    async fn invalidate_table(&self, ident: &TableIdent) -> Result<()> {
        self.inner.invalidate_table(ident).await
    }

    async fn invalidate_view(&self, view: &TableIdent) -> Result<()> {
        self.inner.invalidate_view(view).await
    }
}

#[must_use]
pub fn conformed(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
    Arc::new(HeldConformExec::new(input, schema))
}

#[derive(Debug)]
struct HeldConformExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl HeldConformExec {
    fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        let source = input.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            source.partitioning.clone(),
            source.emission_type,
            source.boundedness,
        ));
        Self {
            input,
            schema,
            properties,
        }
    }
}

impl DisplayAs for HeldConformExec {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "{CONFORM_NODE}")
    }
}

impl ExecutionPlan for HeldConformExec {
    fn name(&self) -> &str {
        CONFORM_NODE
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let input = children
            .pop()
            .ok_or_else(|| internal_datafusion_err!("{CONFORM_NODE} takes one input"))?;
        Ok(Arc::new(Self::new(input, Arc::clone(&self.schema))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let stream = self.input.execute(partition, context)?.map(move |batch| {
            batch.and_then(|batch| {
                RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())
                    .map_err(Into::into)
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests;
