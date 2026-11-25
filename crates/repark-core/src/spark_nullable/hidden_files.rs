use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::common::config::{ConfigOptions, TableOptions};
use datafusion::common::{Constraints, DFSchema, Statistics};
use datafusion::datasource::listing::ListingTable;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::registry::ExtensionTypeRegistryRef;
use datafusion::logical_expr::{
    AggregateUDF, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF, TableProviderFilterPushDown,
    TableType, WindowUDF,
};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::prelude::SessionConfig;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use url::Url;

#[must_use]
pub(crate) fn spark_hidden_name(name: &str) -> bool {
    (name.starts_with('_') && !name.contains('='))
        || name.starts_with('.')
        || name.ends_with("._COPYING_")
}

#[must_use]
pub(crate) fn spark_hidden_below(root: &ObjectPath, location: &ObjectPath) -> bool {
    let root_text: &str = root.as_ref();
    let location_text: &str = location.as_ref();
    let Some(rest) = location_text.strip_prefix(root_text) else {
        return false;
    };
    let rest = rest.trim_start_matches('/');
    if rest.is_empty() {
        return location.filename().is_some_and(spark_hidden_name);
    }
    rest.split('/').any(spark_hidden_name)
}

#[derive(Debug)]
pub(crate) struct SparkListingTable {
    inner: ListingTable,
    root: ObjectPath,
}

impl SparkListingTable {
    pub(crate) fn new(inner: ListingTable, root: ObjectPath) -> Self {
        Self { inner, root }
    }

    pub(crate) fn listing(&self) -> &ListingTable {
        &self.inner
    }
}

#[async_trait]
impl TableProvider for SparkListingTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let session = SparkListingSession::new(state, &self.root);
        self.inner.scan(&session, projection, filters, limit).await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DataFusionResult<ScanResult> {
        let session = SparkListingSession::new(state, &self.root);
        self.inner.scan_with_args(&session, args).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}

pub(crate) struct SparkListingSession<'a> {
    inner: &'a dyn Session,
    runtime: Arc<RuntimeEnv>,
    table_options: TableOptions,
}

impl<'a> SparkListingSession<'a> {
    pub(crate) fn new(inner: &'a dyn Session, root: &ObjectPath) -> Self {
        let mut runtime = RuntimeEnv::clone(inner.runtime_env());
        runtime.object_store_registry = Arc::new(SparkListingRegistry {
            inner: Arc::clone(&runtime.object_store_registry),
            root: root.clone(),
        });
        Self {
            inner,
            runtime: Arc::new(runtime),
            table_options: inner.table_options().clone(),
        }
    }
}

#[async_trait]
impl Session for SparkListingSession<'_> {
    fn session_id(&self) -> &str {
        self.inner.session_id()
    }

    fn config(&self) -> &SessionConfig {
        self.inner.config()
    }

    fn config_options(&self) -> &ConfigOptions {
        self.inner.config_options()
    }

    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(logical_plan).await
    }

    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        self.inner.create_physical_expr(expr, df_schema)
    }

    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        self.inner.scalar_functions()
    }

    fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
        self.inner.higher_order_functions()
    }

    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        self.inner.aggregate_functions()
    }

    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        self.inner.window_functions()
    }

    fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
        self.inner.extension_type_registry()
    }

    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime
    }

    fn execution_props(&self) -> &ExecutionProps {
        self.inner.execution_props()
    }

    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn table_options(&self) -> &TableOptions {
        &self.table_options
    }

    fn table_options_mut(&mut self) -> &mut TableOptions {
        &mut self.table_options
    }

    fn task_ctx(&self) -> Arc<TaskContext> {
        self.inner.task_ctx()
    }
}

#[derive(Debug)]
struct SparkListingRegistry {
    inner: Arc<dyn ObjectStoreRegistry>,
    root: ObjectPath,
}

impl ObjectStoreRegistry for SparkListingRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn deregister_store(&self, url: &Url) -> DataFusionResult<Arc<dyn ObjectStore>> {
        self.inner.deregister_store(url)
    }

    fn get_store(&self, url: &Url) -> DataFusionResult<Arc<dyn ObjectStore>> {
        let inner = self.inner.get_store(url)?;
        Ok(Arc::new(SparkListingStore {
            inner,
            root: self.root.clone(),
        }))
    }
}

#[derive(Debug)]
struct SparkListingStore {
    inner: Arc<dyn ObjectStore>,
    root: ObjectPath,
}

impl SparkListingStore {
    fn visible(
        &self,
        stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let root = self.root.clone();
        stream
            .try_filter(move |meta| {
                futures::future::ready(!spark_hidden_below(&root, &meta.location))
            })
            .boxed()
    }
}

impl fmt::Display for SparkListingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SparkListingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SparkListingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head && spark_hidden_below(&self.root, location) {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "a name Spark's file listing skips".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.visible(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.visible(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
