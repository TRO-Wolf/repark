use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::table::Table;
use iceberg::view::{View, ViewCommit};
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
    ViewCreation,
};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

#[derive(Debug)]
struct FaultCatalog {
    inner: Arc<dyn Catalog>,
    arm_on_publish: AtomicBool,
    fail_listing: AtomicBool,
    fail_lookup: AtomicBool,
}

impl FaultCatalog {
    fn unavailable(what: &str) -> iceberg::Error {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("{what} unavailable"),
        )
    }

    fn published(&self, table: Table) -> Table {
        if self.arm_on_publish.load(Ordering::SeqCst) {
            self.fail_listing.store(true, Ordering::SeqCst);
        }
        table
    }
}

#[async_trait::async_trait]
impl Catalog for FaultCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn update_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        removals: HashSet<String>,
        updates: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner
            .update_namespace_properties(namespace, removals, updates)
            .await
    }

    async fn set_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        updates: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner
            .set_namespace_properties(namespace, updates)
            .await
    }

    async fn remove_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        removals: HashSet<String>,
    ) -> iceberg::Result<()> {
        self.inner
            .remove_namespace_properties(namespace, removals)
            .await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        if self.fail_listing.load(Ordering::SeqCst) {
            return Err(Self::unavailable("table listing"));
        }
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        if self.fail_lookup.load(Ordering::SeqCst) {
            return Err(Self::unavailable("table lookup"));
        }
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(commit).await
    }

    async fn publish_create_table(&self, table: Table) -> iceberg::Result<Table> {
        let committed = self.inner.publish_create_table(table).await?;
        Ok(self.published(committed))
    }

    async fn publish_replace_table(
        &self,
        table: Table,
        expected_base_metadata_location: Option<String>,
    ) -> iceberg::Result<Table> {
        let committed = self
            .inner
            .publish_replace_table(table, expected_base_metadata_location)
            .await?;
        Ok(self.published(committed))
    }

    async fn list_views(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_views(namespace).await
    }

    async fn create_view(
        &self,
        namespace: &NamespaceIdent,
        creation: ViewCreation,
    ) -> iceberg::Result<View> {
        self.inner.create_view(namespace, creation).await
    }

    async fn load_view(&self, view: &TableIdent) -> iceberg::Result<View> {
        self.inner.load_view(view).await
    }

    async fn drop_view(&self, view: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_view(view).await
    }

    async fn view_exists(&self, view: &TableIdent) -> iceberg::Result<bool> {
        self.inner.view_exists(view).await
    }

    async fn rename_view(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_view(src, dest).await
    }

    async fn update_view(&self, commit: ViewCommit) -> iceberg::Result<View> {
        self.inner.update_view(commit).await
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &HashMap<String, String> {
        self.inner.properties()
    }

    async fn invalidate_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.invalidate_table(table).await
    }

    async fn invalidate_view(&self, view: &TableIdent) -> iceberg::Result<()> {
        self.inner.invalidate_view(view).await
    }
}

struct Harness {
    session: ReparkSession,
    inner: Arc<dyn Catalog>,
    fault: Arc<FaultCatalog>,
    _warehouse: TempDir,
}

async fn harness() -> Harness {
    let warehouse = TempDir::new().expect("warehouse");
    let root = warehouse.path().to_str().expect("path").to_string();
    let inner: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), root.clone())]),
            )
            .await
            .expect("catalog"),
    );
    inner
        .create_namespace(
            &NamespaceIdent::new("ns".to_string()),
            HashMap::from([("location".to_string(), format!("{root}/ns"))]),
        )
        .await
        .expect("namespace");
    let fault = Arc::new(FaultCatalog {
        inner: Arc::clone(&inner),
        arm_on_publish: AtomicBool::new(false),
        fail_listing: AtomicBool::new(false),
        fail_lookup: AtomicBool::new(false),
    });
    let session = ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .build()
        .expect("session");
    session
        .register_iceberg_catalog("fault", Arc::clone(&fault) as Arc<dyn Catalog>)
        .await
        .expect("register");
    Harness {
        session,
        inner,
        fault,
        _warehouse: warehouse,
    }
}

async fn attempt(session: &ReparkSession, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let frame = session.sql(sql).await.map_err(|error| error.to_string())?;
    frame.collect().await.map_err(|error| error.to_string())
}

fn int_values(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .to_vec()
        })
        .collect()
}

fn ident() -> TableIdent {
    TableIdent::from_strs(["ns", "t"]).expect("identifier")
}

async fn committed_ids(inner: &Arc<dyn Catalog>) -> Vec<i32> {
    let table = inner.load_table(&ident()).await.expect("published table");
    assert!(table.metadata().current_snapshot().is_some());
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("read")
        .try_collect()
        .await
        .expect("rows");
    int_values(&batches)
}

async fn session_ids(session: &ReparkSession) -> Vec<i32> {
    int_values(
        &attempt(session, "SELECT id FROM fault.ns.t ORDER BY id")
            .await
            .expect("the next statement resolves the committed table"),
    )
}

const CTAS: &str = "CREATE TABLE fault.ns.t USING iceberg TBLPROPERTIES ('format-version'='2') \
                    AS SELECT CAST(7 AS INT) AS id";
const RTAS: &str = "CREATE OR REPLACE TABLE fault.ns.t USING iceberg \
                    TBLPROPERTIES ('format-version'='2') AS SELECT CAST(8 AS INT) AS id";

#[tokio::test]
async fn ctas_with_a_refresh_failure_after_publish_reports_the_committed_outcome() {
    let harness = harness().await;
    harness.fault.arm_on_publish.store(true, Ordering::SeqCst);
    let outcome = attempt(&harness.session, CTAS).await;
    assert!(
        harness.fault.fail_listing.load(Ordering::SeqCst),
        "the fault armed after the publish"
    );
    assert!(
        outcome.is_ok(),
        "the table is committed, so the statement succeeds: {outcome:?}"
    );
    assert_eq!(committed_ids(&harness.inner).await, vec![7]);
    harness.fault.fail_listing.store(false, Ordering::SeqCst);
    assert_eq!(session_ids(&harness.session).await, vec![7]);
}

#[tokio::test]
async fn rtas_with_a_refresh_failure_after_publish_reports_the_committed_outcome() {
    let harness = harness().await;
    attempt(&harness.session, CTAS)
        .await
        .expect("the seed CTAS succeeds");
    harness.fault.arm_on_publish.store(true, Ordering::SeqCst);
    let outcome = attempt(&harness.session, RTAS).await;
    assert!(
        harness.fault.fail_listing.load(Ordering::SeqCst),
        "the fault armed after the publish"
    );
    assert!(
        outcome.is_ok(),
        "the replacement is committed, so the statement succeeds: {outcome:?}"
    );
    assert_eq!(committed_ids(&harness.inner).await, vec![8]);
    harness.fault.fail_listing.store(false, Ordering::SeqCst);
    assert_eq!(session_ids(&harness.session).await, vec![8]);
}

#[tokio::test]
async fn ctas_with_a_catalog_failure_before_publish_reports_failure_and_leaves_no_table() {
    let harness = harness().await;
    harness.fault.fail_lookup.store(true, Ordering::SeqCst);
    let outcome = attempt(&harness.session, CTAS).await;
    let error = outcome.expect_err("nothing is committed, so the statement fails");
    assert!(error.contains("table lookup unavailable"), "{error}");
    assert!(
        !harness
            .inner
            .table_exists(&ident())
            .await
            .expect("lookup on the underlying catalog")
    );
}
