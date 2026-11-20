use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Array, Int32Array};
use datafusion::prelude::DataFrame;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit,
    TableCreation, TableIdent,
};
use repark_core::ReparkSession;
use tempfile::TempDir;

use super::*;
use crate::{SparkDialect, SparkExtension};

#[derive(Debug)]
struct Counted {
    inner: Arc<dyn Catalog>,
    loads: AtomicUsize,
}

#[async_trait]
impl Catalog for Counted {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, ident: &TableIdent) -> IcebergResult<Table> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.inner.load_table(ident).await
    }

    async fn drop_table(&self, ident: &TableIdent) -> IcebergResult<()> {
        self.inner.drop_table(ident).await
    }

    async fn table_exists(&self, ident: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(ident).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(&self, ident: &TableIdent, location: String) -> IcebergResult<Table> {
        self.inner.register_table(ident, location).await
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        self.inner.update_table(commit).await
    }

    async fn publish_create_table(&self, table: Table) -> IcebergResult<Table> {
        self.inner.publish_create_table(table).await
    }

    async fn publish_replace_table(
        &self,
        table: Table,
        expected: Option<String>,
    ) -> IcebergResult<Table> {
        self.inner.publish_replace_table(table, expected).await
    }
}

struct Bed {
    _warehouse: TempDir,
    counted: Arc<Counted>,
    session: ReparkSession,
}

impl Bed {
    fn loads(&self) -> usize {
        self.counted.loads.load(Ordering::SeqCst)
    }

    async fn run(&self, sql: &str) {
        self.session
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("run {sql}: {error}"));
    }

    async fn held(&self) -> DataFrame {
        self.session
            .sql("SELECT * FROM c.ns.t")
            .await
            .unwrap_or_else(|error| panic!("hold: {error}"))
    }
}

async fn bed() -> Bed {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("warehouse: {error}"));
    let root = warehouse
        .path()
        .to_str()
        .unwrap_or_else(|| panic!("utf-8 path"))
        .to_string();
    let raw: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), root.clone())]),
            )
            .await
            .unwrap_or_else(|error| panic!("memory catalog: {error}")),
    );
    let counted = Arc::new(Counted {
        inner: raw,
        loads: AtomicUsize::new(0),
    });
    counted
        .create_namespace(
            &NamespaceIdent::new("ns".to_string()),
            HashMap::from([("location".to_string(), format!("{root}/ns"))]),
        )
        .await
        .unwrap_or_else(|error| panic!("namespace: {error}"));
    let session = ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .build()
        .unwrap_or_else(|error| panic!("session: {error}"));
    session
        .register_iceberg_catalog("c", Arc::clone(&counted) as Arc<dyn Catalog>)
        .await
        .unwrap_or_else(|error| panic!("register: {error}"));
    let bed = Bed {
        _warehouse: warehouse,
        counted,
        session,
    };
    bed.run("CREATE TABLE c.ns.t (n INT, c STRING) USING iceberg")
        .await;
    bed.run("INSERT INTO c.ns.t VALUES (1, 'a'), (2, 'b')")
        .await;
    bed
}

async fn numbers(frame: &DataFrame) -> Vec<i32> {
    let batches = frame
        .clone()
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect: {error}"));
    let mut found = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        let values = column
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("int column"));
        found.extend((0..values.len()).map(|row| values.value(row)));
    }
    found.sort_unstable();
    found
}

fn held_scans(plan: &LogicalPlan) -> usize {
    let mut found = 0;
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Ok(provider) = source_as_provider(&scan.source)
            && (provider.as_ref() as &dyn std::any::Any).is::<HeldIcebergScan>()
        {
            found += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap_or_else(|error| panic!("walk: {error}"));
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn a_held_action_makes_no_catalog_load_with_or_without_a_write() {
    let bed = bed().await;
    let held = bed.held().await;
    let before = bed.loads();
    assert_eq!(numbers(&held).await, vec![1, 2]);
    assert_eq!(bed.loads(), before);
    bed.run("INSERT INTO c.ns.t VALUES (9, 'z')").await;
    let before = bed.loads();
    assert_eq!(numbers(&held).await, vec![1, 2, 9]);
    assert_eq!(bed.loads(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_wrapper_never_reaches_a_stored_plan() {
    let bed = bed().await;
    let held = bed.held().await;
    bed.run("INSERT INTO c.ns.t VALUES (9, 'z')").await;
    let optimized = held
        .clone()
        .into_optimized_plan()
        .unwrap_or_else(|error| panic!("optimize: {error}"));
    assert_eq!(held_scans(&optimized), 1);
    assert_eq!(held_scans(held.logical_plan()), 0);
    bed.session
        .create_or_replace_temp_view_from("v", &held)
        .unwrap_or_else(|error| panic!("view: {error}"));
    let viewed = bed
        .session
        .sql("SELECT * FROM v")
        .await
        .unwrap_or_else(|error| panic!("view read: {error}"));
    assert_eq!(held_scans(viewed.logical_plan()), 0);
    assert_eq!(numbers(&viewed).await, vec![1, 2, 9]);
    assert_eq!(numbers(&held).await, vec![1, 2, 9]);
    assert_eq!(held_scans(held.logical_plan()), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_own_plan_memo_keeps_the_first_binding() {
    let bed = bed().await;
    let held = bed.held().await;
    let memo = Arc::new(ReaderParseMemo::default());
    let first = memo.run(|| held.clone().into_optimized_plan());
    assert!(first.is_ok());
    let kept = memo.run(|| held.clone().into_optimized_plan());
    assert_eq!(kept.map(|plan| held_scans(&plan)).ok(), Some(0));
    bed.run("INSERT INTO c.ns.t VALUES (9, 'z')").await;
    let kept = memo.run(|| held.clone().into_optimized_plan());
    assert_eq!(kept.map(|plan| held_scans(&plan)).ok(), Some(0));
    let fresh = held
        .clone()
        .into_optimized_plan()
        .unwrap_or_else(|error| panic!("optimize: {error}"));
    assert_eq!(held_scans(&fresh), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recreated_table_refuses_by_table_id() {
    let bed = bed().await;
    let held = bed.held().await;
    bed.run("DROP TABLE c.ns.t").await;
    bed.run("CREATE TABLE c.ns.t (n INT, c STRING) USING iceberg")
        .await;
    let refused = held.clone().into_optimized_plan();
    let text = refused.map_or_else(|error| error.to_string(), |_| String::new());
    assert!(
        text.contains("INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH"),
        "{text}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_column_refuses_and_an_added_one_is_allowed() {
    let bed = bed().await;
    let held = bed.held().await;
    bed.run("ALTER TABLE c.ns.t ADD COLUMN d INT").await;
    bed.run("INSERT INTO c.ns.t VALUES (9, 'z', 5)").await;
    assert_eq!(numbers(&held).await, vec![1, 2, 9]);
    bed.run("ALTER TABLE c.ns.t DROP COLUMN c").await;
    let text = held
        .clone()
        .into_optimized_plan()
        .map_or_else(|error| error.to_string(), |_| String::new());
    assert!(
        text.contains("INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH"),
        "{text}"
    );
}

#[test]
fn a_view_name_picks_the_binding_mode() {
    assert_eq!(
        view_mode("`s`.`d`.`_repark_writer_v2_9`"),
        Some(BindMode::Command)
    );
    assert_eq!(view_mode("_repark_writer_1"), Some(BindMode::Command));
    assert_eq!(view_mode("`s`.`d`.`__repark_explain_3`"), None);
    assert_eq!(view_mode("__rp_jl_1"), None);
    assert_eq!(view_mode("dv1"), Some(BindMode::View));
}
