use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

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
        .register_iceberg_catalog(
            "c",
            repark_iceberg::catalog::EncryptionGuardCatalog::install(
                Arc::clone(&counted) as Arc<dyn Catalog>
            ),
        )
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

fn bound_of(frame: &DataFrame) -> Binding {
    let mut found = None;
    frame
        .logical_plan()
        .apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && let Ok(provider) = source_as_provider(&scan.source)
            {
                found = found.take().or_else(|| binding(&provider));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap_or_else(|error| panic!("walk: {error}"));
    found.unwrap_or_else(|| panic!("a bound scan"))
}

async fn viewed(bed: &Bed, held: &DataFrame) -> DataFrame {
    bed.session
        .create_or_replace_temp_view_from("v", held)
        .unwrap_or_else(|error| panic!("view: {error}"));
    bed.session
        .sql("SELECT * FROM v")
        .await
        .unwrap_or_else(|error| panic!("view read: {error}"))
}

fn refusal(frame: &DataFrame) -> String {
    frame
        .clone()
        .into_optimized_plan()
        .map_or_else(|error| error.to_string(), |_| String::new())
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
    let bound = bound_of(&held);
    let memo = Arc::new(ReaderParseMemo::default());
    let first = memo.run(|| held.clone().into_optimized_plan());
    assert!(first.is_ok());
    let kept = memo.run(|| held.clone().into_optimized_plan());
    assert_eq!(kept.map(|plan| held_scans(&plan)).ok(), Some(0));
    bed.run("INSERT INTO c.ns.t VALUES (9, 'z')").await;
    let kept = memo.run(|| held.clone().into_optimized_plan());
    assert_eq!(kept.map(|plan| held_scans(&plan)).ok(), Some(0));
    let pinned = memo.held_table(&bound.memo_key());
    assert!(pinned.is_some_and(|table| same_state(table.table(), bound.table())));
    assert!(
        matches!(bound.current(), Some(TableState::Live(now)) if !same_state(now.table(), bound.table()))
    );
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

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_over_a_view_refuses_a_later_recreate_by_table_id() {
    let bed = bed().await;
    let held = bed.held().await;
    let frame = viewed(&bed, &held).await;
    assert_eq!(bound_of(&frame).mode(), BindMode::Query);
    bed.run("DROP TABLE c.ns.t").await;
    bed.run("CREATE TABLE c.ns.t (n INT, c STRING) USING iceberg")
        .await;
    let text = refusal(&frame);
    assert!(
        text.contains("INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH"),
        "{text}"
    );
    let after = bed
        .session
        .sql("SELECT * FROM v")
        .await
        .unwrap_or_else(|error| panic!("view read: {error}"));
    assert_eq!(numbers(&after).await, Vec::<i32>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_over_a_view_built_after_a_write_checks_the_table_it_was_built_on() {
    let bed = bed().await;
    let held = bed.held().await;
    bed.session
        .create_or_replace_temp_view_from("v", &held)
        .unwrap_or_else(|error| panic!("view: {error}"));
    bed.run("INSERT INTO c.ns.t VALUES (9, 'z')").await;
    let frame = bed
        .session
        .sql("SELECT * FROM v")
        .await
        .unwrap_or_else(|error| panic!("view read: {error}"));
    assert_eq!(numbers(&frame).await, vec![1, 2, 9]);
    assert_eq!(held_scans(frame.logical_plan()), 0);
    bed.run("ALTER TABLE c.ns.t DROP COLUMN c").await;
    let text = refusal(&frame);
    assert!(
        text.contains("INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH"),
        "{text}"
    );
    let text = refusal(&viewed(&bed, &held).await);
    assert!(
        text.contains("INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION"),
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

fn epoch(bed: &Bed) -> Option<u64> {
    repark_iceberg::catalog::session_tables::table_epoch(
        &bed.session.context().copied_config(),
        "c",
        &TableIdent::from_strs(["ns", "t"]).unwrap_or_else(|error| panic!("ident: {error}")),
    )
}

async fn refused(bed: &Bed, sql: &str) -> String {
    let text = match bed.session.sql(sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .map_or_else(|error| error.to_string(), |_| String::new()),
        Err(error) => error.to_string(),
    };
    assert!(text.contains("(ENC-1)"), "{sql}: {text}");
    text
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_write_moves_no_epoch_and_a_held_frame_answers_as_before() {
    let bed = bed().await;
    bed.run("ALTER TABLE c.ns.t SET TBLPROPERTIES ('encryption.key-id' = 'review-test-key')")
        .await;
    let held = bed.held().await;
    assert_eq!(numbers(&held).await, vec![1, 2]);
    let before = epoch(&bed);
    assert!(before.is_some_and(|epoch| epoch > 0));
    for sql in [
        "INSERT INTO c.ns.t VALUES (9, 'z')",
        "INSERT OVERWRITE c.ns.t VALUES (9, 'z')",
        "DELETE FROM c.ns.t WHERE n = 1",
        "UPDATE c.ns.t SET c = 'u' WHERE n = 1",
        "MERGE INTO c.ns.t AS t USING (SELECT 9 AS n, 'z' AS c) AS s ON t.n = s.n \
         WHEN NOT MATCHED THEN INSERT *",
    ] {
        refused(&bed, sql).await;
        assert_eq!(epoch(&bed), before, "{sql}");
        assert_eq!(numbers(&held).await, vec![1, 2], "{sql}");
    }
}

struct Under<F> {
    memo: Arc<ReaderParseMemo>,
    inner: Pin<Box<F>>,
}

impl<F: Future> Future for Under<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        let inner = this.inner.as_mut();
        ReaderParseMemo::within(Some(Arc::clone(&this.memo)), || inner.poll(context))
    }
}

fn under<F: Future>(memo: &Arc<ReaderParseMemo>, inner: F) -> Under<F> {
    Under {
        memo: Arc::clone(memo),
        inner: Box::pin(inner),
    }
}

async fn cached(bed: &Bed, name: &str, owner: Option<Arc<ReaderParseMemo>>) -> DataFrame {
    let held = bed.held().await;
    bed.session
        .register_lazy_cache_view(name, held, (None, None), None, owner)
        .await
        .unwrap_or_else(|error| panic!("cache: {error}"));
    bed.session
        .sql(&format!("SELECT * FROM {name}"))
        .await
        .unwrap_or_else(|error| panic!("cached read: {error}"))
}

async fn served(frame: &DataFrame) -> (Vec<i32>, usize) {
    let batches = frame
        .clone()
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect: {error}"));
    let buffer = batches.first().map_or(0, |batch| {
        batch.column(0).to_data().buffers()[0].as_ptr() as usize
    });
    let mut found = Vec::new();
    for batch in &batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("int column"));
        found.extend((0..values.len()).map(|row| values.value(row)));
    }
    found.sort_unstable();
    (found, buffer)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cached_frame_refills_after_a_same_session_commit_and_keeps_its_fill_otherwise() {
    let bed = bed().await;
    bed.run("CREATE TABLE c.ns.u (n INT) USING iceberg").await;
    let cached = cached(&bed, "__repark_cache_h", None).await;
    let (rows, first) = served(&cached).await;
    assert_eq!(rows, vec![1, 2]);
    for sql in [
        "INSERT INTO c.ns.u VALUES (7)",
        "ALTER TABLE c.ns.t SET TBLPROPERTIES ('k' = 'v')",
        "ALTER TABLE c.ns.t ADD COLUMN d INT",
    ] {
        bed.run(sql).await;
        assert_eq!(served(&cached).await, (vec![1, 2], first), "{sql}");
    }
    bed.run("INSERT INTO c.ns.t VALUES (9, 'z', 5)").await;
    let (rows, refilled) = served(&cached).await;
    assert_eq!(rows, vec![1, 2, 9]);
    assert_ne!(refilled, first);
    assert_eq!(served(&cached).await, (vec![1, 2, 9], refilled));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cached_frame_over_a_recreated_or_dropped_table_refuses_at_its_refill() {
    let bed = bed().await;
    let cached = cached(&bed, "__repark_cache_r", None).await;
    assert_eq!(numbers(&cached).await, vec![1, 2]);
    bed.run("DROP TABLE c.ns.t").await;
    bed.run("CREATE TABLE c.ns.t (n INT, c STRING) USING iceberg")
        .await;
    let text = cached
        .clone()
        .collect()
        .await
        .map_or_else(|error| error.to_string(), |_| String::new());
    assert!(
        text.contains("INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH"),
        "{text}"
    );
    bed.run("DROP TABLE c.ns.t").await;
    let text = cached
        .clone()
        .collect()
        .await
        .map_or_else(|error| error.to_string(), |_| String::new());
    assert!(text.contains("TABLE_OR_VIEW_NOT_FOUND"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_own_plan_memo_keeps_the_binding_its_cache_noted() {
    let bed = bed().await;
    let memo = Arc::new(ReaderParseMemo::default());
    let cached = cached(&bed, "__repark_cache_o", Some(Arc::clone(&memo))).await;
    bed.run("INSERT INTO c.ns.t VALUES (9, 'z')").await;
    assert_eq!(under(&memo, numbers(&cached)).await, vec![1, 2]);
    assert_eq!(numbers(&cached).await, vec![1, 2, 9]);
    assert_eq!(under(&memo, numbers(&cached)).await, vec![1, 2]);
    let fresh = Arc::new(ReaderParseMemo::default());
    assert_eq!(under(&fresh, numbers(&cached)).await, vec![1, 2, 9]);
    bed.run("INSERT INTO c.ns.t VALUES (8, 'q')").await;
    assert_eq!(under(&fresh, numbers(&cached)).await, vec![1, 2, 9]);
    assert_eq!(numbers(&cached).await, vec![1, 2, 8, 9]);
}

struct CacheRead {
    rows: Vec<i32>,
    buffer: usize,
    _kept: Vec<datafusion::arrow::record_batch::RecordBatch>,
}

async fn read_cache(frame: &DataFrame) -> CacheRead {
    let batches = frame
        .clone()
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect: {error}"));
    let buffer = batches.first().map_or(0, |batch| {
        batch.column(0).to_data().buffers()[0].as_ptr() as usize
    });
    let mut rows = Vec::new();
    for batch in &batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("int column"));
        rows.extend((0..values.len()).map(|row| values.value(row)));
    }
    rows.sort_unstable();
    CacheRead {
        rows,
        buffer,
        _kept: batches,
    }
}

async fn other_writer(bed: &Bed) -> ReparkSession {
    let session = ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .build()
        .unwrap_or_else(|error| panic!("session: {error}"));
    session
        .register_iceberg_catalog(
            "c",
            repark_iceberg::catalog::EncryptionGuardCatalog::install(
                Arc::clone(&bed.counted) as Arc<dyn Catalog>
            ),
        )
        .await
        .unwrap_or_else(|error| panic!("register: {error}"));
    session
}

async fn commit_from(other: &ReparkSession, sql: &str) {
    other
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("run {sql}: {error}"));
}

async fn fresh_read(bed: &Bed) -> Vec<i32> {
    let frame = bed
        .session
        .sql("SELECT n FROM c.ns.t")
        .await
        .unwrap_or_else(|error| panic!("fresh read: {error}"));
    numbers(&frame).await
}

async fn kept_over_another_writer(bed: &Bed, name: &str) -> (DataFrame, ReparkSession, CacheRead) {
    let cached = cached(bed, name, None).await;
    let first = read_cache(&cached).await;
    assert_eq!(first.rows, vec![1, 2]);
    let other = other_writer(bed).await;
    commit_from(&other, "INSERT INTO c.ns.t VALUES (9, 'z')").await;
    let before = epoch(bed);
    assert_eq!(fresh_read(bed).await, vec![1, 2, 9]);
    assert_ne!(epoch(bed), before);
    let kept = read_cache(&cached).await;
    assert_eq!((kept.rows, kept.buffer), (vec![1, 2], first.buffer));
    (cached, other, first)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cached_frame_keeps_its_fill_when_a_fresh_read_sees_another_writers_commit() {
    let bed = bed().await;
    let (cached, _other, first) = kept_over_another_writer(&bed, "__repark_cache_x1").await;
    let again = read_cache(&cached).await;
    assert_eq!((again.rows, again.buffer), (vec![1, 2], first.buffer));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cached_frame_kept_over_another_writer_refills_after_a_same_session_commit() {
    let bed = bed().await;
    let (cached, _other, first) = kept_over_another_writer(&bed, "__repark_cache_x2").await;
    bed.run("INSERT INTO c.ns.t VALUES (8, 'q')").await;
    let refilled = read_cache(&cached).await;
    assert_eq!(refilled.rows, vec![1, 2, 8, 9]);
    assert_ne!(refilled.buffer, first.buffer);
    let again = read_cache(&cached).await;
    assert_eq!(again.buffer, refilled.buffer);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cached_frame_kept_over_another_writer_refills_after_a_refresh() {
    let bed = bed().await;
    let (cached, other, first) = kept_over_another_writer(&bed, "__repark_cache_x3").await;
    bed.run("REFRESH TABLE c.ns.t").await;
    let refilled = read_cache(&cached).await;
    assert_eq!(refilled.rows, vec![1, 2, 9]);
    assert_ne!(refilled.buffer, first.buffer);
    commit_from(&other, "ALTER TABLE c.ns.t SET TBLPROPERTIES ('k' = 'v')").await;
    let before = epoch(&bed);
    assert_eq!(fresh_read(&bed).await, vec![1, 2, 9]);
    assert_ne!(epoch(&bed), before);
    let kept = read_cache(&cached).await;
    assert_eq!(kept.buffer, refilled.buffer);
    bed.run("REFRESH TABLE c.ns.t").await;
    let again = read_cache(&cached).await;
    assert_eq!(again.rows, vec![1, 2, 9]);
    assert_ne!(again.buffer, refilled.buffer);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_read_of_an_unchanged_table_after_another_writer_moves_nothing() {
    let bed = bed().await;
    let (cached, _other, first) = kept_over_another_writer(&bed, "__repark_cache_x4").await;
    let before = epoch(&bed);
    assert_eq!(fresh_read(&bed).await, vec![1, 2, 9]);
    assert_eq!(epoch(&bed), before);
    let kept = read_cache(&cached).await;
    assert_eq!((kept.rows, kept.buffer), (vec![1, 2], first.buffer));
}
