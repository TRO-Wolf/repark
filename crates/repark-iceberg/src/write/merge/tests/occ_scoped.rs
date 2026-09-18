use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    FormatVersion, NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation,
    TableIdent,
};
use tempfile::TempDir;

use super::occ::iceberg_error;
use crate::write::merge::{
    MatchedAction, MatchedClause, MergeSpec, NotMatchedBySourceAction, NotMatchedBySourceClause,
    execute_merge,
};
use crate::write::predicate_dml::{PredicateDmlSpec, execute_predicate_dml};

type BoxedCatalogFuture<'a, T> = Pin<Box<dyn Future<Output = iceberg::Result<T>> + Send + 'a>>;

type Race = Box<dyn FnOnce(Arc<dyn Catalog>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub(super) type Row = (i64, Option<String>, Option<String>);

pub(super) const VERSIONS: [FormatVersion; 2] = [FormatVersion::V2, FormatVersion::V3];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    MergeOnRead,
    CopyOnWrite,
}

pub(super) const MODES: [Mode; 2] = [Mode::MergeOnRead, Mode::CopyOnWrite];

impl Mode {
    pub(super) fn property(self) -> &'static str {
        match self {
            Self::MergeOnRead => "merge-on-read",
            Self::CopyOnWrite => "copy-on-write",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Layout {
    PartitionedByKey,
    TwoRangeFiles,
    OneFile,
}

pub(super) struct RaceCatalog {
    inner: Arc<dyn Catalog>,
    race: Mutex<Option<Race>>,
    fired: AtomicBool,
}

impl std::fmt::Debug for RaceCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RaceCatalog")
    }
}

impl RaceCatalog {
    pub(super) fn arm(&self, race: Race) {
        *self.race.lock().expect("race lock") = Some(race);
    }

    pub(super) fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
}

impl Catalog for RaceCatalog {
    fn list_namespaces<'life0, 'life1, 'async_trait>(
        &'life0 self,
        parent: Option<&'life1 NamespaceIdent>,
    ) -> BoxedCatalogFuture<'async_trait, Vec<NamespaceIdent>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.list_namespaces(parent)
    }

    fn create_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> BoxedCatalogFuture<'async_trait, Namespace>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.create_namespace(namespace, properties)
    }

    fn get_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, Namespace>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.get_namespace(namespace)
    }

    fn namespace_exists<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, bool>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.namespace_exists(namespace)
    }

    fn update_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.update_namespace(namespace, properties)
    }

    fn drop_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.drop_namespace(namespace)
    }

    fn list_tables<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, Vec<TableIdent>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.list_tables(namespace)
    }

    fn create_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
        creation: TableCreation,
    ) -> BoxedCatalogFuture<'async_trait, Table>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.create_table(namespace, creation)
    }

    fn load_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, Table>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.load_table(table)
    }

    fn drop_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.drop_table(table)
    }

    fn table_exists<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, bool>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.table_exists(table)
    }

    fn rename_table<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        src: &'life1 TableIdent,
        dest: &'life2 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.rename_table(src, dest)
    }

    fn register_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 TableIdent,
        metadata_location: String,
    ) -> BoxedCatalogFuture<'async_trait, Table>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.register_table(table, metadata_location)
    }

    fn update_table<'life0, 'async_trait>(
        &'life0 self,
        commit: TableCommit,
    ) -> BoxedCatalogFuture<'async_trait, Table>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let race = self.race.lock().expect("race lock").take();
            if let Some(race) = race {
                self.fired.store(true, Ordering::SeqCst);
                race(Arc::clone(&self.inner)).await;
            }
            self.inner.update_table(commit).await
        })
    }
}

pub(super) struct Fixture {
    _warehouse: TempDir,
    pub(super) inner: Arc<dyn Catalog>,
    pub(super) racing: Arc<RaceCatalog>,
    pub(super) ident: TableIdent,
}

impl Fixture {
    pub(super) fn victim(&self) -> Arc<dyn Catalog> {
        let racing: Arc<dyn Catalog> = self.racing.clone();
        racing
    }
}

pub(super) async fn fixture(
    version: FormatVersion,
    mode: Mode,
    layout: Layout,
    extra: &[(&str, &str)],
) -> Fixture {
    let warehouse = TempDir::new().expect("temp warehouse");
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();
    let inner: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("memory catalog"),
    );
    let namespace = NamespaceIdent::new("sales".to_string());
    inner
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "k", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema");
    let creation = if layout == Layout::PartitionedByKey {
        TableCreation::builder()
            .name("occ".to_string())
            .schema(schema)
            .partition_spec(
                UnboundPartitionSpec::builder()
                    .add_partition_field(2, "k", Transform::Identity)
                    .expect("identity(k)")
                    .build(),
            )
            .properties(HashMap::new())
            .build()
    } else {
        TableCreation::builder()
            .name("occ".to_string())
            .schema(schema)
            .properties(HashMap::new())
            .build()
    };
    inner
        .create_table(&namespace, creation)
        .await
        .expect("create target");
    let ident = TableIdent::new(namespace, "occ".to_string());
    let mut properties: HashMap<String, String> =
        ["write.merge.mode", "write.update.mode", "write.delete.mode"]
            .iter()
            .map(|key| ((*key).to_string(), mode.property().to_string()))
            .collect();
    for (key, value) in extra {
        properties.insert((*key).to_string(), (*value).to_string());
    }
    crate::write::format_version::set_properties_and_format_version(
        inner.as_ref(),
        &ident,
        None,
        properties,
        &[],
        Some(version),
    )
    .await
    .expect("stamp the write modes and the format version");
    seed(&inner, &ident, layout).await;
    let racing = Arc::new(RaceCatalog {
        inner: Arc::clone(&inner),
        race: Mutex::new(None),
        fired: AtomicBool::new(false),
    });
    Fixture {
        _warehouse: warehouse,
        inner,
        racing,
        ident,
    }
}

fn key_of(id: i64) -> &'static str {
    ["a", "b", "c", "d"][usize::try_from(id % 4).expect("id is non-negative")]
}

fn rows_batch(ids: impl Iterator<Item = i64>, value: &str) -> RecordBatch {
    let ids: Vec<i64> = ids.collect();
    let keys: Vec<&str> = ids.iter().map(|id| key_of(*id)).collect();
    keyed_batch(ids, keys, value)
}

fn keyed_batch(ids: Vec<i64>, keys: Vec<&str>, value: &str) -> RecordBatch {
    let values: Vec<&str> = ids.iter().map(|_| value).collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(keys)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .expect("seed batch")
}

async fn seed(catalog: &Arc<dyn Catalog>, ident: &TableIdent, layout: Layout) {
    let bounds: &[(i64, i64)] = match layout {
        Layout::PartitionedByKey | Layout::OneFile => &[(0, 100)],
        Layout::TwoRangeFiles => &[(0, 50), (50, 100)],
    };
    for (low, high) in bounds {
        crate::write::append::append(catalog, ident, vec![rows_batch(*low..*high, "init")])
            .await
            .expect("seed append");
    }
}

async fn merge(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    source_from_sql: &str,
    on_sql: &str,
) -> Result<(), DataFusionError> {
    execute_merge(
        &SessionContext::new(),
        catalog,
        &merge_spec(ident, source_from_sql, on_sql),
    )
    .await
}

pub(super) fn merge_spec(ident: &TableIdent, source_from_sql: &str, on_sql: &str) -> MergeSpec {
    MergeSpec {
        target: ident.clone(),
        target_alias: "t".to_string(),
        source_from_sql: source_from_sql.to_string(),
        source_alias: "s".to_string(),
        on_sql: on_sql.to_string(),
        matched: vec![MatchedClause {
            predicate_sql: None,
            action: MatchedAction::Update {
                assignments: vec![("v".to_string(), "s.v".to_string())],
            },
        }],
        not_matched: Vec::new(),
        not_matched_by_source: Vec::new(),
        commit_branch: None,
    }
}

async fn predicate_dml(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    selection_sql: &str,
    set_value: Option<&str>,
) -> Result<(), DataFusionError> {
    let spec = PredicateDmlSpec {
        target: ident.clone(),
        target_alias: "occ".to_string(),
        selection_sql: selection_sql.to_string(),
        assignments: set_value.map(|value| vec![("v".to_string(), format!("'{value}'"))]),
    };
    execute_predicate_dml(&SessionContext::new(), catalog, &spec).await
}

fn racing_dml(
    ident: &TableIdent,
    selection_sql: &'static str,
    set_value: Option<&'static str>,
) -> Race {
    let ident = ident.clone();
    Box::new(move |inner| {
        Box::pin(async move {
            predicate_dml(&inner, &ident, selection_sql, set_value)
                .await
                .expect("the concurrent DML commits first");
        })
    })
}

fn racing_insert(ident: &TableIdent) -> Race {
    racing_insert_row(ident, 1000, "d")
}

pub(super) fn racing_insert_row(ident: &TableIdent, id: i64, key: &'static str) -> Race {
    let ident = ident.clone();
    Box::new(move |inner| {
        Box::pin(async move {
            let row = keyed_batch(vec![id], vec![key], "appended");
            crate::write::append::append(&inner, &ident, vec![row])
                .await
                .expect("the concurrent INSERT commits first");
        })
    })
}

pub(super) async fn live_rows(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<Row> {
    let table = catalog.load_table(ident).await.expect("load");
    let batches: Vec<RecordBatch> = table
        .scan()
        .select(["id", "k", "v"])
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id Int64");
        let keys = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("k Utf8");
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("v Utf8");
        for row in 0..batch.num_rows() {
            let text = |column: &StringArray| {
                (!column.is_null(row)).then(|| column.value(row).to_string())
            };
            rows.push((ids.value(row), text(keys), text(values)));
        }
    }
    rows.sort();
    rows
}

fn expected(ids: impl Iterator<Item = i64>, changed: &[(i64, &str)], deleted: &[i64]) -> Vec<Row> {
    let mut rows: Vec<Row> = ids
        .filter(|id| !deleted.contains(id))
        .map(|id| {
            let value = changed
                .iter()
                .find(|(changed_id, _)| *changed_id == id)
                .map_or("init", |(_, value)| *value);
            (id, Some(key_of(id).to_string()), Some(value.to_string()))
        })
        .collect();
    rows.sort();
    rows
}

pub(super) fn label(version: FormatVersion, mode: Mode) -> String {
    format!("{version:?}/{}", mode.property())
}

pub(super) fn assert_validation_conflict(error: &DataFusionError, needle: &str, cell: &str) {
    let ice = iceberg_error(error);
    assert_eq!(ice.kind(), ErrorKind::DataInvalid, "{cell}: {ice}");
    assert!(
        ice.message().contains(needle),
        "{cell}: the loser must name `{needle}`, got: {}",
        ice.message()
    );
}

#[tokio::test]
async fn partition_scoped_merges_commit_through_a_concurrent_write_to_another_partition() {
    for version in VERSIONS {
        for mode in MODES {
            let cell = label(version, mode);
            let fx = fixture(version, mode, Layout::PartitionedByKey, &[]).await;
            fx.racing
                .arm(racing_dml(&fx.ident, "k = 'b' AND id < 8", Some("ub")));
            merge(
                &fx.victim(),
                &fx.ident,
                "(SELECT CAST(0 AS BIGINT) AS id, 'a' AS k, 'm0' AS v)",
                "t.k = 'a' AND t.k = s.k AND t.id = s.id",
            )
            .await
            .unwrap_or_else(|error| {
                panic!("{cell}: the partition-scoped MERGE must commit: {error}")
            });
            assert!(
                fx.racing.fired(),
                "{cell}: the race must land inside the commit"
            );
            assert_eq!(
                live_rows(&fx.inner, &fx.ident).await,
                expected(0..100, &[(0, "m0"), (1, "ub"), (5, "ub")], &[]),
                "{cell}"
            );
        }
    }
}

#[tokio::test]
async fn partition_scoped_updates_commit_through_a_concurrent_update_of_another_partition() {
    for version in VERSIONS {
        for mode in MODES {
            let cell = label(version, mode);
            let fx = fixture(version, mode, Layout::PartitionedByKey, &[]).await;
            fx.racing
                .arm(racing_dml(&fx.ident, "k = 'b' AND id < 8", Some("ub")));
            predicate_dml(&fx.victim(), &fx.ident, "k = 'a' AND id < 8", Some("ua"))
                .await
                .unwrap_or_else(|error| {
                    panic!("{cell}: the partition-scoped UPDATE must commit: {error}")
                });
            assert!(
                fx.racing.fired(),
                "{cell}: the race must land inside the commit"
            );
            assert_eq!(
                live_rows(&fx.inner, &fx.ident).await,
                expected(0..100, &[(0, "ua"), (4, "ua"), (1, "ub"), (5, "ub")], &[]),
                "{cell}"
            );
        }
    }
}

#[tokio::test]
async fn partition_scoped_deletes_commit_through_a_concurrent_delete_in_another_partition() {
    for version in VERSIONS {
        for mode in MODES {
            let cell = label(version, mode);
            let fx = fixture(version, mode, Layout::PartitionedByKey, &[]).await;
            fx.racing
                .arm(racing_dml(&fx.ident, "k = 'b' AND id > 90", None));
            predicate_dml(&fx.victim(), &fx.ident, "k = 'a' AND id > 90", None)
                .await
                .unwrap_or_else(|error| {
                    panic!("{cell}: the partition-scoped DELETE must commit: {error}")
                });
            assert!(
                fx.racing.fired(),
                "{cell}: the race must land inside the commit"
            );
            assert_eq!(
                live_rows(&fx.inner, &fx.ident).await,
                expected(0..100, &[], &[92, 93, 96, 97]),
                "{cell}"
            );
        }
    }
}

#[tokio::test]
async fn a_partition_scoped_merge_commits_through_a_concurrent_insert_into_another_partition() {
    for version in VERSIONS {
        for mode in MODES {
            let cell = label(version, mode);
            let fx = fixture(version, mode, Layout::PartitionedByKey, &[]).await;
            fx.racing.arm(racing_insert(&fx.ident));
            merge(
                &fx.victim(),
                &fx.ident,
                "(SELECT CAST(4 AS BIGINT) AS id, 'a' AS k, 'mx' AS v)",
                "t.k = 'a' AND t.id = s.id",
            )
            .await
            .unwrap_or_else(|error| {
                panic!("{cell}: the MERGE must commit past the INSERT: {error}")
            });
            assert!(
                fx.racing.fired(),
                "{cell}: the race must land inside the commit"
            );
            let mut want = expected(0..100, &[(4, "mx")], &[]);
            want.push((1000, Some("d".to_string()), Some("appended".to_string())));
            want.sort();
            assert_eq!(live_rows(&fx.inner, &fx.ident).await, want, "{cell}");
        }
    }
}

#[tokio::test]
async fn a_copy_on_write_range_scoped_merge_commits_through_a_disjoint_range_rewrite() {
    for version in VERSIONS {
        let cell = label(version, Mode::CopyOnWrite);
        let fx = fixture(version, Mode::CopyOnWrite, Layout::TwoRangeFiles, &[]).await;
        fx.racing
            .arm(racing_dml(&fx.ident, "id >= 50 AND id = 60", Some("m60")));
        merge(
            &fx.victim(),
            &fx.ident,
            "(SELECT CAST(1 AS BIGINT) AS id, 'm1' AS v)",
            "t.id < 50 AND t.id = s.id",
        )
        .await
        .unwrap_or_else(|error| panic!("{cell}: the range-scoped COW MERGE must commit: {error}"));
        assert!(
            fx.racing.fired(),
            "{cell}: the race must land inside the commit"
        );
        assert_eq!(
            live_rows(&fx.inner, &fx.ident).await,
            expected(0..100, &[(1, "m1"), (60, "m60")], &[]),
            "{cell}"
        );
    }
}

#[tokio::test]
async fn a_merge_on_read_range_scoped_merge_still_loses_to_the_concurrent_delete_file() {
    for version in VERSIONS {
        let cell = label(version, Mode::MergeOnRead);
        let fx = fixture(version, Mode::MergeOnRead, Layout::TwoRangeFiles, &[]).await;
        fx.racing
            .arm(racing_dml(&fx.ident, "id >= 50 AND id = 60", Some("m60")));
        let error = merge(
            &fx.victim(),
            &fx.ident,
            "(SELECT CAST(1 AS BIGINT) AS id, 'm1' AS v)",
            "t.id < 50 AND t.id = s.id",
        )
        .await
        .expect_err("Spark refuses this cell: the new delete file carries no `id` bounds");
        assert!(
            fx.racing.fired(),
            "{cell}: the race must land inside the commit"
        );
        assert_validation_conflict(
            &error,
            "Found new conflicting delete files that can apply to records matching id < 50",
            &cell,
        );
        assert_eq!(
            live_rows(&fx.inner, &fx.ident).await,
            expected(0..100, &[(60, "m60")], &[]),
            "{cell}"
        );
    }
}

#[tokio::test]
async fn disjoint_key_merges_on_an_unpartitioned_table_still_conflict_on_true() {
    let cells = [
        (
            "serializable",
            "Found conflicting files that can contain records matching TRUE",
        ),
        (
            "snapshot",
            "Found new conflicting delete files that can apply to records matching TRUE",
        ),
    ];
    for version in VERSIONS {
        for (isolation, needle) in cells {
            let cell = format!("{}/{isolation}", label(version, Mode::MergeOnRead));
            let fx = fixture(
                version,
                Mode::MergeOnRead,
                Layout::OneFile,
                &[("write.merge.isolation-level", isolation)],
            )
            .await;
            fx.racing.arm(racing_dml(&fx.ident, "id = 1", Some("m1")));
            let error = merge(
                &fx.victim(),
                &fx.ident,
                "(SELECT CAST(0 AS BIGINT) AS id, 'm0' AS v)",
                "t.id = s.id",
            )
            .await
            .expect_err("Spark refuses this cell: the ON condition has no target-only conjunct");
            assert!(
                fx.racing.fired(),
                "{cell}: the race must land inside the commit"
            );
            assert_validation_conflict(&error, needle, &cell);
            assert_eq!(
                live_rows(&fx.inner, &fx.ident).await,
                expected(0..100, &[(1, "m1")], &[]),
                "{cell}"
            );
        }
    }
}

#[tokio::test]
async fn a_merge_with_not_matched_by_source_stays_unscoped() {
    for version in VERSIONS {
        for mode in MODES {
            let cell = label(version, mode);
            let fx = fixture(version, mode, Layout::PartitionedByKey, &[]).await;
            fx.racing
                .arm(racing_dml(&fx.ident, "k = 'b' AND id < 8", Some("ub")));
            let mut spec = merge_spec(
                &fx.ident,
                "(SELECT CAST(0 AS BIGINT) AS id, 'a' AS k, 'm0' AS v)",
                "t.k = 'a' AND t.k = s.k AND t.id = s.id",
            );
            spec.not_matched_by_source = vec![NotMatchedBySourceClause {
                predicate_sql: Some("t.id = 96".to_string()),
                action: NotMatchedBySourceAction::Update {
                    assignments: vec![("v".to_string(), "'nm'".to_string())],
                },
            }];
            let victim = fx.victim();
            let error = execute_merge(&SessionContext::new(), &victim, &spec)
                .await
                .expect_err("NOT MATCHED BY SOURCE reads every target row: nothing is disjoint");
            assert!(
                fx.racing.fired(),
                "{cell}: the race must land inside the commit"
            );
            assert_validation_conflict(&error, "matching TRUE", &cell);
            assert_eq!(
                live_rows(&fx.inner, &fx.ident).await,
                expected(0..100, &[(1, "ub"), (5, "ub")], &[]),
                "{cell}"
            );
        }
    }
}

fn with_appended(mut rows: Vec<Row>, id: i64, key: &str) -> Vec<Row> {
    rows.push((id, Some(key.to_string()), Some("appended".to_string())));
    rows.sort();
    rows
}

#[tokio::test]
async fn a_partition_scoped_merge_loses_to_a_concurrent_insert_into_its_own_partition() {
    for version in VERSIONS {
        for mode in MODES {
            let cell = label(version, mode);
            let fx = fixture(version, mode, Layout::PartitionedByKey, &[]).await;
            fx.racing.arm(racing_insert_row(&fx.ident, 1004, "a"));
            let error = merge(
                &fx.victim(),
                &fx.ident,
                "(SELECT CAST(0 AS BIGINT) AS id, 'a' AS k, 'm0' AS v)",
                "t.k = 'a' AND t.k = s.k AND t.id = s.id",
            )
            .await
            .expect_err("serializable: a new row in the partition the MERGE read is a conflict");
            assert!(
                fx.racing.fired(),
                "{cell}: the race must land inside the commit"
            );
            assert_validation_conflict(
                &error,
                "Found conflicting files that can contain records matching k = \"a\"",
                &cell,
            );
            assert_eq!(
                live_rows(&fx.inner, &fx.ident).await,
                with_appended(expected(0..100, &[], &[]), 1004, "a"),
                "{cell}"
            );
        }
    }
}

#[tokio::test]
async fn a_partition_scoped_delete_loses_to_a_concurrent_insert_its_predicate_matches() {
    for version in VERSIONS {
        for mode in MODES {
            let cell = label(version, mode);
            let fx = fixture(version, mode, Layout::PartitionedByKey, &[]).await;
            fx.racing.arm(racing_insert_row(&fx.ident, 1004, "a"));
            let error = predicate_dml(&fx.victim(), &fx.ident, "k = 'a' AND id > 90", None)
                .await
                .expect_err("serializable: a new row the DELETE's WHERE matches is a conflict");
            assert!(
                fx.racing.fired(),
                "{cell}: the race must land inside the commit"
            );
            assert_validation_conflict(
                &error,
                "Found conflicting files that can contain records matching (k = \"a\") AND (id > 90)",
                &cell,
            );
            assert_eq!(
                live_rows(&fx.inner, &fx.ident).await,
                with_appended(expected(0..100, &[], &[]), 1004, "a"),
                "{cell}"
            );
        }
    }
}

#[tokio::test]
async fn a_partition_scoped_update_loses_to_a_concurrent_insert_its_predicate_matches() {
    for version in VERSIONS {
        for mode in MODES {
            let cell = label(version, mode);
            let fx = fixture(version, mode, Layout::PartitionedByKey, &[]).await;
            fx.racing.arm(racing_insert_row(&fx.ident, -4, "a"));
            let error = predicate_dml(&fx.victim(), &fx.ident, "k = 'a' AND id < 8", Some("ua"))
                .await
                .expect_err("serializable: a new row the UPDATE's WHERE matches is a conflict");
            assert!(
                fx.racing.fired(),
                "{cell}: the race must land inside the commit"
            );
            assert_validation_conflict(
                &error,
                "Found conflicting files that can contain records matching (k = \"a\") AND (id < 8)",
                &cell,
            );
            assert_eq!(
                live_rows(&fx.inner, &fx.ident).await,
                with_appended(expected(0..100, &[], &[]), -4, "a"),
                "{cell}"
            );
        }
    }
}
