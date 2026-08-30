/// WG-1: identity-partitioned MERGE preserves manifests, pruning, and Arrow value and type.
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::TryStreamExt;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{DataFile, Datum, Literal, ManifestContentType, PrimitiveLiteral};
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, TableCommit, TableCreation};

use super::super::*;
use super::common::*;

/// Load `ice.sales.<table>` through the iceberg handle (manifest/scan oracle).
async fn loaded_table(catalogs: &CatalogRegistry, table: &str) -> Table {
    catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            table.to_string(),
        ))
        .await
        .expect("load table")
}

/// The live (Added/Existing) DATA-file entries in the current snapshot's manifests.
async fn live_data_files(table: &Table) -> Vec<DataFile> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Vec::new();
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("load manifest list");
    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("load manifest");
        for entry in manifest.entries() {
            if entry.is_alive() {
                files.push(entry.data_file().clone());
            }
        }
    }
    files
}

/// The data-file paths a partition-filtered scan PLANS — the plan-level pruning oracle.
async fn planned_paths(table: &Table, predicate: Predicate) -> HashSet<String> {
    let scan = table
        .scan()
        .with_filter(predicate)
        .build()
        .expect("build filtered scan");
    let tasks: Vec<_> = scan
        .plan_files()
        .await
        .expect("plan files")
        .try_collect()
        .await
        .expect("collect planned tasks");
    tasks
        .iter()
        .map(|task| task.data_file_path().to_string())
        .collect()
}

/// The single identity partition slot of a `DataFile` as an int.
fn slot_int(file: &DataFile) -> i32 {
    match file.partition().fields().first().cloned().flatten() {
        Some(Literal::Primitive(PrimitiveLiteral::Int(key))) => key,
        other => panic!("partition slot must be a non-null int literal, got {other:?}"),
    }
}

/// Map partition-slot value → total manifest record count across that partition's files.
async fn slot_record_counts(catalogs: &CatalogRegistry, table: &str) -> BTreeMap<i32, u64> {
    let handle = loaded_table(catalogs, table).await;
    let mut counts: BTreeMap<i32, u64> = BTreeMap::new();
    for file in &live_data_files(&handle).await {
        *counts.entry(slot_int(file)).or_insert(0) += file.record_count();
    }
    counts
}

/// The set of live data-file paths whose partition slot equals `key`.
async fn slot_paths(catalogs: &CatalogRegistry, table: &str, key: i32) -> HashSet<String> {
    let handle = loaded_table(catalogs, table).await;
    live_data_files(&handle)
        .await
        .iter()
        .filter(|file| slot_int(file) == key)
        .map(|file| file.file_path().to_string())
        .collect()
}

/// WG1-P1.
#[tokio::test]
async fn merge_partitioned_mixed_upsert_stamps_partition_values() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.pt AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;

    // The matched row took the source value.
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.pt").await,
        vec![
            (1, "a".to_string()),
            (2, "bee".to_string()),
            (3, "c".to_string()),
            (4, "dee".to_string()),
        ],
    );

    // Every committed file.
    assert_eq!(
        slot_record_counts(&catalogs, "pt").await,
        BTreeMap::from([(1, 1), (2, 1), (3, 1), (4, 1)]),
        "one record per partition slot 1..4 (rewrite + insert both correctly partitioned)"
    );

    // The inserted row prunes to exactly the new partition's file; ditto the rewritten row.
    let handle = loaded_table(&catalogs, "pt").await;
    assert_eq!(
        planned_paths(&handle, Reference::new("id").equal_to(Datum::int(4))).await,
        slot_paths(&catalogs, "pt", 4).await,
        "an id=4 scan must plan ONLY the inserted partition's file"
    );
    assert_eq!(
        planned_paths(&handle, Reference::new("id").equal_to(Datum::int(2))).await,
        slot_paths(&catalogs, "pt", 2).await,
        "an id=2 scan must plan ONLY the rewritten partition's file"
    );
}

/// WG1-P2.
#[tokio::test]
async fn merge_partitioned_insert_only_fans_out() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;
    let before = id_file_pairs(&catalogs, "pt").await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.pt AS t USING updates AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;

    // id=2 matched but there is no WHEN MATCHED clause, so it is untouched; id=4 inserts.
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.pt").await,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
            (4, "dee".to_string()),
        ],
    );
    // Every pre-merge (id, file) pair survives — insert-only rewrites nothing.
    let after = id_file_pairs(&catalogs, "pt").await;
    for pair in &before {
        assert!(
            after.contains(pair),
            "pre-merge file for id {} must survive an insert-only merge",
            pair.0
        );
    }
    assert_eq!(
        slot_record_counts(&catalogs, "pt").await,
        BTreeMap::from([(1, 1), (2, 1), (3, 1), (4, 1)]),
        "the inserted id=4 file carries partition slot 4 at the manifest level"
    );
}

/// WG1-P3.
#[tokio::test]
async fn merge_partitioned_multi_partition_unsorted_source() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // Base table spans partitions 1..4.
    register_source(&ctx, "part_base", &[(1, "a"), (2, "b"), (3, "c"), (4, "d")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt USING iceberg PARTITIONED BY (id) AS \
             SELECT * FROM part_base",
    )
    .await;
    register_source(&ctx, "updates", &[(3, "C"), (1, "A"), (5, "E"), (2, "B")]);

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.pt AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.pt").await,
        vec![
            (1, "A".to_string()),
            (2, "B".to_string()),
            (3, "C".to_string()),
            (4, "d".to_string()),
            (5, "E".to_string()),
        ],
    );
    assert_eq!(
        slot_record_counts(&catalogs, "pt").await,
        BTreeMap::from([(1, 1), (2, 1), (3, 1), (4, 1), (5, 1)]),
        "each partition slot 1..5 holds exactly its one row after the unsorted fanout"
    );
}

/// WG1-P4 — a matched UPDATE that CHANGES the partition key moves the row to the NEW partition.
#[tokio::test]
async fn merge_partitioned_update_moves_row_to_new_partition() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "part_base", &[(1, "a"), (2, "b")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt USING iceberg PARTITIONED BY (id) AS \
             SELECT * FROM part_base",
    )
    .await;
    // Source matches id=1; the SET rewrites the partition key to 99 — the row must move.
    register_source(&ctx, "updates", &[(1, "ignored")]);

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.pt AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET id = 99",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.pt").await,
        vec![(2, "b".to_string()), (99, "a".to_string())],
        "the matched row moved from id=1 to id=99, carrying its name"
    );
    // The moved row lands under the NEW partition; the OLD partition has no live file.
    assert_eq!(
        slot_record_counts(&catalogs, "pt").await,
        BTreeMap::from([(2, 1), (99, 1)]),
        "partition 1 is gone (rewritten away); the row is now under partition 99"
    );
    let handle = loaded_table(&catalogs, "pt").await;
    assert_eq!(
        planned_paths(&handle, Reference::new("id").equal_to(Datum::int(99))).await,
        slot_paths(&catalogs, "pt", 99).await,
        "an id=99 scan plans exactly the new partition's file"
    );
    assert!(
        planned_paths(&handle, Reference::new("id").equal_to(Datum::int(1)))
            .await
            .is_empty(),
        "an id=1 scan plans NOTHING — the old partition was rewritten away"
    );
}

/// WG1-P8.
#[tokio::test]
async fn merge_partitioned_star_forms_upsert() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.pt AS Target USING updates AS Source \
             ON Target.id = Source.id \
             WHEN MATCHED THEN UPDATE SET * \
             WHEN NOT MATCHED THEN INSERT *",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.pt").await,
        vec![
            (1, "a".to_string()),
            (2, "bee".to_string()),
            (3, "c".to_string()),
            (4, "dee".to_string()),
        ],
    );
    assert_eq!(
        slot_record_counts(&catalogs, "pt").await,
        BTreeMap::from([(1, 1), (2, 1), (3, 1), (4, 1)]),
        "star-form rewrite + insert both carry correct partition values"
    );
}

// WG1-P5 — partitioned-MERGE optimistic-concurrency (per arm).

/// The concurrent commit the injector lands mid-MERGE, INSIDE the victim's first `update_table`.
#[derive(Clone, Copy, Debug)]
enum ConcurrentOp {
    /// Adds a data file → serializable `validate_no_conflicting_data` must reject the MERGE.
    ConflictingAppend,
    /// Sets a table property → a real CAS conflict + refresh, but NO added data.
    NonConflictingProperty,
}

/// A conforming one-row batch (`id`, `name`) — the injected competing append's payload.
fn conflict_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![7])),
            Arc::new(StringArray::from(vec!["concurrent"])),
        ],
    )
    .expect("build conflict batch")
}

/// The boxed-future return type of an `#[async_trait]` `Catalog` method.
type BoxedCatalogFuture<'a, T> = Pin<Box<dyn Future<Output = iceberg::Result<T>> + Send + 'a>>;

/// A fully-delegating `Catalog` wrapper.
#[derive(Debug)]
struct ConflictInjector {
    inner: Arc<dyn Catalog>,
    victim_ident: TableIdent,
    update_table_attempts: AtomicUsize,
    op: std::sync::Mutex<Option<ConcurrentOp>>,
}

impl ConflictInjector {
    fn new(inner: Arc<dyn Catalog>, victim_ident: TableIdent, op: ConcurrentOp) -> Self {
        Self {
            inner,
            victim_ident,
            update_table_attempts: AtomicUsize::new(0),
            op: std::sync::Mutex::new(Some(op)),
        }
    }
}

impl Catalog for ConflictInjector {
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
            let attempt = self.update_table_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            // Take the op out and DROP the guard before any await.
            let op = if attempt == 1 {
                self.op
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
            } else {
                None
            };
            if let Some(op) = op {
                // The victim's `do_commit` has already refreshed its base.
                match op {
                    ConcurrentOp::ConflictingAppend => {
                        repark_iceberg::write::append(
                            &self.inner,
                            &self.victim_ident,
                            vec![conflict_batch()],
                        )
                        .await
                        .expect("the injected competing append must commit");
                    }
                    ConcurrentOp::NonConflictingProperty => {
                        repark_iceberg::write::alter::set_table_properties(
                            self.inner.as_ref(),
                            &self.victim_ident,
                            &HashMap::from([("injected.concurrent".to_string(), "1".to_string())]),
                        )
                        .await
                        .expect("the injected property commit must land");
                    }
                }
            }
            self.inner.update_table(commit).await
        })
    }
}

/// Build a one-catalog registry over `catalog` (name `ice`) — the shape `execute` consumes.
fn registry_over(catalog: Arc<dyn Catalog>) -> CatalogRegistry {
    CatalogRegistry::from([("ice".to_string(), catalog)])
}

/// WG1-P5a.
#[tokio::test]
async fn merge_partitioned_rewrite_arm_rejects_conflicting_concurrent_append() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;

    let inner = catalogs["ice"].clone();
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "pt".to_string());
    let injector: Arc<dyn Catalog> = Arc::new(ConflictInjector::new(
        inner,
        ident,
        ConcurrentOp::ConflictingAppend,
    ));

    let error = execute(
        &ctx,
        &registry_over(injector),
        "MERGE INTO ice.sales.pt AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await
    .expect_err("the rewrite-arm MERGE must reject the conflicting concurrent add");
    assert!(
        error.to_string().contains("Found conflicting files"),
        "must be the serializable added-data conflict (validate_no_conflicting_data), \
             got: {error}"
    );
}

/// WG1-P5b.
#[tokio::test]
async fn merge_partitioned_insert_only_rejects_conflicting_concurrent_append() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;

    let inner = catalogs["ice"].clone();
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "pt".to_string());
    let injector: Arc<dyn Catalog> = Arc::new(ConflictInjector::new(
        inner,
        ident,
        ConcurrentOp::ConflictingAppend,
    ));

    let error = execute(
        &ctx,
        &registry_over(injector),
        "MERGE INTO ice.sales.pt AS t USING updates AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await
    .expect_err("the insert-only MERGE must reject the conflicting concurrent add");
    assert!(
        error.to_string().contains("Found conflicting files"),
        "must be the serializable added-data conflict (validate_no_conflicting_data), \
             got: {error}"
    );
}

/// WG1-P5c.
#[tokio::test]
async fn merge_partitioned_tolerates_nonconflicting_concurrent_commit() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt USING iceberg PARTITIONED BY (id) AS SELECT * FROM src",
    )
    .await;

    let inner = catalogs["ice"].clone();
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "pt".to_string());
    let injector: Arc<dyn Catalog> = Arc::new(ConflictInjector::new(
        inner,
        ident,
        ConcurrentOp::NonConflictingProperty,
    ));

    execute(
        &ctx,
        &registry_over(injector),
        "MERGE INTO ice.sales.pt AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await
    .expect("a non-conflicting concurrent commit must not block the MERGE");

    // The MERGE ran on top of the concurrent property commit.
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.pt").await,
        vec![
            (1, "a".to_string()),
            (2, "bee".to_string()),
            (3, "c".to_string()),
            (4, "dee".to_string()),
        ],
    );
    let handle = loaded_table(&catalogs, "pt").await;
    assert_eq!(
        handle
            .metadata()
            .properties()
            .get("injected.concurrent")
            .map(String::as_str),
        Some("1"),
        "the non-conflicting concurrent property commit must have survived the race"
    );
}

// Non-identity transform routing uses the shared computed partition path and retains OCC.

/// The distinct partition slots, formatted.
async fn partition_slot_strings(table: &Table) -> HashSet<String> {
    live_data_files(table)
        .await
        .iter()
        .map(|file| format!("{:?}", file.partition().fields().first().cloned().flatten()))
        .collect()
}

/// Register a `(id int, name string, ts timestamp)` source — the temporal-partition fixture.
fn register_ts_source(ctx: &SessionContext, name: &str, rows: &[(i32, &str, i64)]) {
    use datafusion::arrow::array::TimestampMicrosecondArray;
    use datafusion::arrow::datatypes::TimeUnit;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ctx.register_batch(name, batch).unwrap();
}

/// PIN R3a.
#[tokio::test]
async fn merge_truncate_partitioned_reroutes_and_inserts() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "tbase", &[(1, "apple"), (2, "cherry")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.trm USING iceberg PARTITIONED BY (truncate(2, name)) AS \
             SELECT * FROM tbase",
    )
    .await;
    // id=1 "apple"(ap) → "berry"(be): a cross-prefix MOVE; id=3 "cocoa"(co): a new-prefix insert.
    register_source(&ctx, "updates", &[(1, "berry"), (3, "cocoa")]);
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.trm AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.trm").await,
        vec![
            (1, "berry".to_string()),
            (2, "cherry".to_string()),
            (3, "cocoa".to_string()),
        ],
    );
    let handle = loaded_table(&catalogs, "trm").await;
    let slots = partition_slot_strings(&handle).await;
    let expected: HashSet<String> = ["be", "ch", "co"]
        .iter()
        .map(|p| format!("{:?}", Some(Literal::string(*p))))
        .collect();
    assert_eq!(
        slots, expected,
        "truncate(2) survivor MOVED to `be` + insert `co`, `ch` untouched: got {slots:?}"
    );
}

/// PIN R3b.
#[tokio::test]
async fn merge_days_partitioned_upsert() {
    const DAY: i64 = 86_400_000_000;
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_ts_source(&ctx, "dbase", &[(1, "a", 0), (2, "b", DAY)]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.dym USING iceberg PARTITIONED BY (days(ts)) AS \
             SELECT * FROM dbase",
    )
    .await;
    // id=1 matched → name "A" (ts unchanged → stays day0); id=3 not matched → insert day2.
    register_ts_source(&ctx, "updates", &[(1, "A", 0), (3, "c", 2 * DAY)]);
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.dym AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name, ts) VALUES (s.id, s.name, s.ts)",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.dym").await,
        vec![
            (1, "A".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
        ],
    );
    let handle = loaded_table(&catalogs, "dym").await;
    assert_eq!(
        partition_slot_strings(&handle).await.len(),
        3,
        "days(ts) routes the rewrite + insert into 3 distinct day partitions"
    );
}

/// PIN R4.
#[tokio::test]
async fn merge_bucket_partitioned_rewrite_arm_rejects_conflicting_concurrent_append() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pt USING iceberg PARTITIONED BY (bucket(4, id)) AS \
             SELECT * FROM src",
    )
    .await;

    let inner = catalogs["ice"].clone();
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "pt".to_string());
    let injector: Arc<dyn Catalog> = Arc::new(ConflictInjector::new(
        inner,
        ident,
        ConcurrentOp::ConflictingAppend,
    ));

    let error = execute(
        &ctx,
        &registry_over(injector),
        "MERGE INTO ice.sales.pt AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await
    .expect_err(
        "the transform-partitioned rewrite-arm MERGE must reject the conflicting \
             concurrent add",
    );
    assert!(
        error.to_string().contains("Found conflicting files"),
        "must be the serializable added-data conflict on the transform path, got: {error}"
    );
}

// Transform-partitioned MERGE exercises computed fanout and serializable OCC.

/// The live DELETE-file entries in the current snapshot's DELETE manifests.
async fn live_delete_files(table: &Table) -> Vec<DataFile> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Vec::new();
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("load manifest list");
    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("load delete manifest");
        for entry in manifest.entries() {
            if entry.is_alive() {
                files.push(entry.data_file().clone());
            }
        }
    }
    files
}

/// A file's single partition slot, formatted.
fn slot_string(file: &DataFile) -> String {
    format!("{:?}", file.partition().fields().first().cloned().flatten())
}

/// PIN Y3.
#[tokio::test]
async fn merge_days_partitioned_mor_delete_and_insert() {
    const DAY: i64 = 86_400_000_000;
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_ts_source(&ctx, "dbase", &[(1, "a", 0), (2, "b", DAY)]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.dymor USING iceberg PARTITIONED BY (days(ts)) \
             TBLPROPERTIES('write.merge.mode' = 'merge-on-read') AS SELECT * FROM dbase",
    )
    .await;

    let handle = loaded_table(&catalogs, "dymor").await;
    let files_before = live_data_files(&handle).await;
    let paths_before: HashSet<String> = files_before
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();
    assert_eq!(
        paths_before.len(),
        2,
        "day 0 and day 1 each get a data file"
    );
    // The day-0 file is the one holding id=1.
    let day0_path = id_file_pairs(&catalogs, "dymor")
        .await
        .into_iter()
        .find(|(id, _)| *id == 1)
        .expect("id=1 is present before the MERGE")
        .1;
    let day0_slot = slot_string(
        files_before
            .iter()
            .find(|file| file.file_path() == day0_path)
            .expect("the scanned `_file` is a live data file"),
    );

    // id=1 (day 0) is deleted; id=3 (day 2) is inserted.
    register_ts_source(&ctx, "updates", &[(1, "x", 0), (3, "c", 2 * DAY)]);
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.dymor AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN DELETE \
             WHEN NOT MATCHED THEN INSERT (id, name, ts) VALUES (s.id, s.name, s.ts)",
    )
    .await;

    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.dymor").await,
        vec![(2, "b".to_string()), (3, "c".to_string())],
        "the scan applies the position delete on a days(ts)-partitioned table (fork R117)"
    );

    let handle = loaded_table(&catalogs, "dymor").await;
    let files_after = live_data_files(&handle).await;
    let paths_after: HashSet<String> = files_after
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();
    assert!(
        paths_before.is_subset(&paths_after),
        "merge-on-read must leave every pre-merge data file live: {paths_before:?} vs \
             {paths_after:?}"
    );
    let new_slots: Vec<String> = files_after
        .iter()
        .filter(|file| !paths_before.contains(file.file_path()))
        .map(slot_string)
        .collect();
    assert_eq!(
        new_slots.len(),
        1,
        "the not-matched INSERT writes exactly one new data file"
    );

    let deletes = live_delete_files(&handle).await;
    assert_eq!(
        deletes.len(),
        1,
        "exactly one position-delete file committed"
    );
    let delete_slot = slot_string(&deletes[0]);
    assert_eq!(
        delete_slot, day0_slot,
        "the delete file must carry the TRANSFORMED day partition of the data file it \
             deletes from"
    );
    assert_ne!(
        delete_slot, new_slots[0],
        "…and NOT the day the INSERT created — the stamp follows the deleted row's file"
    );
}

/// PIN Y6 — the SERIALIZABLE OCC posture is still ARMED on the merge-on-read × transform path.
#[tokio::test]
async fn merge_bucket_partitioned_mor_rejects_conflicting_concurrent_append() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "updates", &[(2, "bee"), (4, "dee")]);
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ptmor USING iceberg PARTITIONED BY (bucket(4, id)) \
             TBLPROPERTIES('write.merge.mode' = 'merge-on-read') AS SELECT * FROM src",
    )
    .await;

    let inner = catalogs["ice"].clone();
    let ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "ptmor".to_string(),
    );
    let injector: Arc<dyn Catalog> = Arc::new(ConflictInjector::new(
        inner,
        ident,
        ConcurrentOp::ConflictingAppend,
    ));

    let error = execute(
        &ctx,
        &registry_over(injector),
        "MERGE INTO ice.sales.ptmor AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .await
    .expect_err(
        "the transform-partitioned merge-on-read MERGE must reject the conflicting \
             concurrent add",
    );
    assert!(
        error.to_string().contains("Found conflicting files"),
        "must be the serializable added-data conflict on the merge-on-read transform \
             path, got: {error}"
    );
}
