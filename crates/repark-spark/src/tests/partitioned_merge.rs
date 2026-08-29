/// ===========================================================================================
/// WG-1 pins identity-partitioned `MERGE INTO`: both COW and insert-only arms preserve manifest
/// partition values, plan pruning, and Arrow-path value/type round trips.
/// ===========================================================================================
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

/// The single identity partition slot of a `DataFile` as an int — the tables here all
/// partition by the non-null `id` column, so a null or non-int slot is a hard test failure.
fn slot_int(file: &DataFile) -> i32 {
    match file.partition().fields().first().cloned().flatten() {
        Some(Literal::Primitive(PrimitiveLiteral::Int(key))) => key,
        other => panic!("partition slot must be a non-null int literal, got {other:?}"),
    }
}

/// Map partition-slot value → total manifest record count across that partition's files —
/// the manifest-level proof that every committed file carries the right partition value.
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

/// WG1-P1 — mixed MERGE (matched-UPDATE + not-matched-INSERT) on a single-key
/// identity-partitioned table: the matched row is rewritten IN its partition and the
/// not-matched row is inserted into a NEW partition, both carrying correct manifest
/// partition values; the whole table round-trips. Risk: the fanout is bypassed on the
/// MERGE path (as `write_data_files` does — empty partition struct), so the rewritten /
/// inserted files land unpartitioned or in the wrong partition — readable today,
/// unprunable and spec-corrupting forever.
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

    // The matched row (id=2) took the source value; the not-matched row (id=4) inserted;
    // untouched partitions (1, 3) survived.
    assert_eq!(
        table_rows(&ctx, &catalogs, "ice.sales.pt").await,
        vec![
            (1, "a".to_string()),
            (2, "bee".to_string()),
            (3, "c".to_string()),
            (4, "dee".to_string()),
        ],
    );

    // Every committed file — the REWRITTEN id=2 file AND the INSERTED id=4 file — carries
    // its own partition value at the manifest level.
    assert_eq!(
        slot_record_counts(&catalogs, "pt").await,
        BTreeMap::from([(1, 1), (2, 1), (3, 1), (4, 1)]),
        "one record per partition slot 1..4 (rewrite + insert both correctly partitioned)"
    );

    // The inserted row prunes to exactly the new partition's file (plan level); ditto the
    // rewritten row.
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

/// WG1-P2 — insert-only MERGE on a partitioned table: the not-matched row fans out into its
/// partition (correct manifest value), matched-but-unclaused rows are untouched, and every
/// pre-existing partition file survives (insert-only never rewrites). Risk: the insert-only
/// arm appends through the unpartitioned writer, so the new file has an empty partition
/// struct.
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

/// WG1-P3 — a MERGE whose source rows span MULTIPLE partitions in UNSORTED order: the
/// fanout regroups per partition (updates across partitions 1/2/3 + an insert into 5),
/// every file lands in its own partition, and the table round-trips. Risk: a clustered
/// (sort-required) writer would hard-error on the unsorted multi-partition rewrite/insert
/// batch, or route rows to the wrong partition.
#[tokio::test]
async fn merge_partitioned_multi_partition_unsorted_source() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // Base table spans partitions 1..4; the update source is deliberately unsorted and
    // spans partitions 3, 1, 5, 2 (a mix of matched updates and a not-matched insert).
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

/// WG1-P4 — a matched UPDATE that CHANGES the partition key moves the row to the NEW
/// partition (Spark copy-on-write, fork `ENGINE_CONTRACT` §4 UPDATE/COW: "a
/// partition-key-changing UPDATE re-routes rows via the partition-aware writer"). The old
/// partition's file is rewritten away (no live file, empty prune) and the moved row lands
/// under the new key. Risk: the survivor is written back to the OLD partition (partition
/// value inferred from the file, not the row), silently corrupting the layout.
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

/// WG1-P8 — the `UPDATE SET *` / `INSERT *` star forms (the source publish job's MERGE
/// shape) on a partitioned target: star resolution feeds the fanout exactly like the
/// explicit-column forms, so partition values are still stamped. Risk: the star-expanded
/// batch loses a column or bypasses the partitioned writer.
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

// ===========================================================================================
// WG1-P5 — partitioned-MERGE optimistic-concurrency (per arm). The MERGE `commit` seam does
// NOT branch on partitioning, so the exhaustive add-vs-delete false-positive/rejection
// matrix in `repark_iceberg::write::merge::occ_tests` stays green as partition-agnostic regression.
// These pins prove the *identity-partitioned MERGE PATH* (parse → fanout → resolve → commit)
// still ARMS the serializable §5 validations end to end: a conflicting concurrent append
// arriving mid-commit is loudly rejected (both arms), while a genuinely non-conflicting
// concurrent commit on the same table is tolerated (the false-positive guard). Determinism
// is by an attempt counter (fork `ENGINE_CONTRACT` §5; lessons rule 12 — no timing).
// ===========================================================================================

/// The concurrent commit the injector lands mid-MERGE, INSIDE the victim's first
/// `update_table` (after the fork's `do_commit` refresh, before its CAS) — so the victim
/// refreshes to a base carrying it and re-runs the §5 validations against it.
#[derive(Clone, Copy, Debug)]
enum ConcurrentOp {
    /// Adds a data file → serializable `validate_no_conflicting_data` (`AlwaysTrue` filter)
    /// must reject the MERGE.
    ConflictingAppend,
    /// Sets a table property → a real CAS conflict + refresh, but NO added data, so the
    /// validation must NOT reject (the merge retries and commits): the false-positive guard.
    NonConflictingProperty,
}

/// A conforming one-row batch (`id`, `name`) — the injected competing append's payload. With
/// the MERGE's `AlwaysTrue` conflict filter, ANY concurrently-added data file conflicts, so
/// the specific id is irrelevant.
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

/// The boxed-future return type of an `#[async_trait]` `Catalog` method (this crate has no
/// `async-trait` dep, so the trait is implemented in its desugared form — every method
/// forwards the inner catalog's already-boxed future).
type BoxedCatalogFuture<'a, T> = Pin<Box<dyn Future<Output = iceberg::Result<T>> + Send + 'a>>;

/// A fully-delegating `Catalog` wrapper that lands one [`ConcurrentOp`] against the inner
/// catalog on the victim MERGE's FIRST `update_table` (mirrors `append.rs`'s injector —
/// deterministic, attempt-counter-keyed, no timing).
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
            // Take the op out and DROP the guard before any await (a `MutexGuard` is not
            // `Send`; `ConcurrentOp` is `Copy`).
            let op = if attempt == 1 {
                self.op
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
            } else {
                None
            };
            if let Some(op) = op {
                // The victim's `do_commit` has already refreshed its base; landing the
                // concurrent commit NOW (against the inner catalog — no recursion) puts it
                // between the MERGE's pinned snapshot and its CAS.
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

/// WG1-P5a — mixed (rewrite-arm) partitioned MERGE × a conflicting concurrent append: the
/// serializable `validate_no_conflicting_data` guard (armed on the rewrite arm since F-BR-1)
/// must LOUDLY reject the stale-pinned commit — a non-retryable data conflict — so the
/// concurrent add is never silently duplicated. Risk: the partitioned write path reaches
/// `commit` without the pin / validation armed, so a mid-flight add slips through.
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

/// WG1-P5b — insert-only partitioned MERGE × a conflicting concurrent append: the same
/// serializable guard armed on the add-only arm (BUG-005) must reject. Risk: only the
/// rewrite arm was rerouted through the armed commit and the insert-only arm appends blindly.
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

/// WG1-P5c — the false-positive guard: a NON-conflicting concurrent commit (a table-property
/// set — a real CAS conflict + refresh, but NO added data) must NOT trip the serializable
/// guard: the partitioned MERGE retries and commits, and the row result is correct. Risk:
/// an over-broad conflict filter rejects every concurrent commit, breaking liveness. This is
/// the GREEN half of the concurrency mutation proof (dropping `validate_no_conflicting_data`
/// reddens P5a/P5b while this stays green).
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

    // The MERGE ran on top of the concurrent property commit, with the right rows AND
    // the concurrent property still present (proving it really raced through the CAS).
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

/// The distinct partition slots (first field), formatted — for non-int transform slots
/// (string truncate, temporal date/int) where `slot_int` does not apply.
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

/// PIN R3a — MERGE into a `truncate(2, name)` table: a matched UPDATE that changes `name`
/// ACROSS the truncate boundary re-routes the survivor to the NEW prefix partition, and a
/// not-matched INSERT lands in its own prefix. Manifest slots are the 2-char string prefixes
/// (Iceberg string truncate), and the table round-trips. Restoring the non-identity gate in
/// `reject_unsupported` → the MERGE returns `NotImplemented` → RED.
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
    // id=1 "apple"(ap) → "berry"(be): a cross-prefix MOVE; id=3 "cocoa"(co): a new-prefix
    // insert. id=2 "cherry"(ch) is untouched.
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

/// PIN R3b — MERGE into a `days(ts)` (temporal) table: a matched UPDATE (name only, `ts`
/// unchanged so the survivor stays in its day) rewrites in-partition and a not-matched INSERT
/// lands in a NEW day partition; the temporal transform drives placement (3 distinct day
/// slots), and the table round-trips. Restoring the non-identity gate → RED.
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

/// PIN R4 — the serializable OCC guard is still ARMED on the NON-identity transform write
/// path: a mixed (rewrite-arm) MERGE into a `bucket(4, id)` table, raced by a conflicting
/// concurrent append arrives mid-commit, it is LOUDLY rejected (non-retryable
/// `validate_no_conflicting_data`). Removing the transform gate must not have exposed an
/// unvalidated append. Mirrors the identity WG1-P5a on a transform-partitioned table; the
/// `commit` seam is partition-agnostic, so the same guard fires. Dropping
/// `validate_no_conflicting_data` on the MERGE commit reddens this exactly as it reddens P5a.
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

/// The live (Added/Existing) DELETE-file entries in the current snapshot's DELETE manifests
/// — the manifest-level oracle for "this really was a merge-on-read commit", plus the
/// partition stamp each delete file carries.
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

/// A file's single partition slot, formatted — works for every transform's literal type
/// (int bucket ordinal, string truncate prefix, date/int temporal), and for the NULL slot.
fn slot_string(file: &DataFile) -> String {
    format!("{:?}", file.partition().fields().first().cloned().flatten())
}

/// PIN Y3 — a `days(ts)` TEMPORAL transform-partitioned table under
/// `write.merge.mode = 'merge-on-read'`, end to end through SQL: a matched DELETE and a
/// not-matched INSERT in one MERGE. Temporal is the transform family whose partition value
/// is neither the source value (unlike identity) nor an ordinal derived from a hash (unlike
/// bucket) — a date ordinal — so it is a genuinely independent instance of "the stamp is
/// the file's own TRANSFORMED partition".
///
/// The discriminating assertions: the committed delete file's partition slot must equal the
/// slot of the DATA FILE the deleted row lives in (day 0), NOT the day the INSERT created
/// (day 2) and not an empty/default slot; every pre-merge data file survives; and the
/// insert lands in its own new day partition. Restoring the transform gate → the MERGE
/// raises `NotImplemented` and `run` panics ⇒ RED.
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
    // The day-0 file is the one holding id=1 — resolved through the scan's `_file` column,
    // so the expected stamp is READ from the fixture rather than assumed.
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

    // id=1 (day 0) is deleted; id=3 (day 2) is inserted. id=2 (day 1) is untouched.
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

/// PIN Y6 — the SERIALIZABLE OCC posture is still ARMED on the merge-on-read × transform
/// path. A `bucket(4, id)` + `merge-on-read` MERGE, raced by a conflicting concurrent
/// append arrives mid-commit, it is LOUDLY rejected. Two gates could have quietly disarmed
/// here and neither may: dropping the transform gate must not have exposed an unvalidated
/// row-delta, and the `RowDelta` commit's `validate_no_conflicting_data_files` (the
/// merge-on-read analogue of R4's `validate_no_conflicting_data`) must fire on a
/// transform-partitioned target exactly as it does on an unpartitioned one — the commit
/// seam is partition-agnostic, and this pin holds that to execution.
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
