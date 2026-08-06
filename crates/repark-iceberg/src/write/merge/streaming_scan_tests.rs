use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Array, Int32Array, Int64Array};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionConfig;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{DataContentType, NestedField, PrimitiveType, Schema, Type};
use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation};

use crate::write::position_delete::PositionDeletePair;
use tempfile::TempDir;

use super::*;

/// A [`PartitionStream`] over a scripted batch list that counts how many batches it has
/// PRODUCED (yielded on a poll) — the streaming-target analogue of the F-BR-4
/// `CountingPartitionStream`. Proves `register_streaming_target` wires a LAZY `StreamingTable`
/// (nothing produced until a query pulls) instead of collecting the whole target up front.
#[derive(Debug)]
struct ScriptedTargetStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    produced: Arc<AtomicUsize>,
}

impl PartitionStream for ScriptedTargetStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let produced = Arc::clone(&self.produced);
        let items: Vec<Result<RecordBatch>> = self.batches.iter().cloned().map(Ok).collect();
        // `inspect` fires per yielded item — the "batches produced so far" counter.
        let counted = futures::stream::iter(items).inspect(move |_batch| {
            produced.fetch_add(1, Ordering::SeqCst);
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            counted,
        ))
    }
}

/// A `[id Int32, _file Utf8, _pos Int64]` batch — the scratch-target shape.
fn scripted_batch(schema: &SchemaRef, ids: &[i32], file: &str, pos: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec![file; ids.len()])),
            Arc::new(Int64Array::from(pos.to_vec())),
        ],
    )
    .expect("scripted batch builds")
}

/// The scalar `count(*)` value across the returned batches.
fn count_value(rows: &[RecordBatch]) -> i64 {
    rows.iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count(*) is Int64")
                .values()
                .to_vec()
        })
        .sum()
}

/// PIN K-MEM (OTH-001/SAF-001) — `register_streaming_target` wires a LAZY, re-scannable
/// `StreamingTable`, so the whole target is NEVER collected up front. A scripted source counts
/// every batch it PRODUCES: immediately after registration the count is 0 (nothing scanned),
/// each query streams every batch, and a SECOND query RE-SCANS (the count climbs again — a
/// materialized `MemTable` is built once and never re-pulls). Mutation M-K-MEM: reverting
/// `register_streaming_target` to collect the source into a `MemTable` drains all N batches at
/// registration ⇒ `produced == N` at the first assert ⇒ RED (a materialized full-target copy is
/// exactly the residency this unit removes). Deterministic (rule 12): synchronous atomic reads,
/// `target_partitions = 1` so no `RepartitionExec` pulls the source on a background task.
#[tokio::test]
async fn register_streaming_target_is_lazy_and_rescannable() {
    let produced = Arc::new(AtomicUsize::new(0));
    let schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(FILE_PATH_COL, DataType::Utf8, false),
        Field::new(POS_COL, DataType::Int64, false),
    ]));
    let batches = vec![
        scripted_batch(&schema, &[1], "f1", &[0]),
        scripted_batch(&schema, &[2], "f2", &[0]),
        scripted_batch(&schema, &[3], "f3", &[0]),
    ];
    let n = batches.len();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let source = Arc::new(ScriptedTargetStream {
        schema: Arc::clone(&schema),
        batches,
        produced: Arc::clone(&produced),
    });
    let name = register_streaming_target(&ctx, Arc::clone(&schema), source).unwrap();
    // Structural bind (review 2026-07-23, K-S2): the registered provider MUST be a
    // StreamingTable, never a MemTable — a downcast that fails the instant anyone swaps the
    // registration back to a materializing provider (the O(target-rows) residency class).
    let provider = ctx.table_provider(name.as_str()).await.unwrap();
    assert!(
        provider.as_ref().is::<StreamingTable>(),
        "target must be registered as a lazy StreamingTable, not a materialized provider"
    );

    assert_eq!(
        produced.load(Ordering::SeqCst),
        0,
        "registration must NOT collect the target up front (the StreamingTable is lazy) — a \
         collect-then-MemTable registration would have produced {n} batches here"
    );

    let rows = ctx
        .sql(&format!("SELECT count(*) AS c FROM \"{name}\""))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        count_value(&rows),
        i64::try_from(n).expect("batch count fits i64"),
        "every row streams through the query"
    );
    assert_eq!(
        produced.load(Ordering::SeqCst),
        n,
        "the first scan produced every batch on demand"
    );

    ctx.sql(&format!("SELECT count(*) AS c FROM \"{name}\""))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        produced.load(Ordering::SeqCst),
        2 * n,
        "a second query RE-SCANS the streaming target (a MemTable would not re-pull)"
    );
}

/// An in-memory Iceberg catalog over a local-FS warehouse with a `sales` namespace.
async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("build memory catalog"),
    );
    catalog
        .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
        .await
        .expect("create namespace");
    catalog
}

/// Create `sales.<name>` with `id int` (required) + `v string` (optional), unpartitioned.
async fn create_target(catalog: &Arc<dyn Catalog>, name: &str) -> TableIdent {
    create_target_with(catalog, name, HashMap::new()).await
}

/// [`create_target`] with table properties — the seam the merge-on-read pins use to set
/// `write.merge.mode` (and the copy-on-write side of the T4 differential to set it explicitly).
async fn create_target_with(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    properties: HashMap<String, String>,
) -> TableIdent {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build target schema");
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .properties(properties)
        .build();
    catalog
        .create_table(&NamespaceIdent::new("sales".to_string()), creation)
        .await
        .expect("create target table");
    TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string())
}

/// A consumer batch: `id Int32` (non-null) + `v Utf8` (nullable) — plain Arrow, no field ids.
fn consumer_batch(ids: &[i32], vs: &[Option<&str>]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vs.to_vec())),
        ],
    )
    .expect("consumer batch builds")
}

/// Append `batch` as its OWN data file (one `fast_append` commit) — separate appends produce
/// separate files, so `_pos` (per-file ordinal) recurs across them.
async fn append_file(catalog: &Arc<dyn Catalog>, ident: &TableIdent, batch: RecordBatch) {
    crate::write::append::append(catalog, ident, vec![batch])
        .await
        .expect("append a data file");
}

/// Register the MERGE source as a `src` `MemTable` in `ctx` (the generated SQL references it).
fn register_source(ctx: &SessionContext, ids: &[i32], vs: &[Option<&str>]) {
    let batch = consumer_batch(ids, vs);
    let source = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("source memtable");
    ctx.register_table("src", Arc::new(source))
        .expect("register src");
}

/// Read the target back on the Arrow scan path: `(id, v)` rows, sorted (order-insensitive).
async fn read_back(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<(i32, Option<String>)> {
    let table = catalog.load_table(ident).await.expect("load table");
    let scan = table
        .scan()
        .select(["id", "v"])
        .build()
        .expect("build scan");
    let batches: Vec<RecordBatch> = scan
        .to_arrow()
        .await
        .expect("scan to_arrow")
        .try_collect()
        .await
        .expect("collect scan batches");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id Int32");
        let vs = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("v Utf8");
        for row in 0..batch.num_rows() {
            let v = (!vs.is_null(row)).then(|| vs.value(row).to_string());
            rows.push((ids.value(row), v));
        }
    }
    rows.sort();
    rows
}

/// A `MERGE INTO sales.<name> AS t USING src AS s ON t.id = s.id` spec.
fn merge_spec(
    name: &str,
    matched: Vec<MatchedClause>,
    not_matched: Vec<InsertClause>,
) -> MergeSpec {
    MergeSpec {
        target: TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string()),
        target_alias: "t".to_string(),
        source_from_sql: "src".to_string(),
        source_alias: "s".to_string(),
        on_sql: "t.id = s.id".to_string(),
        matched,
        not_matched,
    }
}

/// `WHEN MATCHED THEN UPDATE SET <col> = <expr>`.
fn update_set(col: &str, expr: &str) -> MatchedClause {
    MatchedClause {
        predicate_sql: None,
        action: MatchedAction::Update {
            assignments: vec![(col.to_string(), expr.to_string())],
        },
    }
}

/// `WHEN NOT MATCHED THEN INSERT (<cols>) VALUES (<vals>)`.
fn insert_values(cols: &[&str], vals: &[&str]) -> InsertClause {
    InsertClause {
        predicate_sql: None,
        action: InsertAction::Explicit {
            columns: cols.iter().map(ToString::to_string).collect(),
            values_sql: vals.iter().map(ToString::to_string).collect(),
        },
    }
}

/// PIN K-IDENTITY (NOVEL target≫source, multi-file) — a streamed, RE-SCANNED target keeps
/// `(_file, _pos)` row identity exact across data files. Three separate appends put rows at
/// `_pos = 0` in THREE different files (id1@f1/0, id3@f2/0, id5@f3/0); the MERGE updates id1 and
/// id3 (same `_pos`, different files) and inserts id99 — every row lands correctly, so a
/// `_pos`-alone identity (which would conflate the three `_pos=0` rows and raise a spurious
/// cardinality violation) is provably wrong. This is the plan's target≫source multi-partition
/// NOVEL execution; results are row-for-row what the pre-change materialized engine produced.
#[tokio::test]
async fn merge_streams_multi_file_target_identity_holds_across_files() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "multi").await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2], &[Some("a"), Some("b")]),
    )
    .await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[3, 4], &[Some("c"), Some("d")]),
    )
    .await;
    append_file(&catalog, &ident, consumer_batch(&[5], &[Some("e")])).await;

    let ctx = SessionContext::new();
    register_source(&ctx, &[1, 3, 99], &[Some("A"), Some("C"), Some("Z")]);
    let spec = merge_spec(
        "multi",
        vec![update_set("v", "s.v")],
        vec![insert_values(&["id", "v"], &["s.id", "s.v"])],
    );
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("multi-file MERGE commits");

    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (1, Some("A".to_string())),  // f1/pos0 updated
            (2, Some("b".to_string())),  // f1/pos1 untouched
            (3, Some("C".to_string())),  // f2/pos0 updated — SAME _pos as id1, different file
            (4, Some("d".to_string())),  // f2/pos1 untouched
            (5, Some("e".to_string())),  // f3/pos0 untouched — SAME _pos again
            (99, Some("Z".to_string())), // inserted via the `_pos IS NULL` anti-join
        ],
    );
}

/// PERF-01 pin: COW Stage A discovery retains **O(files)** path strings, not O(matched rows).
///
/// Fixture: one data file, `n` matched rows, then `10×n` matched rows (same single file).
/// Driver path-`String` allocations (test-only counter at first-seen insert) must stay
/// `== 1` in both cases — bar: 10× rows ≲ 2× driver alloc. Knobs: default
/// `file-scoped-rewrite` + default `scan-pruning` (no custom `SessionConfig`).
#[tokio::test]
async fn cow_discovery_path_allocs_scale_with_files_not_matched_rows() {
    // --- n matched rows, 1 file ----------------------------------------------------------
    let counter_small = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "perf01").await;
    // 20 rows in ONE file; source matches all of them.
    let n = 20i32;
    let ids_small: Vec<i32> = (1..=n).collect();
    let values_small: Vec<Option<&str>> = (0..ids_small.len()).map(|_| Some("a")).collect();
    append_file(&catalog, &ident, consumer_batch(&ids_small, &values_small)).await;
    let ctx = SessionContext::new();
    let src_values: Vec<Option<&str>> = (0..ids_small.len()).map(|_| Some("A")).collect();
    register_source(&ctx, &ids_small, &src_values);
    let spec = merge_spec("perf01", vec![update_set("v", "s.v")], vec![]);
    let instruments_small = MergeTestInstruments {
        discovery_path_alloc: Some(std::sync::Arc::clone(&counter_small)),
        ..MergeTestInstruments::default()
    };
    MERGE_TEST_INSTRUMENTS
        .scope(instruments_small, async {
            execute_merge(&ctx, &catalog, &spec)
                .await
                .expect("small COW discovery MERGE");
        })
        .await;
    let allocs_small = counter_small.load(Ordering::SeqCst);
    assert_eq!(
        allocs_small, 1,
        "one data file → exactly one path String retained (got {allocs_small})"
    );

    // --- 10× rows, still 1 file ----------------------------------------------------------
    let counter_large = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let warehouse2 = TempDir::new().expect("temp warehouse 2");
    let catalog2 = memory_catalog(&warehouse2).await;
    let ident2 = create_target(&catalog2, "perf01b").await;
    let n_large = n * 10;
    let ids_large: Vec<i32> = (1..=n_large).collect();
    let values_large: Vec<Option<&str>> = (0..ids_large.len()).map(|_| Some("a")).collect();
    append_file(
        &catalog2,
        &ident2,
        consumer_batch(&ids_large, &values_large),
    )
    .await;
    let ctx2 = SessionContext::new();
    let src_values_large: Vec<Option<&str>> = (0..ids_large.len()).map(|_| Some("A")).collect();
    register_source(&ctx2, &ids_large, &src_values_large);
    let spec2 = merge_spec("perf01b", vec![update_set("v", "s.v")], vec![]);
    let instruments_large = MergeTestInstruments {
        discovery_path_alloc: Some(std::sync::Arc::clone(&counter_large)),
        ..MergeTestInstruments::default()
    };
    MERGE_TEST_INSTRUMENTS
        .scope(instruments_large, async {
            execute_merge(&ctx2, &catalog2, &spec2)
                .await
                .expect("10× COW discovery MERGE");
        })
        .await;
    let allocs_large = counter_large.load(Ordering::SeqCst);
    assert_eq!(
        allocs_large, 1,
        "10× rows in one file still one path String (got {allocs_large})"
    );
    // Bar Q18: 10× rows ≲ 2× driver alloc (here both are 1 ⇒ 1× ≤ 2×).
    assert!(
        // 10x rows must stay within 2x driver path allocations (O(files), not O(rows)).
        allocs_large as u64 <= allocs_small as u64 * 2,
        "10× rows must not grow driver path allocs beyond 2× (small={allocs_small}, large={allocs_large})"
    );
    // Correctness: all rows updated (sample endpoints).
    let rows = read_back(&catalog2, &ident2).await;
    assert_eq!(rows.len(), ids_large.len());
    assert_eq!(rows[0], (1, Some("A".to_string())));
    assert_eq!(rows[ids_large.len() - 1], (n_large, Some("A".to_string())));
}

/// R-MERGE-ONEPASS Stage B + PERF-19 pin: `MoR` upsert (MATCHED UPDATE + NOT MATCHED INSERT)
/// issues exactly **two** logical target-SQL consumptions — `matched_work` (discovery+updates)
/// + insert anti-join — not three (pre-Stage-B) or four (pre-Stage-A).
///
/// Counts repark `stream_sql` calls (plan-level), **not**
/// `PartitionStream::execute` (flaky under DF re-plans / parallel cargo tests — Q16).
#[tokio::test]
async fn mor_upsert_target_scan_pass_count_is_two() {
    let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target_with(&catalog, "pass2", mor_props()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2, 3], &[Some("a"), Some("b"), Some("c")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_source(&ctx, &[2, 99], &[Some("B"), Some("Z")]);
    let spec = merge_spec(
        "pass2",
        vec![update_set("v", "s.v")],
        vec![insert_values(&["id", "v"], &["s.id", "s.v"])],
    );
    let instruments = MergeTestInstruments {
        logical_pass: Some(std::sync::Arc::clone(&counter)),
        ..MergeTestInstruments::default()
    };
    MERGE_TEST_INSTRUMENTS
        .scope(instruments, async {
            execute_merge(&ctx, &catalog, &spec)
                .await
                .expect("MoR upsert for pass count");
        })
        .await;
    let passes = counter.load(Ordering::SeqCst);
    // Exact: Stage B MoR upsert = matched_work stream + one insert anti-join stream.
    assert_eq!(
        passes, 2,
        "MoR upsert must issue exactly two logical target-SQL passes (matched_work + insert); got {passes}"
    );
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (1, Some("a".to_string())),
            (2, Some("B".to_string())),
            (3, Some("c".to_string())),
            (99, Some("Z".to_string())),
        ],
    );
}

/// PERF-04 pin: COW equi-key upsert with default scan-pruning (residual on) + default
/// file-scoped rewrite keeps co-located survivors. Reproduces the R-PERF-MERGE-PRUNE
/// hazard class (matched id=10 in a file that also holds id=11) without lost rows —
/// residual is on the primary discovery/insert scan only; rewrite is whole-file scoped.
///
/// Critic-octo C1-Q-001: also pins that residual **pushed** (task-local `residual_push` == 1).
/// Reverting `residual_join_key_filter` to always `None` keeps row asserts green but fails
/// the push counter — mutation-proof for the PERF-04 shipping claim.
#[tokio::test]
async fn cow_equi_key_residual_keeps_colocated_survivors() {
    let push_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "residual_surv").await;
    // File A: {1,2} far from source keys; File B: {10,11} — update only 10, survivor 11.
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2], &[Some("a"), Some("b")]),
    )
    .await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[10, 11], &[Some("x"), Some("y")]),
    )
    .await;
    // Default SessionConfig: scan-pruning=true, file-scoped-rewrite=true.
    let ctx = SessionContext::new();
    register_source(&ctx, &[10], &[Some("X")]);
    let spec = merge_spec("residual_surv", vec![update_set("v", "s.v")], vec![]);
    let instruments = MergeTestInstruments {
        residual_push: Some(std::sync::Arc::clone(&push_counter)),
        ..MergeTestInstruments::default()
    };
    MERGE_TEST_INSTRUMENTS
        .scope(instruments, async {
            execute_merge(&ctx, &catalog, &spec)
                .await
                .expect("COW residual equi-key MERGE");
        })
        .await;
    assert_eq!(
        push_counter.load(Ordering::SeqCst),
        1,
        "PERF-04: equi Int32 ON under default knobs must push residual (got 0 ⇒ filter None)"
    );
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (1, Some("a".to_string())),
            (2, Some("b".to_string())),
            (10, Some("X".to_string())),
            (11, Some("y".to_string())), // co-located survivor must survive residual
        ],
    );
}

/// PERF-04 pin: `MoR` equi-key upsert under residual still updates + inserts correctly
/// (unmatched target rows outside source key range stay; not-matched source inserts).
/// C1-Q-001: residual push counter must fire for `MoR` equi as well.
#[tokio::test]
async fn mor_equi_key_residual_upsert_correct() {
    let push_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target_with(&catalog, "mor_residual", mor_props()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2, 100], &[Some("a"), Some("b"), Some("far")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_source(&ctx, &[2, 99], &[Some("B"), Some("Z")]);
    let spec = merge_spec(
        "mor_residual",
        vec![update_set("v", "s.v")],
        vec![insert_values(&["id", "v"], &["s.id", "s.v"])],
    );
    let instruments = MergeTestInstruments {
        residual_push: Some(std::sync::Arc::clone(&push_counter)),
        ..MergeTestInstruments::default()
    };
    MERGE_TEST_INSTRUMENTS
        .scope(instruments, async {
            execute_merge(&ctx, &catalog, &spec)
                .await
                .expect("MoR residual equi-key upsert");
        })
        .await;
    assert_eq!(
        push_counter.load(Ordering::SeqCst),
        1,
        "PERF-04: MoR equi Int32 ON must push residual"
    );
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (1, Some("a".to_string())),
            (2, Some("B".to_string())),
            (99, Some("Z".to_string())),
            (100, Some("far".to_string())),
        ],
    );
}

/// PERF-04 mode-gate pin: COW + `file-scoped-rewrite=false` must **not** push residual
/// (R-PERF-MERGE-PRUNE STOP) while still keeping co-located survivors. Mutation: drop the
/// mode gate → residual pushes and survivors outside `[min_s,max_s]` are dropped on path-semijoin
/// rewrite through the primary → rows pin RED *or* push counter is non-zero here.
#[tokio::test]
async fn cow_file_scoped_off_does_not_push_residual() {
    let push_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "residual_gate").await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[10, 11], &[Some("x"), Some("y")]),
    )
    .await;
    let config = repark_write_scan_prune_file_scoped_off();
    let ctx = SessionContext::new_with_config(config);
    register_source(&ctx, &[10], &[Some("X")]);
    let spec = merge_spec("residual_gate", vec![update_set("v", "s.v")], vec![]);
    let instruments = MergeTestInstruments {
        residual_push: Some(std::sync::Arc::clone(&push_counter)),
        ..MergeTestInstruments::default()
    };
    MERGE_TEST_INSTRUMENTS
        .scope(instruments, async {
            execute_merge(&ctx, &catalog, &spec)
                .await
                .expect("COW file-scoped-off MERGE");
        })
        .await;
    assert_eq!(
        push_counter.load(Ordering::SeqCst),
        0,
        "COW + file-scoped OFF must not push residual (R-PERF-MERGE-PRUNE STOP)"
    );
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(10, Some("X".to_string())), (11, Some("y".to_string())),],
    );
}

/// PERF-04 conf pin (critic-octo C4-Q-001): `repark.merge.scan-pruning=false` must not push
/// residual even for equi Int32 ON. Mutation: drop the `scan_pruning_from_ctx` gate → push==1.
#[tokio::test]
async fn scan_pruning_false_does_not_push_residual() {
    let push_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "scan_prune_off").await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[10, 11], &[Some("x"), Some("y")]),
    )
    .await;
    let config = crate::write::scan_prune::with_scan_pruning(SessionConfig::new(), false);
    let ctx = SessionContext::new_with_config(config);
    register_source(&ctx, &[10], &[Some("X")]);
    let spec = merge_spec("scan_prune_off", vec![update_set("v", "s.v")], vec![]);
    let instruments = MergeTestInstruments {
        residual_push: Some(std::sync::Arc::clone(&push_counter)),
        ..MergeTestInstruments::default()
    };
    MERGE_TEST_INSTRUMENTS
        .scope(instruments, async {
            execute_merge(&ctx, &catalog, &spec)
                .await
                .expect("scan-pruning=false MERGE");
        })
        .await;
    assert_eq!(
        push_counter.load(Ordering::SeqCst),
        0,
        "scan-pruning=false must not push residual"
    );
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(10, Some("X".to_string())), (11, Some("y".to_string()))],
    );
}

/// R-MERGE-FILE-SCAN pin: multi-file COW where only file B is affected — survivors
/// co-located in B are preserved (P2-shape), files A/C untouched, and escape hatch
/// `file-scoped-rewrite=false` yields the same rows. Task-count filtering is unit-pinned
/// in `file_scoped_rewrite::filter_tasks_keeps_only_allowlisted_paths`.
#[tokio::test]
async fn cow_file_scoped_rewrite_opens_only_affected_files_and_keeps_survivors() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "scoped").await;
    // Three files: A={1,2}, B={10,11}, C={20}. Source updates id=10 only → only B affected.
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2], &[Some("a"), Some("b")]),
    )
    .await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[10, 11], &[Some("x"), Some("y")]),
    )
    .await;
    append_file(&catalog, &ident, consumer_batch(&[20], &[Some("z")])).await;

    // --- file-scoped path (default conf true) -------------------------------------------
    let ctx = SessionContext::new();
    register_source(&ctx, &[10], &[Some("X")]);
    let spec = merge_spec("scoped", vec![update_set("v", "s.v")], vec![]);
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("file-scoped COW MERGE");
    let scoped_rows = read_back(&catalog, &ident).await;
    assert_eq!(
        scoped_rows,
        vec![
            (1, Some("a".to_string())),
            (2, Some("b".to_string())),
            (10, Some("X".to_string())), // updated in file B
            (11, Some("y".to_string())), // survivor co-located in file B
            (20, Some("z".to_string())), // untouched file C
        ],
    );

    // --- escape hatch: conf false still correct -----------------------------------------
    let warehouse2 = TempDir::new().expect("temp warehouse 2");
    let catalog2 = memory_catalog(&warehouse2).await;
    let ident2 = create_target(&catalog2, "scoped2").await;
    append_file(
        &catalog2,
        &ident2,
        consumer_batch(&[1, 2], &[Some("a"), Some("b")]),
    )
    .await;
    append_file(
        &catalog2,
        &ident2,
        consumer_batch(&[10, 11], &[Some("x"), Some("y")]),
    )
    .await;
    append_file(&catalog2, &ident2, consumer_batch(&[20], &[Some("z")])).await;
    let config = repark_write_scan_prune_file_scoped_off();
    let ctx2 = SessionContext::new_with_config(config);
    register_source(&ctx2, &[10], &[Some("X")]);
    let spec2 = merge_spec("scoped2", vec![update_set("v", "s.v")], vec![]);
    execute_merge(&ctx2, &catalog2, &spec2)
        .await
        .expect("full-scan COW MERGE with hatch off");
    assert_eq!(read_back(&catalog2, &ident2).await, scoped_rows);
}

/// Scout #18 survivor-row pin: multi-clause first-match (`clause_id` CASE) on COW + `MoR`.
///
/// Target: id∈{1,2,3} with v=`old`. Source carries `flag` that steers clause selection:
/// - clause 0: `s.flag = 1` → UPDATE v = 'first'
/// - clause 1: unconditional → UPDATE v = 'second'  (must NOT win when flag=1)
/// - unmatched id=3 is a co-located survivor (same data file) and stays `old`
/// - `MoR` twin must scan-equal the COW result (first-match + survivors bit-identical)
#[tokio::test]
async fn multi_clause_first_match_survivors_cow_and_mor() {
    let matched = vec![
        MatchedClause {
            predicate_sql: Some("s.flag = 1".to_string()),
            action: MatchedAction::Update {
                assignments: vec![("v".to_string(), "'first'".to_string())],
            },
        },
        MatchedClause {
            predicate_sql: None,
            action: MatchedAction::Update {
                assignments: vec![("v".to_string(), "'second'".to_string())],
            },
        },
    ];
    let expected = vec![
        (1, Some("first".to_string())),  // flag=1 → clause 0
        (2, Some("second".to_string())), // flag=0 → clause 1 (first did not apply)
        (3, Some("old".to_string())),    // unmatched survivor in same file
    ];

    // --- COW -----------------------------------------------------------------------------
    let warehouse_cow = TempDir::new().expect("temp warehouse cow");
    let catalog_cow = memory_catalog(&warehouse_cow).await;
    let ident_cow = create_target_with(&catalog_cow, "fm_cow", cow_props()).await;
    append_file(
        &catalog_cow,
        &ident_cow,
        consumer_batch(&[1, 2, 3], &[Some("old"), Some("old"), Some("old")]),
    )
    .await;
    let ctx_cow = SessionContext::new();
    // Source schema: id + flag (no v — SET uses literals).
    let source_batch = {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("flag", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![1, 0])),
            ],
        )
        .expect("source batch")
    };
    let source =
        MemTable::try_new(source_batch.schema(), vec![vec![source_batch]]).expect("src mt");
    ctx_cow
        .register_table("src", Arc::new(source))
        .expect("register src");
    let spec_cow = merge_spec("fm_cow", matched.clone(), vec![]);
    execute_merge(&ctx_cow, &catalog_cow, &spec_cow)
        .await
        .expect("COW multi-clause first-match MERGE");
    assert_eq!(
        read_back(&catalog_cow, &ident_cow).await,
        expected,
        "COW first-match + survivor rows"
    );

    // --- MoR twin ------------------------------------------------------------------------
    let warehouse_mor = TempDir::new().expect("temp warehouse mor");
    let catalog_mor = memory_catalog(&warehouse_mor).await;
    let ident_mor = create_target_with(&catalog_mor, "fm_mor", mor_props()).await;
    append_file(
        &catalog_mor,
        &ident_mor,
        consumer_batch(&[1, 2, 3], &[Some("old"), Some("old"), Some("old")]),
    )
    .await;
    let ctx_mor = SessionContext::new();
    let source_batch_mor = {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("flag", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![1, 0])),
            ],
        )
        .expect("source batch mor")
    };
    let source_mor = MemTable::try_new(source_batch_mor.schema(), vec![vec![source_batch_mor]])
        .expect("src mt mor");
    ctx_mor
        .register_table("src", Arc::new(source_mor))
        .expect("register src mor");
    let spec_mor = merge_spec("fm_mor", matched, vec![]);
    execute_merge(&ctx_mor, &catalog_mor, &spec_mor)
        .await
        .expect("MoR multi-clause first-match MERGE");
    assert_eq!(
        read_back(&catalog_mor, &ident_mor).await,
        expected,
        "MoR first-match + survivor rows must match COW"
    );
}

/// Critic-octo C1-Q1: NULL-predicate first-match (3VL) under `clause_id` — COW + `MoR`.
///
/// Clause 0: `s.flag = 1` → UPDATE v = 'first'. When `flag` is SQL NULL the comparison is
/// UNKNOWN; COALESCE → does-not-apply, so clause 1 (unconditional → 'second') must win.
/// A regression that dropped COALESCE inside the O(C) CASE would mis-assign or drop the row.
#[tokio::test]
async fn multi_clause_null_predicate_first_match_3vl_cow_and_mor() {
    let matched = vec![
        MatchedClause {
            predicate_sql: Some("s.flag = 1".to_string()),
            action: MatchedAction::Update {
                assignments: vec![("v".to_string(), "'first'".to_string())],
            },
        },
        MatchedClause {
            predicate_sql: None,
            action: MatchedAction::Update {
                assignments: vec![("v".to_string(), "'second'".to_string())],
            },
        },
    ];
    // id=1 flag=NULL → clause0 UNKNOWN → clause1 'second'
    // id=2 flag=1    → clause0 'first'
    // id=3 unmatched survivor → 'old'
    let expected = vec![
        (1, Some("second".to_string())),
        (2, Some("first".to_string())),
        (3, Some("old".to_string())),
    ];

    for (mode_name, props, table_name) in [
        ("COW", cow_props(), "fm_3vl_cow"),
        ("MoR", mor_props(), "fm_3vl_mor"),
    ] {
        let warehouse = TempDir::new().expect("temp warehouse 3vl");
        let catalog = memory_catalog(&warehouse).await;
        let ident = create_target_with(&catalog, table_name, props).await;
        append_file(
            &catalog,
            &ident,
            consumer_batch(&[1, 2, 3], &[Some("old"), Some("old"), Some("old")]),
        )
        .await;
        let ctx = SessionContext::new();
        let source_batch = {
            let schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("flag", DataType::Int32, true),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(Int32Array::from(vec![None, Some(1)])),
                ],
            )
            .expect("source batch 3vl")
        };
        let source =
            MemTable::try_new(source_batch.schema(), vec![vec![source_batch]]).expect("src mt");
        ctx.register_table("src", Arc::new(source))
            .expect("register src");
        let spec = merge_spec(table_name, matched.clone(), vec![]);
        execute_merge(&ctx, &catalog, &spec)
            .await
            .unwrap_or_else(|error| panic!("{mode_name} 3VL first-match MERGE: {error}"));
        assert_eq!(
            read_back(&catalog, &ident).await,
            expected,
            "{mode_name}: NULL flag must fall through to clause 1 ('second')"
        );
    }
}

/// Critic-octo C1-Q2: NOT MATCHED multi-clause first-match via `clause_id = index`.
///
/// Empty target: every source row is NOT MATCHED. Clause 0 inserts when `flag = 1`;
/// clause 1 is the unconditional fallback. `flag` NULL must not take clause 0 (3VL).
#[tokio::test]
async fn multi_not_matched_clause_first_match_and_3vl() {
    let warehouse = TempDir::new().expect("temp warehouse insert fm");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "ins_fm").await; // no append → empty snapshot
    let ctx = SessionContext::new();
    let source_batch = {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("flag", DataType::Int32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![Some(1), Some(0), None])),
            ],
        )
        .expect("source batch insert fm")
    };
    let source =
        MemTable::try_new(source_batch.schema(), vec![vec![source_batch]]).expect("src mt");
    ctx.register_table("src", Arc::new(source))
        .expect("register src");
    let not_matched = vec![
        InsertClause {
            predicate_sql: Some("s.flag = 1".to_string()),
            action: InsertAction::Explicit {
                columns: vec!["id".to_string(), "v".to_string()],
                values_sql: vec!["s.id".to_string(), "'from0'".to_string()],
            },
        },
        InsertClause {
            predicate_sql: None,
            action: InsertAction::Explicit {
                columns: vec!["id".to_string(), "v".to_string()],
                values_sql: vec!["s.id".to_string(), "'from1'".to_string()],
            },
        },
    ];
    let spec = merge_spec("ins_fm", vec![], not_matched);
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("multi NOT MATCHED first-match MERGE");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (1, Some("from0".to_string())), // flag=1 → clause 0
            (2, Some("from1".to_string())), // flag=0 → clause 1
            (3, Some("from1".to_string())), // flag=NULL → clause 0 UNKNOWN → clause 1
        ],
        "NOT MATCHED clause_id first-match + 3VL fallthrough"
    );
}

/// Critic-octo C2-Q3: multi NOT MATCHED first-match against a **non-empty** target (anti-join).
/// id=1 is matched UPDATE; id=2/3 insert via dual NOT MATCHED clauses (flag steers).
#[tokio::test]
async fn multi_not_matched_with_partial_target_match() {
    let warehouse = TempDir::new().expect("temp warehouse partial ins");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "ins_partial").await;
    append_file(&catalog, &ident, consumer_batch(&[1], &[Some("old")])).await;
    let ctx = SessionContext::new();
    let source_batch = {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("flag", DataType::Int32, true),
            Field::new("v", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![None, Some(1), Some(0)])),
                Arc::new(StringArray::from(vec![Some("NEW"), Some("i2"), Some("i3")])),
            ],
        )
        .expect("source batch partial")
    };
    let source =
        MemTable::try_new(source_batch.schema(), vec![vec![source_batch]]).expect("src mt");
    ctx.register_table("src", Arc::new(source))
        .expect("register src");
    let matched = vec![update_set("v", "s.v")];
    let not_matched = vec![
        InsertClause {
            predicate_sql: Some("s.flag = 1".to_string()),
            action: InsertAction::Explicit {
                columns: vec!["id".to_string(), "v".to_string()],
                values_sql: vec!["s.id".to_string(), "'from0'".to_string()],
            },
        },
        InsertClause {
            predicate_sql: None,
            action: InsertAction::Explicit {
                columns: vec!["id".to_string(), "v".to_string()],
                values_sql: vec!["s.id".to_string(), "'from1'".to_string()],
            },
        },
    ];
    let spec = merge_spec("ins_partial", matched, not_matched);
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("partial-target multi NOT MATCHED MERGE");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (1, Some("NEW".to_string())),   // matched UPDATE
            (2, Some("from0".to_string())), // NOT MATCHED clause 0
            (3, Some("from1".to_string())), // NOT MATCHED clause 1
        ],
        "matched UPDATE + multi NOT MATCHED first-match must coexist"
    );
}

/// Critic-octo C2-S1: two sequential COW MERGEs on one `SessionContext` with file-scoped off
/// (path `MemTable` semi-join) — scratch guard must not leave a broken catalog between runs.
#[tokio::test]
async fn sequential_cow_path_semijoin_same_session_ctx() {
    let warehouse = TempDir::new().expect("temp warehouse seq cow");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "seq_cow").await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2], &[Some("a"), Some("b")]),
    )
    .await;
    let config = repark_write_scan_prune_file_scoped_off();
    let ctx = SessionContext::new_with_config(config);
    register_source(&ctx, &[1], &[Some("A")]);
    let spec1 = merge_spec("seq_cow", vec![update_set("v", "s.v")], vec![]);
    execute_merge(&ctx, &catalog, &spec1)
        .await
        .expect("first path-semijoin COW");
    // Re-register source for second merge (MemTable was for first only).
    let _ = ctx.deregister_table("src");
    register_source(&ctx, &[2], &[Some("B")]);
    let spec2 = merge_spec("seq_cow", vec![update_set("v", "s.v")], vec![]);
    execute_merge(&ctx, &catalog, &spec2)
        .await
        .expect("second path-semijoin COW on same SessionContext");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(1, Some("A".to_string())), (2, Some("B".to_string())),],
    );
}

/// `SessionConfig` with `repark.merge.file-scoped-rewrite=false`.
fn repark_write_scan_prune_file_scoped_off() -> datafusion::prelude::SessionConfig {
    crate::write::scan_prune::with_file_scoped_rewrite(
        datafusion::prelude::SessionConfig::new(),
        false,
    )
}

/// PIN K-CARDINALITY (row-identity mutation target M-K-ID) — the cardinality check groups on
/// `(_file, _pos)`, NOT `_file` alone. TWO distinct-key target rows in ONE file, each matched by
/// exactly one source row, must NOT raise a cardinality violation; a `_file`-alone GROUP BY
/// would mis-count them as a single 2-source-match group and FALSELY error (the M-K-ID mutation:
/// drop `_pos` from `match_discovery_sql`'s GROUP BY → this pin RED). A GENUINE multi-source match
/// (two source rows for one target row) still errors — the guard stays live.
#[tokio::test]
async fn merge_cardinality_uses_file_and_pos_not_file_alone() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "card").await;
    // ONE file, two rows: id1 @ _pos0, id2 @ _pos1 — they share a `_file`.
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2], &[Some("a"), Some("b")]),
    )
    .await;
    let spec = merge_spec("card", vec![update_set("v", "s.v")], vec![]);

    // One source row per target id (distinct keys, NO duplicate) ⇒ no cardinality violation.
    let ctx = SessionContext::new();
    register_source(&ctx, &[1, 2], &[Some("A"), Some("B")]);
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("distinct-key matches in one file must NOT raise a cardinality violation");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(1, Some("A".to_string())), (2, Some("B".to_string()))],
    );

    // A genuine multi-source match (two source rows for id=1) still errors — guard is live.
    let ctx2 = SessionContext::new();
    register_source(&ctx2, &[1, 1], &[Some("X"), Some("Y")]);
    let err = execute_merge(&ctx2, &catalog, &spec)
        .await
        .expect_err("two source rows for one target row must raise MERGE_CARDINALITY_VIOLATION");
    assert!(
        err.to_string().contains("MERGE_CARDINALITY_VIOLATION"),
        "must be the cardinality guard, got: {err}"
    );
}

/// PIN K-EMPTY — a target with NO snapshot streams as an empty relation: every source row is
/// NOT MATCHED (`_pos IS NULL` over the empty target) and is inserted. Risk: the streaming
/// empty-target path (no scan opened) diverges from the old empty-`MemTable` behavior.
#[tokio::test]
async fn merge_empty_target_streams_all_inserts() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "empty").await; // created, never appended → no snapshot

    let ctx = SessionContext::new();
    register_source(&ctx, &[1, 2], &[Some("a"), Some("b")]);
    let spec = merge_spec(
        "empty",
        vec![update_set("v", "s.v")],
        vec![insert_values(&["id", "v"], &["s.id", "s.v"])],
    );
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("MERGE into an empty target commits the inserts");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(1, Some("a".to_string())), (2, Some("b".to_string()))],
    );
}

// ===========================================================================================
// Group R — MERGE INTO a NON-identity transform-partitioned table (the gate `reject_unsupported`
// used to fence). Both arms route through the SAME `append::write_partitioned_data_files` fanout
// in the fork's COMPUTED-transform mode (Group P), so every produced `DataFile` carries its
// transform-computed partition value at the manifest level. These write-crate pins read the
// committed partition slots straight off the manifests (NOT just row values), with the fork's own
// `Transform::Bucket(N)` function as the self-oracle — a survivor silently left in the old
// partition is the exact failure mode R2 catches. (R3 temporal/truncate + R4 transform-path OCC
// + R5 MoR-on-a-transform-table live end-to-end in `repark-sql::tests::partitioned_merge`.)
// ===========================================================================================

/// Create `sales.<name>` with `id int` (required) + `v string` (optional), partitioned by
/// `bucket(<num_buckets>, id)` — a NON-identity transform spec.
async fn create_bucket_target(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    num_buckets: u32,
) -> TableIdent {
    create_bucket_target_with(catalog, name, num_buckets, HashMap::new()).await
}

/// [`create_bucket_target`] with table properties — the seam the Group Y merge-on-read ×
/// transform pins use to set `write.merge.mode` on a `bucket(N, id)` table.
async fn create_bucket_target_with(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    num_buckets: u32,
    properties: HashMap<String, String>,
) -> TableIdent {
    use iceberg::spec::{Transform, UnboundPartitionSpec};
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build bucket target schema");
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "id_bucket", Transform::Bucket(num_buckets))
        .expect("add bucket partition field")
        .build();
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .partition_spec(spec)
        .properties(properties)
        .build();
    catalog
        .create_table(&NamespaceIdent::new("sales".to_string()), creation)
        .await
        .expect("create bucket-partitioned target");
    TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string())
}

/// The live (Added/Existing) DATA-file entries in the current snapshot's manifests — the
/// manifest-level oracle for the partition slot every committed file actually carries.
async fn live_data_files(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<DataFile> {
    let table = catalog.load_table(ident).await.expect("load table");
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

/// The single bucket-ordinal partition slot of a `DataFile` (the tables here partition by one
/// non-null `bucket(N, id)` field, so a null or non-int slot is a hard test failure).
fn bucket_slot(file: &DataFile) -> i32 {
    use iceberg::spec::{Literal, PrimitiveLiteral};
    match file.partition().fields().first().cloned().flatten() {
        Some(Literal::Primitive(PrimitiveLiteral::Int(bucket))) => bucket,
        other => panic!("bucket partition slot must be a non-null int literal, got {other:?}"),
    }
}

/// The fork's OWN `Transform::Bucket(n)` ordinal for a key — the self-oracle: the engine must
/// drive the same hash the fork computes, so expected buckets come from the fork, not a re-impl.
fn fork_bucket(n: u32, key: i32) -> i32 {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Int32Type;
    use iceberg::spec::Transform;
    use iceberg::transform::create_transform_function;
    let bucket_fn = create_transform_function(&Transform::Bucket(n)).expect("bucket fn");
    let out = bucket_fn
        .transform(Arc::new(Int32Array::from(vec![key])))
        .expect("apply bucket transform");
    out.as_primitive::<Int32Type>().value(0)
}

/// PIN R1 — MERGE into a `bucket(4, id)` table: a matched UPDATE (rewrite arm) and a not-matched
/// INSERT both route through the computed-mode fanout, so every committed `DataFile` lands in the
/// bucket the FORK's `Bucket(4)` assigns its key — NOT the identity key, NOT a single silent
/// partition. Manifest slots are checked against the self-oracle AND the table round-trips
/// value+type. Mutation M-R (restore the non-identity gate in `reject_unsupported`) → the MERGE
/// returns `NotImplemented` and `execute_merge` errors → RED.
#[tokio::test]
async fn merge_bucket_partitioned_routes_by_fork_hash() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_bucket_target(&catalog, "bkt", 4).await;
    // Base rows id 1,2,3 (spread across buckets); the MERGE updates id=2 and inserts id=8.
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2, 3], &[Some("a"), Some("b"), Some("c")]),
    )
    .await;

    let ctx = SessionContext::new();
    register_source(&ctx, &[2, 8], &[Some("B"), Some("H")]);
    let spec = merge_spec(
        "bkt",
        vec![update_set("v", "s.v")],
        vec![insert_values(&["id", "v"], &["s.id", "s.v"])],
    );
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("bucket-partitioned MERGE commits via the computed-mode fanout");

    // Value+type round-trip: id=2 updated, id=8 inserted, 1 and 3 untouched.
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (1, Some("a".to_string())),
            (2, Some("B".to_string())),
            (3, Some("c".to_string())),
            (8, Some("H".to_string())),
        ],
    );

    // Manifest-level: total records per bucket slot equal the fork's own Bucket(4) routing of the
    // FINAL key set {1,2,3,8}. A slot that carried the identity key (or a single silent
    // partition) would fail this against the self-oracle.
    let mut expected: HashMap<i32, u64> = HashMap::new();
    for key in [1, 2, 3, 8] {
        *expected.entry(fork_bucket(4, key)).or_insert(0) += 1;
    }
    let mut actual: HashMap<i32, u64> = HashMap::new();
    for file in &live_data_files(&catalog, &ident).await {
        let slot = bucket_slot(file);
        assert!(
            (0..4).contains(&slot),
            "committed slot must be a bucket ordinal 0..4 (not the identity key), got {slot}"
        );
        *actual.entry(slot).or_insert(0) += file.record_count();
    }
    assert_eq!(
        actual, expected,
        "rewrite + insert manifest routing must match the fork's own Bucket(4) hash"
    );
}

/// PIN R2 (the load-bearing partition-move pin) — a matched UPDATE that CHANGES the partition
/// SOURCE column (`id`) re-routes the survivor to the NEW bucket: the old data file is rewritten
/// away and the survivor is written into the bucket the FORK assigns the NEW key. Verified via
/// the written PARTITION METADATA (not only the row value) — a survivor silently left in the old
/// bucket is the exact failure mode. Keys chosen so `bucket(old) != bucket(new)` (asserted), so
/// the move is observable. Mutation M-R (restore the non-identity gate) → RED.
#[tokio::test]
async fn merge_bucket_partition_key_changing_update_reroutes_survivor() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_bucket_target(&catalog, "mv", 4).await;
    // Base rows id 1 and 7. The MERGE moves id=1 → id=42; id=7 is untouched.
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 7], &[Some("a"), Some("g")]),
    )
    .await;

    // Guard: the move is only observable if the new key hashes to a DIFFERENT bucket.
    let (old_bucket, new_bucket, other_bucket) =
        (fork_bucket(4, 1), fork_bucket(4, 42), fork_bucket(4, 7));
    assert_ne!(
        old_bucket, new_bucket,
        "test keys must straddle a bucket boundary for the move to be observable"
    );

    let ctx = SessionContext::new();
    register_source(&ctx, &[1], &[Some("ignored")]);
    let spec = merge_spec("mv", vec![update_set("id", "42")], vec![]);
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("partition-key-changing UPDATE commits via the computed-mode fanout");

    // The row moved from id=1 to id=42 carrying its name; id=7 survived.
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(7, Some("g".to_string())), (42, Some("a".to_string()))],
        "the matched row moved from id=1 to id=42, carrying its value",
    );

    // Manifest metadata: every surviving row's file carries the bucket of its CURRENT key. The
    // survivor is in `bucket(42)`, id=7 in `bucket(7)`, and `bucket(1)` holds nothing anymore
    // (its old file was rewritten away) — unless bucket(42)/bucket(7) happen to collide with it,
    // which the per-slot record counts still disambiguate.
    let mut records_by_slot: HashMap<i32, u64> = HashMap::new();
    for file in &live_data_files(&catalog, &ident).await {
        *records_by_slot.entry(bucket_slot(file)).or_insert(0) += file.record_count();
    }
    let mut expected: HashMap<i32, u64> = HashMap::new();
    *expected.entry(new_bucket).or_insert(0) += 1; // the moved survivor (id=42)
    *expected.entry(other_bucket).or_insert(0) += 1; // the untouched id=7
    assert_eq!(
        records_by_slot, expected,
        "the survivor must land in bucket(42), NOT stay in bucket(1); id=7 stays in bucket(7)"
    );
    assert!(
        !records_by_slot.contains_key(&old_bucket)
            || (old_bucket == new_bucket || old_bucket == other_bucket),
        "bucket(1) must hold no live row after the move (it was rewritten away)"
    );
}

// ===================================================================================
// GROUP T — merge-on-read MERGE INTO pins (T1-T4, T7, T8 gates).
//
// The claim these pins defend: a merge-on-read MERGE and a copy-on-write MERGE of the SAME
// source into the SAME target are SCAN-EQUIVALENT (identical rows, identical types, on the
// Arrow path), while being PHYSICALLY different — merge-on-read leaves every original data
// file in place and adds position-delete files, copy-on-write rewrites the affected files
// away. Every pin below asserts BOTH halves where they apply: a pin that only checked rows
// would stay green if the merge-on-read arm silently fell back to copy-on-write.
// ===================================================================================

/// Table properties selecting merge-on-read.
fn mor_props() -> HashMap<String, String> {
    HashMap::from([(MERGE_MODE_PROP.to_string(), "merge-on-read".to_string())])
}

/// Table properties selecting copy-on-write EXPLICITLY (the T4 differential's control arm —
/// stated rather than defaulted, so the pin compares two deliberate modes).
fn cow_props() -> HashMap<String, String> {
    HashMap::from([(MERGE_MODE_PROP.to_string(), "copy-on-write".to_string())])
}

/// The live (Added/Existing) DELETE-file entries in the current snapshot's DELETE manifests —
/// the manifest-level oracle for "a position-delete file was actually committed". The
/// physical-shape half of every merge-on-read pin reads this; without it a pin cannot tell a
/// real merge-on-read commit from a copy-on-write fallback that happens to produce the same rows.
async fn live_delete_files(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<DataFile> {
    let table = catalog.load_table(ident).await.expect("load table");
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

/// The live data-file PATHS, sorted — the "were the data files touched?" oracle.
async fn live_data_file_paths(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<String> {
    let mut paths: Vec<String> = live_data_files(catalog, ident)
        .await
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();
    paths.sort();
    paths
}

/// The Arrow SCHEMA the read-back scan produces — the TYPE half of the T4 differential (rule:
/// a parity claim pins value AND type on the Arrow path, never value alone).
async fn read_back_schema(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> SchemaRef {
    let table = catalog.load_table(ident).await.expect("load table");
    let batches: Vec<RecordBatch> = table
        .scan()
        .select(["id", "v"])
        .build()
        .expect("build scan")
        .to_arrow()
        .await
        .expect("scan to_arrow")
        .try_collect()
        .await
        .expect("collect scan batches");
    batches
        .first()
        .expect("read-back produced at least one batch")
        .schema()
}

/// `WHEN MATCHED THEN DELETE`.
fn delete_matched() -> MatchedClause {
    MatchedClause {
        predicate_sql: None,
        action: MatchedAction::Delete,
    }
}

/// R-MERGE-TRACING pin: on a local merge-on-read MERGE, all five phase spans fire and `merge.commit`
/// is the last of the five by first-record order. Zero overhead when no subscriber is installed
/// (standard `tracing` macros only). Live profile:
/// `RUST_LOG=repark_write=info` (or `RUST_LOG=merge=info` if targets are filtered by name).
///
/// Capture is a GLOBAL subscriber (installed once per process), not `set_default`: the merge
/// future can be polled on threads other than the test thread (CI 2-core runners provoke
/// this), and a thread-local subscriber records nothing there. Spans from parallel tests are
/// excluded by requiring the unique `merge.trace_test_root` ancestor this test wraps its
/// merge in — the five production spans are created while that root is entered, so contextual
/// parenting links them regardless of which thread polls.
#[tokio::test]
async fn mor_merge_emits_five_phase_spans_with_commit_last() {
    use tracing::Instrument;

    // v1 installed THIS test's own process-global subscriber (a per-binary invariant its
    // `expect` message asserted). Merged with the catalog cohort into one binary that
    // collides with the catalog capture install — forced-edit class 6
    // (docs/design/session-api.md §5) — so the `SpanNameRecorder` layer and the single
    // global install live in `crate::test_tracing`; the recorder's semantics (global
    // subscriber, `merge.trace_test_root`-descended `merge.*` spans only) are v1's,
    // unchanged, and every assertion below is byte-unchanged from v1.
    let recorded = crate::test_tracing::merge_span_names();
    recorded.lock().expect("span name lock").clear();

    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target_with(&catalog, "mor_trace", mor_props()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2, 3], &[Some("a"), Some("b"), Some("c")]),
    )
    .await;
    let ctx = SessionContext::new();
    register_source(&ctx, &[2], &[Some("ignored")]);
    let spec = merge_spec("mor_trace", vec![delete_matched()], vec![]);
    execute_merge(&ctx, &catalog, &spec)
        .instrument(tracing::info_span!("merge.trace_test_root"))
        .await
        .expect("MoR MERGE for span capture");

    let names = recorded.lock().expect("span name lock").clone();
    let required = [
        "merge.target_scan",
        "merge.join",
        "merge.write_data",
        "merge.write_deletes",
        "merge.commit",
    ];
    for expected in required {
        assert!(
            names.iter().any(|name| name == expected),
            "expected span {expected} to fire; recorded (root-descended): {names:?}"
        );
    }
    // First record of each of the five — commit must be last among them.
    let mut first_enter_order = Vec::new();
    for name in &names {
        if required.contains(&name.as_str()) && !first_enter_order.contains(name) {
            first_enter_order.push(name.clone());
        }
    }
    assert_eq!(
        first_enter_order.last().map(String::as_str),
        Some("merge.commit"),
        "merge.commit must nest/enter last among the five phases; order={first_enter_order:?}"
    );
}

/// PIN T1 (matched DELETE, merge-on-read) — the deleted row vanishes from the next scan, a
/// POSITION-DELETE file is committed, and every original DATA file is left byte-identically in
/// place (same paths, same count). The data-file assertion is the load-bearing half: it is what
/// distinguishes a real merge-on-read commit from a copy-on-write rewrite that produces the same
/// rows. Mutation M-T-PD (skip the position-delete write — return `Ok(Vec::new())` from
/// `write_position_deletes`): the deleted row survives the read-back ⇒ RED.
#[tokio::test]
async fn mor_matched_delete_position_deletes_row_and_leaves_data_files() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target_with(&catalog, "mor_del", mor_props()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2, 3], &[Some("a"), Some("b"), Some("c")]),
    )
    .await;
    let data_before = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(data_before.len(), 1, "one appended data file");

    let ctx = SessionContext::new();
    register_source(&ctx, &[2], &[Some("ignored")]);
    let spec = merge_spec("mor_del", vec![delete_matched()], vec![]);
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("merge-on-read DELETE commits");

    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(1, Some("a".to_string())), (3, Some("c".to_string()))],
        "the scan applies the position delete — id=2 is gone"
    );
    assert_eq!(
        live_data_file_paths(&catalog, &ident).await,
        data_before,
        "merge-on-read must leave the ORIGINAL data files untouched (a copy-on-write rewrite \
         would have replaced this path)"
    );
    let deletes = live_delete_files(&catalog, &ident).await;
    assert_eq!(
        deletes.len(),
        1,
        "exactly one position-delete file committed"
    );
    assert_eq!(
        deletes[0].content_type(),
        DataContentType::PositionDeletes,
        "the committed delete file must be a POSITION delete (equality deletes are out of scope)"
    );
    assert_eq!(
        deletes[0].record_count(),
        1,
        "one deleted row ⇒ one position-delete record"
    );
}

/// PIN QA-176 (fork #176 rider, repin `14921e78`) — position deletes scope to the data file
/// they name, across a MULTI-file scan. Fork #176 (`fix/delete-filter-per-task-scope`) fixed
/// a per-scan `DeleteFilter` cache that could apply position deletes across scan tasks
/// (cross-task OVER-delete = silent row loss on read; Java builds one filter per task). The
/// shape here is the RePark-visible contract: two data files with ALIGNED ordinal positions
/// and ONE MERGE deleting the pos-1 row of BOTH files, so a single position-delete file is in
/// scope for both scan tasks — each task must apply only the entries naming its own data
/// file. HONEST LIMIT: this pin ran GREEN at the pre-#176 rev `a08a0957` in both this shape
/// and a two-sequential-MERGE variant (repin ledger C-011) — it does NOT discriminate the
/// fixed defect (whose trigger needs a task shape `RePark`'s writers don't produce here); the
/// authoritative pre-fix repro is the fork's own interop crosstask leg (id 30 survives =
/// Java {10,30,40,60}). It stands as the regression contract for the over-delete class.
#[tokio::test]
async fn mor_deletes_scope_to_their_own_data_file_across_a_multi_file_scan() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target_with(&catalog, "mor_scope", mor_props()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[10, 20, 30], &[Some("a"), Some("b"), Some("c")]),
    )
    .await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[40, 50, 60], &[Some("d"), Some("e"), Some("f")]),
    )
    .await;
    assert_eq!(
        live_data_file_paths(&catalog, &ident).await.len(),
        2,
        "two data files with aligned ordinal positions"
    );

    // ONE MERGE deleting the pos-1 row of BOTH files: the committed position-delete file(s)
    // carry entries for two data files, so both scan tasks have the same delete file in
    // scope — the cross-task sharing shape #176's cache fix is about.
    let ctx = SessionContext::new();
    register_source(&ctx, &[20, 50], &[Some("ignored"), Some("ignored")]);
    execute_merge(
        &ctx,
        &catalog,
        &merge_spec("mor_scope", vec![delete_matched()], vec![]),
    )
    .await
    .expect("merge-on-read DELETE spanning both data files commits");

    assert!(
        !live_delete_files(&catalog, &ident).await.is_empty(),
        "the MERGE commits position-delete file(s)"
    );
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (10, Some("a".to_string())),
            (30, Some("c".to_string())),
            (40, Some("d".to_string())),
            (60, Some("f".to_string()))
        ],
        "a position delete applies ONLY to the data file it names: the pos-1 deletes in file 1 \
         and file 2 must not cross-kill the pos-1 row of the OTHER file (the fork #176 \
         over-delete class)"
    );
}

/// PIN T2 (matched UPDATE, merge-on-read) — an update is delete-old + insert-new: the OLD row's
/// `(_file, _pos)` is position-deleted and the NEW values land in a FRESH data file, so the scan
/// shows the updated row EXACTLY ONCE (never twice — the duplicate is the signature failure of
/// writing the new row without deleting the old) and the untouched sibling row is unchanged. The
/// original data file survives. Mutation M-T-PD (skip the position-delete write): id=2 appears
/// twice, once with the old value ⇒ RED.
#[tokio::test]
async fn mor_matched_update_deletes_old_row_and_writes_new_values() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target_with(&catalog, "mor_upd", mor_props()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2], &[Some("a"), Some("b")]),
    )
    .await;
    let data_before = live_data_file_paths(&catalog, &ident).await;

    let ctx = SessionContext::new();
    register_source(&ctx, &[2], &[Some("B")]);
    let spec = merge_spec("mor_upd", vec![update_set("v", "s.v")], vec![]);
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("merge-on-read UPDATE commits");

    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(1, Some("a".to_string())), (2, Some("B".to_string()))],
        "the updated row appears ONCE with its new value; the sibling row is untouched"
    );
    let data_after = live_data_file_paths(&catalog, &ident).await;
    assert!(
        data_before.iter().all(|path| data_after.contains(path)),
        "the original data file must still be live under merge-on-read"
    );
    assert_eq!(
        data_after.len(),
        data_before.len() + 1,
        "the new values land in ONE additional data file, not a rewrite of the original"
    );
    let deletes = live_delete_files(&catalog, &ident).await;
    assert_eq!(deletes.len(), 1, "the old row is position-deleted");
    assert_eq!(deletes[0].record_count(), 1, "exactly the one updated row");
}

/// PIN T3 (not-matched INSERT, merge-on-read) — a pure insert writes a new data file and NO
/// delete file at all (there is no old row to retire). Guards the arm that would otherwise
/// commit an empty or spurious position-delete file: an unconditional delete-file write would
/// make this RED.
#[tokio::test]
async fn mor_not_matched_insert_writes_data_file_and_no_delete_file() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target_with(&catalog, "mor_ins", mor_props()).await;
    append_file(&catalog, &ident, consumer_batch(&[1], &[Some("a")])).await;
    let data_before = live_data_file_paths(&catalog, &ident).await;

    let ctx = SessionContext::new();
    register_source(&ctx, &[9], &[Some("Z")]);
    let spec = merge_spec(
        "mor_ins",
        vec![],
        vec![insert_values(&["id", "v"], &["s.id", "s.v"])],
    );
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("merge-on-read INSERT commits");

    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(1, Some("a".to_string())), (9, Some("Z".to_string()))],
    );
    let data_after = live_data_file_paths(&catalog, &ident).await;
    assert!(
        data_before.iter().all(|path| data_after.contains(path)),
        "the original data file is untouched"
    );
    assert_eq!(data_after.len(), data_before.len() + 1, "one new data file");
    assert!(
        live_delete_files(&catalog, &ident).await.is_empty(),
        "an insert-only merge-on-read MERGE must commit NO position-delete file"
    );
}

/// PIN T4 — THE differential oracle (the whole point of Group T). A three-clause MERGE
/// (`WHEN MATCHED AND v = 'drop' THEN DELETE`, then `WHEN MATCHED THEN UPDATE`, then
/// `WHEN NOT MATCHED THEN INSERT`) is run against two IDENTICAL targets that differ only in
/// `write.merge.mode`, from the same source. The pin asserts:
///   * **scan-equivalence** — identical rows AND an identical Arrow schema on the read path
///     (value + type, per the divergence-class rule);
///   * **physical divergence** — merge-on-read keeps EVERY original data file and commits
///     position-delete files; copy-on-write commits NO delete file and has rewritten the
///     affected original file away.
/// Either half alone is insufficient: rows-only would pass if merge-on-read secretly ran the
/// copy-on-write arm, and shape-only would pass if the deletes landed on the wrong rows.
#[tokio::test]
async fn mor_and_cow_merges_are_scan_equivalent_but_physically_different() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let mor = create_target_with(&catalog, "diff_mor", mor_props()).await;
    let cow = create_target_with(&catalog, "diff_cow", cow_props()).await;

    // Same two data files in each target: id=1 is deleted, id=2 updated, id=3 untouched
    // (and lives in the same file as id=1, so copy-on-write must rewrite it while merge-on-read
    // must NOT disturb it), id=9 inserted.
    for ident in [&mor, &cow] {
        append_file(
            &catalog,
            ident,
            consumer_batch(&[1, 3], &[Some("drop"), Some("keep")]),
        )
        .await;
        append_file(&catalog, ident, consumer_batch(&[2], &[Some("old")])).await;
    }
    let mor_data_before = live_data_file_paths(&catalog, &mor).await;
    let cow_data_before = live_data_file_paths(&catalog, &cow).await;

    let clauses = || {
        (
            vec![
                MatchedClause {
                    predicate_sql: Some("t.v = 'drop'".to_string()),
                    action: MatchedAction::Delete,
                },
                update_set("v", "s.v"),
            ],
            vec![insert_values(&["id", "v"], &["s.id", "s.v"])],
        )
    };
    for name in ["diff_mor", "diff_cow"] {
        let ctx = SessionContext::new();
        register_source(&ctx, &[1, 2, 9], &[Some("X"), Some("NEW"), Some("INS")]);
        let (matched, not_matched) = clauses();
        let spec = merge_spec(name, matched, not_matched);
        execute_merge(&ctx, &catalog, &spec)
            .await
            .unwrap_or_else(|error| panic!("{name} MERGE commits: {error}"));
    }

    // --- scan-equivalence: value AND type ---
    let expected = vec![
        (2, Some("NEW".to_string())), // first-match-wins: clause 2 (UPDATE) applied
        (3, Some("keep".to_string())), // untouched sibling of the deleted row
        (9, Some("INS".to_string())), // not-matched INSERT
    ];
    let mor_rows = read_back(&catalog, &mor).await;
    assert_eq!(
        mor_rows,
        read_back(&catalog, &cow).await,
        "merge-on-read and copy-on-write MERGE must be scan-equivalent"
    );
    assert_eq!(mor_rows, expected, "and both must be Spark-correct");
    assert_eq!(
        read_back_schema(&catalog, &mor).await,
        read_back_schema(&catalog, &cow).await,
        "scan-equivalence is value AND type — the Arrow schemas must match too"
    );

    // --- physical divergence ---
    let mor_data_after = live_data_file_paths(&catalog, &mor).await;
    assert!(
        mor_data_before
            .iter()
            .all(|path| mor_data_after.contains(path)),
        "merge-on-read must leave EVERY original data file live"
    );
    assert!(
        !live_delete_files(&catalog, &mor).await.is_empty(),
        "merge-on-read must commit position-delete files"
    );

    let cow_data_after = live_data_file_paths(&catalog, &cow).await;
    assert!(
        cow_data_before
            .iter()
            .any(|path| !cow_data_after.contains(path)),
        "copy-on-write must have rewritten at least one original data file AWAY"
    );
    assert!(
        live_delete_files(&catalog, &cow).await.is_empty(),
        "copy-on-write must commit NO delete file"
    );
}

/// Create `sales.<name>` with `id int` + `v string`, IDENTITY-partitioned on `id`, with the
/// given properties — the T7 target (each distinct id is its own partition, so a position-delete
/// file must carry the partition of the data file it deletes from).
async fn create_identity_target(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    properties: HashMap<String, String>,
) -> TableIdent {
    use iceberg::spec::{Transform, UnboundPartitionSpec};
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build identity target schema");
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "id", Transform::Identity)
        .expect("add identity partition field")
        .build();
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .partition_spec(spec)
        .properties(properties)
        .build();
    catalog
        .create_table(&NamespaceIdent::new("sales".to_string()), creation)
        .await
        .expect("create identity-partitioned target");
    TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string())
}

/// The single identity-`id` partition slot of a `DataFile`.
fn identity_slot(file: &DataFile) -> i32 {
    use iceberg::spec::{Literal, PrimitiveLiteral};
    match file.partition().fields().first().cloned().flatten() {
        Some(Literal::Primitive(PrimitiveLiteral::Int(id))) => id,
        other => {
            panic!("identity partition slot must be a non-null int literal, got {other:?}")
        }
    }
}

/// PIN T7 (identity-partitioned merge-on-read) — a position-delete file is stamped with the
/// partition of the DATA FILE IT DELETES FROM, not the table's default or the first partition
/// seen. With one partition per id, a DELETE of id=1 and an UPDATE of id=2 must produce delete
/// files in partitions {1, 2} respectively — a single mis-stamped delete file would either be
/// rejected at commit or, worse, be pruned away at scan time and silently resurrect the row.
/// The read-back is asserted too, so a "right partition, wrong rows" stamp cannot pass.
#[tokio::test]
async fn mor_identity_partitioned_stamps_deletes_with_the_owning_partition() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_identity_target(&catalog, "mor_part", mor_props()).await;
    // One append fanned out into one data file per identity partition (id = 1, 2, 3).
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2, 3], &[Some("a"), Some("b"), Some("c")]),
    )
    .await;
    let data_before = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(data_before.len(), 3, "one data file per identity partition");

    let ctx = SessionContext::new();
    register_source(&ctx, &[1, 2, 7], &[None, Some("B"), Some("G")]);
    let spec = merge_spec(
        "mor_part",
        vec![
            MatchedClause {
                predicate_sql: Some("s.v IS NULL".to_string()),
                action: MatchedAction::Delete,
            },
            update_set("v", "s.v"),
        ],
        vec![insert_values(&["id", "v"], &["s.id", "s.v"])],
    );
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("identity-partitioned merge-on-read MERGE commits");

    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (2, Some("B".to_string())), // updated
            (3, Some("c".to_string())), // untouched
            (7, Some("G".to_string())), // inserted into a NEW partition
        ],
        "id=1 is position-deleted in its own partition; the rest are unaffected"
    );

    // Every ORIGINAL data file survives (merge-on-read never rewrites).
    let data_after = live_data_file_paths(&catalog, &ident).await;
    assert!(
        data_before.iter().all(|path| data_after.contains(path)),
        "merge-on-read leaves every original partition's data file in place"
    );

    // The delete files carry partitions {1, 2}: id=1 (DELETE clause) and id=2's OLD row (UPDATE).
    let mut delete_slots: Vec<i32> = live_delete_files(&catalog, &ident)
        .await
        .iter()
        .map(identity_slot)
        .collect();
    delete_slots.sort_unstable();
    assert_eq!(
        delete_slots,
        vec![1, 2],
        "each position-delete file must be stamped with the partition of the data file it \
         deletes from — one in partition id=1, one in partition id=2"
    );
}

// ===================================================================================
// GROUP Y — merge-on-read MERGE × NON-IDENTITY TRANSFORM partitioning (Y1, Y2, Y4, Y5, Y7,
// Y8a). The composition of Group T (merge-on-read) and Group R (transform partitioning),
// which Group T deliberately gated as UNPROVEN. Repro-first observation (2026-07-25) settled
// the crux before a line of the gate moved: on a `bucket(4, id)` table the committed
// position-delete file is stamped `Struct { fields: [Some(Primitive(Int(0)))] }` for a row
// whose identity key is `2` — i.e. the OWNING data file's TRANSFORMED bucket ordinal
// (`bucket(4, 2) = 0`), never the identity key, and never the table's default/empty partition.
// Nothing recomputes it: `write_position_deletes` reads the stamp straight off the manifests,
// where a transform-partitioned file already carries its transformed value.
//
// Every pin below therefore asserts the TRANSFORMED stamp against the fork's own transform
// function as self-oracle AND the physical merge-on-read shape (originals live + delete files
// committed) — a rows-only pin would stay green under a copy-on-write fallback, and a
// shape-only pin would stay green under a mis-targeted stamp.
// ===================================================================================

/// The single bucket-ordinal slot of a committed DELETE file (same shape as [`bucket_slot`];
/// named separately so a pin reads as "the delete file's stamp", not "some file's partition").
fn delete_bucket_slot(file: &DataFile) -> i32 {
    bucket_slot(file)
}

/// The live data file whose bucket slot is `slot` — the "owning file" a delete stamped with
/// `slot` must actually reference. Exactly one such file is expected in these fixtures.
fn only_file_in_bucket(files: &[DataFile], slot: i32) -> &DataFile {
    let mut matching = files.iter().filter(|file| bucket_slot(file) == slot);
    let file = matching.next().unwrap_or_else(|| {
        panic!("no live data file in bucket slot {slot} (the fixture expects exactly one)")
    });
    assert!(
        matching.next().is_none(),
        "bucket slot {slot} holds more than one live data file; the fixture assumes one"
    );
    file
}

/// PIN Y1 (THE Group Y crux — matched DELETE on a `bucket(4, id)` merge-on-read table) — every
/// committed position-delete file is stamped with the OWNING data file's TRANSFORMED partition
/// value (its bucket ordinal), the data files are left completely untouched, and the next scan
/// hides the deleted rows.
///
/// The fixture is built to be discriminating three ways at once:
///   * the deleted rows' identity keys differ from their bucket ordinals (`bucket(4, 2) = 0`),
///     so a stamp that leaked the identity key is a different value;
///   * the MERGE deletes rows from **two different buckets**, so the stamp must be resolved
///     PER OWNING FILE — one partition broadcast across every delete file (a plausible
///     simplification, and what a "look up the first pair's file" implementation does) is
///     visibly wrong here, where a single-bucket fixture could not tell the difference;
///   * each delete file's decoded `(file_path, pos)` rows must name the data file that actually
///     lives in the bucket it is stamped with — "the owning file's partition", not merely "a
///     partition that type-checks".
///
/// Mutation M-Y-GATE (restore the transform+merge-on-read gate) → `NotImplemented` ⇒ RED.
/// Mutation M-Y-DEFAULTSPEC (stamp the default spec's placeholder partition instead of the data
/// file's own) ⇒ RED. Mutation M-Y-COLLAPSE (stamp every delete with the FIRST pair's owning
/// partition) ⇒ RED — the two-bucket fixture is what buys that one.
#[tokio::test]
async fn mor_bucket_partitioned_stamps_deletes_with_the_owning_transformed_partition() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_bucket_target_with(&catalog, "mor_bkt_del", 4, mor_props()).await;
    // Rows 1,2 hash to one bucket and 3,7 to another ⇒ the fanout writes two data files.
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2, 3, 7], &[Some("a"), Some("b"), Some("c"), Some("g")]),
    )
    .await;
    let data_before = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(data_before.len(), 2, "two buckets ⇒ two data files");

    // The discriminating facts: each deleted row's TRANSFORMED partition differs from its key;
    // the two deleted rows live in DIFFERENT buckets; and they sit at DIFFERENT physical
    // ordinals within their files (id=1 at pos 0, id=7 at pos 1), so a delete file that took
    // the other file's ordinal would be caught too.
    let (bucket_of_1, bucket_of_7) = (fork_bucket(4, 1), fork_bucket(4, 7));
    assert_ne!(
        bucket_of_1, 1,
        "the fixture is only discriminating if bucket(4, 1) != 1 (the identity key)"
    );
    assert_ne!(bucket_of_7, 7, "…and if bucket(4, 7) != 7");
    assert_ne!(
        bucket_of_1, bucket_of_7,
        "the two deleted rows must live in DIFFERENT buckets, or a single broadcast stamp \
         would be indistinguishable from a per-file one"
    );

    let ctx = SessionContext::new();
    register_source(&ctx, &[1, 7], &[Some("ignored"), Some("ignored")]);
    let spec = merge_spec("mor_bkt_del", vec![delete_matched()], vec![]);
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("transform-partitioned merge-on-read DELETE commits");

    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(2, Some("b".to_string())), (3, Some("c".to_string()))],
        "the scan applies the position deletes on a transform-partitioned table (fork R117)"
    );

    // Physical half #1: merge-on-read never rewrites. Byte-identical file set.
    assert_eq!(
        live_data_file_paths(&catalog, &ident).await,
        data_before,
        "merge-on-read must leave every ORIGINAL data file in place (a copy-on-write rewrite \
         would have replaced the affected buckets' paths)"
    );

    // Physical half #2: the stamps — one delete file per owning bucket, each naming its owner.
    let deletes = live_delete_files(&catalog, &ident).await;
    assert_eq!(
        deletes.len(),
        2,
        "one position-delete file per owning bucket (they cannot coalesce across partitions)"
    );
    let data_files = live_data_files(&catalog, &ident).await;
    let mut seen: Vec<i32> = Vec::new();
    for delete in &deletes {
        assert_eq!(
            delete.content_type(),
            DataContentType::PositionDeletes,
            "position deletes only — equality deletes are out of scope"
        );
        assert_eq!(delete.record_count(), 1, "one deleted row per bucket");
        let slot = delete_bucket_slot(delete);
        assert!(
            (0..4).contains(&slot),
            "the stamp must be a bucket ORDINAL 0..4, never the identity key: got {slot}"
        );
        // …and it names the file it really deletes from, at that row's physical ordinal.
        let owner = only_file_in_bucket(&data_files, slot);
        // id=1 is the FIRST row of its file, id=7 the SECOND of its own.
        let expected_pos = i64::from(slot != bucket_of_1);
        assert_eq!(
            decode_position_delete_file(&catalog, &ident, delete.file_path()).await,
            vec![(std::sync::Arc::<str>::from(owner.file_path()), expected_pos)],
            "the delete rows must reference the data file living in the stamped bucket"
        );
        seen.push(slot);
    }
    seen.sort_unstable();
    let mut expected = vec![bucket_of_1, bucket_of_7];
    expected.sort_unstable();
    assert_eq!(
        seen, expected,
        "the delete files must carry the OWNING files' transformed bucket ordinals — one per \
         deleted row's bucket, never one partition broadcast across both"
    );
}

/// PIN Y2 (partition-MOVING matched UPDATE, merge-on-read × transform) — the load-bearing
/// composition pin. A merge-on-read UPDATE is delete-old + insert-new, and on a transform table
/// those two halves live in DIFFERENT partitions when the update changes the partition-source
/// column: the NEW row must be routed by `RePark`'s own computed-mode fanout into
/// `bucket(new key)`, while the OLD row's position delete must be stamped `bucket(old key)` —
/// the partition of the file it deletes from, which the update did not move.
///
/// Keys are asserted to straddle a bucket boundary, so "stamped the new bucket" and "stamped
/// the old bucket" are distinguishable. Mutation M-Y-DEFAULTSPEC (stamp the default spec) → RED;
/// M-Y-GATE (restore the gate) → RED.
#[tokio::test]
async fn mor_bucket_partition_key_changing_update_splits_across_old_and_new_buckets() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_bucket_target_with(&catalog, "mor_bkt_mv", 4, mor_props()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 7], &[Some("a"), Some("g")]),
    )
    .await;
    let data_before = live_data_file_paths(&catalog, &ident).await;

    let (old_bucket, new_bucket, other_bucket) =
        (fork_bucket(4, 1), fork_bucket(4, 42), fork_bucket(4, 7));
    assert_ne!(
        old_bucket, new_bucket,
        "the keys must straddle a bucket boundary for the split to be observable"
    );
    assert_ne!(
        new_bucket, other_bucket,
        "the moved row must not land in the untouched row's bucket, or the new-file assertion \
         would not isolate it"
    );

    let ctx = SessionContext::new();
    register_source(&ctx, &[1], &[Some("ignored")]);
    let spec = merge_spec("mor_bkt_mv", vec![update_set("id", "42")], vec![]);
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("partition-key-changing merge-on-read UPDATE commits");

    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(7, Some("g".to_string())), (42, Some("a".to_string()))],
        "the row moved from id=1 to id=42 carrying its value, exactly ONCE (a missing position \
         delete would leave the old row visible too)"
    );

    // Physical: every original file survives, and the new values land in ONE fresh file.
    let data_after = live_data_files(&catalog, &ident).await;
    let paths_after: Vec<String> = data_after
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();
    for path in &data_before {
        assert!(
            paths_after.contains(path),
            "merge-on-read must leave the original data file `{path}` live"
        );
    }
    let new_files: Vec<&DataFile> = data_after
        .iter()
        .filter(|file| !data_before.contains(&file.file_path().to_string()))
        .collect();
    assert_eq!(
        new_files.len(),
        1,
        "the new values land in ONE new data file"
    );
    assert_eq!(
        bucket_slot(new_files[0]),
        new_bucket,
        "the survivor's NEW row must be routed to bucket(42) by the computed-mode fanout, not \
         left in bucket(1)"
    );

    // …while the delete stamps the OLD bucket, and references the OLD file.
    let deletes = live_delete_files(&catalog, &ident).await;
    assert_eq!(deletes.len(), 1, "the old row is position-deleted once");
    assert_eq!(
        delete_bucket_slot(&deletes[0]),
        old_bucket,
        "the position delete belongs to the OLD bucket — the update moved the row, not the \
         file the old row physically lives in"
    );
    let owner = only_file_in_bucket(&data_after, old_bucket);
    assert_eq!(
        decode_position_delete_file(&catalog, &ident, deletes[0].file_path()).await,
        vec![(std::sync::Arc::<str>::from(owner.file_path()), 0)],
        "the delete references the original bucket(1) data file at the old row's ordinal"
    );
}

/// PIN Y4 — the merge-on-read/copy-on-write DIFFERENTIAL, run on a TRANSFORM-partitioned table
/// (the T4 oracle transferred to the Group Y surface). Two `bucket(4, id)` targets differ ONLY
/// in `write.merge.mode`; the same three-clause MERGE runs against both. Rows AND the Arrow
/// schema must be identical (scan-equivalence, value + type), while the physical shapes must
/// DIVERGE — merge-on-read keeps every original data file and commits position deletes,
/// copy-on-write rewrites an original away and commits none. The copy-on-write arm is already
/// Spark-pinned on transform tables by Group R, so this transfers that pinning to the
/// merge-on-read arm without a live Spark, and it cannot be satisfied by a fallback: a
/// merge-on-read arm that secretly ran copy-on-write would make the shapes match ⇒ RED.
///
/// Mutation M-Y-COW (route the merge-on-read arm to `plan_and_commit_cow`) → RED on the
/// PHYSICAL assertions only; the row/schema assertions still pass. That asymmetry is the point.
#[tokio::test]
async fn mor_and_cow_transform_partitioned_merges_are_scan_equivalent_but_divergent() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let mor = create_bucket_target_with(&catalog, "ydiff_mor", 4, mor_props()).await;
    let cow = create_bucket_target_with(&catalog, "ydiff_cow", 4, cow_props()).await;

    // id=1 is deleted, id=2 updated, id=3 untouched, id=42 inserted. `bucket(4, 1)` and
    // `bucket(4, 2)` coincide, so the deleted and updated rows share a data file — copy-on-write
    // must rewrite it, merge-on-read must not touch it.
    for ident in [&mor, &cow] {
        append_file(
            &catalog,
            ident,
            consumer_batch(&[1, 2, 3], &[Some("drop"), Some("old"), Some("keep")]),
        )
        .await;
    }
    let mor_before = live_data_file_paths(&catalog, &mor).await;
    let cow_before = live_data_file_paths(&catalog, &cow).await;

    for name in ["ydiff_mor", "ydiff_cow"] {
        let ctx = SessionContext::new();
        register_source(&ctx, &[1, 2, 42], &[Some("X"), Some("NEW"), Some("INS")]);
        let spec = merge_spec(
            name,
            vec![
                MatchedClause {
                    predicate_sql: Some("t.v = 'drop'".to_string()),
                    action: MatchedAction::Delete,
                },
                update_set("v", "s.v"),
            ],
            vec![insert_values(&["id", "v"], &["s.id", "s.v"])],
        );
        execute_merge(&ctx, &catalog, &spec)
            .await
            .unwrap_or_else(|error| panic!("{name} transform MERGE commits: {error}"));
    }

    // --- scan-equivalence: value AND type ---
    let expected = vec![
        (2, Some("NEW".to_string())),
        (3, Some("keep".to_string())),
        (42, Some("INS".to_string())),
    ];
    let mor_rows = read_back(&catalog, &mor).await;
    assert_eq!(
        mor_rows,
        read_back(&catalog, &cow).await,
        "merge-on-read and copy-on-write must be scan-equivalent on a transform table too"
    );
    assert_eq!(mor_rows, expected, "and both must be Spark-correct");
    assert_eq!(
        read_back_schema(&catalog, &mor).await,
        read_back_schema(&catalog, &cow).await,
        "scan-equivalence is value AND type"
    );

    // --- physical divergence ---
    let mor_after = live_data_file_paths(&catalog, &mor).await;
    assert!(
        mor_before.iter().all(|path| mor_after.contains(path)),
        "merge-on-read must leave EVERY original data file live"
    );
    assert!(
        !live_delete_files(&catalog, &mor).await.is_empty(),
        "merge-on-read must commit position-delete files"
    );
    let cow_after = live_data_file_paths(&catalog, &cow).await;
    assert!(
        cow_before.iter().any(|path| !cow_after.contains(path)),
        "copy-on-write must have rewritten at least one original data file AWAY"
    );
    assert!(
        live_delete_files(&catalog, &cow).await.is_empty(),
        "copy-on-write must commit NO delete file"
    );
}

/// PIN Y5 (ORDINAL VALIDITY across SEQUENTIAL merge-on-read MERGEs on a TRANSFORM table) — T9's
/// deep question, re-asked where partitioning is transform-computed: after MERGE #1 has
/// position-deleted a row, does MERGE #2 still see each survivor's ORIGINAL physical ordinal in
/// its data file, and does the second delete file still carry the owning file's bucket ordinal?
///
/// Five keys chosen at runtime to share ONE bucket (so they land in ONE data file at physical
/// `_pos` 0..4 — derived from the FORK's own `Bucket(4)` function, never hard-coded). MERGE #1
/// deletes `ids[1]` (physical `_pos` 1); MERGE #2 deletes `ids[3]`, whose ORIGINAL ordinal is 3
/// but whose ordinal among the four SURVIVORS would be 2. The final table decides:
///   * original ordinals (correct)  ⇒ `[ids[0], ids[2], ids[4]]`
///   * renumbered survivors (wrong) ⇒ `[ids[0], ids[3], ids[4]]`
/// Both delete files must be stamped with the shared bucket ordinal and reference the ORIGINAL
/// data file, which is never rewritten.
#[tokio::test]
async fn sequential_mor_merges_on_a_transform_table_keep_original_ordinals_and_stamps() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_bucket_target_with(&catalog, "mor_bkt_seq", 4, mor_props()).await;

    // Five ascending keys that the FORK's Bucket(4) puts in the SAME bucket ⇒ ONE data file.
    let slot = fork_bucket(4, 1);
    let ids: Vec<i32> = (1..1000)
        .filter(|key| fork_bucket(4, *key) == slot)
        .take(5)
        .collect();
    assert_eq!(ids.len(), 5, "five same-bucket keys must exist below 1000");
    let vs: Vec<Option<&str>> = vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")];
    append_file(&catalog, &ident, consumer_batch(&ids, &vs)).await;
    let data_before = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        data_before.len(),
        1,
        "all five keys share a bucket ⇒ exactly one data file, ordinals 0..4"
    );

    // MERGE #1 — delete ids[1] (physical ordinal 1).
    let ctx = SessionContext::new();
    register_source(&ctx, &[ids[1]], &[Some("ignored")]);
    execute_merge(
        &ctx,
        &catalog,
        &merge_spec("mor_bkt_seq", vec![delete_matched()], vec![]),
    )
    .await
    .expect("merge-on-read MERGE #1 commits");

    // MERGE #2 — delete ids[3]: ORIGINAL ordinal 3, survivor ordinal 2 (which names ids[2]).
    let ctx = SessionContext::new();
    register_source(&ctx, &[ids[3]], &[Some("ignored")]);
    execute_merge(
        &ctx,
        &catalog,
        &merge_spec("mor_bkt_seq", vec![delete_matched()], vec![]),
    )
    .await
    .expect("merge-on-read MERGE #2 commits");

    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (ids[0], Some("a".to_string())),
            (ids[2], Some("c".to_string())),
            (ids[4], Some("e".to_string())),
        ],
        "sequential merge-on-read MERGEs on a transform table must address ORIGINAL physical \
         ordinals; renumbered survivors would have deleted id={} instead of id={}",
        ids[2],
        ids[3]
    );
    assert_eq!(
        live_data_file_paths(&catalog, &ident).await,
        data_before,
        "neither MERGE may rewrite the data file"
    );

    // Both delete files carry the shared bucket ordinal and reference the ONE original file.
    let deletes = live_delete_files(&catalog, &ident).await;
    assert_eq!(
        deletes.len(),
        2,
        "one delete file per MERGE, both still live"
    );
    let mut ordinals = Vec::new();
    for delete in &deletes {
        assert_eq!(
            delete_bucket_slot(delete),
            slot,
            "every delete file keeps the owning data file's bucket ordinal across MERGEs"
        );
        for (path, pos) in decode_position_delete_file(&catalog, &ident, delete.file_path()).await {
            assert_eq!(
                path.as_ref(),
                data_before[0].as_str(),
                "deletes reference the original data file"
            );
            ordinals.push(pos);
        }
    }
    ordinals.sort_unstable();
    assert_eq!(
        ordinals,
        vec![1, 3],
        "the two MERGEs deleted ORIGINAL ordinals 1 and 3 (a renumbering would have written 2)"
    );
}

/// Create `sales.<name>` with `id int` (required) + `k int` (OPTIONAL, the partition source) +
/// `v string` (optional), partitioned by `bucket(4, k)` — the Y7 fixture. The partition source
/// being NULLABLE is the whole point: a NULL `k` has no bucket, so its rows must route to the
/// partition's `None` slot.
async fn create_nullable_bucket_target(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    properties: HashMap<String, String>,
) -> TableIdent {
    use iceberg::spec::{Transform, UnboundPartitionSpec};
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "k", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build nullable-partition-source schema");
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "k_bucket", Transform::Bucket(4))
        .expect("add bucket partition field on the nullable column")
        .build();
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .partition_spec(spec)
        .properties(properties)
        .build();
    catalog
        .create_table(&NamespaceIdent::new("sales".to_string()), creation)
        .await
        .expect("create nullable-partition-source target");
    TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string())
}

/// An `(id Int32 non-null, k Int32 nullable, v Utf8 nullable)` batch — the Y7 consumer shape.
fn nullable_key_batch(rows: &[(i32, Option<i32>, Option<&str>)]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("k", DataType::Int32, true),
        Field::new("v", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("nullable-key batch builds")
}

/// Read `(id, k, v)` back on the Arrow scan path, sorted — the Y7 round-trip.
async fn read_back_with_key(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
) -> Vec<(i32, Option<i32>, Option<String>)> {
    let table = catalog.load_table(ident).await.expect("load table");
    let batches: Vec<RecordBatch> = table
        .scan()
        .select(["id", "k", "v"])
        .build()
        .expect("build scan")
        .to_arrow()
        .await
        .expect("scan to_arrow")
        .try_collect()
        .await
        .expect("collect scan batches");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id Int32");
        let ks = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("k Int32");
        let vs = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("v Utf8");
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                (!ks.is_null(row)).then(|| ks.value(row)),
                (!vs.is_null(row)).then(|| vs.value(row).to_string()),
            ));
        }
    }
    rows.sort();
    rows
}

/// The single partition slot of a file as an `Option<i32>` — `None` is the NULL-partition slot,
/// which is a DIFFERENT thing from "bucket ordinal 0" and must never be conflated with it.
fn optional_bucket_slot(file: &DataFile) -> Option<i32> {
    use iceberg::spec::{Literal, PrimitiveLiteral};
    match file.partition().fields().first().cloned().flatten() {
        Some(Literal::Primitive(PrimitiveLiteral::Int(bucket))) => Some(bucket),
        None => None,
        other => panic!("bucket slot must be a null-or-int literal, got {other:?}"),
    }
}

/// PIN Y7 (NULL partition source through a merge-on-read MERGE — the FORK-O7-class CONTROL) —
/// a row whose partition-source column is NULL has no bucket, so it belongs in the partition's
/// `None` slot, which is NOT bucket ordinal 0. Group O found the fork provider's overwrite path
/// mis-slotting NULLs (FORK-O7); this path is `RePark`'s OWN computed-mode fanout, so O7 does
/// not apply to it — but "does not apply" is a claim, and this pin is the measurement.
///
/// Three things are checked on the NULL row, end to end: the pre-merge APPEND puts it in the
/// `None` slot; a merge-on-read matched DELETE of a NULL-`k` row stamps its delete file with
/// the `None` slot (not `Some(0)`, and not the first slot seen); and a NOT-MATCHED INSERT of a
/// fresh NULL-`k` row routes its NEW data file to the `None` slot too. A non-NULL row is
/// updated in the same MERGE so the pin also proves NULL and non-NULL stamps do not collapse
/// onto each other.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one linear end-to-end fixture: append → MERGE → three stamps.
async fn mor_null_partition_source_row_routes_to_the_none_slot() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_nullable_bucket_target(&catalog, "mor_null", mor_props()).await;
    // id=1 (k NULL) is deleted, id=2 (k=10) is updated, id=3 (k NULL) is inserted.
    append_file(
        &catalog,
        &ident,
        nullable_key_batch(&[(1, None, Some("drop")), (2, Some(10), Some("old"))]),
    )
    .await;
    let data_before = live_data_files(&catalog, &ident).await;
    let slots_before: Vec<Option<i32>> = {
        let mut slots: Vec<Option<i32>> = data_before.iter().map(optional_bucket_slot).collect();
        slots.sort_unstable();
        slots
    };
    assert_eq!(
        slots_before,
        vec![None, Some(fork_bucket(4, 10))],
        "the append must already put the NULL-k row in the None slot, never bucket 0"
    );
    let null_owner = data_before
        .iter()
        .find(|file| optional_bucket_slot(file).is_none())
        .expect("a data file in the None slot")
        .file_path()
        .to_string();
    let paths_before: Vec<String> = {
        let mut paths: Vec<String> = data_before
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();
        paths.sort();
        paths
    };

    let ctx = SessionContext::new();
    let source = nullable_key_batch(&[
        (1, None, None),
        (2, Some(10), Some("NEW")),
        (3, None, Some("INS")),
    ]);
    let source_table =
        MemTable::try_new(source.schema(), vec![vec![source]]).expect("source memtable");
    ctx.register_table("src", Arc::new(source_table))
        .expect("register src");
    let spec = merge_spec(
        "mor_null",
        vec![
            MatchedClause {
                predicate_sql: Some("s.v IS NULL".to_string()),
                action: MatchedAction::Delete,
            },
            update_set("v", "s.v"),
        ],
        vec![insert_values(&["id", "k", "v"], &["s.id", "s.k", "s.v"])],
    );
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("merge-on-read MERGE with a NULL partition source commits");

    assert_eq!(
        read_back_with_key(&catalog, &ident).await,
        vec![
            (2, Some(10), Some("NEW".to_string())),
            (3, None, Some("INS".to_string())),
        ],
        "the NULL-k row is deleted, the non-NULL row updated once, a NULL-k row inserted"
    );

    // The delete file for the NULL-k row is stamped with the None slot.
    let deletes = live_delete_files(&catalog, &ident).await;
    assert_eq!(
        deletes.len(),
        2,
        "one delete per mutated row (DELETE + UPDATE's old row)"
    );
    let null_deletes: Vec<&DataFile> = deletes
        .iter()
        .filter(|file| optional_bucket_slot(file).is_none())
        .collect();
    assert_eq!(
        null_deletes.len(),
        1,
        "exactly ONE delete file carries the None slot — the NULL-k row's; the other must \
         carry Some(bucket(10)). Slots seen: {:?}",
        deletes.iter().map(optional_bucket_slot).collect::<Vec<_>>()
    );
    assert_eq!(
        decode_position_delete_file(&catalog, &ident, null_deletes[0].file_path()).await,
        vec![(std::sync::Arc::<str>::from(null_owner.as_str()), 0)],
        "the None-slotted delete must reference the None-slotted data file"
    );

    // The INSERTED NULL-k row's new data file is in the None slot too (the O7-class check).
    let data_after = live_data_files(&catalog, &ident).await;
    let mut new_slots: Vec<Option<i32>> = data_after
        .iter()
        .filter(|file| !paths_before.contains(&file.file_path().to_string()))
        .map(optional_bucket_slot)
        .collect();
    new_slots.sort_unstable();
    assert_eq!(
        new_slots,
        vec![None, Some(fork_bucket(4, 10))],
        "RePark's fanout must route the inserted NULL-k row to the None slot and the updated \
         k=10 row to bucket(10) — a NULL mis-slotted into bucket 0 is the FORK-O7 class"
    );
    let paths_after = live_data_file_paths(&catalog, &ident).await;
    for path in &paths_before {
        assert!(
            paths_after.contains(path),
            "merge-on-read leaves the original `{path}` live"
        );
    }
}

/// PIN Y8a (gate-retirement guard) — Group Y removed the transform gate from `resolve_merge_mode`
/// and must NOT have removed the V2-format gate that precedes it. A `bucket(4, id)` V1 table
/// asking for merge-on-read is STILL a deterministic `NotImplemented` naming V2. The
/// transform-partitioned V1 combination is the one a "delete the whole tail of the function"
/// edit would have silently accepted.
#[tokio::test]
async fn mor_on_v1_transform_partitioned_table_is_still_rejected() {
    use iceberg::spec::{FormatVersion, Transform, UnboundPartitionSpec};
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("build v1 schema");
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "id_bucket", Transform::Bucket(4))
        .expect("add bucket partition field")
        .build();
    let creation = TableCreation::builder()
        .name("mor_v1_bkt".to_string())
        .schema(schema)
        .partition_spec(spec)
        .format_version(FormatVersion::V1)
        .properties(mor_props())
        .build();
    let table = catalog
        .create_table(&NamespaceIdent::new("sales".to_string()), creation)
        .await
        .expect("create v1 bucket-partitioned table");

    let error =
        resolve_merge_mode(&table).expect_err("merge-on-read on a V1 transform table is refused");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "must be a deterministic NotImplemented: {error}"
    );
    assert!(
        error.to_string().contains("V2"),
        "the refusal must still name the V2 requirement: {error}"
    );
}

/// PIN T8b — the merge-on-read gate REFUSES a non-V2 table. Position-delete files exist only in
/// V2: V1 has no delete files at all, and V3 mandates deletion vectors the fork's
/// `PositionDeleteFileWriter` does not produce. Checked BEFORE any write, so a commit-time
/// format rejection can never orphan an already-written delete file.
#[tokio::test]
async fn mor_on_v1_table_is_rejected_before_any_write() {
    use iceberg::spec::FormatVersion;
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("build v1 schema");
    let creation = TableCreation::builder()
        .name("mor_v1".to_string())
        .schema(schema)
        .format_version(FormatVersion::V1)
        .properties(mor_props())
        .build();
    let table = catalog
        .create_table(&NamespaceIdent::new("sales".to_string()), creation)
        .await
        .expect("create v1 table");

    let error = resolve_merge_mode(&table).expect_err("merge-on-read on V1 is refused");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "must be a deterministic NotImplemented: {error}"
    );
    assert!(
        error.to_string().contains("V2"),
        "the refusal must name the V2 requirement: {error}"
    );
}

/// PIN T8c — mode RESOLUTION itself: unset and `copy-on-write` both resolve to the
/// copy-on-write arm (Iceberg's default), `merge-on-read` resolves to the new arm, and an
/// unrecognised value is a loud `NotImplemented` rather than a silent fallback to either arm
/// (the failure mode a bare `!= "copy-on-write"` check or a `== "merge-on-read"` check would
/// each produce, in opposite directions).
#[tokio::test]
async fn merge_mode_resolves_from_the_table_property() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    for (name, properties, expected) in [
        ("mode_unset", HashMap::new(), Some(MergeMode::CopyOnWrite)),
        ("mode_cow", cow_props(), Some(MergeMode::CopyOnWrite)),
        ("mode_mor", mor_props(), Some(MergeMode::MergeOnRead)),
        (
            "mode_bogus",
            HashMap::from([(MERGE_MODE_PROP.to_string(), "banana".to_string())]),
            None,
        ),
    ] {
        let ident = create_target_with(&catalog, name, properties).await;
        let table = catalog.load_table(&ident).await.expect("load table");
        match (resolve_merge_mode(&table), expected) {
            (Ok(mode), Some(want)) => assert_eq!(mode, want, "{name} resolves to {want:?}"),
            (Err(error), None) => {
                assert!(
                    matches!(error, DataFusionError::NotImplemented(_))
                        && error.to_string().contains("banana"),
                    "{name} must be a loud NotImplemented naming the value: {error}"
                );
            }
            (Ok(mode), None) => panic!("{name} must NOT silently resolve to {mode:?}"),
            (Err(error), Some(want)) => panic!("{name} must resolve to {want:?}: {error}"),
        }
    }
}

/// The current snapshot id (for "did this MERGE commit anything at all?" assertions).
async fn snapshot_id(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Option<i64> {
    catalog
        .load_table(ident)
        .await
        .expect("load table")
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id())
}

/// PIN T9 (ORDINAL VALIDITY across sequential merge-on-read MERGEs) — the deep correctness
/// question this whole arm rests on: after MERGE #1 has position-deleted a row, does the NEXT
/// scan report each surviving row's ORIGINAL physical ordinal in its data file, or does it
/// renumber the survivors? Position deletes are `(file, physical ordinal)`; if a re-scan
/// renumbered, MERGE #2's `_pos` would name a DIFFERENT physical row and delete the wrong one —
/// silently, with no error anywhere.
///
/// Five rows in ONE data file. MERGE #1 deletes id=2 (physical `_pos` 1). MERGE #2 then deletes
/// id=4, whose ORIGINAL ordinal is 3 but whose ordinal among the four SURVIVORS would be 2. The
/// answer is decided by the final table:
///   * original ordinals (correct)  ⇒ `[1, 3, 5]`
///   * renumbered survivors (wrong) ⇒ `[1, 4, 5]` (a `_pos` of 2 names id=3)
/// Confirmed against fork source (`reader.rs` threads a per-file counter over the WHOLE file,
/// independent of applied deletes) — this pin holds that guarantee to execution, so a future
/// fork change that renumbered would fail HERE instead of corrupting data.
///
/// MERGE #3 then targets the ALREADY-deleted id=2: it must be a pure no-op — the scan no longer
/// sees that row, so nothing matches, and a MERGE that changes nothing commits nothing (same
/// snapshot id, no phantom delete file re-deleting an already-deleted ordinal).
#[tokio::test]
async fn sequential_mor_merges_use_original_physical_ordinals() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target_with(&catalog, "mor_seq", mor_props()).await;
    // ONE data file, five rows at physical _pos 0..4.
    append_file(
        &catalog,
        &ident,
        consumer_batch(
            &[1, 2, 3, 4, 5],
            &[Some("a"), Some("b"), Some("c"), Some("d"), Some("e")],
        ),
    )
    .await;

    // --- MERGE #1: delete id=2 (physical _pos 1) ---
    let ctx = SessionContext::new();
    register_source(&ctx, &[2], &[Some("x")]);
    execute_merge(
        &ctx,
        &catalog,
        &merge_spec("mor_seq", vec![delete_matched()], vec![]),
    )
    .await
    .expect("MERGE #1 commits");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (1, Some("a".to_string())),
            (3, Some("c".to_string())),
            (4, Some("d".to_string())),
            (5, Some("e".to_string())),
        ],
        "MERGE #1 removes exactly id=2"
    );

    // --- MERGE #2: delete id=4 — ORIGINAL _pos 3, SURVIVOR ordinal 2 ---
    let ctx = SessionContext::new();
    register_source(&ctx, &[4], &[Some("x")]);
    execute_merge(
        &ctx,
        &catalog,
        &merge_spec("mor_seq", vec![delete_matched()], vec![]),
    )
    .await
    .expect("MERGE #2 commits");
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (1, Some("a".to_string())),
            (3, Some("c".to_string())),
            (5, Some("e".to_string())),
        ],
        "the re-scan must report ORIGINAL physical ordinals: id=4 lives at _pos 3, not at the \
         survivor ordinal 2. A renumbering scan would have deleted id=3 and left [1, 4, 5]"
    );
    // Both original data files are still live; two delete files now exist (one per MERGE).
    assert_eq!(
        live_data_file_paths(&catalog, &ident).await.len(),
        1,
        "merge-on-read never rewrote the single data file"
    );
    assert_eq!(
        live_delete_files(&catalog, &ident).await.len(),
        2,
        "one position-delete file per committed MERGE"
    );

    // --- MERGE #3: target the ALREADY-deleted id=2 ⇒ a pure no-op ---
    let before = snapshot_id(&catalog, &ident).await;
    let ctx = SessionContext::new();
    register_source(&ctx, &[2], &[Some("x")]);
    execute_merge(
        &ctx,
        &catalog,
        &merge_spec("mor_seq", vec![delete_matched()], vec![]),
    )
    .await
    .expect("MERGE #3 succeeds (as a no-op)");
    assert_eq!(
        snapshot_id(&catalog, &ident).await,
        before,
        "a MERGE that matches nothing must commit NOTHING — no empty snapshot"
    );
    assert_eq!(
        live_delete_files(&catalog, &ident).await.len(),
        2,
        "and must not write a phantom delete file re-deleting an already-deleted ordinal"
    );
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (1, Some("a".to_string())),
            (3, Some("c".to_string())),
            (5, Some("e".to_string())),
        ],
        "the table is unchanged"
    );
}

/// Decode a committed position-delete Parquet file into its `(file_path, pos)` rows, in FILE
/// ORDER — read back through the table's own `FileIO`, so this is what a Java reader would see.
async fn decode_position_delete_file(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    path: &str,
) -> Vec<PositionDeletePair> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let table = catalog.load_table(ident).await.expect("load table");
    let bytes = table
        .file_io()
        .new_input(path)
        .expect("open delete file")
        .read()
        .await
        .expect("read delete file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("parquet reader")
        .build()
        .expect("build parquet reader");
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.expect("read delete-file batch");
        let paths = batch
            .column_by_name("file_path")
            .expect("delete file has a `file_path` column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("`file_path` is Utf8");
        let positions = batch
            .column_by_name("pos")
            .expect("delete file has a `pos` column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("`pos` is Int64");
        for row in 0..batch.num_rows() {
            rows.push((
                std::sync::Arc::<str>::from(paths.value(row)),
                positions.value(row),
            ));
        }
    }
    rows
}

/// PIN T-SORT-ONDISK — the Iceberg spec's ascending `(file_path, pos)` ordering survives all the
/// way ONTO DISK, and the deletes for several data files are coalesced into ONE delete file.
/// `sorts_pairs_by_file_then_position` pins the helper in isolation; this pins the property a
/// Java reader actually depends on, by DECODING the committed Parquet through the table's own
/// `FileIO`. The gap it closes is real: the helper could be correct and still never be reached
/// (or be reached before the pairs are assembled), and the scan produces the pairs in arbitrary
/// interleaved file order, so unsorted output is the natural failure.
///
/// Three data files; matches in files #1 and #3 only, three matched rows total. Sortedness is
/// asserted structurally (the decoded rows equal their own sorted copy) rather than against a
/// hard-coded order, because the writer's UUID file names make the lexicographic order of the
/// two referenced paths unknowable up front — while the SET of referenced `(path, pos)` pairs is
/// pinned exactly, so a sorted-but-wrong file cannot pass.
#[tokio::test]
async fn position_delete_file_is_sorted_on_disk_and_coalesced() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target_with(&catalog, "mor_sorted", mor_props()).await;
    // Three separate appends ⇒ three data files, each with its own _pos 0..1.
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2], &[Some("a"), Some("b")]),
    )
    .await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[3, 4], &[Some("c"), Some("d")]),
    )
    .await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[5, 6], &[Some("e"), Some("f")]),
    )
    .await;
    let data_before = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(data_before.len(), 3, "three data files");

    // Delete id=1 and id=2 (both in file #1, _pos 0 and 1) and id=5 (file #3, _pos 0).
    // File #2 is untouched, so the delete file must reference exactly TWO data files.
    let ctx = SessionContext::new();
    register_source(&ctx, &[1, 2, 5], &[Some("x"), Some("y"), Some("z")]);
    execute_merge(
        &ctx,
        &catalog,
        &merge_spec("mor_sorted", vec![delete_matched()], vec![]),
    )
    .await
    .expect("multi-file merge-on-read DELETE commits");

    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![
            (3, Some("c".to_string())),
            (4, Some("d".to_string())),
            (6, Some("f".to_string())),
        ],
    );

    // ONE delete file for all three deletes: the table is unpartitioned, so every pair resolves
    // to the same (spec_id, partition) group and coalesces.
    let deletes = live_delete_files(&catalog, &ident).await;
    assert_eq!(
        deletes.len(),
        1,
        "deletes spanning several data files coalesce into ONE delete file (same partition group)"
    );
    assert_eq!(deletes[0].record_count(), 3, "three deleted rows");
    assert_eq!(
        deletes[0].content_type(),
        DataContentType::PositionDeletes,
        "position deletes, not equality deletes"
    );

    let rows = decode_position_delete_file(&catalog, &ident, deletes[0].file_path()).await;
    assert_eq!(rows.len(), 3, "the file on disk carries all three deletes");
    let mut sorted = rows.clone();
    sorted.sort();
    assert_eq!(
        rows, sorted,
        "the Iceberg spec requires position-delete rows ascending by (file_path, pos), and the \
         scan produces them interleaved — the sort must survive onto disk, got: {rows:?}"
    );
    // HONESTY (measured, not assumed): this sortedness assertion is a real guard but only a
    // PROBABILISTIC mutation carrier — with the sort no-op'd it reddens ~4 runs in 10, because
    // the scan's join output order is sometimes already ascending for this small fixture. The
    // DETERMINISTIC carrier for "the sort reaches disk" is
    // `write_position_deletes_sorts_reverse_ordered_pairs_onto_disk` below, which feeds
    // deliberately reverse-ordered pairs. What THIS pin carries deterministically is the
    // coalescing + record_count + exact-referenced-set half asserted around it.

    // The exact rows: one referenced file at _pos 0 and 1 (ids 1,2), the other at _pos 0 (id 5),
    // and the untouched file never referenced. `data_before` is path-sorted rather than
    // append-sorted (UUID file names), so the files are identified by their delete SHAPE, not by
    // index — which is the property that matters and is stable.
    let mut referenced: Vec<String> = rows
        .iter()
        .map(|(path, _)| path.as_ref().to_string())
        .collect();
    referenced.sort();
    referenced.dedup();
    assert_eq!(
        referenced.len(),
        2,
        "exactly the TWO data files that contained a matched row are referenced (the untouched \
         file must not appear), got: {referenced:?}"
    );
    assert!(
        referenced.iter().all(|path| data_before.contains(path)),
        "every referenced path must be one of the pre-merge data files"
    );
    let positions_per_file: HashMap<&str, Vec<i64>> =
        referenced.iter().fold(HashMap::new(), |mut acc, path| {
            acc.insert(
                path.as_str(),
                rows.iter()
                    .filter(|(p, _)| p.as_ref() == path.as_str())
                    .map(|(_, pos)| *pos)
                    .collect(),
            );
            acc
        });
    let mut shapes: Vec<Vec<i64>> = positions_per_file.into_values().collect();
    shapes.sort();
    assert_eq!(
        shapes,
        vec![vec![0], vec![0, 1]],
        "one referenced file contributes _pos 0 and 1 (ids 1,2), the other just _pos 0 (id 5)"
    );
}

/// PIN T-SORT-ONDISK-DET — the DETERMINISTIC companion to
/// `position_delete_file_is_sorted_on_disk_and_coalesced`: drive `write_position_deletes`
/// directly with pairs in deliberately REVERSE `(file_path, pos)` order and decode the Parquet
/// it produced. Because the input order is chosen rather than observed, no-op'ing
/// `sort_position_delete_pairs` reddens this EVERY run — where the end-to-end pin only reddens
/// when the scan happens to emit unsorted order (~4 runs in 10, measured).
///
/// This is the pin that actually holds "the sort survives from the helper onto disk": the helper
/// unit test proves the comparator, this proves the write path calls it.
#[tokio::test]
async fn write_position_deletes_sorts_reverse_ordered_pairs_onto_disk() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target_with(&catalog, "mor_sortdet", mor_props()).await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[1, 2, 3], &[Some("a"), Some("b"), Some("c")]),
    )
    .await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[4, 5, 6], &[Some("d"), Some("e"), Some("f")]),
    )
    .await;
    // `live_data_file_paths` sorts, so `files[0] < files[1]` lexicographically.
    let files = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(files.len(), 2, "two data files");

    // Strictly DESCENDING by (file_path, pos) — the exact reverse of the required order.
    let path0: Arc<str> = Arc::from(files[0].as_str());
    let path1: Arc<str> = Arc::from(files[1].as_str());
    let pairs: Vec<PositionDeletePair> = vec![
        (Arc::clone(&path1), 2),
        (Arc::clone(&path1), 0),
        (Arc::clone(&path0), 2),
        (Arc::clone(&path0), 1),
    ];
    let table = catalog.load_table(&ident).await.expect("load table");
    let written = crate::write::position_delete::write_position_deletes(
        &table,
        &pairs,
        crate::write::concurrency::WriteConcurrency::default(),
    )
    .await
    .expect("write position deletes");
    assert_eq!(
        written.len(),
        1,
        "one unpartitioned group ⇒ one delete file"
    );

    let rows = decode_position_delete_file(&catalog, &ident, written[0].file_path()).await;
    assert_eq!(
        rows,
        vec![
            (Arc::clone(&path0), 1),
            (Arc::clone(&path0), 2),
            (Arc::clone(&path1), 0),
            (Arc::clone(&path1), 2),
        ],
        "reverse-ordered input must be written ascending by (file_path, pos) — the Iceberg spec \
         requirement the fork's write-as-given writer leaves to us"
    );
}

/// P2a hour-0 evidence for scout #6: serial `resolve_affected_data_files` share of a
/// same-process multi-file COW MERGE wall on local-fs. Concurrent resolve is **not**
/// implemented when the share is under 10% (WIN close with the number).
///
/// Measures resolve in isolation after a warm load, and full `execute_merge` wall on a
/// fresh 8-file target of the same shape. Ratio is profile-stable (debug vs release both
/// scale). Python hour-0 release wheel re-derived COW merge ~0.18 s at 8×5k rows.
#[tokio::test]
async fn resolve_affected_data_files_local_fs_is_sub_10pct_of_merge_budget() {
    use std::time::Instant;

    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;

    // --- resolve absolute (8 files, warm) ---
    let ident_resolve = create_target(&catalog, "p2a_resolve").await;
    for file_index in 0..8_i32 {
        let base = file_index * 100;
        append_file(
            &catalog,
            &ident_resolve,
            consumer_batch(
                &[base, base + 1, base + 2, base + 3, base + 4],
                &[Some("a"), Some("b"), Some("c"), Some("d"), Some("e")],
            ),
        )
        .await;
    }
    let table = catalog.load_table(&ident_resolve).await.expect("load");
    let paths = live_data_file_paths(&catalog, &ident_resolve).await;
    assert_eq!(paths.len(), 8, "eight seed files");
    let _ = resolve_affected_data_files(&table, &paths)
        .await
        .expect("warm resolve");
    let resolve_rounds = 10_u32;
    let t_resolve = Instant::now();
    for _ in 0..resolve_rounds {
        let resolved = resolve_affected_data_files(&table, &paths)
            .await
            .expect("resolve");
        assert_eq!(resolved.len(), paths.len());
    }
    let resolve_ms = t_resolve.elapsed().as_secs_f64() * 1000.0 / f64::from(resolve_rounds);

    // --- full COW MERGE wall (same 8-file shape, update a row in every file) ---
    let ident_merge = create_target(&catalog, "p2a_merge_wall").await;
    for file_index in 0..8_i32 {
        let base = file_index * 100;
        append_file(
            &catalog,
            &ident_merge,
            consumer_batch(
                &[base, base + 1, base + 2, base + 3, base + 4],
                &[Some("a"), Some("b"), Some("c"), Some("d"), Some("e")],
            ),
        )
        .await;
    }
    let ctx = SessionContext::new();
    // Touch one id per file so every data file is affected (worst-case resolve set).
    register_source(
        &ctx,
        &[0, 100, 200, 300, 400, 500, 600, 700],
        &[
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G"),
            Some("H"),
        ],
    );
    let spec = merge_spec("p2a_merge_wall", vec![update_set("v", "s.v")], Vec::new());
    // Warm plan path once, then time a second MERGE is not free (table already mutated);
    // time the first execute_merge as the wall (includes resolve).
    let t_merge = Instant::now();
    execute_merge(&ctx, &catalog, &spec)
        .await
        .expect("COW merge commits");
    let merge_ms = t_merge.elapsed().as_secs_f64() * 1000.0;
    let share_pct = if merge_ms > 0.0 {
        100.0 * resolve_ms / merge_ms
    } else {
        0.0
    };
    // Absolute floor: resolve must finish — regression guard against multi-second hangs.
    assert!(
        resolve_ms < 500.0,
        "resolve_affected avg {resolve_ms:.3} ms looks hung on local-fs"
    );
    // Scout #6 gate is profile-sensitive: debug inflates both sides and the ratio on
    // tiny fixtures. The authoritative hour-0 is release (Python wheel + this test under
    // `--release`): 2026-08-03 release measured ~5.4% — under the 10% implement bar.
    // In debug, only pin absolute hang; in release, pin share <10%.
    #[cfg(not(debug_assertions))]
    assert!(
        share_pct < 10.0,
        "resolve_affected avg {resolve_ms:.3} ms is {share_pct:.1}% of MERGE wall \
         {merge_ms:.1} ms (gate 10%); re-open scout #6 concurrent resolve if this trips"
    );
    let _ = share_pct; // used under release; keep debug clean of unused-var
    eprintln!(
        "p2a_hour0_#6: resolve_ms={resolve_ms:.3} merge_ms={merge_ms:.1} share_pct={share_pct:.2}"
    );
}
