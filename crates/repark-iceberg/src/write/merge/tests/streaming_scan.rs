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

use super::super::*;

/// A [`PartitionStream`] over a scripted batch list that counts how many batches it has PRODUCED.
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

/// PIN K-MEM.
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
    // Structural bind: the registered provider MUST be a StreamingTable, never a MemTable.
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

/// [`create_target`] with table properties.
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

/// Append `batch` as its OWN data file.
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
        not_matched_by_source: vec![],
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

/// PIN K-IDENTITY: a streamed re-scanned target keeps `(_file, _pos)` identity across data files.
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
#[tokio::test]
async fn cow_discovery_path_allocs_scale_with_files_not_matched_rows() {
    // --- n matched rows, 1 file ----------------------------------------------------------.
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

    // --- 10× rows, still 1 file ----------------------------------------------------------.
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

/// `MoR` upsert issues two logical target-SQL consumptions: `matched_work` plus insert anti-join.
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

/// PERF-04: COW equi-key upsert with default pruning and file-scoped rewrite keeps survivors.
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

/// PERF-04 pin: `MoR` equi-key upsert under residual still updates + inserts correctly.
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

/// PERF-04: COW with `file-scoped-rewrite=false` must not push residual.
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

/// PERF-04: `repark.merge.scan-pruning=false` must not push residual even for an equi Int32 ON.
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

/// M1 scan-level pin: Utf8 source keys vs Int32 target must **not** push residual.
#[tokio::test]
async fn utf8_source_int32_target_does_not_push_residual() {
    let push_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "m1_utf8").await;
    append_file(
        &catalog,
        &ident,
        consumer_batch(&[9, 10], &[Some("x"), Some("y")]),
    )
    .await;
    let ctx = SessionContext::new();
    let source_batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["9", "10"])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .expect("utf8 source batch");
    let source = MemTable::try_new(source_batch.schema(), vec![vec![source_batch]])
        .expect("utf8 source memtable");
    ctx.register_table("src", Arc::new(source))
        .expect("register utf8 src");
    let spec = merge_spec("m1_utf8", vec![update_set("v", "s.v")], vec![]);
    let instruments = MergeTestInstruments {
        residual_push: Some(std::sync::Arc::clone(&push_counter)),
        ..MergeTestInstruments::default()
    };
    MERGE_TEST_INSTRUMENTS
        .scope(instruments, async {
            execute_merge(&ctx, &catalog, &spec)
                .await
                .expect("Utf8→Int32 MERGE must not abort");
        })
        .await;
    assert_eq!(
        push_counter.load(Ordering::SeqCst),
        0,
        "M1: Utf8 source vs Int32 target must skip residual (got a push)"
    );
    assert_eq!(
        read_back(&catalog, &ident).await,
        vec![(9, Some("a".to_string())), (10, Some("b".to_string()))],
        "M1: both INT keys must update; inverted Utf8 bounds lose id=9"
    );
}

/// R-MERGE-FILE-SCAN pin: multi-file COW where only file B is affected.
#[tokio::test]
async fn cow_file_scoped_rewrite_opens_only_affected_files_and_keeps_survivors() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_target(&catalog, "scoped").await;
    // Three files: A={1,2}, B={10,11}, C={20}.
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

    // --- file-scoped path (default conf true) -------------------------------------------.
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

    // --- escape hatch: conf false still correct -----------------------------------------.
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

    // --- COW -----------------------------------------------------------------------------.
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

    // --- MoR twin ------------------------------------------------------------------------.
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

/// NULL-predicate first-match (3VL) under `clause_id` — COW + `MoR`.
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
    // id=1 flag=NULL takes clause1; id=2 flag=1 takes clause0; id=3 is an unmatched survivor.
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

/// NOT MATCHED multi-clause first-match via `clause_id = index`.
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

/// Multi NOT MATCHED first-match against a **non-empty** target (anti-join).
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

/// Two sequential COW MERGEs on one `SessionContext` with file-scoped off.
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

/// PIN K-CARDINALITY: the cardinality check groups on `(_file, _pos)`, not `_file` alone.
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

/// PIN K-EMPTY.
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

// Group R: MERGE INTO a non-identity transform-partitioned table (`reject_unsupported` gate).

/// Create `sales.<name>` with `id int` + `v string`, partitioned by `bucket`.
async fn create_bucket_target(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    num_buckets: u32,
) -> TableIdent {
    create_bucket_target_with(catalog, name, num_buckets, HashMap::new()).await
}

/// [`create_bucket_target`] with table properties.
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

/// The live DATA-file entries in the current snapshot's manifests.
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

/// The single bucket-ordinal partition slot of a `DataFile`.
fn bucket_slot(file: &DataFile) -> i32 {
    use iceberg::spec::{Literal, PrimitiveLiteral};
    match file.partition().fields().first().cloned().flatten() {
        Some(Literal::Primitive(PrimitiveLiteral::Int(bucket))) => bucket,
        other => panic!("bucket partition slot must be a non-null int literal, got {other:?}"),
    }
}

/// The fork's OWN `Transform::Bucket` ordinal for a key.
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

/// PIN R1.
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

    // Manifest totals per bucket equal the fork's Bucket(4) routing of the final key set {1,2,3,8}.
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

/// PIN R2.
#[tokio::test]
async fn merge_bucket_partition_key_changing_update_reroutes_survivor() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_bucket_target(&catalog, "mv", 4).await;
    // Base rows id 1 and 7.
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

    // Manifest metadata: every surviving row's file carries the bucket of its CURRENT key.
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

// GROUP T — merge-on-read MERGE INTO pins (T1-T4, T7, T8 gates).

/// Table properties selecting merge-on-read.
fn mor_props() -> HashMap<String, String> {
    HashMap::from([
        (MERGE_MODE_PROP.to_string(), "merge-on-read".to_string()),
        // pins: mw-9-delete-granularity/C-008 — these fixtures were written against
        // implicit partition grouping; keep that layout after the Spark-default flip.
        (
            crate::write::position_delete::DELETE_GRANULARITY_PROP.to_string(),
            "partition".to_string(),
        ),
    ])
}

/// Table properties selecting copy-on-write EXPLICITLY.
fn cow_props() -> HashMap<String, String> {
    HashMap::from([(MERGE_MODE_PROP.to_string(), "copy-on-write".to_string())])
}

/// The live DELETE-file entries in the current snapshot's DELETE manifests.
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

/// The Arrow SCHEMA the read-back scan produces — the TYPE half of the T4 differential.
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

/// R-MERGE-TRACING: on a local `MoR` MERGE, all five phase spans fire and `merge.commit` is last.
#[tokio::test]
async fn mor_merge_emits_five_phase_spans_with_commit_last() {
    use tracing::Instrument;

    // v1 installed THIS test's own process-global subscriber.
    let recorded = crate::tests::tracing::merge_span_names();
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

/// PIN T1.
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

/// PIN QA-176 — position deletes scope to the data file they name, across a MULTI-file scan.
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

    // One MERGE deleting pos-1 of both files commits position-deletes that span two data files.
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

/// PIN T2.
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

/// PIN T3 — a pure insert writes a new data file and NO delete file at all.
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

/// PIN T4 — THE differential oracle (the whole point of Group T).
#[tokio::test]
async fn mor_and_cow_merges_are_scan_equivalent_but_physically_different() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let mor = create_target_with(&catalog, "diff_mor", mor_props()).await;
    let cow = create_target_with(&catalog, "diff_cow", cow_props()).await;

    // Same two data files: id=1 deleted, id=2 updated, id=3 untouched, id=9 inserted.
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

    // --- scan-equivalence: value AND type ---.
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

    // --- physical divergence ---.
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

/// Create `sales.<name>` with `id int` plus `v string`, IDENTITY-partitioned on `id`.
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

/// PIN T7.
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

// GROUP Y — merge-on-read MERGE × NON-IDENTITY TRANSFORM partitioning (Y1, Y2, Y4, Y5, Y7, Y8a).

/// The single bucket-ordinal slot of a committed DELETE file.
fn delete_bucket_slot(file: &DataFile) -> i32 {
    bucket_slot(file)
}

/// The live data file whose bucket slot is `slot`.
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

/// PIN Y1.
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

    // The discriminating facts: each deleted row's TRANSFORMED partition differs from its key.
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

    // Physical half #1: merge-on-read never rewrites.
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

/// PIN Y2 — the load-bearing composition pin.
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

/// PIN Y4 — the merge-on-read/copy-on-write DIFFERENTIAL, run on a TRANSFORM-partitioned table.
#[tokio::test]
async fn mor_and_cow_transform_partitioned_merges_are_scan_equivalent_but_divergent() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let mor = create_bucket_target_with(&catalog, "ydiff_mor", 4, mor_props()).await;
    let cow = create_bucket_target_with(&catalog, "ydiff_cow", 4, cow_props()).await;

    // id=1 is deleted, id=2 updated, id=3 untouched, id=42 inserted.
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

    // --- scan-equivalence: value AND type ---.
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

    // --- physical divergence ---.
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

/// PIN Y5.
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

/// Create `sales.<name>` with `id int` + `k int` + `v string`, partitioned by `bucket`.
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

/// The single partition slot of a file as an `Option<i32>`.
fn optional_bucket_slot(file: &DataFile) -> Option<i32> {
    use iceberg::spec::{Literal, PrimitiveLiteral};
    match file.partition().fields().first().cloned().flatten() {
        Some(Literal::Primitive(PrimitiveLiteral::Int(bucket))) => Some(bucket),
        None => None,
        other => panic!("bucket slot must be a null-or-int literal, got {other:?}"),
    }
}

/// PIN Y7.
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

/// PIN Y8a.
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

/// PIN T8b — the merge-on-read gate REFUSES a non-V2 table.
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

/// PIN T8c.
#[tokio::test]
async fn merge_mode_resolves_from_the_table_property() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    for (name, properties, expected) in [
        ("mode_unset", HashMap::new(), Some(MergeMode::CopyOnWrite)),
        ("mode_cow", cow_props(), Some(MergeMode::CopyOnWrite)),
        ("mode_mor", mor_props(), Some(MergeMode::MergeOnRead)),
        // Iceberg-Java mode names are case-insensitive; mixed-case or padded values must resolve.
        (
            "mode_mor_upper",
            HashMap::from([(MERGE_MODE_PROP.to_string(), "MERGE-ON-READ".to_string())]),
            Some(MergeMode::MergeOnRead),
        ),
        (
            "mode_cow_padded",
            HashMap::from([(MERGE_MODE_PROP.to_string(), " Copy-On-Write ".to_string())]),
            Some(MergeMode::CopyOnWrite),
        ),
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

/// PIN T9.
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

    // --- MERGE #1: delete id=2 (physical _pos 1) ---.
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

    // --- MERGE #2: delete id=4 — ORIGINAL _pos 3, SURVIVOR ordinal 2 ---.
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

    // --- MERGE #3: target the ALREADY-deleted id=2 ⇒ a pure no-op ---.
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

/// Decode a committed position-delete Parquet file into `(file_path, pos)` rows, in file order.
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

/// PIN T-SORT-ONDISK.
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

    // One delete file for all three deletes: unpartitioned pairs share the same partition group.
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
    // HONESTY: this sortedness assertion is a real guard but only a PROBABILISTIC mutation carrier.

    // Exact rows: one file at _pos 0,1 (ids 1,2), the other at _pos 0 (id 5); third is untouched.
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

/// PIN T-SORT-ONDISK-DET.
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

/// P2a hour-0: serial `resolve_affected_data_files` share of a multi-file COW MERGE wall.
#[tokio::test]
async fn resolve_affected_data_files_local_fs_is_sub_10pct_of_merge_budget() {
    use std::time::Instant;

    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;

    // --- resolve absolute (8 files, warm) ---.
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

    // --- full COW MERGE wall (same 8-file shape, update a row in every file) ---.
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
    // Warm plan path once, then time a second MERGE is not free.
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
