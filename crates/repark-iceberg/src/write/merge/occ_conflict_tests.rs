//! M19/M20 OCC conflict batteries B/C/E/F/G/H/I (M14 abort-path cleanup lives in the
//! commit functions; this battery flips the orphan characterization).
//!
//! House style matches [`super::occ_tests`]: `MemoryCatalog` + local-FS warehouse + synthetic
//! `DataFile`s. Two-handle races are sequential (the MERGE executor serializes under
//! `cfg(test)`); they are not threaded. Each battery names its audit letter in the test
//! doc comment. No `#[ignore]`. Pins name M14/M15/M20 rather than xfail.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::DataFusionError;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
    NestedField, Operation, PrimitiveType, Schema, Struct, Transform, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::{
    IsolationLevel, OPERATION_ID_PROP, RowDeltaKind, RowDeltaPolicy, commit, commit_row_delta,
    commit_row_delta_kind, write_data_files,
};

/// An in-memory Iceberg catalog (local-FS warehouse, format-version 2 by default) with a
/// `sales` namespace and one UNPARTITIONED table `t (id int)`. No AWS, no network.
async fn setup(warehouse: &TempDir) -> (Arc<dyn Catalog>, TableIdent) {
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
    let namespace = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("build schema");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    (catalog, TableIdent::new(namespace, "t".to_string()))
}

/// Partitioned twin of [`setup`]: identity on `part`, so two files can live in different
/// partitions without a real Parquet payload.
async fn setup_partitioned(warehouse: &TempDir) -> (Arc<dyn Catalog>, TableIdent) {
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
    let namespace = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "part", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("build schema");
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "part", Transform::Identity)
        .expect("add identity partition field")
        .build();
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .partition_spec(spec)
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create partitioned table");
    (catalog, TableIdent::new(namespace, "t".to_string()))
}

/// An unpartitioned synthetic DATA file (manifest-only) with a unique path.
fn data_file(path: &str) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .expect("build data file")
}

/// A partitioned synthetic DATA file carrying identity `part = partition_value`.
fn data_file_in_partition(path: &str, spec_id: i32, partition_value: i32) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(1)
        .partition_spec_id(spec_id)
        .partition(Struct::from_iter([Some(Literal::int(partition_value))]))
        .build()
        .expect("build partitioned data file")
}

/// Fast-append `files` in one commit; return the table AT the new snapshot and that id.
async fn append(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    files: Vec<DataFile>,
) -> (Table, i64) {
    let table = catalog.load_table(ident).await.expect("load table");
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(files);
    let tx = action.apply(tx).expect("apply fast_append");
    let table = tx
        .commit(catalog.as_ref())
        .await
        .expect("commit fast_append");
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .snapshot_id();
    (table, snapshot_id)
}

/// A concurrent DELETE-only overwrite that removes `files` (adds NO data files).
async fn concurrent_delete(catalog: &Arc<dyn Catalog>, ident: &TableIdent, files: Vec<DataFile>) {
    let table = catalog.load_table(ident).await.expect("load table");
    let tx = Transaction::new(&table);
    let action = tx.overwrite_files().delete_data_files(files);
    let tx = action.apply(tx).expect("apply delete-only overwrite");
    tx.commit(catalog.as_ref())
        .await
        .expect("commit delete-only overwrite");
}

/// The set of live (Added/Existing) DATA-file paths in the table's current snapshot.
async fn live_data_file_paths(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> HashSet<String> {
    let table = catalog.load_table(ident).await.expect("load table");
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return HashSet::new();
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("manifest list");
    let mut live = HashSet::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("manifest");
        for entry in manifest.entries() {
            if entry.is_alive() {
                live.insert(entry.file_path().to_string());
            }
        }
    }
    live
}

/// Downcast `commit`'s folded `DataFusionError::External` back to the iceberg error it wraps.
fn iceberg_error(error: &DataFusionError) -> &iceberg::Error {
    let DataFusionError::External(boxed) = error else {
        panic!("expected an External(iceberg) error, got: {error}");
    };
    boxed
        .downcast_ref::<iceberg::Error>()
        .expect("the crate's iceberg_err fold wraps an iceberg::Error")
}

fn default_concurrency() -> crate::write::concurrency::WriteConcurrency {
    crate::write::concurrency::WriteConcurrency::default()
}

fn id_batch(values: &[i32]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
        .expect("id batch builds")
}

async fn current_snapshot(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
) -> iceberg::spec::Snapshot {
    catalog
        .load_table(ident)
        .await
        .expect("load table")
        .metadata()
        .current_snapshot()
        .expect("current snapshot")
        .as_ref()
        .clone()
}

async fn path_exists(table: &Table, path: &str) -> bool {
    table.file_io().exists(path).await.expect("FileIO exists")
}

// ===================================================================================
// BATTERY B — `RowDeltaKind::Delete` vs Merge-kind on a concurrent DELETE-op removal.
// ===================================================================================

/// Battery B — `commit_row_delta_kind` with `RowDeltaKind::Delete` at snapshot isolation.
/// Concurrent DELETE-op file removal is TOLERATED (`validate_deleted_files` is NOT armed;
/// `validate_data_files_exist` without that flag inspects only `{OVERWRITE}`). Risk: a
/// kind-blind recipe that always arms the UPDATE/MERGE guards would reject a legal
/// snapshot-isolation DELETE.
#[tokio::test]
async fn commit_row_delta_kind_delete_snapshot_tolerates_concurrent_delete_op_removal() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    let a = data_file("test/a.parquet");
    let (table_at_pin, pin) = append(
        &catalog,
        &ident,
        vec![a.clone(), data_file("test/b.parquet")],
    )
    .await;

    // DELETE-op snapshot (delete-only overwrite records Operation::Delete).
    concurrent_delete(&catalog, &ident, vec![a]).await;

    commit_row_delta_kind(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        Vec::new(),
        default_concurrency(),
        RowDeltaPolicy {
            kind: RowDeltaKind::Delete,
            isolation: IsolationLevel::Snapshot,
        },
    )
    .await
    .expect(
        "Delete-kind + snapshot must tolerate a concurrent DELETE-op removal of the referenced file",
    );
}

/// Battery B — same race as the snapshot pin, Delete-kind + serializable. Isolation only
/// arms `validate_no_conflicting_data_files`; it must not secretly arm `validate_deleted_files`.
#[tokio::test]
async fn commit_row_delta_kind_delete_serializable_tolerates_concurrent_delete_op_removal() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    let a = data_file("test/a.parquet");
    let (table_at_pin, pin) = append(
        &catalog,
        &ident,
        vec![a.clone(), data_file("test/b.parquet")],
    )
    .await;
    concurrent_delete(&catalog, &ident, vec![a]).await;

    commit_row_delta_kind(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        Vec::new(),
        default_concurrency(),
        RowDeltaPolicy {
            kind: RowDeltaKind::Delete,
            isolation: IsolationLevel::Serializable,
        },
    )
    .await
    .expect(
        "Delete-kind + serializable still omits validate_deleted_files; DELETE-op removal is tolerated",
    );
}

/// Battery B — Merge-kind at the SAME snapshot isolation rejects the identical DELETE-op
/// removal. Isolates `RowDeltaKind` from `IsolationLevel`: only Merge arms
/// `validate_deleted_files` (widens the exist-check op set to `{OVERWRITE, DELETE}`).
#[tokio::test]
async fn commit_row_delta_kind_merge_snapshot_rejects_concurrent_delete_op_removal() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    let a = data_file("test/a.parquet");
    let (table_at_pin, pin) = append(
        &catalog,
        &ident,
        vec![a.clone(), data_file("test/b.parquet")],
    )
    .await;
    concurrent_delete(&catalog, &ident, vec![a]).await;

    let error = commit_row_delta_kind(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        Vec::new(),
        default_concurrency(),
        RowDeltaPolicy {
            kind: RowDeltaKind::Merge,
            isolation: IsolationLevel::Snapshot,
        },
    )
    .await
    .expect_err("Merge-kind must reject a concurrent DELETE-op removal of the referenced file");

    let ice = iceberg_error(&error);
    assert_eq!(ice.kind(), iceberg::ErrorKind::DataInvalid);
    assert!(
        !ice.retryable(),
        "the dangling-reference conflict is non-retryable"
    );
    assert!(
        ice.message().contains("test/a.parquet"),
        "the failure must NAME the missing referenced data file, got: {}",
        ice.message()
    );
}

// ===================================================================================
// BATTERY C — MERGE↔MERGE race, both orders, through the real MERGE commit arms.
// ===================================================================================

async fn cow_rewrite_merge_merge_race(winner_path: &str, loser_path: &str) {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let a = data_file("test/a.parquet");
    let (table_at_pin, pin) = append(&catalog, &ident, vec![a.clone()]).await;

    commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![a.clone()],
        vec![data_file(winner_path)],
    )
    .await
    .expect("first MERGE rewrite (winner) must land");

    let error = commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![a],
        vec![data_file(loser_path)],
    )
    .await
    .expect_err("second MERGE rewrite (loser) must reject");

    let ice = iceberg_error(&error);
    assert_eq!(ice.kind(), iceberg::ErrorKind::DataInvalid);
    assert!(!ice.retryable(), "a MERGE↔MERGE conflict is non-retryable");
    assert!(
        ice.message().to_ascii_lowercase().contains("conflict")
            || ice.message().contains("missing data files"),
        "loser must be an OCC/validation reject, got: {}",
        ice.message()
    );

    let live = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        live,
        HashSet::from([winner_path.to_string()]),
        "winner landed and loser did not, live={live:?}"
    );
}

/// Battery C — MERGE↔MERGE copy-on-write, order winner-then-loser. Two rewrite commits
/// through the real [`commit`] arm, same pin. Loser rejects; winner is the only live file.
#[tokio::test]
async fn commit_cow_merge_merge_race_first_rewrite_wins() {
    cow_rewrite_merge_merge_race("test/winner.parquet", "test/loser.parquet").await;
}

/// Battery C — MERGE↔MERGE copy-on-write, swapped order (the other writer wins).
#[tokio::test]
async fn commit_cow_merge_merge_race_second_rewrite_wins() {
    cow_rewrite_merge_merge_race("test/other-winner.parquet", "test/other-loser.parquet").await;
}

async fn mor_merge_merge_race(winner_insert: &str, loser_insert: &str) {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;

    commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        vec![data_file(winner_insert)],
        default_concurrency(),
    )
    .await
    .expect("first merge-on-read MERGE (winner) must land");

    let error = commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        vec![data_file(loser_insert)],
        default_concurrency(),
    )
    .await
    .expect_err("second merge-on-read MERGE (loser) must reject");

    let ice = iceberg_error(&error);
    assert_eq!(ice.kind(), iceberg::ErrorKind::DataInvalid);
    assert!(!ice.retryable(), "a MERGE↔MERGE conflict is non-retryable");
    assert!(
        ice.message().to_ascii_lowercase().contains("conflict")
            || ice.message().contains("missing data files"),
        "loser must be an OCC/validation reject, got: {}",
        ice.message()
    );

    let live = live_data_file_paths(&catalog, &ident).await;
    assert!(
        live.contains(winner_insert) && !live.contains(loser_insert),
        "winner insert landed and loser insert did not, live={live:?}"
    );
}

/// Battery C — MERGE↔MERGE merge-on-read, both commits through the real [`commit_row_delta`]
/// arm (Merge + serializable). First writer wins.
#[tokio::test]
async fn commit_row_delta_merge_merge_race_first_wins() {
    mor_merge_merge_race("test/mor-winner.parquet", "test/mor-loser.parquet").await;
}

/// Battery C — MERGE↔MERGE merge-on-read, swapped order.
#[tokio::test]
async fn commit_row_delta_merge_merge_race_second_wins() {
    mor_merge_merge_race(
        "test/mor-other-winner.parquet",
        "test/mor-other-loser.parquet",
    )
    .await;
}

// ===================================================================================
// BATTERY E — retry-through-benign-commit / validate_from_snapshot precedence.
// ===================================================================================

/// Battery E — a non-conflicting concurrent commit forces a re-base. The retry is modeled
/// as a REFRESHED table handle (what `do_commit` loads after the concurrent snapshot
/// lands) plus the ORIGINAL pin. The rewrite must succeed and parent the concurrent
/// snapshot — the fork's `validate_from_snapshot(pin).or(tx_start)` must walk S0→S1
/// and still accept the benign delete.
#[tokio::test]
async fn commit_retry_through_benign_commit_revalidates_from_original_pin_and_succeeds() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    let a = data_file("test/a.parquet");
    let b = data_file("test/b.parquet");
    let (_table_at_pin, pin) = append(&catalog, &ident, vec![a.clone(), b.clone()]).await;

    concurrent_delete(&catalog, &ident, vec![b]).await;
    let table_at_rebase = catalog
        .load_table(&ident)
        .await
        .expect("load refreshed handle");
    let rebase_snapshot = table_at_rebase
        .metadata()
        .current_snapshot()
        .expect("S1")
        .snapshot_id();
    assert_ne!(
        rebase_snapshot, pin,
        "the concurrent commit must have moved the head"
    );

    commit(
        &catalog,
        &table_at_rebase,
        Some(pin),
        vec![a],
        vec![data_file("test/a-prime.parquet")],
    )
    .await
    .expect("benign concurrent delete of an untouched file must not fail the rebased rewrite");

    let snapshot = current_snapshot(&catalog, &ident).await;
    assert_eq!(
        snapshot.parent_snapshot_id(),
        Some(rebase_snapshot),
        "the MERGE snapshot must parent the concurrent head (re-base), not the original pin"
    );
    let live = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        live,
        HashSet::from(["test/a-prime.parquet".to_string()]),
        "rewrite landed (A→A') and the concurrent delete stuck (B gone), live={live:?}"
    );
}

/// Battery E — pin-precedence mutation. A refreshed handle's tx-captured start is S1;
/// a conflicting append between S0 and S1 must still be rejected because
/// `validate_from_snapshot(S0)` WINS over that start. Dropping the from-snapshot call
/// empties the walk (start=S1) and this pin goes red.
#[tokio::test]
async fn commit_refreshed_handle_still_validates_from_original_pin() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    let (_table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;
    append(&catalog, &ident, vec![data_file("test/concurrent.parquet")]).await;
    let table_at_rebase = catalog
        .load_table(&ident)
        .await
        .expect("load refreshed handle");

    let error = commit(
        &catalog,
        &table_at_rebase,
        Some(pin),
        Vec::new(),
        vec![data_file("test/insert.parquet")],
    )
    .await
    .expect_err(
        "insert-only MERGE must still reject a conflicting add between the original pin and now",
    );

    let ice = iceberg_error(&error);
    assert_eq!(ice.kind(), iceberg::ErrorKind::DataInvalid);
    assert!(!ice.retryable());
    assert!(
        ice.message().contains("Found conflicting files"),
        "must be the serializable added-data conflict from the ORIGINAL pin, got: {}",
        ice.message()
    );
    let live = live_data_file_paths(&catalog, &ident).await;
    assert!(
        !live.contains("test/insert.parquet"),
        "the rejected insert must not be in the table, live={live:?}"
    );
}

// ===================================================================================
// BATTERY F — empty-table `snapshot_id == None` under concurrency.
// ===================================================================================

/// Battery F — empty-table `snapshot_id == None`. `validate_from_snapshot` is not armed
/// (Java runs no from-snapshot check on an empty-at-read table). The fork's
/// `effective_start = None` walk is from-root, so a concurrent insert-only append
/// between the empty read and this commit is still a serializable data conflict.
#[tokio::test]
async fn commit_empty_table_none_pin_from_root_walk_rejects_concurrent_insert() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let table_at_empty = catalog.load_table(&ident).await.expect("load empty table");
    assert!(
        table_at_empty.metadata().current_snapshot().is_none(),
        "the empty-table pin is literally None"
    );

    append(&catalog, &ident, vec![data_file("test/concurrent.parquet")]).await;

    let error = commit(
        &catalog,
        &table_at_empty,
        None,
        Vec::new(),
        vec![data_file("test/insert.parquet")],
    )
    .await
    .expect_err("from-root walk must catch the concurrent insert-only race on an empty table");

    let ice = iceberg_error(&error);
    assert_eq!(ice.kind(), iceberg::ErrorKind::DataInvalid);
    assert!(!ice.retryable());
    assert!(
        ice.message().contains("Found conflicting files"),
        "must be the serializable added-data conflict (from-root), got: {}",
        ice.message()
    );
    let live = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        live,
        HashSet::from(["test/concurrent.parquet".to_string()]),
        "only the concurrent insert is live; the rejected MERGE insert did not land, live={live:?}"
    );
}

// ===================================================================================
// BATTERY G — partitioned target + AlwaysTrue (M15 over-rejection). Not an xfail.
// ===================================================================================

/// Battery G / M15 — serializable MERGE + `AlwaysTrue` DOES trip on a concurrent append
/// in a DIFFERENT partition. Current behavior (over-rejection). A future residual-narrow
/// of the conflict filter would be WRONG (audit M15: residual is source-key min/max, not
/// the ON condition); a future *partition-aware* narrowing flips this pin from reject
/// to commit. Not an xfail — the red-to-green flip is the future fix's proof.
#[tokio::test]
async fn commit_serializable_merge_rejects_concurrent_append_in_a_different_partition_m15() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup_partitioned(&warehouse).await;
    let table = catalog.load_table(&ident).await.expect("load");
    let spec_id = table.metadata().default_partition_spec_id();

    let in_part_one = data_file_in_partition("test/p1.parquet", spec_id, 1);
    let (table_at_pin, pin) = append(&catalog, &ident, vec![in_part_one]).await;

    append(
        &catalog,
        &ident,
        vec![data_file_in_partition("test/p2.parquet", spec_id, 2)],
    )
    .await;

    let error = commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        Vec::new(),
        vec![data_file_in_partition("test/insert-p1.parquet", spec_id, 1)],
    )
    .await
    .expect_err(
        "M15: AlwaysTrue serializable MERGE currently over-rejects a different-partition append",
    );

    let ice = iceberg_error(&error);
    assert_eq!(ice.kind(), iceberg::ErrorKind::DataInvalid);
    assert!(!ice.retryable());
    assert!(
        ice.message().contains("Found conflicting files"),
        "M15 over-rejection is the added-data conflict (AlwaysTrue), got: {}",
        ice.message()
    );
    let live = live_data_file_paths(&catalog, &ident).await;
    assert!(
        !live.contains("test/insert-p1.parquet"),
        "the over-rejected insert must not have landed, live={live:?}"
    );
}

/// Battery G control — a concurrent DELETE in a different partition is NOT a data
/// conflict. If G's reject were "any concurrent commit on a partitioned table", this
/// control would also fail. Stays green when M15 is later narrowed.
#[tokio::test]
async fn commit_serializable_merge_allows_concurrent_delete_in_a_different_partition() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup_partitioned(&warehouse).await;
    let table = catalog.load_table(&ident).await.expect("load");
    let spec_id = table.metadata().default_partition_spec_id();

    let in_part_one = data_file_in_partition("test/p1.parquet", spec_id, 1);
    let in_part_two = data_file_in_partition("test/p2.parquet", spec_id, 2);
    let (table_at_pin, pin) = append(
        &catalog,
        &ident,
        vec![in_part_one.clone(), in_part_two.clone()],
    )
    .await;

    concurrent_delete(&catalog, &ident, vec![in_part_two]).await;

    commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![in_part_one],
        vec![data_file_in_partition("test/p1-prime.parquet", spec_id, 1)],
    )
    .await
    .expect("a concurrent delete in a different partition is not a serializable data conflict");

    let live = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        live,
        HashSet::from(["test/p1-prime.parquet".to_string()]),
        "rewrite of P1 landed and the concurrent P2 delete stuck, live={live:?}"
    );
}

// ===================================================================================
// BATTERY H / M20 — operation-stamp pins + CDC mode-flip hazard.
// ===================================================================================

/// Battery H / M20 — insert-only copy-on-write stamps `append`.
///
/// CDC / `IncrementalAppendScan` mode-flip hazard: consumers that filter
/// `operation == append` see this shape and silently lose the insert-only
/// merge-on-read twin (stamped `overwrite`) after a `write.merge.mode` flip.
#[tokio::test]
async fn merge_insert_only_cow_stamps_append_m20() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;
    commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        Vec::new(),
        vec![data_file("test/insert.parquet")],
    )
    .await
    .expect("insert-only COW");
    assert_eq!(
        current_snapshot(&catalog, &ident).await.summary().operation,
        Operation::Append,
        "insert-only COW must stamp append"
    );
}

/// Battery H / M20 — insert-only merge-on-read stamps `overwrite` (Java 1.10.0
/// `BaseRowDelta.operation()` else-OVERWRITE). Pair with the COW `append` pin:
/// a `write.merge.mode` flip changes the stamp `IncrementalAppendScan` consumers
/// filter on.
#[tokio::test]
async fn merge_insert_only_mor_stamps_overwrite_m20() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;
    commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        Vec::new(),
        vec![data_file("test/insert.parquet")],
        default_concurrency(),
    )
    .await
    .expect("insert-only merge-on-read");
    assert_eq!(
        current_snapshot(&catalog, &ident).await.summary().operation,
        Operation::Overwrite,
        "insert-only merge-on-read must stamp overwrite (CDC mode-flip vs COW append)"
    );
}

/// Battery H / M20 — mixed copy-on-write (rewrite) stamps `overwrite`.
#[tokio::test]
async fn merge_mixed_cow_stamps_overwrite_m20() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let a = data_file("test/a.parquet");
    let (table_at_pin, pin) = append(&catalog, &ident, vec![a.clone()]).await;
    commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![a],
        vec![data_file("test/a-prime.parquet")],
    )
    .await
    .expect("mixed COW");
    assert_eq!(
        current_snapshot(&catalog, &ident).await.summary().operation,
        Operation::Overwrite,
        "mixed COW must stamp overwrite"
    );
}

/// Battery H / M20 — mixed merge-on-read (position deletes + new data) stamps
/// `overwrite` and still carries the §8 `engine.operation-id`.
#[tokio::test]
async fn merge_mixed_mor_stamps_overwrite_m20() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;
    commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        vec![data_file("test/new.parquet")],
        default_concurrency(),
    )
    .await
    .expect("mixed merge-on-read");
    let snapshot = current_snapshot(&catalog, &ident).await;
    assert_eq!(
        snapshot.summary().operation,
        Operation::Overwrite,
        "mixed merge-on-read must stamp overwrite"
    );
    assert!(
        snapshot
            .summary()
            .additional_properties
            .contains_key(OPERATION_ID_PROP),
        "every MERGE commit carries the §8 engine.operation-id stamp"
    );
}

/// Battery H / M20 — delete-only merge-on-read stamps `delete`.
#[tokio::test]
async fn merge_delete_only_mor_stamps_delete_m20() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;
    commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        Vec::new(),
        default_concurrency(),
    )
    .await
    .expect("delete-only merge-on-read");
    assert_eq!(
        current_snapshot(&catalog, &ident).await.summary().operation,
        Operation::Delete,
        "delete-only merge-on-read must stamp delete"
    );
}

// ===================================================================================
// BATTERY I / M14 — rejected commit abort-deletes written files (design A).
// ===================================================================================

/// Battery I / M14 — after a rejected copy-on-write commit the staged data files are
/// gone from the warehouse and the original OCC error still surfaces.
#[tokio::test]
async fn rejected_cow_commit_files_are_removed_m14() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;

    append(&catalog, &ident, vec![data_file("test/concurrent.parquet")]).await;

    let written = write_data_files(&table_at_pin, vec![id_batch(&[99])])
        .await
        .expect("stage the insert files before the OCC commit");
    assert!(
        !written.is_empty(),
        "the writer must produce at least one real Parquet file"
    );
    let staged_paths: Vec<String> = written
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();

    let error = commit(&catalog, &table_at_pin, Some(pin), Vec::new(), written)
        .await
        .expect_err("serializable insert-only MERGE must reject the concurrent append");
    let ice = iceberg_error(&error);
    assert_eq!(ice.kind(), iceberg::ErrorKind::DataInvalid);

    let live = live_data_file_paths(&catalog, &ident).await;
    assert!(
        live.contains("test/a.parquet"),
        "abort must not delete referenced existing data files, live={live:?}"
    );
    assert!(
        live.contains("test/concurrent.parquet"),
        "abort must not delete the concurrent winner's files, live={live:?}"
    );
    for path in &staged_paths {
        assert!(
            !live.contains(path),
            "the rejected file must not be in the live snapshot, path={path}, live={live:?}"
        );
        assert!(
            !path_exists(&table_at_pin, path).await,
            "M14: staged data file must be gone after OCC reject: {path}"
        );
    }
}

/// Battery I / M14 — merge-on-read writes position-delete files BEFORE `tx.commit`; a
/// rejected row delta abort-deletes those files and still surfaces the OCC error.
#[tokio::test]
async fn rejected_row_delta_files_are_removed_m14() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;

    append(&catalog, &ident, vec![data_file("test/concurrent.parquet")]).await;

    let before = parquet_paths_under(warehouse.path());
    let error = commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        Vec::new(),
        default_concurrency(),
    )
    .await
    .expect_err("serializable merge-on-read MERGE must reject the concurrent append");
    let ice = iceberg_error(&error);
    assert_eq!(ice.kind(), iceberg::ErrorKind::DataInvalid);

    let after = parquet_paths_under(warehouse.path());
    let orphans: HashSet<_> = after.difference(&before).cloned().collect();
    assert!(
        orphans.is_empty(),
        "M14: the rejected row delta must not leave new Parquet files on disk, \
         orphans={orphans:?} before={before:?} after={after:?}"
    );
}

/// Battery I / M14 success path — a successful copy-on-write overwrite commit leaves
/// the newly written data files in the warehouse (cleanup must not fire after Ok).
#[tokio::test]
async fn successful_cow_overwrite_commit_leaves_written_data_files_m14() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;

    let written = write_data_files(&table_at_pin, vec![id_batch(&[99])])
        .await
        .expect("stage the insert files");
    assert!(
        !written.is_empty(),
        "the writer must produce at least one real Parquet file"
    );
    let staged_paths: Vec<String> = written
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();

    commit(&catalog, &table_at_pin, Some(pin), Vec::new(), written)
        .await
        .expect("insert-only copy-on-write commit succeeds with no concurrent writer");

    let live = live_data_file_paths(&catalog, &ident).await;
    for path in &staged_paths {
        assert!(
            live.contains(path),
            "committed data file must be live, path={path}, live={live:?}"
        );
        assert!(
            path_exists(&table_at_pin, path).await,
            "success-path abort must not delete committed data files: {path}"
        );
    }
}

/// Battery I / M14 success path — a successful row-delta commit leaves the newly
/// written position-delete files in the warehouse.
#[tokio::test]
async fn successful_row_delta_leaves_written_delete_files_m14() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;

    let before = parquet_paths_under(warehouse.path());
    commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        Vec::new(),
        default_concurrency(),
    )
    .await
    .expect("delete-only merge-on-read commit succeeds with no concurrent writer");

    let after = parquet_paths_under(warehouse.path());
    let written: HashSet<_> = after.difference(&before).cloned().collect();
    assert!(
        !written.is_empty(),
        "a successful row delta must have written at least one position-delete Parquet file, \
         before={before:?} after={after:?}"
    );
    for path in &written {
        assert!(
            path_exists(&table_at_pin, path).await,
            "success-path abort must not delete committed delete files: {path}"
        );
    }
}

/// Battery I / M14 — a failing `FileIO::delete` must not replace the original OCC
/// `DataInvalid`. A custom `Storage` wrapper is not injected (typetag + lockfile).
/// The scripted failure is a real directory path: `LocalFs` `delete` uses `remove_file`,
/// which errors with "Is a directory".
#[tokio::test]
async fn delete_failure_does_not_mask_cow_commit_error_m14() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;
    append(&catalog, &ident, vec![data_file("test/concurrent.parquet")]).await;

    let not_a_file = warehouse.path().join("m14-delete-fail-dir");
    std::fs::create_dir(&not_a_file).expect("create a directory FileIO::delete cannot unlink");
    let not_a_file_path = not_a_file
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();

    let error = commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        Vec::new(),
        vec![data_file(&not_a_file_path)],
    )
    .await
    .expect_err("serializable insert-only MERGE must reject the concurrent append");
    let ice = iceberg_error(&error);
    assert_eq!(
        ice.kind(),
        iceberg::ErrorKind::DataInvalid,
        "a failed abort delete must not mask the OCC reject, got: {ice}"
    );
    assert!(
        not_a_file.is_dir(),
        "the scripted delete target must still be a directory (delete failed as intended)"
    );
}

fn parquet_paths_under(root: &std::path::Path) -> HashSet<String> {
    let mut paths = HashSet::new();
    collect_parquet_paths(root, &mut paths);
    paths
}

fn collect_parquet_paths(dir: &std::path::Path, out: &mut HashSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_paths(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            out.insert(path.to_string_lossy().into_owned());
        }
    }
}
