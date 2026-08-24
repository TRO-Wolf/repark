use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::error::DataFusionError;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, ManifestContentType, NestedField,
    Operation, PrimitiveType, Schema, Struct, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::{
    IsolationLevel, OPERATION_ID_PROP, WRITE_MERGE_ISOLATION_LEVEL, commit, commit_row_delta,
    resolve_merge_isolation,
};

/// An in-memory Iceberg catalog (local-FS warehouse, format-version 2 by default) with a
/// `sales` namespace and one UNPARTITIONED table `t (id int)`. No AWS, no network.
async fn setup(warehouse: &TempDir) -> (Arc<dyn Catalog>, TableIdent) {
    setup_with_properties(warehouse, HashMap::new()).await
}

/// [`setup`] with Iceberg table properties set at create time (so the pin handle sees them).
async fn setup_with_properties(
    warehouse: &TempDir,
    properties: HashMap<String, String>,
) -> (Arc<dyn Catalog>, TableIdent) {
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
        .properties(properties)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    (catalog, TableIdent::new(namespace, "t".to_string()))
}

/// [`setup`] with `write.merge.isolation-level` set at create time.
async fn setup_with_isolation(
    warehouse: &TempDir,
    isolation: &str,
) -> (Arc<dyn Catalog>, TableIdent) {
    setup_with_properties(
        warehouse,
        HashMap::from([(
            WRITE_MERGE_ISOLATION_LEVEL.to_string(),
            isolation.to_string(),
        )]),
    )
    .await
}

/// Resolve `write.merge.isolation-level` off a freshly created table (parse-only).
async fn resolve_isolation_of(value: Option<&str>) -> Result<IsolationLevel, DataFusionError> {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = match value {
        Some(name) => setup_with_isolation(&warehouse, name).await,
        None => setup(&warehouse).await,
    };
    let table = catalog.load_table(&ident).await.expect("load table");
    resolve_merge_isolation(&table)
}

/// Exact `DataFusionError::Plan` needle from the DML isolation resolver.
fn assert_invalid_isolation(error: DataFusionError, name: &str) {
    match error {
        DataFusionError::Plan(message) => {
            assert_eq!(
                message,
                format!("Invalid isolation level: {name}"),
                "garbage isolation must use the DML Plan needle, got: {message}"
            );
        }
        other => panic!("expected DataFusionError::Plan, got: {other}"),
    }
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

/// An unpartitioned synthetic POSITION-delete file (manifest-only).
fn position_delete_file(path: &str) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .expect("build position-delete file")
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

/// A concurrent merge-on-read commit that adds `deletes` (position/equality delete files).
async fn concurrent_add_deletes(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    deletes: Vec<DataFile>,
) {
    let table = catalog.load_table(ident).await.expect("load table");
    let tx = Transaction::new(&table);
    let action = tx.row_delta().add_deletes(deletes);
    let tx = action.apply(tx).expect("apply row_delta");
    tx.commit(catalog.as_ref()).await.expect("commit row_delta");
}

/// A concurrent compaction that rewrites `delete` into `add` as `Operation::Replace`.
async fn concurrent_replace_compaction(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    delete: DataFile,
    add: DataFile,
) -> Table {
    let table = catalog.load_table(ident).await.expect("load table");
    let tx = Transaction::new(&table);
    let action = tx.rewrite_files(vec![delete], vec![add]);
    let tx = action.apply(tx).expect("apply rewrite_files");
    tx.commit(catalog.as_ref())
        .await
        .expect("commit replace compaction")
}

/// The set of live (Added/Existing) DATA-file paths in the table's current snapshot.
async fn live_data_file_paths(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> HashSet<String> {
    let table = catalog.load_table(ident).await.expect("load table");
    let metadata = table.metadata();
    let snapshot = metadata.current_snapshot().expect("current snapshot");
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

/// PIN — conflicting-commit-detected × INSERT-ONLY MERGE (the S0 bug). An insert-only MERGE
/// pinned snapshot S to compute its NOT-MATCHED set; a concurrent commit that ADDED a matching
/// row between S and the commit must be caught, or the append is a silent duplicate. Risk: the
/// pre-fix `fast_append` path carried no validation and committed the duplicate blindly. The
/// failure must be loud — a NON-retryable serializable data conflict.
#[tokio::test]
async fn commit_insert_only_rejects_conflicting_concurrent_append() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    // Base: row-file A at snapshot S0; the insert-only MERGE pins S0.
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;

    // A concurrent writer inserts a matching row (a new DATA file) AFTER S0.
    append(&catalog, &ident, vec![data_file("test/concurrent.parquet")]).await;

    // The insert-only MERGE (affected empty) appends its NOT-MATCHED rows, pinned to S0.
    let error = commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        Vec::new(),
        vec![data_file("test/insert.parquet")],
    )
    .await
    .expect_err("insert-only MERGE must reject the conflicting concurrent add");

    let ice = iceberg_error(&error);
    assert_eq!(
        ice.kind(),
        iceberg::ErrorKind::DataInvalid,
        "a conflict is a non-retryable validation failure (DataInvalid)"
    );
    assert!(
        !ice.retryable(),
        "the validation failure must be NON-retryable so the retry loop stops"
    );
    assert!(
        ice.message().contains("Found conflicting files"),
        "must be the serializable added-data conflict (validate_no_conflicting_data), got: {}",
        ice.message()
    );

    // The blind duplicate must NOT have landed.
    let live = live_data_file_paths(&catalog, &ident).await;
    assert!(
        !live.contains("test/insert.parquet"),
        "the rejected insert must not be in the table, live={live:?}"
    );
}

/// PIN — conflicting-commit-detected × MIXED MERGE (regression: the rewrite arm stays
/// validated). The rewrite arm removes affected file A via `delete_data_files` and carries
/// `validate_no_conflicting_deletes`; a concurrent merge-on-read delete applying to A must be
/// caught (you cannot drop A out from under a concurrent row-level delete). This proves the
/// insert-only-arm fix left the rewrite arm's OCC intact.
#[tokio::test]
async fn commit_rewrite_path_rejects_conflicting_concurrent_delete() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    // Base: data file A (the mixed MERGE rewrites it) at snapshot S0.
    let a = data_file("test/a.parquet");
    let (table_at_pin, pin) = append(&catalog, &ident, vec![a.clone()]).await;

    // A concurrent merge-on-read DELETE lands a position delete applying to A (seq > S0).
    concurrent_add_deletes(
        &catalog,
        &ident,
        vec![position_delete_file("test/pos-del.parquet")],
    )
    .await;

    // Rewrite path: affected = [A], plus the rewritten survivor A'.
    let error = commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![a],
        vec![data_file("test/a-prime.parquet")],
    )
    .await
    .expect_err("the rewrite path must reject a concurrent delete on a rewritten file");

    let ice = iceberg_error(&error);
    assert_eq!(
        ice.kind(),
        iceberg::ErrorKind::DataInvalid,
        "a conflict is a non-retryable validation failure (DataInvalid)"
    );
    assert!(
        !ice.retryable(),
        "the delete conflict must be NON-retryable"
    );
    assert!(
        ice.message()
            .contains("found new delete for replaced data file"),
        "must be the removed-file delete conflict (validate_no_conflicting_deletes), got: {}",
        ice.message()
    );

    // The rejected rewrite's added survivor did NOT land.
    let live = live_data_file_paths(&catalog, &ident).await;
    assert!(
        !live.contains("test/a-prime.parquet"),
        "the rejected rewrite must not have committed, live={live:?}"
    );
}

/// PIN — non-conflicting concurrent commit succeeds (no false positive). A concurrent
/// DELETE-only commit adds NO data files, so it cannot create a duplicate for an insert-only
/// MERGE; `validate_no_conflicting_data` (added-data only) must NOT flag it. Risk: an
/// over-broad guard that rejects ANY concurrent commit would break legitimate MERGEs.
#[tokio::test]
async fn commit_insert_only_allows_nonconflicting_concurrent_delete() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    let a = data_file("test/a.parquet");
    let (table_at_pin, pin) = append(&catalog, &ident, vec![a.clone()]).await;

    // Concurrent DELETE removes A (adds no data) — not a conflict for an insert-only MERGE.
    concurrent_delete(&catalog, &ident, vec![a]).await;

    commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        Vec::new(),
        vec![data_file("test/insert.parquet")],
    )
    .await
    .expect("a concurrent delete-only commit is not a conflict for an insert-only MERGE");

    let live = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        live,
        HashSet::from(["test/insert.parquet".to_string()]),
        "the insert landed and the concurrent delete stuck (A gone), live={live:?}"
    );
}

/// PIN — no-concurrency baseline unchanged. With no concurrent commit, an insert-only MERGE
/// appends cleanly and raises no spurious conflict — the fix does not regress the normal path.
#[tokio::test]
async fn commit_insert_only_no_concurrency_appends_both_rows() {
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
    .expect("a no-concurrency insert-only MERGE must commit");

    let live = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        live,
        HashSet::from([
            "test/a.parquet".to_string(),
            "test/insert.parquet".to_string(),
        ]),
        "both the base row and the inserted row are live, live={live:?}"
    );
}

/// PIN — conflicting-commit-detected × MIXED MERGE, concurrent ADD (the F-BR-1 S1). A mixed
/// MERGE (a WHEN MATCHED clause rewrote a file) pinned snapshot S to compute its NOT-MATCHED
/// set; a concurrent commit that ADDED a matching row between S and the rewrite commit must be
/// caught, or the not-matched INSERT is a silent duplicate (the audit's `[0,1,999,999]`). Risk:
/// the rewrite arm carried only `validate_no_conflicting_deletes` (snapshot isolation), so it
/// committed the concurrent-add duplicate blindly; the serializable `validate_no_conflicting_data`
/// guard must reject it — loud, non-retryable. Exact-scope mutation target for this unit.
#[tokio::test]
async fn commit_rewrite_path_rejects_conflicting_concurrent_append() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    // Base: data file A (the mixed MERGE rewrites it) at snapshot S0; the merge pins S0.
    let a = data_file("test/a.parquet");
    let (table_at_pin, pin) = append(&catalog, &ident, vec![a.clone()]).await;

    // A concurrent writer APPENDS a matching row (a new DATA file) AFTER S0 — the `[999]` add.
    append(&catalog, &ident, vec![data_file("test/concurrent.parquet")]).await;

    // Rewrite path: affected = [A], plus the rewritten survivor A'.
    let error = commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![a],
        vec![data_file("test/a-prime.parquet")],
    )
    .await
    .expect_err("the mixed-MERGE rewrite path must reject a conflicting concurrent add");

    let ice = iceberg_error(&error);
    assert_eq!(
        ice.kind(),
        iceberg::ErrorKind::DataInvalid,
        "a conflict is a non-retryable validation failure (DataInvalid)"
    );
    assert!(
        !ice.retryable(),
        "the serializable data conflict must be NON-retryable so the retry loop stops"
    );
    assert!(
        ice.message().contains("Found conflicting files"),
        "must be the serializable added-data conflict (validate_no_conflicting_data), got: {}",
        ice.message()
    );

    // The blind duplicate (the rewrite's added survivor) must NOT have landed.
    let live = live_data_file_paths(&catalog, &ident).await;
    assert!(
        !live.contains("test/a-prime.parquet"),
        "the rejected rewrite must not be in the table, live={live:?}"
    );
}

/// PIN — non-conflicting concurrent commit succeeds on the rewrite path (no false positive from
/// the new serializable guard). A concurrent DELETE-only overwrite of an UNTOUCHED file adds NO
/// data (so `validate_no_conflicting_data` must not flag it) and NO delete file applying to the
/// rewritten file (so `validate_no_conflicting_deletes` must not flag it). Risk: an over-broad
/// guard that rejects ANY concurrent commit would break legitimate mixed MERGEs. Stays GREEN
/// when the new `validate_no_conflicting_data` is dropped — the other half of the mutation proof.
#[tokio::test]
async fn commit_rewrite_path_allows_nonconflicting_concurrent_delete() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    // Base: A (the mixed MERGE rewrites it) + B (untouched) at snapshot S0; the merge pins S0.
    let a = data_file("test/a.parquet");
    let b = data_file("test/b.parquet");
    let (table_at_pin, pin) = append(&catalog, &ident, vec![a.clone(), b.clone()]).await;

    // Concurrent DELETE removes the UNTOUCHED B — adds no data, adds no delete file for A.
    concurrent_delete(&catalog, &ident, vec![b]).await;

    // Rewrite path: affected = [A], survivor A'. Neither validation should fire.
    commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![a],
        vec![data_file("test/a-prime.parquet")],
    )
    .await
    .expect("a concurrent delete of an untouched file is not a conflict for the rewrite path");

    let live = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        live,
        HashSet::from(["test/a-prime.parquet".to_string()]),
        "the rewrite landed (A→A') and the concurrent delete stuck (B gone), live={live:?}"
    );
}

/// PIN — no-concurrency mixed-MERGE baseline unchanged. With no concurrent commit the rewrite
/// path commits cleanly (survivor added, affected file removed) and the new serializable guard
/// raises no spurious conflict — the fix does not regress the normal mixed-MERGE path.
#[tokio::test]
async fn commit_rewrite_path_no_concurrency_commits() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    let a = data_file("test/a.parquet");
    let (table_at_pin, pin) = append(&catalog, &ident, vec![a.clone()]).await;

    // Rewrite A → A' and add a NOT-MATCHED insert, no concurrent writer.
    commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![a],
        vec![
            data_file("test/a-prime.parquet"),
            data_file("test/insert.parquet"),
        ],
    )
    .await
    .expect("a no-concurrency mixed MERGE must commit");

    let live = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        live,
        HashSet::from([
            "test/a-prime.parquet".to_string(),
            "test/insert.parquet".to_string(),
        ]),
        "the rewritten survivor and the inserted row are live, A gone, live={live:?}"
    );
}

// ===================================================================================
// GROUP T — merge-on-read `commit_row_delta` OCC pins (T5, T6).
//
// Same two-handle race as the copy-on-write pins above, against the NEW `RowDelta` commit seam.
// These write REAL position-delete parquet files (through the warehouse `FileIO`) from synthetic
// `(path, pos)` pairs: the delete file records the referenced path, it never reads the data file,
// so a manifest-only data file is faithful input here exactly as it is for the overwrite arm.
// ===================================================================================

/// PIN T5 — SERIALIZABLE isolation is ARMED on the merge-on-read commit. A MERGE pinned snapshot
/// S to decide its matched/not-matched split; a concurrent commit that ADDED a data file between
/// S and the commit could contain a row the MERGE would have matched (or would have refused to
/// insert), so the row delta must be rejected — the F-BR-1 silent-duplicate class, on the
/// merge-on-read arm. Mutation M-T-OCC: delete `.validate_no_conflicting_data_files()` from
/// `commit_row_delta` → the conflicting add slips through and this pin goes RED.
#[tokio::test]
async fn commit_row_delta_rejects_conflicting_concurrent_append() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    // Base: data file A at snapshot S0; the merge-on-read MERGE pins S0 and deletes A's row 0.
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;

    // A concurrent writer adds a matching row (a new DATA file) AFTER S0.
    append(&catalog, &ident, vec![data_file("test/concurrent.parquet")]).await;

    let error = commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        Vec::new(),
        crate::write::concurrency::WriteConcurrency::default(),
    )
    .await
    .expect_err("merge-on-read MERGE must reject the conflicting concurrent add");

    let ice = iceberg_error(&error);
    assert_eq!(
        ice.kind(),
        iceberg::ErrorKind::DataInvalid,
        "a conflict is a non-retryable validation failure (DataInvalid)"
    );
    assert!(
        !ice.retryable(),
        "the validation failure must be NON-retryable so the retry loop stops"
    );
    assert!(
        ice.message().contains("conflicting"),
        "must be the serializable added-data conflict, got: {}",
        ice.message()
    );
}

/// PIN T6 — the REFERENCED-DATA-FILE validation is ARMED. A position delete names a
/// `(data file path, ordinal)`; if a concurrent commit compacted or rewrote that data file away
/// between the pin and this commit, the delete has nothing to apply to and committing it would
/// SILENTLY lose the deletion (the row stays visible forever). Because the concurrent removal
/// here is recorded as a DELETE-op snapshot, this pin is load-bearing for BOTH
/// `validate_data_files_exist` (which supplies the referenced set) and `validate_deleted_files`
/// (which widens the checked op set from `{OVERWRITE}` to `{OVERWRITE, DELETE}` — Java arms it
/// for UPDATE/MERGE). Mutation M-T-REF: delete EITHER call from `commit_row_delta` → the
/// dangling position delete commits and this pin goes RED.
#[tokio::test]
async fn commit_row_delta_rejects_position_delete_on_a_removed_data_file() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    // Base: data files A and B at snapshot S0; the MERGE pins S0 and deletes a row in A.
    let a = data_file("test/a.parquet");
    let (table_at_pin, pin) = append(
        &catalog,
        &ident,
        vec![a.clone(), data_file("test/b.parquet")],
    )
    .await;

    // A concurrent compaction/rewrite removes A entirely, AFTER S0.
    concurrent_delete(&catalog, &ident, vec![a]).await;

    let error = commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        Vec::new(),
        crate::write::concurrency::WriteConcurrency::default(),
    )
    .await
    .expect_err("a position delete referencing a removed data file must be rejected");

    let ice = iceberg_error(&error);
    assert_eq!(
        ice.kind(),
        iceberg::ErrorKind::DataInvalid,
        "a dangling referenced data file is a non-retryable validation failure"
    );
    assert!(
        !ice.retryable(),
        "the validation failure must be NON-retryable so the retry loop stops"
    );
    assert!(
        ice.message().contains("test/a.parquet"),
        "the failure must NAME the missing referenced data file, got: {}",
        ice.message()
    );
}

/// PIN AB7 (Group AB rider, closing Group Y's open C-Y-3) — the CONFLICTING-DELETE-FILE
/// validation is ARMED on the merge-on-read commit. This is the row-delta twin of the
/// copy-on-write `commit_rewrite_path_rejects_conflicting_concurrent_delete` above, and the last
/// unpinned arm of `commit_row_delta`'s four validations (T5 pinned
/// `validate_no_conflicting_data_files`, T6 pinned `validate_data_files_exist` +
/// `validate_deleted_files`).
///
/// The class: a MERGE READ the rows it is about to delete/update, deciding its matched split
/// from snapshot S. If a concurrent merge-on-read DELETE lands its OWN position-delete file
/// against the same data file between S and this commit, those two row-level decisions were made
/// against different views of the same rows — Java arms `validateNoConflictingDeleteFiles` for
/// exactly this (`SparkPositionDeltaWrite.commit`, `command == UPDATE || MERGE`, L251-254), and
/// it must be a NON-retryable `DataInvalid` so the retry loop stops rather than re-racing.
///
/// Group Y's Critic EXECUTED this arm and confirmed it fires, but left no committed pin (C-Y-3).
/// Committed here.
///
/// MUTATION M-AB-DELFILES: delete `.validate_no_conflicting_delete_files()` from
/// `commit_row_delta` → the conflicting concurrent delete file slips through and this pin goes
/// RED (the commit succeeds). The sibling
/// `commit_row_delta_commits_deletes_and_data_without_concurrency` is the no-false-positive
/// control that keeps the claim non-vacuous.
#[tokio::test]
async fn commit_row_delta_rejects_conflicting_concurrent_delete_file() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;

    // Base: data file A at snapshot S0; the merge-on-read MERGE pins S0 and deletes A's row 0.
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;

    // A concurrent merge-on-read DELETE lands its own position delete (seq > S0). No data file
    // is added or removed, so neither `validate_no_conflicting_data_files` nor
    // `validate_data_files_exist` can flag it — only the delete-file validation can.
    concurrent_add_deletes(
        &catalog,
        &ident,
        vec![position_delete_file("test/pos-del.parquet")],
    )
    .await;

    let error = commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        Vec::new(),
        crate::write::concurrency::WriteConcurrency::default(),
    )
    .await
    .expect_err("merge-on-read MERGE must reject a concurrent delete file over its read rows");

    let ice = iceberg_error(&error);
    assert_eq!(
        ice.kind(),
        iceberg::ErrorKind::DataInvalid,
        "a conflict is a non-retryable validation failure (DataInvalid)"
    );
    assert!(
        !ice.retryable(),
        "the delete-file conflict must be NON-retryable so the retry loop stops"
    );
    assert!(
        ice.message().contains("conflicting delete files"),
        "must be the new-delete-file conflict (validate_no_conflicting_delete_files), got: {}",
        ice.message()
    );

    // The rejected row delta committed nothing: A is still the only live data file, and no new
    // snapshot carries this MERGE's operation id.
    let live = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        live,
        HashSet::from(["test/a.parquet".to_string()]),
        "the rejected row delta must not have changed the data files, live={live:?}"
    );
}

/// PIN T5/T6 NO-FALSE-POSITIVE baseline — with NO concurrent commit, the identical merge-on-read
/// row delta COMMITS. Without this, both pins above would still pass if `commit_row_delta`
/// rejected everything unconditionally, and the "armed OCC" claim would be vacuous. Also the
/// happy-path proof that a position-delete file plus a data file land in ONE snapshot.
#[tokio::test]
async fn commit_row_delta_commits_deletes_and_data_without_concurrency() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup(&warehouse).await;
    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;

    commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        vec![data_file("test/new.parquet")],
        crate::write::concurrency::WriteConcurrency::default(),
    )
    .await
    .expect("an unconflicted merge-on-read MERGE commits");

    // ONE snapshot carries BOTH the new data file and the position delete — the RowDelta claim.
    let live = live_data_file_paths(&catalog, &ident).await;
    assert!(
        live.contains("test/a.parquet") && live.contains("test/new.parquet"),
        "merge-on-read adds the new data file WITHOUT removing the original, live={live:?}"
    );
    let table = catalog.load_table(&ident).await.expect("load table");
    let metadata = table.metadata();
    let snapshot = metadata.current_snapshot().expect("snapshot");
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("manifest list");
    let delete_manifests = manifest_list
        .entries()
        .iter()
        .filter(|entry| entry.content == ManifestContentType::Deletes)
        .count();
    assert_eq!(
        delete_manifests, 1,
        "the same snapshot must carry the position-delete manifest"
    );
    assert!(
        snapshot
            .summary()
            .additional_properties
            .contains_key(OPERATION_ID_PROP),
        "every MERGE commit carries the §8 engine.operation-id stamp"
    );
}

// ===================================================================================
// GROUP M13 — `write.merge.isolation-level` parse + M19-A serializable-vs-snapshot
// split. The resolver copies DML `resolve_isolation_property` BYTE-FOR-BYTE: no trim,
// `to_ascii_lowercase`, default serializable, garbage ⇒ Plan
// `Invalid isolation level: {name}`. Padded `  snapshot  ` is therefore GARBAGE.
// ===================================================================================

/// PIN M13 parse — upper `SNAPSHOT` / `SERIALIZABLE` honor the DML case fold.
/// Risk: a byte-exact match would silently ignore Spark/Java's case-insensitive
/// property values and keep MERGE serializable.
#[tokio::test]
async fn merge_isolation_property_parses_upper_snapshot_and_serializable() {
    assert_eq!(
        resolve_isolation_of(Some("SNAPSHOT"))
            .await
            .expect("SNAPSHOT must parse"),
        IsolationLevel::Snapshot
    );
    assert_eq!(
        resolve_isolation_of(Some("SERIALIZABLE"))
            .await
            .expect("SERIALIZABLE must parse"),
        IsolationLevel::Serializable
    );
    assert_eq!(
        resolve_isolation_of(None)
            .await
            .expect("unset defaults to serializable"),
        IsolationLevel::Serializable
    );
}

/// PIN M13 parse — NO trim. A padded value is garbage, not snapshot.
/// Risk: "improving" the DML copy with `.trim()` would honor `  snapshot  `
/// and diverge from DELETE/UPDATE isolation parse.
#[tokio::test]
async fn merge_isolation_property_padded_snapshot_is_garbage() {
    let error = resolve_isolation_of(Some("  snapshot  "))
        .await
        .expect_err("padded snapshot is garbage: the DML resolver does not trim");
    assert_invalid_isolation(error, "  snapshot  ");
}

/// PIN M13 parse — unknown token is a loud Plan error naming the raw value.
/// Risk: silent default-to-serializable would hide a typo'd table property.
#[tokio::test]
async fn merge_isolation_property_garbage_is_plan_error() {
    let error = resolve_isolation_of(Some("read-committed"))
        .await
        .expect_err("unknown isolation must Plan-error");
    assert_invalid_isolation(error, "read-committed");
}

/// PIN M13 thread-through — `commit` surfaces the Plan needle (not only the helper).
/// Uses a non-empty add so an empty-commit short-circuit cannot skip the resolve.
#[tokio::test]
async fn commit_rejects_invalid_merge_isolation_level() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup_with_isolation(&warehouse, "read-committed").await;
    let table = catalog.load_table(&ident).await.expect("load table");
    let error = commit(
        &catalog,
        &table,
        None,
        Vec::new(),
        vec![data_file("test/insert.parquet")],
    )
    .await
    .expect_err("commit must resolve isolation before writing");
    assert_invalid_isolation(error, "read-committed");
}

/// PIN M19-A (Spark S5) — SERIALIZABLE half of the same-race split. Explicit
/// `write.merge.isolation-level=serializable` (not merely the unset default)
/// must reject a concurrent append on an insert-only MERGE — the F-BR-1 class.
/// Control for
/// [`commit_insert_only_snapshot_isolation_commits_through_conflicting_concurrent_append`].
#[tokio::test]
async fn commit_insert_only_serializable_isolation_rejects_conflicting_concurrent_append() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup_with_isolation(&warehouse, "serializable").await;

    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;
    append(&catalog, &ident, vec![data_file("test/concurrent.parquet")]).await;

    let error = commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        Vec::new(),
        vec![data_file("test/insert.parquet")],
    )
    .await
    .expect_err("serializable MERGE must reject the conflicting concurrent add");

    let ice = iceberg_error(&error);
    assert_eq!(ice.kind(), iceberg::ErrorKind::DataInvalid);
    assert!(!ice.retryable());
    assert!(
        ice.message().contains("Found conflicting files"),
        "must be validate_no_conflicting_data, got: {}",
        ice.message()
    );
    let live = live_data_file_paths(&catalog, &ident).await;
    assert!(
        !live.contains("test/insert.parquet"),
        "the rejected insert must not be in the table, live={live:?}"
    );
}

/// PIN M19-A (Spark S5) — SNAPSHOT half of the same-race split. RED-THEN-GREEN:
/// without reading `write.merge.isolation-level`, `commit` hard-wires
/// `IsolationLevel::Serializable` and this case rejects the concurrent append
/// (same as the serializable sibling). With the resolver threaded through,
/// snapshot drops `validate_no_conflicting_data` and the insert commits —
/// Spark S5: snapshot MERGE allows a concurrent unrelated append.
#[tokio::test]
async fn commit_insert_only_snapshot_isolation_commits_through_conflicting_concurrent_append() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup_with_isolation(&warehouse, "SNAPSHOT").await;

    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;
    append(&catalog, &ident, vec![data_file("test/concurrent.parquet")]).await;

    commit(
        &catalog,
        &table_at_pin,
        Some(pin),
        Vec::new(),
        vec![data_file("test/insert.parquet")],
    )
    .await
    .expect("snapshot MERGE must commit through a concurrent append (Spark S5 / M19-A)");

    let live = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        live,
        HashSet::from([
            "test/a.parquet".to_string(),
            "test/concurrent.parquet".to_string(),
            "test/insert.parquet".to_string(),
        ]),
        "base + concurrent add + snapshot MERGE insert are all live, live={live:?}"
    );
}

/// PIN M19-A merge-on-read twin — `commit_row_delta` must honor snapshot the same way
/// (drop `validate_no_conflicting_data_files`). Same race as T5; T5 is the
/// serializable reject. RED-THEN-GREEN without threading isolation through
/// `commit_row_delta`.
#[tokio::test]
async fn commit_row_delta_snapshot_isolation_commits_through_conflicting_concurrent_append() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup_with_isolation(&warehouse, "snapshot").await;

    let (table_at_pin, pin) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;
    append(&catalog, &ident, vec![data_file("test/concurrent.parquet")]).await;

    commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        Vec::new(),
        crate::write::concurrency::WriteConcurrency::default(),
    )
    .await
    .expect("snapshot merge-on-read MERGE must commit through a concurrent append");

    let live = live_data_file_paths(&catalog, &ident).await;
    assert_eq!(
        live,
        HashSet::from([
            "test/a.parquet".to_string(),
            "test/concurrent.parquet".to_string(),
        ]),
        "MoR snapshot MERGE leaves A + the concurrent add live, live={live:?}"
    );
}

/// pins: rp-1-fork-repin/C-010
///
/// F-0 engine follow-up. Snapshot isolation is a supported opt-down (drops
/// `validate_no_conflicting_data_files`) but still arms `validate_data_files_exist`. After
/// fork `#214` that walk includes `Operation::Replace`. A concurrent compaction that
/// REPLACES the referenced data file must reject the snapshot-arm row delta rather than
/// committing a dangling position delete (silent resurrection).
#[tokio::test]
async fn commit_row_delta_snapshot_rejects_concurrent_replace_compaction_of_referenced_file() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup_with_isolation(&warehouse, "snapshot").await;

    let a = data_file("test/a.parquet");
    let (table_at_pin, pin) = append(&catalog, &ident, vec![a.clone()]).await;

    let compacted =
        concurrent_replace_compaction(&catalog, &ident, a, data_file("test/a-compacted.parquet"))
            .await;
    let concurrent_snapshot = compacted
        .metadata()
        .current_snapshot()
        .expect("the compaction produced a snapshot");
    assert_eq!(
        concurrent_snapshot.summary().operation,
        Operation::Replace,
        "the concurrent compaction must record Operation::Replace — otherwise this pin \
         would not exercise F-0"
    );

    let error = commit_row_delta(
        &catalog,
        &table_at_pin,
        Some(pin),
        vec![(std::sync::Arc::<str>::from("test/a.parquet"), 0)],
        Vec::new(),
        crate::write::concurrency::WriteConcurrency::default(),
    )
    .await
    .expect_err(
        "snapshot MERGE must still reject a Replace-compaction of the referenced data file",
    );

    let ice = iceberg_error(&error);
    assert_eq!(
        ice.kind(),
        iceberg::ErrorKind::DataInvalid,
        "a dangling referenced data file is a non-retryable validation failure"
    );
    assert!(!ice.retryable());
    assert!(
        ice.message().contains("test/a.parquet"),
        "the failure must NAME the missing referenced data file, got: {}",
        ice.message()
    );
}
