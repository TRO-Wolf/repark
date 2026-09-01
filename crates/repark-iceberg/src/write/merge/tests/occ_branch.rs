use std::sync::Arc;

use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};
use tempfile::TempDir;

use super::super::commit_on_ref;
use super::occ::{append, data_file, iceberg_error, setup_with_isolation};

const _: &str = "pins: rp-5-fork-repin/C-004";

async fn append_to_branch(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    branch: &str,
    files: Vec<DataFile>,
) -> (Table, i64) {
    let table = catalog.load_table(ident).await.expect("load table");
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(files).to_branch(branch);
    let tx = action.apply(tx).expect("apply fast_append to branch");
    let table = tx
        .commit(catalog.as_ref())
        .await
        .expect("commit fast_append to branch");
    let snapshot_id = table
        .metadata()
        .snapshot_for_ref(branch)
        .expect("branch snapshot")
        .snapshot_id();
    (table, snapshot_id)
}

#[tokio::test]
async fn commit_on_branch_rejects_or_retries_concurrent_branch_append() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup_with_isolation(&warehouse, "serializable").await;
    let (_seed, snap) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;
    crate::write::testing_create_ref(
        catalog.as_ref(),
        &ident,
        crate::write::SnapshotRefKind::Branch,
        "audit",
        snap,
    )
    .await
    .expect("create branch");
    let table = catalog.load_table(&ident).await.expect("reload");
    let pin = table
        .metadata()
        .snapshot_for_ref("audit")
        .expect("audit")
        .snapshot_id();
    append_to_branch(
        &catalog,
        &ident,
        "audit",
        vec![data_file("test/branch-concurrent.parquet")],
    )
    .await;
    let result = commit_on_ref(
        &catalog,
        &table,
        Some(pin),
        Vec::new(),
        vec![data_file("test/branch-insert.parquet")],
        Some("audit"),
    )
    .await;
    match result {
        Ok(()) => {
            let live = catalog.load_table(&ident).await.expect("load");
            let branch_id = live
                .metadata()
                .snapshot_for_ref("audit")
                .expect("audit after commit")
                .snapshot_id();
            assert_ne!(branch_id, pin, "retry must move the branch head");
        }
        Err(error) => {
            let ice = iceberg_error(&error);
            assert!(
                ice.retryable() || ice.kind() == iceberg::ErrorKind::DataInvalid,
                "measured branch OCC: retryable or validation, got kind={:?} retryable={} msg={}",
                ice.kind(),
                ice.retryable(),
                ice.message()
            );
        }
    }
}

#[tokio::test]
async fn commit_on_branch_ignores_concurrent_main_append() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, ident) = setup_with_isolation(&warehouse, "serializable").await;
    let (_seed, snap) = append(&catalog, &ident, vec![data_file("test/a.parquet")]).await;
    crate::write::testing_create_ref(
        catalog.as_ref(),
        &ident,
        crate::write::SnapshotRefKind::Branch,
        "audit",
        snap,
    )
    .await
    .expect("create branch");
    let table = catalog.load_table(&ident).await.expect("reload");
    let pin = table
        .metadata()
        .snapshot_for_ref("audit")
        .expect("audit")
        .snapshot_id();
    let main_before = table
        .metadata()
        .current_snapshot_id()
        .expect("main snapshot");
    append(
        &catalog,
        &ident,
        vec![data_file("test/main-concurrent.parquet")],
    )
    .await;
    commit_on_ref(
        &catalog,
        &table,
        Some(pin),
        Vec::new(),
        vec![data_file("test/branch-insert.parquet")],
        Some("audit"),
    )
    .await
    .expect("a concurrent main append must not fail a branch MERGE commit");
    let live = catalog.load_table(&ident).await.expect("load");
    let main_after = live.metadata().current_snapshot_id().expect("main after");
    let branch_after = live
        .metadata()
        .snapshot_for_ref("audit")
        .expect("audit after")
        .snapshot_id();
    assert_ne!(main_after, main_before, "main concurrent append must land");
    assert_ne!(branch_after, pin, "branch commit must land");
    assert_ne!(
        main_after, branch_after,
        "main and branch heads stay distinct"
    );
}
