use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fence {
    None,
    BranchA,
    BranchAEmptyAllowed,
    Catalog,
}

struct Measured {
    _warehouse: TempDir,
    result: iceberg::Result<Table>,
    table: Table,
    added: Vec<Operation>,
}

fn operations(table: &Table) -> Vec<Operation> {
    table
        .metadata()
        .snapshots()
        .map(|snapshot| snapshot.summary().operation.clone())
        .collect()
}

fn fenced(tx: Transaction, base: Option<i64>, fence: Fence) -> Transaction {
    if matches!(fence, Fence::None | Fence::Catalog) {
        return tx;
    }
    let action = tx.overwrite_files().validate_no_conflicting_data();
    let action = match base {
        Some(base) => action.validate_from_snapshot(base),
        None => action,
    };
    let action = match fence {
        Fence::BranchAEmptyAllowed => action.allow_empty_commit(),
        _ => action,
    };
    action.apply(tx).expect("apply the branch-A fence")
}

async fn measure(name: &str, fence: Fence, race: bool) -> Measured {
    let (warehouse, catalog, ident) = fixture(name).await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let base = seeded.metadata().current_snapshot_id();
    let files = stage(&seeded, &[2]).await;
    if race {
        append_plain(&catalog, &ident, &[3]).await;
    }
    let before = catalog
        .load_table(&ident)
        .await
        .expect("load")
        .metadata()
        .snapshots()
        .count();
    let claimed = ClaimedStamp {
        stamp: stamp_for(1, SinkDoor::Table),
        base: base.map(SnapshotId::new),
        started: None,
    };
    let summary: HashMap<String, String> = claimed
        .summary_entries()
        .expect("entries")
        .into_iter()
        .collect();
    let tx = Transaction::new(&seeded);
    let tx = tx
        .merge_append()
        .add_data_files(files)
        .set_snapshot_properties(summary)
        .apply(tx)
        .expect("apply append");
    let tx = claimed.stamp_transaction(tx).expect("stamp");
    let tx = fenced(tx, base, fence);
    let committer = match fence {
        Fence::Catalog => AppendFence::install(&catalog, &claimed),
        _ => Arc::clone(&catalog),
    };
    let result = tx.commit(committer.as_ref()).await;
    let table = catalog.load_table(&ident).await.expect("reload");
    let added = operations(&table).split_off(before);
    Measured {
        _warehouse: warehouse,
        result,
        table,
        added,
    }
}

#[tokio::test]
async fn dm6_the_stamped_append_rebases_past_a_moved_base() {
    let measured = measure("dm6_baseline", Fence::None, true).await;
    assert!(measured.result.is_ok());
    assert_eq!(measured.added, vec![Operation::Append]);
    assert_eq!(live_ids(&measured.table).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn dm6_branch_a_refuses_every_commit_without_a_race() {
    let measured = measure("dm6_branch_a_quiet", Fence::BranchA, false).await;
    let error = measured.result.expect_err("branch A without a race");
    assert_eq!(error.kind(), iceberg::ErrorKind::PreconditionFailed);
    assert!(measured.added.is_empty());
    assert_eq!(live_ids(&measured.table).await, vec![1]);
}

#[tokio::test]
async fn dm6_branch_a_fails_a_moved_base_at_validation() {
    let measured = measure("dm6_branch_a_race", Fence::BranchA, true).await;
    let error = measured.result.expect_err("branch A under a race");
    assert_eq!(error.kind(), iceberg::ErrorKind::DataInvalid);
    assert!(measured.added.is_empty());
    assert_eq!(live_ids(&measured.table).await, vec![1, 3]);
}

#[tokio::test]
async fn dm6_branch_a_with_empty_commits_allowed_never_commits() {
    let measured = measure("dm6_branch_a_empty", Fence::BranchAEmptyAllowed, false).await;
    let error = measured
        .result
        .expect_err("two snapshot producers in one transaction");
    assert_eq!(error.kind(), iceberg::ErrorKind::CatalogCommitConflicts);
    assert!(measured.added.is_empty());
    assert_eq!(live_ids(&measured.table).await, vec![1]);
}

#[tokio::test]
async fn dm6_the_catalog_fence_lands_one_append_on_a_quiet_commit() {
    let measured = measure("dm6_catalog_fence_quiet", Fence::Catalog, false).await;
    assert!(measured.result.is_ok());
    assert_eq!(measured.added, vec![Operation::Append]);
    assert_eq!(live_ids(&measured.table).await, vec![1, 2]);
}

#[tokio::test]
async fn dm6_the_catalog_fence_rebases_past_an_unrelated_append() {
    let measured = measure("dm6_catalog_fence_race", Fence::Catalog, true).await;
    assert!(measured.result.is_ok());
    assert_eq!(measured.added, vec![Operation::Append]);
    assert_eq!(live_ids(&measured.table).await, vec![1, 2, 3]);
}
