use super::*;

#[tokio::test]
async fn a_landed_unknown_outcome_marks_the_scope_committed_at_the_resolved_snapshot() {
    let (_warehouse, memory, ident) =
        fixture_with("w_walk_marks", &[("commit.status-check.num-retries", "0")]).await;
    stamped_append(&memory, &ident, &stamp_for(0, SinkDoor::Table), &[1]).await;
    let table = memory.load_table(&ident).await.expect("load");
    let probe = Arc::new(ProbeCatalog::new(
        Arc::clone(&memory),
        ProbeMode::LandedThenReconcileFails,
    ));
    let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
    let stamp = stamp_for(1, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let resolved = commit_stamp_only(&catalog, &table, &stamp, Some(guard.token()))
        .await
        .expect("the landed attempt resolves");
    assert_eq!(
        guard.outcome(),
        ScopeOutcome::Committed { snapshot: resolved }
    );
    drop(guard);
    assert_eq!(probe.seen().len(), 1);
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(
        reloaded.metadata().current_snapshot_id(),
        Some(resolved.get())
    );
}

#[tokio::test]
async fn a_failed_reload_in_the_walk_refuses_unknown_with_no_durable_record() {
    let (_warehouse, memory, ident) = fixture_with(
        "w_walk_reload_fails",
        &[("commit.status-check.num-retries", "0")],
    )
    .await;
    stamped_append(&memory, &ident, &stamp_for(0, SinkDoor::Table), &[1]).await;
    let table = memory.load_table(&ident).await.expect("load");
    let snapshots = table.metadata().snapshots().count();
    let probe = Arc::new(ProbeCatalog::new(
        Arc::clone(&memory),
        ProbeMode::UnknownThenLoadsFail,
    ));
    let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
    let stamp = stamp_for(1, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let refused = commit_stamp_only(&catalog, &table, &stamp, Some(guard.token()))
        .await
        .expect_err("an unreadable sink never resolves");
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);
    let (epoch, found, reason) = recovery_reason(refused);
    assert_eq!(epoch, Epoch::new(1));
    assert_eq!(found, None);
    assert!(matches!(
        reason,
        RecoveryReason::CommitOutcomeUnknown {
            operation_id: Some(_)
        }
    ));
    assert_eq!(probe.seen().len(), 1);
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(reloaded.metadata().snapshots().count(), snapshots);
}
