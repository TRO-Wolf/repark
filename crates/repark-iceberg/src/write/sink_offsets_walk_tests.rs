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

#[tokio::test]
async fn a_rolled_back_attempt_stays_unknown_at_its_epoch_with_the_durable_record() {
    let (_warehouse, catalog, ident) = fixture("w_walk_rolled_back").await;
    let durable = stamp_for(0, SinkDoor::Table);
    stamped_append(&catalog, &ident, &durable, &[1]).await;
    let base = catalog.load_table(&ident).await.expect("load");
    let base_head = base.metadata().current_snapshot_id().expect("base head");
    let stamp = stamp_for(1, SinkDoor::ForeachBatch);
    let landed_id = commit_stamp_only(&catalog, &base, &stamp, None)
        .await
        .expect("stamp-only");
    let landed = catalog.load_table(&ident).await.expect("reload");
    let operation = landed
        .metadata()
        .current_snapshot()
        .expect("head")
        .summary()
        .additional_properties
        .get(OPERATION_ID_PROP)
        .cloned()
        .expect("operation id");
    let tx = Transaction::new(&landed);
    let tx = tx
        .manage_snapshots()
        .create_branch("side", landed_id.get())
        .rollback_to(base_head)
        .apply(tx)
        .expect("rollback");
    let rolled = tx.commit(catalog.as_ref()).await.expect("commit rollback");
    assert_eq!(rolled.metadata().current_snapshot_id(), Some(base_head));
    let refused = resolve_unknown_outcome(&catalog, &base, &stamp, Some(&operation))
        .await
        .expect_err("a rolled-back attempt on a side branch is not landed");
    let (epoch, found, reason) = recovery_reason(refused);
    assert_eq!(epoch, Epoch::new(1));
    assert_eq!(found, Some(durable.record));
    assert_eq!(
        reason,
        RecoveryReason::CommitOutcomeUnknown {
            operation_id: Some(operation)
        }
    );
}
