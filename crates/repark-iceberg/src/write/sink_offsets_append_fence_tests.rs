use super::*;

fn racer_run() -> RunId {
    RunId::new(Uuid::parse_str("cccccccc-0000-4000-8000-0000000000c3").expect("uuid"))
}

fn racer_of_this_query(epoch: u64) -> CommitStamp {
    let mut stamp = stamp_for(epoch, SinkDoor::Table);
    stamp.record.run = racer_run();
    stamp
}

fn probed(memory: &Arc<dyn Catalog>) -> (Arc<ProbeCatalog>, Arc<dyn Catalog>) {
    let probe = Arc::new(ProbeCatalog::new(Arc::clone(memory), ProbeMode::Forward));
    let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
    (probe, catalog)
}

fn head_carries_the_query(table: &Table) -> bool {
    table.metadata().current_snapshot().is_some_and(|head| {
        head.summary()
            .additional_properties
            .contains_key(QUERY_ID_KEY)
    })
}

#[tokio::test]
async fn a_stamped_append_fails_at_its_base_when_its_own_query_stamped_above_it() {
    let (_warehouse, memory, ident) = fixture("af_append_race").await;
    let table = append_plain(&memory, &ident, &[1]).await;
    let (probe, catalog) = probed(&memory);
    let racer = racer_of_this_query(3);
    *probe.stamped_racer.lock().expect("stamped") =
        Some((racer.clone(), stage(&table, &[70]).await));
    let stamp = stamp_for(3, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let files = stage(&table, &[2]).await;
    let error = commit_append_with_summary(&catalog, &table, files, &scoped(&guard), None)
        .await
        .expect_err("the append must fail at its pinned base");
    let fenced = MicroBatchError::Fenced {
        query: query(),
        epoch: Epoch::new(3),
        winner: racer_run(),
    };
    assert_eq!(microbatch_cause(&error), &fenced);
    assert_eq!(probe.seen().len(), 1);
    assert_eq!(probe.loads(), 2);
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    assert_eq!(
        BatchScope::claim(&table, guard.token()),
        Err(fenced.clone())
    );
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(BatchScope::claim(&reloaded, guard.token()), Err(fenced));
    drop(guard);
    assert_eq!(stamped_snapshots(&reloaded), 1);
    assert_eq!(live_ids(&reloaded).await, vec![1, 70]);
    assert_eq!(
        read_resume_point(&reloaded, query()).expect("resume"),
        Some(racer.record)
    );
}

#[tokio::test]
async fn a_stamp_only_commit_fails_at_its_base_when_its_own_query_stamped_above_it() {
    let (_warehouse, memory, ident) = fixture("af_stamp_only_race").await;
    let table = append_plain(&memory, &ident, &[1]).await;
    let (probe, catalog) = probed(&memory);
    let racer = racer_of_this_query(3);
    *probe.stamped_racer.lock().expect("stamped") =
        Some((racer.clone(), stage(&table, &[70]).await));
    let stamp = stamp_for(3, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let refused = commit_stamp_only(&catalog, &table, &stamp, Some(guard.token()))
        .await
        .expect_err("the stamp-only commit must fail at its pinned base");
    let fenced = MicroBatchError::Fenced {
        query: query(),
        epoch: Epoch::new(3),
        winner: racer_run(),
    };
    assert_eq!(refused, fenced);
    assert_eq!(probe.seen().len(), 1);
    assert_eq!(probe.loads(), 2);
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    assert_eq!(BatchScope::claim(&table, guard.token()), Err(fenced));
    drop(guard);
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(stamped_snapshots(&reloaded), 1);
    assert_eq!(live_ids(&reloaded).await, vec![1, 70]);
}

#[tokio::test]
async fn an_empty_base_treats_every_snapshot_on_main_as_concurrent() {
    let (_warehouse, memory, ident) = fixture("af_empty_base").await;
    let empty = memory.load_table(&ident).await.expect("load");
    assert_eq!(empty.metadata().current_snapshot_id(), None);
    let first = stamp_for(0, SinkDoor::Table);
    let landed = stamped_append(&memory, &ident, &first, &[1]).await;
    let newer = landed.metadata().current_snapshot_id().expect("head");
    let snapshots = landed.metadata().snapshots().count();

    let replayed = commit_stamp_only(&memory, &empty, &first, None)
        .await
        .expect_err("epoch 0 from the empty view must not land again");
    assert_eq!(
        replayed,
        MicroBatchError::AlreadyCommitted {
            query: query(),
            epoch: Epoch::new(0),
        }
    );

    let next = stamp_for(1, SinkDoor::Table);
    let ahead = commit_stamp_only(&memory, &empty, &next, None)
        .await
        .expect_err("epoch 1 from the empty view must not re-base over epoch 0");
    assert_eq!(
        ahead,
        MicroBatchError::Catalog(format!(
            "append fence: query {query} epoch 1 pinned base snapshot none; newer snapshot {newer} on main carries repark.cdc.query-id={query}",
            query = query()
        ))
    );
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(reloaded.metadata().snapshots().count(), snapshots);
    assert_eq!(
        read_resume_point(&reloaded, query()).expect("resume"),
        Some(first.record)
    );
}

#[tokio::test]
async fn a_base_that_left_main_fails_the_commit_on_both_doors() {
    let (_warehouse, memory, ident) = fixture("af_base_left_main").await;
    let kept = append_plain(&memory, &ident, &[1]).await;
    let kept_head = kept.metadata().current_snapshot_id().expect("kept head");
    let stale = append_plain(&memory, &ident, &[2]).await;
    let base = stale.metadata().current_snapshot_id().expect("base");
    let tx = Transaction::new(&stale);
    let tx = tx
        .manage_snapshots()
        .rollback_to(kept_head)
        .apply(tx)
        .expect("rollback");
    let rolled = tx.commit(memory.as_ref()).await.expect("commit rollback");
    assert_eq!(rolled.metadata().current_snapshot_id(), Some(kept_head));
    let snapshots = rolled.metadata().snapshots().count();
    let left_main = |epoch: u64| {
        MicroBatchError::Catalog(format!(
            "append fence: query {query} epoch {epoch} pinned base snapshot {base}, which is no longer an ancestor of main (head {kept_head}); nothing can be proven about repark.cdc.query-id={query} above it",
            query = query()
        ))
    };

    let refused = commit_stamp_only(&memory, &stale, &stamp_for(0, SinkDoor::ForeachBatch), None)
        .await
        .expect_err("the stamp-only door must fail on a base off main");
    assert_eq!(refused, left_main(0));

    let stamp = stamp_for(1, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&stale), stamp).expect("enter");
    let files = stage(&stale, &[3]).await;
    let error = commit_append_with_summary(&memory, &stale, files, &scoped(&guard), None)
        .await
        .expect_err("the append door must fail on a base off main");
    assert_eq!(microbatch_cause(&error), &left_main(1));
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);

    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(reloaded.metadata().snapshots().count(), snapshots);
    assert_eq!(stamped_snapshots(&reloaded), 0);
    assert_eq!(live_ids(&reloaded).await, vec![1]);
}

#[tokio::test]
async fn an_unstamped_append_commits_through_the_callers_own_catalog() {
    let (_warehouse, memory, ident) = fixture("af_unstamped").await;
    let table = append_plain(&memory, &ident, &[1]).await;
    let (probe, catalog) = probed(&memory);
    let unstamped = SiteStamp::claim(&table, None, &[]).expect("no claim");
    assert!(Arc::ptr_eq(&unstamped.fenced(&catalog), &catalog));

    *probe.stamped_racer.lock().expect("stamped") =
        Some((racer_of_this_query(3), stage(&table, &[70]).await));
    let files = stage(&table, &[2]).await;
    let committed = commit_append_with_summary(&catalog, &table, files, &[], None)
        .await
        .expect("an unstamped append re-bases past this query's stamp");
    assert_eq!(probe.seen().len(), 2);
    assert_eq!(probe.loads(), 2);
    assert!(!head_carries_the_query(&committed));
    assert_eq!(stamped_snapshots(&committed), 1);
    assert_eq!(live_ids(&committed).await, vec![1, 2, 70]);

    let stamp = stamp_for(4, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&committed), stamp).expect("enter");
    let stamped = SiteStamp::claim(&committed, None, &scoped(&guard)).expect("claim");
    assert!(!Arc::ptr_eq(&stamped.fenced(&catalog), &catalog));
}

#[tokio::test]
async fn the_fence_forwards_every_other_catalog_call() {
    let (_warehouse, memory, ident) = fixture("af_forward").await;
    let table = append_plain(&memory, &ident, &[1]).await;
    let (probe, catalog) = probed(&memory);
    let claimed = ClaimedStamp {
        stamp: stamp_for(0, SinkDoor::Table),
        base: table.metadata().current_snapshot_id().map(SnapshotId::new),
    };
    let direct = AppendFence::install(&memory, &claimed);
    assert_eq!(direct.name(), memory.name());
    assert_eq!(direct.name(), "memory");
    assert_eq!(direct.properties(), memory.properties());
    assert_eq!(
        direct
            .list_views(ident.namespace())
            .await
            .expect("the memory catalog lists views"),
        Vec::<TableIdent>::new()
    );
    let fenced = AppendFence::install(&catalog, &claimed);
    let namespace = ident.namespace();
    assert_eq!(
        fenced.list_namespaces(None).await.expect("namespaces"),
        memory.list_namespaces(None).await.expect("namespaces")
    );
    assert!(fenced.namespace_exists(namespace).await.expect("exists"));
    assert_eq!(
        fenced.list_tables(namespace).await.expect("tables"),
        vec![ident.clone()]
    );
    assert!(fenced.table_exists(&ident).await.expect("table exists"));
    assert_eq!(probe.loads(), 0);
    let loaded = fenced.load_table(&ident).await.expect("load");
    assert_eq!(probe.loads(), 1);
    assert_eq!(loaded.metadata_location(), table.metadata_location());
    assert_eq!(loaded.metadata(), table.metadata());
}

async fn expire(catalog: &Arc<dyn Catalog>, ident: &TableIdent, snapshot: i64) -> Table {
    let table = catalog.load_table(ident).await.expect("load");
    let tx = Transaction::new(&table);
    let tx = tx
        .expire_snapshots()
        .expire_snapshot_id(snapshot)
        .apply(tx)
        .expect("apply expire");
    tx.commit(catalog.as_ref()).await.expect("expire")
}

struct ExpiredAboveEmpty {
    empty: Table,
    first: CommitStamp,
    snapshots: usize,
}

async fn expired_stamp_above_an_empty_base(
    name: &str,
) -> (TempDir, Arc<dyn Catalog>, TableIdent, ExpiredAboveEmpty) {
    let (warehouse, memory, ident) = fixture(name).await;
    let empty = memory.load_table(&ident).await.expect("load");
    assert_eq!(empty.metadata().current_snapshot_id(), None);
    let first = stamp_for(0, SinkDoor::Table);
    let landed = stamped_append(&memory, &ident, &first, &[1]).await;
    let stamped = landed.metadata().current_snapshot_id().expect("stamped");
    append_plain(&memory, &ident, &[2]).await;
    let expired = expire(&memory, &ident, stamped).await;
    assert!(expired.metadata().snapshot_by_id(stamped).is_none());
    assert!(matches!(
        read_resume_point(&expired, query()),
        Err(MicroBatchError::RecoveryRequired {
            reason: RecoveryReason::StampedSnapshotExpired,
            ..
        })
    ));
    let snapshots = expired.metadata().snapshots().count();
    (
        warehouse,
        memory,
        ident,
        ExpiredAboveEmpty {
            empty,
            first,
            snapshots,
        },
    )
}

fn stamped_snapshot_expired(first: &CommitStamp) -> MicroBatchError {
    MicroBatchError::RecoveryRequired {
        query: query(),
        epoch: Epoch::new(0),
        durable: Some(Box::new(first.record.clone())),
        reason: RecoveryReason::StampedSnapshotExpired,
    }
}

#[tokio::test]
async fn an_expired_stamp_above_an_empty_base_refuses_the_append_door() {
    let (_warehouse, memory, ident, setup) =
        expired_stamp_above_an_empty_base("af_expired_empty_append").await;
    let guard = BatchScope::enter(TableUuid::of(&setup.empty), setup.first.clone()).expect("enter");
    let files = stage(&setup.empty, &[1]).await;
    let error = commit_append_with_summary(&memory, &setup.empty, files, &scoped(&guard), None)
        .await
        .expect_err("epoch 0 must not land again above an expired stamp");
    assert_eq!(
        microbatch_cause(&error),
        &stamped_snapshot_expired(&setup.first)
    );
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(reloaded.metadata().snapshots().count(), setup.snapshots);
    assert_eq!(live_ids(&reloaded).await, vec![1, 2]);
}

#[tokio::test]
async fn an_expired_stamp_above_an_empty_base_refuses_the_stamp_only_door() {
    let (_warehouse, memory, ident, setup) =
        expired_stamp_above_an_empty_base("af_expired_empty_stamp_only").await;
    let stamp = stamp_for(0, SinkDoor::ForeachBatch);
    let refused = commit_stamp_only(&memory, &setup.empty, &stamp, None)
        .await
        .expect_err("epoch 0 must not land again above an expired stamp");
    assert_eq!(refused, stamped_snapshot_expired(&setup.first));
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(reloaded.metadata().snapshots().count(), setup.snapshots);
    assert_eq!(live_ids(&reloaded).await, vec![1, 2]);
}

struct ExpiredBelowLive {
    view: Table,
    first: CommitStamp,
    snapshots: usize,
}

async fn expired_stamp_below_a_live_base(
    name: &str,
) -> (TempDir, Arc<dyn Catalog>, TableIdent, ExpiredBelowLive) {
    let (warehouse, memory, ident) = fixture(name).await;
    let first = stamp_for(0, SinkDoor::Table);
    let landed = stamped_append(&memory, &ident, &first, &[1]).await;
    let stamped = landed.metadata().current_snapshot_id().expect("stamped");
    let view = append_plain(&memory, &ident, &[2]).await;
    assert!(view.metadata().current_snapshot_id().is_some());
    let expired = expire(&memory, &ident, stamped).await;
    assert!(expired.metadata().snapshot_by_id(stamped).is_none());
    let snapshots = expired.metadata().snapshots().count();
    (
        warehouse,
        memory,
        ident,
        ExpiredBelowLive {
            view,
            first,
            snapshots,
        },
    )
}

#[tokio::test]
async fn an_expired_stamp_below_a_live_base_refuses_the_append_door() {
    let (_warehouse, memory, ident, setup) =
        expired_stamp_below_a_live_base("af_expired_below_append").await;
    let next = stamp_for(1, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&setup.view), next).expect("enter");
    let files = stage(&setup.view, &[3]).await;
    let error = commit_append_with_summary(&memory, &setup.view, files, &scoped(&guard), None)
        .await
        .expect_err("epoch 1 must not land from a stale view while epoch 0's stamp is expired");
    assert_eq!(
        microbatch_cause(&error),
        &stamped_snapshot_expired(&setup.first)
    );
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(reloaded.metadata().snapshots().count(), setup.snapshots);
    assert_eq!(stamped_snapshots(&reloaded), 0);
    assert_eq!(live_ids(&reloaded).await, vec![1, 2]);
}

#[tokio::test]
async fn an_expired_stamp_below_a_live_base_refuses_the_stamp_only_door() {
    let (_warehouse, memory, ident, setup) =
        expired_stamp_below_a_live_base("af_expired_below_stamp_only").await;
    let next = stamp_for(1, SinkDoor::ForeachBatch);
    let refused = commit_stamp_only(&memory, &setup.view, &next, None)
        .await
        .expect_err("epoch 1 must not land from a stale view while epoch 0's stamp is expired");
    assert_eq!(refused, stamped_snapshot_expired(&setup.first));
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(reloaded.metadata().snapshots().count(), setup.snapshots);
    assert_eq!(stamped_snapshots(&reloaded), 0);
    assert_eq!(live_ids(&reloaded).await, vec![1, 2]);
}

#[tokio::test]
async fn an_expired_base_with_an_unrelated_append_is_refused_like_a_rollback_and_a_fresh_view_commits()
 {
    let (_warehouse, memory, ident) = fixture("af_expired_base").await;
    let view = append_plain(&memory, &ident, &[1]).await;
    let base = view.metadata().current_snapshot_id().expect("base");
    let head = append_plain(&memory, &ident, &[2]).await;
    let head_id = head.metadata().current_snapshot_id().expect("head");
    let expired = expire(&memory, &ident, base).await;
    assert!(expired.metadata().snapshot_by_id(base).is_none());
    let snapshots = expired.metadata().snapshots().count();

    let stamp = stamp_for(0, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&view), stamp.clone()).expect("enter");
    let files = stage(&view, &[3]).await;
    let error = commit_append_with_summary(&memory, &view, files, &scoped(&guard), None)
        .await
        .expect_err("an expired base is refused like a rollback");
    assert_eq!(
        microbatch_cause(&error),
        &MicroBatchError::Catalog(format!(
            "append fence: query {query} epoch 0 pinned base snapshot {base}, which is no longer an ancestor of main (head {head_id}); nothing can be proven about repark.cdc.query-id={query} above it",
            query = query()
        ))
    );
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(reloaded.metadata().snapshots().count(), snapshots);
    assert_eq!(stamped_snapshots(&reloaded), 0);
    assert_eq!(live_ids(&reloaded).await, vec![1, 2]);

    let guard = BatchScope::enter(TableUuid::of(&reloaded), stamp).expect("enter again");
    let files = stage(&reloaded, &[3]).await;
    let committed = commit_append_with_summary(&memory, &reloaded, files, &scoped(&guard), None)
        .await
        .expect("a retry from a freshly loaded table commits");
    assert!(matches!(guard.outcome(), ScopeOutcome::Committed { .. }));
    drop(guard);
    assert_eq!(stamped_snapshots(&committed), 1);
    assert_eq!(live_ids(&committed).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn a_commit_without_a_base_table_is_checked_against_a_fresh_load() {
    let (_warehouse, memory, ident) = fixture("af_no_base_table").await;
    let view = append_plain(&memory, &ident, &[1]).await;
    let base = view.metadata().current_snapshot_id().map(SnapshotId::new);
    let racer = racer_of_this_query(3);
    stamped_append(&memory, &ident, &racer, &[70]).await;

    let capture = Arc::new(ProbeCatalog::new(Arc::clone(&memory), ProbeMode::Capture));
    let capturing: Arc<dyn Catalog> = Arc::clone(&capture) as Arc<dyn Catalog>;
    let files = stage(&view, &[2]).await;
    let tx = Transaction::new(&view);
    let tx = tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)
        .expect("apply");
    tx.commit(capturing.as_ref())
        .await
        .expect_err("the capturing catalog does not apply the commit");
    let mut commit = capture
        .take_captured()
        .expect("the commit reached the catalog");
    assert!(commit.take_base_table().is_some());
    assert!(commit.base_table().is_none());

    let (probe, inner) = probed(&memory);
    let claimed = ClaimedStamp {
        stamp: stamp_for(3, SinkDoor::Table),
        base,
    };
    let fence = AppendFence::install(&inner, &claimed);
    let error = fence
        .update_table(commit)
        .await
        .expect_err("the fence must check a commit that carries no base table");
    assert_eq!(
        crate::write::sink_offsets::append_fence::refusal_of(&error),
        Some(MicroBatchError::Fenced {
            query: query(),
            epoch: Epoch::new(3),
            winner: racer_run(),
        })
    );
    assert_eq!(probe.loads(), 1);
    assert!(probe.seen().is_empty());
    let reloaded = memory.load_table(&ident).await.expect("reload");
    assert_eq!(stamped_snapshots(&reloaded), 1);
    assert_eq!(live_ids(&reloaded).await, vec![1, 70]);
}

#[tokio::test]
async fn a_tokenless_stamp_only_refusal_does_not_latch_another_stamps_scope() {
    let (_warehouse, memory, ident) = fixture("af_tokenless_latch").await;
    let view = append_plain(&memory, &ident, &[1]).await;
    let racer = racer_of_this_query(3);
    stamped_append(&memory, &ident, &racer, &[70]).await;
    let fresh = memory.load_table(&ident).await.expect("load");

    let scope_stamp = stamp_for(4, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&fresh), scope_stamp).expect("enter");
    BatchScope::claim(&fresh, guard.token())
        .expect("claim")
        .expect("claimed");

    let other = stamp_for(3, SinkDoor::ForeachBatch);
    let refused = commit_stamp_only(&memory, &view, &other, None)
        .await
        .expect_err("the stale tokenless stamp-only commit must be fenced");
    assert_eq!(
        refused,
        MicroBatchError::Fenced {
            query: query(),
            epoch: Epoch::new(3),
            winner: racer_run(),
        }
    );
    assert_eq!(
        BatchScope::claim(&fresh, guard.token()),
        Err(MicroBatchError::SinkCommittedTwice {
            epoch: Epoch::new(4)
        })
    );
    drop(guard);
}
