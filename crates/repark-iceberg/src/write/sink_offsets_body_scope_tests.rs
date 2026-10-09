use super::*;

fn body_stamp(epoch: u64) -> CommitStamp {
    stamp_for(epoch, SinkDoor::ForeachBatch)
}

async fn set_property(catalog: &Arc<dyn Catalog>, table: &Table) -> iceberg::Result<Table> {
    let tx = Transaction::new(table);
    let tx = tx
        .update_table_properties()
        .set("eo.touched".to_string(), "yes".to_string())
        .apply(tx)
        .expect("apply the property change");
    tx.commit(catalog.as_ref()).await
}

fn refusal_in(error: &iceberg::Error) -> MicroBatchError {
    std::error::Error::source(error)
        .and_then(|source| source.downcast_ref::<MicroBatchError>())
        .cloned()
        .expect("a typed refusal")
}

#[tokio::test]
async fn a_commit_inside_the_body_scope_claims_without_a_token_in_its_extras() {
    let (_warehouse, catalog, ident) = fixture("body_claims").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stamp = body_stamp(0);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let files = stage(&table, &[2]).await;
    let committed = guard
        .scope_body(commit_append_with_summary(
            &catalog,
            &table,
            files,
            &[],
            None,
        ))
        .await
        .expect("the body's append");
    assert_stamped_head(&committed, &stamp);
    let head = committed.metadata().current_snapshot_id().expect("head");
    assert_eq!(
        guard.outcome(),
        ScopeOutcome::Committed {
            snapshot: SnapshotId::new(head)
        }
    );
    assert_eq!(guard.body_refusal(), None);
    let files = stage(&committed, &[3]).await;
    let error = guard
        .scope_body(commit_append_with_summary(
            &catalog,
            &committed,
            files,
            &[],
            None,
        ))
        .await
        .expect_err("the second write");
    let twice = MicroBatchError::SinkCommittedTwice {
        epoch: Epoch::new(0),
    };
    assert_eq!(microbatch_cause(&error), &twice);
    assert_eq!(guard.body_refusal(), Some(twice));
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(stamped_snapshots(&table), 1);
    assert_eq!(live_ids(&table).await, vec![1, 2]);
}

#[tokio::test]
async fn a_commit_outside_the_body_scope_never_claims() {
    let (_warehouse, catalog, ident) = fixture("body_outside").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let guard = BatchScope::enter(TableUuid::of(&table), body_stamp(0)).expect("enter");
    let committed = append_plain(&catalog, &ident, &[1]).await;
    assert_unstamped(&committed);
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    let head = committed.metadata().current_snapshot_id().expect("head");
    assert_eq!(
        unstamped_above(&committed, None),
        Some(SnapshotId::new(head))
    );
    assert_eq!(unstamped_above(&committed, Some(head)), None);
}

#[tokio::test]
async fn unstamped_above_skips_stamped_snapshots_and_stops_at_the_base() {
    let (_warehouse, catalog, ident) = fixture("body_above").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let base = seeded.metadata().current_snapshot_id();
    let guard = BatchScope::enter(TableUuid::of(&seeded), body_stamp(0)).expect("enter");
    let files = stage(&seeded, &[2]).await;
    let stamped = guard
        .scope_body(commit_append_with_summary(
            &catalog,
            &seeded,
            files,
            &[],
            None,
        ))
        .await
        .expect("the stamped append");
    assert_eq!(unstamped_above(&stamped, base), None);
    assert_eq!(
        unstamped_above(&stamped, None).map(SnapshotId::get),
        base,
        "with no base the walk reaches the seeded snapshot"
    );
    drop(guard);
    let foreign = append_plain(&catalog, &ident, &[3]).await;
    assert_eq!(
        unstamped_above(&foreign, base).map(SnapshotId::get),
        foreign.metadata().current_snapshot_id()
    );
}

#[tokio::test]
async fn the_guard_refuses_an_unstampable_sink_commit_and_passes_another_table() {
    let (_warehouse, catalog, ident) = fixture("body_guard").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    catalog
        .create_table(
            ident.namespace(),
            TableCreation::builder()
                .name("body_guard_side".to_string())
                .schema(id_schema())
                .build(),
        )
        .await
        .expect("the side table");
    let side_ident = TableIdent::new(ident.namespace().clone(), "body_guard_side".to_string());
    let side = catalog.load_table(&side_ident).await.expect("load side");
    assert!(Arc::ptr_eq(&guard_body_catalog(&catalog), &catalog));
    let guard = BatchScope::enter(TableUuid::of(&table), body_stamp(3)).expect("enter");
    let (refused, passed) = guard
        .scope_body(async {
            let guarded = guard_body_catalog(&catalog);
            assert!(!Arc::ptr_eq(&guarded, &catalog));
            (
                set_property(&guarded, &table).await,
                set_property(&guarded, &side).await,
            )
        })
        .await;
    let refusal = MicroBatchError::UnstampedSinkWrite {
        sink: ident.to_string(),
        epoch: Epoch::new(3),
    };
    assert_eq!(refusal_in(&refused.expect_err("the sink commit")), refusal);
    assert_eq!(guard.body_refusal(), Some(refusal));
    assert!(
        passed
            .expect("the side commit")
            .metadata()
            .properties()
            .contains_key("eo.touched")
    );
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert!(!reloaded.metadata().properties().contains_key("eo.touched"));
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    assert!(Arc::ptr_eq(&guard_body_catalog(&catalog), &catalog));
}

#[tokio::test]
async fn the_guard_admits_the_stamped_commit_and_refuses_what_follows_it() {
    let (_warehouse, catalog, ident) = fixture("body_guard_after").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stamp = body_stamp(1);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let files = stage(&table, &[2]).await;
    let (stamped, after) = guard
        .scope_body(async {
            let guarded = guard_body_catalog(&catalog);
            let stamped = commit_append_with_summary(&guarded, &table, files, &[], None)
                .await
                .expect("the stamped append through the guard");
            let after = set_property(&guarded, &stamped).await;
            (stamped, after)
        })
        .await;
    assert_stamped_head(&stamped, &stamp);
    assert!(matches!(
        refusal_in(&after.expect_err("the commit after the stamp")),
        MicroBatchError::UnstampedSinkWrite { .. }
    ));
}

#[tokio::test]
async fn a_guard_that_outlives_its_scope_commits_as_on_main() {
    let (_warehouse, catalog, ident) = fixture("body_guard_stale").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let guard = BatchScope::enter(TableUuid::of(&table), body_stamp(0)).expect("enter");
    let stale = guard
        .scope_body(async { guard_body_catalog(&catalog) })
        .await;
    drop(guard);
    let committed = set_property(&stale, &table)
        .await
        .expect("no scope is active");
    assert!(committed.metadata().properties().contains_key("eo.touched"));
    let next = BatchScope::enter(TableUuid::of(&table), body_stamp(1)).expect("enter again");
    let table = catalog.load_table(&ident).await.expect("reload");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set("eo.second".to_string(), "yes".to_string())
        .apply(tx)
        .expect("apply");
    tx.commit(stale.as_ref())
        .await
        .expect("a guard of an older scope does not bind the next scope");
    assert_eq!(next.body_refusal(), None);
}

#[tokio::test]
async fn the_body_scope_does_not_reach_a_spawned_task() {
    let (_warehouse, catalog, ident) = fixture("body_spawn").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let guard = BatchScope::enter(TableUuid::of(&table), body_stamp(0)).expect("enter");
    let outside = Arc::clone(&catalog);
    let spawned_sees_the_scope = guard
        .scope_body(async move {
            tokio::spawn(async move { !Arc::ptr_eq(&guard_body_catalog(&outside), &outside) })
                .await
                .expect("the task joins")
        })
        .await;
    assert!(!spawned_sees_the_scope);
}

#[tokio::test]
async fn an_unknown_outcome_seen_by_the_guard_is_latched_on_the_scope() {
    let (_warehouse, inner, ident) = fixture("body_unknown").await;
    let table = append_plain(&inner, &ident, &[1]).await;
    let catalog: Arc<dyn Catalog> = Arc::new(ProbeCatalog::new(
        Arc::clone(&inner),
        ProbeMode::UnknownWithoutLanding,
    ));
    let guard = BatchScope::enter(TableUuid::of(&table), body_stamp(0)).expect("enter");
    assert!(!guard.outcome_unknown());
    let files = stage(&table, &[2]).await;
    let result = guard
        .scope_body(async {
            let guarded = guard_body_catalog(&catalog);
            commit_append_with_summary(&guarded, &table, files, &[], None).await
        })
        .await;
    assert!(result.is_err());
    assert!(guard.outcome_unknown());
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
}
