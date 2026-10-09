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
}

#[tokio::test]
async fn a_token_in_the_extras_decides_before_the_ambient_scope() {
    let (_warehouse, catalog, ident) = fixture("body_order").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let guard = BatchScope::enter(TableUuid::of(&table), body_stamp(0)).expect("enter");
    let foreign = ScopeToken::parse("eeeeeeee-0000-4000-8000-0000000000e5").expect("token");
    let files = stage(&table, &[2]).await;
    let committed = guard
        .scope_body(commit_append_with_summary(
            &catalog,
            &table,
            files,
            &[foreign.summary_entry()],
            None,
        ))
        .await
        .expect("the append commits as on main");
    assert_unstamped(&committed);
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
}

#[tokio::test]
async fn a_failed_stamped_attempt_releases_the_claim_and_the_retry_is_the_stamped_commit() {
    let (_warehouse, inner, ident) = fixture("body_retry").await;
    let table = append_plain(&inner, &ident, &[1]).await;
    let failing: Arc<dyn Catalog> = Arc::new(ProbeCatalog::new(
        Arc::clone(&inner),
        ProbeMode::FailBeforeLanding,
    ));
    let stamp = body_stamp(0);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let first = stage(&table, &[2]).await;
    let again = stage(&table, &[2]).await;
    let (failed, unstamped_after, retried) = guard
        .scope_body(async {
            let failed = commit_append_with_summary(&failing, &table, first, &[], None).await;
            let guarded = guard_body_catalog(&inner);
            let unstamped_after = set_property(&guarded, &table).await;
            let retried = commit_append_with_summary(&guarded, &table, again, &[], None).await;
            (failed, unstamped_after, retried)
        })
        .await;
    assert!(failed.is_err());
    assert!(matches!(
        refusal_in(&unstamped_after.expect_err("the claim was released")),
        MicroBatchError::UnstampedSinkWrite { .. }
    ));
    let committed = retried.expect("the retry is admitted");
    assert_stamped_head(&committed, &stamp);
    assert_eq!(stamped_snapshots(&committed), 1);
    assert_eq!(live_ids(&committed).await, vec![1, 2]);
    assert!(!committed.metadata().properties().contains_key("eo.touched"));
}

#[tokio::test]
async fn a_stamped_attempt_that_never_reached_its_commit_releases_the_claim() {
    let (_warehouse, catalog, ident) = fixture("body_dropped").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stamp = body_stamp(0);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let files = stage(&table, &[2]).await;
    let committed = guard
        .scope_body(async {
            drop(SiteStamp::claim(&table, None, &[]).expect("the first claim"));
            commit_append_with_summary(&catalog, &table, files, &[], None).await
        })
        .await
        .expect("the claim was free again");
    assert_stamped_head(&committed, &stamp);
}

#[tokio::test]
async fn a_second_claim_beside_an_unknown_outcome_refuses_without_the_twice_text() {
    let (_warehouse, inner, ident) = fixture("body_unknown_retry").await;
    let table = append_plain(&inner, &ident, &[1]).await;
    let catalog: Arc<dyn Catalog> = Arc::new(ProbeCatalog::new(
        Arc::clone(&inner),
        ProbeMode::UnknownWithoutLanding,
    ));
    let guard = BatchScope::enter(TableUuid::of(&table), body_stamp(0)).expect("enter");
    let first = stage(&table, &[2]).await;
    let again = stage(&table, &[2]).await;
    let (unknown, retried) = guard
        .scope_body(async {
            let guarded = guard_body_catalog(&catalog);
            let unknown = commit_append_with_summary(&guarded, &table, first, &[], None).await;
            let retried = commit_append_with_summary(&inner, &table, again, &[], None).await;
            (unknown, retried)
        })
        .await;
    assert!(unknown.is_err());
    let refusal = retried.expect_err("the retry is refused");
    let text = microbatch_cause(&refusal).to_string();
    assert!(
        text.contains("still in flight or its outcome is unknown"),
        "{text}"
    );
    assert!(!text.contains("already stamped"), "{text}");
    assert_eq!(guard.body_refusal(), None);
}

#[tokio::test]
async fn the_guard_loads_the_table_when_the_commit_brings_no_base() {
    let (_warehouse, inner, ident) = fixture("body_nobase").await;
    let table = append_plain(&inner, &ident, &[1]).await;
    let probe = Arc::new(ProbeCatalog::new(Arc::clone(&inner), ProbeMode::Capture));
    let capturing = Arc::clone(&probe) as Arc<dyn Catalog>;
    assert!(set_property(&capturing, &table).await.is_err());
    let mut commit = probe.take_captured().expect("a captured commit");
    commit.take_base_table();
    let guard = BatchScope::enter(TableUuid::of(&table), body_stamp(2)).expect("enter");
    let refused = guard
        .scope_body(async { guard_body_catalog(&inner).update_table(commit).await })
        .await;
    assert_eq!(
        refusal_in(&refused.expect_err("the sink commit")),
        MicroBatchError::UnstampedSinkWrite {
            sink: ident.to_string(),
            epoch: Epoch::new(2),
        }
    );
    let reloaded = inner.load_table(&ident).await.expect("reload");
    assert!(!reloaded.metadata().properties().contains_key("eo.touched"));
}

#[tokio::test]
async fn a_planned_write_to_the_sink_refuses_inside_the_scope_only() {
    let (_warehouse, catalog, ident) = fixture("body_planned").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    assert_eq!(refuse_planned_sink_write(&table), None);
    let guard = BatchScope::enter(TableUuid::of(&table), body_stamp(5)).expect("enter");
    assert_eq!(refuse_planned_sink_write(&table), None);
    let refused = guard
        .scope_body(async { refuse_planned_sink_write(&table) })
        .await;
    let refusal = MicroBatchError::UnstampedSinkWrite {
        sink: ident.to_string(),
        epoch: Epoch::new(5),
    };
    assert_eq!(refused, Some(refusal.clone()));
    assert_eq!(guard.body_refusal(), Some(refusal));
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
    let (refused, passed, replaced, side_replaced) = guard
        .scope_body(async {
            let guarded = guard_body_catalog(&catalog);
            assert!(!Arc::ptr_eq(&guarded, &catalog));
            (
                set_property(&guarded, &table).await,
                set_property(&guarded, &side).await,
                guarded.publish_replace_table(table.clone(), None).await,
                guarded.publish_replace_table(side.clone(), None).await,
            )
        })
        .await;
    let refusal = MicroBatchError::UnstampedSinkWrite {
        sink: ident.to_string(),
        epoch: Epoch::new(3),
    };
    assert_eq!(refusal_in(&refused.expect_err("the sink commit")), refusal);
    assert_eq!(
        refusal_in(&replaced.expect_err("the sink's replacement")),
        refusal
    );
    assert!(side_replaced.is_ok_and(|table| table.identifier() == &side_ident));
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
