use super::*;

async fn stamped(catalog: &Arc<dyn Catalog>, table: &Table, epoch: u64, ids: &[i32]) -> Table {
    let stamp = stamp_for(epoch, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(table), stamp).expect("enter");
    let files = stage(table, ids).await;
    guard
        .scope_body(commit_append_with_summary(catalog, table, files, &[], None))
        .await
        .expect("the stamped append")
}

async fn with_property(catalog: &Arc<dyn Catalog>, table: &Table, key: &str) -> Table {
    let tx = Transaction::new(table);
    let tx = tx
        .update_table_properties()
        .set(key.to_string(), "yes".to_string())
        .apply(tx)
        .expect("apply");
    tx.commit(catalog.as_ref())
        .await
        .expect("the property commit")
}

fn commit_named(reason: Option<RecoveryReason>) -> (i64, String) {
    match reason {
        Some(RecoveryReason::UnstampedSinkCommit {
            snapshot,
            operation: Some(operation),
        }) => (snapshot.get(), operation),
        other => panic!("expected an unstamped commit, got {other:?}"),
    }
}

fn change_named(reason: Option<RecoveryReason>) -> String {
    match reason {
        Some(RecoveryReason::UnstampedSinkChange { what }) => what,
        other => panic!("expected an unstamped change, got {other:?}"),
    }
}

fn head(table: &Table) -> i64 {
    table.metadata().current_snapshot_id().expect("a head")
}

#[tokio::test]
async fn the_walk_stops_at_this_query_s_newest_stamp_and_ignores_what_lies_below() {
    let (_warehouse, catalog, ident) = fixture("lineage_walk").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    assert_eq!(
        commit_named(unstamped_since_stamp(&seeded, query(), None)),
        (head(&seeded), String::from("append"))
    );
    assert_eq!(
        unstamped_since_stamp(&seeded, query(), Some(head(&seeded))),
        None
    );
    let epoch_zero = stamped(&catalog, &seeded, 0, &[2]).await;
    assert_eq!(unstamped_since_stamp(&epoch_zero, query(), None), None);
    let stray = append_plain(&catalog, &ident, &[3]).await;
    assert_eq!(
        commit_named(unstamped_since_stamp(&stray, query(), None)),
        (head(&stray), String::from("append"))
    );
    assert_eq!(
        unstamped_since_stamp(&stray, other_query(), Some(head(&stray))),
        None
    );
}

#[tokio::test]
async fn a_snapshot_stamped_by_another_query_is_not_a_violation() {
    let (_warehouse, catalog, ident) = fixture("lineage_other").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let ours = stamped(&catalog, &seeded, 0, &[2]).await;
    let mark = SinkMark::of(&ours);
    let theirs = CommitStamp {
        record: record_for(other_query(), 0, 7),
        door: SinkDoor::Table,
    };
    let guard = BatchScope::enter(TableUuid::of(&ours), theirs).expect("enter");
    let files = stage(&ours, &[3]).await;
    let after = commit_append_with_summary(&catalog, &ours, files, &scoped(&guard), None)
        .await
        .expect("the other query commits");
    assert_eq!(mark.violation(&after), None);
    assert_eq!(unstamped_since_stamp(&after, query(), None), None);
}

#[tokio::test]
async fn the_mark_admits_nothing_and_the_one_stamped_commit() {
    let (_warehouse, catalog, ident) = fixture("lineage_mark").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let mark = SinkMark::of(&seeded);
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(mark.violation(&reloaded), None);
    let committed = stamped(&catalog, &seeded, 0, &[2]).await;
    assert_eq!(mark.violation(&committed), None);
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(mark.violation(&reloaded), None);
}

#[tokio::test]
async fn the_mark_names_an_unstamped_snapshot_before_and_after_the_stamped_one() {
    let (_warehouse, catalog, ident) = fixture("lineage_stray").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let mark = SinkMark::of(&seeded);
    let before = append_plain(&catalog, &ident, &[2]).await;
    let committed = stamped(&catalog, &before, 0, &[3]).await;
    assert_eq!(
        commit_named(mark.violation(&committed)),
        (head(&before), String::from("append"))
    );
    let mark = SinkMark::of(&committed);
    let next = stamped(&catalog, &committed, 1, &[4]).await;
    let after = append_plain(&catalog, &ident, &[5]).await;
    assert_eq!(mark.violation(&next), None);
    assert_eq!(
        commit_named(mark.violation(&after)),
        (head(&after), String::from("append"))
    );
}

#[tokio::test]
async fn the_mark_names_a_property_change_with_and_without_the_stamped_commit() {
    let (_warehouse, catalog, ident) = fixture("lineage_property").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let mark = SinkMark::of(&seeded);
    let touched = with_property(&catalog, &seeded, "eo.touched").await;
    assert_eq!(
        change_named(mark.violation(&touched)),
        "table property eo.touched changed"
    );
    let committed = stamped(&catalog, &touched, 0, &[2]).await;
    assert_eq!(
        change_named(mark.violation(&committed)),
        "table property eo.touched changed"
    );
}

#[tokio::test]
async fn the_mark_names_a_branch_a_rollback_and_a_replaced_table() {
    let (_warehouse, catalog, ident) = fixture("lineage_refs").await;
    let first = append_plain(&catalog, &ident, &[1]).await;
    let second = append_plain(&catalog, &ident, &[2]).await;
    let mark = SinkMark::of(&second);
    let tx = Transaction::new(&second);
    let tx = tx
        .manage_snapshots()
        .create_branch("audit", head(&second))
        .apply(tx)
        .expect("apply the branch");
    let branched = tx
        .commit(catalog.as_ref())
        .await
        .expect("the branch commit");
    assert_eq!(
        change_named(mark.violation(&branched)),
        "branch or tag audit changed"
    );
    let mark = SinkMark::of(&branched);
    let tx = Transaction::new(&branched);
    let tx = tx
        .manage_snapshots()
        .rollback_to(head(&first))
        .apply(tx)
        .expect("apply the rollback");
    let rolled = tx.commit(catalog.as_ref()).await.expect("the rollback");
    assert_eq!(
        change_named(mark.violation(&rolled)),
        format!("main was moved to the existing snapshot {}", head(&first))
    );
    let (_other_warehouse, other_catalog, other_ident) = fixture("lineage_refs").await;
    let replaced = append_plain(&other_catalog, &other_ident, &[1]).await;
    assert!(
        change_named(mark.violation(&replaced))
            .starts_with("the table under the sink's name was replaced (uuid ")
    );
}
