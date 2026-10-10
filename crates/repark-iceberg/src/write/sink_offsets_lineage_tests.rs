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

fn offsets_key() -> String {
    format!("{OFFSETS_PROPERTY_PREFIX}{query}", query = query())
}

async fn with_raw_mark(catalog: &Arc<dyn Catalog>, table: &Table, head: &str) -> Table {
    let value = format!("{{\"format-version\":1,\"pending-epoch\":0,\"starting-head\":{head}}}");
    let tx = Transaction::new(table);
    let tx = tx
        .update_table_properties()
        .set(offsets_key(), value)
        .apply(tx)
        .expect("apply the mark");
    tx.commit(catalog.as_ref()).await.expect("the mark commits")
}

#[tokio::test]
async fn a_pending_starting_mark_reads_as_no_durable_record_and_the_first_stamp_replaces_it() {
    let (_warehouse, catalog, ident) = fixture("mark_pending").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let marked = with_raw_mark(&catalog, &seeded, &head(&seeded).to_string()).await;
    assert_eq!(read_resume_point(&marked, query()).expect("resume"), None);
    assert_eq!(
        unstamped_since_stamp(&marked, query(), Some(head(&seeded))),
        None
    );
    let committed = stamped(&catalog, &marked, 0, &[2]).await;
    let resumed = read_resume_point(&committed, query())
        .expect("resume")
        .expect("the first stamp is durable");
    assert_eq!(resumed.epoch, Epoch::new(0));
    let value = &committed.metadata().properties()[&offsets_key()];
    assert!(!value.contains("pending-epoch"), "{value}");
}
#[tokio::test]
async fn the_starting_mark_round_trips_its_head_through_the_offsets_property() {
    let (_warehouse, catalog, ident) = fixture("mark_round_trip").await;
    let empty = catalog.load_table(&ident).await.expect("load");
    assert_eq!(read_starting_mark(&empty, query()).expect("no mark"), None);
    let marked = commit_starting_mark(&catalog, &empty, query())
        .await
        .expect("the mark commits");
    assert_eq!(
        read_starting_mark(&marked, query()).expect("the mark"),
        Some(StartingMark { head: None })
    );
    assert_eq!(marked.metadata().snapshots().count(), 0);
    assert_eq!(
        marked.metadata().properties()[&offsets_key()],
        "{\"format-version\":1,\"pending-epoch\":0,\"starting-head\":null}"
    );
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    assert_eq!(
        read_starting_mark(&seeded, other_query()).expect("another query"),
        None
    );
    let other = commit_starting_mark(&catalog, &seeded, other_query())
        .await
        .expect("the second query's mark");
    assert_eq!(
        read_starting_mark(&other, other_query()).expect("the mark"),
        Some(StartingMark {
            head: Some(SnapshotId::new(head(&seeded)))
        })
    );
    assert_eq!(
        read_starting_mark(&other, query()).expect("the first mark"),
        Some(StartingMark { head: None })
    );
    let committed = stamped(&catalog, &other, 0, &[2]).await;
    assert_eq!(
        read_starting_mark(&committed, query()).expect("replaced"),
        None
    );
}

#[tokio::test]
async fn a_corrupt_offsets_property_is_neither_a_mark_nor_a_record() {
    let (_warehouse, catalog, ident) = fixture("mark_corrupt").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(
            offsets_key(),
            "{\"format-version\":1,\"pending-epoch\":3}".to_string(),
        )
        .apply(tx)
        .expect("apply");
    let corrupt = tx.commit(catalog.as_ref()).await.expect("commit");
    assert!(read_starting_mark(&corrupt, query()).is_err());
    assert!(read_resume_point(&corrupt, query()).is_err());
}

#[tokio::test]
async fn the_mark_is_written_once_and_never_over_a_record() {
    let (_warehouse, catalog, ident) = fixture("mark_once").await;
    let empty = catalog.load_table(&ident).await.expect("load");
    commit_starting_mark(&catalog, &empty, query())
        .await
        .expect("the first mark");
    let stray = append_plain(&catalog, &ident, &[1]).await;
    let kept = commit_starting_mark(&catalog, &stray, query())
        .await
        .expect("the second writer reads the first mark");
    assert_eq!(
        read_starting_mark(&kept, query()).expect("the mark"),
        Some(StartingMark { head: None }),
        "the first head stays"
    );
    assert_eq!(
        commit_named(unstamped_since_stamp(&kept, query(), None)),
        (head(&stray), String::from("append"))
    );
    let (_other_warehouse, catalog, ident) = fixture("mark_once_stamped").await;
    let stale = catalog.load_table(&ident).await.expect("load");
    let committed = stamped(&catalog, &stale, 0, &[1]).await;
    let record = committed.metadata().properties()[&offsets_key()].clone();
    let kept = commit_starting_mark(&catalog, &stale, query())
        .await
        .expect("the late mark reads the record");
    assert_eq!(kept.metadata().properties()[&offsets_key()], record);
    assert_eq!(read_starting_mark(&kept, query()).expect("no mark"), None);
    assert!(read_resume_point(&kept, query()).expect("resume").is_some());
}
