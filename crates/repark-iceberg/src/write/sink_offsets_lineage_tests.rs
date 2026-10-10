use super::*;
use crate::microbatch::stray_remedy::{
    Discard, HEAD_GONE, HEAD_UNRECORDED, INSIDE_A_SNAPSHOT, PREVIOUS_GONE, Restart, SHARED_STRETCH,
    StrayRemedy,
};

async fn stamped(catalog: &Arc<dyn Catalog>, table: &Table, epoch: u64, ids: &[i32]) -> Table {
    stamped_through(catalog, table, epoch, SinkDoor::ForeachBatch, ids).await
}

async fn first_stamp(catalog: &Arc<dyn Catalog>, seeded: &Table, ids: &[i32]) -> Table {
    let marked = commit_starting_mark(catalog, seeded, query())
        .await
        .expect("the mark");
    stamped(catalog, &marked, 0, ids).await
}

async fn stamped_through(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    epoch: u64,
    door: SinkDoor,
    ids: &[i32],
) -> Table {
    let stamp = stamp_for(epoch, door);
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

fn walked(table: &Table, query: QueryId, baseline: Option<i64>) -> Option<(i64, String)> {
    stray_on_main(table, query, baseline)
        .expect("the walk")
        .map(|stray| (stray.snapshot.get(), stray.operation))
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
async fn the_walk_above_the_newest_stamp_names_the_stray_and_stops_at_the_baseline() {
    let (_warehouse, catalog, ident) = fixture("lineage_walk").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    assert_eq!(
        walked(&seeded, query(), None),
        Some((head(&seeded), String::from("append")))
    );
    assert_eq!(walked(&seeded, query(), Some(head(&seeded))), None);
    let epoch_zero = first_stamp(&catalog, &seeded, &[2]).await;
    assert_eq!(walked(&epoch_zero, query(), None), None);
    let stray = append_plain(&catalog, &ident, &[3]).await;
    assert_eq!(
        walked(&stray, query(), None),
        Some((head(&stray), String::from("append")))
    );
    assert_eq!(walked(&stray, other_query(), Some(head(&stray))), None);
}

#[tokio::test]
async fn a_snapshot_stamped_by_another_query_is_not_a_violation() {
    let (_warehouse, catalog, ident) = fixture("lineage_other").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let ours = first_stamp(&catalog, &seeded, &[2]).await;
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
    assert_eq!(walked(&after, query(), None), None);
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
    assert_eq!(walked(&marked, query(), Some(head(&seeded))), None);
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
        walked(&kept, query(), None),
        Some((head(&stray), String::from("append")))
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

async fn stamped_by_another_query(catalog: &Arc<dyn Catalog>, table: &Table, ids: &[i32]) -> Table {
    let theirs = CommitStamp {
        record: record_for(other_query(), 0, 7),
        door: SinkDoor::Table,
    };
    let guard = BatchScope::enter(TableUuid::of(table), theirs).expect("enter");
    let files = stage(table, ids).await;
    commit_append_with_summary(catalog, table, files, &scoped(&guard), None)
        .await
        .expect("the other query commits")
}

fn stamp_of(table: &Table, epoch: u64) -> SnapshotId {
    let snapshot = table
        .metadata()
        .snapshots()
        .find(|snapshot| {
            let summary = &snapshot.summary().additional_properties;
            stamped_by(summary, query())
                && summary.get("repark.cdc.epoch") == Some(&epoch.to_string())
        })
        .expect("the stamped snapshot");
    SnapshotId::new(snapshot.snapshot_id())
}

#[tokio::test]
async fn a_stray_under_the_newest_stamp_is_found_down_to_the_previous_stamp() {
    let (_warehouse, catalog, ident) = fixture("lineage_under").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let epoch_zero = first_stamp(&catalog, &seeded, &[2]).await;
    let stray = append_plain(&catalog, &ident, &[3]).await;
    let epoch_one = stamped(&catalog, &stray, 1, &[3]).await;
    let found = stray_on_main(&epoch_one, query(), None)
        .expect("the walk")
        .expect("the stray under the stamp");
    assert_eq!(found.snapshot.get(), head(&stray));
    assert!(found.below);
    assert_eq!(
        found.newest.as_ref().map(|newest| newest.snapshot),
        Some(stamp_of(&epoch_one, 1))
    );
    match &found.floor {
        Floor::Previous(previous) => {
            assert_eq!(previous.snapshot, stamp_of(&epoch_one, 0));
            assert_eq!(previous.record.epoch.get(), 0);
        }
        other => panic!("expected the previous stamp as the floor, got {other:?}"),
    }
    assert_eq!(found.records().count(), 2);
    assert_eq!(walked(&epoch_zero, query(), None), None);
    let clean = stamped(&catalog, &epoch_one, 2, &[4]).await;
    assert_eq!(walked(&clean, query(), None), None);
}

#[tokio::test]
async fn the_first_stamp_records_the_starting_head_and_bounds_the_walk_under_it() {
    let (_warehouse, catalog, ident) = fixture("lineage_first").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let marked = commit_starting_mark(&catalog, &seeded, query())
        .await
        .expect("the mark");
    let clean = stamped(&catalog, &marked, 0, &[2]).await;
    let summary = &clean.metadata().current_snapshot().expect("head").summary();
    assert_eq!(
        StartingMark::from_summary(&summary.additional_properties),
        Some(StartingMark {
            head: Some(SnapshotId::new(head(&seeded)))
        })
    );
    assert_eq!(walked(&clean, query(), None), None);
    let later = stamped(&catalog, &clean, 1, &[5]).await;
    let summary = &later.metadata().current_snapshot().expect("head").summary();
    assert_eq!(
        StartingMark::from_summary(&summary.additional_properties),
        None
    );

    let (_second, catalog, ident) = fixture("lineage_first_stray").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    commit_starting_mark(&catalog, &seeded, query())
        .await
        .expect("the mark");
    let stray = append_plain(&catalog, &ident, &[2]).await;
    let stamped_over = stamped(&catalog, &stray, 0, &[2]).await;
    let found = stray_on_main(&stamped_over, query(), Some(head(&seeded)))
        .expect("the walk")
        .expect("the stray under the first stamp");
    assert_eq!(found.snapshot.get(), head(&stray));
    assert!(found.below);
    assert_eq!(
        found.floor,
        Floor::Head(Some(SnapshotId::new(head(&seeded))))
    );
}

#[tokio::test]
async fn an_empty_start_a_markless_first_stamp_and_a_shared_stretch_are_told_apart() {
    let (_warehouse, catalog, ident) = fixture("lineage_empty").await;
    let empty = catalog.load_table(&ident).await.expect("load");
    commit_starting_mark(&catalog, &empty, query())
        .await
        .expect("the mark");
    let stray = append_plain(&catalog, &ident, &[2]).await;
    let stamped_over = stamped(&catalog, &stray, 0, &[2]).await;
    let found = stray_on_main(&stamped_over, query(), None)
        .expect("the walk")
        .expect("the stray");
    assert_eq!(found.floor, Floor::Head(None));
    assert!(found.below);

    let (_second, catalog, ident) = fixture("lineage_markless").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let markless = stamped(&catalog, &seeded, 0, &[2]).await;
    assert_eq!(
        walked(&markless, query(), None),
        Some((head(&seeded), String::from("append"))),
        "a first stamp that records no head cannot vouch for what lies under it"
    );

    let (_third, catalog, ident) = fixture("lineage_shared").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let epoch_zero = first_stamp(&catalog, &seeded, &[2]).await;
    let theirs = stamped_by_another_query(&catalog, &epoch_zero, &[3]).await;
    assert_eq!(walked(&theirs, query(), None), None);
    let stray = append_plain(&catalog, &ident, &[4]).await;
    let above = stray_on_main(&stray, query(), None)
        .expect("the walk")
        .expect("the stray above");
    assert!(!above.below);
    assert_eq!(above.floor, Floor::Shared);
    let epoch_one = stamped(&catalog, &stray, 1, &[5]).await;
    let under = stray_on_main(&epoch_one, query(), None)
        .expect("the walk")
        .expect("the stray under");
    assert!(under.below);
    assert_eq!(under.floor, Floor::Shared);
}

fn remedy_of(stray: &Stray, whole: &[u64]) -> StrayRemedy {
    let position = |record: &SinkRecord| {
        whole
            .contains(&record.epoch.get())
            .then(|| SnapshotId::new(1000 + i64::try_from(record.epoch.get()).expect("epoch")))
    };
    match stray.reason(&position) {
        RecoveryReason::StraySinkCommit { remedy, .. } => remedy,
        other => panic!("expected a stray commit, got {other:?}"),
    }
}

fn stamped_record(epoch: u64, snapshot: i64) -> Stamped {
    Stamped {
        snapshot: SnapshotId::new(snapshot),
        record: record_for(query(), epoch, snapshot),
    }
}

#[test]
fn the_remedy_offers_only_what_the_walk_can_prove() {
    let id = SnapshotId::new;
    let stray = |newest: Option<Stamped>, below: bool, floor: Floor| Stray {
        snapshot: id(50),
        operation: String::from("append"),
        newest,
        below,
        floor,
    };
    let above = stray(Some(stamped_record(3, 30)), false, Floor::Newest);
    assert_eq!(
        remedy_of(&above, &[3]),
        StrayRemedy {
            under: None,
            discard: Discard::RollBack {
                to: id(30),
                then: Restart::SameName
            },
            keep: Restart::NewName(Some(id(1003))),
        }
    );
    assert_eq!(remedy_of(&above, &[]).keep, Restart::Unnamed);
    let unstamped = stray(None, false, Floor::Head(Some(id(9))));
    assert_eq!(
        remedy_of(&unstamped, &[]),
        StrayRemedy {
            under: None,
            discard: Discard::RollBack {
                to: id(9),
                then: Restart::SameName
            },
            keep: Restart::NewName(None),
        }
    );
    let empty = stray(None, false, Floor::Head(None));
    assert_eq!(remedy_of(&empty, &[]).discard, Discard::EmptyStart);
    let previous = Floor::Previous(stamped_record(2, 20));
    let under = stray(Some(stamped_record(3, 30)), true, previous);
    assert_eq!(
        remedy_of(&under, &[2, 3]),
        StrayRemedy {
            under: Some(id(30)),
            discard: Discard::RollBack {
                to: id(20),
                then: Restart::NewName(Some(id(1002)))
            },
            keep: Restart::NewName(Some(id(1003))),
        }
    );
    assert_eq!(
        remedy_of(&under, &[3]).discard,
        Discard::Unproven(INSIDE_A_SNAPSHOT)
    );
    let first = stray(Some(stamped_record(0, 30)), true, Floor::Head(Some(id(9))));
    assert_eq!(
        remedy_of(&first, &[0]).discard,
        Discard::RollBack {
            to: id(9),
            then: Restart::NewName(None)
        }
    );
    let first_on_empty = stray(Some(stamped_record(0, 30)), true, Floor::Head(None));
    assert_eq!(
        remedy_of(&first_on_empty, &[0]).discard,
        Discard::EmptyStart
    );
    let shared = stray(Some(stamped_record(3, 30)), true, Floor::Shared);
    assert_eq!(
        remedy_of(&shared, &[3]).discard,
        Discard::Unproven(SHARED_STRETCH)
    );
}

#[tokio::test]
async fn a_stamp_alone_refuses_the_mark_even_without_the_offsets_property() {
    let (_warehouse, catalog, ident) = fixture("mark_stamp_alone").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let committed = stamped(&catalog, &seeded, 0, &[2]).await;
    let tx = Transaction::new(&committed);
    let tx = tx
        .update_table_properties()
        .remove(offsets_key())
        .apply(tx)
        .expect("apply");
    let bare = tx
        .commit(catalog.as_ref())
        .await
        .expect("the property leaves");
    assert!(!bare.metadata().properties().contains_key(&offsets_key()));
    let after = commit_starting_mark(&catalog, &bare, query())
        .await
        .expect("the refused mark settles on a reload");
    assert!(
        !after.metadata().properties().contains_key(&offsets_key()),
        "a mark landed over a stamped query"
    );
    assert_eq!(after.metadata_location(), bare.metadata_location());
}

fn lost(table: &Table, baseline: Option<i64>) -> (bool, Floor, Discard) {
    let stray = stray_on_main(table, query(), baseline)
        .expect("the walk")
        .expect("a stray");
    let discard = remedy_of(&stray, &[0, 1, 2, 3]).discard;
    (stray.below, stray.floor, discard)
}

#[tokio::test]
async fn a_stray_under_the_newest_stamp_is_reported_when_no_lower_bound_is_left() {
    let (_warehouse, catalog, ident) = fixture("lost_previous").await;
    let stray = append_plain(&catalog, &ident, &[1]).await;
    let later = stamped(&catalog, &stray, 3, &[2]).await;
    assert_eq!(
        lost(&later, None),
        (
            true,
            Floor::Lost(PREVIOUS_GONE),
            Discard::Unproven(PREVIOUS_GONE)
        )
    );
    assert_eq!(
        walked(&later, query(), None).map(|found| found.0),
        Some(head(&stray))
    );

    let (_second, catalog, ident) = fixture("lost_unrecorded").await;
    let before = append_plain(&catalog, &ident, &[1]).await;
    let first = stamped(&catalog, &before, 0, &[2]).await;
    assert_eq!(
        lost(&first, None),
        (
            true,
            Floor::Lost(HEAD_UNRECORDED),
            Discard::Unproven(HEAD_UNRECORDED)
        )
    );

    let (_third, catalog, ident) = fixture("lost_head").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let marked = with_raw_mark(&catalog, &seeded, "424242").await;
    assert_eq!(
        lost(&marked, Some(424_242)),
        (false, Floor::Lost(HEAD_GONE), Discard::Unproven(HEAD_GONE))
    );
    let first = stamped(&catalog, &marked, 0, &[2]).await;
    assert_eq!(
        lost(&first, Some(424_242)),
        (true, Floor::Lost(HEAD_GONE), Discard::Unproven(HEAD_GONE))
    );
}

#[tokio::test]
async fn a_sink_with_nothing_unstamped_under_its_newest_stamp_is_healthy_without_a_bound() {
    let (_warehouse, catalog, ident) = fixture("healthy_unbounded").await;
    let empty = catalog.load_table(&ident).await.expect("load");
    let third = stamped(&catalog, &empty, 3, &[1]).await;
    assert_eq!(walked(&third, query(), None), None);
    let theirs = stamped_by_another_query(&catalog, &third, &[2]).await;
    let fourth = stamped(&catalog, &theirs, 4, &[3]).await;
    assert_eq!(walked(&fourth, query(), None), None);

    let (_second, catalog, ident) = fixture("healthy_table_begun").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let begun = stamped_through(&catalog, &seeded, 0, SinkDoor::Table, &[2]).await;
    assert_eq!(walked(&begun, query(), None), None);
    assert!(!carried(&begun));
}

fn carried(table: &Table) -> bool {
    carried_by_foreach(table, query()).expect("the lineage reads")
}

#[tokio::test]
async fn a_name_is_walked_on_the_table_door_once_it_ever_carried_a_foreach_stamp_or_mark() {
    let (_warehouse, catalog, ident) = fixture("table_only").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    assert!(!carried(&seeded));
    let first = stamped_through(&catalog, &seeded, 0, SinkDoor::Table, &[2]).await;
    let foreign = append_plain(&catalog, &ident, &[3]).await;
    let second = stamped_through(&catalog, &foreign, 1, SinkDoor::Table, &[4]).await;
    assert!(!carried(&first));
    assert!(!carried(&second));
    let theirs = stamped_by_another_query(&catalog, &second, &[5]).await;
    assert!(!carried(&theirs));

    let (_second, catalog, ident) = fixture("table_after_mark").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let marked = commit_starting_mark(&catalog, &seeded, query())
        .await
        .expect("the mark");
    assert!(carried(&marked));
    let zero = stamped_through(&catalog, &marked, 0, SinkDoor::Table, &[2]).await;
    assert!(
        !carried(&zero),
        "a mark the table door replaced leaves a table stamp and nothing else"
    );

    let (_third, catalog, ident) = fixture("table_after_foreach").await;
    let empty = catalog.load_table(&ident).await.expect("load");
    let zero = first_stamp(&catalog, &empty, &[1]).await;
    assert!(carried(&zero));
    let one = stamped_through(&catalog, &zero, 1, SinkDoor::Table, &[2]).await;
    let two = stamped_through(&catalog, &one, 2, SinkDoor::Table, &[3]).await;
    assert!(carried(&two), "the foreach stamp is still on the lineage");
    let foreign = append_plain(&catalog, &ident, &[4]).await;
    assert_eq!(
        walked(&foreign, query(), None),
        Some((head(&foreign), String::from("append")))
    );
    let three = stamped_through(&catalog, &foreign, 3, SinkDoor::Table, &[5]).await;
    let under = stray_on_main(&three, query(), None)
        .expect("the walk")
        .expect("the stray between two table stamps of a foreach name");
    assert!(under.below);
    assert_eq!(under.snapshot.get(), head(&foreign));
}

fn refusal_text(error: &DataFusionError) -> String {
    microbatch_cause(error).to_string()
}

#[tokio::test]
async fn a_stamped_commit_is_refused_over_a_stray_that_landed_after_the_batch_began() {
    let (_warehouse, catalog, ident) = fixture("over_a_stray").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let began = commit_starting_mark(&catalog, &seeded, query())
        .await
        .expect("the mark");
    let stamp = stamp_for(0, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter_on(&began, stamp.clone()).expect("enter");
    let stray = append_plain(&catalog, &ident, &[2]).await;
    for attempt in 0..2 {
        let files = stage(&stray, &[3]).await;
        let refused = guard
            .scope_body(commit_append_with_summary(
                &catalog,
                &stray,
                files,
                &[],
                None,
            ))
            .await
            .expect_err("a stamped commit over a stray");
        assert_eq!(
            refusal_text(&refused),
            format!(
                "epoch 0: snapshot {id} (append) landed on the sink without a stamp after this batch began, so the batch's stamped commit is refused before it lands over it",
                id = head(&stray)
            ),
            "attempt {attempt}"
        );
        assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    }
    let after = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(head(&after), head(&stray));
    drop(guard);

    let table_door = stamp_for(0, SinkDoor::Table);
    let guard = BatchScope::enter_on(&began, table_door).expect("enter");
    let files = stage(&stray, &[4]).await;
    let landed = commit_append_with_summary(&catalog, &stray, files, &scoped(&guard), None)
        .await
        .expect("the table door takes other writers");
    assert_eq!(stamped_snapshots(&landed), 1);
}

#[tokio::test]
async fn another_query_s_stamp_since_the_batch_began_does_not_refuse_the_stamped_commit() {
    let (_warehouse, catalog, ident) = fixture("over_theirs").await;
    let empty = catalog.load_table(&ident).await.expect("load");
    let began = commit_starting_mark(&catalog, &empty, query())
        .await
        .expect("the mark");
    let theirs = stamped_by_another_query(&catalog, &began, &[1]).await;
    let guard = BatchScope::enter_on(&began, stamp_for(0, SinkDoor::ForeachBatch)).expect("enter");
    let files = stage(&theirs, &[2]).await;
    let landed = guard
        .scope_body(commit_append_with_summary(
            &catalog,
            &theirs,
            files,
            &[],
            None,
        ))
        .await
        .expect("another query's stamp is not a stray");
    assert_eq!(
        guard.outcome(),
        ScopeOutcome::Committed {
            snapshot: SnapshotId::new(head(&landed))
        }
    );
}

#[tokio::test]
async fn the_fence_refuses_a_stray_that_landed_between_the_claim_and_the_commit() {
    let (_warehouse, catalog, ident) = fixture("fence_stray").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let stray = append_plain(&catalog, &ident, &[2]).await;
    for (door, refuses) in [(SinkDoor::ForeachBatch, true), (SinkDoor::Table, false)] {
        let claimed = ClaimedStamp {
            stamp: stamp_for(0, door),
            base: Some(SnapshotId::new(head(&seeded))),
            started: None,
        };
        let fenced = AppendFence::install(&catalog, &claimed);
        let current = catalog.load_table(&ident).await.expect("reload");
        let tx = Transaction::new(&current);
        let tx = tx
            .update_table_properties()
            .set(String::from("fence.probe"), String::from("x"))
            .apply(tx)
            .expect("apply");
        let outcome = tx.commit(fenced.as_ref()).await;
        match outcome {
            Err(error) if refuses => {
                let text = error.to_string();
                assert!(
                    text.contains(&format!(
                        "snapshot {id} (append) landed on the sink without a stamp after this batch began",
                        id = head(&stray)
                    )),
                    "{text}"
                );
            }
            Ok(_) if !refuses => {}
            other => panic!("{door:?}: {other:?}"),
        }
    }
}
