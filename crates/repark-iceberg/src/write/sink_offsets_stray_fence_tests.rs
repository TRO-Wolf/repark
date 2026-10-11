use super::*;
use crate::microbatch::starting_mark::STARTING_HEAD_SUMMARY_KEY;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Foreign {
    Stray,
    OtherStamp,
    Property,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Window {
    BeforeTheClaim,
    ClaimToCommit,
    DuringARetry,
}

const WINDOWS: [Window; 3] = [
    Window::BeforeTheClaim,
    Window::ClaimToCommit,
    Window::DuringARetry,
];

fn probed(memory: &Arc<dyn Catalog>) -> (Arc<ProbeCatalog>, Arc<dyn Catalog>) {
    let probe = Arc::new(ProbeCatalog::new(Arc::clone(memory), ProbeMode::Forward));
    let catalog: Arc<dyn Catalog> = Arc::clone(&probe) as Arc<dyn Catalog>;
    (probe, catalog)
}

struct Raced {
    outcome: Result<(), String>,
    after: Table,
    began: Option<i64>,
    updates: usize,
}

fn their_stamp() -> CommitStamp {
    CommitStamp {
        record: record_for(other_query(), 0, 7),
        door: SinkDoor::Table,
    }
}

async fn land(memory: &Arc<dyn Catalog>, ident: &TableIdent, foreign: Foreign, id: i32) {
    let (probe, catalog) = probed(memory);
    let current = memory.load_table(ident).await.expect("load");
    let files = stage(&current, &[id]).await;
    match foreign {
        Foreign::Stray => *probe.racers.lock().expect("racers") = vec![files],
        Foreign::OtherStamp => {
            *probe.stamped_racer.lock().expect("stamped") = Some((their_stamp(), files));
        }
        Foreign::Property => {
            *probe.property_racer.lock().expect("property") = Some(format!("foreign.p{id}"));
        }
    }
    let tx = Transaction::new(&current);
    let tx = tx
        .update_table_properties()
        .set(format!("carrier.p{id}"), String::from("x"))
        .apply(tx)
        .expect("apply");
    tx.commit(catalog.as_ref())
        .await
        .expect("the carrier lands");
}

async fn raced(arm: Arm, door: SinkDoor, foreign: Foreign, window: Window) -> Raced {
    let name = format!("{}_{door:?}_{foreign:?}_{window:?}", arm_name(arm)).to_lowercase();
    let (_warehouse, memory, ident) = fixture(&name).await;
    let (table, seeded) = seed(&memory, &ident).await;
    let began = table.metadata().current_snapshot_id();
    let (probe, catalog) = probed(&memory);
    let stamp = stamp_for(1, door);
    let guard = BatchScope::enter_on(&table, stamp.clone()).expect("enter");
    let mut loaded = table.clone();
    match window {
        Window::BeforeTheClaim => {
            land(&memory, &ident, foreign, 90).await;
            loaded = memory.load_table(&ident).await.expect("reload");
        }
        Window::ClaimToCommit | Window::DuringARetry => {
            let files = stage(&table, &[91]).await;
            let mut racers = Vec::new();
            match foreign {
                Foreign::Stray => racers.push(files),
                Foreign::OtherStamp => {
                    *probe.stamped_racer.lock().expect("stamped") = Some((their_stamp(), files));
                }
                Foreign::Property => {
                    *probe.property_racer.lock().expect("property") =
                        Some(String::from("foreign.raced"));
                }
            }
            if window == Window::DuringARetry {
                racers.push(Vec::new());
                let opener = stage(&table, &[92]).await;
                *probe.stamped_racer.lock().expect("stamped") = Some((their_stamp(), opener));
            }
            *probe.racers.lock().expect("racers") = racers;
        }
    }
    let outcome = run_arm(arm, &catalog, &loaded, &seeded, &guard, &stamp, false).await;
    drop(guard);
    Raced {
        outcome,
        after: memory.load_table(&ident).await.expect("reload"),
        began,
        updates: probe.seen().len(),
    }
}

fn unstamped_on_main(table: &Table, began: Option<i64>) -> Vec<i64> {
    main_lineage(table.metadata())
        .take_while(|snapshot| Some(snapshot.snapshot_id()) != began)
        .filter(|snapshot| lineage::unstamped(snapshot))
        .map(|snapshot| snapshot.snapshot_id())
        .collect()
}

fn ours_on_main(table: &Table) -> Vec<(i64, Option<i64>)> {
    main_lineage(table.metadata())
        .filter(|snapshot| stamped_by(&snapshot.summary().additional_properties, query()))
        .map(|snapshot| (snapshot.snapshot_id(), snapshot.parent_snapshot_id()))
        .collect()
}

#[tokio::test]
async fn no_arm_commits_a_foreach_stamp_over_a_stray_in_any_window() {
    for arm in ARMS {
        for window in WINDOWS {
            let raced = raced(arm, SinkDoor::ForeachBatch, Foreign::Stray, window).await;
            let cell = format!("{arm:?} {window:?}");
            let strays = unstamped_on_main(&raced.after, raced.began);
            assert_eq!(strays.len(), 1, "{cell}: the stray landed");
            let refused = raced.outcome.expect_err(&cell);
            assert!(
                refused.contains(&format!(
                    "snapshot {stray} (append) landed on the sink without a stamp after this batch began",
                    stray = strays[0]
                )),
                "{cell}: {refused}"
            );
            assert!(ours_on_main(&raced.after).is_empty(), "{cell}");
            assert_eq!(
                raced.after.metadata().current_snapshot_id(),
                Some(strays[0]),
                "{cell}: the stray is the head"
            );
            let attempts = match window {
                Window::BeforeTheClaim => 1,
                Window::ClaimToCommit => 2,
                Window::DuringARetry => 3,
            };
            assert!(
                raced.updates <= attempts,
                "{cell}: {} commits reached the catalog",
                raced.updates
            );
        }
    }
}

#[tokio::test]
async fn every_arm_lands_its_foreach_stamp_over_what_is_not_a_stray() {
    for arm in ARMS {
        for foreign in [Foreign::OtherStamp, Foreign::Property] {
            for window in WINDOWS {
                let raced = raced(arm, SinkDoor::ForeachBatch, foreign, window).await;
                let cell = format!("{arm:?} {foreign:?} {window:?}");
                raced
                    .outcome
                    .unwrap_or_else(|error| panic!("{cell}: {error}"));
                assert!(
                    unstamped_on_main(&raced.after, raced.began).is_empty(),
                    "{cell}"
                );
                let ours = ours_on_main(&raced.after);
                assert_eq!(ours.len(), 1, "{cell}");
                assert_eq!(
                    Some(ours[0].0),
                    raced.after.metadata().current_snapshot_id(),
                    "{cell}: our stamp is the head"
                );
                let theirs = foreign == Foreign::OtherStamp || window == Window::DuringARetry;
                assert_eq!(ours[0].1 != raced.began, theirs, "{cell}: the parent");
            }
        }
    }
}

#[tokio::test]
async fn every_arm_of_the_table_door_still_lands_over_a_foreign_commit() {
    for arm in ARMS {
        for window in WINDOWS {
            let raced = raced(arm, SinkDoor::Table, Foreign::Stray, window).await;
            let cell = format!("{arm:?} {window:?}");
            raced
                .outcome
                .unwrap_or_else(|error| panic!("{cell}: {error}"));
            let strays = unstamped_on_main(&raced.after, raced.began);
            assert_eq!(strays.len(), 1, "{cell}");
            let ours = ours_on_main(&raced.after);
            assert_eq!(ours.len(), 1, "{cell}");
            assert_eq!(
                Some(ours[0].0),
                raced.after.metadata().current_snapshot_id(),
                "{cell}"
            );
        }
    }
}

fn ours_since(table: &Table, began: Option<i64>) -> Vec<(i64, Option<i64>)> {
    main_lineage(table.metadata())
        .take_while(|snapshot| Some(snapshot.snapshot_id()) != began)
        .filter(|snapshot| stamped_by(&snapshot.summary().additional_properties, query()))
        .map(|snapshot| (snapshot.snapshot_id(), snapshot.parent_snapshot_id()))
        .collect()
}

async fn raced_on_carried(arm: Arm, window: Window, marked: bool) -> Raced {
    let carried = if marked { "marked" } else { "carried" };
    let name = format!("{}_{carried}_{window:?}", arm_name(arm)).to_lowercase();
    let (_warehouse, memory, ident) = fixture(&name).await;
    let (table, seeded) = seed(&memory, &ident).await;
    let epoch = if marked {
        commit_starting_mark(&memory, &table, query())
            .await
            .expect("mark");
        0
    } else {
        stamped_append(
            &memory,
            &ident,
            &stamp_for(0, SinkDoor::ForeachBatch),
            &[80],
        )
        .await;
        1
    };
    let table = memory.load_table(&ident).await.expect("reload");
    let began = table.metadata().current_snapshot_id();
    let (probe, catalog) = probed(&memory);
    let stamp = stamp_for(epoch, SinkDoor::Table);
    let guard = BatchScope::enter_on(&table, stamp.clone()).expect("enter");
    let mut loaded = table.clone();
    match window {
        Window::BeforeTheClaim => {
            land(&memory, &ident, Foreign::Stray, 90).await;
            loaded = memory.load_table(&ident).await.expect("reload");
        }
        Window::ClaimToCommit | Window::DuringARetry => {
            let files = stage(&table, &[91]).await;
            let mut racers = Vec::new();
            racers.push(files);
            if window == Window::DuringARetry {
                racers.push(Vec::new());
                let opener = stage(&table, &[92]).await;
                *probe.stamped_racer.lock().expect("stamped") = Some((their_stamp(), opener));
            }
            *probe.racers.lock().expect("racers") = racers;
        }
    }
    let outcome = run_arm(arm, &catalog, &loaded, &seeded, &guard, &stamp, false).await;
    drop(guard);
    Raced {
        outcome,
        after: memory.load_table(&ident).await.expect("reload"),
        began,
        updates: probe.seen().len(),
    }
}

#[tokio::test]
async fn a_table_door_stamp_under_a_foreach_carried_name_is_refused_over_a_stray() {
    for arm in ARMS {
        for window in WINDOWS {
            let raced = raced_on_carried(arm, window, false).await;
            let cell = format!("{arm:?} {window:?}");
            let strays = unstamped_on_main(&raced.after, raced.began);
            assert_eq!(strays.len(), 1, "{cell}: the stray landed");
            let refused = raced.outcome.expect_err(&cell);
            assert!(
                refused.contains(&format!(
                    "snapshot {stray} (append) landed on the sink without a stamp after this batch began",
                    stray = strays[0]
                )),
                "{cell}: {refused}"
            );
            assert!(ours_since(&raced.after, raced.began).is_empty(), "{cell}");
            assert_eq!(
                raced.after.metadata().current_snapshot_id(),
                Some(strays[0]),
                "{cell}: the stray is the head"
            );
        }
    }
}

async fn raced_after_a_first_table_stamp(arm: Arm) -> Raced {
    let name = format!("{}_marked_table_first", arm_name(arm)).to_lowercase();
    let (_warehouse, memory, ident) = fixture(&name).await;
    let (table, seeded) = seed(&memory, &ident).await;
    commit_starting_mark(&memory, &table, query())
        .await
        .expect("mark");
    let first = stamped_append(&memory, &ident, &stamp_for(0, SinkDoor::Table), &[80]).await;
    let summary = &first
        .metadata()
        .current_snapshot()
        .expect("head")
        .summary()
        .additional_properties;
    assert!(summary.contains_key(STARTING_HEAD_SUMMARY_KEY));
    let table = memory.load_table(&ident).await.expect("reload");
    let began = table.metadata().current_snapshot_id();
    let stamp = stamp_for(1, SinkDoor::Table);
    let guard = BatchScope::enter_on(&table, stamp.clone()).expect("enter");
    land(&memory, &ident, Foreign::Stray, 90).await;
    let loaded = memory.load_table(&ident).await.expect("reload");
    let outcome = run_arm(arm, &memory, &loaded, &seeded, &guard, &stamp, false).await;
    drop(guard);
    Raced {
        outcome,
        after: memory.load_table(&ident).await.expect("reload"),
        began,
        updates: 0,
    }
}

#[tokio::test]
async fn a_second_table_stamp_of_a_marked_name_is_refused_over_a_stray() {
    for arm in ARMS {
        let raced = raced_after_a_first_table_stamp(arm).await;
        let cell = format!("{arm:?}");
        let strays = unstamped_on_main(&raced.after, raced.began);
        assert_eq!(strays.len(), 1, "{cell}: the stray landed");
        let refused = raced.outcome.expect_err(&cell);
        assert!(
            refused.contains(&format!(
                "snapshot {stray} (append) landed on the sink without a stamp after this batch began",
                stray = strays[0]
            )),
            "{cell}: {refused}"
        );
        assert!(ours_since(&raced.after, raced.began).is_empty(), "{cell}");
        assert_eq!(
            raced.after.metadata().current_snapshot_id(),
            Some(strays[0]),
            "{cell}: the stray is the head"
        );
    }
}

#[tokio::test]
async fn a_table_door_stamp_under_a_marked_name_is_refused_over_a_stray() {
    for arm in ARMS {
        for window in WINDOWS {
            let raced = raced_on_carried(arm, window, true).await;
            let cell = format!("{arm:?} {window:?}");
            let strays = unstamped_on_main(&raced.after, raced.began);
            assert_eq!(strays.len(), 1, "{cell}: the stray landed");
            let refused = raced.outcome.expect_err(&cell);
            assert!(
                refused.contains(&format!(
                    "snapshot {stray} (append) landed on the sink without a stamp after this batch began",
                    stray = strays[0]
                )),
                "{cell}: {refused}"
            );
            assert!(ours_since(&raced.after, raced.began).is_empty(), "{cell}");
            assert_eq!(
                raced.after.metadata().current_snapshot_id(),
                Some(strays[0]),
                "{cell}: the stray is the head"
            );
        }
    }
}

async fn with_corrupt_mark(catalog: &Arc<dyn Catalog>, table: &Table) -> Table {
    let key = format!("{OFFSETS_PROPERTY_PREFIX}{query}", query = query());
    let value = String::from("{\"format-version\":1,\"pending-epoch\":1,\"starting-head\":null}");
    let tx = Transaction::new(table);
    let tx = tx
        .update_table_properties()
        .set(key, value)
        .apply(tx)
        .expect("apply the corrupt mark");
    tx.commit(catalog.as_ref())
        .await
        .expect("the corrupt mark commits")
}

async fn refused_at_the_fence(catalog: &Arc<dyn Catalog>, table: &Table, epoch: u64) -> String {
    let claimed = ClaimedStamp {
        stamp: stamp_for(epoch, SinkDoor::Table),
        base: table.metadata().current_snapshot_id().map(SnapshotId::new),
        started: None,
    };
    let fenced = AppendFence::install(catalog, &claimed);
    let files = stage(table, &[90]).await;
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)
        .expect("apply the append");
    tx.commit(fenced.as_ref())
        .await
        .expect_err("the fence refuses")
        .to_string()
}

#[tokio::test]
async fn a_corrupt_starting_mark_refuses_the_stamped_commit_at_the_fence_and_the_guard() {
    let (_warehouse, memory, ident) = fixture("corrupt_mark_fence").await;
    let (table, _seeded) = seed(&memory, &ident).await;
    let corrupt = with_corrupt_mark(&memory, &table).await;
    let before = corrupt.metadata().current_snapshot_id();
    let snapshots = corrupt.metadata().snapshots().count();
    let refused = refused_at_the_fence(&memory, &corrupt, 0).await;
    assert!(refused.contains("only epoch 0 can be pending"), "{refused}");
    let after = memory.load_table(&ident).await.expect("reload");
    assert_eq!(after.metadata().current_snapshot_id(), before);
    assert_eq!(after.metadata().snapshots().count(), snapshots);

    let (_warehouse, memory, ident) = fixture("corrupt_mark_guard").await;
    let (table, _seeded) = seed(&memory, &ident).await;
    let stamp = stamp_for(0, SinkDoor::Table);
    let guard = BatchScope::enter_on(&table, stamp.clone()).expect("enter");
    BatchScope::claim(&table, guard.token()).expect("claim");
    let corrupt = with_corrupt_mark(&memory, &table).await;
    let before = corrupt.metadata().current_snapshot_id();
    let refused = guard
        .scope_body(async {
            let guarded = guard_body_catalog(&memory);
            let tx = Transaction::new(&corrupt);
            let tx = tx
                .update_table_properties()
                .set(String::from("eo.touched"), String::from("yes"))
                .apply(tx)
                .expect("apply");
            tx.commit(guarded.as_ref()).await
        })
        .await
        .expect_err("the guard refuses")
        .to_string();
    assert!(refused.contains("only epoch 0 can be pending"), "{refused}");
    drop(guard);
    let after = memory.load_table(&ident).await.expect("reload");
    assert_eq!(after.metadata().current_snapshot_id(), before);
    assert!(!after.metadata().properties().contains_key("eo.touched"));

    let (_warehouse, memory, ident) = fixture("corrupt_mark_precedence").await;
    let (table, _seeded) = seed(&memory, &ident).await;
    let files = stage(&table, &[70]).await;
    let summary = HashMap::from([
        (String::from("repark.cdc.format-version"), String::from("1")),
        (
            String::from("repark.cdc.query-id"),
            query().get().to_string(),
        ),
        (
            String::from("repark.cdc.run-id"),
            String::from("bbbbbbbb-0000-4000-8000-0000000000b2"),
        ),
        (String::from("repark.cdc.epoch"), String::from("soon")),
        (String::from("repark.cdc.generation"), String::from("1")),
        (String::from("repark.cdc.offsets"), String::from("[]")),
    ]);
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(files)
        .set_snapshot_properties(summary)
        .apply(tx)
        .expect("apply the corrupt stamp");
    let stamped = tx
        .commit(memory.as_ref())
        .await
        .expect("the corrupt stamp lands");
    let corrupt = with_corrupt_mark(&memory, &stamped).await;
    let before = corrupt.metadata().current_snapshot_id();
    let refused = refused_at_the_fence(&memory, &corrupt, 0).await;
    assert!(refused.contains("only epoch 0 can be pending"), "{refused}");
    assert!(!refused.contains("soon"), "{refused}");
    let after = memory.load_table(&ident).await.expect("reload");
    assert_eq!(after.metadata().current_snapshot_id(), before);
}

async fn land_row_overwrite(
    memory: &Arc<dyn Catalog>,
    ident: &TableIdent,
    extra: &[DataFile],
    id: i32,
) {
    let current = memory.load_table(ident).await.expect("load");
    let files = stage(&current, &[id]).await;
    crate::write::merge::commit_on_ref(
        memory,
        &current,
        None,
        extra.to_vec(),
        files,
        &below_the_racers(),
        None,
        &[],
    )
    .await
    .expect("the overwrite lands");
}

async fn land_row_delete(memory: &Arc<dyn Catalog>, ident: &TableIdent, extra: &[DataFile]) {
    let current = memory.load_table(ident).await.expect("load");
    crate::write::merge::commit_on_ref(
        memory,
        &current,
        None,
        extra.to_vec(),
        Vec::new(),
        &below_the_racers(),
        None,
        &[],
    )
    .await
    .expect("the delete lands");
}

async fn stray_setup(
    name: &str,
) -> (
    TempDir,
    Arc<dyn Catalog>,
    TableIdent,
    Vec<DataFile>,
    Vec<DataFile>,
) {
    let (warehouse, memory, ident) = fixture(name).await;
    let (table, seeded) = seed(&memory, &ident).await;
    let extra_table = memory.load_table(&ident).await.expect("load");
    let extra = stage(&extra_table, &[50]).await;
    commit_append_with_summary(&memory, &extra_table, extra.clone(), &[], None)
        .await
        .expect("extra file");
    drop(table);
    (warehouse, memory, ident, seeded, extra)
}

#[tokio::test]
async fn a_stray_overwrite_refuses_every_arm() {
    for arm in ARMS {
        let name = format!("{}_overwrite_stray", arm_name(arm));
        let (_warehouse, memory, ident, seeded, extra) = stray_setup(&name).await;
        assert_eq!(extra.len(), 1, "{arm:?}: one extra file");
        let table = memory.load_table(&ident).await.expect("reload");
        let began = table.metadata().current_snapshot_id();
        let stamp = stamp_for(1, SinkDoor::ForeachBatch);
        let guard = BatchScope::enter_on(&table, stamp.clone()).expect("enter");
        land_row_overwrite(&memory, &ident, &extra, 90).await;
        let loaded = memory.load_table(&ident).await.expect("reload");
        let refused = run_arm(arm, &memory, &loaded, &seeded, &guard, &stamp, false)
            .await
            .expect_err(arm_name(arm));
        assert!(
            refused.contains("landed on the sink without a stamp after this batch began")
                && refused.contains("(overwrite)"),
            "{arm:?}: {refused}"
        );
        drop(guard);
        let after = memory.load_table(&ident).await.expect("reload");
        let strays = unstamped_on_main(&after, began);
        assert_eq!(strays.len(), 1, "{arm:?}: the stray landed");
        assert!(ours_on_main(&after).is_empty(), "{arm:?}");
        assert_eq!(
            after.metadata().current_snapshot_id(),
            Some(strays[0]),
            "{arm:?}: the stray is the head"
        );
    }
}

#[tokio::test]
async fn a_stray_delete_refuses_every_arm() {
    for arm in ARMS {
        let name = format!("{}_delete_stray", arm_name(arm));
        let (_warehouse, memory, ident, seeded, extra) = stray_setup(&name).await;
        assert_eq!(extra.len(), 1, "{arm:?}: one extra file");
        let table = memory.load_table(&ident).await.expect("reload");
        let began = table.metadata().current_snapshot_id();
        let stamp = stamp_for(1, SinkDoor::ForeachBatch);
        let guard = BatchScope::enter_on(&table, stamp.clone()).expect("enter");
        land_row_delete(&memory, &ident, &extra).await;
        let loaded = memory.load_table(&ident).await.expect("reload");
        let refused = run_arm(arm, &memory, &loaded, &seeded, &guard, &stamp, false)
            .await
            .expect_err(arm_name(arm));
        assert!(
            refused.contains("landed on the sink without a stamp after this batch began")
                && refused.contains("(delete)"),
            "{arm:?}: {refused}"
        );
        drop(guard);
        let after = memory.load_table(&ident).await.expect("reload");
        let strays = unstamped_on_main(&after, began);
        assert_eq!(strays.len(), 1, "{arm:?}: the stray landed");
        assert!(ours_on_main(&after).is_empty(), "{arm:?}");
        assert_eq!(
            after.metadata().current_snapshot_id(),
            Some(strays[0]),
            "{arm:?}: the stray is the head"
        );
    }
}

#[tokio::test]
async fn a_stray_buried_under_another_query_s_stamp_still_refuses_every_arm() {
    for arm in ARMS {
        let name = format!("{}_buried", arm_name(arm));
        let (_warehouse, memory, ident) = fixture(&name).await;
        let (table, seeded) = seed(&memory, &ident).await;
        let stamp = stamp_for(1, SinkDoor::ForeachBatch);
        let guard = BatchScope::enter_on(&table, stamp.clone()).expect("enter");
        land(&memory, &ident, Foreign::Stray, 90).await;
        land(&memory, &ident, Foreign::OtherStamp, 93).await;
        let loaded = memory.load_table(&ident).await.expect("reload");
        let refused = run_arm(arm, &memory, &loaded, &seeded, &guard, &stamp, false)
            .await
            .expect_err(arm_name(arm));
        assert!(
            refused.contains("landed on the sink without a stamp after this batch began"),
            "{arm:?}: {refused}"
        );
        drop(guard);
        let after = memory.load_table(&ident).await.expect("reload");
        assert!(ours_on_main(&after).is_empty(), "{arm:?}");
        assert_eq!(
            unstamped_on_main(&after, table.metadata().current_snapshot_id()).len(),
            1,
            "{arm:?}"
        );
    }
}
