use super::*;

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
