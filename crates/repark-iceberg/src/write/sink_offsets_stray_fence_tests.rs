use super::*;

async fn raced(arm: Arm, door: SinkDoor) -> (Result<(), String>, Table, i64) {
    let name = format!("{}_stray_{door:?}", arm_name(arm)).to_lowercase();
    let (_warehouse, memory, ident) = fixture(&name).await;
    let (table, seeded) = seed(&memory, &ident).await;
    let racer = stage(&table, &[99]).await;
    let racing = Arc::new(RacingCatalog::new(Arc::clone(&memory), racer));
    let catalog: Arc<dyn Catalog> = Arc::clone(&racing) as Arc<dyn Catalog>;
    let stamp = stamp_for(1, door);
    let guard = BatchScope::enter_on(&table, stamp.clone()).expect("enter");
    let outcome = run_arm(arm, &catalog, &table, &seeded, &guard, &stamp, false).await;
    drop(guard);
    let after = memory.load_table(&ident).await.expect("reload");
    let stray = after
        .metadata()
        .snapshots()
        .find(|snapshot| {
            snapshot.parent_snapshot_id() == table.metadata().current_snapshot_id()
                && !snapshot
                    .summary()
                    .additional_properties
                    .contains_key(QUERY_ID_KEY)
        })
        .expect("the racer's snapshot landed on the seed")
        .snapshot_id();
    (outcome, after, stray)
}

#[tokio::test]
async fn no_arm_commits_a_foreach_stamp_over_a_stray_that_landed_at_its_commit() {
    for arm in ARMS {
        let (outcome, after, stray) = raced(arm, SinkDoor::ForeachBatch).await;
        let refused = outcome.expect_err(arm_name(arm));
        assert!(
            refused.contains(&format!(
                "snapshot {stray} (append) landed on the sink without a stamp after this batch began"
            )),
            "{arm:?}: {refused}"
        );
        assert_eq!(stamped_snapshots(&after), 0, "{arm:?}");
        assert_eq!(
            after.metadata().current_snapshot_id(),
            Some(stray),
            "{arm:?}: the stray is the head"
        );
        assert_no_half(&after);
    }
}

#[tokio::test]
async fn every_arm_of_the_table_door_still_lands_over_a_foreign_commit() {
    for arm in ARMS {
        let (outcome, after, stray) = raced(arm, SinkDoor::Table).await;
        outcome.unwrap_or_else(|error| panic!("{arm:?}: {error}"));
        assert_eq!(stamped_snapshots(&after), 1, "{arm:?}");
        let head = after.metadata().current_snapshot().expect("head");
        assert_eq!(head.parent_snapshot_id(), Some(stray), "{arm:?}");
    }
}
