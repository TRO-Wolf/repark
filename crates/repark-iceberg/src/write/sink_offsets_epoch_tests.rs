use super::*;

fn run_b() -> RunId {
    RunId::new(Uuid::parse_str("eeeeeeee-0000-4000-8000-0000000000e5").expect("uuid"))
}

fn stamp_of(epoch: u64, run: RunId, generation: u64) -> CommitStamp {
    let mut stamp = stamp_for(epoch, SinkDoor::Table);
    stamp.record.run = run;
    stamp.record.generation = Generation::new(generation).expect("generation");
    stamp
}

fn claim_once(table: &Table, stamp: &CommitStamp) -> Result<bool, MicroBatchError> {
    let guard = BatchScope::enter(TableUuid::of(table), stamp.clone()).expect("enter");
    let claimed = BatchScope::claim(table, guard.token()).map(|claimed| claimed.is_some());
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    claimed
}

async fn append_refused(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    stamp: &CommitStamp,
) -> MicroBatchError {
    let table = catalog.load_table(ident).await.expect("load");
    let snapshots = table.metadata().snapshots().count();
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let files = stage(&table, &[90]).await;
    let error = commit_append_with_summary(catalog, &table, files, &scoped(&guard), None)
        .await
        .expect_err("a refused epoch must not commit");
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);
    let reloaded = catalog.load_table(ident).await.expect("reload");
    assert_eq!(reloaded.metadata().snapshots().count(), snapshots);
    assert!(!live_ids(&reloaded).await.contains(&90));
    microbatch_cause(&error).clone()
}

#[tokio::test]
async fn a_claim_of_an_epoch_this_run_committed_is_already_committed_and_adds_nothing() {
    let (_warehouse, catalog, ident) = fixture("epoch_already").await;
    let first = stamp_for(0, SinkDoor::Table);
    stamped_append(&catalog, &ident, &first, &[1]).await;
    let committed = MicroBatchError::AlreadyCommitted {
        query: query(),
        epoch: Epoch::new(0),
    };
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(claim_once(&table, &first), Err(committed.clone()));
    assert_eq!(append_refused(&catalog, &ident, &first).await, committed);

    stamped_append(&catalog, &ident, &stamp_for(1, SinkDoor::Table), &[2]).await;
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(claim_once(&table, &first), Err(committed));
    assert_eq!(claim_once(&table, &stamp_for(2, SinkDoor::Table)), Ok(true));
    assert_eq!(live_ids(&table).await, vec![1, 2]);
}

#[tokio::test]
async fn a_claim_of_an_epoch_another_run_committed_is_fenced_by_the_winner() {
    let (_warehouse, catalog, ident) = fixture("epoch_fenced").await;
    let winner = stamp_for(1, SinkDoor::Table);
    stamped_append(&catalog, &ident, &winner, &[1]).await;
    let loser = stamp_of(1, run_b(), 1);
    let fenced = MicroBatchError::Fenced {
        query: query(),
        epoch: Epoch::new(1),
        winner: winner.record.run,
    };
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(claim_once(&table, &loser), Err(fenced.clone()));
    assert_eq!(
        claim_once(&table, &stamp_of(0, run_b(), 1)),
        Err(MicroBatchError::Fenced {
            query: query(),
            epoch: Epoch::new(0),
            winner: winner.record.run,
        })
    );
    assert_eq!(append_refused(&catalog, &ident, &loser).await, fenced);

    let guard = BatchScope::enter(TableUuid::of(&table), loser.clone()).expect("enter");
    let stamp_only = commit_stamp_only(&catalog, &table, &loser, Some(guard.token())).await;
    assert_eq!(stamp_only, Err(fenced));
    drop(guard);
    assert_eq!(claim_once(&table, &stamp_of(2, run_b(), 1)), Ok(true));
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(
        read_resume_point(&reloaded, query()).expect("resume"),
        Some(winner.record)
    );
}

#[tokio::test]
async fn a_claim_under_another_generation_is_a_generation_mismatch() {
    let (_warehouse, catalog, ident) = fixture("epoch_generation").await;
    stamped_append(&catalog, &ident, &stamp_for(0, SinkDoor::Table), &[1]).await;
    let table = catalog.load_table(&ident).await.expect("reload");
    for (epoch, run) in [(1, record_for(query(), 0, 0).run), (0, run_b())] {
        let stamp = stamp_of(epoch, run, 2);
        assert_eq!(
            claim_once(&table, &stamp),
            Err(MicroBatchError::GenerationMismatch {
                query: query(),
                resumed: Generation::new(2).expect("generation"),
                stamped: Generation::new(1).expect("generation"),
            })
        );
    }
    let mismatch = append_refused(&catalog, &ident, &stamp_of(1, run_b(), 2)).await;
    assert!(matches!(
        mismatch,
        MicroBatchError::GenerationMismatch { .. }
    ));
    assert_eq!(claim_once(&table, &stamp_of(1, run_b(), 1)), Ok(true));

    let (_fresh_warehouse, fresh_catalog, fresh) = fixture("epoch_generation_first").await;
    let empty = fresh_catalog.load_table(&fresh).await.expect("load");
    assert_eq!(claim_once(&empty, &stamp_of(0, run_b(), 2)), Ok(true));
}

#[tokio::test]
async fn an_unscoped_stamp_only_commit_runs_the_epoch_check() {
    let (_warehouse, catalog, ident) = fixture("epoch_unscoped").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stamp = stamp_for(0, SinkDoor::ForeachBatch);
    commit_stamp_only(&catalog, &table, &stamp, None)
        .await
        .expect("first stamp-only");
    let table = catalog.load_table(&ident).await.expect("reload");
    let snapshots = table.metadata().snapshots().count();
    assert_eq!(
        commit_stamp_only(&catalog, &table, &stamp, None).await,
        Err(MicroBatchError::AlreadyCommitted {
            query: query(),
            epoch: Epoch::new(0),
        })
    );
    let mut loser = stamp_of(0, run_b(), 1);
    loser.door = SinkDoor::ForeachBatch;
    assert!(matches!(
        commit_stamp_only(&catalog, &table, &loser, None).await,
        Err(MicroBatchError::Fenced { .. })
    ));
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(table.metadata().snapshots().count(), snapshots);
}

fn stamps_at(table: &Table, epoch: u64) -> usize {
    table
        .metadata()
        .snapshots()
        .filter(|snapshot| {
            SinkRecord::from_summary(&snapshot.summary().additional_properties)
                .ok()
                .flatten()
                .is_some_and(|record| record.epoch == Epoch::new(epoch))
        })
        .count()
}

#[tokio::test]
async fn a_refused_epoch_check_refuses_every_later_claim_in_the_scope() {
    let (_warehouse, catalog, ident) = fixture("epoch_refused_scope").await;
    let empty = catalog.load_table(&ident).await.expect("load");
    let stamp = stamp_for(0, SinkDoor::Table);
    stamped_append(&catalog, &ident, &stamp, &[1]).await;
    let committed = catalog.load_table(&ident).await.expect("reload");
    let refused = MicroBatchError::AlreadyCommitted {
        query: query(),
        epoch: Epoch::new(0),
    };
    let guard = BatchScope::enter(TableUuid::of(&committed), stamp.clone()).expect("enter");
    assert_eq!(
        BatchScope::claim(&committed, guard.token()),
        Err(refused.clone())
    );
    assert_eq!(
        BatchScope::claim(&empty, guard.token()),
        Err(refused.clone())
    );
    assert_eq!(
        commit_stamp_only(&catalog, &empty, &stamp, Some(guard.token())).await,
        Err(refused)
    );
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);
    assert_eq!(
        claim_once(&committed, &stamp_for(1, SinkDoor::Table)),
        Ok(true)
    );
}

#[tokio::test]
async fn a_stale_view_after_a_refused_claim_does_not_commit_the_epoch_twice() {
    let (_warehouse, catalog, ident) = fixture("epoch_stale_after_refusal").await;
    let seeded = append_plain(&catalog, &ident, &[1]).await;
    let stamp = stamp_for(0, SinkDoor::Table);
    stamped_append(&catalog, &ident, &stamp, &[2]).await;
    let fresh = catalog.load_table(&ident).await.expect("load");
    let guard = BatchScope::enter(TableUuid::of(&fresh), stamp.clone()).expect("enter");
    let refused = MicroBatchError::AlreadyCommitted {
        query: query(),
        epoch: Epoch::new(0),
    };
    assert_eq!(
        BatchScope::claim(&fresh, guard.token()),
        Err(refused.clone())
    );
    let files = stage(&seeded, &[2]).await;
    let error = commit_append_with_summary(&catalog, &seeded, files, &scoped(&guard), None)
        .await
        .expect_err("the stale view must not commit epoch 0 again");
    assert_eq!(microbatch_cause(&error), &refused);
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    drop(guard);
    let after = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(stamps_at(&after, 0), 1);
    assert_eq!(live_ids(&after).await, vec![1, 2]);
}

fn run_of(tag: u8) -> RunId {
    RunId::new(
        Uuid::parse_str(&format!(
            "{tag:02x}{tag:02x}{tag:02x}{tag:02x}-0000-4000-8000-000000000000"
        ))
        .expect("uuid"),
    )
}

#[tokio::test]
async fn fenced_names_the_committer_of_the_claimed_epoch_and_otherwise_the_owner() {
    let (_warehouse, catalog, ident) = fixture("epoch_fenced_committer").await;
    stamped_append(&catalog, &ident, &stamp_of(1, run_of(0x11), 1), &[1]).await;
    stamped_append(&catalog, &ident, &stamp_of(2, run_of(0x22), 1), &[2]).await;
    let table = catalog.load_table(&ident).await.expect("load");
    let fenced = |epoch: u64, winner: u8| MicroBatchError::Fenced {
        query: query(),
        epoch: Epoch::new(epoch),
        winner: run_of(winner),
    };
    assert_eq!(
        claim_once(&table, &stamp_of(1, run_of(0x33), 1)),
        Err(fenced(1, 0x11))
    );
    assert_eq!(
        claim_once(&table, &stamp_of(1, run_of(0x11), 1)),
        Err(fenced(1, 0x22))
    );
    assert_eq!(
        claim_once(&table, &stamp_of(2, run_of(0x33), 1)),
        Err(fenced(2, 0x22))
    );
    assert_eq!(
        claim_once(&table, &stamp_of(0, run_of(0x33), 1)),
        Err(fenced(0, 0x22))
    );
    assert_eq!(
        claim_once(&table, &stamp_of(2, run_of(0x22), 1)),
        Err(MicroBatchError::AlreadyCommitted {
            query: query(),
            epoch: Epoch::new(2),
        })
    );
}
