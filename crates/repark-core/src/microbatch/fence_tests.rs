use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_common::Generation;
use repark_iceberg::microbatch::error::MicroBatchError;
use repark_iceberg::microbatch::offset::{
    EPOCH_KEY, Epoch, OffsetFormatVersion, OffsetVector, QUERY_ID_KEY, QueryId, RUN_ID_KEY, RunId,
    SinkDoor, SinkRecord,
};
use repark_iceberg::microbatch::window::WindowLimit;
use repark_iceberg::write::sink_offsets::{CommitStamp, commit_stamp_only};

use crate::Session;
use crate::microbatch::driver::{
    BatchBody, QueryHandle, QueryState, SinkSpec, StreamSpec, StreamingQueryManager, Trigger,
};
use crate::microbatch::lifecycle_tests::{Mode, ONE, Probe, ended, eventually, named};
use crate::microbatch::progress::StatusMessage;
use crate::microbatch::table_door_tests::{FLAKY_SINK, LoadHook, flaky};
use crate::microbatch::testing::{Fixture, SINK, SOURCE, SinkWriter, options};
use crate::time_travel::microbatch_source::MicroBatchSource;

const DOORS: [bool; 2] = [false, true];
const RACE_ROUNDS: usize = 20;
const FILES: u64 = 3;

fn spec(sink: &str, body: Option<&Arc<Probe>>, trigger: Trigger, name: &str) -> StreamSpec {
    let sink = sink.to_string();
    let sink = match body {
        Some(body) => SinkSpec::ForeachBatch {
            sink,
            body: Arc::clone(body) as Arc<dyn BatchBody>,
        },
        None => SinkSpec::Table { sink },
    };
    let mut spec = StreamSpec::new(SOURCE, options(ONE), sink);
    spec.trigger = trigger;
    named(spec, name)
}

async fn registered(session: &Session, spec: StreamSpec) -> QueryHandle {
    StreamingQueryManager::of(session)
        .register(session, spec)
        .await
        .expect("register")
}

async fn second_session(fixture: &Fixture) -> Session {
    let shared = fixture
        .session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("the memory catalog");
    let other = Session::builder().build().expect("a second session");
    other
        .register_iceberg_catalog("ice", shared)
        .await
        .expect("the shared catalog registers");
    other
}

fn stamps_of(table: &Table, query: QueryId) -> Vec<(u64, String)> {
    let metadata = table.metadata();
    let mut stamps = Vec::new();
    let mut cursor = metadata.current_snapshot();
    while let Some(snapshot) = cursor {
        let summary = &snapshot.summary().additional_properties;
        if summary.get(QUERY_ID_KEY) == Some(&query.to_string()) {
            let epoch = summary.get(EPOCH_KEY).expect("a stamp carries its epoch");
            let run = summary.get(RUN_ID_KEY).expect("a stamp carries its run");
            stamps.push((epoch.parse().expect("an epoch is a number"), run.clone()));
        }
        cursor = snapshot
            .parent_snapshot_id()
            .and_then(|parent| metadata.snapshot_by_id(parent));
    }
    stamps.reverse();
    stamps
}

fn epochs_of(table: &Table, query: QueryId) -> Vec<u64> {
    stamps_of(table, query)
        .into_iter()
        .map(|(epoch, _)| epoch)
        .collect()
}

fn fenced_by(ending: &Result<bool, Arc<MicroBatchError>>, winner: RunId) -> bool {
    matches!(
        ending.as_ref().map_err(AsRef::as_ref),
        Err(MicroBatchError::Fenced { winner: named, .. }) if *named == winner
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_drivers_on_one_sink_land_each_epoch_exactly_once() {
    for foreach in DOORS {
        let fixture = Fixture::new().await;
        for value in 1..=FILES {
            fixture.insert(SOURCE, &format!("({value})")).await;
        }
        let other = second_session(&fixture).await;
        let every_epoch: Vec<u64> = (0..FILES).collect();
        for round in 0..RACE_ROUNDS {
            let name = format!("race{round}");
            let (left, right) = (Probe::new(Mode::Record), Probe::new(Mode::Record));
            let draining = Trigger::AvailableNow;
            let a = registered(
                &fixture.session,
                spec(SINK, foreach.then_some(&left), draining, &name),
            )
            .await;
            let b = registered(
                &other,
                spec(SINK, foreach.then_some(&right), draining, &name),
            )
            .await;
            assert_eq!(a.id(), b.id());
            assert_ne!(a.run_id(), b.run_id());
            a.start_below_catalog_check().expect("start");
            b.start_below_catalog_check().expect("start");
            let (ended_a, ended_b) = (ended(&a).await, ended(&b).await);
            let context =
                format!("foreachBatch: {foreach}, round {round}: {ended_a:?} {ended_b:?}");
            assert!(
                ended_a == Ok(true) || fenced_by(&ended_a, b.run_id()),
                "{context}"
            );
            assert!(
                ended_b == Ok(true) || fenced_by(&ended_b, a.run_id()),
                "{context}"
            );
            assert!(ended_a == Ok(true) || ended_b == Ok(true), "{context}");
            let sink = fixture.table("silver").await;
            assert_eq!(
                epochs_of(&sink, a.id()),
                every_epoch,
                "an epoch landed twice or not at all: {context}"
            );
            if foreach {
                let mut delivered: Vec<u64> = left
                    .seen()
                    .into_iter()
                    .chain(right.seen())
                    .map(|(epoch, _)| epoch)
                    .collect();
                delivered.sort_unstable();
                assert_eq!(delivered, every_epoch, "a fenced body ran: {context}");
            } else {
                let landed = fixture.ids(SINK).await;
                let rounds = round + 1;
                assert_eq!(landed.len(), every_epoch.len() * rounds, "{context}");
                for value in 1..=FILES {
                    let value = i64::try_from(value).expect("a small id");
                    let copies = landed.iter().filter(|id| **id == value).count();
                    assert_eq!(copies, rounds, "{context}");
                }
            }
            for (session, loser, ending) in [
                (&fixture.session, &left, &ended_a),
                (&other, &right, &ended_b),
            ] {
                if ending.is_err() {
                    let again = registered(
                        session,
                        spec(SINK, foreach.then_some(loser), draining, &name),
                    )
                    .await;
                    again.start_below_catalog_check().expect("start");
                    assert_eq!(ended(&again).await, Ok(true), "{context}");
                }
            }
            let sink = fixture.table("silver").await;
            assert_eq!(epochs_of(&sink, a.id()), every_epoch, "{context}");
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Racer {
    Stamp,
    PropertyOnly,
}

async fn race(inner: &Arc<dyn Catalog>, stamp: &CommitStamp, racer: Racer) {
    let silver = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "silver".to_string(),
    );
    let sink = inner.load_table(&silver).await.expect("the sink loads");
    if racer == Racer::Stamp {
        commit_stamp_only(inner, &sink, stamp, None)
            .await
            .expect("the racing commit lands");
        return;
    }
    let (key, value) = stamp.record.property().expect("the offsets property");
    let tx = Transaction::new(&sink);
    let tx = tx
        .update_table_properties()
        .set(key, value)
        .apply(tx)
        .expect("the property applies");
    tx.commit(inner.as_ref())
        .await
        .expect("the property commit lands");
}

fn racing(inner: &Arc<dyn Catalog>, stamp: &CommitStamp, nth: usize, racer: Racer) -> LoadHook {
    let (inner, stamp) = (Arc::clone(inner), stamp.clone());
    let seen = Arc::new(AtomicUsize::new(0));
    Arc::new(move |table: &TableIdent| {
        let fires = table.name() == "silver" && seen.fetch_add(1, Ordering::SeqCst) == nth;
        let (inner, stamp) = (Arc::clone(&inner), stamp.clone());
        Box::pin(async move {
            if fires {
                race(&inner, &stamp, racer).await;
            }
        })
    })
}

async fn first_epoch_of(fixture: &Fixture, query: QueryId, run: RunId) -> CommitStamp {
    let source = MicroBatchSource::open(&fixture.session, SOURCE, options(ONE))
        .await
        .expect("the source opens");
    let from = source
        .initial_offset()
        .await
        .expect("the start plans")
        .expect("the source has data");
    let window = source
        .next_batch(&from, WindowLimit::Capped)
        .await
        .expect("the window plans")
        .expect("a batch");
    CommitStamp {
        record: SinkRecord {
            format: OffsetFormatVersion::CURRENT,
            query,
            run,
            epoch: Epoch::FIRST,
            generation: Generation::new(1).expect("generation 1"),
            offsets: OffsetVector::single(window.end),
        },
        door: SinkDoor::ForeachBatch,
    }
}

struct Raced {
    fixture: Fixture,
    handle: QueryHandle,
    racer: RunId,
    body: Arc<Probe>,
    error: Arc<MicroBatchError>,
}

async fn raced(foreach: bool, same_run: bool, racer: Racer) -> Raced {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    fixture.insert(SOURCE, "(2)").await;
    let catalog = flaky(&fixture).await;
    let inner = fixture
        .session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("the memory catalog");
    let body = Probe::new(Mode::Record);
    let draining = Trigger::AvailableNow;
    let handle = registered(
        &fixture.session,
        spec(FLAKY_SINK, foreach.then_some(&body), draining, "raced"),
    )
    .await;
    let run = if same_run {
        handle.run_id()
    } else {
        RunId::fresh()
    };
    let stamp = first_epoch_of(&fixture, handle.id(), run).await;
    let refresh_inside_the_commit = if foreach { 3 } else { 2 };
    catalog.on_load(Some(racing(
        &inner,
        &stamp,
        refresh_inside_the_commit,
        racer,
    )));
    handle.start_below_catalog_check().expect("start");
    let error = ended(&handle).await.expect_err("the fence refuses");
    catalog.on_load(None);
    Raced {
        fixture,
        handle,
        racer: run,
        body,
        error,
    }
}

async fn stamped_race(foreach: bool, same_run: bool) -> Raced {
    let raced = raced(foreach, same_run, Racer::Stamp).await;
    let (handle, winner) = (&raced.handle, raced.racer);
    let expected = if same_run {
        MicroBatchError::AlreadyCommitted {
            query: handle.id(),
            epoch: Epoch::FIRST,
        }
    } else {
        MicroBatchError::Fenced {
            query: handle.id(),
            epoch: Epoch::FIRST,
            winner,
        }
    };
    assert_eq!(*raced.error, expected, "foreachBatch: {foreach}");
    assert_eq!(handle.state(), QueryState::Failed);
    let sink = raced.fixture.table("silver").await;
    assert_eq!(
        stamps_of(&sink, handle.id()),
        [(0, winner.to_string())],
        "epoch 0 landed once, as the racing commit"
    );
    assert!(raced.fixture.ids(SINK).await.is_empty());
    raced
}

async fn assert_the_restart_continues(fixture: &Fixture, foreach: bool, raced: &QueryHandle) {
    let body = Probe::new(Mode::Record);
    let restart = registered(
        &fixture.session,
        spec(
            FLAKY_SINK,
            foreach.then_some(&body),
            Trigger::AvailableNow,
            "raced",
        ),
    )
    .await;
    assert_eq!(restart.id(), raced.id());
    restart.start_below_catalog_check().expect("start");
    assert_eq!(ended(&restart).await, Ok(true));
    let sink = fixture.table("silver").await;
    assert_eq!(epochs_of(&sink, raced.id()), [0, 1]);
    if foreach {
        assert_eq!(body.seen(), [(1, vec![2])]);
    } else {
        assert_eq!(fixture.ids(SINK).await, [2]);
    }
}

#[tokio::test]
async fn a_racing_commit_at_the_fence_ends_the_driver_fenced() {
    for foreach in DOORS {
        let raced = stamped_race(foreach, false).await;
        assert_eq!(raced.body.calls(), usize::from(foreach));
        assert_the_restart_continues(&raced.fixture, foreach, &raced.handle).await;
    }
}

#[tokio::test]
async fn a_writing_body_that_loses_its_epoch_ends_fenced_and_lands_no_row() {
    for the_bodys_load_or_its_commit in [2, 3] {
        let fixture = Fixture::new().await;
        fixture.insert(SOURCE, "(1)").await;
        fixture.insert(SOURCE, "(2)").await;
        let catalog = flaky(&fixture).await;
        let inner = fixture
            .session
            .catalogs_snapshot()
            .get("ice")
            .cloned()
            .expect("the memory catalog");
        let body = SinkWriter::new(&fixture.session, FLAKY_SINK, 1);
        let handle = registered(&fixture.session, named(body.spec(&options(ONE)), "raced")).await;
        let winner = RunId::fresh();
        let stamp = first_epoch_of(&fixture, handle.id(), winner).await;
        catalog.on_load(Some(racing(
            &inner,
            &stamp,
            the_bodys_load_or_its_commit,
            Racer::Stamp,
        )));
        handle.start_below_catalog_check().expect("start");
        let ending = ended(&handle).await;
        catalog.on_load(None);
        assert!(
            fenced_by(&ending, winner),
            "load {the_bodys_load_or_its_commit}: {ending:?}"
        );
        assert_eq!(body.calls(), 1);
        let sink = fixture.table("silver").await;
        assert_eq!(
            stamps_of(&sink, handle.id()),
            [(0, winner.to_string())],
            "load {the_bodys_load_or_its_commit}"
        );
        assert!(fixture.ids(SINK).await.is_empty());
    }
}

#[tokio::test]
async fn a_stale_re_delivery_of_an_epoch_ends_already_committed() {
    for foreach in DOORS {
        let raced = stamped_race(foreach, true).await;
        assert_eq!(raced.racer, raced.handle.run_id());
        assert_the_restart_continues(&raced.fixture, foreach, &raced.handle).await;
    }
}

#[tokio::test]
async fn a_fence_refusal_that_needs_recovery_ends_recovery_required() {
    for foreach in DOORS {
        let raced = raced(foreach, false, Racer::PropertyOnly).await;
        assert!(
            matches!(
                raced.error.as_ref(),
                MicroBatchError::RecoveryRequired { epoch, .. } if *epoch == Epoch::FIRST
            ),
            "foreachBatch: {foreach}: {error:?}",
            error = raced.error
        );
        assert_eq!(raced.handle.state(), QueryState::RecoveryRequired);
        let sink = raced.fixture.table("silver").await;
        assert!(stamps_of(&sink, raced.handle.id()).is_empty());
        assert!(raced.fixture.ids(SINK).await.is_empty());
    }
}

fn wall_millis() -> u64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past the epoch");
    u64::try_from(since.as_millis()).expect("the wall clock fits")
}

#[tokio::test]
async fn a_driver_resumed_from_a_stale_view_is_fenced_before_its_body() {
    for foreach in DOORS {
        let fixture = Fixture::new().await;
        let other = second_session(&fixture).await;
        let stale = Probe::new(Mode::Record);
        let boundary = Trigger::ProcessingTime(Duration::from_millis(wall_millis() + 1_500));
        let late = registered(
            &other,
            spec(SINK, foreach.then_some(&stale), boundary, "stale"),
        )
        .await;
        late.start_below_catalog_check().expect("start");
        eventually("the late driver never resumed", || {
            late.status().message == StatusMessage::WaitingForNextTrigger
        })
        .await;
        fixture.insert(SOURCE, "(1)").await;
        let fresh = Probe::new(Mode::Record);
        let draining = Trigger::AvailableNow;
        let winner = registered(
            &fixture.session,
            spec(SINK, foreach.then_some(&fresh), draining, "stale"),
        )
        .await;
        winner.start_below_catalog_check().expect("start");
        assert_eq!(ended(&winner).await, Ok(true));
        let ending = ended(&late).await;
        assert!(fenced_by(&ending, winner.run_id()), "{ending:?}");
        assert_eq!(stale.calls(), 0, "a fenced driver never runs its body");
        let sink = fixture.table("silver").await;
        assert_eq!(
            stamps_of(&sink, winner.id()),
            [(0, winner.run_id().to_string())]
        );
        fixture.insert(SOURCE, "(2)").await;
        let resumed = Probe::new(Mode::Record);
        let restart = registered(
            &other,
            spec(SINK, foreach.then_some(&resumed), draining, "stale"),
        )
        .await;
        restart.start_below_catalog_check().expect("start");
        assert_eq!(ended(&restart).await, Ok(true));
        let sink = fixture.table("silver").await;
        assert_eq!(epochs_of(&sink, winner.id()), [0, 1]);
        if foreach {
            assert_eq!(resumed.seen(), [(1, vec![2])]);
        } else {
            assert_eq!(fixture.ids(SINK).await, [1, 2]);
        }
    }
}
