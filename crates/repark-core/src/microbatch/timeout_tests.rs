use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use iceberg::TableIdent;
use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
use repark_iceberg::microbatch::offset::Epoch;
use tokio::sync::Semaphore;

use crate::microbatch::driver::{
    BatchBody, QueryHandle, QueryState, ShutdownOutcome, SinkSpec, StreamSpec,
    StreamingQueryManager, Trigger,
};
use crate::microbatch::lifecycle_tests::{BOUND, Mode, Probe, ended, eventually};
use crate::microbatch::table_door_tests::{
    ARMED_STALL_LANDED, ARMED_STALL_LOST, FLAKY_SINK, FLAKY_SOURCE, LoadHook, flaky,
};
use crate::microbatch::testing::{Fixture, SINK, SOURCE, options, stamped_epochs};

const LIMIT: Duration = Duration::from_millis(100);

fn stall_from(name: &'static str, nth: usize) -> LoadHook {
    let seen = Arc::new(AtomicUsize::new(0));
    Arc::new(move |table: &TableIdent| {
        let stalls = table.name() == name && seen.fetch_add(1, Ordering::SeqCst) >= nth;
        Box::pin(async move {
            if stalls {
                futures::future::pending::<()>().await;
            }
        })
    })
}

fn door_spec(body: Option<&Arc<Probe>>, trigger: Trigger) -> StreamSpec {
    let sink = FLAKY_SINK.to_string();
    let sink = match body {
        Some(body) => SinkSpec::ForeachBatch {
            sink,
            body: Arc::clone(body) as Arc<dyn BatchBody>,
        },
        None => SinkSpec::Table { sink },
    };
    let mut spec = StreamSpec::new(FLAKY_SOURCE, options(&[]), sink);
    spec.trigger = trigger;
    spec.catalog_timeout = LIMIT;
    spec
}

fn timed_out(call: &'static str) -> MicroBatchError {
    MicroBatchError::CatalogTimeout {
        call,
        waited: LIMIT,
    }
}

async fn registered(fixture: &Fixture, spec: StreamSpec) -> QueryHandle {
    StreamingQueryManager::of(&fixture.session)
        .register(&fixture.session, spec)
        .await
        .expect("register")
}

async fn restarted(fixture: &Fixture, spec: StreamSpec) -> QueryHandle {
    let handle = registered(fixture, spec).await;
    handle.start_below_catalog_check().expect("start");
    assert_eq!(ended(&handle).await, Ok(true));
    handle
}

#[tokio::test]
async fn a_stalled_catalog_refuses_register_with_the_typed_timeout() {
    for (table, call) in [("silver", "load the sink"), ("orders", "open the source")] {
        let fixture = Fixture::new().await;
        let catalog = flaky(&fixture).await;
        catalog.on_load(Some(stall_from(table, 0)));
        let manager = StreamingQueryManager::of(&fixture.session);
        let refused = tokio::time::timeout(
            BOUND,
            manager.register(&fixture.session, door_spec(None, Trigger::Once)),
        )
        .await
        .expect("register returns within the bound")
        .expect_err("the catalog stalls");
        assert_eq!(refused, timed_out(call));
    }
}

#[tokio::test]
async fn a_stalled_read_fails_the_batch_and_never_advances_the_offset() {
    for (trigger, table, nth, call) in [
        (Trigger::Once, "silver", 0, "load the sink"),
        (Trigger::Once, "silver", 1, "load the sink"),
        (Trigger::Once, "orders", 0, "read the source's start"),
        (Trigger::Once, "orders", 1, "plan the batch"),
        (
            Trigger::AvailableNow,
            "orders",
            1,
            "fix the availableNow end",
        ),
        (Trigger::AvailableNow, "orders", 2, "plan the batch"),
    ] {
        let fixture = Fixture::new().await;
        fixture.insert(SOURCE, "(1)").await;
        let catalog = flaky(&fixture).await;
        let handle = registered(&fixture, door_spec(None, trigger)).await;
        catalog.on_load(Some(stall_from(table, nth)));
        handle.start_below_catalog_check().expect("start");
        let error = ended(&handle).await.expect_err("the catalog stalls");
        assert_eq!(*error, timed_out(call), "{trigger:?} {table} {nth}");
        assert_eq!(handle.state(), QueryState::Failed);
        assert_eq!(handle.durable(), None, "the offset does not advance");
        assert!(fixture.ids(SINK).await.is_empty());
        catalog.on_load(None);
        restarted(&fixture, door_spec(None, trigger)).await;
        assert_eq!(fixture.ids(SINK).await, [1]);
        assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
    }
}

#[tokio::test]
async fn a_stalled_reload_after_the_body_leaves_the_batch_unstamped() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let catalog = flaky(&fixture).await;
    let body = Probe::new(Mode::Record);
    let handle = registered(&fixture, door_spec(Some(&body), Trigger::Once)).await;
    catalog.on_load(Some(stall_from("silver", 2)));
    handle.start_below_catalog_check().expect("start");
    let error = ended(&handle).await.expect_err("the catalog stalls");
    assert_eq!(*error, timed_out("load the sink"));
    assert_eq!(body.calls(), 1);
    assert_eq!(handle.durable(), None, "the offset does not advance");
    assert!(stamped_epochs(&fixture.table("silver").await).is_empty());
    catalog.on_load(None);
    let again = Probe::new(Mode::Record);
    restarted(&fixture, door_spec(Some(&again), Trigger::Once)).await;
    assert_eq!(again.seen(), [(0, vec![1])]);
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
}

#[tokio::test]
async fn a_stalled_commit_goes_to_the_unknown_outcome_walk() {
    for foreach in [false, true] {
        let fixture = Fixture::new().await;
        fixture.insert(SOURCE, "(1)").await;
        let catalog = flaky(&fixture).await;
        let body = Probe::new(Mode::Record);
        let spec = || door_spec(foreach.then_some(&body), Trigger::Once);
        let landed = registered(&fixture, spec()).await;
        catalog.arm(ARMED_STALL_LANDED);
        landed.start_below_catalog_check().expect("start");
        assert_eq!(ended(&landed).await, Ok(true), "foreachBatch: {foreach}");
        assert_eq!(
            landed.durable().map(|record| record.epoch),
            Some(Epoch::FIRST)
        );
        assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);

        fixture.insert(SOURCE, "(2)").await;
        let lost = registered(&fixture, spec()).await;
        catalog.arm(ARMED_STALL_LOST);
        lost.start_below_catalog_check().expect("start");
        let error = ended(&lost).await.expect_err("the commit never lands");
        assert!(
            matches!(
                error.as_ref(),
                MicroBatchError::RecoveryRequired {
                    epoch,
                    durable: Some(durable),
                    reason: RecoveryReason::CommitOutcomeUnknown { operation_id: None, .. },
                    ..
                } if epoch.get() == 1 && durable.epoch == Epoch::FIRST
            ),
            "foreachBatch: {foreach}: {error:?}"
        );
        assert_eq!(lost.state(), QueryState::RecoveryRequired);
        assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
        restarted(&fixture, spec()).await;
        assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1]);
        if !foreach {
            assert_eq!(fixture.ids(SINK).await, [1, 2]);
        }
    }
}

#[tokio::test]
async fn a_stop_timeout_over_a_stalled_catalog_still_returns() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let catalog = flaky(&fixture).await;
    let body = Probe::new(Mode::Hold(Arc::new(Semaphore::new(0))));
    let mut spec = door_spec(Some(&body), Trigger::Once);
    spec.stop_timeout = Some(Duration::from_millis(50));
    let handle = registered(&fixture, spec).await;
    catalog.on_load(Some(stall_from("silver", 2)));
    handle.start_below_catalog_check().expect("start");
    eventually("the body never ran", || body.calls() == 1).await;
    let outcome = tokio::time::timeout(BOUND, handle.stop())
        .await
        .expect("stop returns over a stalled catalog");
    assert!(
        matches!(
            &outcome,
            ShutdownOutcome::RecoveryRequired {
                durable: None,
                reason: RecoveryReason::StopTimeout { .. }
            }
        ),
        "{outcome:?}"
    );
}
