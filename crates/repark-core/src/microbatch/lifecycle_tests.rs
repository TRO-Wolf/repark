use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::{DataFrame, col};
use futures::future::BoxFuture;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
use repark_iceberg::microbatch::offset::Epoch;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

use crate::Session;
use crate::microbatch::driver::{
    BatchBody, QueryHandle, QueryState, ShutdownOutcome, SinkSpec, StreamSpec,
    StreamingQueryManager, Trigger,
};
use crate::microbatch::progress::StatusMessage;
use crate::microbatch::relation::{PlanTemplate, streaming_frame};
use crate::microbatch::table_door_tests::{FLAKY_SINK, FLAKY_SOURCE, LoadHook, flaky};
use crate::microbatch::testing::{
    Fixture, SINK, SOURCE, creation, ids_of, options, stamped_epochs, started, table_spec,
    wait_for_epoch,
};

pub(super) const ONE: &[(&str, &str)] = &[("streaming-max-files-per-micro-batch", "1")];
pub(super) const BOUND: Duration = Duration::from_secs(10);

pub(super) enum Mode {
    Record,
    PanicAt(u64),
    FailAt(u64),
    Sleep(Duration),
    Hold(Arc<Semaphore>),
    ReplaceSink(Arc<dyn Catalog>, String),
}

pub(super) struct Probe {
    mode: Mode,
    seen: Mutex<Vec<(u64, Vec<i64>)>>,
    calls: AtomicUsize,
}

impl Probe {
    pub(super) fn new(mode: Mode) -> Arc<Probe> {
        Arc::new(Probe {
            mode,
            seen: Mutex::default(),
            calls: AtomicUsize::new(0),
        })
    }

    pub(super) fn seen(&self) -> Vec<(u64, Vec<i64>)> {
        self.seen.lock().expect("seen").clone()
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    async fn run_batch(&self, frame: DataFrame, epoch: Epoch) -> Result<(), MicroBatchError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let rows = frame
            .collect()
            .await
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
        self.seen
            .lock()
            .expect("seen")
            .push((epoch.get(), ids_of(&rows)));
        match &self.mode {
            Mode::PanicAt(at) if *at == epoch.get() => panic!("the body panicked"),
            Mode::FailAt(at) if *at == epoch.get() => {
                Err(MicroBatchError::Catalog(String::from("the body failed")))
            }
            Mode::Sleep(pause) => {
                tokio::time::sleep(*pause).await;
                Ok(())
            }
            Mode::Hold(gate) => {
                gate.acquire().await.expect("the gate is open").forget();
                Ok(())
            }
            Mode::ReplaceSink(catalog, location) => {
                let sales = NamespaceIdent::new("sales".to_string());
                catalog
                    .drop_table(&TableIdent::new(sales.clone(), "silver".to_string()))
                    .await
                    .expect("the sink drops");
                catalog
                    .create_table(&sales, creation("silver", location))
                    .await
                    .expect("the sink is created again");
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl BatchBody for Probe {
    fn run(&self, frame: DataFrame, epoch: Epoch) -> BoxFuture<'_, Result<(), MicroBatchError>> {
        Box::pin(self.run_batch(frame, epoch))
    }
}

pub(super) fn foreach(body: &Arc<Probe>, trigger: Trigger, caps: &[(&str, &str)]) -> StreamSpec {
    let mut spec = StreamSpec::new(
        SOURCE,
        options(caps),
        SinkSpec::ForeachBatch {
            sink: SINK.to_string(),
            body: Arc::clone(body) as Arc<dyn BatchBody>,
        },
    );
    spec.trigger = trigger;
    spec
}

pub(super) fn named(mut spec: StreamSpec, name: &str) -> StreamSpec {
    spec.query_name = Some(name.to_string());
    spec
}

pub(super) async fn ended(handle: &QueryHandle) -> Result<bool, Arc<MicroBatchError>> {
    tokio::time::timeout(BOUND, handle.await_termination(None))
        .await
        .expect("the query ends within the bound")
}

pub(super) async fn eventually(what: &str, check: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + BOUND;
    while !check() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn assert_panicked(fixture: &Fixture, handle: &QueryHandle, message: &str) {
    let error = ended(handle).await.expect_err("a panic fails the query");
    assert!(
        matches!(
            error.as_ref(),
            MicroBatchError::DriverPanicked { epoch, message: reported }
                if *epoch == Epoch::FIRST && reported == message
        ),
        "{error:?}"
    );
    assert_eq!(handle.state(), QueryState::Failed);
    assert!(!handle.is_active());
    assert_eq!(handle.exception(), Some(Arc::clone(&error)));
    assert_eq!(handle.durable(), None, "the offset does not advance");
    let stopped = tokio::time::timeout(BOUND, handle.stop())
        .await
        .expect("stop returns");
    assert_eq!(
        stopped,
        ShutdownOutcome::Failed {
            durable: None,
            error
        }
    );
    assert!(
        StreamingQueryManager::of(&fixture.session)
            .active()
            .is_empty()
    );
}

#[tokio::test]
async fn a_panicking_body_fails_the_query_and_frees_its_id() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1), (2), (3)").await;
    let body = Probe::new(Mode::PanicAt(0));
    let handle = started(&fixture, foreach(&body, Trigger::AvailableNow, ONE)).await;
    assert_panicked(&fixture, &handle, "the body panicked").await;
    assert!(stamped_epochs(&fixture.table("silver").await).is_empty());
    let again = Probe::new(Mode::Record);
    let restart = started(&fixture, foreach(&again, Trigger::AvailableNow, ONE)).await;
    assert_eq!(restart.id(), handle.id());
    assert_eq!(ended(&restart).await, Ok(true));
    assert_eq!(again.seen(), [(0, vec![1, 2, 3])]);
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
}

#[tokio::test]
async fn a_panic_in_plan_execution_fails_the_query_on_the_table_door() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1), (2), (3)").await;
    let frame = streaming_frame(&fixture.session, SOURCE, &BTreeMap::new())
        .await
        .expect("the streaming frame");
    let boom = create_udf(
        "boom",
        vec![DataType::Int64],
        DataType::Boolean,
        Volatility::Volatile,
        Arc::new(|_: &[ColumnarValue]| panic!("the plan panicked")),
    );
    let frame = frame
        .filter(boom.call(vec![col("id")]))
        .expect("the filter plans");
    let mut spec = table_spec(Trigger::AvailableNow, &options(&[]));
    spec.plan = Some(PlanTemplate::from_frame(&frame).expect("a template"));
    let handle = started(&fixture, spec).await;
    assert_panicked(&fixture, &handle, "the plan panicked").await;
    assert!(fixture.ids(SINK).await.is_empty());
    let restart = started(&fixture, table_spec(Trigger::AvailableNow, &options(&[]))).await;
    assert_eq!(ended(&restart).await, Ok(true));
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3]);
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
}

fn flaky_spec() -> StreamSpec {
    let mut spec = StreamSpec::new(
        FLAKY_SOURCE,
        options(&[]),
        SinkSpec::Table {
            sink: FLAKY_SINK.to_string(),
        },
    );
    spec.trigger = Trigger::Once;
    spec
}

fn panic_on(name: &'static str) -> LoadHook {
    Arc::new(move |table: &TableIdent| {
        assert!(table.name() != name, "the catalog panicked");
        Box::pin(async {})
    })
}

fn gate_on(name: &'static str, gate: &Arc<Semaphore>, arrivals: &Arc<AtomicUsize>) -> LoadHook {
    let (gate, arrivals) = (Arc::clone(gate), Arc::clone(arrivals));
    Arc::new(move |table: &TableIdent| {
        let (gate, arrivals) = (Arc::clone(&gate), Arc::clone(&arrivals));
        let held = table.name() == name;
        Box::pin(async move {
            if held {
                arrivals.fetch_add(1, Ordering::SeqCst);
                gate.acquire().await.expect("the gate is open").forget();
            }
        })
    })
}

#[tokio::test]
async fn a_panic_during_planning_fails_the_query_and_frees_its_id() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let catalog = flaky(&fixture).await;
    let manager = StreamingQueryManager::of(&fixture.session);
    let handle = manager
        .register(&fixture.session, flaky_spec())
        .await
        .expect("register");
    catalog.on_load(Some(panic_on("orders")));
    handle.start_below_catalog_check().expect("start");
    assert_panicked(&fixture, &handle, "the catalog panicked").await;
    catalog.on_load(None);
    let restart = manager
        .register(&fixture.session, flaky_spec())
        .await
        .expect("register");
    restart.start_below_catalog_check().expect("start");
    assert_eq!(ended(&restart).await, Ok(true));
    assert_eq!(fixture.ids(SINK).await, [1]);
}

async fn race(fixture: &Fixture, name: String) -> (QueryHandle, ShutdownOutcome) {
    let spec = named(
        table_spec(Trigger::ProcessingTime(Duration::ZERO), &options(&[])),
        &name,
    );
    let handle = StreamingQueryManager::of(&fixture.session)
        .register(&fixture.session, spec)
        .await
        .expect("register");
    let barrier = Arc::new(Barrier::new(2));
    let starter = std::thread::spawn({
        let (handle, barrier, runtime) = (handle.clone(), Arc::clone(&barrier), Handle::current());
        move || {
            let _entered = runtime.enter();
            barrier.wait();
            let _ = handle.start_below_catalog_check();
        }
    });
    let stopper = std::thread::spawn({
        let (handle, runtime) = (handle.clone(), Handle::current());
        move || {
            barrier.wait();
            runtime.block_on(handle.stop())
        }
    });
    starter.join().expect("the starter joins");
    let outcome = stopper.join().expect("the stopper joins");
    (handle, outcome)
}

const RACE_ROUNDS: usize = 300;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_racing_stop_leaves_no_task_behind() {
    let fixture = Fixture::new().await;
    let metrics = Handle::current().metrics();
    for round in 0..RACE_ROUNDS {
        let baseline = metrics.num_alive_tasks();
        let (handle, outcome) = race(&fixture, format!("race{round}")).await;
        assert_eq!(outcome, ShutdownOutcome::Stopped { durable: None });
        assert_eq!(handle.state(), QueryState::Stopped);
        eventually(&format!("round {round} left a driver task alive"), || {
            metrics.num_alive_tasks() <= baseline
        })
        .await;
    }
    assert!(
        StreamingQueryManager::of(&fixture.session)
            .active()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_commits_after_a_stop_that_reported_stopped() {
    let fixture = Fixture::new().await;
    let mut handles = Vec::new();
    for round in 0..RACE_ROUNDS {
        let (handle, outcome) = race(&fixture, format!("quiet{round}")).await;
        assert_eq!(outcome, ShutdownOutcome::Stopped { durable: None });
        handles.push(handle);
    }
    fixture.insert(SOURCE, "(7), (8)").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(fixture.ids(SINK).await.is_empty());
    assert!(stamped_epochs(&fixture.table("silver").await).is_empty());
    assert!(handles.iter().all(|handle| handle.durable().is_none()));
}

async fn assert_released_after_the_drop(trigger: Trigger) {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let metrics = Handle::current().metrics();
    let baseline = metrics.num_alive_tasks();
    let body = Probe::new(Mode::Record);
    let handle = started(&fixture, foreach(&body, trigger, ONE)).await;
    wait_for_epoch(&handle, 0).await;
    let catalog = Arc::downgrade(
        fixture
            .session
            .catalogs_snapshot()
            .get("ice")
            .expect("the catalog is visible"),
    );
    assert!(catalog.strong_count() > 0);
    let Fixture { warehouse, session } = fixture;
    drop(session);
    eventually("the query never saw the session drop", || {
        handle.state() == QueryState::Stopped
    })
    .await;
    assert_eq!(handle.exception(), None);
    assert_eq!(handle.durable().map(|record| record.epoch.get()), Some(0));
    assert_eq!(body.calls(), 1);
    drop(handle);
    eventually("the driver task outlived its session", || {
        metrics.num_alive_tasks() <= baseline
    })
    .await;
    eventually("the catalog outlived its session", || {
        catalog.strong_count() == 0
    })
    .await;
    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_session_stops_its_queries_and_releases_the_catalog() {
    assert_released_after_the_drop(Trigger::ProcessingTime(Duration::ZERO)).await;
    assert_released_after_the_drop(Trigger::ProcessingTime(Duration::from_hours(1))).await;
}

#[tokio::test]
async fn a_registered_query_does_not_keep_the_session_alive() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let body = Probe::new(Mode::Record);
    let handle = StreamingQueryManager::of(&fixture.session)
        .register(&fixture.session, foreach(&body, Trigger::Once, &[]))
        .await
        .expect("register");
    let state = fixture.session.context().state_weak_ref();
    let Fixture { warehouse, session } = fixture;
    drop(session);
    assert_eq!(
        state.strong_count(),
        0,
        "a registered query keeps the session state alive"
    );
    assert!(handle.start_below_catalog_check().is_err());
    assert_eq!(
        handle.stop().await,
        ShutdownOutcome::Stopped { durable: None }
    );
    assert_eq!(body.calls(), 0);
    drop(warehouse);
}

const WAKE_ROUNDS: usize = 8;
const WAKE_BOUND: Duration = Duration::from_millis(25);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_session_drop_wakes_a_waiting_query_without_a_poll() {
    let mut prompt = 0;
    let mut waits = Vec::new();
    for _ in 0..WAKE_ROUNDS {
        let fixture = Fixture::new().await;
        fixture.insert(SOURCE, "(1)").await;
        let trigger = Trigger::ProcessingTime(Duration::from_hours(1));
        let handle = started(&fixture, table_spec(trigger, &options(&[]))).await;
        wait_for_epoch(&handle, 0).await;
        eventually("the query never reached its trigger wait", || {
            handle.status().message == StatusMessage::WaitingForNextTrigger
        })
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let Fixture { warehouse, session } = fixture;
        let dropped = std::time::Instant::now();
        drop(session);
        assert_eq!(ended(&handle).await, Ok(true));
        let waited = dropped.elapsed();
        prompt += usize::from(waited < WAKE_BOUND);
        waits.push(waited);
        assert_eq!(handle.state(), QueryState::Stopped);
        drop(warehouse);
    }
    assert!(
        prompt + 1 >= WAKE_ROUNDS,
        "the stop waited for a poll tick: {waits:?}"
    );
}

const STREAMED: usize = 40;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_gone_behind_a_live_frame_still_ends_its_queries() {
    for busy in [false, true] {
        let fixture = Fixture::new().await;
        for value in 0..if busy { STREAMED } else { 1 } {
            fixture.insert(SOURCE, &format!("({value})")).await;
        }
        let body = Probe::new(Mode::Sleep(Duration::from_millis(20)));
        let trigger = if busy {
            Trigger::ProcessingTime(Duration::ZERO)
        } else {
            Trigger::ProcessingTime(Duration::from_hours(1))
        };
        let handle = started(&fixture, foreach(&body, trigger, ONE)).await;
        wait_for_epoch(&handle, 0).await;
        let live = fixture
            .session
            .sql("SELECT 1")
            .await
            .expect("a frame that outlives the session");
        let manager = Arc::downgrade(&StreamingQueryManager::of(&fixture.session));
        let Fixture { warehouse, session } = fixture;
        drop(session);
        assert_eq!(ended(&handle).await, Ok(true), "busy: {busy}");
        assert_eq!(handle.state(), QueryState::Stopped);
        assert!(
            manager.strong_count() > 0,
            "the frame keeps the manager, so its Drop never signalled"
        );
        assert!(body.calls() < STREAMED, "busy: {busy}");
        drop(live);
        drop(warehouse);
    }
}

#[tokio::test]
async fn a_registered_query_whose_session_is_gone_concludes_stopped() {
    let fixture = Fixture::new().await;
    let body = Probe::new(Mode::Record);
    let handle = StreamingQueryManager::of(&fixture.session)
        .register(&fixture.session, foreach(&body, Trigger::Once, &[]))
        .await
        .expect("register");
    let awaiting = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.await_termination(None).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!awaiting.is_finished());
    let Fixture { warehouse, session } = fixture;
    drop(session);
    let waited = tokio::time::timeout(BOUND, awaiting)
        .await
        .expect("await_termination returns once the session is gone")
        .expect("the waiter joins");
    assert_eq!(waited, Ok(true));
    assert_eq!(handle.state(), QueryState::Stopped);
    assert_eq!(body.calls(), 0);
    drop(warehouse);
}

#[tokio::test]
async fn stopping_the_session_stops_every_query_and_waits_for_each() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    fixture.insert("ice.sales.other", "(1)").await;
    let first = started(
        &fixture,
        table_spec(Trigger::ProcessingTime(Duration::ZERO), &options(&[])),
    )
    .await;
    let mut second = table_spec(
        Trigger::ProcessingTime(Duration::from_hours(1)),
        &options(&[]),
    );
    second.sink = SinkSpec::Table {
        sink: "ice.sales.other".to_string(),
    };
    let second = started(&fixture, second).await;
    wait_for_epoch(&first, 0).await;
    wait_for_epoch(&second, 0).await;
    let manager = StreamingQueryManager::of(&fixture.session);
    let outcomes = tokio::time::timeout(BOUND, manager.stop_all())
        .await
        .expect("the session stop returns");
    assert_eq!(outcomes.len(), 2);
    assert!(
        outcomes.iter().all(|outcome| matches!(
            outcome,
            ShutdownOutcome::Stopped { durable: Some(record) } if record.epoch == Epoch::FIRST
        )),
        "{outcomes:?}"
    );
    assert_eq!(first.state(), QueryState::Stopped);
    assert_eq!(second.state(), QueryState::Stopped);
    assert!(manager.active().is_empty());
}

fn door_spec(door: &str, body: &Arc<Probe>, trigger: Trigger) -> StreamSpec {
    match door {
        "foreachBatch" => foreach(body, trigger, ONE),
        _ => table_spec(trigger, &options(ONE)),
    }
}

#[tokio::test]
async fn a_second_query_on_an_active_sink_is_refused_at_start() {
    for door in ["foreachBatch", "toTable"] {
        let fixture = Fixture::new().await;
        fixture.insert(SOURCE, "(1)").await;
        let manager = StreamingQueryManager::of(&fixture.session);
        let body = Probe::new(Mode::Record);
        let running = Trigger::ProcessingTime(Duration::ZERO);
        let first = started(&fixture, named(door_spec(door, &body, running), "a")).await;
        let late = Probe::new(Mode::Record);
        let second = manager
            .register(
                &fixture.session,
                named(door_spec(door, &late, Trigger::AvailableNow), "b"),
            )
            .await
            .expect("register");
        assert_eq!(
            second.start_below_catalog_check(),
            Err(MicroBatchError::SinkBusy {
                sink: SINK.to_string()
            }),
            "{door}"
        );
        assert_eq!(second.state(), QueryState::Registered);
        assert_eq!(late.calls(), 0);
        wait_for_epoch(&first, 0).await;
        assert!(first.is_active(), "{door}");
        assert!(matches!(
            first.stop().await,
            ShutdownOutcome::Stopped { .. }
        ));
        second
            .start_below_catalog_check()
            .expect("the sink is free again");
        assert_eq!(ended(&second).await, Ok(true), "{door}");
        assert_eq!(
            second.durable().map(|record| record.epoch.get()),
            Some(0),
            "{door}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sessions_on_one_sink_wait_for_the_scope() {
    for door in ["foreachBatch", "toTable"] {
        let fixture = Fixture::new().await;
        for value in 1..=6 {
            fixture.insert(SOURCE, &format!("({value})")).await;
        }
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
        let pause = Duration::from_millis(30);
        let (left, right) = (
            Probe::new(Mode::Sleep(pause)),
            Probe::new(Mode::Sleep(pause)),
        );
        let a = started(
            &fixture,
            named(door_spec(door, &left, Trigger::AvailableNow), "a"),
        )
        .await;
        let b = StreamingQueryManager::of(&other)
            .register(
                &other,
                named(door_spec(door, &right, Trigger::AvailableNow), "b"),
            )
            .await
            .expect("register");
        b.start_below_catalog_check().expect("start");
        assert_eq!(ended(&a).await, Ok(true), "{door}");
        assert_eq!(ended(&b).await, Ok(true), "{door}");
        assert_eq!(a.durable().map(|record| record.epoch.get()), Some(5));
        assert_eq!(b.durable().map(|record| record.epoch.get()), Some(5));
        if door == "foreachBatch" {
            assert_eq!(left.calls(), 6);
            assert_eq!(right.calls(), 6);
        } else {
            assert_eq!(
                fixture.ids(SINK).await,
                [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6]
            );
        }
    }
}

#[tokio::test]
async fn a_scope_wait_that_outlives_the_timeout_ends_sink_busy() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
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
    let gate = Arc::new(Semaphore::new(0));
    let holding = Probe::new(Mode::Hold(Arc::clone(&gate)));
    let holder = started(
        &fixture,
        named(foreach(&holding, Trigger::Once, &[]), "holder"),
    )
    .await;
    eventually("the holder never ran", || holding.calls() == 1).await;
    let waiting = Probe::new(Mode::Record);
    let waiter = || {
        let mut spec = named(foreach(&waiting, Trigger::Once, &[]), "waiter");
        spec.catalog_timeout = Duration::from_millis(150);
        spec
    };
    let refused = StreamingQueryManager::of(&other)
        .register(&other, waiter())
        .await
        .expect("register");
    refused.start_below_catalog_check().expect("start");
    let error = ended(&refused).await.expect_err("the scope stays held");
    assert_eq!(
        *error,
        MicroBatchError::SinkBusy {
            sink: SINK.to_string()
        }
    );
    assert_eq!(refused.durable(), None, "the offset does not advance");
    assert_eq!(waiting.calls(), 0);
    gate.add_permits(1);
    assert_eq!(ended(&holder).await, Ok(true));
    let retried = StreamingQueryManager::of(&other)
        .register(&other, waiter())
        .await
        .expect("register");
    retried.start_below_catalog_check().expect("start");
    assert_eq!(ended(&retried).await, Ok(true));
    assert_eq!(waiting.seen(), [(0, vec![1])]);
}

#[tokio::test]
async fn a_stop_during_planning_never_starts_the_body() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let catalog = flaky(&fixture).await;
    let body = Probe::new(Mode::Record);
    let mut spec = foreach(&body, Trigger::Once, &[]);
    spec.source = FLAKY_SOURCE.to_string();
    let handle = StreamingQueryManager::of(&fixture.session)
        .register(&fixture.session, spec)
        .await
        .expect("register");
    let (gate, arrivals) = (Arc::new(Semaphore::new(0)), Arc::new(AtomicUsize::new(0)));
    catalog.on_load(Some(gate_on("orders", &gate, &arrivals)));
    handle.start_below_catalog_check().expect("start");
    eventually("planning never reached the catalog", || {
        arrivals.load(Ordering::SeqCst) > 0
    })
    .await;
    let stopping = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.stop().await })
    };
    eventually("stop never began", || {
        handle.state() == QueryState::Draining
    })
    .await;
    gate.add_permits(Semaphore::MAX_PERMITS / 2);
    let outcome = stopping.await.expect("stop joins");
    assert_eq!(outcome, ShutdownOutcome::Stopped { durable: None });
    assert_eq!(body.calls(), 0, "no batch starts after stop");
    assert!(stamped_epochs(&fixture.table("silver").await).is_empty());
}

#[tokio::test]
async fn a_zero_stop_timeout_waits_for_the_batch() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let gate = Arc::new(Semaphore::new(0));
    let body = Probe::new(Mode::Hold(Arc::clone(&gate)));
    let mut spec = foreach(&body, Trigger::ProcessingTime(Duration::ZERO), ONE);
    spec.stop_timeout = Some(Duration::ZERO);
    let handle = started(&fixture, spec).await;
    eventually("the body never ran", || body.calls() == 1).await;
    let stopping = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.stop().await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!stopping.is_finished(), "a zero stopTimeout waits forever");
    assert_eq!(handle.state(), QueryState::Draining);
    gate.add_permits(1);
    let outcome = stopping.await.expect("stop joins");
    assert!(
        matches!(&outcome, ShutdownOutcome::Stopped { durable: Some(record) } if record.epoch == Epoch::FIRST),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_sink_replaced_under_the_body_ends_recovery_required() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let catalog = fixture
        .session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("the memory catalog");
    let location = format!("{root}/sales/silver_again", root = fixture.root());
    let body = Probe::new(Mode::ReplaceSink(catalog, location));
    let handle = started(&fixture, foreach(&body, Trigger::AvailableNow, ONE)).await;
    let error = ended(&handle).await.expect_err("the batch is unstamped");
    assert!(
        matches!(
            error.as_ref(),
            MicroBatchError::RecoveryRequired {
                epoch,
                durable: None,
                reason: RecoveryReason::UnstampedSinkChange { what },
                ..
            } if *epoch == Epoch::FIRST
                && what.starts_with("the table under the sink's name was replaced")
        ),
        "{error:?}"
    );
    assert_eq!(handle.state(), QueryState::RecoveryRequired);
    assert_eq!(handle.durable(), None, "the offset does not advance");
}

#[tokio::test]
async fn a_restart_after_a_failed_batch_replans_its_window_over_a_grown_source() {
    for (caps, replayed) in [
        (&[][..], vec![(0, vec![1, 2, 3, 4, 5])]),
        (ONE, vec![(0, vec![1, 2, 3]), (1, vec![4, 5])]),
    ] {
        let fixture = Fixture::new().await;
        fixture.insert(SOURCE, "(1), (2), (3)").await;
        let failing = Probe::new(Mode::FailAt(0));
        let first = started(&fixture, foreach(&failing, Trigger::AvailableNow, caps)).await;
        let error = ended(&first).await.expect_err("the body fails");
        assert!(
            matches!(error.as_ref(), MicroBatchError::BatchFailed { epoch, .. } if *epoch == Epoch::FIRST),
            "{error:?}"
        );
        assert_eq!(failing.seen(), [(0, vec![1, 2, 3])]);
        assert_eq!(first.durable(), None);
        fixture.insert(SOURCE, "(4), (5)").await;
        let body = Probe::new(Mode::Record);
        let second = started(&fixture, foreach(&body, Trigger::AvailableNow, caps)).await;
        assert_eq!(ended(&second).await, Ok(true));
        assert_eq!(body.seen(), replayed);
    }
}

fn wall_millis() -> u64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past the epoch");
    u64::try_from(since.as_millis()).expect("the wall clock fits")
}

#[tokio::test]
async fn a_processing_time_trigger_fires_on_the_interval_boundary() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let boundary = wall_millis() + 2_000;
    let trigger = Trigger::ProcessingTime(Duration::from_millis(boundary));
    let handle = started(&fixture, table_spec(trigger, &options(ONE))).await;
    wait_for_epoch(&handle, 0).await;
    fixture.insert(SOURCE, "(2)").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let early = handle.durable().map(|record| record.epoch.get());
    if wall_millis() + 100 < boundary {
        assert_eq!(early, Some(0), "the next trigger waits for the boundary");
    }
    wait_for_epoch(&handle, 1).await;
    assert!(
        wall_millis() + 50 >= boundary,
        "the next trigger fired before the interval boundary"
    );
    assert!(matches!(
        handle.stop().await,
        ShutdownOutcome::Stopped { .. }
    ));
    assert_eq!(fixture.ids(SINK).await, [1, 2]);
}
