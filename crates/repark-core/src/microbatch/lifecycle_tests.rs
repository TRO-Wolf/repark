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
use crate::microbatch::relation::{PlanTemplate, streaming_frame};
use crate::microbatch::table_door_tests::{FLAKY_SINK, FLAKY_SOURCE, LoadHook, flaky};
use crate::microbatch::testing::{
    Fixture, SINK, SOURCE, creation, ids_of, options, stamped_epochs, started, table_spec,
    wait_for_epoch,
};

const ONE: &[(&str, &str)] = &[("streaming-max-files-per-micro-batch", "1")];
const BOUND: Duration = Duration::from_secs(10);

enum Mode {
    Record,
    PanicAt(u64),
    FailAt(u64),
    Sleep(Duration),
    Hold(Arc<Semaphore>),
    ReplaceSink(Arc<dyn Catalog>, String),
}

struct Probe {
    mode: Mode,
    seen: Mutex<Vec<(u64, Vec<i64>)>>,
    calls: AtomicUsize,
}

impl Probe {
    fn new(mode: Mode) -> Arc<Probe> {
        Arc::new(Probe {
            mode,
            seen: Mutex::default(),
            calls: AtomicUsize::new(0),
        })
    }

    fn seen(&self) -> Vec<(u64, Vec<i64>)> {
        self.seen.lock().expect("seen").clone()
    }

    fn calls(&self) -> usize {
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

fn foreach(body: &Arc<Probe>, trigger: Trigger, caps: &[(&str, &str)]) -> StreamSpec {
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

fn named(mut spec: StreamSpec, name: &str) -> StreamSpec {
    spec.query_name = Some(name.to_string());
    spec
}

async fn ended(handle: &QueryHandle) -> Result<bool, Arc<MicroBatchError>> {
    tokio::time::timeout(BOUND, handle.await_termination(None))
        .await
        .expect("the query ends within the bound")
}

async fn eventually(what: &str, check: impl Fn() -> bool) {
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
            MicroBatchError::BatchFailed { epoch, cause }
                if *epoch == Epoch::FIRST && cause.contains(message)
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

fn door_spec(door: &str, body: &Arc<Probe>, trigger: Trigger) -> StreamSpec {
    match door {
        "foreachBatch" => foreach(body, trigger, ONE),
        _ => table_spec(trigger, &options(ONE)),
    }
}

fn wall_millis() -> u64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past the epoch");
    u64::try_from(since.as_millis()).expect("the wall clock fits")
}
