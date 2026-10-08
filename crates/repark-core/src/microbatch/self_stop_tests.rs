use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use datafusion::prelude::DataFrame;
use futures::future::BoxFuture;
use repark_iceberg::microbatch::error::MicroBatchError;
use repark_iceberg::microbatch::offset::Epoch;
use tokio::runtime::Handle;
use tokio::sync::Barrier;

use crate::microbatch::driver::{
    BatchBody, QueryHandle, QueryState, ShutdownOutcome, SinkSpec, StreamSpec,
    StreamingQueryManager, Trigger,
};
use crate::microbatch::lifecycle_tests::{ONE, ended, eventually, named};
use crate::microbatch::testing::{Fixture, SINK, SOURCE, options, stamped_epochs, wait_for_epoch};

const OTHER_SINK: &str = "ice.sales.other";

enum Act {
    Nothing,
    StopSelf,
    StopAll(Weak<StreamingQueryManager>),
    StopPeer(Arc<Barrier>),
    AwaitSelf,
}

#[derive(Debug, PartialEq)]
enum Answer {
    Stopped(Vec<ShutdownOutcome>),
    Awaited(Result<bool, Arc<MicroBatchError>>),
}

struct Inside {
    act: Act,
    own: Mutex<Option<QueryHandle>>,
    peer: Mutex<Option<QueryHandle>>,
    answers: Mutex<Vec<Answer>>,
    calls: AtomicUsize,
}

impl Inside {
    fn new(act: Act) -> Arc<Inside> {
        Arc::new(Inside {
            act,
            own: Mutex::default(),
            peer: Mutex::default(),
            answers: Mutex::default(),
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn answers(&self) -> Vec<Answer> {
        std::mem::take(&mut *self.answers.lock().expect("the answers"))
    }

    fn handle(slot: &Mutex<Option<QueryHandle>>) -> QueryHandle {
        slot.lock()
            .expect("the slot")
            .clone()
            .expect("the body knows the query")
    }

    async fn act(&self) -> Option<Answer> {
        match &self.act {
            Act::Nothing => None,
            Act::StopSelf => Some(Answer::Stopped(vec![
                Inside::handle(&self.own).stop().await,
            ])),
            Act::StopAll(manager) => {
                let manager = manager.upgrade().expect("the manager is alive");
                Some(Answer::Stopped(manager.stop_all().await))
            }
            Act::StopPeer(meeting) => {
                meeting.wait().await;
                Some(Answer::Stopped(vec![
                    Inside::handle(&self.peer).stop().await,
                ]))
            }
            Act::AwaitSelf => {
                let own = Inside::handle(&self.own);
                Some(Answer::Awaited(own.await_termination(None).await))
            }
        }
    }

    async fn run_batch(&self) -> Result<(), MicroBatchError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0
            && let Some(answer) = self.act().await
        {
            self.answers.lock().expect("the answers").push(answer);
        }
        Ok(())
    }
}

impl BatchBody for Inside {
    fn run(&self, _frame: DataFrame, _epoch: Epoch) -> BoxFuture<'_, Result<(), MicroBatchError>> {
        Box::pin(self.run_batch())
    }
}

fn spec(body: &Arc<Inside>, sink: &str, trigger: Trigger) -> StreamSpec {
    let mut spec = StreamSpec::new(
        SOURCE,
        options(ONE),
        SinkSpec::ForeachBatch {
            sink: sink.to_string(),
            body: Arc::clone(body) as Arc<dyn BatchBody>,
        },
    );
    spec.trigger = trigger;
    spec
}

async fn launched(fixture: &Fixture, body: &Arc<Inside>, spec: StreamSpec) -> QueryHandle {
    let handle = StreamingQueryManager::of(&fixture.session)
        .register(&fixture.session, spec)
        .await
        .expect("register");
    *body.own.lock().expect("the slot") = Some(handle.clone());
    handle
}

fn forget(bodies: &[&Arc<Inside>]) {
    for body in bodies {
        body.own.lock().expect("the slot").take();
        body.peer.lock().expect("the slot").take();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_that_stops_its_own_query_ends_it_stopped() {
    for through_the_manager in [false, true] {
        let fixture = Fixture::new().await;
        fixture.insert(SOURCE, "(1)").await;
        fixture.insert(SOURCE, "(2)").await;
        let manager = StreamingQueryManager::of(&fixture.session);
        let metrics = Handle::current().metrics();
        let baseline = metrics.num_alive_tasks();
        let body = Inside::new(if through_the_manager {
            Act::StopAll(Arc::downgrade(&manager))
        } else {
            Act::StopSelf
        });
        let running = Trigger::ProcessingTime(Duration::ZERO);
        let handle = launched(&fixture, &body, spec(&body, SINK, running)).await;
        handle.start_below_catalog_check().expect("start");
        assert_eq!(
            ended(&handle).await,
            Ok(true),
            "stop_all: {through_the_manager}"
        );
        assert_eq!(handle.state(), QueryState::Stopped);
        assert_eq!(
            body.answers(),
            [Answer::Stopped(vec![ShutdownOutcome::Stopped {
                durable: None
            }])],
            "the stop returns without waiting for its own batch"
        );
        assert_eq!(body.calls(), 1);
        assert_eq!(
            handle.durable().map(|record| record.epoch),
            Some(Epoch::FIRST)
        );
        assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
        assert!(manager.active().is_empty());
        forget(&[&body]);
        eventually("the driver task outlived its stop", || {
            metrics.num_alive_tasks() <= baseline
        })
        .await;
        let next = Inside::new(Act::Nothing);
        let restart = launched(&fixture, &next, spec(&next, SINK, Trigger::Once)).await;
        assert_eq!(restart.id(), handle.id());
        restart
            .start_below_catalog_check()
            .expect("the id and the sink are free");
        assert_eq!(ended(&restart).await, Ok(true));
        assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1]);
        forget(&[&next]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_all_from_a_body_waits_for_the_other_queries() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let manager = StreamingQueryManager::of(&fixture.session);
    let idle = Inside::new(Act::Nothing);
    let hourly = Trigger::ProcessingTime(Duration::from_hours(1));
    let other = launched(
        &fixture,
        &idle,
        named(spec(&idle, OTHER_SINK, hourly), "other"),
    )
    .await;
    other.start_below_catalog_check().expect("start");
    wait_for_epoch(&other, 0).await;
    let body = Inside::new(Act::StopAll(Arc::downgrade(&manager)));
    let handle = launched(&fixture, &body, spec(&body, SINK, Trigger::AvailableNow)).await;
    handle.start_below_catalog_check().expect("start");
    assert_eq!(ended(&handle).await, Ok(true));
    let answers = body.answers();
    let [Answer::Stopped(outcomes)] = answers.as_slice() else {
        panic!("{answers:?}");
    };
    assert_eq!(outcomes.len(), 2);
    assert!(
        outcomes.iter().any(|outcome| matches!(
            outcome,
            ShutdownOutcome::Stopped { durable: Some(record) } if record.query == other.id()
        )),
        "the other query was waited for: {outcomes:?}"
    );
    assert_eq!(other.state(), QueryState::Stopped);
    assert_eq!(handle.state(), QueryState::Stopped);
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
    forget(&[&body, &idle]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_bodies_stopping_each_other_both_end_stopped() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let meeting = Arc::new(Barrier::new(2));
    let (left, right) = (
        Inside::new(Act::StopPeer(Arc::clone(&meeting))),
        Inside::new(Act::StopPeer(meeting)),
    );
    let running = Trigger::ProcessingTime(Duration::ZERO);
    let a = launched(&fixture, &left, spec(&left, SINK, running)).await;
    let b = launched(
        &fixture,
        &right,
        named(spec(&right, OTHER_SINK, running), "b"),
    )
    .await;
    *left.peer.lock().expect("the slot") = Some(b.clone());
    *right.peer.lock().expect("the slot") = Some(a.clone());
    a.start_below_catalog_check().expect("start");
    b.start_below_catalog_check().expect("start");
    assert_eq!(ended(&a).await, Ok(true));
    assert_eq!(ended(&b).await, Ok(true));
    assert_eq!(a.state(), QueryState::Stopped);
    assert_eq!(b.state(), QueryState::Stopped);
    assert_eq!((left.calls(), right.calls()), (1, 1));
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
    assert_eq!(stamped_epochs(&fixture.table("other").await), [0]);
    assert!(
        StreamingQueryManager::of(&fixture.session)
            .active()
            .is_empty()
    );
    forget(&[&left, &right]);
}

#[tokio::test]
async fn a_body_awaiting_its_own_termination_is_refused() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    fixture.insert(SOURCE, "(2)").await;
    let body = Inside::new(Act::AwaitSelf);
    let handle = launched(&fixture, &body, spec(&body, SINK, Trigger::AvailableNow)).await;
    handle.start_below_catalog_check().expect("start");
    assert_eq!(ended(&handle).await, Ok(true));
    assert_eq!(
        body.answers(),
        [Answer::Awaited(Err(Arc::new(
            MicroBatchError::AwaitFromDriver { query: handle.id() }
        )))]
    );
    assert_eq!(body.calls(), 2);
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1]);
    forget(&[&body]);
}
