use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::DataFusionError;
use datafusion::prelude::DataFrame;
use futures::future::BoxFuture;
use iceberg::{NamespaceIdent, TableIdent};
use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
use repark_iceberg::microbatch::offset::{
    EPOCH_KEY, Epoch, QUERY_ID_KEY, SPARK_EPOCH_ID_KEY, SPARK_QUERY_ID_KEY,
};
use repark_iceberg::write::sink_offsets::SCOPE_TOKEN_KEY;
use repark_iceberg::write::{
    SESSION_SNAPSHOT_PREFIX, commit_append_with_summary, concurrency_from_ctx,
    resolve_empty_session_write, stage_overwrite_files_with,
};
use tokio::sync::Notify;

use crate::Session;
use crate::microbatch::driver::{
    BatchBody, QueryState, ShutdownOutcome, SinkSpec, StreamSpec, Trigger,
};
use crate::microbatch::testing::{
    Fixture, SINK, SOURCE, ids_of, options, stamped_epochs, started, table_spec, wait_for_epoch,
};
use crate::time_travel::microbatch_source::SourceOptions;

#[derive(Default)]
struct Plan {
    sink_writes: usize,
    fail_at: Option<u64>,
    fail_after_write: bool,
    hold_at: Option<(u64, Arc<Notify>)>,
    source_append_at: Option<u64>,
}

type Seen = Arc<Mutex<Vec<(u64, Vec<i64>)>>>;

struct Body {
    session: Session,
    plan: Plan,
    seen: Seen,
    leaked_token: Arc<Mutex<bool>>,
}

impl Body {
    fn new(session: &Session, plan: Plan) -> Arc<Body> {
        Arc::new(Body {
            session: session.clone(),
            plan,
            seen: Arc::default(),
            leaked_token: Arc::default(),
        })
    }

    fn seen(&self) -> Vec<(u64, Vec<i64>)> {
        self.seen.lock().expect("seen").clone()
    }

    async fn append(&self, table: &str, frame: DataFrame) -> Result<(), MicroBatchError> {
        let context = self.session.context();
        let catalog = self
            .session
            .catalogs_snapshot()
            .get("ice")
            .cloned()
            .ok_or_else(|| MicroBatchError::Catalog("no catalog".to_string()))?;
        let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
        let target = catalog
            .load_table(&ident)
            .await
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
        let (extra, staging) =
            resolve_empty_session_write(context).map_err(|error| engine(&error))?;
        let stream = frame
            .execute_stream()
            .await
            .map_err(|error| engine(&error))?;
        let files = stage_overwrite_files_with(
            &target,
            stream,
            Vec::new(),
            concurrency_from_ctx(context),
            &staging,
        )
        .await
        .map_err(|error| engine(&error))?;
        commit_append_with_summary(&catalog, &target, files, &extra, None)
            .await
            .map_err(|error| engine(&error))?;
        Ok(())
    }

    async fn run_batch(&self, frame: DataFrame, epoch: Epoch) -> Result<(), MicroBatchError> {
        let rows = frame
            .clone()
            .collect()
            .await
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
        let ids = ids_of(&rows);
        self.seen
            .lock()
            .expect("seen")
            .push((epoch.get(), ids.clone()));
        let token_key = format!("{SESSION_SNAPSHOT_PREFIX}{SCOPE_TOKEN_KEY}");
        if self
            .session
            .context()
            .copied_config()
            .options()
            .entries()
            .iter()
            .any(|entry| entry.key.contains(&token_key))
            || repark_iceberg::write::session_write_conf_from_ctx(self.session.context())
                .merged_snapshot_extra(&[])
                .iter()
                .any(|(key, _)| key == SCOPE_TOKEN_KEY)
        {
            *self.leaked_token.lock().expect("leak flag") = true;
        }
        if self.plan.source_append_at == Some(epoch.get()) {
            let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![100]))])
                .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
            let frame = self
                .session
                .context()
                .read_batch(batch)
                .map_err(|error| engine(&error))?;
            self.append("orders", frame).await?;
        }
        if let Some((at, gate)) = &self.plan.hold_at
            && *at == epoch.get()
        {
            gate.notified().await;
        }
        for _ in 0..self.plan.sink_writes {
            self.append("silver", frame.clone()).await?;
            if self.plan.fail_after_write && self.plan.fail_at == Some(epoch.get()) {
                return Err(MicroBatchError::Catalog(
                    "body failed after its sink write".to_string(),
                ));
            }
        }
        if self.plan.fail_at == Some(epoch.get()) {
            return Err(MicroBatchError::Catalog(
                "body failed at http://user:hunter2@example.com/x".to_string(),
            ));
        }
        Ok(())
    }
}

impl BatchBody for Body {
    fn run(&self, frame: DataFrame, epoch: Epoch) -> BoxFuture<'_, Result<(), MicroBatchError>> {
        Box::pin(self.run_batch(frame, epoch))
    }
}

fn engine(error: &DataFusionError) -> MicroBatchError {
    MicroBatchError::Catalog(error.to_string())
}

fn foreach_spec(body: &Arc<Body>, trigger: Trigger, source: &SourceOptions) -> StreamSpec {
    let mut spec = StreamSpec::new(
        SOURCE,
        source.clone(),
        SinkSpec::ForeachBatch {
            sink: SINK.to_string(),
            body: Arc::clone(body) as Arc<dyn BatchBody>,
        },
    );
    spec.trigger = trigger;
    spec
}

async fn bronze(fixture: &Fixture) {
    fixture.insert(SOURCE, "(1), (2), (3)").await;
    fixture.insert(SOURCE, "(4), (5)").await;
}

fn capped() -> SourceOptions {
    options(&[("streaming-max-files-per-micro-batch", "1")])
}

#[tokio::test]
async fn the_foreach_batch_door_stamps_once_after_the_body() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let body = Body::new(
        &fixture.session,
        Plan {
            sink_writes: 2,
            ..Plan::default()
        },
    );
    let handle = started(
        &fixture,
        foreach_spec(&body, Trigger::AvailableNow, &capped()),
    )
    .await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(body.seen(), [(0, vec![1, 2, 3]), (1, vec![4, 5])]);
    assert!(!*body.leaked_token.lock().expect("leak flag"));
    let sink = fixture.table("silver").await;
    assert_eq!(stamped_epochs(&sink), [0, 1]);
    let metadata = sink.metadata();
    let mut cursor = metadata.current_snapshot();
    let mut kinds = Vec::new();
    while let Some(snapshot) = cursor {
        let summary = &snapshot.summary().additional_properties;
        assert!(!summary.contains_key(SPARK_QUERY_ID_KEY));
        assert!(!summary.contains_key(SPARK_EPOCH_ID_KEY));
        assert!(!summary.contains_key(SCOPE_TOKEN_KEY));
        if let Some(epoch) = summary.get(EPOCH_KEY) {
            kinds.push(format!("stamp {epoch}"));
        } else {
            assert!(!summary.contains_key(QUERY_ID_KEY));
            kinds.push("data".to_string());
        }
        cursor = snapshot
            .parent_snapshot_id()
            .and_then(|parent| metadata.snapshot_by_id(parent));
    }
    kinds.reverse();
    assert_eq!(
        kinds,
        ["data", "data", "stamp 0", "data", "data", "stamp 1"]
    );
    assert_eq!(fixture.ids(SINK).await, [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]);
}

#[tokio::test]
async fn a_failed_body_fails_the_query_and_the_restart_replays_its_epoch() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let failing = Body::new(
        &fixture.session,
        Plan {
            sink_writes: 1,
            fail_at: Some(1),
            ..Plan::default()
        },
    );
    let handle = started(
        &fixture,
        foreach_spec(&failing, Trigger::AvailableNow, &capped()),
    )
    .await;
    let error = handle
        .await_termination(None)
        .await
        .expect_err("the body raised");
    let MicroBatchError::BatchFailed { epoch, cause } = error.as_ref() else {
        panic!("{error:?}");
    };
    assert_eq!(epoch.get(), 1);
    assert!(cause.contains("body failed at"), "{cause}");
    assert!(!cause.contains("hunter2"), "{cause}");
    assert_eq!(handle.state(), QueryState::Failed);
    assert_eq!(handle.exception().as_deref(), Some(error.as_ref()));
    let outcome = handle.stop().await;
    assert!(matches!(outcome, ShutdownOutcome::Failed { .. }));
    assert_eq!(outcome.durable().map(|record| record.epoch.get()), Some(0));
    let healthy = Body::new(
        &fixture.session,
        Plan {
            sink_writes: 1,
            ..Plan::default()
        },
    );
    let restart = started(
        &fixture,
        foreach_spec(&healthy, Trigger::AvailableNow, &capped()),
    )
    .await;
    assert_eq!(restart.await_termination(None).await, Ok(true));
    assert_eq!(healthy.seen(), [(1, vec![4, 5])]);
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1]);
}

#[tokio::test]
async fn a_body_that_dies_after_its_sink_write_replays_at_least_once() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let failing = Body::new(
        &fixture.session,
        Plan {
            sink_writes: 1,
            fail_at: Some(0),
            fail_after_write: true,
            ..Plan::default()
        },
    );
    let handle = started(
        &fixture,
        foreach_spec(&failing, Trigger::AvailableNow, &options(&[])),
    )
    .await;
    assert!(handle.await_termination(None).await.is_err());
    assert!(stamped_epochs(&fixture.table("silver").await).is_empty());
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
    let healthy = Body::new(
        &fixture.session,
        Plan {
            sink_writes: 1,
            ..Plan::default()
        },
    );
    let restart = started(
        &fixture,
        foreach_spec(&healthy, Trigger::AvailableNow, &options(&[])),
    )
    .await;
    assert_eq!(restart.await_termination(None).await, Ok(true));
    assert_eq!(healthy.seen(), [(0, vec![1, 2, 3, 4, 5])]);
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
    assert_eq!(
        fixture.ids(SINK).await,
        [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        "foreachBatch is at-least-once across a crash between the body's write and the stamp"
    );
}

#[tokio::test]
async fn stop_waits_for_the_in_flight_batch() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let gate = Arc::new(Notify::new());
    let body = Body::new(
        &fixture.session,
        Plan {
            sink_writes: 1,
            hold_at: Some((0, Arc::clone(&gate))),
            ..Plan::default()
        },
    );
    let handle = started(
        &fixture,
        foreach_spec(&body, Trigger::ProcessingTime(Duration::ZERO), &capped()),
    )
    .await;
    while body.seen().is_empty() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stopping = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.stop().await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(handle.state(), QueryState::Draining);
    assert!(handle.is_active());
    gate.notify_one();
    let outcome = stopping.await.expect("stop joins");
    assert!(
        matches!(outcome, ShutdownOutcome::Stopped { .. }),
        "{outcome:?}"
    );
    assert_eq!(outcome.durable().map(|record| record.epoch.get()), Some(0));
    assert_eq!(body.seen().len(), 1, "no batch starts after stop");
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
}

#[tokio::test]
async fn a_stop_timeout_yields_recovery_required_with_the_durable_offset() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let gate = Arc::new(Notify::new());
    let body = Body::new(
        &fixture.session,
        Plan {
            sink_writes: 1,
            hold_at: Some((1, Arc::clone(&gate))),
            ..Plan::default()
        },
    );
    let mut spec = foreach_spec(&body, Trigger::ProcessingTime(Duration::ZERO), &capped());
    spec.stop_timeout = Some(Duration::from_millis(50));
    let handle = started(&fixture, spec).await;
    wait_for_epoch(&handle, 0).await;
    while body.seen().len() < 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let outcome = handle.stop().await;
    let ShutdownOutcome::RecoveryRequired { durable, reason } = &outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(
        reason,
        &RecoveryReason::StopTimeout {
            waited: Duration::from_millis(50)
        }
    );
    assert_eq!(durable.as_ref().map(|record| record.epoch.get()), Some(0));
    assert_eq!(handle.state(), QueryState::RecoveryRequired);
    let error = handle
        .await_termination(None)
        .await
        .expect_err("recovery required raises");
    assert!(
        matches!(
            error.as_ref(),
            MicroBatchError::RecoveryRequired { epoch, reason: RecoveryReason::StopTimeout { .. }, durable: Some(_), .. }
                if epoch.get() == 1
        ),
        "{error:?}"
    );
    gate.notify_one();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
    assert_eq!(handle.stop().await, outcome);
}

#[tokio::test]
async fn available_now_fixes_its_end_at_start() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let body = Body::new(
        &fixture.session,
        Plan {
            source_append_at: Some(0),
            ..Plan::default()
        },
    );
    let handle = started(
        &fixture,
        foreach_spec(&body, Trigger::AvailableNow, &options(&[])),
    )
    .await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(body.seen(), [(0, vec![1, 2, 3, 4, 5])]);
    let capped_body = Body::new(
        &fixture.session,
        Plan {
            source_append_at: Some(1),
            ..Plan::default()
        },
    );
    let mut spec = foreach_spec(
        &capped_body,
        Trigger::AvailableNow,
        &options(&[("streaming-max-rows-per-micro-batch", "1")]),
    );
    spec.query_name = Some("second".to_string());
    let second = started(&fixture, spec).await;
    assert_eq!(second.await_termination(None).await, Ok(true));
    assert_eq!(
        capped_body.seen(),
        [(0, vec![1, 2, 3]), (1, vec![4, 5]), (2, vec![100])]
    );
}

#[tokio::test]
async fn the_table_door_keeps_the_token_out_of_the_shared_session() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let handle = started(&fixture, table_spec(Trigger::AvailableNow, &options(&[]))).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    let shared = repark_iceberg::write::session_write_conf_from_ctx(fixture.session.context());
    assert!(
        !shared
            .merged_snapshot_extra(&[])
            .iter()
            .any(|(key, _)| key == SCOPE_TOKEN_KEY)
    );
    fixture.insert(SINK, "(42)").await;
    let sink = fixture.table("silver").await;
    let head = sink.metadata().current_snapshot().expect("a head");
    assert!(
        !head
            .summary()
            .additional_properties
            .contains_key(QUERY_ID_KEY)
    );
    assert_eq!(stamped_epochs(&sink), [0]);
}
