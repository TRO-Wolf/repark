use std::sync::Arc;

use datafusion::prelude::DataFrame;
use futures::future::BoxFuture;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{NamespaceIdent, TableIdent};
use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
use repark_iceberg::microbatch::offset::{Epoch, QUERY_ID_KEY, TableUuid};
use tokio::runtime::Handle;

use crate::Session;
use crate::microbatch::driver::{
    BatchBody, QueryState, ShutdownOutcome, SinkSpec, StreamSpec, Trigger,
};
use crate::microbatch::lifecycle_tests::ONE;
use crate::microbatch::table_door_tests::{ARMED_LANDED, ARMED_LOST, FLAKY_SINK, flaky};
use crate::microbatch::testing::{
    Fixture, SINK, SOURCE, SinkWriter, append_frame, options, stamped_epochs, started,
};

async fn bronze(fixture: &Fixture) {
    fixture.insert(SOURCE, "(1), (2), (3)").await;
    fixture.insert(SOURCE, "(4), (5)").await;
}

fn unstamped_snapshots(fixture_sink: &iceberg::table::Table) -> Vec<i64> {
    fixture_sink
        .metadata()
        .snapshots()
        .filter(|snapshot| {
            !snapshot
                .summary()
                .additional_properties
                .contains_key(QUERY_ID_KEY)
        })
        .map(|snapshot| snapshot.snapshot_id())
        .collect()
}

enum Shape {
    SpawnedOnly,
    SpawnedThenOwn,
    BareInsert,
    PropertyChange,
}

struct Shaped {
    session: Session,
    shape: Shape,
}

impl Shaped {
    fn spec(session: &Session, shape: Shape) -> StreamSpec {
        let body = Arc::new(Shaped {
            session: session.clone(),
            shape,
        });
        let mut spec = StreamSpec::new(
            SOURCE,
            options(ONE),
            SinkSpec::ForeachBatch {
                sink: SINK.to_string(),
                body: body as Arc<dyn BatchBody>,
            },
        );
        spec.trigger = Trigger::AvailableNow;
        spec
    }

    async fn spawned(&self, frame: DataFrame) -> Result<(), MicroBatchError> {
        let session = self.session.clone();
        tokio::spawn(async move { append_frame(&session, SINK, frame).await })
            .await
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))?
    }

    async fn touch_the_sink(&self) -> Result<(), MicroBatchError> {
        let catalog = self
            .session
            .catalogs_snapshot()
            .guarded_in_batch_body()
            .get("ice")
            .cloned()
            .ok_or_else(|| MicroBatchError::Catalog("no catalog".to_string()))?;
        let ident = TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "silver".to_string(),
        );
        let sink = catalog
            .load_table(&ident)
            .await
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
        let tx = Transaction::new(&sink);
        let tx = tx
            .update_table_properties()
            .set("eo.touched".to_string(), "yes".to_string())
            .apply(tx)
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
        tx.commit(catalog.as_ref())
            .await
            .map(|_| ())
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))
    }

    fn statement(&self, text: &str) -> Result<(), MicroBatchError> {
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async {
                self.session
                    .sql(text)
                    .await
                    .map_err(|error| MicroBatchError::Catalog(error.to_string()))?
                    .collect()
                    .await
                    .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
                Ok(())
            })
        })
    }
}

impl BatchBody for Shaped {
    fn run(&self, frame: DataFrame, epoch: Epoch) -> BoxFuture<'_, Result<(), MicroBatchError>> {
        Box::pin(async move {
            match self.shape {
                Shape::SpawnedOnly => self.spawned(frame).await,
                Shape::SpawnedThenOwn => {
                    self.spawned(frame.clone()).await?;
                    append_frame(&self.session, SINK, frame).await
                }
                Shape::BareInsert => {
                    let id = 70 + epoch.get();
                    self.statement(&format!("INSERT INTO {SINK} VALUES ({id})"))
                }
                Shape::PropertyChange => self.touch_the_sink().await,
            }
        })
    }
}

#[tokio::test]
async fn a_second_sink_write_in_one_epoch_refuses_and_the_first_stays_once() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let body = SinkWriter::new(&fixture.session, SINK, 2);
    let handle = started(&fixture, body.spec(&options(ONE))).await;
    let error = handle
        .await_termination(None)
        .await
        .expect_err("the second write refuses");
    assert_eq!(
        *error,
        MicroBatchError::SinkCommittedTwice {
            epoch: Epoch::FIRST
        }
    );
    assert_eq!(handle.state(), QueryState::Failed);
    assert_eq!(
        handle.stop().await.durable().map(|record| record.epoch),
        Some(Epoch::FIRST)
    );
    let sink = fixture.table("silver").await;
    assert_eq!(stamped_epochs(&sink), [0]);
    assert_eq!(sink.metadata().snapshots().count(), 1);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3]);
}

#[tokio::test]
async fn a_body_without_a_sink_write_gets_one_stamp_only_snapshot_per_epoch() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let body = SinkWriter::new(&fixture.session, SINK, 0);
    let handle = started(&fixture, body.spec(&options(ONE))).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(body.calls(), 2);
    let sink = fixture.table("silver").await;
    assert_eq!(stamped_epochs(&sink), [0, 1]);
    assert_eq!(sink.metadata().snapshots().count(), 2);
    assert!(fixture.ids(SINK).await.is_empty());
}

#[tokio::test]
async fn a_sink_write_outside_the_body_scope_with_no_stamped_commit_ends_recovery_required() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let handle = started(&fixture, Shaped::spec(&fixture.session, Shape::SpawnedOnly)).await;
    let error = handle
        .await_termination(None)
        .await
        .expect_err("the unstamped commit is named");
    let sink = fixture.table("silver").await;
    let unstamped = unstamped_snapshots(&sink);
    assert_eq!(unstamped.len(), 1);
    assert!(
        matches!(
            error.as_ref(),
            MicroBatchError::RecoveryRequired {
                epoch,
                durable: None,
                reason: RecoveryReason::UnstampedSinkCommit { snapshot },
                ..
            } if *epoch == Epoch::FIRST && snapshot.get() == unstamped[0]
        ),
        "{error:?}"
    );
    assert_eq!(handle.state(), QueryState::RecoveryRequired);
    assert!(stamped_epochs(&sink).is_empty());
    assert_eq!(sink.metadata().snapshots().count(), 1);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3]);
}

#[tokio::test]
async fn a_foreign_snapshot_beside_a_stamped_epoch_is_tolerated() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let handle = started(
        &fixture,
        Shaped::spec(&fixture.session, Shape::SpawnedThenOwn),
    )
    .await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    let sink = fixture.table("silver").await;
    assert_eq!(stamped_epochs(&sink), [0, 1]);
    assert_eq!(unstamped_snapshots(&sink).len(), 2);
    assert_eq!(fixture.ids(SINK).await, [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]);
}

#[tokio::test]
async fn an_unstampable_commit_to_the_sink_refuses_before_it_lands() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let handle = started(
        &fixture,
        Shaped::spec(&fixture.session, Shape::PropertyChange),
    )
    .await;
    let error = handle
        .await_termination(None)
        .await
        .expect_err("the property change refuses");
    assert!(
        matches!(
            error.as_ref(),
            MicroBatchError::UnstampedSinkWrite { sink, epoch }
                if sink == "sales.silver" && *epoch == Epoch::FIRST
        ),
        "{error:?}"
    );
    assert_eq!(
        handle.stop().await,
        ShutdownOutcome::Failed {
            durable: None,
            error: Arc::clone(&error),
        }
    );
    let sink = fixture.table("silver").await;
    assert_eq!(sink.metadata().snapshots().count(), 0);
    assert!(!sink.metadata().properties().contains_key("eo.touched"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sink_write_the_guard_cannot_see_ends_recovery_required() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let handle = started(&fixture, Shaped::spec(&fixture.session, Shape::BareInsert)).await;
    let error = handle
        .await_termination(None)
        .await
        .expect_err("the unstamped commit is named");
    assert!(
        matches!(
            error.as_ref(),
            MicroBatchError::RecoveryRequired {
                durable: None,
                reason: RecoveryReason::UnstampedSinkCommit { .. },
                ..
            }
        ),
        "{error:?}"
    );
    assert!(stamped_epochs(&fixture.table("silver").await).is_empty());
    assert_eq!(fixture.ids(SINK).await, [70]);
}

#[tokio::test]
async fn an_unknown_outcome_of_the_body_s_commit_that_landed_reconciles_and_the_query_runs_on() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let catalog = flaky(&fixture).await;
    let body = SinkWriter::new(&fixture.session, FLAKY_SINK, 1);
    catalog.arm(ARMED_LANDED);
    let handle = started(&fixture, body.spec(&options(ONE))).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(body.calls(), 2);
    let sink = fixture.table("silver").await;
    assert_eq!(stamped_epochs(&sink), [0, 1]);
    assert_eq!(sink.metadata().snapshots().count(), 2);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn an_unknown_outcome_of_the_body_s_commit_that_never_landed_ends_recovery_required() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let catalog = flaky(&fixture).await;
    let body = SinkWriter::new(&fixture.session, FLAKY_SINK, 1);
    catalog.arm(ARMED_LOST);
    let handle = started(&fixture, body.spec(&options(ONE))).await;
    let error = handle
        .await_termination(None)
        .await
        .expect_err("the outcome stays unknown");
    assert!(
        matches!(
            error.as_ref(),
            MicroBatchError::RecoveryRequired {
                epoch,
                durable: None,
                reason: RecoveryReason::CommitOutcomeUnknown { .. },
                ..
            } if *epoch == Epoch::FIRST
        ),
        "{error:?}"
    );
    assert!(fixture.ids(SINK).await.is_empty());
    let resumed = SinkWriter::new(&fixture.session, FLAKY_SINK, 1);
    let restart = started(&fixture, resumed.spec(&options(ONE))).await;
    assert_eq!(restart.await_termination(None).await, Ok(true));
    assert_eq!(resumed.calls(), 2);
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1]);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn the_body_scope_ends_with_the_batch() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let body = SinkWriter::new(&fixture.session, SINK, 1);
    let handle = started(&fixture, body.spec(&options(ONE))).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    let before = TableUuid::of(&fixture.table("silver").await);
    fixture.insert(SINK, "(9)").await;
    let sink = fixture.table("silver").await;
    assert_eq!(TableUuid::of(&sink), before);
    assert_eq!(unstamped_snapshots(&sink).len(), 1);
    assert_eq!(stamped_epochs(&sink), [0, 1]);
}
