use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::prelude::DataFrame;
use futures::future::BoxFuture;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{NamespaceIdent, TableIdent};
use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
use repark_iceberg::microbatch::offset::{Epoch, QUERY_ID_KEY, TableUuid};
use tokio::runtime::Handle;

use crate::Session;
use crate::microbatch::driver::{
    BatchBody, QueryState, ShutdownOutcome, SinkSpec, StreamSpec, StreamingQueryManager, Trigger,
};
use crate::microbatch::lifecycle_tests::{ONE, named};
use crate::microbatch::table_door_tests::{ARMED_LANDED, ARMED_LOST, FLAKY_SINK, flaky};
use crate::microbatch::testing::{
    Fixture, SINK, SOURCE, SinkWriter, append_frame, mark_the_start, options, stamped_epochs,
    started, table_spec,
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stray {
    SpawnedAppend,
    SpawnedProperty,
    BareInsert,
    ExplainAnalyzeInsert,
    ExplainInsert,
    GuardedProperty,
    Nothing,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Own {
    Nothing,
    AppendAfter,
    AppendBefore,
    RaiseAfter,
}

struct Shaped {
    session: Session,
    stray: Stray,
    at: u64,
    own: Own,
    calls: AtomicUsize,
}

impl Shaped {
    fn new(session: &Session, stray: Stray, at: u64, own: Own) -> Arc<Shaped> {
        Arc::new(Shaped {
            session: session.clone(),
            stray,
            at,
            own,
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn spec(self: &Arc<Self>) -> StreamSpec {
        let mut spec = StreamSpec::new(
            SOURCE,
            options(ONE),
            SinkSpec::ForeachBatch {
                sink: SINK.to_string(),
                body: Arc::clone(self) as Arc<dyn BatchBody>,
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

    async fn spawned_property(&self) -> Result<(), MicroBatchError> {
        let session = self.session.clone();
        tokio::spawn(async move { touch_the_sink(&session).await })
            .await
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))?
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

    async fn stray(&self, frame: DataFrame, epoch: Epoch) -> Result<(), MicroBatchError> {
        let id = 70 + epoch.get();
        match self.stray {
            Stray::Nothing => Ok(()),
            Stray::SpawnedAppend => self.spawned(frame).await,
            Stray::SpawnedProperty => self.spawned_property().await,
            Stray::GuardedProperty => touch_the_sink(&self.session).await,
            Stray::BareInsert => self.statement(&format!("INSERT INTO {SINK} VALUES ({id})")),
            Stray::ExplainAnalyzeInsert => {
                self.statement(&format!("EXPLAIN ANALYZE INSERT INTO {SINK} VALUES ({id})"))
            }
            Stray::ExplainInsert => {
                self.statement(&format!("EXPLAIN INSERT INTO {SINK} VALUES ({id})"))
            }
        }
    }
}

async fn touch_the_sink(session: &Session) -> Result<(), MicroBatchError> {
    let catalog = session
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

impl BatchBody for Shaped {
    fn run(&self, frame: DataFrame, epoch: Epoch) -> BoxFuture<'_, Result<(), MicroBatchError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let hit = epoch.get() == self.at;
            if self.own == Own::AppendBefore || (self.own != Own::Nothing && !hit) {
                append_frame(&self.session, SINK, frame.clone()).await?;
            }
            if hit {
                self.stray(frame.clone(), epoch).await?;
                match self.own {
                    Own::AppendAfter => append_frame(&self.session, SINK, frame).await?,
                    Own::RaiseAfter => {
                        return Err(MicroBatchError::Catalog("the body raised".to_string()));
                    }
                    Own::Nothing | Own::AppendBefore => {}
                }
            }
            Ok(())
        })
    }
}

fn unstamped_commit(error: &MicroBatchError) -> (u64, Option<u64>, i64, String) {
    match error {
        MicroBatchError::RecoveryRequired {
            epoch,
            durable,
            reason:
                RecoveryReason::UnstampedSinkCommit {
                    snapshot,
                    operation: Some(operation),
                },
            ..
        } => (
            epoch.get(),
            durable.as_ref().map(|record| record.epoch.get()),
            snapshot.get(),
            operation.clone(),
        ),
        other => panic!("expected an unstamped commit, got {other:?}"),
    }
}

async fn ended_with(fixture: &Fixture, body: &Arc<Shaped>) -> Arc<MicroBatchError> {
    let handle = started(fixture, body.spec()).await;
    let error = handle
        .await_termination(None)
        .await
        .expect_err("the query ends with an error");
    if matches!(error.as_ref(), MicroBatchError::RecoveryRequired { .. }) {
        assert_eq!(handle.state(), QueryState::RecoveryRequired);
    }
    error
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
async fn a_stray_sink_write_with_no_stamped_commit_ends_recovery_required() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let body = Shaped::new(&fixture.session, Stray::SpawnedAppend, 0, Own::Nothing);
    let error = ended_with(&fixture, &body).await;
    let sink = fixture.table("silver").await;
    let unstamped = unstamped_snapshots(&sink);
    assert_eq!(
        unstamped_commit(&error),
        (0, None, unstamped[0], String::from("append"))
    );
    assert_eq!(unstamped.len(), 1);
    assert!(stamped_epochs(&sink).is_empty());
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3]);
}

#[tokio::test]
async fn a_stray_sink_write_beside_the_stamped_commit_ends_recovery_required() {
    for own in [Own::AppendAfter, Own::AppendBefore] {
        let fixture = Fixture::new().await;
        bronze(&fixture).await;
        let body = Shaped::new(&fixture.session, Stray::SpawnedAppend, 1, own);
        let error = ended_with(&fixture, &body).await;
        let sink = fixture.table("silver").await;
        let unstamped = unstamped_snapshots(&sink);
        assert_eq!(unstamped.len(), 1);
        assert_eq!(
            unstamped_commit(&error),
            (1, Some(0), unstamped[0], String::from("append")),
            "the handle does not record the epoch durable"
        );
        assert_eq!(stamped_epochs(&sink), [0, 1]);
        assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 4, 5, 5]);
        fixture.insert(SOURCE, "(6)").await;
        let resumed = Shaped::new(&fixture.session, Stray::SpawnedAppend, 9, Own::AppendAfter);
        let restart = started(&fixture, resumed.spec()).await;
        let ending = restart.await_termination(None).await;
        if own == Own::AppendAfter {
            assert_eq!(
                ending,
                Ok(true),
                "the stamp is the head: epoch 1 is durable"
            );
            assert_eq!(resumed.calls(), 1);
            assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 4, 5, 5, 6]);
        } else {
            let refused = ending.expect_err("the stray is above the newest stamp");
            assert_eq!(
                unstamped_commit(&refused),
                (2, Some(1), unstamped[0], String::from("append"))
            );
            assert_eq!(resumed.calls(), 0, "a refused restart runs no body");
            assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 4, 5, 5]);
        }
    }
}

#[tokio::test]
async fn a_failed_body_is_audited_and_the_restart_refuses_before_any_body() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let body = Shaped::new(&fixture.session, Stray::SpawnedAppend, 1, Own::RaiseAfter);
    let error = ended_with(&fixture, &body).await;
    let sink = fixture.table("silver").await;
    let stray = unstamped_snapshots(&sink)[0];
    assert_eq!(
        unstamped_commit(&error),
        (1, Some(0), stray, String::from("append"))
    );
    assert_eq!(stamped_epochs(&sink), [0]);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
    for attempt in 0..2 {
        let resumed = Shaped::new(&fixture.session, Stray::SpawnedAppend, 1, Own::AppendAfter);
        let refused = ended_with(&fixture, &resumed).await;
        assert_eq!(
            unstamped_commit(&refused),
            (1, Some(0), stray, String::from("append")),
            "restart {attempt}"
        );
        assert_eq!(resumed.calls(), 0, "restart {attempt} ran a body");
        assert_eq!(
            fixture.ids(SINK).await,
            [1, 2, 3, 4, 5],
            "restart {attempt}"
        );
    }
}

fn offsets_property(sink: &iceberg::table::Table) -> Vec<String> {
    let mut values: Vec<String> = sink
        .metadata()
        .properties()
        .iter()
        .filter(|(key, _)| key.starts_with("repark.cdc.offsets."))
        .map(|(_, value)| value.clone())
        .collect();
    values.sort();
    values
}

#[tokio::test]
async fn at_epoch_zero_the_restart_reads_the_mark_and_refuses_before_any_body() {
    let fixture = Fixture::new().await;
    fixture.insert(SINK, "(90)").await;
    bronze(&fixture).await;
    let body = Shaped::new(&fixture.session, Stray::SpawnedAppend, 0, Own::RaiseAfter);
    let first = unstamped_commit(ended_with(&fixture, &body).await.as_ref());
    assert_eq!((first.0, first.1), (0, None));
    for attempt in 0..2 {
        let resumed = Shaped::new(&fixture.session, Stray::SpawnedAppend, 0, Own::AppendAfter);
        let refused = unstamped_commit(ended_with(&fixture, &resumed).await.as_ref());
        assert_eq!(refused, first, "restart {attempt} names the first stray");
        assert_eq!(resumed.calls(), 0, "restart {attempt} ran a body");
        assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 90], "restart {attempt}");
    }
    assert!(stamped_epochs(&fixture.table("silver").await).is_empty());
}

#[tokio::test]
async fn the_first_start_writes_the_mark_and_the_first_stamp_replaces_it() {
    for seeded in [false, true] {
        let fixture = Fixture::new().await;
        if seeded {
            fixture.insert(SINK, "(90)").await;
        }
        let head = fixture
            .table("silver")
            .await
            .metadata()
            .current_snapshot_id();
        bronze(&fixture).await;
        let failing = Shaped::new(&fixture.session, Stray::Nothing, 0, Own::RaiseAfter);
        let error = ended_with(&fixture, &failing).await;
        assert!(
            matches!(error.as_ref(), MicroBatchError::BatchFailed { .. }),
            "{error:?}"
        );
        let sink = fixture.table("silver").await;
        let starting = head.map_or_else(|| String::from("null"), |id| id.to_string());
        assert_eq!(
            offsets_property(&sink),
            [format!(
                "{{\"format-version\":1,\"pending-epoch\":0,\"starting-head\":{starting}}}"
            )],
            "seeded: {seeded}"
        );
        assert_eq!(sink.metadata().current_snapshot_id(), head);
        let location = sink.metadata_location().map(str::to_string);
        let again = Shaped::new(&fixture.session, Stray::Nothing, 0, Own::RaiseAfter);
        ended_with(&fixture, &again).await;
        assert_eq!(
            fixture
                .table("silver")
                .await
                .metadata_location()
                .map(str::to_string),
            location,
            "a restart that finds the mark writes nothing"
        );
        let healthy = SinkWriter::new(&fixture.session, SINK, 1);
        let handle = started(&fixture, healthy.spec(&options(ONE))).await;
        assert_eq!(handle.await_termination(None).await, Ok(true));
        let sink = fixture.table("silver").await;
        assert_eq!(stamped_epochs(&sink), [0, 1]);
        let values = offsets_property(&sink);
        assert_eq!(values.len(), 1);
        assert!(!values[0].contains("pending-epoch"), "{values:?}");
        let expected: Vec<i64> = if seeded {
            vec![1, 2, 3, 4, 5, 90]
        } else {
            vec![1, 2, 3, 4, 5]
        };
        assert_eq!(fixture.ids(SINK).await, expected);
    }
}

#[tokio::test]
async fn a_start_with_no_mark_and_no_stamp_takes_the_head_it_finds_and_marks_it() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    fixture.insert(SINK, "(70)").await;
    fixture.insert(SINK, "(71)").await;
    let head = fixture
        .table("silver")
        .await
        .metadata()
        .current_snapshot_id()
        .expect("the rows a markless run left");
    let body = SinkWriter::new(&fixture.session, SINK, 1);
    let handle = started(&fixture, body.spec(&options(ONE))).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(body.calls(), 2);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5, 70, 71]);
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    fixture.insert(SINK, "(70)").await;
    let head_before = fixture
        .table("silver")
        .await
        .metadata()
        .current_snapshot_id()
        .expect("a head");
    assert_ne!(head, head_before);
    let failing = Shaped::new(&fixture.session, Stray::Nothing, 0, Own::RaiseAfter);
    ended_with(&fixture, &failing).await;
    assert_eq!(
        offsets_property(&fixture.table("silver").await),
        [format!(
            "{{\"format-version\":1,\"pending-epoch\":0,\"starting-head\":{head_before}}}"
        )]
    );
}

#[tokio::test]
async fn two_query_names_on_one_sink_each_keep_their_own_mark_and_stamps() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let first = SinkWriter::new(&fixture.session, SINK, 1);
    let a = started(&fixture, named(first.spec(&options(ONE)), "a")).await;
    assert_eq!(a.await_termination(None).await, Ok(true));
    let failing = Shaped::new(&fixture.session, Stray::Nothing, 0, Own::RaiseAfter);
    let b = started(&fixture, named(failing.spec(), "b")).await;
    assert!(b.await_termination(None).await.is_err());
    assert_ne!(a.id(), b.id());
    let values = offsets_property(&fixture.table("silver").await);
    assert_eq!(values.len(), 2);
    assert_eq!(
        values
            .iter()
            .filter(|value| value.contains("pending-epoch"))
            .count(),
        1,
        "{values:?}"
    );
    let second = SinkWriter::new(&fixture.session, SINK, 1);
    let b = started(&fixture, named(second.spec(&options(ONE)), "b")).await;
    assert_eq!(b.await_termination(None).await, Ok(true));
    fixture.insert(SOURCE, "(6)").await;
    let resumed = SinkWriter::new(&fixture.session, SINK, 1);
    let a = started(&fixture, named(resumed.spec(&options(ONE)), "a")).await;
    assert_eq!(a.await_termination(None).await, Ok(true));
    assert_eq!(resumed.calls(), 1);
    assert_eq!(fixture.ids(SINK).await, [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6]);
    let values = offsets_property(&fixture.table("silver").await);
    assert!(values.iter().all(|value| !value.contains("pending-epoch")));
}

#[tokio::test]
async fn the_table_door_writes_no_mark() {
    let fixture = Fixture::new().await;
    let handle = started(&fixture, table_spec(Trigger::AvailableNow, &options(ONE))).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    let sink = fixture.table("silver").await;
    assert!(offsets_property(&sink).is_empty());
    assert_eq!(sink.metadata().metadata_log().len(), 0);
    let idle = SinkWriter::new(&fixture.session, SINK, 1);
    let foreach = started(&fixture, named(idle.spec(&options(ONE)), "f")).await;
    assert_eq!(foreach.await_termination(None).await, Ok(true));
    assert_eq!(idle.calls(), 0);
    let values = offsets_property(&fixture.table("silver").await);
    assert_eq!(
        values,
        ["{\"format-version\":1,\"pending-epoch\":0,\"starting-head\":null}"]
    );
}

#[tokio::test]
async fn rows_already_in_the_sink_and_a_seeded_restart_are_not_violations() {
    let fixture = Fixture::new().await;
    fixture.insert(SINK, "(90)").await;
    fixture.insert(SINK, "(91)").await;
    bronze(&fixture).await;
    let body = SinkWriter::new(&fixture.session, SINK, 1);
    let handle = started(&fixture, body.spec(&options(ONE))).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    fixture.insert(SOURCE, "(6)").await;
    let resumed = SinkWriter::new(&fixture.session, SINK, 1);
    let restart = started(&fixture, resumed.spec(&options(ONE))).await;
    assert_eq!(restart.await_termination(None).await, Ok(true));
    assert_eq!(resumed.calls(), 1);
    let sink = fixture.table("silver").await;
    assert_eq!(stamped_epochs(&sink), [0, 1, 2]);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5, 6, 90, 91]);
}

#[tokio::test]
async fn a_foreign_snapshot_between_runs_refuses_the_restart() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let body = SinkWriter::new(&fixture.session, SINK, 1);
    let handle = started(&fixture, body.spec(&options(ONE))).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    fixture.insert(SINK, "(99)").await;
    fixture.insert(SOURCE, "(6)").await;
    let foreign = fixture
        .table("silver")
        .await
        .metadata()
        .current_snapshot_id()
        .expect("the foreign head");
    let resumed = SinkWriter::new(&fixture.session, SINK, 1);
    let restart = started(&fixture, resumed.spec(&options(ONE))).await;
    let refused = restart
        .await_termination(None)
        .await
        .expect_err("the head is not the newest stamp");
    assert_eq!(
        unstamped_commit(&refused),
        (2, Some(1), foreign, String::from("append"))
    );
    assert_eq!(resumed.calls(), 0);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5, 99]);
}

#[tokio::test]
async fn a_property_change_beside_the_stamped_commit_ends_recovery_required() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let body = Shaped::new(
        &fixture.session,
        Stray::SpawnedProperty,
        1,
        Own::AppendAfter,
    );
    let error = ended_with(&fixture, &body).await;
    assert!(
        matches!(
            error.as_ref(),
            MicroBatchError::RecoveryRequired {
                epoch,
                reason: RecoveryReason::UnstampedSinkChange { what },
                ..
            } if epoch.get() == 1 && what == "table property eo.touched changed"
        ),
        "{error:?}"
    );
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1]);
}

#[tokio::test]
async fn an_unstampable_commit_to_the_sink_refuses_before_it_lands() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let body = Shaped::new(&fixture.session, Stray::GuardedProperty, 0, Own::Nothing);
    let handle = started(&fixture, body.spec()).await;
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
async fn a_planned_write_to_the_sink_refuses_before_it_lands_and_a_plain_explain_passes() {
    for stray in [Stray::BareInsert, Stray::ExplainAnalyzeInsert] {
        let fixture = Fixture::new().await;
        bronze(&fixture).await;
        let body = Shaped::new(&fixture.session, stray, 0, Own::AppendAfter);
        let error = ended_with(&fixture, &body).await;
        assert!(
            matches!(
                error.as_ref(),
                MicroBatchError::UnstampedSinkWrite { sink, epoch }
                    if sink == "sales.silver" && *epoch == Epoch::FIRST
            ),
            "{error:?}"
        );
        assert_eq!(
            fixture.table("silver").await.metadata().snapshots().count(),
            0
        );
    }
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let body = Shaped::new(&fixture.session, Stray::ExplainInsert, 0, Own::AppendAfter);
    let handle = started(&fixture, body.spec()).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1]);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn an_unknown_outcome_of_the_body_s_commit_that_landed_reconciles_and_the_query_runs_on() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let catalog = flaky(&fixture).await;
    let body = SinkWriter::new(&fixture.session, FLAKY_SINK, 1);
    let handle = StreamingQueryManager::of(&fixture.session)
        .register(&fixture.session, body.spec(&options(ONE)))
        .await
        .expect("register");
    mark_the_start(&fixture, &handle).await;
    catalog.arm(ARMED_LANDED);
    handle.start_below_catalog_check().expect("start");
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
    let handle = StreamingQueryManager::of(&fixture.session)
        .register(&fixture.session, body.spec(&options(ONE)))
        .await
        .expect("register");
    mark_the_start(&fixture, &handle).await;
    catalog.arm(ARMED_LOST);
    handle.start_below_catalog_check().expect("start");
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
