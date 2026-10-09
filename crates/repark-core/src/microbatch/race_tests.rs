use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use repark_common::Generation;
use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
use repark_iceberg::microbatch::offset::{
    EPOCH_KEY, Epoch, OffsetFormatVersion, OffsetVector, QUERY_ID_KEY, QueryId, RUN_ID_KEY, RunId,
    SinkRecord,
};
use repark_iceberg::microbatch::window::WindowLimit;
use tokio::runtime::Handle;

use crate::Session;
use crate::microbatch::driver::{
    BatchBody, QueryHandle, QueryState, ShutdownOutcome, SinkSpec, StreamSpec,
    StreamingQueryManager, Trigger,
};
use crate::microbatch::lifecycle_tests::{Mode, ONE, Probe, ended, named};
use crate::microbatch::table_door_tests::{FLAKY_SINK, FaultCatalog, LoadHook, flaky};
use crate::microbatch::testing::{Fixture, SINK, SOURCE, SinkWriter, options, stamped_epochs};
use crate::time_travel::microbatch_source::MicroBatchSource;

const ITERATIONS: usize = 50;
const FENCE_FLOOR: usize = 10;
const BODY_FLOOR: usize = 0;
const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
const SLOWEST_LOAD_MILLIS: u64 = 12;
const SLOW_SINK: &str = "slow.sales.silver";
const BATCHES: u64 = 3;
const ROWS_PER_BATCH: u64 = 2;
const ADDED_FILES_KEY: &str = "added-data-files";

#[derive(Clone, Copy)]
enum Raced {
    Table,
    ForeachBatch,
}

struct SlowCatalog {
    inner: Arc<dyn Catalog>,
    state: AtomicU64,
}

impl std::fmt::Debug for SlowCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SlowCatalog")
            .finish_non_exhaustive()
    }
}

impl SlowCatalog {
    fn next_pause(&self) -> Duration {
        let step = |state: u64| {
            let state = state ^ (state << 13);
            let state = state ^ (state >> 7);
            state ^ (state << 17)
        };
        let drawn = self
            .state
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |state| {
                Some(step(state))
            })
            .map_or(0, step);
        Duration::from_millis(drawn % (SLOWEST_LOAD_MILLIS + 1))
    }
}

#[async_trait]
impl Catalog for SlowCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        let loaded = self.inner.load_table(table).await?;
        if table.name() == "silver" {
            tokio::time::sleep(self.next_pause()).await;
        }
        Ok(loaded)
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(commit).await
    }
}

async fn slowed(session: &Session, seed: u64) {
    let inner = session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("the memory catalog");
    let catalog = Arc::new(SlowCatalog {
        inner,
        state: AtomicU64::new(seed | 1),
    });
    session
        .register_iceberg_catalog("slow", catalog as Arc<dyn Catalog>)
        .await
        .expect("the slowed catalog registers");
}

fn racing_spec(session: &Session, door: Raced, name: &str) -> StreamSpec {
    let mut spec = match door {
        Raced::Table => {
            let sink = SinkSpec::Table {
                sink: SLOW_SINK.to_string(),
            };
            StreamSpec::new(SOURCE, options(ONE), sink)
        }
        Raced::ForeachBatch => SinkWriter::new(session, SLOW_SINK, 1).spec(&options(ONE)),
    };
    spec.trigger = Trigger::AvailableNow;
    spec.polling_delay = Duration::ZERO;
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

fn start_on_a_barrier(drivers: [&QueryHandle; 2]) {
    let barrier = Arc::new(Barrier::new(drivers.len()));
    let starters = drivers.map(|driver| {
        let (driver, barrier, runtime) = (driver.clone(), Arc::clone(&barrier), Handle::current());
        std::thread::spawn(move || {
            let _entered = runtime.enter();
            barrier.wait();
            driver.start_below_catalog_check().expect("start");
        })
    });
    for starter in starters {
        starter.join().expect("the starter joins");
    }
}

fn history_of(table: &Table, query: QueryId) -> Vec<(u64, String)> {
    let mut history: Vec<(u64, String)> = table
        .metadata()
        .snapshots()
        .filter_map(|snapshot| {
            let summary = &snapshot.summary().additional_properties;
            (summary.get(QUERY_ID_KEY) == Some(&query.to_string())).then(|| {
                let epoch = summary.get(EPOCH_KEY).expect("a stamp carries its epoch");
                let run = summary.get(RUN_ID_KEY).expect("a stamp carries its run");
                (epoch.parse().expect("an epoch is a number"), run.clone())
            })
        })
        .collect();
    history.sort();
    history
}

fn committed_files(table: &Table) -> usize {
    table
        .metadata()
        .snapshots()
        .map(|snapshot| {
            snapshot
                .summary()
                .additional_properties
                .get(ADDED_FILES_KEY)
                .map_or(0, |added| added.parse().expect("a file count is a number"))
        })
        .sum()
}

fn staged_files(directory: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return 0;
    };
    entries
        .map(|entry| entry.expect("a directory entry").path())
        .map(|path| {
            if path.is_dir() {
                staged_files(&path)
            } else {
                usize::from(path.extension().is_some_and(|kind| kind == "parquet"))
            }
        })
        .sum()
}

fn lost_to(ending: &Result<bool, Arc<MicroBatchError>>, winner: RunId) -> bool {
    match ending.as_ref().map_err(AsRef::as_ref) {
        Err(MicroBatchError::Fenced { winner: named, .. }) => *named == winner,
        Err(MicroBatchError::RecoveryRequired {
            durable: Some(record),
            ..
        }) => record.run == winner,
        _ => false,
    }
}

async fn race_once(iteration: usize, door: Raced) -> bool {
    let fixture = Fixture::new().await;
    let seed = SEED.wrapping_mul(u64::try_from(iteration).expect("a small count") + 1);
    let every_row: Vec<i64> = (1..=BATCHES * ROWS_PER_BATCH)
        .map(|id| i64::try_from(id).expect("a small id"))
        .collect();
    for file in every_row.chunks(usize::try_from(ROWS_PER_BATCH).expect("a small count")) {
        let values: Vec<String> = file.iter().map(|id| format!("({id})")).collect();
        fixture.insert(SOURCE, &values.join(", ")).await;
    }
    let other = second_session(&fixture).await;
    slowed(&fixture.session, seed).await;
    slowed(&other, seed.rotate_left(32)).await;
    let a = registered(
        &fixture.session,
        racing_spec(&fixture.session, door, "raced"),
    )
    .await;
    let b = registered(&other, racing_spec(&other, door, "raced")).await;
    assert_eq!(a.id(), b.id());
    assert_ne!(a.run_id(), b.run_id());
    start_on_a_barrier([&a, &b]);
    let (ended_a, ended_b) = (ended(&a).await, ended(&b).await);
    let sink = fixture.table("silver").await;
    let history = history_of(&sink, a.id());
    let landed = fixture.ids(SINK).await;
    let context = format!(
        "iteration {iteration}, seed {seed:#x}: a {run_a} ended {ended_a:?}, b {run_b} ended {ended_b:?}, \
         sink rows {landed:?}, summary history {history:?}",
        run_a = a.run_id(),
        run_b = b.run_id()
    );
    assert_eq!(
        landed, every_row,
        "a row was duplicated or dropped: {context}"
    );
    let every_epoch: Vec<u64> = (0..BATCHES).collect();
    let stamped: Vec<u64> = history.iter().map(|(epoch, _)| *epoch).collect();
    assert_eq!(
        stamped, every_epoch,
        "an epoch was stamped twice or never: {context}"
    );
    assert_eq!(stamped_epochs(&sink), every_epoch, "{context}");
    let runs: BTreeSet<&str> = history.iter().map(|(_, run)| run.as_str()).collect();
    assert_eq!(runs.len(), 1, "two runs both committed: {context}");
    let (winner, winner_ended, loser_ended) = if runs.contains(a.run_id().to_string().as_str()) {
        (a.run_id(), &ended_a, &ended_b)
    } else {
        (b.run_id(), &ended_b, &ended_a)
    };
    assert_eq!(
        *winner_ended,
        Ok(true),
        "the winner did not drain: {context}"
    );
    assert!(
        *loser_ended == Ok(true) || lost_to(loser_ended, winner),
        "the loser does not name the winner: {context}"
    );
    let silver = fixture.warehouse.path().join("sales").join("silver");
    let (staged, committed) = (staged_files(&silver), committed_files(&sink));
    assert!(
        staged >= committed,
        "{staged} staged, {committed} committed: {context}"
    );
    staged > committed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sessions_racing_one_query_land_every_row_exactly_once() {
    let mut refused_at_the_commit = 0usize;
    for iteration in 0..ITERATIONS {
        refused_at_the_commit += usize::from(race_once(iteration, Raced::Table).await);
    }
    assert!(
        refused_at_the_commit >= FENCE_FLOOR,
        "{refused_at_the_commit} of {ITERATIONS} iterations reached the append fence, under the floor of {FENCE_FLOOR}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sessions_racing_foreach_bodies_land_every_row_exactly_once() {
    let mut refused_after_staging = 0usize;
    for iteration in 0..ITERATIONS {
        refused_after_staging += usize::from(race_once(iteration, Raced::ForeachBatch).await);
    }
    assert!(
        refused_after_staging >= BODY_FLOOR,
        "{refused_after_staging} of {ITERATIONS} iterations refused a body's staged write, under the floor of {BODY_FLOOR}"
    );
}

fn recovering_spec(body: Option<&Arc<Probe>>) -> StreamSpec {
    let sink = FLAKY_SINK.to_string();
    let sink = match body {
        Some(body) => SinkSpec::ForeachBatch {
            sink,
            body: Arc::clone(body) as Arc<dyn BatchBody>,
        },
        None => SinkSpec::Table { sink },
    };
    let mut spec = StreamSpec::new(SOURCE, options(ONE), sink);
    spec.trigger = Trigger::AvailableNow;
    named(spec, "raced")
}

async fn first_epoch_of(fixture: &Fixture, query: QueryId, run: RunId) -> SinkRecord {
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
    SinkRecord {
        format: OffsetFormatVersion::CURRENT,
        query,
        run,
        epoch: Epoch::FIRST,
        generation: Generation::new(1).expect("generation 1"),
        offsets: OffsetVector::single(window.end),
    }
}

async fn set_the_offsets_property(inner: &Arc<dyn Catalog>, record: &SinkRecord) {
    let silver = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "silver".to_string(),
    );
    let sink = inner.load_table(&silver).await.expect("the sink loads");
    let (key, value) = record.property().expect("the offsets property");
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

fn property_race(inner: &Arc<dyn Catalog>, record: &SinkRecord, nth: usize) -> LoadHook {
    let (inner, record) = (Arc::clone(inner), record.clone());
    let seen = Arc::new(AtomicUsize::new(0));
    Arc::new(move |table: &TableIdent| {
        let fires = table.name() == "silver" && seen.fetch_add(1, Ordering::SeqCst) == nth;
        let (inner, record) = (Arc::clone(&inner), record.clone());
        Box::pin(async move {
            if fires {
                set_the_offsets_property(&inner, &record).await;
            }
        })
    })
}

async fn restarted_session(inner: &Arc<dyn Catalog>, flaky: &Arc<FaultCatalog>) -> Session {
    let session = Session::builder().build().expect("a restarted session");
    session
        .register_iceberg_catalog("ice", Arc::clone(inner))
        .await
        .expect("the shared catalog registers");
    session
        .register_iceberg_catalog("flaky", Arc::clone(flaky) as Arc<dyn Catalog>)
        .await
        .expect("the wrapped catalog registers");
    session
}

#[tokio::test]
async fn a_restart_after_the_fences_recovery_required_ending_refuses_by_name() {
    for foreach in [false, true] {
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
        let first = registered(&fixture.session, recovering_spec(foreach.then_some(&body))).await;
        let racer = first_epoch_of(&fixture, first.id(), RunId::fresh()).await;
        let refresh_inside_the_commit = if foreach { 3 } else { 2 };
        catalog.on_load(Some(property_race(
            &inner,
            &racer,
            refresh_inside_the_commit,
        )));
        first.start_below_catalog_check().expect("start");
        let ending = ended(&first).await.expect_err("the fence refuses");
        catalog.on_load(None);
        assert!(
            matches!(
                ending.as_ref(),
                MicroBatchError::RecoveryRequired { epoch, .. } if *epoch == Epoch::FIRST
            ),
            "foreachBatch: {foreach}: {ending:?}"
        );
        assert_eq!(first.state(), QueryState::RecoveryRequired);
        assert_eq!(body.calls(), usize::from(foreach));

        let refusal = MicroBatchError::RecoveryRequired {
            query: first.id(),
            epoch: Epoch::FIRST,
            durable: Some(Box::new(racer.clone())),
            reason: RecoveryReason::StampedSnapshotExpired,
        };
        for attempt in 0..2 {
            let context = format!("foreachBatch: {foreach}, restart {attempt}");
            let session = restarted_session(&inner, &catalog).await;
            let resumed = Probe::new(Mode::Record);
            let restart = registered(&session, recovering_spec(foreach.then_some(&resumed))).await;
            assert_eq!(restart.id(), first.id(), "{context}");
            assert_ne!(restart.run_id(), first.run_id(), "{context}");
            restart.start_below_catalog_check().expect("start");
            let answer = ended(&restart).await.expect_err("the restart refuses");
            assert_eq!(*answer, refusal, "{context}");
            assert_eq!(restart.state(), QueryState::RecoveryRequired, "{context}");
            assert_eq!(
                restart.stop().await,
                ShutdownOutcome::RecoveryRequired {
                    durable: Some(racer.clone()),
                    reason: RecoveryReason::StampedSnapshotExpired,
                },
                "{context}"
            );
            assert_eq!(restart.durable(), Some(racer.clone()), "{context}");
            assert_eq!(
                resumed.calls(),
                0,
                "the refused restart ran a body: {context}"
            );
            let sink = fixture.table("silver").await;
            assert!(history_of(&sink, first.id()).is_empty(), "{context}");
            assert_eq!(sink.metadata().snapshots().count(), 0, "{context}");
            assert!(fixture.ids(SINK).await.is_empty(), "{context}");
        }
    }
}
