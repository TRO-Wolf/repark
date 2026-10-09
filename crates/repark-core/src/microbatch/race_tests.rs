use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use repark_iceberg::microbatch::error::MicroBatchError;
use repark_iceberg::microbatch::offset::{EPOCH_KEY, QUERY_ID_KEY, QueryId, RUN_ID_KEY, RunId};
use tokio::runtime::Handle;

use crate::Session;
use crate::microbatch::driver::{
    QueryHandle, SinkSpec, StreamSpec, StreamingQueryManager, Trigger,
};
use crate::microbatch::lifecycle_tests::{ONE, ended, named};
use crate::microbatch::testing::{Fixture, SINK, SOURCE, options, stamped_epochs};

const ITERATIONS: usize = 50;
const FENCE_FLOOR: usize = 10;
const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
const SLOWEST_LOAD_MILLIS: u64 = 12;
const SLOW_SINK: &str = "slow.sales.silver";
const BATCHES: u64 = 3;
const ROWS_PER_BATCH: u64 = 2;
const ADDED_FILES_KEY: &str = "added-data-files";

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

fn racing_spec(name: &str) -> StreamSpec {
    let sink = SinkSpec::Table {
        sink: SLOW_SINK.to_string(),
    };
    let mut spec = StreamSpec::new(SOURCE, options(ONE), sink);
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

async fn race_once(iteration: usize) -> bool {
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
    let a = registered(&fixture.session, racing_spec("raced")).await;
    let b = registered(&other, racing_spec("raced")).await;
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
        refused_at_the_commit += usize::from(race_once(iteration).await);
    }
    assert!(
        refused_at_the_commit >= FENCE_FLOOR,
        "{refused_at_the_commit} of {ITERATIONS} iterations reached the append fence, under the floor of {FENCE_FLOOR}"
    );
}
