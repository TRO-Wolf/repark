use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use futures::future::BoxFuture;
use iceberg::spec::Operation;
use iceberg::table::Table;
use iceberg::{
    Catalog, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use repark_iceberg::microbatch::error::{MicroBatchError, RecoveryReason};
use repark_iceberg::microbatch::offset::{
    EPOCH_KEY, Epoch, OFFSETS_PROPERTY_PREFIX, QUERY_ID_KEY, RUN_ID_KEY, SPARK_EPOCH_ID_KEY,
    SPARK_QUERY_ID_KEY, SinkRecord,
};
use repark_iceberg::write::sink_offsets::SCOPE_TOKEN_KEY;

use crate::Session;
use crate::microbatch::driver::{
    BatchBody, QueryState, ShutdownOutcome, SinkSpec, StreamSpec, StreamingQueryManager, Trigger,
};
use crate::microbatch::testing::{
    Fixture, SINK, SOURCE, options, stamped_epochs, started, table_spec,
};

const ARMED_NONE: u8 = 0;
const ARMED_LANDED: u8 = 1;
const ARMED_LOST: u8 = 2;
pub(super) const ARMED_STALL_LANDED: u8 = 3;
pub(super) const ARMED_STALL_LOST: u8 = 4;

pub(super) type LoadHook = Arc<dyn Fn(&TableIdent) -> BoxFuture<'static, ()> + Send + Sync>;

pub(super) struct FaultCatalog {
    inner: Arc<dyn Catalog>,
    armed: AtomicU8,
    fail_next_load: AtomicBool,
    load_hook: Mutex<Option<LoadHook>>,
}

impl std::fmt::Debug for FaultCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FaultCatalog")
            .finish_non_exhaustive()
    }
}

impl FaultCatalog {
    pub(super) fn arm(&self, fault: u8) {
        self.armed.store(fault, Ordering::SeqCst);
    }

    pub(super) fn on_load(&self, hook: Option<LoadHook>) {
        *self.load_hook.lock().expect("the load hook") = hook;
    }
}

fn unknown(message: &str) -> iceberg::Error {
    iceberg::Error::new(ErrorKind::CommitStateUnknown, message.to_string())
}

#[async_trait]
impl Catalog for FaultCatalog {
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
        let hook = self.load_hook.lock().expect("the load hook").clone();
        if let Some(hook) = hook {
            hook(table).await;
        }
        if self.fail_next_load.swap(false, Ordering::SeqCst) {
            return Err(iceberg::Error::new(
                ErrorKind::Unexpected,
                "injected: the reconcile reload failed",
            ));
        }
        self.inner.load_table(table).await
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
        match self.armed.swap(ARMED_NONE, Ordering::SeqCst) {
            ARMED_STALL_LANDED => {
                self.inner.update_table(commit).await?;
                futures::future::pending().await
            }
            ARMED_STALL_LOST => futures::future::pending().await,
            ARMED_LANDED => {
                self.inner.update_table(commit).await?;
                self.fail_next_load.store(true, Ordering::SeqCst);
                Err(unknown("injected: landed, outcome unknown"))
            }
            ARMED_LOST => {
                self.fail_next_load.store(true, Ordering::SeqCst);
                Err(unknown("injected: not landed, outcome unknown"))
            }
            _ => self.inner.update_table(commit).await,
        }
    }
}

pub(super) const FLAKY_SINK: &str = "flaky.sales.silver";
pub(super) const FLAKY_SOURCE: &str = "flaky.sales.orders";

pub(super) async fn flaky(fixture: &Fixture) -> Arc<FaultCatalog> {
    let inner = fixture
        .session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("the memory catalog");
    let catalog = Arc::new(FaultCatalog {
        inner,
        armed: AtomicU8::new(ARMED_NONE),
        fail_next_load: AtomicBool::new(false),
        load_hook: Mutex::new(None),
    });
    fixture
        .session
        .register_iceberg_catalog("flaky", Arc::clone(&catalog) as Arc<dyn Catalog>)
        .await
        .expect("the wrapped catalog registers");
    catalog
}

fn flaky_spec(trigger: Trigger) -> StreamSpec {
    let mut spec = StreamSpec::new(
        SOURCE,
        options(&[]),
        SinkSpec::Table {
            sink: FLAKY_SINK.to_string(),
        },
    );
    spec.trigger = trigger;
    spec
}

async fn bronze(fixture: &Fixture) {
    fixture.insert(SOURCE, "(1), (2), (3)").await;
    fixture.insert(SOURCE, "(4), (5)").await;
}

#[tokio::test]
async fn each_batch_is_one_stamped_append_carrying_the_spark_keys() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let spec = table_spec(
        Trigger::AvailableNow,
        &options(&[("streaming-max-files-per-micro-batch", "1")]),
    );
    let handle = started(&fixture, spec).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    let sink = fixture.table("silver").await;
    let metadata = sink.metadata();
    assert_eq!(metadata.snapshots().count(), 2);
    let query = handle.id().to_string();
    let run = handle.run_id().to_string();
    let mut epoch = 0u64;
    let mut cursor = metadata
        .snapshots()
        .find(|snapshot| snapshot.parent_snapshot_id().is_none());
    while let Some(snapshot) = cursor {
        let summary = snapshot.summary();
        assert_eq!(summary.operation, Operation::Append);
        let extra = &summary.additional_properties;
        assert_eq!(extra.get(QUERY_ID_KEY), Some(&query));
        assert_eq!(extra.get(SPARK_QUERY_ID_KEY), Some(&query));
        assert_eq!(extra.get(RUN_ID_KEY), Some(&run));
        assert_eq!(extra.get(EPOCH_KEY), Some(&epoch.to_string()));
        assert_eq!(extra.get(SPARK_EPOCH_ID_KEY), Some(&epoch.to_string()));
        assert!(!extra.contains_key(SCOPE_TOKEN_KEY));
        epoch += 1;
        cursor = metadata
            .snapshots()
            .find(|child| child.parent_snapshot_id() == Some(snapshot.snapshot_id()));
    }
    assert_eq!(epoch, 2);
    let durable = handle.durable().expect("a durable record");
    let (key, value) = durable.property().expect("the property");
    assert_eq!(key, format!("{OFFSETS_PROPERTY_PREFIX}{query}"));
    assert_eq!(metadata.properties().get(&key), Some(&value));
    let outputs: Vec<serde_json::Value> = handle
        .recent_progress()
        .iter()
        .map(|progress| progress.json()["sink"]["numOutputRows"].clone())
        .collect();
    assert_eq!(outputs, [serde_json::json!(3), serde_json::json!(2)]);
}

#[tokio::test]
async fn a_shared_catalog_passes_the_start_check() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    flaky(&fixture).await;
    let manager = StreamingQueryManager::of(&fixture.session);
    let handle = manager
        .register(&fixture.session, flaky_spec(Trigger::AvailableNow))
        .await
        .expect("register");
    handle.start().await.expect("a non-local catalog starts");
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn an_unknown_outcome_that_landed_reconciles_and_the_query_runs_on() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let catalog = flaky(&fixture).await;
    catalog.arm(ARMED_LANDED);
    let manager = StreamingQueryManager::of(&fixture.session);
    let handle = manager
        .register(&fixture.session, flaky_spec(Trigger::AvailableNow))
        .await
        .expect("register");
    handle.start().await.expect("start");
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(catalog.armed.load(Ordering::SeqCst), ARMED_NONE);
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
    fixture.insert(SOURCE, "(6)").await;
    let next = manager
        .register(&fixture.session, flaky_spec(Trigger::AvailableNow))
        .await
        .expect("register");
    next.start().await.expect("start");
    assert_eq!(next.await_termination(None).await, Ok(true));
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1]);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn an_unknown_outcome_that_never_landed_ends_recovery_required_and_replays_once() {
    let fixture = Fixture::new().await;
    bronze(&fixture).await;
    let catalog = flaky(&fixture).await;
    catalog.arm(ARMED_LOST);
    let manager = StreamingQueryManager::of(&fixture.session);
    let handle = manager
        .register(&fixture.session, flaky_spec(Trigger::AvailableNow))
        .await
        .expect("register");
    handle.start().await.expect("start");
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
                reason: RecoveryReason::CommitOutcomeUnknown { operation_id: Some(_), .. },
                ..
            } if *epoch == Epoch::FIRST
        ),
        "{error:?}"
    );
    assert_eq!(handle.state(), QueryState::RecoveryRequired);
    let outcome = handle.stop().await;
    assert!(
        matches!(
            outcome,
            ShutdownOutcome::RecoveryRequired {
                durable: None,
                reason: RecoveryReason::CommitOutcomeUnknown { .. }
            }
        ),
        "{outcome:?}"
    );
    assert!(fixture.ids(SINK).await.is_empty());
    let replay = manager
        .register(&fixture.session, flaky_spec(Trigger::AvailableNow))
        .await
        .expect("register");
    replay.start().await.expect("start");
    assert_eq!(replay.await_termination(None).await, Ok(true));
    assert_eq!(stamped_epochs(&fixture.table("silver").await), [0]);
    assert_eq!(fixture.ids(SINK).await, [1, 2, 3, 4, 5]);
}

struct Witness {
    seen: Mutex<Vec<u64>>,
}

impl BatchBody for Witness {
    fn run(&self, _frame: DataFrame, epoch: Epoch) -> BoxFuture<'_, Result<(), MicroBatchError>> {
        self.seen.lock().expect("seen").push(epoch.get());
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn a_driver_fenced_by_another_run_never_runs_its_body() {
    let fixture = Fixture::new().await;
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
    let witness = Arc::new(Witness {
        seen: Mutex::new(Vec::new()),
    });
    let mut late = StreamSpec::new(
        SOURCE,
        options(&[]),
        SinkSpec::ForeachBatch {
            sink: SINK.to_string(),
            body: Arc::clone(&witness) as Arc<dyn BatchBody>,
        },
    );
    late.trigger = Trigger::ProcessingTime(Duration::ZERO);
    let late = StreamingQueryManager::of(&other)
        .register(&other, late)
        .await
        .expect("register");
    late.start().await.expect("start");
    tokio::time::sleep(Duration::from_millis(50)).await;
    fixture.insert("ice.sales.other", "(9)").await;
    let mut winner = table_spec(Trigger::AvailableNow, &options(&[]));
    winner.source = "ice.sales.other".to_string();
    let winner = started(&fixture, winner).await;
    assert_eq!(winner.await_termination(None).await, Ok(true));
    assert_eq!(winner.id(), late.id());
    fixture.insert(SOURCE, "(1)").await;
    let error = tokio::time::timeout(Duration::from_secs(30), late.await_termination(None))
        .await
        .expect("the late driver ends")
        .expect_err("the late driver is fenced");
    assert!(
        matches!(
            error.as_ref(),
            MicroBatchError::Fenced { epoch, winner: run, .. }
                if *epoch == Epoch::FIRST && *run == winner.run_id()
        ),
        "{error:?}"
    );
    assert!(witness.seen.lock().expect("seen").is_empty());
    let sink = fixture.table("silver").await;
    assert_eq!(stamped_epochs(&sink), [0]);
    assert_eq!(fixture.ids(SINK).await, [9]);
    let durable: Option<SinkRecord> = late.stop().await.durable().cloned();
    assert_eq!(durable.map(|record| record.run), Some(winner.run_id()));
}
