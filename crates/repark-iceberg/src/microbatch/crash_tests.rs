use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use iceberg::spec::{DataFile, NestedField, Operation, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use repark_common::Generation;
use tempfile::TempDir;

use crate::microbatch::error::{MicroBatchError, RecoveryReason};
use crate::microbatch::offset::{
    EPOCH_KEY, Epoch, InputOffset, OffsetFormatVersion, OffsetVector, QUERY_ID_KEY, QueryId, RunId,
    SinkDoor, SinkRecord, SnapshotId, TableUuid,
};
use crate::microbatch::provider::provider_for_plan;
use crate::microbatch::window::{ReadCaps, StartPosition, WindowLimit, WindowPlan, WindowPlanner};
use crate::write::merge::{
    InsertAction, InsertClause, MergeSpec, OPERATION_ID_PROP, execute_merge,
};
use crate::write::session_write_conf::{
    SESSION_SNAPSHOT_PREFIX, apply_session_write_key, resolve_empty_session_write,
};
use crate::write::sink_offsets::{
    BatchScope, BatchScopeGuard, ClaimedStamp, CommitStamp, SCOPE_TOKEN_KEY, ScopeOutcome,
    commit_stamp_only, read_resume_point,
};
use crate::write::write_options::commit_append_with_summary;

const QUERY_NAME: &str = "orders";

struct Harness {
    _warehouse: TempDir,
    catalog: Arc<dyn Catalog>,
    bronze: TableIdent,
    sink: TableIdent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    Race,
    UnknownAfterLanding,
    UnknownWithoutLanding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Door {
    Append,
    CopyOnWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mutation {
    Overwrite,
    Delete,
    Replace,
}

#[derive(Debug)]
struct FaultCatalog {
    inner: Arc<dyn Catalog>,
    fault: Fault,
    updates: AtomicUsize,
    fail_next_load: AtomicBool,
    racer: Mutex<Option<(CommitStamp, Vec<DataFile>)>>,
}

impl FaultCatalog {
    fn new(inner: Arc<dyn Catalog>, fault: Fault) -> Self {
        Self {
            inner,
            fault,
            updates: AtomicUsize::new(0),
            fail_next_load: AtomicBool::new(false),
            racer: Mutex::new(None),
        }
    }

    fn arm_racer(&self, stamp: CommitStamp, files: Vec<DataFile>) {
        *self.racer.lock().expect("racer") = Some((stamp, files));
    }

    fn racer_ran(&self) -> bool {
        self.racer.lock().expect("racer").is_none()
    }

    async fn race(&self, ident: &TableIdent) -> iceberg::Result<()> {
        let racer = self.racer.lock().expect("racer").take();
        let Some((stamp, files)) = racer else {
            return Ok(());
        };
        let current = self.inner.load_table(ident).await?;
        let claimed = ClaimedStamp {
            stamp,
            base: None,
            started: None,
        };
        let summary: HashMap<String, String> = claimed
            .summary_entries()
            .expect("racer entries")
            .into_iter()
            .collect();
        let tx = Transaction::new(&current);
        let tx = tx
            .fast_append()
            .add_data_files(files)
            .set_snapshot_properties(summary)
            .apply(tx)?;
        let tx = claimed.stamp_transaction(tx).expect("racer property");
        tx.commit(self.inner.as_ref()).await.map(|_| ())
    }
}

fn unknown_outcome(message: &str) -> iceberg::Error {
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
        self.updates.fetch_add(1, Ordering::SeqCst);
        match self.fault {
            Fault::Race => {
                self.race(commit.identifier()).await?;
                self.inner.update_table(commit).await
            }
            Fault::UnknownAfterLanding => {
                self.inner.update_table(commit).await?;
                self.fail_next_load.store(true, Ordering::SeqCst);
                Err(unknown_outcome("injected: landed, outcome unknown"))
            }
            Fault::UnknownWithoutLanding => {
                self.fail_next_load.store(true, Ordering::SeqCst);
                Err(unknown_outcome("injected: not landed, outcome unknown"))
            }
        }
    }
}

fn id_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("id schema")
}

async fn create(catalog: &Arc<dyn Catalog>, namespace: &str, name: &str) -> TableIdent {
    let namespace = NamespaceIdent::new(namespace.to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(name.to_string())
                .schema(id_schema())
                .build(),
        )
        .await
        .expect("create table");
    TableIdent::new(namespace, name.to_string())
}

async fn harness() -> Harness {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = crate::memory_catalog(warehouse.path().to_str().expect("utf8"))
        .await
        .expect("catalog");
    let bronze = create(&catalog, "bronze", "events").await;
    let sink = create(&catalog, "silver", "events").await;
    Harness {
        _warehouse: warehouse,
        catalog,
        bronze,
        sink,
    }
}

fn id_batch(ids: &[i32]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).expect("batch")
}

async fn stage(table: &Table, batches: Vec<RecordBatch>) -> Vec<DataFile> {
    crate::write::merge::write_data_files(table, batches)
        .await
        .expect("stage")
}

async fn load(harness: &Harness, ident: &TableIdent) -> Table {
    harness.catalog.load_table(ident).await.expect("load table")
}

async fn bronze_append(harness: &Harness, ids: &[i32]) -> Vec<DataFile> {
    let table = load(harness, &harness.bronze).await;
    let files = stage(&table, vec![id_batch(ids)]).await;
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(files.clone())
        .apply(tx)
        .expect("apply bronze append");
    tx.commit(harness.catalog.as_ref())
        .await
        .expect("commit bronze append");
    files
}

async fn bronze_mutate(harness: &Harness, mutation: Mutation, removed: Vec<DataFile>) -> i64 {
    let table = load(harness, &harness.bronze).await;
    let tx = Transaction::new(&table);
    let tx = match mutation {
        Mutation::Overwrite => tx
            .overwrite_files()
            .add_files(stage(&table, vec![id_batch(&[9])]).await)
            .delete_data_files(removed)
            .apply(tx),
        Mutation::Delete => tx.overwrite_files().delete_data_files(removed).apply(tx),
        Mutation::Replace => {
            let rewritten = stage(&table, vec![id_batch(&[1])]).await;
            tx.rewrite_files(removed, rewritten).apply(tx)
        }
    }
    .expect("apply bronze mutation");
    let committed = tx
        .commit(harness.catalog.as_ref())
        .await
        .expect("commit bronze mutation");
    let head = committed
        .metadata()
        .current_snapshot()
        .expect("bronze head");
    let expected = match mutation {
        Mutation::Overwrite => Operation::Overwrite,
        Mutation::Delete => Operation::Delete,
        Mutation::Replace => Operation::Replace,
    };
    assert_eq!(head.summary().operation, expected);
    head.snapshot_id()
}

async fn set_sink_property(harness: &Harness, key: &str, value: &str) {
    let sink = load(harness, &harness.sink).await;
    let tx = Transaction::new(&sink);
    let tx = tx
        .update_table_properties()
        .set(key.to_string(), value.to_string())
        .apply(tx)
        .expect("apply property");
    tx.commit(harness.catalog.as_ref())
        .await
        .expect("commit property");
}

fn ids_of(batches: &[RecordBatch]) -> Vec<i32> {
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column");
        ids.extend(column.iter().flatten());
    }
    ids.sort_unstable();
    ids
}

async fn table_ids(table: &Table) -> Vec<i32> {
    let batches: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect");
    ids_of(&batches)
}

async fn files_in_any_snapshot(table: &Table) -> Vec<String> {
    let mut paths = Vec::new();
    for snapshot in table.metadata().snapshots() {
        let tasks: Vec<_> = table
            .scan()
            .snapshot_id(snapshot.snapshot_id())
            .select_all()
            .build()
            .expect("snapshot scan")
            .plan_files()
            .await
            .expect("plan files")
            .try_collect()
            .await
            .expect("collect tasks");
        paths.extend(
            tasks
                .into_iter()
                .map(|task| task.data_file_path.to_string()),
        );
    }
    paths
}

fn query_of(sink: &Table) -> QueryId {
    QueryId::derive(TableUuid::of(sink), Some(QUERY_NAME))
}

async fn resume(harness: &Harness) -> (Table, Option<SinkRecord>) {
    let sink = load(harness, &harness.sink).await;
    let record = read_resume_point(&sink, query_of(&sink)).expect("resume point");
    (sink, record)
}

async fn try_plan_window(
    harness: &Harness,
    resumed: Option<&SinkRecord>,
) -> Result<Option<WindowPlan>, MicroBatchError> {
    let bronze = load(harness, &harness.bronze).await;
    let planner = WindowPlanner::new(
        bronze.clone(),
        ReadCaps {
            max_files: None,
            max_rows: None,
        },
    );
    let from = match resumed {
        Some(record) => record
            .offsets
            .get(TableUuid::of(&bronze))
            .cloned()
            .expect("bronze offset in the record"),
        None => planner
            .initial_offset(&StartPosition::Earliest)
            .await
            .expect("initial offset")
            .expect("bronze has data"),
    };
    planner.next_window(&from, WindowLimit::Unbounded).await
}

async fn plan_window(harness: &Harness, resumed: Option<&SinkRecord>) -> Option<WindowPlan> {
    try_plan_window(harness, resumed).await.expect("window")
}

async fn register_window(harness: &Harness, plan: &WindowPlan, ctx: &SessionContext) {
    let bronze = load(harness, &harness.bronze).await;
    let schema = bronze.metadata().current_schema().clone();
    let provider = provider_for_plan(bronze, plan, &schema).expect("provider");
    ctx.register_table("window", provider).expect("register");
}

async fn read_window(harness: &Harness, plan: &WindowPlan) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    register_window(harness, plan, &ctx).await;
    ctx.sql("SELECT id FROM window")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect window")
}

fn record_for(query: QueryId, run: RunId, epoch: Epoch, end: InputOffset) -> SinkRecord {
    SinkRecord {
        format: OffsetFormatVersion::CURRENT,
        query,
        run,
        epoch,
        generation: Generation::new(1).expect("generation"),
        offsets: OffsetVector::single(end),
    }
}

fn stamp_for(
    sink: &Table,
    run: RunId,
    epoch: Epoch,
    plan: &WindowPlan,
    door: SinkDoor,
) -> CommitStamp {
    CommitStamp {
        record: record_for(query_of(sink), run, epoch, plan.end.clone()),
        door,
    }
}

fn next_epoch(resumed: Option<&SinkRecord>) -> Epoch {
    resumed.map_or(Epoch::FIRST, |record| record.epoch.next())
}

fn batch_session(guard: &BatchScopeGuard) -> SessionContext {
    let mut config = SessionConfig::new();
    assert!(apply_session_write_key(
        config.options_mut(),
        &format!("{SESSION_SNAPSHOT_PREFIX}{SCOPE_TOKEN_KEY}"),
        &guard.token().to_string(),
    ));
    SessionContext::new_with_config(config)
}

fn session_extra(ctx: &SessionContext) -> Vec<(String, String)> {
    let (extra, _) = resolve_empty_session_write(ctx).expect("session snapshot properties");
    assert!(extra.iter().any(|(key, _)| key == SCOPE_TOKEN_KEY));
    extra
}

fn merge_window_into(sink: &TableIdent) -> MergeSpec {
    MergeSpec {
        target: sink.clone(),
        target_alias: String::from("t"),
        source_from_sql: String::from("window"),
        source_alias: String::from("s"),
        on_sql: String::from("t.id = s.id"),
        matched: Vec::new(),
        not_matched: vec![InsertClause {
            predicate_sql: None,
            action: InsertAction::All,
        }],
        not_matched_by_source: Vec::new(),
        commit_branch: None,
        case_insensitive: false,
        schema_evolution: false,
    }
}

async fn deliver(
    catalog: &Arc<dyn Catalog>,
    sink: &Table,
    stamp: &CommitStamp,
    batches: Vec<RecordBatch>,
) -> Result<(), String> {
    let files = stage(sink, batches).await;
    let guard = BatchScope::enter(TableUuid::of(sink), stamp.clone()).expect("enter scope");
    let extra = session_extra(&batch_session(&guard));
    commit_append_with_summary(catalog, sink, files, &extra, None)
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert!(matches!(guard.outcome(), ScopeOutcome::Committed { .. }));
    Ok(())
}

async fn run_epoch(harness: &Harness, run: RunId) -> Option<Epoch> {
    let (sink, resumed) = resume(harness).await;
    let epoch = next_epoch(resumed.as_ref());
    let plan = plan_window(harness, resumed.as_ref()).await?;
    let stamp = stamp_for(&sink, run, epoch, &plan, SinkDoor::Table);
    let batches = read_window(harness, &plan).await;
    deliver(&harness.catalog, &sink, &stamp, batches)
        .await
        .expect("stamped sink commit");
    Some(epoch)
}

fn epochs_stamped(sink: &Table, query: QueryId) -> Vec<String> {
    let query = query.to_string();
    let mut epochs: Vec<String> = sink
        .metadata()
        .snapshots()
        .filter_map(|snapshot| {
            let summary = &snapshot.summary().additional_properties;
            (summary.get(QUERY_ID_KEY) == Some(&query))
                .then(|| summary.get(EPOCH_KEY).cloned())
                .flatten()
        })
        .collect();
    epochs.sort();
    epochs
}

fn claim_on(sink: &Table, stamp: &CommitStamp) -> Result<bool, MicroBatchError> {
    let guard = BatchScope::enter(TableUuid::of(sink), stamp.clone()).expect("enter scope");
    BatchScope::claim(sink, guard.token()).map(|claimed| claimed.is_some())
}

#[tokio::test]
async fn test_microbatch_kill_after_commit_resumes_1() {
    let harness = harness().await;
    bronze_append(&harness, &[1]).await;
    bronze_append(&harness, &[2]).await;

    assert_eq!(
        run_epoch(&harness, RunId::fresh()).await,
        Some(Epoch::FIRST)
    );

    let (_, resumed) = resume(&harness).await;
    let resumed = resumed.expect("epoch 0 is durable");
    assert_eq!(resumed.epoch, Epoch::FIRST);
    bronze_append(&harness, &[3]).await;
    let plan = plan_window(&harness, Some(&resumed))
        .await
        .expect("epoch 1 window");
    assert_eq!(ids_of(&read_window(&harness, &plan).await), vec![3]);
    drop(plan);

    let (_, after_staging_kill) = resume(&harness).await;
    assert_eq!(after_staging_kill, Some(resumed));
    assert_eq!(
        run_epoch(&harness, RunId::fresh()).await,
        Some(Epoch::new(1))
    );
    assert_eq!(run_epoch(&harness, RunId::fresh()).await, None);

    let (sink, last) = resume(&harness).await;
    let bronze = harness
        .catalog
        .load_table(&harness.bronze)
        .await
        .expect("load bronze");
    assert_eq!(table_ids(&sink).await, vec![1, 2, 3]);
    assert_eq!(table_ids(&sink).await, table_ids(&bronze).await);
    let query = query_of(&sink);
    assert_eq!(epochs_stamped(&sink, query), vec!["0", "1"]);
    let last = last.expect("epoch 1 is durable");
    assert_eq!(last.epoch, Epoch::new(1));
    let head = sink.metadata().current_snapshot().expect("sink head");
    assert_eq!(
        SinkRecord::from_summary(&head.summary().additional_properties).expect("head stamp"),
        Some(last.clone())
    );
    let (key, value) = last.property().expect("property");
    assert_eq!(sink.metadata().properties().get(&key), Some(&value));
    assert_eq!(
        last.offsets
            .get(TableUuid::of(&bronze))
            .expect("bronze offset")
            .snapshot
            .get(),
        bronze
            .metadata()
            .current_snapshot_id()
            .expect("bronze head")
    );
}

#[tokio::test]
async fn test_microbatch_duplicate_delivery_skips_1() {
    let harness = harness().await;
    bronze_append(&harness, &[1]).await;
    bronze_append(&harness, &[2]).await;
    let run = RunId::fresh();

    let (stale, resumed) = resume(&harness).await;
    assert_eq!(resumed, None);
    let plan = plan_window(&harness, None).await.expect("epoch 0 window");
    let stamp = stamp_for(&stale, run, Epoch::FIRST, &plan, SinkDoor::Table);
    let orphaned: Vec<String> = stage(&stale, read_window(&harness, &plan).await)
        .await
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();
    assert!(!orphaned.is_empty());

    assert_eq!(run_epoch(&harness, run).await, Some(Epoch::FIRST));
    let (committed, durable) = resume(&harness).await;
    assert_eq!(durable.as_ref(), Some(&stamp.record));
    let live = files_in_any_snapshot(&committed).await;
    assert!(
        orphaned.iter().all(|path| !live.contains(path)),
        "the files staged before the kill must stay out of every snapshot"
    );
    let snapshots = committed.metadata().snapshots().count();

    let redelivered = deliver(
        &harness.catalog,
        &stale,
        &stamp,
        read_window(&harness, &plan).await,
    )
    .await;
    assert!(
        redelivered.is_err(),
        "the stale re-delivery of epoch 0 must not commit a second time"
    );

    let (sink, after) = resume(&harness).await;
    assert_eq!(sink.metadata().snapshots().count(), snapshots);
    assert_eq!(table_ids(&sink).await, vec![1, 2]);
    assert_eq!(epochs_stamped(&sink, query_of(&sink)), vec!["0"]);
    assert_eq!(after.as_ref(), Some(&stamp.record));
    match claim_on(&sink, &stamp) {
        Err(MicroBatchError::AlreadyCommitted { query, epoch }) => {
            assert_eq!(query, stamp.record.query);
            assert_eq!(epoch, Epoch::FIRST);
        }
        other => panic!("claim of a committed epoch must return AlreadyCommitted, got {other:?}"),
    }
}

async fn two_drivers_one_sink(door: Door) {
    let harness = harness().await;
    bronze_append(&harness, &[1]).await;
    bronze_append(&harness, &[2]).await;
    assert_eq!(
        run_epoch(&harness, RunId::fresh()).await,
        Some(Epoch::FIRST)
    );
    bronze_append(&harness, &[3]).await;

    let fault = Arc::new(FaultCatalog::new(Arc::clone(&harness.catalog), Fault::Race));
    let catalog: Arc<dyn Catalog> = Arc::clone(&fault) as Arc<dyn Catalog>;
    let sink_b = catalog
        .load_table(&harness.sink)
        .await
        .expect("run B loads");
    let resumed = read_resume_point(&sink_b, query_of(&sink_b))
        .expect("resume point")
        .expect("epoch 0 is durable");
    let epoch = next_epoch(Some(&resumed));
    let plan = plan_window(&harness, Some(&resumed))
        .await
        .expect("epoch 1 window");
    let (run_a, run_b) = (RunId::fresh(), RunId::fresh());
    let stamp_a = stamp_for(&sink_b, run_a, epoch, &plan, SinkDoor::Table);
    let stamp_b = stamp_for(&sink_b, run_b, epoch, &plan, SinkDoor::Table);
    fault.arm_racer(
        stamp_a.clone(),
        stage(&sink_b, read_window(&harness, &plan).await).await,
    );

    let guard = BatchScope::enter(TableUuid::of(&sink_b), stamp_b.clone()).expect("enter scope");
    let session = batch_session(&guard);
    let outcome = match door {
        Door::Append => {
            let files_b = stage(&sink_b, read_window(&harness, &plan).await).await;
            commit_append_with_summary(&catalog, &sink_b, files_b, &session_extra(&session), None)
                .await
                .map(|_| ())
        }
        Door::CopyOnWrite => {
            register_window(&harness, &plan, &session).await;
            execute_merge(&session, &catalog, &merge_window_into(&harness.sink)).await
        }
    };
    drop(guard);
    assert!(
        fault.racer_ran(),
        "{door:?}: run A must commit inside B's commit"
    );
    assert!(
        outcome.is_err(),
        "{door:?}: run B's commit of epoch 1 must fail at its base once run A has committed epoch 1"
    );

    let sink = load(&harness, &harness.sink).await;
    let query = query_of(&sink);
    assert_eq!(epochs_stamped(&sink, query), vec!["0", "1"], "{door:?}");
    assert_eq!(table_ids(&sink).await, vec![1, 2, 3], "{door:?}");
    let (key, _) = stamp_a.record.property().expect("property");
    let property = sink
        .metadata()
        .properties()
        .get(&key)
        .expect("offset property");
    assert_eq!(
        SinkRecord::from_property(query, property)
            .expect("property record")
            .run,
        run_a,
        "{door:?}"
    );
    assert_eq!(
        read_resume_point(&sink, query).expect("resume point"),
        Some(stamp_a.record.clone())
    );
    match claim_on(&sink, &stamp_b) {
        Err(MicroBatchError::Fenced {
            query: fenced,
            epoch: lost,
            winner,
        }) => {
            assert_eq!(fenced, query, "{door:?}");
            assert_eq!(lost, epoch, "{door:?}");
            assert_eq!(winner, run_a, "{door:?}");
        }
        other => {
            panic!("{door:?}: run B's claim of epoch 1 must be Fenced by run A, got {other:?}")
        }
    }
}

#[tokio::test]
async fn test_microbatch_two_drivers_one_sink_1() {
    two_drivers_one_sink(Door::Append).await;
    two_drivers_one_sink(Door::CopyOnWrite).await;
}

struct UnknownOutcome {
    harness: Harness,
    injector: Arc<FaultCatalog>,
    stamp: CommitStamp,
    durable: SinkRecord,
    snapshots: usize,
    outcome: Result<SnapshotId, MicroBatchError>,
}

async fn stamp_under_unknown_outcome(fault: Fault) -> UnknownOutcome {
    let harness = harness().await;
    set_sink_property(&harness, "commit.status-check.num-retries", "0").await;
    bronze_append(&harness, &[1]).await;
    bronze_append(&harness, &[2]).await;
    assert_eq!(
        run_epoch(&harness, RunId::fresh()).await,
        Some(Epoch::FIRST)
    );
    bronze_append(&harness, &[3]).await;

    let (sink, resumed) = resume(&harness).await;
    let durable = resumed.expect("epoch 0 is durable");
    let plan = plan_window(&harness, Some(&durable))
        .await
        .expect("epoch 1 window");
    let body = stage(&sink, read_window(&harness, &plan).await).await;
    let sink = commit_append_with_summary(&harness.catalog, &sink, body, &[], None)
        .await
        .expect("the batch body's unstamped write");
    let snapshots = sink.metadata().snapshots().count();
    let stamp = stamp_for(
        &sink,
        RunId::fresh(),
        durable.epoch.next(),
        &plan,
        SinkDoor::ForeachBatch,
    );

    let injector = Arc::new(FaultCatalog::new(Arc::clone(&harness.catalog), fault));
    let catalog: Arc<dyn Catalog> = Arc::clone(&injector) as Arc<dyn Catalog>;
    let guard = BatchScope::enter(TableUuid::of(&sink), stamp.clone()).expect("enter scope");
    let outcome = commit_stamp_only(&catalog, &sink, &stamp, Some(guard.token())).await;
    drop(guard);
    UnknownOutcome {
        harness,
        injector,
        stamp,
        durable,
        snapshots,
        outcome,
    }
}

fn assert_no_replace(sink: &Table) {
    assert!(
        sink.metadata()
            .snapshots()
            .all(|snapshot| snapshot.summary().operation == Operation::Append),
        "the reconcile must never commit a replace or an overwrite"
    );
}

#[tokio::test]
async fn test_microbatch_unknown_outcome_reconciles_1() {
    let UnknownOutcome {
        harness,
        injector,
        stamp,
        snapshots,
        outcome,
        ..
    } = stamp_under_unknown_outcome(Fault::UnknownAfterLanding).await;
    let sink = load(&harness, &harness.sink).await;
    assert_eq!(sink.metadata().snapshots().count(), snapshots + 1);
    let landed = sink.metadata().current_snapshot().expect("landed head");
    assert!(
        landed
            .summary()
            .additional_properties
            .contains_key(OPERATION_ID_PROP)
    );
    assert!(
        matches!(outcome, Ok(snapshot) if snapshot == SnapshotId::new(landed.snapshot_id())),
        "the landed stamp must resolve to its snapshot {}, got {outcome:?}",
        landed.snapshot_id()
    );
    assert_eq!(
        injector.updates.load(Ordering::SeqCst),
        1,
        "a landed commit is never submitted a second time"
    );
    assert_no_replace(&sink);
    assert_eq!(
        read_resume_point(&sink, stamp.record.query).expect("resume point"),
        Some(stamp.record.clone())
    );

    let UnknownOutcome {
        harness,
        injector,
        stamp,
        durable,
        snapshots,
        outcome,
    } = stamp_under_unknown_outcome(Fault::UnknownWithoutLanding).await;
    let sink = load(&harness, &harness.sink).await;
    assert_eq!(sink.metadata().snapshots().count(), snapshots);
    assert_eq!(injector.updates.load(Ordering::SeqCst), 1);
    assert_no_replace(&sink);
    match outcome {
        Err(MicroBatchError::RecoveryRequired {
            query,
            epoch,
            durable: Some(found),
            reason: RecoveryReason::CommitOutcomeUnknown { .. },
        }) => {
            assert_eq!(query, stamp.record.query);
            assert_eq!(epoch, stamp.record.epoch);
            assert_eq!(*found, durable);
        }
        other => panic!(
            "an unlanded stamp must refuse CommitOutcomeUnknown with epoch {} durable, got {other:?}",
            durable.epoch
        ),
    }
    assert_eq!(
        read_resume_point(&sink, stamp.record.query).expect("resume point"),
        Some(durable)
    );
}

async fn bronze_mutation_inside_a_window(mutation: Mutation) {
    let harness = harness().await;
    let first = bronze_append(&harness, &[1]).await;
    let second = bronze_append(&harness, &[2]).await;
    assert_eq!(
        run_epoch(&harness, RunId::fresh()).await,
        Some(Epoch::FIRST)
    );
    let removed = match mutation {
        Mutation::Overwrite => [first, second].concat(),
        Mutation::Delete | Mutation::Replace => first,
    };
    let mutated = bronze_mutate(&harness, mutation, removed).await;
    bronze_append(&harness, &[4]).await;
    let (before, resumed) = resume(&harness).await;
    let snapshots = before.metadata().snapshots().count();

    if mutation == Mutation::Replace {
        assert_eq!(
            run_epoch(&harness, RunId::fresh()).await,
            Some(Epoch::new(1))
        );
        let sink = load(&harness, &harness.sink).await;
        assert_eq!(table_ids(&sink).await, vec![1, 2, 4]);
        assert_eq!(epochs_stamped(&sink, query_of(&sink)), vec!["0", "1"]);
        return;
    }

    match try_plan_window(&harness, resumed.as_ref()).await {
        Err(MicroBatchError::NonAppendSnapshot {
            snapshot,
            operation,
            ..
        }) => {
            assert_eq!(snapshot, SnapshotId::new(mutated), "{mutation:?}");
            let expected = match mutation {
                Mutation::Overwrite => Operation::Overwrite,
                _ => Operation::Delete,
            };
            assert_eq!(operation, expected);
        }
        other => panic!("{mutation:?}: the window must refuse NonAppendSnapshot, got {other:?}"),
    }
    let (sink, after) = resume(&harness).await;
    assert_eq!(
        sink.metadata().snapshots().count(),
        snapshots,
        "{mutation:?}"
    );
    assert_eq!(table_ids(&sink).await, vec![1, 2], "{mutation:?}");
    assert_eq!(after, resumed, "{mutation:?}");
}

#[tokio::test]
async fn test_microbatch_bronze_overwrite_refuses_1() {
    bronze_mutation_inside_a_window(Mutation::Overwrite).await;
    bronze_mutation_inside_a_window(Mutation::Delete).await;
    bronze_mutation_inside_a_window(Mutation::Replace).await;
}
