use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::DataFusionError;
use futures::TryStreamExt;
use iceberg::expr::Predicate;
use iceberg::spec::{DataFile, NestedField, Operation, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use repark_common::Generation;
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::microbatch::offset::{
    Epoch, FilePosition, InputOffset, OffsetFormatVersion, OffsetVector, RunId, SPARK_EPOCH_ID_KEY,
    SPARK_QUERY_ID_KEY,
};
use crate::write::concurrency::WriteConcurrency;
use crate::write::write_options::commit_append_with_summary;

const STAMP_KEY: &str = "repark.cdc.epoch";
const STAMP_PROPERTY: &str = "repark.cdc.offsets.dm-5";
const BRONZE_UUID: &str = "0b0b0b0b-0000-4000-8000-00000000b0b0";
const RETRY_PROPERTIES: [(&str, &str); 3] = [
    ("commit.retry.num-retries", "3"),
    ("commit.retry.min-wait-ms", "1"),
    ("commit.retry.max-wait-ms", "5"),
];

fn id_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("id schema")
}

async fn fixture(name: &str) -> (TempDir, Arc<dyn Catalog>, TableIdent) {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = crate::memory_catalog(warehouse.path().to_str().expect("utf8"))
        .await
        .expect("catalog");
    catalog
        .create_namespace(&NamespaceIdent::new("silver".to_string()), HashMap::new())
        .await
        .expect("namespace");
    let ident = TableIdent::new(NamespaceIdent::new("silver".to_string()), name.to_string());
    let properties: HashMap<String, String> = RETRY_PROPERTIES
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect();
    catalog
        .create_table(
            ident.namespace(),
            TableCreation::builder()
                .name(name.to_string())
                .schema(id_schema())
                .properties(properties)
                .build(),
        )
        .await
        .expect("create table");
    (warehouse, catalog, ident)
}

fn query() -> QueryId {
    QueryId::new(Uuid::parse_str("aaaaaaaa-0000-4000-8000-0000000000a1").expect("uuid"))
}

fn record_for(query: QueryId, epoch: u64, snapshot: i64) -> SinkRecord {
    SinkRecord {
        format: OffsetFormatVersion::CURRENT,
        query,
        run: RunId::new(Uuid::parse_str("bbbbbbbb-0000-4000-8000-0000000000b2").expect("uuid")),
        epoch: Epoch::new(epoch),
        generation: Generation::new(1).expect("generation"),
        offsets: OffsetVector::single(InputOffset {
            table: TableUuid::new(Uuid::parse_str(BRONZE_UUID).expect("uuid")),
            table_name: String::from("bronze.events"),
            snapshot: SnapshotId::new(snapshot),
            position: FilePosition::new(1),
        }),
    }
}

fn stamp_for(epoch: u64, door: SinkDoor) -> CommitStamp {
    CommitStamp {
        record: record_for(query(), epoch, 100 + i64::try_from(epoch).expect("epoch")),
        door,
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

async fn stage(table: &Table, ids: &[i32]) -> Vec<DataFile> {
    crate::write::merge::write_data_files(table, vec![id_batch(ids)])
        .await
        .expect("stage")
}

async fn live_ids(table: &Table) -> Vec<i32> {
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
    let mut ids = Vec::new();
    for batch in &batches {
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

async fn append_plain(catalog: &Arc<dyn Catalog>, ident: &TableIdent, ids: &[i32]) -> Table {
    let table = catalog.load_table(ident).await.expect("load");
    let files = stage(&table, ids).await;
    commit_append_with_summary(catalog, &table, files, &[], None)
        .await
        .expect("plain append")
}

fn scoped(guard: &BatchScopeGuard) -> Vec<(String, String)> {
    vec![guard.token().summary_entry()]
}

fn stamped_snapshots(table: &Table) -> usize {
    table
        .metadata()
        .snapshots()
        .filter(|snapshot| {
            snapshot
                .summary()
                .additional_properties
                .contains_key(QUERY_ID_KEY)
        })
        .count()
}

fn assert_stamped_head(table: &Table, stamp: &CommitStamp) {
    let head = table.metadata().current_snapshot().expect("head");
    let summary = &head.summary().additional_properties;
    assert!(!summary.contains_key(SCOPE_TOKEN_KEY));
    assert_eq!(
        SinkRecord::from_summary(summary).expect("summary stamp"),
        Some(stamp.record.clone())
    );
    let (key, value) = stamp.record.property().expect("property");
    assert_eq!(table.metadata().properties().get(&key), Some(&value));
    assert_eq!(
        SinkRecord::from_property(stamp.record.query, &value).expect("property stamp"),
        stamp.record
    );
    let spark_keys =
        summary.contains_key(SPARK_QUERY_ID_KEY) && summary.contains_key(SPARK_EPOCH_ID_KEY);
    assert_eq!(spark_keys, matches!(stamp.door, SinkDoor::Table));
}

fn assert_unstamped(table: &Table) {
    if let Some(head) = table.metadata().current_snapshot() {
        let summary = &head.summary().additional_properties;
        assert!(summary.keys().all(|key| !key.starts_with("repark.cdc.")));
        assert!(!summary.contains_key(SPARK_QUERY_ID_KEY));
        assert!(!summary.contains_key(SPARK_EPOCH_ID_KEY));
    }
    assert!(
        table
            .metadata()
            .properties()
            .keys()
            .all(|key| !key.starts_with(OFFSETS_PROPERTY_PREFIX))
    );
}

fn microbatch_cause(error: &DataFusionError) -> &MicroBatchError {
    match error {
        DataFusionError::External(inner) => inner
            .downcast_ref::<MicroBatchError>()
            .expect("a MicroBatchError cause"),
        other => panic!("expected an external MicroBatchError, got {other:?}"),
    }
}

async fn empty_merge_append(catalog: &Arc<dyn Catalog>, table: &Table) -> Table {
    let tx = Transaction::new(table);
    let tx = tx
        .merge_append()
        .set_snapshot_properties(HashMap::from([(STAMP_KEY.to_string(), "0".to_string())]))
        .apply(tx)
        .expect("apply empty merge_append");
    let tx = tx
        .update_table_properties()
        .set(STAMP_PROPERTY.to_string(), "{}".to_string())
        .apply(tx)
        .expect("apply property");
    tx.commit(catalog.as_ref())
        .await
        .expect("commit stamp-only")
}

#[tokio::test]
async fn dm5_empty_merge_append_commits_one_stamp_only_append_snapshot() {
    let (_warehouse, catalog, ident) = fixture("dm5").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let base_log = table.metadata().metadata_log().len();
    let committed = empty_merge_append(&catalog, &table).await;
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    for view in [&committed, &reloaded] {
        let metadata = view.metadata();
        assert_eq!(metadata.metadata_log().len(), base_log + 1);
        assert_eq!(metadata.snapshots().count(), 1);
        let head = metadata.current_snapshot().expect("stamp-only snapshot");
        assert_eq!(head.summary().operation, Operation::Append);
        assert_eq!(
            head.summary()
                .additional_properties
                .get(STAMP_KEY)
                .map(String::as_str),
            Some("0")
        );
        assert_eq!(
            head.summary()
                .additional_properties
                .get("added-data-files")
                .map(String::as_str),
            None
        );
        assert_eq!(
            metadata
                .properties()
                .get(STAMP_PROPERTY)
                .map(String::as_str),
            Some("{}")
        );
    }
    let second = empty_merge_append(&catalog, &reloaded).await;
    let head = second.metadata().current_snapshot().expect("second stamp");
    assert_eq!(head.summary().operation, Operation::Append);
    assert_eq!(
        head.parent_snapshot_id(),
        reloaded.metadata().current_snapshot_id()
    );
    let tasks: Vec<_> = second
        .scan()
        .build()
        .expect("scan")
        .plan_files()
        .await
        .expect("plan")
        .try_collect()
        .await
        .expect("tasks");
    assert!(tasks.is_empty());
}

#[tokio::test]
async fn scope_holds_one_stamp_per_sink_and_claims_once() {
    let (_warehouse, catalog, ident) = fixture("scope").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let stranger = ScopeToken::parse("cccccccc-0000-4000-8000-0000000000c3").expect("token");
    assert_eq!(
        BatchScope::claim(&table, &stranger).expect("no scope"),
        None
    );
    let sink = TableUuid::of(&table);
    let guard = BatchScope::enter(sink, stamp_for(0, SinkDoor::Table)).expect("enter");
    assert_ne!(guard.token(), &stranger);
    assert_eq!(
        BatchScope::claim(&table, &stranger).expect("wrong token"),
        None
    );
    let busy = BatchScope::enter(sink, stamp_for(1, SinkDoor::Table)).expect_err("busy");
    assert_eq!(
        busy,
        MicroBatchError::SinkBusy {
            sink: sink.to_string()
        }
    );
    let claimed = BatchScope::claim(&table, guard.token())
        .expect("claim")
        .expect("stamp");
    assert_eq!(claimed.stamp, stamp_for(0, SinkDoor::Table));
    assert_eq!(
        claimed.base,
        table.metadata().current_snapshot_id().map(SnapshotId::new)
    );
    assert_eq!(
        BatchScope::claim(&table, guard.token()).expect_err("second claim"),
        MicroBatchError::SinkCommittedTwice {
            epoch: Epoch::new(0)
        }
    );
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    let token = guard.token().clone();
    drop(guard);
    assert_eq!(BatchScope::claim(&table, &token).expect("scope gone"), None);
    let again = BatchScope::enter(sink, stamp_for(1, SinkDoor::Table)).expect("re-enter");
    drop(again);
}

#[tokio::test]
async fn append_arm_stamps_summary_and_property_in_one_commit_on_both_doors() {
    for (name, door) in [
        ("append_table", SinkDoor::Table),
        ("append_batch", SinkDoor::ForeachBatch),
    ] {
        let (_warehouse, catalog, ident) = fixture(name).await;
        let table = append_plain(&catalog, &ident, &[1]).await;
        let base_log = table.metadata().metadata_log().len();
        let base_snapshots = table.metadata().snapshots().count();
        let stamp = stamp_for(0, door);
        let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
        let files = stage(&table, &[2, 3]).await;
        let mut extra = scoped(&guard);
        extra.push((String::from("run_id"), String::from("caller")));
        let committed = commit_append_with_summary(&catalog, &table, files, &extra, None)
            .await
            .expect("stamped append");
        assert_eq!(committed.metadata().metadata_log().len(), base_log + 1);
        assert_eq!(committed.metadata().snapshots().count(), base_snapshots + 1);
        let reloaded = catalog.load_table(&ident).await.expect("reload");
        for view in [&committed, &reloaded] {
            assert_stamped_head(view, &stamp);
            let head = view.metadata().current_snapshot().expect("head");
            assert_eq!(head.summary().operation, Operation::Append);
            assert_eq!(
                head.summary()
                    .additional_properties
                    .get("run_id")
                    .map(String::as_str),
                Some("caller")
            );
        }
        assert_eq!(
            guard.outcome(),
            ScopeOutcome::Committed {
                snapshot: SnapshotId::new(
                    reloaded.metadata().current_snapshot_id().expect("head id")
                )
            }
        );
        assert_eq!(live_ids(&reloaded).await, vec![1, 2, 3]);
    }
}

#[tokio::test]
async fn copy_on_write_arm_stamps_both_overwrite_shapes() {
    let (_warehouse, catalog, ident) = fixture("cow").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let seeded = stage(&table, &[1, 2]).await;
    let table = commit_append_with_summary(&catalog, &table, seeded.clone(), &[], None)
        .await
        .expect("seed");
    let insert_only = stamp_for(0, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), insert_only.clone()).expect("enter");
    let base_log = table.metadata().metadata_log().len();
    let files = stage(&table, &[3]).await;
    let pin = table.metadata().current_snapshot_id();
    crate::write::merge::commit_on_ref(
        &catalog,
        &table,
        pin,
        Vec::new(),
        files,
        &Predicate::AlwaysTrue,
        None,
        &scoped(&guard),
    )
    .await
    .expect("insert-only overwrite");
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(table.metadata().metadata_log().len(), base_log + 1);
    assert_eq!(
        table
            .metadata()
            .current_snapshot()
            .expect("head")
            .summary()
            .operation,
        Operation::Append
    );
    assert_stamped_head(&table, &insert_only);
    assert!(matches!(guard.outcome(), ScopeOutcome::Committed { .. }));
    drop(guard);

    let rewrite = stamp_for(1, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), rewrite.clone()).expect("enter");
    let base_log = table.metadata().metadata_log().len();
    let replacement = stage(&table, &[2]).await;
    let pin = table.metadata().current_snapshot_id();
    crate::write::merge::commit_on_ref(
        &catalog,
        &table,
        pin,
        seeded,
        replacement,
        &Predicate::AlwaysTrue,
        None,
        &scoped(&guard),
    )
    .await
    .expect("delete-and-add overwrite");
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(table.metadata().metadata_log().len(), base_log + 1);
    assert_eq!(
        table
            .metadata()
            .current_snapshot()
            .expect("head")
            .summary()
            .operation,
        Operation::Overwrite
    );
    assert_stamped_head(&table, &rewrite);
    assert_eq!(stamped_snapshots(&table), 2);
    assert_eq!(live_ids(&table).await, vec![2, 3]);
    drop(guard);
}

#[tokio::test]
async fn merge_on_read_arm_stamps_the_row_delta() {
    let (_warehouse, catalog, ident) = fixture("mor").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let seeded = stage(&table, &[1, 2]).await;
    let target: Arc<str> = Arc::from(seeded[0].file_path());
    let table = commit_append_with_summary(&catalog, &table, seeded, &[], None)
        .await
        .expect("seed");
    let stamp = stamp_for(0, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let base_log = table.metadata().metadata_log().len();
    let added = stage(&table, &[10]).await;
    let pin = table.metadata().current_snapshot_id();
    crate::write::merge::commit_row_delta_on_ref_with_partitions(
        &catalog,
        &table,
        pin,
        vec![(target, 0)],
        added,
        WriteConcurrency::new(1).expect("K=1"),
        &Predicate::AlwaysTrue,
        None,
        crate::write::merge::KnownPartitions::new(),
        &scoped(&guard),
        &crate::write::write_options::WriterStagingOverrides::none(),
    )
    .await
    .expect("stamped row delta");
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(table.metadata().metadata_log().len(), base_log + 1);
    assert_stamped_head(&table, &stamp);
    assert_eq!(live_ids(&table).await, vec![2, 10]);
    assert_eq!(
        guard.outcome(),
        ScopeOutcome::Committed {
            snapshot: SnapshotId::new(table.metadata().current_snapshot_id().expect("head"))
        }
    );
}

#[tokio::test]
async fn unscoped_arms_commit_without_any_stamp() {
    let (_warehouse, catalog, ident) = fixture("unscoped").await;
    let table = catalog.load_table(&ident).await.expect("load");
    let seeded = stage(&table, &[1, 2]).await;
    let target: Arc<str> = Arc::from(seeded[0].file_path());
    let table = commit_append_with_summary(&catalog, &table, seeded, &[], None)
        .await
        .expect("append");
    assert_unstamped(&table);
    let files = stage(&table, &[3]).await;
    let pin = table.metadata().current_snapshot_id();
    crate::write::merge::commit_on_ref(
        &catalog,
        &table,
        pin,
        Vec::new(),
        files,
        &Predicate::AlwaysTrue,
        None,
        &[],
    )
    .await
    .expect("overwrite");
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_unstamped(&table);
    let pin = table.metadata().current_snapshot_id();
    crate::write::merge::commit_row_delta(
        &catalog,
        &table,
        pin,
        vec![(target, 0)],
        Vec::new(),
        WriteConcurrency::new(1).expect("K=1"),
    )
    .await
    .expect("row delta");
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_unstamped(&table);
    assert_eq!(stamped_snapshots(&table), 0);
    assert_eq!(read_resume_point(&table, query()).expect("resume"), None);
}

#[tokio::test]
async fn a_branch_commit_leaves_the_claim_for_main() {
    let (_warehouse, catalog, ident) = fixture("branch").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let head = table.metadata().current_snapshot_id().expect("head");
    crate::write::testing_create_ref(
        catalog.as_ref(),
        &ident,
        crate::write::SnapshotRefKind::Branch,
        "audit",
        head,
    )
    .await
    .expect("branch");
    let table = catalog.load_table(&ident).await.expect("reload");
    let stamp = stamp_for(0, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let files = stage(&table, &[2]).await;
    let table = commit_append_with_summary(&catalog, &table, files, &scoped(&guard), Some("audit"))
        .await
        .expect("branch append");
    assert_eq!(guard.outcome(), ScopeOutcome::NotCommitted);
    assert_unstamped(&table);
    let files = stage(&table, &[3]).await;
    let table =
        commit_append_with_summary(&catalog, &table, files, &scoped(&guard), Some(MAIN_BRANCH))
            .await
            .expect("main append");
    assert_stamped_head(&table, &stamp);
    assert!(matches!(guard.outcome(), ScopeOutcome::Committed { .. }));
}

#[tokio::test]
async fn a_second_stamped_commit_in_one_batch_refuses() {
    let (_warehouse, catalog, ident) = fixture("twice").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let guard =
        BatchScope::enter(TableUuid::of(&table), stamp_for(4, SinkDoor::Table)).expect("enter");
    let files = stage(&table, &[2]).await;
    let table = commit_append_with_summary(&catalog, &table, files, &scoped(&guard), None)
        .await
        .expect("first");
    let files = stage(&table, &[3]).await;
    let error = commit_append_with_summary(&catalog, &table, files, &scoped(&guard), None)
        .await
        .expect_err("second");
    assert_eq!(
        microbatch_cause(&error),
        &MicroBatchError::SinkCommittedTwice {
            epoch: Epoch::new(4)
        }
    );
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(stamped_snapshots(&table), 1);
    assert_eq!(live_ids(&table).await, vec![1, 2]);
    drop(guard);
}

#[tokio::test]
async fn record_commit_refuses_a_head_without_the_stamp() {
    let (_warehouse, catalog, ident) = fixture("unstamped_head").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let claimed = ClaimedStamp {
        stamp: stamp_for(2, SinkDoor::Table),
        base: None,
    };
    let head = SnapshotId::new(table.metadata().current_snapshot_id().expect("head"));
    match claimed.record_commit(&table).expect_err("unstamped head") {
        MicroBatchError::RecoveryRequired {
            epoch,
            durable: None,
            reason: RecoveryReason::UnstampedSinkCommit { snapshot },
            ..
        } => {
            assert_eq!(epoch, Epoch::new(2));
            assert_eq!(snapshot, head);
        }
        other => panic!("expected UnstampedSinkCommit, got {other:?}"),
    }
}

#[tokio::test]
async fn commit_stamp_only_commits_one_append_snapshot_with_both_halves() {
    let (_warehouse, catalog, ident) = fixture("stamp_only").await;
    let table = append_plain(&catalog, &ident, &[1]).await;
    let base_log = table.metadata().metadata_log().len();
    let stamp = stamp_for(0, SinkDoor::ForeachBatch);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let snapshot = commit_stamp_only(&catalog, &table, &stamp, Some(guard.token()))
        .await
        .expect("stamp-only");
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(table.metadata().metadata_log().len(), base_log + 1);
    assert_eq!(table.metadata().current_snapshot_id(), Some(snapshot.get()));
    assert_eq!(
        table
            .metadata()
            .current_snapshot()
            .expect("head")
            .summary()
            .operation,
        Operation::Append
    );
    assert_stamped_head(&table, &stamp);
    assert_eq!(guard.outcome(), ScopeOutcome::Committed { snapshot });
    assert_eq!(live_ids(&table).await, vec![1]);
    let again = commit_stamp_only(&catalog, &table, &stamp, Some(guard.token()))
        .await
        .expect_err("claimed");
    assert_eq!(
        again,
        MicroBatchError::SinkCommittedTwice {
            epoch: Epoch::new(0)
        }
    );
    drop(guard);
    let next = stamp_for(1, SinkDoor::ForeachBatch);
    commit_stamp_only(&catalog, &table, &next, None)
        .await
        .expect("unscoped stamp-only");
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_stamped_head(&table, &next);
    assert_eq!(
        read_resume_point(&table, query()).expect("resume"),
        Some(next.record)
    );
}

async fn stamped_append(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    stamp: &CommitStamp,
    ids: &[i32],
) -> Table {
    let table = catalog.load_table(ident).await.expect("load");
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let files = stage(&table, ids).await;
    let committed = commit_append_with_summary(catalog, &table, files, &scoped(&guard), None)
        .await
        .expect("stamped append");
    assert!(matches!(guard.outcome(), ScopeOutcome::Committed { .. }));
    committed
}

#[tokio::test]
async fn resume_reads_the_last_offset_from_one_loaded_table() {
    let (_warehouse, catalog, ident) = fixture("resume").await;
    let empty = catalog.load_table(&ident).await.expect("load");
    assert_eq!(read_resume_point(&empty, query()).expect("first run"), None);
    for epoch in 0..3 {
        let id = i32::try_from(epoch).expect("id");
        stamped_append(&catalog, &ident, &stamp_for(epoch, SinkDoor::Table), &[id]).await;
    }
    append_plain(&catalog, &ident, &[50]).await;
    let other =
        QueryId::new(Uuid::parse_str("cccccccc-0000-4000-8000-0000000000c3").expect("uuid"));
    let foreign = CommitStamp {
        record: record_for(other, 9, 900),
        door: SinkDoor::Table,
    };
    stamped_append(&catalog, &ident, &foreign, &[60]).await;
    let table = catalog.load_table(&ident).await.expect("one read");
    assert_eq!(
        read_resume_point(&table, query()).expect("resume"),
        Some(stamp_for(2, SinkDoor::Table).record)
    );
    assert_eq!(
        read_resume_point(&table, other).expect("other query"),
        Some(foreign.record)
    );
}

#[tokio::test]
async fn resume_refuses_when_summary_and_property_disagree() {
    let (_warehouse, catalog, ident) = fixture("mismatch").await;
    let stamp = stamp_for(0, SinkDoor::Table);
    let table = stamped_append(&catalog, &ident, &stamp, &[1]).await;
    let (key, _) = stamp.record.property().expect("property");
    let (_, ahead) = stamp_for(1, SinkDoor::Table)
        .record
        .property()
        .expect("ahead");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(key.clone(), ahead)
        .apply(tx)
        .expect("apply");
    let table = tx.commit(catalog.as_ref()).await.expect("drift property");
    match read_resume_point(&table, query()).expect_err("mismatch") {
        MicroBatchError::RecoveryRequired {
            epoch,
            durable,
            reason:
                RecoveryReason::OffsetMismatch {
                    summary_epoch,
                    property_epoch,
                },
            ..
        } => {
            assert_eq!(epoch, Epoch::new(0));
            assert_eq!(durable.as_deref(), Some(&stamp.record));
            assert_eq!(summary_epoch, Some(Epoch::new(0)));
            assert_eq!(property_epoch, Some(Epoch::new(1)));
        }
        other => panic!("expected OffsetMismatch, got {other:?}"),
    }
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .remove(key)
        .apply(tx)
        .expect("apply");
    let table = tx.commit(catalog.as_ref()).await.expect("drop property");
    assert!(matches!(
        read_resume_point(&table, query()),
        Err(MicroBatchError::RecoveryRequired {
            reason: RecoveryReason::OffsetMismatch {
                summary_epoch: Some(_),
                property_epoch: None,
            },
            ..
        })
    ));
}

#[tokio::test]
async fn resume_refuses_a_property_whose_stamped_snapshot_expired() {
    let (_warehouse, catalog, ident) = fixture("expired").await;
    let stamp = stamp_for(0, SinkDoor::Table);
    let stamped = stamped_append(&catalog, &ident, &stamp, &[1]).await;
    let stamped_id = stamped.metadata().current_snapshot_id().expect("stamped");
    let table = append_plain(&catalog, &ident, &[2]).await;
    let tx = Transaction::new(&table);
    let tx = tx
        .expire_snapshots()
        .expire_snapshot_id(stamped_id)
        .apply(tx)
        .expect("apply expire");
    let table = tx.commit(catalog.as_ref()).await.expect("expire");
    assert!(table.metadata().snapshot_by_id(stamped_id).is_none());
    match read_resume_point(&table, query()).expect_err("expired") {
        MicroBatchError::RecoveryRequired {
            epoch,
            durable,
            reason: RecoveryReason::StampedSnapshotExpired,
            ..
        } => {
            assert_eq!(epoch, Epoch::new(0));
            assert_eq!(durable.as_deref(), Some(&stamp.record));
        }
        other => panic!("expected StampedSnapshotExpired, got {other:?}"),
    }
}

#[tokio::test]
async fn resume_refuses_a_newer_offset_format() {
    let (_warehouse, catalog, ident) = fixture("format").await;
    let stamp = stamp_for(0, SinkDoor::Table);
    let table = stamped_append(&catalog, &ident, &stamp, &[1]).await;
    let (key, value) = stamp.record.property().expect("property");
    let future = value.replace("\"format-version\":1", "\"format-version\":2");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(key, future)
        .apply(tx)
        .expect("apply");
    let table = tx.commit(catalog.as_ref()).await.expect("future property");
    assert!(matches!(
        read_resume_point(&table, query()),
        Err(MicroBatchError::UnsupportedOffsetFormat { ref found, supported: 1 }) if found == "2"
    ));
}

#[derive(Debug)]
struct RacingCatalog {
    inner: Arc<dyn Catalog>,
    loads: AtomicUsize,
    updates: AtomicUsize,
    racer: Mutex<Option<Vec<DataFile>>>,
}

impl RacingCatalog {
    fn new(inner: Arc<dyn Catalog>, racer: Vec<DataFile>) -> Self {
        Self {
            inner,
            loads: AtomicUsize::new(0),
            updates: AtomicUsize::new(0),
            racer: Mutex::new(Some(racer)),
        }
    }
}

#[async_trait]
impl Catalog for RacingCatalog {
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
        self.loads.fetch_add(1, Ordering::SeqCst);
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
        let racer = self.racer.lock().expect("racer lock").take();
        if let Some(files) = racer {
            let current = self.inner.load_table(commit.identifier()).await?;
            let tx = Transaction::new(&current);
            let tx = tx.fast_append().add_data_files(files).apply(tx)?;
            tx.commit(self.inner.as_ref()).await?;
        }
        self.inner.update_table(commit).await
    }
}

#[tokio::test]
async fn a_concurrent_unrelated_append_still_commits_both_halves_exactly_once() {
    let (_warehouse, memory, ident) = fixture("race").await;
    let table = append_plain(&memory, &ident, &[1]).await;
    let racer_files = stage(&table, &[99]).await;
    let racing = Arc::new(RacingCatalog::new(Arc::clone(&memory), racer_files));
    let catalog: Arc<dyn Catalog> = Arc::clone(&racing) as Arc<dyn Catalog>;
    let base_snapshots = table.metadata().snapshots().count();
    let stamp = stamp_for(0, SinkDoor::Table);
    let guard = BatchScope::enter(TableUuid::of(&table), stamp.clone()).expect("enter");
    let files = stage(&table, &[2]).await;
    let committed = commit_append_with_summary(&catalog, &table, files, &scoped(&guard), None)
        .await
        .expect("stamped append over a racer");
    assert_eq!(racing.updates.load(Ordering::SeqCst), 2);
    let reloaded = memory.load_table(&ident).await.expect("reload");
    for view in [&committed, &reloaded] {
        assert_eq!(view.metadata().snapshots().count(), base_snapshots + 2);
        assert_eq!(stamped_snapshots(view), 1);
        assert_stamped_head(view, &stamp);
        let head = view.metadata().current_snapshot().expect("head");
        let parent = view
            .metadata()
            .snapshot_by_id(head.parent_snapshot_id().expect("parent"))
            .expect("racer snapshot");
        assert!(
            !parent
                .summary()
                .additional_properties
                .contains_key(QUERY_ID_KEY)
        );
        assert_eq!(
            parent.parent_snapshot_id(),
            table.metadata().current_snapshot_id()
        );
    }
    assert_eq!(live_ids(&reloaded).await, vec![1, 2, 99]);
    assert_eq!(
        guard.outcome(),
        ScopeOutcome::Committed {
            snapshot: SnapshotId::new(reloaded.metadata().current_snapshot_id().expect("head"))
        }
    );
    let loads_before = racing.loads.load(Ordering::SeqCst);
    let resumed = catalog.load_table(&ident).await.expect("one read");
    assert_eq!(
        read_resume_point(&resumed, query()).expect("resume"),
        Some(stamp.record)
    );
    assert_eq!(racing.loads.load(Ordering::SeqCst), loads_before + 1);
}

#[test]
fn site_stamp_without_a_claim_borrows_the_caller_extras() {
    let extra = [(String::from("k"), String::from("v"))];
    let site = SiteStamp::default();
    assert!(matches!(
        site.extras(&extra).expect("extras"),
        std::borrow::Cow::Borrowed(_)
    ));
}

#[path = "sink_offsets_scope_tests.rs"]
mod scope;

#[path = "sink_offsets_probe_tests.rs"]
mod probe;

#[path = "sink_offsets_fence_tests.rs"]
mod fence;

#[path = "sink_offsets_epoch_tests.rs"]
mod epoch;
