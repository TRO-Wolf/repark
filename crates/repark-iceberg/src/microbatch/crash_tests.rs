use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use repark_common::Generation;
use tempfile::TempDir;

use crate::microbatch::offset::{
    EPOCH_KEY, Epoch, InputOffset, OffsetFormatVersion, OffsetVector, QUERY_ID_KEY, QueryId, RunId,
    SinkDoor, SinkRecord, TableUuid,
};
use crate::microbatch::provider::provider_for_plan;
use crate::microbatch::window::{ReadCaps, StartPosition, WindowLimit, WindowPlan, WindowPlanner};
use crate::write::sink_offsets::{BatchScope, CommitStamp, ScopeOutcome, read_resume_point};
use crate::write::write_options::commit_append_with_summary;

const QUERY_NAME: &str = "orders";

struct Harness {
    _warehouse: TempDir,
    catalog: Arc<dyn Catalog>,
    bronze: TableIdent,
    sink: TableIdent,
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

async fn bronze_append(harness: &Harness, ids: &[i32]) {
    let table = harness
        .catalog
        .load_table(&harness.bronze)
        .await
        .expect("load bronze");
    let files = crate::write::merge::write_data_files(&table, vec![id_batch(ids)])
        .await
        .expect("stage bronze");
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)
        .expect("apply bronze append");
    tx.commit(harness.catalog.as_ref())
        .await
        .expect("commit bronze append");
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

fn query_of(sink: &Table) -> QueryId {
    QueryId::derive(TableUuid::of(sink), Some(QUERY_NAME))
}

async fn resume(harness: &Harness) -> (Table, Option<SinkRecord>) {
    let sink = harness
        .catalog
        .load_table(&harness.sink)
        .await
        .expect("load sink");
    let record = read_resume_point(&sink, query_of(&sink)).expect("resume point");
    (sink, record)
}

async fn plan_window(harness: &Harness, resumed: Option<&SinkRecord>) -> Option<WindowPlan> {
    let bronze = harness
        .catalog
        .load_table(&harness.bronze)
        .await
        .expect("load bronze");
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
    planner
        .next_window(&from, WindowLimit::Unbounded)
        .await
        .expect("window")
}

async fn read_window(harness: &Harness, plan: &WindowPlan) -> Vec<RecordBatch> {
    let bronze = harness
        .catalog
        .load_table(&harness.bronze)
        .await
        .expect("load bronze");
    let schema = bronze.metadata().current_schema().clone();
    let provider = provider_for_plan(bronze, plan, &schema).expect("provider");
    let ctx = SessionContext::new();
    ctx.register_table("window", provider).expect("register");
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

async fn run_epoch(harness: &Harness, run: RunId) -> Option<Epoch> {
    let (sink, resumed) = resume(harness).await;
    let epoch = resumed
        .as_ref()
        .map_or(Epoch::FIRST, |record| record.epoch.next());
    let plan = plan_window(harness, resumed.as_ref()).await?;
    let batches = read_window(harness, &plan).await;
    let files = crate::write::merge::write_data_files(&sink, batches)
        .await
        .expect("stage sink");
    let stamp = CommitStamp {
        record: record_for(query_of(&sink), run, epoch, plan.end.clone()),
        door: SinkDoor::Table,
    };
    let guard = BatchScope::enter(TableUuid::of(&sink), stamp).expect("enter scope");
    commit_append_with_summary(&harness.catalog, &sink, files, &[], None)
        .await
        .expect("stamped sink commit");
    assert!(matches!(guard.outcome(), ScopeOutcome::Committed { .. }));
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
