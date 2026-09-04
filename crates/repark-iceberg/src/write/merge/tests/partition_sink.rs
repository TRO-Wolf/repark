use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::SessionContext;
use futures::{StreamExt, TryStreamExt};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    FormatVersion, ManifestContentType, NestedField, PrimitiveType, Schema, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use crate::write::merge::{
    KnownPartitions, TargetScanStream, commit, drain_partition_sink, new_partition_sink,
    scratch_schema,
};

async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();
    Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "mem",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("memory catalog"),
    )
}

async fn partitioned_table(catalog: &Arc<dyn Catalog>) -> Table {
    let namespace = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "part", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("schema");
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "part", Transform::Identity)
        .expect("identity partition field")
        .build();
    let creation = TableCreation::builder()
        .name("parts".to_string())
        .schema(schema)
        .partition_spec(spec)
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create partitioned table");
    let ident = TableIdent::new(namespace, "parts".to_string());
    let table = catalog.load_table(&ident).await.expect("load");
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("part", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(Int32Array::from(vec![0, 0, 1, 1, 2, 2])),
        ],
    )
    .expect("seed batch");
    let files = crate::write::append::write_partitioned_data_files(&table, vec![batch])
        .await
        .expect("write partitioned seed");
    assert_eq!(files.len(), 3, "one data file per identity partition");
    commit(catalog, &table, None, Vec::new(), files)
        .await
        .expect("append the seed");
    catalog.load_table(&ident).await.expect("reload")
}

async fn manifest_partitions(table: &Table) -> KnownPartitions {
    let metadata = table.metadata();
    let snapshot = metadata.current_snapshot().expect("snapshot");
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("manifest list");
    let mut known = KnownPartitions::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("data manifest");
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let file = entry.data_file();
            known.insert(
                file.file_path().to_string(),
                (file.partition_spec_id(), file.partition().clone()),
            );
        }
    }
    known
}

#[tokio::test]
async fn the_target_scan_records_every_planned_file_partition() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = partitioned_table(&catalog).await;
    let write_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .expect("write schema"),
    );
    let scratch = scratch_schema(&write_schema);
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let sink = new_partition_sink();
    let stream = TargetScanStream::new(
        table.clone(),
        snapshot_id,
        Arc::clone(&scratch),
        &write_schema,
        None,
        Some(1),
        None,
    )
    .with_partition_sink(Arc::clone(&sink));
    let mut rows = 0;
    let mut batches = stream.execute(Arc::new(TaskContext::default()));
    while let Some(batch) = batches.next().await {
        rows += batch.expect("scan batch").num_rows();
    }
    assert_eq!(rows, 6);
    let recorded = drain_partition_sink(&sink);
    assert_eq!(recorded, manifest_partitions(&table).await);
    assert_eq!(recorded.len(), 3);
    assert!(
        recorded
            .values()
            .all(|(_, partition)| partition.fields().len() == 1),
        "an identity-partitioned file must carry its one partition value"
    );
}

async fn eight_manifest_v3_table(catalog: &Arc<dyn Catalog>) -> Table {
    let namespace = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "part", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("schema");
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "part", Transform::Identity)
        .expect("identity partition field")
        .build();
    let creation = TableCreation::builder()
        .name("puredv".to_string())
        .schema(schema)
        .partition_spec(spec)
        .properties(HashMap::from([(
            "commit.manifest-merge.enabled".to_string(),
            "false".to_string(),
        )]))
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create");
    let ident = TableIdent::new(namespace, "puredv".to_string());
    crate::write::format_version::set_properties_and_format_version(
        catalog.as_ref(),
        &ident,
        None,
        HashMap::new(),
        &[],
        Some(FormatVersion::V3),
    )
    .await
    .expect("upgrade to v3");
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("part", DataType::Int32, false),
    ]));
    for part in 0..8_i32 {
        let table = catalog.load_table(&ident).await.expect("load");
        let batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(Int32Array::from(vec![part])),
                Arc::new(Int32Array::from(vec![part])),
            ],
        )
        .expect("seed batch");
        let files = crate::write::append::write_partitioned_data_files(&table, vec![batch])
            .await
            .expect("write");
        commit(catalog, &table, None, Vec::new(), files)
            .await
            .expect("append");
    }
    catalog.load_table(&ident).await.expect("reload")
}

async fn planned_file_tasks(table: &Table) -> Vec<FileScanTask> {
    table
        .scan()
        .select(["id", "_file", "_pos"])
        .build()
        .expect("scan")
        .plan_files()
        .await
        .expect("plan")
        .try_collect()
        .await
        .expect("collect tasks")
}

async fn touched_file_for_id_zero(table: &Table) -> String {
    let row_scan = table
        .scan()
        .select(["id", "_file"])
        .build()
        .expect("row scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect rows");
    for batch in &row_scan {
        let ids = batch
            .column_by_name("id")
            .expect("id")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id i32");
        let files_col = cast(
            batch.column_by_name("_file").expect("_file"),
            &DataType::Utf8,
        )
        .expect("cast _file");
        let files = files_col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("_file utf8");
        for index in 0..batch.num_rows() {
            if ids.value(index) == 0 {
                return files.value(index).to_string();
            }
        }
    }
    panic!("id 0");
}

async fn drain_identity_sql_sink(table: &Table, touched: &str) -> KnownPartitions {
    let write_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .expect("write schema"),
    );
    let scratch = scratch_schema(&write_schema);
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let partitions = new_partition_sink();
    let source: Arc<dyn PartitionStream> = Arc::new(
        TargetScanStream::new(
            table.clone(),
            snapshot_id,
            Arc::clone(&scratch),
            &write_schema,
            None,
            Some(1),
            None,
        )
        .with_partition_sink(Arc::clone(&partitions)),
    );
    let ctx = SessionContext::new();
    let target_name = super::super::register_streaming_target(&ctx, Arc::clone(&scratch), source)
        .expect("register streaming target");
    let sql = format!("SELECT \"_file\", \"_pos\" FROM {target_name} AS t WHERE id = 0");
    let mut stream = ctx
        .sql(&sql)
        .await
        .expect("plan identity sql")
        .execute_stream()
        .await
        .expect("execute identity sql");
    let mut pair_paths = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("identity batch");
        let files_col = cast(
            batch.column_by_name("_file").expect("_file"),
            &DataType::Utf8,
        )
        .expect("cast");
        let files = files_col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        for index in 0..batch.num_rows() {
            pair_paths.push(files.value(index).to_string());
        }
    }
    assert_eq!(pair_paths.len(), 1, "id 0 is one row");
    assert_eq!(pair_paths[0], touched);
    drain_partition_sink(&partitions)
}

fn puredv_ident() -> TableIdent {
    TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "puredv".to_string(),
    )
}

#[tokio::test]
async fn a_multi_manifest_identity_scan_records_the_touched_path() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = eight_manifest_v3_table(&catalog).await;
    let planned = planned_file_tasks(&table).await;
    assert_eq!(planned.len(), 8, "one task per data file");
    let missing_spec = planned
        .iter()
        .filter(|task| task.partition_spec.is_none())
        .count();
    let missing_partition = planned
        .iter()
        .filter(|task| task.partition.is_none())
        .count();
    assert_eq!(missing_spec, 0, "every FileScanTask carries partition_spec");
    assert_eq!(
        missing_partition, 0,
        "every FileScanTask carries partition values"
    );
    let touched = touched_file_for_id_zero(&table).await;
    let drained = drain_identity_sql_sink(&table, &touched).await;
    assert!(
        drained.contains_key(&touched),
        "production identity SQL must leave the touched path in the sink; \
         drained={} touched={touched:?}",
        drained.len(),
    );
    assert_eq!(
        drained,
        manifest_partitions(&table).await,
        "plan-once must record the same known_partitions map as a full manifest walk"
    );
}

async fn live_ids(table: &Table) -> Vec<i32> {
    let batches: Vec<_> = table
        .scan()
        .select(["id"])
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect");
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("id")
            .expect("id")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("i32");
        ids.extend(column.iter().map(|value| value.expect("id")));
    }
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn execute_predicate_dml_deletes_id_zero_on_an_eight_manifest_table() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let _table = eight_manifest_v3_table(&catalog).await;
    let ident = puredv_ident();
    let ctx = SessionContext::new();
    crate::write::predicate_dml::execute_predicate_dml(
        &ctx,
        &catalog,
        &crate::write::predicate_dml::PredicateDmlSpec {
            target: ident.clone(),
            target_alias: "t".to_string(),
            selection_sql: "id = 0".to_string(),
            assignments: None,
        },
    )
    .await
    .expect("plain identity delete");
    let table = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(live_ids(&table).await, vec![1, 2, 3, 4, 5, 6, 7]);
}

async fn drain_scan_rows(
    stream: &mut datafusion::physical_plan::SendableRecordBatchStream,
) -> usize {
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.expect("scan batch").num_rows();
    }
    rows
}

#[tokio::test]
async fn three_concurrent_target_scan_executes_plan_data_manifests_once() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let table = eight_manifest_v3_table(&catalog).await;
    let write_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .expect("write schema"),
    );
    let scratch = scratch_schema(&write_schema);
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let sink = new_partition_sink();
    let stream = TargetScanStream::new(
        table.clone(),
        snapshot_id,
        Arc::clone(&scratch),
        &write_schema,
        None,
        Some(1),
        None,
    )
    .with_partition_sink(Arc::clone(&sink));
    let counter = Arc::new(AtomicUsize::new(0));
    super::super::target_scan::PLAN_FILES_INVOCATIONS.with(|slot| {
        *slot.borrow_mut() = Some(Arc::clone(&counter));
    });
    let task_ctx = Arc::new(TaskContext::default());
    let mut first = stream.execute(Arc::clone(&task_ctx));
    let mut second = stream.execute(Arc::clone(&task_ctx));
    let mut third = stream.execute(Arc::clone(&task_ctx));
    let (first_rows, second_rows, third_rows) = tokio::join!(
        drain_scan_rows(&mut first),
        drain_scan_rows(&mut second),
        drain_scan_rows(&mut third),
    );
    super::super::target_scan::PLAN_FILES_INVOCATIONS.with(|slot| {
        *slot.borrow_mut() = None;
    });
    assert_eq!(first_rows, 8);
    assert_eq!(second_rows, 8);
    assert_eq!(third_rows, 8);
    let calls = counter.load(Ordering::SeqCst);
    assert_eq!(
        calls, 1,
        "three concurrent StreamingTable executes must plan once, not {calls}"
    );
    assert_eq!(
        drain_partition_sink(&sink),
        manifest_partitions(&table).await
    );
}
