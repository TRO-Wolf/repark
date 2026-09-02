use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::StreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    ManifestContentType, NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec,
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
