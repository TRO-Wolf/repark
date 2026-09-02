use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    FormatVersion, ManifestContentType, NestedField, PrimitiveType, Schema, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use crate::write::concurrency::WriteConcurrency;
use crate::write::merge::{
    IsolationLevel, KnownPartitions, RowDeltaKind, RowDeltaPolicy, commit,
    commit_row_delta_kind_with_partitions,
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

async fn partitioned_v3_target(catalog: &Arc<dyn Catalog>) -> (Table, TableIdent) {
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
        .name("occparts".to_string())
        .schema(schema)
        .partition_spec(spec)
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create partitioned table");
    let ident = TableIdent::new(namespace, "occparts".to_string());
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
    let table = catalog.load_table(&ident).await.expect("load fresh");
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("part", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![0, 0, 1, 1])),
        ],
    )
    .expect("seed batch");
    let files = crate::write::append::write_partitioned_data_files(&table, vec![batch])
        .await
        .expect("write partitioned seed");
    commit(catalog, &table, None, Vec::new(), files)
        .await
        .expect("append the seed");
    let table = catalog.load_table(&ident).await.expect("reload seeded");
    (table, ident)
}

async fn live_partitions(table: &Table) -> KnownPartitions {
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

async fn pair_for_id(table: &Table, target_id: i32) -> (Arc<str>, i64) {
    let scan = table
        .scan()
        .select(["id", "_file", "_pos"])
        .build()
        .expect("scan identity");
    let batches: Vec<_> = scan
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect identity");
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .expect("id")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is Int32");
        let files = datafusion::arrow::compute::cast(
            batch.column_by_name("_file").expect("_file"),
            &DataType::Utf8,
        )
        .expect("cast _file");
        let files = files
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("_file Utf8");
        let positions = datafusion::arrow::compute::cast(
            batch.column_by_name("_pos").expect("_pos"),
            &DataType::Int64,
        )
        .expect("cast _pos");
        let positions = positions
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("_pos Int64");
        for row in 0..batch.num_rows() {
            if ids.value(row) == target_id {
                return (Arc::from(files.value(row)), positions.value(row));
            }
        }
    }
    panic!("id {target_id} is not a live row");
}

async fn live_ids(table: &Table) -> Vec<i32> {
    let scan = table.scan().select(["id"]).build().expect("scan ids");
    let batches: Vec<_> = scan
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect ids");
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is Int32");
        for row in 0..batch.num_rows() {
            ids.push(column.value(row));
        }
    }
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn the_production_partition_carrying_commit_honors_the_snapshot_pin() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let (table, ident) = partitioned_v3_target(&catalog).await;
    let known = live_partitions(&table).await;
    assert_eq!(known.len(), 2, "one data file per identity partition");
    let pin = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let policy = RowDeltaPolicy {
        kind: RowDeltaKind::Delete,
        isolation: IsolationLevel::Serializable,
    };
    commit_row_delta_kind_with_partitions(
        &catalog,
        &table,
        pin,
        vec![pair_for_id(&table, 1).await],
        Vec::new(),
        WriteConcurrency::new(1).expect("K=1"),
        policy,
        known.clone(),
    )
    .await
    .expect("the production partition-carrying commit lands on a partitioned v3 table");
    let after = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(live_ids(&after).await, vec![2, 3, 4]);

    let stale = commit_row_delta_kind_with_partitions(
        &catalog,
        &table,
        pin,
        vec![pair_for_id(&table, 2).await],
        Vec::new(),
        WriteConcurrency::new(1).expect("K=1"),
        policy,
        known,
    )
    .await;
    assert!(
        stale.is_err(),
        "a stale pin must still be rejected when the partition map is supplied"
    );
    let unchanged = catalog.load_table(&ident).await.expect("reload");
    assert_eq!(live_ids(&unchanged).await, vec![2, 3, 4]);
}
