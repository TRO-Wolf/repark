use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
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

const DATA_MANIFEST_COUNT: usize = 192;
const NEWEST_ID: i32 = 191;

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

async fn seeded_table(catalog: &Arc<dyn Catalog>, manifests: usize) -> (Table, TableIdent) {
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
    for part in 0..i32::try_from(manifests).expect("manifest count fits i32") {
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
    (catalog.load_table(&ident).await.expect("reload"), ident)
}

async fn data_manifest_paths(table: &Table) -> Vec<String> {
    let metadata = table.metadata();
    let snapshot = metadata.current_snapshot().expect("current snapshot");
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("manifest list");
    manifest_list
        .entries()
        .iter()
        .filter(|manifest_file| manifest_file.content == ManifestContentType::Data)
        .map(|manifest_file| manifest_file.manifest_path.clone())
        .collect()
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

async fn manifest_holding_path(table: &Table, data_file_path: &str) -> String {
    let metadata = table.metadata();
    let snapshot = metadata.current_snapshot().expect("snapshot");
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .expect("manifest list");
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("data manifest");
        for entry in manifest.entries() {
            if entry.is_alive() && entry.data_file().file_path() == data_file_path {
                return manifest_file.manifest_path.clone();
            }
        }
    }
    panic!("no data manifest names {data_file_path}");
}

struct HiddenManifests {
    moved: Vec<(PathBuf, PathBuf)>,
}

impl HiddenManifests {
    fn hide(paths: &[String]) -> Self {
        let mut moved = Vec::new();
        for path in paths {
            let from = PathBuf::from(path);
            let to = from.with_extension("avro.hidden");
            fs::rename(&from, &to).expect("hide data manifest");
            moved.push((from, to));
        }
        Self { moved }
    }
}

impl Drop for HiddenManifests {
    fn drop(&mut self) {
        for (from, to) in &self.moved {
            let _ = fs::rename(to, from);
        }
    }
}

fn delete_policy() -> RowDeltaPolicy {
    RowDeltaPolicy {
        kind: RowDeltaKind::Delete,
        isolation: IsolationLevel::Serializable,
    }
}

#[tokio::test]
async fn a_newest_file_identity_delete_commits_with_one_data_manifest() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let (table, ident) = seeded_table(&catalog, DATA_MANIFEST_COUNT).await;
    let paths = data_manifest_paths(&table).await;
    assert_eq!(paths.len(), DATA_MANIFEST_COUNT);
    let pair = pair_for_id(&table, NEWEST_ID).await;
    let touched = pair.0.to_string();
    let mut known = live_partitions(&table).await;
    known.retain(|path, _| path == &touched);
    assert!(
        known.contains_key(&touched),
        "the identity scan must record the newest touched path"
    );
    let keep = manifest_holding_path(&table, &touched).await;
    let hide: Vec<String> = paths.into_iter().filter(|path| path != &keep).collect();
    assert_eq!(hide.len(), DATA_MANIFEST_COUNT - 1);
    let hidden = HiddenManifests::hide(&hide);
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    commit_row_delta_kind_with_partitions(
        &catalog,
        &table,
        snapshot_id,
        vec![pair],
        Vec::new(),
        WriteConcurrency::new(1).expect("K=1"),
        delete_policy(),
        known,
    )
    .await
    .expect("F-25 stops once the newest added DV key is found");
    drop(hidden);
    let after = catalog.load_table(&ident).await.expect("reload");
    let ids = live_ids(&after).await;
    assert_eq!(ids.len(), DATA_MANIFEST_COUNT - 1);
    assert!(!ids.contains(&NEWEST_ID));
}

#[tokio::test]
async fn hiding_the_newest_data_manifest_too_refuses_the_commit() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let (table, _) = seeded_table(&catalog, DATA_MANIFEST_COUNT).await;
    let paths = data_manifest_paths(&table).await;
    let pair = pair_for_id(&table, NEWEST_ID).await;
    let touched = pair.0.to_string();
    let mut known = live_partitions(&table).await;
    known.retain(|path, _| path == &touched);
    let hidden = HiddenManifests::hide(&paths);
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let refused = commit_row_delta_kind_with_partitions(
        &catalog,
        &table,
        snapshot_id,
        vec![pair],
        Vec::new(),
        WriteConcurrency::new(1).expect("K=1"),
        delete_policy(),
        known,
    )
    .await;
    assert!(
        refused.is_err(),
        "commit still needs the one data manifest that holds the added DV key"
    );
    drop(hidden);
}

#[tokio::test]
async fn execute_predicate_dml_deletes_the_newest_id_on_a_192_manifest_table() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let (_table, ident) = seeded_table(&catalog, DATA_MANIFEST_COUNT).await;
    let ctx = SessionContext::new();
    crate::write::predicate_dml::execute_predicate_dml(
        &ctx,
        &catalog,
        &crate::write::predicate_dml::PredicateDmlSpec {
            target: ident.clone(),
            target_alias: "t".to_string(),
            selection_sql: format!("id = {NEWEST_ID}"),
            assignments: None,
        },
    )
    .await
    .expect("plain identity delete of the newest row");
    let table = catalog.load_table(&ident).await.expect("reload");
    let ids = live_ids(&table).await;
    assert_eq!(ids.len(), DATA_MANIFEST_COUNT - 1);
    assert!(!ids.contains(&NEWEST_ID));
}
