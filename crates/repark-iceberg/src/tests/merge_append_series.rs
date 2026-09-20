use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, ManifestContentType,
    NestedField, PrimitiveType, Schema, Struct, Transform, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

pub(crate) const NAMESPACE: &str = "ns";

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Probe {
    pub(crate) appends: usize,
    pub(crate) manifests: usize,
    pub(crate) data_files: usize,
}

pub(crate) fn truth_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../python/repark/tests/ice_merge_append_1_truth.json")
}

pub(crate) fn spark_series(variant: &str) -> Vec<Probe> {
    let raw = std::fs::read_to_string(truth_path()).expect("read the recorded Spark fixture");
    let document: serde_json::Value =
        serde_json::from_str(&raw).expect("parse the recorded Spark fixture");
    document["series"][variant]
        .as_array()
        .unwrap_or_else(|| panic!("fixture carries a `{variant}` series"))
        .iter()
        .map(|row| Probe {
            appends: usize::try_from(row["appends"].as_u64().expect("appends")).expect("usize"),
            manifests: usize::try_from(row["manifests"].as_u64().expect("manifests"))
                .expect("usize"),
            data_files: usize::try_from(row["data_files"].as_u64().expect("data_files"))
                .expect("usize"),
        })
        .collect()
}

pub(crate) fn variant_properties(variant: &str) -> HashMap<String, String> {
    let mut properties: HashMap<String, String> = HashMap::new();
    match variant {
        "defaults" => {}
        "min_count_5" => {
            properties.insert(
                "commit.manifest.min-count-to-merge".to_string(),
                "5".to_string(),
            );
        }
        "merge_disabled" => {
            properties.insert(
                "commit.manifest-merge.enabled".to_string(),
                "false".to_string(),
            );
        }
        other => panic!("unknown variant `{other}`"),
    }
    properties
}

pub(crate) async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("build memory catalog"),
    );
    catalog
        .create_namespace(&NamespaceIdent::new(NAMESPACE.to_string()), HashMap::new())
        .await
        .expect("create namespace");
    catalog
}

pub(crate) fn series_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build series schema")
}

pub(crate) async fn create_series_table(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    properties: HashMap<String, String>,
    partition_field: Option<&str>,
) -> TableIdent {
    let builder = TableCreation::builder()
        .name(name.to_string())
        .schema(series_schema())
        .format_version(FormatVersion::V2)
        .properties(properties);
    let creation = match partition_field {
        None => builder.build(),
        Some(field) => builder
            .partition_spec(
                UnboundPartitionSpec::builder()
                    .add_partition_field(1, field.to_string(), Transform::Identity)
                    .expect("add identity partition field")
                    .build(),
            )
            .build(),
    };
    catalog
        .create_table(&NamespaceIdent::new(NAMESPACE.to_string()), creation)
        .await
        .expect("create table");
    TableIdent::new(NamespaceIdent::new(NAMESPACE.to_string()), name.to_string())
}

pub(crate) fn synthetic_data_file(
    step: usize,
    spec_id: i32,
    partition: Struct,
) -> iceberg::spec::DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("series/step-{step}.parquet"))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(571)
        .record_count(1)
        .partition_spec_id(spec_id)
        .partition(partition)
        .build()
        .expect("build synthetic data file")
}

pub(crate) async fn manifest_and_file_counts(table: &Table) -> (usize, usize) {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return (0, 0);
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .expect("load manifest list");
    let entries = manifest_list.entries();
    let data_files = entries
        .iter()
        .filter(|entry| entry.content == ManifestContentType::Data)
        .map(|entry| {
            usize::try_from(entry.added_files_count.unwrap_or(0)).expect("usize")
                + usize::try_from(entry.existing_files_count.unwrap_or(0)).expect("usize")
        })
        .sum();
    (entries.len(), data_files)
}

pub(crate) async fn replay_series(variant: &str) -> Vec<Probe> {
    let expected = spark_series(variant);
    let probes: Vec<usize> = expected.iter().map(|row| row.appends).collect();
    let steps = *probes.last().expect("a non-empty probe list");
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_series_table(
        &catalog,
        &format!("t_{variant}"),
        variant_properties(variant),
        None,
    )
    .await;
    let mut measured = Vec::with_capacity(expected.len());
    for step in 1..=steps {
        let table = catalog.load_table(&ident).await.expect("load table");
        let file = synthetic_data_file(step, 0, Struct::empty());
        crate::write::commit_append(&catalog, &table, vec![file])
            .await
            .expect("append commit");
        if probes.contains(&step) {
            let table = catalog.load_table(&ident).await.expect("reload table");
            let (manifests, data_files) = manifest_and_file_counts(&table).await;
            measured.push(Probe {
                appends: step,
                manifests,
                data_files,
            });
        }
    }
    measured
}

async fn assert_series_matches_spark(variant: &str) {
    let expected = spark_series(variant);
    let measured = replay_series(variant).await;
    assert_eq!(
        measured, expected,
        "the `{variant}` manifest/data-file series must reproduce the recorded Spark 4.1.2 numbers"
    );
}

#[tokio::test]
async fn defaults_series_matches_spark() {
    assert_series_matches_spark("defaults").await;
}

#[tokio::test]
async fn min_count_to_merge_series_matches_spark() {
    assert_series_matches_spark("min_count_5").await;
}

#[tokio::test]
async fn merge_disabled_series_matches_spark() {
    assert_series_matches_spark("merge_disabled").await;
}
