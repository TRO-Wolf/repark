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

async fn append_steps(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    steps: std::ops::RangeInclusive<usize>,
    spec_id: i32,
    partition: impl Fn(usize) -> Struct,
    branch: Option<&str>,
) {
    for step in steps {
        let table = catalog.load_table(ident).await.expect("load table");
        let file = synthetic_data_file(step, spec_id, partition(step));
        crate::write::commit_append_to(catalog, &table, vec![file], branch)
            .await
            .expect("append commit");
    }
}

async fn manifest_entries(table: &Table) -> Vec<iceberg::spec::ManifestFile> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Vec::new();
    };
    snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .expect("load manifest list")
        .entries()
        .to_vec()
}

#[tokio::test]
async fn merge_never_mixes_partition_spec_ids() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_series_table(
        &catalog,
        "t_multi_spec",
        variant_properties("defaults"),
        None,
    )
    .await;

    append_steps(&catalog, &ident, 1..=60, 0, |_| Struct::empty(), None).await;

    crate::write::partition_spec::apply_partition_spec_changes(
        catalog.as_ref(),
        &ident,
        &[
            crate::write::partition_spec::PartitionSpecChange::AddField {
                source_name: "id".to_string(),
                transform: Transform::Identity,
                name: Some("id_part".to_string()),
            },
        ],
    )
    .await
    .expect("evolve the partition spec");

    let evolved = catalog
        .load_table(&ident)
        .await
        .expect("load evolved table");
    let new_spec_id = evolved.metadata().default_partition_spec().spec_id();
    assert_eq!(new_spec_id, 1, "the spec evolution must mint spec id 1");

    append_steps(
        &catalog,
        &ident,
        61..=120,
        new_spec_id,
        |step| {
            Struct::from_iter([Some(iceberg::spec::Literal::int(
                i32::try_from(step).expect("i32"),
            ))])
        },
        None,
    )
    .await;

    let table = catalog.load_table(&ident).await.expect("reload table");
    let manifests = manifest_entries(&table).await;
    let spec_ids: std::collections::BTreeSet<i32> = manifests
        .iter()
        .map(|manifest| manifest.partition_spec_id)
        .collect();
    assert_eq!(
        spec_ids,
        std::collections::BTreeSet::from([0, 1]),
        "both spec ids must survive the merge — Java never merges across spec ids"
    );
    let (_, data_files) = manifest_and_file_counts(&table).await;
    assert_eq!(
        data_files, 120,
        "every appended file stays live through the merge"
    );
    assert!(
        manifests.len() < 120,
        "the merge must have fired: {} manifests for 120 appends",
        manifests.len()
    );
}

#[tokio::test]
async fn delete_manifests_carry_forward_through_a_merge() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_series_table(&catalog, "t_mor", variant_properties("defaults"), None).await;

    append_steps(&catalog, &ident, 1..=1, 0, |_| Struct::empty(), None).await;

    let table = catalog.load_table(&ident).await.expect("load table");
    let delete_file = DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path("series/deletes-1.parquet".to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .expect("build position-delete file");
    let tx = iceberg::transaction::Transaction::new(&table);
    let action = tx.row_delta().add_deletes(vec![delete_file]);
    let tx =
        iceberg::transaction::ApplyTransactionAction::apply(action, tx).expect("apply row delta");
    tx.commit(catalog.as_ref())
        .await
        .expect("commit the position delete");

    let before = catalog.load_table(&ident).await.expect("load after delete");
    let delete_manifests_before: Vec<String> = manifest_entries(&before)
        .await
        .into_iter()
        .filter(|manifest| manifest.content == ManifestContentType::Deletes)
        .map(|manifest| manifest.manifest_path)
        .collect();
    assert_eq!(
        delete_manifests_before.len(),
        1,
        "the row delta must have written exactly one DELETE manifest"
    );

    append_steps(&catalog, &ident, 2..=120, 0, |_| Struct::empty(), None).await;

    let after = catalog.load_table(&ident).await.expect("reload table");
    let manifests = manifest_entries(&after).await;
    let delete_manifests_after: Vec<String> = manifests
        .iter()
        .filter(|manifest| manifest.content == ManifestContentType::Deletes)
        .map(|manifest| manifest.manifest_path.clone())
        .collect();
    assert_eq!(
        delete_manifests_after, delete_manifests_before,
        "the DELETE manifest must carry forward byte-identically through every merging append"
    );
    let data_manifests = manifests
        .iter()
        .filter(|manifest| manifest.content == ManifestContentType::Data)
        .count();
    assert!(
        data_manifests < 120,
        "the DATA manifests must still merge beside the untouched DELETE manifest (got {data_manifests})"
    );
}

#[tokio::test]
async fn sequence_numbers_and_file_provenance_survive_a_merge() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident = create_series_table(&catalog, "t_seq", variant_properties("defaults"), None).await;
    append_steps(&catalog, &ident, 1..=100, 0, |_| Struct::empty(), None).await;

    let table = catalog.load_table(&ident).await.expect("reload table");
    let manifests = manifest_entries(&table).await;
    assert_eq!(
        manifests.len(),
        1,
        "the hundredth append merges to ONE manifest"
    );

    let mut measured: Vec<(String, i64)> = Vec::new();
    for manifest in &manifests {
        let loaded = manifest
            .load_manifest(table.file_io())
            .await
            .expect("load manifest");
        for entry in loaded.entries() {
            measured.push((
                entry.data_file().file_path().to_string(),
                entry
                    .sequence_number()
                    .expect("an inherited sequence number"),
            ));
        }
    }
    measured.sort();
    let expected: Vec<(String, i64)> = (1..=100)
        .map(|step| {
            (
                format!("series/step-{step}.parquet"),
                i64::try_from(step).expect("i64"),
            )
        })
        .collect();
    let mut expected_sorted = expected;
    expected_sorted.sort();
    assert_eq!(
        measured, expected_sorted,
        "each merged entry keeps the sequence number of the append that added it"
    );
}

#[tokio::test]
async fn branch_merging_append_moves_only_the_branch() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let catalog = memory_catalog(&warehouse).await;
    let ident =
        create_series_table(&catalog, "t_branch", variant_properties("defaults"), None).await;

    append_steps(&catalog, &ident, 1..=1, 0, |_| Struct::empty(), None).await;
    let main_after_seed = catalog
        .load_table(&ident)
        .await
        .expect("load table")
        .metadata()
        .current_snapshot_id();

    append_steps(
        &catalog,
        &ident,
        2..=100,
        0,
        |_| Struct::empty(),
        Some("feat"),
    )
    .await;

    let table = catalog.load_table(&ident).await.expect("reload table");
    assert_eq!(
        table.metadata().current_snapshot_id(),
        main_after_seed,
        "a branch-targeted merging append must not move main"
    );
    let branch = table
        .metadata()
        .snapshot_for_ref("feat")
        .expect("the branch ref exists");
    let manifest_list = branch
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .expect("load the branch manifest list");
    assert_eq!(
        manifest_list.entries().len(),
        1,
        "the hundredth manifest ON THE BRANCH (the carried seed plus 99 branch appends) merges to one"
    );
    let live: usize = manifest_list
        .entries()
        .iter()
        .map(|entry| {
            usize::try_from(entry.added_files_count.unwrap_or(0)).expect("usize")
                + usize::try_from(entry.existing_files_count.unwrap_or(0)).expect("usize")
        })
        .sum();
    assert_eq!(
        live, 100,
        "the branch carries the seed plus its own 99 files"
    );
}
