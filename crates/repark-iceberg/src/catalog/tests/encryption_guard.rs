use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use crate::catalog::EncryptionGuardCatalog;
use crate::write::EncryptedTableRefusal;

const KEY: &str = "SEKRETKEYVAL9f3a7";
const REFUSAL: &str = "Table sales.t carries property 'encryption.key-id': RePark has no table \
                       encryption and refuses to write plaintext into a table that asks for it \
                       (ENC-1).";

struct Bed {
    _warehouse: TempDir,
    raw: Arc<dyn Catalog>,
    guarded: Arc<dyn Catalog>,
}

fn ident() -> TableIdent {
    TableIdent::from_strs(["sales", "t"]).expect("ident")
}

async fn bed(properties: &[(&str, &str)]) -> Bed {
    let warehouse = TempDir::new().expect("warehouse");
    let root = warehouse.path().to_str().expect("utf-8 path").to_string();
    let raw: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), root)]),
            )
            .await
            .expect("memory catalog"),
    );
    let guarded = EncryptionGuardCatalog::install(Arc::clone(&raw));
    let namespace = NamespaceIdent::new("sales".to_string());
    guarded
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("schema");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .properties(
            properties
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
        )
        .build();
    guarded
        .create_table(&namespace, creation)
        .await
        .expect("create");
    Bed {
        _warehouse: warehouse,
        raw,
        guarded,
    }
}

fn data_path(table: &Table, name: &str) -> String {
    format!("{}/data/{name}", table.metadata().location())
}

fn assert_refused<T: std::fmt::Debug>(result: iceberg::Result<T>) {
    let error = result.expect_err("a keyed table must refuse");
    assert_eq!(error.kind(), iceberg::ErrorKind::FeatureUnsupported);
    let refusal = EncryptedTableRefusal::find(&error).expect("typed refusal in the chain");
    assert_eq!(refusal.to_string(), REFUSAL);
    assert!(!error.to_string().contains(KEY), "{error}");
}

async fn snapshot_commit(catalog: &dyn Catalog, table: &Table) -> iceberg::Result<Table> {
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .set_snapshot_properties(HashMap::from([("probe".to_string(), "1".to_string())]))
        .apply(tx)?;
    tx.commit(catalog).await
}

#[tokio::test]
async fn a_keyed_table_handle_creates_no_file_but_metadata_json() {
    let bed = bed(&[("encryption.key-id", KEY)]).await;
    let table = bed.guarded.load_table(&ident()).await.expect("load");
    let file_io = table.file_io();
    for name in [
        "a.parquet",
        "a-deletes.parquet",
        "a.puffin",
        "a-m0.avro",
        "snap-1.avro",
        "a.stats",
        "partition-stats-1.parquet",
        "file-list",
        "metadata.json.bak",
    ] {
        let path = data_path(&table, name);
        let output = file_io.new_output(&path).expect("output handle");
        assert_refused(output.write(Bytes::from_static(b"x")).await);
        assert_refused(output.writer().await.map(|_| ()));
        assert_refused(file_io.write_new(&path, Bytes::from_static(b"x")).await);
        assert!(!file_io.exists(&path).await.expect("exists"), "{path}");
    }
    let metadata_json = format!(
        "{}/metadata/00009-probe.metadata.json",
        table.metadata().location()
    );
    file_io
        .new_output(&metadata_json)
        .expect("output handle")
        .write(Bytes::from_static(b"{}"))
        .await
        .expect("metadata JSON is the one named exception");
    assert_eq!(
        file_io
            .new_input(&metadata_json)
            .expect("input")
            .read()
            .await
            .expect("read"),
        Bytes::from_static(b"{}")
    );
    file_io.delete(&metadata_json).await.expect("delete");
}

#[tokio::test]
async fn an_unkeyed_or_lookalike_table_handle_writes_as_before() {
    let bed = bed(&[
        ("encryption.keyid", KEY),
        ("encryption.key-id-x", KEY),
        ("Encryption.Key-ID", KEY),
    ])
    .await;
    let table = bed.guarded.load_table(&ident()).await.expect("load");
    let path = data_path(&table, "a.parquet");
    table
        .file_io()
        .new_output(&path)
        .expect("output handle")
        .write(Bytes::from_static(b"x"))
        .await
        .expect("an unkeyed table writes");
    let committed = snapshot_commit(bed.guarded.as_ref(), &table)
        .await
        .expect("an unkeyed table commits a snapshot");
    assert_eq!(committed.metadata().snapshots().count(), 1);
}

#[tokio::test]
async fn a_snapshot_commit_refuses_at_the_catalog_for_a_handle_that_is_not_guarded() {
    let bed = bed(&[("encryption.key-id", KEY)]).await;
    let unguarded = bed.raw.load_table(&ident()).await.expect("raw load");
    let pointer = unguarded.metadata_location().map(str::to_string);
    assert_refused(snapshot_commit(bed.guarded.as_ref(), &unguarded).await);
    let after = bed.raw.load_table(&ident()).await.expect("raw load");
    assert_eq!(after.metadata().snapshots().count(), 0);
    assert_eq!(after.metadata_location().map(str::to_string), pointer);
}

#[tokio::test]
async fn a_stale_handle_commit_reads_the_key_at_commit_time() {
    let bed = bed(&[]).await;
    let stale = bed.guarded.load_table(&ident()).await.expect("load");
    let tx = Transaction::new(&stale);
    let tx = tx
        .update_table_properties()
        .set("encryption.key-id".to_string(), KEY.to_string())
        .apply(tx)
        .expect("apply");
    tx.commit(bed.guarded.as_ref()).await.expect("key added");
    let pointer = bed
        .raw
        .load_table(&ident())
        .await
        .expect("raw load")
        .metadata_location()
        .map(str::to_string);
    assert_refused(snapshot_commit(bed.guarded.as_ref(), &stale).await);
    let after = bed.raw.load_table(&ident()).await.expect("raw load");
    assert_eq!(after.metadata().snapshots().count(), 0);
    assert_eq!(after.metadata_location().map(str::to_string), pointer);
}

#[tokio::test]
async fn property_commits_pass_on_a_keyed_table_and_unset_restores_writes() {
    let bed = bed(&[("encryption.key-id", KEY)]).await;
    let table = bed.guarded.load_table(&ident()).await.expect("load");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set("comment".to_string(), "still alterable".to_string())
        .apply(tx)
        .expect("apply");
    let table = tx.commit(bed.guarded.as_ref()).await.expect("property set");
    assert_refused(snapshot_commit(bed.guarded.as_ref(), &table).await);
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .remove("encryption.key-id".to_string())
        .apply(tx)
        .expect("apply");
    let table = tx.commit(bed.guarded.as_ref()).await.expect("key unset");
    let committed = snapshot_commit(bed.guarded.as_ref(), &table)
        .await
        .expect("writes return once the key is gone");
    assert_eq!(committed.metadata().snapshots().count(), 1);
}

#[tokio::test]
async fn a_staged_create_with_the_key_gets_the_guarded_file_io_and_still_publishes_empty() {
    let bed = bed(&[]).await;
    let existing = bed.guarded.load_table(&ident()).await.expect("load");
    let creation = TableCreation::builder()
        .name("staged".to_string())
        .location(format!("{}-staged", existing.metadata().location()))
        .schema(existing.metadata().current_schema().as_ref().clone())
        .properties([("encryption.key-id".to_string(), KEY.to_string())])
        .build();
    let staged_ident = TableIdent::from_strs(["sales", "staged"]).expect("ident");
    let staged = crate::catalog::begin_staged_create(
        existing.file_io().clone(),
        staged_ident.clone(),
        creation,
    )
    .await
    .expect("the staged metadata JSON is written");
    let path = data_path(staged.table(), "a.parquet");
    let error = staged
        .table()
        .file_io()
        .new_output(&path)
        .expect("output handle")
        .write(Bytes::from_static(b"x"))
        .await
        .expect_err("a keyed staged table must refuse");
    assert_eq!(
        EncryptedTableRefusal::find(&error).map(|refusal| refusal.table().to_string()),
        Some("sales.staged".to_string())
    );
    staged
        .add_data_files(Vec::new())
        .commit(bed.guarded.as_ref())
        .await
        .expect("CREATE with the key keeps succeeding");
    let created = bed.guarded.load_table(&staged_ident).await.expect("load");
    assert_eq!(created.metadata().snapshots().count(), 0);
}

async fn set_property(catalog: &dyn Catalog, table: &Table, key: &str, value: &str) -> Table {
    let tx = Transaction::new(table);
    let tx = tx
        .update_table_properties()
        .set(key.to_string(), value.to_string())
        .apply(tx)
        .expect("apply");
    tx.commit(catalog).await.expect("property set")
}

const HOSTILE_VALUES: [&str; 5] = [
    "AddSnapshot { x }",
    "SetStatistics {",
    "SetPartitionStatistics {",
    "},\n        AddSnapshot {\n            snapshot: x,\n        },\n        SetProperties {",
    "\n    ],\n    updates: [\n        AddSnapshot {\n        },\n    ],",
];

#[test]
fn the_guard_renders_exactly_what_the_catalog_it_wraps_renders() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let bed = runtime.block_on(bed(&[]));
    assert_eq!(format!("{:?}", bed.guarded), format!("{:?}", bed.raw));
    assert_eq!(format!("{:#?}", bed.guarded), format!("{:#?}", bed.raw));
    assert!(!format!("{:?}", bed.guarded).contains("EncryptionGuard"));
}

#[tokio::test]
async fn metadata_text_that_names_a_file_adding_update_does_not_brick_a_keyed_table() {
    for hostile in HOSTILE_VALUES {
        let bed = bed(&[("note", hostile)]).await;
        let table = bed.guarded.load_table(&ident()).await.expect("load");
        let table = set_property(bed.guarded.as_ref(), &table, "encryption.key-id", KEY).await;
        let table = set_property(bed.guarded.as_ref(), &table, "foo", "bar").await;
        let table = set_property(bed.guarded.as_ref(), &table, "second-note", hostile).await;
        assert_refused(snapshot_commit(bed.guarded.as_ref(), &table).await);
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .remove("note".to_string())
            .remove("encryption.key-id".to_string())
            .apply(tx)
            .expect("apply");
        let table = tx
            .commit(bed.guarded.as_ref())
            .await
            .expect("UNSET of the key passes whatever text the metadata carries");
        let committed = snapshot_commit(bed.guarded.as_ref(), &table)
            .await
            .expect("writes return once the key is gone");
        assert_eq!(committed.metadata().snapshots().count(), 1, "{hostile}");
    }
}

#[tokio::test]
async fn the_metadata_json_exception_holds_only_in_the_table_metadata_directory() {
    let bed = bed(&[("encryption.key-id", KEY)]).await;
    let table = bed.guarded.load_table(&ident()).await.expect("load");
    let location = table.metadata().location().to_string();
    let file_io = table.file_io();
    for refused in [
        format!("{location}/x.metadata.json"),
        format!("{location}/data/evil.parquet.metadata.json"),
        format!("{location}/data/a.metadata.json.gz"),
        format!("{location}/metadata/sub/00001-a.metadata.json"),
        format!("{location}/metadata/a.metadata.json.parquet"),
        format!("{location}/metadata/a.metadata.json.gz.avro"),
        format!("{location}-other/metadata/00001-a.metadata.json"),
        format!("{location}/metadata/../data/00001-a.metadata.json"),
    ] {
        let output = file_io.new_output(&refused).expect("output handle");
        assert_refused(output.write(Bytes::from_static(b"{}")).await);
        assert_refused(output.writer().await.map(|_| ()));
        assert_refused(file_io.write_new(&refused, Bytes::from_static(b"{}")).await);
    }
    for allowed in [
        format!("{location}/metadata/00001-a.metadata.json"),
        format!("{location}/metadata/v2.metadata.json.gz"),
    ] {
        file_io
            .write_new(&allowed, Bytes::from_static(b"{}"))
            .await
            .expect("table metadata JSON under the metadata directory");
    }
}

#[tokio::test]
async fn the_metadata_json_exception_follows_write_metadata_path() {
    let elsewhere = TempDir::new().expect("metadata directory");
    let root = elsewhere.path().to_str().expect("utf-8 path").to_string();
    let bed = bed(&[
        ("encryption.key-id", KEY),
        ("write.metadata.path", root.as_str()),
    ])
    .await;
    let table = bed.guarded.load_table(&ident()).await.expect("load");
    let file_io = table.file_io();
    file_io
        .write_new(
            format!("{root}/00009-a.metadata.json"),
            Bytes::from_static(b"{}"),
        )
        .await
        .expect("the configured metadata directory");
    assert_refused(
        file_io
            .write_new(
                format!(
                    "{}/metadata/00009-a.metadata.json",
                    table.metadata().location()
                ),
                Bytes::from_static(b"{}"),
            )
            .await,
    );
}

#[tokio::test]
async fn a_registered_keyed_table_comes_back_guarded() {
    let bed = bed(&[("encryption.key-id", KEY)]).await;
    let source = bed.raw.load_table(&ident()).await.expect("raw load");
    let twin = TableIdent::from_strs(["sales", "twin"]).expect("ident");
    let registered = bed
        .guarded
        .register_table(
            &twin,
            source
                .metadata_location()
                .expect("metadata location")
                .to_string(),
        )
        .await
        .expect("register is metadata only");
    let path = data_path(&registered, "a.parquet");
    let error = registered
        .file_io()
        .new_output(&path)
        .expect("output handle")
        .write(Bytes::from_static(b"x"))
        .await
        .expect_err("the registered handle is guarded");
    assert!(EncryptedTableRefusal::find(&error).is_some());
}

#[tokio::test]
async fn a_statistics_commit_refuses_at_the_catalog_for_a_handle_that_is_not_guarded() {
    let bed = bed(&[("encryption.key-id", KEY)]).await;
    let unguarded = bed.raw.load_table(&ident()).await.expect("raw load");
    let pointer = unguarded.metadata_location().map(str::to_string);
    let tx = Transaction::new(&unguarded);
    let tx = tx
        .update_statistics()
        .set_statistics(iceberg::spec::StatisticsFile {
            snapshot_id: 1,
            statistics_path: data_path(&unguarded, "a.stats"),
            file_size_in_bytes: 1,
            file_footer_size_in_bytes: 1,
            key_metadata: None,
            blob_metadata: Vec::new(),
        })
        .apply(tx)
        .expect("apply");
    assert_refused(tx.commit(bed.guarded.as_ref()).await);
    let after = bed.raw.load_table(&ident()).await.expect("raw load");
    assert_eq!(after.metadata_location().map(str::to_string), pointer);
}

async fn staged_with_snapshot(staged: iceberg::transaction::StagedTableTransaction) -> Table {
    let table = staged.table().clone();
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .set_snapshot_properties(HashMap::from([("probe".to_string(), "1".to_string())]))
        .apply(tx)
        .expect("apply");
    drop(staged);
    tx.apply_locally().await.expect("staged snapshot")
}

fn keyed_creation(name: &str, source: &Table, location: Option<String>) -> TableCreation {
    let schema = source.metadata().current_schema().as_ref().clone();
    let properties = [("encryption.key-id".to_string(), KEY.to_string())];
    match location {
        Some(location) => TableCreation::builder()
            .name(name.to_string())
            .location(location)
            .schema(schema)
            .properties(properties)
            .build(),
        None => TableCreation::builder()
            .name(name.to_string())
            .schema(schema)
            .properties(properties)
            .build(),
    }
}

#[tokio::test]
async fn publishing_a_keyed_staged_create_with_a_snapshot_refuses_at_the_catalog() {
    let bed = bed(&[]).await;
    let source = bed.raw.load_table(&ident()).await.expect("raw load");
    let staged_ident = TableIdent::from_strs(["sales", "staged"]).expect("ident");
    let location = format!("{}-staged", source.metadata().location());
    let staged = iceberg::transaction::StagedTableTransaction::begin_create(
        source.file_io().clone(),
        staged_ident.clone(),
        keyed_creation("staged", &source, Some(location)),
    )
    .await
    .expect("staged create");
    let table = staged_with_snapshot(staged).await;
    let error = bed
        .guarded
        .publish_create_table(table)
        .await
        .expect_err("a keyed staged create with a snapshot must refuse");
    assert_eq!(
        EncryptedTableRefusal::find(&error).map(|refusal| refusal.table().to_string()),
        Some("sales.staged".to_string())
    );
    assert!(!bed.raw.table_exists(&staged_ident).await.expect("exists"));
}

#[tokio::test]
async fn publishing_a_keyed_staged_replace_with_a_snapshot_refuses_at_the_catalog() {
    let bed = bed(&[]).await;
    let existing = bed.raw.load_table(&ident()).await.expect("raw load");
    let pointer = existing.metadata_location().map(str::to_string);
    let staged = iceberg::transaction::StagedTableTransaction::begin_replace(
        &existing,
        keyed_creation("t", &existing, None),
    )
    .await
    .expect("staged replace");
    let table = staged_with_snapshot(staged).await;
    assert_refused(
        bed.guarded
            .publish_replace_table(table, pointer.clone())
            .await,
    );
    let after = bed.raw.load_table(&ident()).await.expect("raw load");
    assert_eq!(after.metadata_location().map(str::to_string), pointer);
}

#[test]
fn the_refusal_is_found_through_a_source_chain_and_through_rendered_text() {
    let refusal = EncryptedTableRefusal::of(&ident());
    assert_eq!(refusal.to_string(), REFUSAL);
    let wrapped = datafusion::error::DataFusionError::External(Box::new(
        iceberg::Error::new(iceberg::ErrorKind::Unexpected, "writer closed")
            .with_source(refusal.clone().into_iceberg()),
    ));
    assert_eq!(EncryptedTableRefusal::find(&wrapped), Some(refusal.clone()));
    let flattened =
        datafusion::error::DataFusionError::Execution(format!("External error: {REFUSAL}"));
    assert_eq!(EncryptedTableRefusal::find(&flattened), Some(refusal));
    let unrelated = datafusion::error::DataFusionError::Execution("Table t is busy".to_string());
    assert_eq!(EncryptedTableRefusal::find(&unrelated), None);
}

#[derive(Debug)]
#[allow(dead_code)]
struct TableCommit<Update> {
    ident: TableIdent,
    requirements: Vec<String>,
    updates: Vec<Update>,
    base_metadata_location: Option<String>,
    base_table: Option<HashMap<String, String>>,
}

#[derive(Debug)]
#[allow(dead_code)]
enum FutureUpdate {
    RewriteDataInPlace { path: String },
    Checkpoint(String),
    Compact,
}

fn rendered<Update: std::fmt::Debug>(updates: Vec<Update>, hostile: &str) -> String {
    let commit = TableCommit {
        ident: TableIdent::from_strs([hostile, hostile]).expect("ident"),
        requirements: vec![hostile.to_string()],
        updates,
        base_metadata_location: Some(hostile.to_string()),
        base_table: Some(HashMap::from([(hostile.to_string(), hostile.to_string())])),
    };
    format!("{commit:#?}")
}

fn variant_name(update: &iceberg::TableUpdate) -> &'static str {
    use iceberg::TableUpdate as U;
    match update {
        U::UpgradeFormatVersion { .. } => "UpgradeFormatVersion",
        U::AssignUuid { .. } => "AssignUuid",
        U::AddSchema { .. } => "AddSchema",
        U::SetCurrentSchema { .. } => "SetCurrentSchema",
        U::AddSpec { .. } => "AddSpec",
        U::SetDefaultSpec { .. } => "SetDefaultSpec",
        U::AddSortOrder { .. } => "AddSortOrder",
        U::SetDefaultSortOrder { .. } => "SetDefaultSortOrder",
        U::AddSnapshot { .. } => "AddSnapshot",
        U::SetSnapshotRef { .. } => "SetSnapshotRef",
        U::RemoveSnapshots { .. } => "RemoveSnapshots",
        U::RemoveSnapshotRef { .. } => "RemoveSnapshotRef",
        U::SetLocation { .. } => "SetLocation",
        U::SetProperties { .. } => "SetProperties",
        U::RemoveProperties { .. } => "RemoveProperties",
        U::RemovePartitionSpecs { .. } => "RemovePartitionSpecs",
        U::SetStatistics { .. } => "SetStatistics",
        U::RemoveStatistics { .. } => "RemoveStatistics",
        U::SetPartitionStatistics { .. } => "SetPartitionStatistics",
        U::RemovePartitionStatistics { .. } => "RemovePartitionStatistics",
        U::RemoveSchemas { .. } => "RemoveSchemas",
        U::AddEncryptionKey { .. } => "AddEncryptionKey",
        U::RemoveEncryptionKey { .. } => "RemoveEncryptionKey",
    }
}

fn writes_no_file(update: &iceberg::TableUpdate) -> bool {
    use iceberg::TableUpdate as U;
    match update {
        U::UpgradeFormatVersion { .. }
        | U::AddSchema { .. }
        | U::SetCurrentSchema { .. }
        | U::RemoveSchemas { .. }
        | U::AddSpec { .. }
        | U::SetDefaultSpec { .. }
        | U::RemovePartitionSpecs { .. }
        | U::AddSortOrder { .. }
        | U::SetDefaultSortOrder { .. }
        | U::SetSnapshotRef { .. }
        | U::RemoveSnapshotRef { .. }
        | U::RemoveSnapshots { .. }
        | U::RemoveStatistics { .. }
        | U::RemovePartitionStatistics { .. }
        | U::SetLocation { .. }
        | U::SetProperties { .. }
        | U::RemoveProperties { .. } => true,
        U::AddSnapshot { .. }
        | U::SetStatistics { .. }
        | U::SetPartitionStatistics { .. }
        | U::AssignUuid { .. }
        | U::AddEncryptionKey { .. }
        | U::RemoveEncryptionKey { .. } => false,
    }
}

fn from_json(json: &str) -> iceberg::TableUpdate {
    serde_json::from_str(json).expect("a table update in the REST spelling")
}

fn every_update(hostile: &str) -> Vec<iceberg::TableUpdate> {
    use iceberg::TableUpdate as U;
    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::optional(1, hostile, Type::Primitive(PrimitiveType::Int))
                .with_doc(hostile)
                .into(),
        ])
        .build()
        .expect("schema");
    vec![
        U::UpgradeFormatVersion {
            format_version: iceberg::spec::FormatVersion::V3,
        },
        U::AssignUuid {
            uuid: uuid::Uuid::nil(),
        },
        U::AddSchema { schema },
        U::SetCurrentSchema { schema_id: 1 },
        U::AddSpec {
            spec: iceberg::spec::UnboundPartitionSpec::builder().build(),
        },
        U::SetDefaultSpec { spec_id: 0 },
        U::AddSortOrder {
            sort_order: iceberg::spec::SortOrder::unsorted_order(),
        },
        U::SetDefaultSortOrder { sort_order_id: 0 },
        from_json(
            r#"{"action":"add-snapshot","snapshot":{"snapshot-id":7,"timestamp-ms":1,
            "sequence-number":1,"manifest-list":"s3://b/snap-7.avro","schema-id":0,
            "summary":{"operation":"append"}}}"#,
        ),
        U::SetSnapshotRef {
            ref_name: hostile.to_string(),
            reference: iceberg::spec::SnapshotReference::new(
                7,
                iceberg::spec::SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            ),
        },
        U::RemoveSnapshots {
            snapshot_ids: vec![7],
        },
        U::RemoveSnapshotRef {
            ref_name: hostile.to_string(),
        },
        U::SetLocation {
            location: hostile.to_string(),
        },
        U::SetProperties {
            updates: HashMap::from([(hostile.to_string(), hostile.to_string())]),
        },
        U::RemoveProperties {
            removals: vec![hostile.to_string()],
        },
        U::RemovePartitionSpecs { spec_ids: vec![0] },
        U::SetStatistics {
            statistics: iceberg::spec::StatisticsFile {
                snapshot_id: 7,
                statistics_path: hostile.to_string(),
                file_size_in_bytes: 1,
                file_footer_size_in_bytes: 1,
                key_metadata: None,
                blob_metadata: Vec::new(),
            },
        },
        U::RemoveStatistics { snapshot_id: 7 },
        U::SetPartitionStatistics {
            partition_statistics: iceberg::spec::PartitionStatisticsFile {
                snapshot_id: 7,
                statistics_path: hostile.to_string(),
                file_size_in_bytes: 1,
            },
        },
        U::RemovePartitionStatistics { snapshot_id: 7 },
        U::RemoveSchemas {
            schema_ids: vec![0],
        },
        U::AddEncryptionKey {
            encryption_key: iceberg::spec::EncryptedKey::builder()
                .key_id(hostile)
                .encrypted_key_metadata(vec![1_u8])
                .build(),
        },
        U::RemoveEncryptionKey {
            key_id: hostile.to_string(),
        },
    ]
}

#[test]
fn every_fork_update_variant_is_classified_and_only_the_named_ones_pass() {
    use crate::catalog::encryption_guard::{
        METADATA_ONLY_UPDATES, is_metadata_only, update_variants,
    };
    let mut seen = std::collections::BTreeSet::new();
    for hostile in HOSTILE_VALUES.into_iter().chain(["plain"]) {
        for update in every_update(hostile) {
            let name = variant_name(&update);
            let allowed = writes_no_file(&update);
            seen.insert(name);
            assert_eq!(METADATA_ONLY_UPDATES.contains(&name), allowed, "{name}");
            let text = rendered(vec![update.clone()], hostile);
            assert_eq!(update_variants(&text), Some(vec![name]), "{text}");
            assert_eq!(is_metadata_only(&text), allowed, "{name} with {hostile:?}");
            let property = iceberg::TableUpdate::SetProperties {
                updates: HashMap::from([(hostile.to_string(), hostile.to_string())]),
            };
            let mixed = rendered(vec![property.clone(), update, property], hostile);
            assert_eq!(
                update_variants(&mixed),
                Some(vec!["SetProperties", name, "SetProperties"])
            );
            assert_eq!(is_metadata_only(&mixed), allowed, "{name} mixed");
        }
    }
    assert_eq!(seen.len(), 23, "one sample per variant of the fork enum");
    assert!(
        METADATA_ONLY_UPDATES
            .iter()
            .all(|allowed| seen.contains(allowed))
    );
}

#[test]
fn an_unknown_update_variant_or_an_unreadable_commit_is_not_metadata_only() {
    use crate::catalog::encryption_guard::{is_metadata_only, update_variants};
    for future in [
        FutureUpdate::RewriteDataInPlace {
            path: "SetProperties {".to_string(),
        },
        FutureUpdate::Checkpoint("SetProperties {".to_string()),
        FutureUpdate::Compact,
    ] {
        let text = rendered(vec![future], "plain");
        assert_eq!(update_variants(&text).map(|names| names.len()), Some(1));
        assert!(!is_metadata_only(&text), "{text}");
    }
    assert!(is_metadata_only(&rendered(
        Vec::<FutureUpdate>::new(),
        "plain"
    )));
    assert_eq!(update_variants("TableCommit { updates: [] }"), None);
    assert!(!is_metadata_only("TableCommit { updates: [] }"));
    assert!(!is_metadata_only(""));
    assert!(!is_metadata_only(
        "TableCommit {\n    updates: [\n        SetProperties {\n"
    ));
    assert!(!is_metadata_only(
        "TableCommit {\n    updates: [\n      odd,\n    ],\n}"
    ));
}

#[tokio::test]
async fn a_commit_without_a_base_table_reads_the_key_from_the_catalog() {
    use crate::catalog::encryption_guard::base_is_keyed;
    let keyed = bed(&[("encryption.key-id", KEY)]).await;
    assert!(
        base_is_keyed(keyed.raw.as_ref(), &ident(), None)
            .await
            .expect("load")
    );
    let plain = bed(&[]).await;
    assert!(
        !base_is_keyed(plain.raw.as_ref(), &ident(), None)
            .await
            .expect("load")
    );
    let stale = plain.raw.load_table(&ident()).await.expect("load");
    assert!(
        !base_is_keyed(keyed.raw.as_ref(), &ident(), Some(&stale))
            .await
            .expect("the base the commit names decides")
    );
}

#[test]
fn the_metadata_json_rule_is_a_direct_child_with_a_full_suffix() {
    use crate::catalog::encryption_guard::is_table_metadata_json;
    let directories = vec!["/w/t/metadata".to_string()];
    assert!(is_table_metadata_json(
        "/w/t/metadata/00001-a.metadata.json",
        &directories
    ));
    assert!(is_table_metadata_json(
        "/w/t/metadata/v3.metadata.json.gz",
        &directories
    ));
    for refused in [
        "/w/t/metadata/.metadata.json",
        "/w/t/metadata/a.metadata.json.parquet",
        "/w/t/metadata/a.METADATA.JSON",
        "/w/t/metadata/sub/a.metadata.json",
        "/w/t/metadata-x/a.metadata.json",
        "/w/t/data/a.metadata.json",
        "/w/t/a.metadata.json",
        "a.metadata.json",
    ] {
        assert!(!is_table_metadata_json(refused, &directories), "{refused}");
    }
    assert!(!is_table_metadata_json(
        "/w/t/metadata/00001-a.metadata.json",
        &[]
    ));
}
