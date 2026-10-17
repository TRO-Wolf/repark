use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema, Struct,
    Type,
};
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::*;
use crate::write::encryption::refusal_text;

fn ident() -> TableIdent {
    TableIdent::new(NamespaceIdent::new("sweep".to_string()), "t".to_string())
}

fn refusal() -> iceberg::Error {
    EncryptedTableRefusal::of(&ident()).into_iceberg()
}

async fn plain_table(warehouse: &TempDir) -> (Arc<dyn Catalog>, Table) {
    let catalog = crate::memory_catalog(warehouse.path().to_str().expect("warehouse path is utf8"))
        .await
        .expect("memory catalog builds");
    catalog
        .create_namespace(&NamespaceIdent::new("sweep".to_string()), HashMap::new())
        .await
        .expect("namespace creates");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("id schema builds");
    catalog
        .create_table(
            ident().namespace(),
            TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .properties(HashMap::new())
                .build(),
        )
        .await
        .expect("table creates");
    let table = catalog.load_table(&ident()).await.expect("table loads");
    (catalog, table)
}

fn warehouse_file(warehouse: &TempDir, name: &str) -> String {
    let path = warehouse.path().join(name);
    std::fs::write(&path, b"probe").expect("probe file writes");
    path.to_str().expect("probe path is utf8").to_string()
}

fn data_file(path: &str) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(5)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .expect("data file builds")
}

fn unique(name: &str) -> String {
    format!("/staged-commit-pins/{}/{name}", uuid::Uuid::new_v4())
}

#[test]
fn a_claim_takes_only_paths_a_writer_recorded() {
    let written = unique("written.parquet");
    let foreign = unique("foreign.parquet");
    unclaimed().record(&written);
    let staged = StagedFiles::claimed(vec![data_file(&written), data_file(&foreign)]);
    assert_eq!(staged.own, vec![written.clone()]);
    let again = StagedFiles::claimed(vec![data_file(&written)]);
    assert!(
        again.own.is_empty(),
        "a second attempt cannot claim the same file"
    );
}

#[test]
fn a_released_claim_can_be_claimed_by_the_next_attempt() {
    let written = unique("retried.parquet");
    unclaimed().record(&written);
    let first = StagedFiles::claimed(vec![data_file(&written)]);
    release(&first.own);
    let second = StagedFiles::claimed(vec![data_file(&written)]);
    assert_eq!(second.own, vec![written]);
}

#[test]
fn delete_files_the_operation_wrote_join_its_own_set() {
    let written = unique("merge.parquet");
    let delete = unique("merge-delete.parquet");
    unclaimed().record(&written);
    unclaimed().record(&delete);
    let staged = StagedFiles::claimed(vec![data_file(&written)])
        .with_written_deletes(vec![data_file(&delete)]);
    assert_eq!(staged.own, vec![written, delete.clone()]);
    assert!(
        StagedFiles::claimed(vec![data_file(&delete)])
            .own
            .is_empty()
    );
}

#[test]
fn the_ledger_forgets_its_oldest_paths_past_its_capacity() {
    let mut ledger = Unclaimed::default();
    for index in 0..5 {
        ledger.record_within(&format!("/capacity/{index}.parquet"), 4);
    }
    assert_eq!(ledger.paths.len(), 4);
    assert!(!ledger.paths.contains("/capacity/0.parquet"));
    assert!(ledger.paths.contains("/capacity/4.parquet"));
    ledger.paths.remove("/capacity/1.parquet");
    ledger.record_within("/capacity/5.parquet", 4);
    assert_eq!(ledger.order.len(), 4);
    assert!(ledger.paths.contains("/capacity/2.parquet"));
}

#[tokio::test]
async fn a_writer_location_is_recorded_as_written() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (_catalog, table) = plain_table(&warehouse).await;
    let generator = OwnLocations::new(
        DefaultLocationGenerator::new(table.metadata().clone()).expect("location generator"),
    );
    let location = generator.generate_location(None, &format!("{}.parquet", uuid::Uuid::new_v4()));
    let staged = StagedFiles::claimed(vec![data_file(&location)]);
    assert_eq!(staged.own, vec![location]);
}

#[test]
fn a_file_url_and_a_bare_path_compare_equal() {
    assert_eq!(
        canonical("file:///w/t/data/a.parquet"),
        "/w/t/data/a.parquet"
    );
    assert_eq!(canonical("file:/w/t/data/a.parquet"), "/w/t/data/a.parquet");
    assert_eq!(canonical("/w/t/data/a.parquet"), "/w/t/data/a.parquet");
    assert_eq!(
        canonical("s3://bucket/t/data/a.parquet"),
        "s3://bucket/t/data/a.parquet"
    );
}

#[tokio::test]
async fn keyed_refusal_with_nothing_staged_returns_the_same_error() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (_catalog, table) = plain_table(&warehouse).await;
    let swept = sweep_refused(&table, &[], refusal()).await;
    assert_eq!(swept.message(), refusal_text("sweep.t").as_str());
    assert!(EncryptedTableRefusal::find(&swept).is_some());
}

#[tokio::test]
async fn keyed_refusal_sweep_deletes_only_the_own_path() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (_catalog, table) = plain_table(&warehouse).await;
    let own = warehouse_file(&warehouse, "own.parquet");
    let kept = warehouse_file(&warehouse, "kept.parquet");
    let swept = sweep_refused(&table, std::slice::from_ref(&own), refusal()).await;
    assert_eq!(swept.message(), refusal_text("sweep.t").as_str());
    assert!(!std::path::Path::new(&own).exists());
    assert!(std::path::Path::new(&kept).exists());
}

#[tokio::test]
async fn keyed_refusal_names_a_file_it_could_not_delete() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (_catalog, table) = plain_table(&warehouse).await;
    let directory = warehouse.path().join("sweep-probe-dir");
    std::fs::create_dir(&directory).expect("probe directory creates");
    let path = directory.to_str().expect("probe path is utf8").to_string();
    let swept = sweep_refused(&table, std::slice::from_ref(&path), refusal()).await;
    assert!(swept.message().contains(refusal_text("sweep.t").as_str()));
    assert!(swept.message().contains(path.as_str()));
    assert!(EncryptedTableRefusal::find(&swept).is_some());
    assert_eq!(swept.kind(), iceberg::ErrorKind::FeatureUnsupported);
    assert!(!swept.retryable());
}

#[tokio::test]
async fn the_read_back_names_only_own_paths_a_snapshot_references() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, table) = plain_table(&warehouse).await;
    let referenced = warehouse_file(&warehouse, "referenced.parquet");
    let unreferenced = warehouse_file(&warehouse, "unreferenced.parquet");
    let staged = StagedFiles::claimed(vec![data_file(&format!("file://{referenced}"))]);
    let committed = staged
        .stage(
            Transaction::new(&table).fast_append(),
            Transaction::new(&table),
        )
        .expect("the append stages")
        .commit(&table, catalog.as_ref(), "read-back-probe")
        .await
        .expect("the append commits");
    let found = referenced_among(&committed, &[referenced.clone(), unreferenced])
        .await
        .expect("the snapshots read");
    assert_eq!(found, HashSet::from([referenced]));
}

#[tokio::test]
async fn our_snapshot_is_found_by_the_operation_id_it_was_sent_with() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (catalog, table) = plain_table(&warehouse).await;
    let path = warehouse_file(&warehouse, "ours.parquet");
    let (operation_id, summary) = crate::write::commit_error::operation_id_and_summary();
    let committed = StagedFiles::claimed(vec![data_file(&path)])
        .stage(
            Transaction::new(&table)
                .fast_append()
                .set_snapshot_properties(summary),
            Transaction::new(&table),
        )
        .expect("the append stages")
        .commit(&table, catalog.as_ref(), &operation_id)
        .await
        .expect("the append commits");
    assert!(carries_snapshot_of(&committed, &operation_id));
    assert!(!carries_snapshot_of(
        &committed,
        &uuid::Uuid::new_v4().to_string()
    ));
}
