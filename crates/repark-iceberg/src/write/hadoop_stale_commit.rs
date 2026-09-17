use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::error::DataFusionError;
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::append::append;

async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
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
        .create_namespace(&NamespaceIdent::new("ns".to_string()), HashMap::new())
        .await
        .expect("create namespace");
    catalog
}

fn table_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "key", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "payload", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build schema")
}

fn consumer_batch(keys: &[Option<i32>], payloads: &[Option<&str>]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("key", DataType::Int32, true),
        Field::new("payload", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys.to_vec())),
            Arc::new(StringArray::from(payloads.to_vec())),
        ],
    )
    .expect("build consumer batch")
}

async fn read_rows(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<(i32, String)> {
    let table = catalog.load_table(ident).await.expect("load table");
    let scan = table
        .scan()
        .select(["key", "payload"])
        .build()
        .expect("build scan");
    let batches: Vec<RecordBatch> = scan
        .to_arrow()
        .await
        .expect("scan to_arrow")
        .try_collect()
        .await
        .expect("collect scan batches");
    let mut rows = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("key column downcasts to Int32Array");
        let payloads = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload column downcasts to StringArray");
        for index in 0..batch.num_rows() {
            rows.push((keys.value(index), payloads.value(index).to_string()));
        }
    }
    rows.sort();
    rows
}

fn stale_conflict_message(error: &DataFusionError) -> String {
    let DataFusionError::External(inner) = error else {
        panic!("stale append must surface External, got {error:?}");
    };
    let iceberg_error = inner
        .downcast_ref::<iceberg::Error>()
        .expect("External must hold the fork error");
    assert_eq!(
        iceberg_error.kind(),
        ErrorKind::CatalogCommitConflicts,
        "stale vN append must keep the fork conflict kind, got {iceberg_error:?}"
    );
    iceberg_error.to_string()
}

async fn two_catalogs_over_hadoop_v2() -> (TempDir, Arc<dyn Catalog>, Arc<dyn Catalog>, TableIdent)
{
    let warehouse = TempDir::new().expect("temp warehouse");
    let first = memory_catalog(&warehouse).await;
    let second = memory_catalog(&warehouse).await;
    let namespace = NamespaceIdent::new("ns".to_string());
    let properties = HashMap::from([
        ("commit.retry.min-wait-ms".to_string(), "1".to_string()),
        ("commit.retry.max-wait-ms".to_string(), "5".to_string()),
        ("commit.retry.num-retries".to_string(), "2".to_string()),
    ]);
    first
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("src".to_string())
                .schema(table_schema())
                .properties(properties)
                .build(),
        )
        .await
        .expect("create source table");
    let source = first
        .load_table(&TableIdent::new(namespace.clone(), "src".to_string()))
        .await
        .expect("load source");
    let created_file = source
        .metadata_location()
        .expect("created table carries its metadata location")
        .to_string();
    let table_dir = source.metadata().location().to_string();
    let v2 = format!("{table_dir}/metadata/v2.metadata.json");
    let created_bytes = std::fs::read(&created_file).expect("read created metadata");
    std::fs::write(&v2, created_bytes).expect("plant hadoop v2");
    let ident = TableIdent::new(namespace, "t".to_string());
    first
        .register_table(&ident, v2.clone())
        .await
        .expect("first catalog adopts v2");
    second
        .register_table(&ident, v2)
        .await
        .expect("second catalog adopts v2");
    (warehouse, first, second, ident)
}

#[tokio::test]
async fn stale_hadoop_pointer_second_append_fails_loud_and_keeps_winner() {
    let (_warehouse, first, second, ident) = two_catalogs_over_hadoop_v2().await;
    append(
        &first,
        &ident,
        vec![consumer_batch(&[Some(1)], &[Some("winner")])],
    )
    .await
    .expect("first append lands v3");
    let table = first.load_table(&ident).await.expect("load winner");
    let winner_location = table
        .metadata_location()
        .expect("winner carries its metadata location")
        .to_string();
    assert!(
        winner_location.ends_with("/metadata/v3.metadata.json"),
        "hadoop pointer math must continue v2 with v3, got {winner_location}"
    );
    let winner_bytes = std::fs::read(&winner_location).expect("read winner metadata");

    let error = append(
        &second,
        &ident,
        vec![consumer_batch(&[Some(2)], &[Some("stale")])],
    )
    .await
    .expect_err("stale vN append must fail loud");
    let message = stale_conflict_message(&error);
    assert!(
        message.starts_with("CatalogCommitConflicts"),
        "the kind must lead the message, got {message:?}"
    );
    assert!(
        message.contains("version file already exists"),
        "the message must name the guard, got {message:?}"
    );
    assert!(
        message.contains("v3.metadata.json"),
        "the message must name the existing version, got {message:?}"
    );
    assert_eq!(
        std::fs::read(&winner_location).expect("re-read winner metadata"),
        winner_bytes,
        "the stale append must not touch the winner's bytes"
    );
    assert_eq!(
        read_rows(&first, &ident).await,
        vec![(1, "winner".to_string())],
        "only the winner's row stays live"
    );
}

#[tokio::test]
async fn stale_hadoop_pointer_stays_wedged_loud() {
    let (_warehouse, first, second, ident) = two_catalogs_over_hadoop_v2().await;
    append(
        &first,
        &ident,
        vec![consumer_batch(&[Some(1)], &[Some("winner")])],
    )
    .await
    .expect("first append lands v3");
    for key in [2, 3] {
        let error = append(
            &second,
            &ident,
            vec![consumer_batch(&[Some(key)], &[Some("stale")])],
        )
        .await
        .expect_err("every stale vN append must fail loud");
        let message = stale_conflict_message(&error);
        assert!(
            message.contains("version file already exists"),
            "the wedge must keep the guard message, got {message:?}"
        );
    }
    assert_eq!(
        read_rows(&first, &ident).await,
        vec![(1, "winner".to_string())],
    );
}
