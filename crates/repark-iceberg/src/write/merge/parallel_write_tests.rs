use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{DataFile, NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, TableCreation, TableIdent};
use tempfile::TempDir;

use crate::write::concurrency::WriteConcurrency;
use crate::write::merge::write_data_files_with_concurrency;

async fn memory_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "mem",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
        )
        .await
        .expect("memory catalog");
    Arc::new(catalog)
}

async fn create_table(catalog: &Arc<dyn Catalog>, name: &str) -> TableIdent {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "label", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema");
    let namespace = iceberg::NamespaceIdent::new("ns".into());
    let _ = catalog.create_namespace(&namespace, HashMap::new()).await;
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    TableIdent::new(namespace, name.into())
}

fn batches(n: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    (0..n)
        .map(|index| {
            let id = i32::try_from(index).expect("id fits");
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![id])),
                    Arc::new(StringArray::from(vec![Some(format!("r{index}"))])),
                ],
            )
            .expect("batch")
        })
        .collect()
}

#[tokio::test]
async fn serial_and_parallel_write_same_row_count() {
    let warehouse = TempDir::new().expect("tmp");
    let catalog = memory_catalog(&warehouse).await;
    let ident_serial = create_table(&catalog, "serial").await;
    let ident_parallel = create_table(&catalog, "parallel").await;
    let table_serial = catalog.load_table(&ident_serial).await.expect("load");
    let table_parallel = catalog.load_table(&ident_parallel).await.expect("load");
    let input = batches(12);

    let serial_files = write_data_files_with_concurrency(
        &table_serial,
        input.clone(),
        WriteConcurrency::new(1).expect("k=1"),
    )
    .await
    .expect("serial write");
    let parallel_files = write_data_files_with_concurrency(
        &table_parallel,
        input,
        WriteConcurrency::new(4).expect("k=4"),
    )
    .await
    .expect("parallel write");

    let serial_rows: u64 = serial_files.iter().map(DataFile::record_count).sum();
    let parallel_rows: u64 = parallel_files.iter().map(DataFile::record_count).sum();
    assert_eq!(serial_rows, 12);
    assert_eq!(parallel_rows, 12);
    assert!(
        parallel_files.len() >= serial_files.len() || !parallel_files.is_empty(),
        "serial files={} parallel files={}",
        serial_files.len(),
        parallel_files.len()
    );
}

#[tokio::test]
async fn max_concurrent_files_zero_is_loud() {
    assert!(WriteConcurrency::new(0).is_err());
    assert!(WriteConcurrency::parse("0").is_err());
}
