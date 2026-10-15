use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use crate::ReparkSession;

const PROBE_KEY: &str = "registration-probe-key";

fn ident() -> TableIdent {
    TableIdent::new(NamespaceIdent::new("db".to_string()), "t".to_string())
}

fn expected_text() -> String {
    repark_iceberg::write::EncryptedTableRefusal::of(&ident()).to_string()
}

async fn born_keyed_table(handle: &Arc<dyn Catalog>) -> TableIdent {
    handle
        .create_namespace(ident().namespace(), HashMap::new())
        .await
        .expect("namespace creates");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("id schema builds");
    let properties = HashMap::from([(
        repark_iceberg::write::ENCRYPTION_KEY_ID_PROPERTY.to_string(),
        PROBE_KEY.to_string(),
    )]);
    handle
        .create_table(
            ident().namespace(),
            TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .properties(properties)
                .build(),
        )
        .await
        .expect("keyed table creates");
    ident()
}

async fn refused_write_text(handle: &Arc<dyn Catalog>, ident: &TableIdent) -> String {
    let table = handle.load_table(ident).await.expect("keyed table loads");
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1])) as _])
        .expect("batch builds");
    repark_iceberg::write::write_data_files(&table, vec![batch])
        .await
        .expect_err("the keyed write refuses")
        .to_string()
}

fn parquet_names(warehouse: &TempDir) -> BTreeSet<String> {
    let mut pending = vec![warehouse.path().to_path_buf()];
    let mut found = BTreeSet::new();
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                found.insert(path.display().to_string());
            }
        }
    }
    found
}

async fn registered_session(name: &str, catalog: Arc<dyn Catalog>) -> ReparkSession {
    let session = ReparkSession::builder()
        .build()
        .expect("empty session builds");
    session
        .register_iceberg_catalog(name, catalog)
        .await
        .expect("catalog registers");
    session
}

#[tokio::test]
async fn session_registration_wraps_a_builder_catalog_twice_with_one_refusal_text() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let single = repark_iceberg::catalog::memory_catalog(
        warehouse.path().to_str().expect("warehouse path is utf8"),
    )
    .await
    .expect("builder catalog builds");
    let ident = born_keyed_table(&single).await;
    let single_text = refused_write_text(&single, &ident).await;
    let session = registered_session("ice", Arc::clone(&single)).await;
    let handle = session.catalog_handle("ice").expect("handle resolves");
    let double_text = refused_write_text(&handle, &ident).await;
    assert_eq!(double_text, single_text);
    assert!(double_text.contains(expected_text().as_str()));
    assert!(parquet_names(&warehouse).is_empty());
}

#[tokio::test]
async fn session_registration_guards_a_directly_built_catalog() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let mut properties = HashMap::new();
    properties.insert(
        MEMORY_CATALOG_WAREHOUSE.to_string(),
        warehouse
            .path()
            .to_str()
            .expect("warehouse path is utf8")
            .to_string(),
    );
    let direct = MemoryCatalogBuilder::default()
        .load("memory", properties)
        .await
        .expect("direct catalog builds");
    let session = registered_session("ice", Arc::new(direct)).await;
    let handle = session.catalog_handle("ice").expect("handle resolves");
    let ident = born_keyed_table(&handle).await;
    let direct_text = refused_write_text(&handle, &ident).await;
    let comparison = TempDir::new().expect("comparison warehouse");
    let builder = repark_iceberg::catalog::memory_catalog(
        comparison.path().to_str().expect("warehouse path is utf8"),
    )
    .await
    .expect("builder catalog builds");
    let builder_ident = born_keyed_table(&builder).await;
    let builder_text = refused_write_text(&builder, &builder_ident).await;
    assert_eq!(direct_text, builder_text);
    assert!(direct_text.contains(expected_text().as_str()));
    assert!(parquet_names(&warehouse).is_empty());
}
