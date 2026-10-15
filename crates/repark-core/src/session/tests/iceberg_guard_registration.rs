use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::prelude::SessionContext;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use repark_iceberg::catalog::ReparkCatalogProvider;
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

async fn disk_catalog(warehouse: &TempDir) -> Arc<dyn Catalog> {
    let location = warehouse
        .path()
        .to_str()
        .expect("warehouse path is utf8")
        .to_string();
    let storage =
        repark_iceberg::storage_factory_for_location(&location).expect("local storage resolves");
    Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(storage)
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), location)]),
            )
            .await
            .expect("direct disk catalog builds"),
    )
}

fn provider_handle(ctx: &SessionContext, name: &str) -> Arc<dyn Catalog> {
    ctx.catalog(name)
        .expect("the catalog is registered")
        .as_ref()
        .downcast_ref::<ReparkCatalogProvider>()
        .expect("the provider is a ReparkCatalogProvider")
        .catalog_handle()
}

async fn insert_refusal(ctx: &SessionContext, name: &str) -> String {
    let statement = format!("INSERT INTO {name}.db.t VALUES (1)");
    match ctx.sql(&statement).await {
        Ok(frame) => frame
            .collect()
            .await
            .expect_err("the keyed insert refuses")
            .to_string(),
        Err(error) => error.to_string(),
    }
}

async fn session_insert_refusal(session: &ReparkSession, name: &str) -> String {
    let statement = format!("INSERT INTO {name}.db.t VALUES (1)");
    match session.sql(&statement).await {
        Ok(frame) => frame
            .collect()
            .await
            .expect_err("the keyed session insert refuses")
            .to_string(),
        Err(error) => error.to_string(),
    }
}

fn assert_refused(message: &str) {
    assert!(message.contains(expected_text().as_str()), "{message}");
}

fn assert_no_parquet(warehouse: &TempDir) {
    let found = parquet_names(warehouse);
    assert!(found.is_empty(), "unencrypted files on disk: {found:?}");
}

async fn plain_context_through_free_register(catalog: Arc<dyn Catalog>) -> SessionContext {
    let ctx = SessionContext::new();
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "raw", catalog)
        .await
        .expect("the free register succeeds");
    ctx
}

async fn plain_context_through_provider(catalog: Arc<dyn Catalog>) -> SessionContext {
    let ctx = SessionContext::new();
    let provider = ReparkCatalogProvider::try_new(catalog)
        .await
        .expect("the provider builds");
    ctx.register_catalog("raw", Arc::new(provider));
    ctx
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

#[tokio::test]
async fn session_door_refuses_a_keyed_insert_and_writes_no_file() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let raw = disk_catalog(&warehouse).await;
    born_keyed_table(&raw).await;
    let session = registered_session("ice", Arc::clone(&raw)).await;
    assert_refused(&session_insert_refusal(&session, "ice").await);
    assert_refused(&insert_refusal(session.context(), "ice").await);
    assert_no_parquet(&warehouse);
}

#[tokio::test]
async fn free_register_door_refuses_a_keyed_insert_and_writes_no_file() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let raw = disk_catalog(&warehouse).await;
    born_keyed_table(&raw).await;
    let ctx = plain_context_through_free_register(raw).await;
    assert_refused(&insert_refusal(&ctx, "raw").await);
    assert_no_parquet(&warehouse);
}

#[tokio::test]
async fn provider_door_refuses_a_keyed_insert_and_writes_no_file() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let raw = disk_catalog(&warehouse).await;
    born_keyed_table(&raw).await;
    let ctx = plain_context_through_provider(raw).await;
    assert_refused(&insert_refusal(&ctx, "raw").await);
    assert_no_parquet(&warehouse);
}

#[tokio::test]
async fn session_registry_holds_the_provider_guarded_handle() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let raw = disk_catalog(&warehouse).await;
    let ident = born_keyed_table(&raw).await;
    let session = registered_session("ice", Arc::clone(&raw)).await;
    let registry = session.catalog_handle("ice").expect("handle resolves");
    let provider = provider_handle(session.context(), "ice");
    assert!(Arc::ptr_eq(&registry, &provider));
    assert!(!Arc::ptr_eq(&registry, &raw));
    assert_refused(&refused_write_text(&registry, &ident).await);
    assert_no_parquet(&warehouse);
}

#[tokio::test]
async fn every_door_wraps_a_guarded_catalog_again_with_one_refusal_text() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let guarded = repark_iceberg::catalog::memory_catalog(
        warehouse.path().to_str().expect("warehouse path is utf8"),
    )
    .await
    .expect("builder catalog builds");
    let ident = born_keyed_table(&guarded).await;
    let single_text = refused_write_text(&guarded, &ident).await;
    let raw_warehouse = TempDir::new().expect("raw warehouse");
    let raw = disk_catalog(&raw_warehouse).await;
    born_keyed_table(&raw).await;
    let session = registered_session("ice", Arc::clone(&guarded)).await;
    let free = plain_context_through_free_register(Arc::clone(&guarded)).await;
    let provider = plain_context_through_provider(Arc::clone(&guarded)).await;
    let wrapped = [
        session.catalog_handle("ice").expect("handle resolves"),
        provider_handle(&free, "raw"),
        provider_handle(&provider, "raw"),
    ];
    for handle in &wrapped {
        assert!(!Arc::ptr_eq(handle, &guarded));
        assert_eq!(refused_write_text(handle, &ident).await, single_text);
    }
    let single_session = registered_session("ice", Arc::clone(&raw)).await;
    let single_free = plain_context_through_free_register(Arc::clone(&raw)).await;
    let single_provider = plain_context_through_provider(Arc::clone(&raw)).await;
    let pairs = [
        (
            session_insert_refusal(&session, "ice").await,
            session_insert_refusal(&single_session, "ice").await,
        ),
        (
            insert_refusal(&free, "raw").await,
            insert_refusal(&single_free, "raw").await,
        ),
        (
            insert_refusal(&provider, "raw").await,
            insert_refusal(&single_provider, "raw").await,
        ),
    ];
    for (double, single) in &pairs {
        assert_refused(single);
        assert_eq!(double, single);
    }
    assert_no_parquet(&warehouse);
    assert_no_parquet(&raw_warehouse);
}
