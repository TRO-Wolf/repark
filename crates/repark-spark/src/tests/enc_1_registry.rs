use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

use super::super::*;
use super::common::*;

fn parquet_files(root: &std::path::Path) -> Vec<String> {
    let mut pending = vec![root.to_path_buf()];
    let mut found = Vec::new();
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
                found.push(path.display().to_string());
            }
        }
    }
    found
}

async fn unguarded_catalog(location: &str) -> Arc<dyn iceberg::Catalog> {
    let catalog: Arc<dyn iceberg::Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), location.to_string())]),
            )
            .await
            .expect("the unguarded catalog builds"),
    );
    let namespace = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(
            &namespace,
            HashMap::from([("location".to_string(), format!("{location}/sales"))]),
        )
        .await
        .expect("the namespace creates");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("the schema builds");
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .properties(HashMap::from([(
                    "encryption.key-id".to_string(),
                    "registry-entry-check-key".to_string(),
                )]))
                .build(),
        )
        .await
        .expect("the keyed table creates");
    catalog
}

#[tokio::test]
async fn registry_insert_of_an_unguarded_catalog_refuses_a_keyed_insert_at_its_entry_check() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let location = warehouse
        .path()
        .to_str()
        .expect("warehouse path is utf8")
        .to_string();
    let raw = unguarded_catalog(&location).await;
    let ctx = SessionContext::new();
    let unguarded = iceberg_datafusion::IcebergCatalogProvider::try_new(Arc::clone(&raw))
        .await
        .expect("the unguarded provider builds");
    let provider = MemoryCatalogProvider::new();
    for name in unguarded.schema_names() {
        let schema = unguarded.schema(&name).expect("a listed schema resolves");
        provider
            .register_schema(&name, schema)
            .expect("the memory provider takes the schema");
    }
    ctx.register_catalog("ice", Arc::new(provider));
    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "ice".to_string(),
        Arc::clone(&raw),
        LocationPolicy::TempFallbackAllowed {
            root: warehouse.path().to_path_buf(),
        },
    );
    assert!(Arc::ptr_eq(
        catalogs.get("ice").expect("the entry is registered"),
        &raw
    ));
    let error = match execute(&ctx, &catalogs, "INSERT INTO ice.sales.t VALUES (1)").await {
        Ok(frame) => frame.collect().await.expect_err("the keyed insert refuses"),
        Err(error) => error,
    };
    let mapped = repark_core::engine_err(error);
    assert!(
        matches!(&mapped, repark_core::Error::NotImplemented(_)),
        "{mapped:?}"
    );
    assert_eq!(
        mapped.to_string(),
        "Table sales.t carries property 'encryption.key-id': RePark has no table encryption \
         and refuses to write plaintext into a table that asks for it (ENC-1)."
    );
    let found = parquet_files(warehouse.path());
    assert!(found.is_empty(), "unencrypted files on disk: {found:?}");
    ctx.sql("INSERT INTO ice.sales.t VALUES (2)")
        .await
        .expect("the provider plans the insert")
        .collect()
        .await
        .expect("the unguarded provider stores past the entry check");
    assert!(
        !parquet_files(warehouse.path()).is_empty(),
        "the control insert must land, or the refusal above proves nothing about the entry check"
    );
}
