use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableCreation};
use tempfile::TempDir;

use crate::ReparkSession;

async fn configured_session(catalog_type: &str, warehouse: &Path) -> ReparkSession {
    let session = ReparkSession::builder()
        .config("spark.sql.catalog.ice.type", catalog_type)
        .config(
            "spark.sql.catalog.ice.warehouse",
            warehouse.to_str().unwrap(),
        )
        .build()
        .unwrap();
    session.register_configured_catalogs().await.unwrap();
    session
}

async fn create_table_and_insert_twice(session: &ReparkSession) {
    let handle = session.catalog_handle("ice").unwrap();
    let namespace = NamespaceIdent::new("db".to_string());
    handle
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "id",
            Type::Primitive(PrimitiveType::Int),
        ))])
        .build()
        .unwrap();
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .build();
    handle.create_table(&namespace, creation).await.unwrap();
    session.refresh_catalog_provider("ice").await.unwrap();
    for statement in [
        "INSERT INTO ice.db.t VALUES (1)",
        "INSERT INTO ice.db.t VALUES (2)",
    ] {
        session
            .sql(statement)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }
}

fn metadata_file_names(warehouse: &Path) -> BTreeSet<String> {
    std::fs::read_dir(warehouse.join("db/t/metadata"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

#[tokio::test]
async fn hadoop_type_catalog_writes_hadoop_metadata_names() {
    let warehouse = TempDir::new().unwrap();
    let session = configured_session("hadoop", warehouse.path()).await;
    create_table_and_insert_twice(&session).await;
    let names = metadata_file_names(warehouse.path());
    let metadata_json: BTreeSet<&str> = names
        .iter()
        .map(String::as_str)
        .filter(|name| name.ends_with(".metadata.json"))
        .collect();
    assert_eq!(
        metadata_json,
        BTreeSet::from(["v1.metadata.json", "v2.metadata.json", "v3.metadata.json"]),
        "{names:?}"
    );
    assert!(names.contains("version-hint.text"), "{names:?}");
    let hint =
        std::fs::read_to_string(warehouse.path().join("db/t/metadata/version-hint.text")).unwrap();
    assert_eq!(hint.trim(), "3");
}

#[tokio::test]
async fn memory_type_catalog_keeps_uuid_metadata_names() {
    let warehouse = TempDir::new().unwrap();
    let session = configured_session("memory", warehouse.path()).await;
    create_table_and_insert_twice(&session).await;
    let names = metadata_file_names(warehouse.path());
    assert!(!names.contains("version-hint.text"), "{names:?}");
    assert!(!names.contains("v1.metadata.json"), "{names:?}");
    assert_eq!(
        names
            .iter()
            .filter(|name| name.ends_with(".metadata.json"))
            .count(),
        3,
        "{names:?}"
    );
}
