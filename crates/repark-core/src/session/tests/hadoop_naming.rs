use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableCreation};
use tempfile::TempDir;

use crate::ReparkSession;

async fn configured_session(props: &[(&str, &str)], warehouse: &Path) -> ReparkSession {
    let mut builder = ReparkSession::builder().config(
        "spark.sql.catalog.ice.warehouse",
        warehouse.to_str().unwrap(),
    );
    for (prop, value) in props {
        builder = builder.config(format!("spark.sql.catalog.ice.{prop}"), *value);
    }
    let session = builder.build().unwrap();
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

fn metadata_json_names(names: &BTreeSet<String>) -> Vec<&str> {
    names
        .iter()
        .map(String::as_str)
        .filter(|name| name.ends_with(".metadata.json"))
        .collect()
}

fn is_lower_hex_group(group: &str, length: usize) -> bool {
    group.len() == length
        && group
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_uuid_metadata_name(name: &str, version: &str) -> bool {
    let Some(stem) = name.strip_suffix(".metadata.json") else {
        return false;
    };
    let Some((prefix, uuid)) = stem.split_once('-') else {
        return false;
    };
    let groups: Vec<&str> = uuid.split('-').collect();
    prefix == version
        && groups.len() == 5
        && groups
            .iter()
            .zip([8, 4, 4, 4, 12])
            .all(|(group, length)| is_lower_hex_group(group, length))
}

fn assert_uuid_metadata_names(warehouse: &Path) {
    let names = metadata_file_names(warehouse);
    assert!(!names.contains("version-hint.text"), "{names:?}");
    let metadata_json = metadata_json_names(&names);
    assert_eq!(metadata_json.len(), 3, "{names:?}");
    for (name, version) in metadata_json.iter().zip(["00000", "00001", "00002"]) {
        assert!(is_uuid_metadata_name(name, version), "{name} in {names:?}");
    }
    let uuids: BTreeSet<&str> = metadata_json.iter().map(|name| &name[6..]).collect();
    assert_eq!(uuids.len(), 3, "{names:?}");
}

#[test]
fn uuid_metadata_name_check_rejects_near_misses() {
    let good = "00001-0a1b2c3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d.metadata.json";
    assert!(is_uuid_metadata_name(good, "00001"));
    for bad in [
        "v1.metadata.json",
        "00001-0a1b2c3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d.metadata.json.gz",
        "1-0a1b2c3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d.metadata.json",
        "00001-0A1B2C3D-4e5f-4a6b-8c7d-0e1f2a3b4c5d.metadata.json",
        "00001-0a1b2c3d-4e5f-4a6b-8c7d.metadata.json",
        "00001-0a1b2c3d4e5f4a6b8c7d0e1f2a3b4c5d.metadata.json",
        "00001-fixed.metadata.json",
    ] {
        assert!(!is_uuid_metadata_name(bad, "00001"), "{bad}");
    }
    assert!(!is_uuid_metadata_name(good, "00002"));
}

#[tokio::test]
async fn hadoop_type_catalog_writes_hadoop_metadata_names() {
    let warehouse = TempDir::new().unwrap();
    let session = configured_session(&[("type", "hadoop")], warehouse.path()).await;
    create_table_and_insert_twice(&session).await;
    let names = metadata_file_names(warehouse.path());
    assert_eq!(
        metadata_json_names(&names),
        ["v1.metadata.json", "v2.metadata.json", "v3.metadata.json"],
        "{names:?}"
    );
    assert!(names.contains("version-hint.text"), "{names:?}");
    let hint =
        std::fs::read_to_string(warehouse.path().join("db/t/metadata/version-hint.text")).unwrap();
    assert_eq!(hint.trim(), "3");
}

#[tokio::test]
async fn hadoop_type_with_explicit_uuid_naming_keeps_uuid_metadata_names() {
    let warehouse = TempDir::new().unwrap();
    let session = configured_session(
        &[("type", "hadoop"), ("metadata-naming", "uuid")],
        warehouse.path(),
    )
    .await;
    create_table_and_insert_twice(&session).await;
    assert_uuid_metadata_names(warehouse.path());
}

#[tokio::test]
async fn memory_type_catalog_keeps_uuid_metadata_names() {
    let warehouse = TempDir::new().unwrap();
    let session = configured_session(&[("type", "memory")], warehouse.path()).await;
    create_table_and_insert_twice(&session).await;
    assert_uuid_metadata_names(warehouse.path());
}

#[tokio::test]
async fn in_memory_catalog_impl_keeps_uuid_metadata_names() {
    let warehouse = TempDir::new().unwrap();
    let session = configured_session(
        &[(
            "catalog-impl",
            "org.apache.iceberg.inmemory.InMemoryCatalog",
        )],
        warehouse.path(),
    )
    .await;
    create_table_and_insert_twice(&session).await;
    assert_uuid_metadata_names(warehouse.path());
}

#[tokio::test]
async fn direct_memory_registration_keeps_uuid_metadata_names() {
    let warehouse = TempDir::new().unwrap();
    let session = ReparkSession::new().unwrap();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().unwrap())
        .await
        .unwrap();
    create_table_and_insert_twice(&session).await;
    assert_uuid_metadata_names(warehouse.path());
}

#[test]
fn rename_error_mapping_keeps_the_engine_error_class() {
    use iceberg::ErrorKind;
    use repark_common::Error;
    use repark_iceberg::write::unsupported_message_error;

    let refusal = crate::engine_err(unsupported_message_error(iceberg::Error::new(
        ErrorKind::FeatureUnsupported,
        "Cannot rename Hadoop tables",
    )));
    assert!(
        matches!(&refusal, Error::NotImplemented(message) if message == "Cannot rename Hadoop tables"),
        "{refusal:?}"
    );
    let missing = iceberg::Error::new(ErrorKind::TableNotFound, "Table does not exist: db.t");
    let rendered = missing.to_string();
    let missing = crate::engine_err(unsupported_message_error(missing));
    assert!(
        matches!(&missing, Error::Analysis(message) if *message == rendered),
        "{missing:?}"
    );
    let unexpected = crate::engine_err(unsupported_message_error(iceberg::Error::new(
        ErrorKind::Unexpected,
        "metadata write failed",
    )));
    assert!(matches!(unexpected, Error::Iceberg(_)), "{unexpected:?}");
}

#[tokio::test]
async fn memory_arm_forwards_only_metadata_naming_to_the_fork_builder() {
    for (props, expected) in [
        (
            vec![
                ("type", "hadoop"),
                ("s3.region", "us-east-1"),
                ("x-extra", "1"),
            ],
            HashMap::from([("metadata-naming".to_string(), "hadoop".to_string())]),
        ),
        (
            vec![
                ("type", "memory"),
                ("s3.region", "us-east-1"),
                ("x-extra", "1"),
            ],
            HashMap::new(),
        ),
    ] {
        let warehouse = TempDir::new().unwrap();
        let session = configured_session(&props, warehouse.path()).await;
        let handle = session.catalog_handle("ice").unwrap();
        assert_eq!(handle.properties(), &expected, "{props:?}");
    }
}

#[tokio::test]
async fn memory_registration_keeps_fallback_root_and_local_write_root() {
    let hadoop = TempDir::new().unwrap();
    let hadoop_session = configured_session(&[("type", "hadoop")], hadoop.path()).await;
    let direct = TempDir::new().unwrap();
    let direct_session = ReparkSession::new().unwrap();
    direct_session
        .register_memory_catalog("ice", direct.path().to_str().unwrap())
        .await
        .unwrap();
    for (session, warehouse) in [(&hadoop_session, &hadoop), (&direct_session, &direct)] {
        let catalogs = session.catalogs_snapshot();
        let warehouse_text = warehouse.path().to_str().unwrap();
        assert!(
            catalogs
                .local_warehouse_roots()
                .iter()
                .any(|root| root == warehouse_text),
            "{:?}",
            catalogs.local_warehouse_roots()
        );
        match catalogs.location_policy("ice") {
            Some(crate::LocationPolicy::TempFallbackAllowed { root }) => {
                assert_eq!(root, warehouse.path());
            }
            other => panic!("expected TempFallbackAllowed, got {other:?}"),
        }
        assert_eq!(
            catalogs.warehouse_layout_root("ice"),
            Some(warehouse.path().to_path_buf())
        );
    }
}
