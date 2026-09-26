use std::collections::HashMap;

use tempfile::TempDir;

use super::super::sources::{IN_MEMORY_CATALOG_CLASS, profile_sources};
use super::super::wiring::{FileConfig, load_file_config};
use super::{stub_environment, write_file};
use crate::catalog_config::{CatalogKind, parse_catalog_specs};
use crate::session::ReparkSessionBuilder;
use crate::{ErrorClass, ReparkSession};

fn try_loaded_file(text: &str, variables: &[(&str, &str)]) -> crate::Result<FileConfig> {
    let directory = TempDir::new().expect("config fixture directory");
    let path = directory.path().join("repark.toml");
    write_file(&path, text);
    let environment = stub_environment(variables);
    load_file_config(Some(path), &environment, directory.path(), None)
}

fn staged_file(text: &str) -> (TempDir, std::path::PathBuf) {
    let directory = TempDir::new().expect("config fixture directory");
    let path = directory.path().join("repark.toml");
    write_file(&path, text);
    (directory, path)
}

fn pairs_of(file: &FileConfig) -> HashMap<String, String> {
    file.pairs.iter().cloned().collect()
}

async fn file_built_session(path: std::path::PathBuf) -> ReparkSession {
    let session = ReparkSessionBuilder::default()
        .from_config_file(Some(path))
        .build()
        .expect("file build");
    session
        .register_configured_catalogs()
        .await
        .expect("file catalogs register");
    session
}

#[test]
fn a_memory_type_rewrites_to_the_catalog_impl_long_form() {
    for value in ["memory", "MEMORY", "Memory"] {
        let text = format!("[default.catalog.m]\ntype = \"{value}\"\nwarehouse = \"/tmp/wh\"\n");
        let file = try_loaded_file(&text, &[]).expect("fixture loads");
        let pairs = pairs_of(&file);
        assert_eq!(
            pairs
                .get("repark.sql.catalog.m.catalog-impl")
                .map(String::as_str),
            Some(IN_MEMORY_CATALOG_CLASS),
            "{value}"
        );
        assert!(
            !pairs.contains_key("repark.sql.catalog.m.type"),
            "the type key is replaced, not doubled: {pairs:?}"
        );
    }
    let config =
        super::super::parse("[default.catalog.m]\ntype = \"memory\"\nwarehouse = \"/tmp/wh\"\n")
            .expect("config fixture");
    let (_, profile) = config.profiles.iter().next().expect("one profile");
    let sources = profile_sources("default", profile).expect("profile sources");
    assert_eq!(sources.catalogs.len(), 1);
    assert_eq!(sources.catalogs[0].kind, CatalogKind::Memory);
    assert_eq!(sources.catalogs[0].refusal, None);
    let flat = HashMap::from([
        ("spark.sql.catalog.m.type".to_string(), "memory".to_string()),
        (
            "spark.sql.catalog.m.warehouse".to_string(),
            "/tmp/wh".to_string(),
        ),
    ]);
    let flat_specs = parse_catalog_specs(&flat).expect("flat specs");
    assert_eq!(flat_specs[0].kind, CatalogKind::Refused);
}

#[test]
fn a_memory_kind_impl_beside_type_keeps_the_impl_and_drops_type() {
    let class = "com.acme.CustomInMemoryCatalog";
    let file = try_loaded_file(
        &format!(
            "[default.catalog.m]\ntype = \"memory\"\ncatalog-impl = \"{class}\"\nwarehouse = \"/tmp/wh\"\n"
        ),
        &[],
    )
    .expect("fixture loads");
    let pairs = pairs_of(&file);
    assert_eq!(
        pairs
            .get("repark.sql.catalog.m.catalog-impl")
            .map(String::as_str),
        Some(class)
    );
    assert!(
        !pairs.contains_key("repark.sql.catalog.m.type"),
        "the agreeing type key is dropped: {pairs:?}"
    );
    let config = super::super::parse(
        &format!(
            "[default.catalog.m]\ntype = \"memory\"\ncatalog-impl = \"{class}\"\nwarehouse = \"/tmp/wh\"\n"
        ),
    )
    .expect("config fixture");
    let (_, profile) = config.profiles.iter().next().expect("one profile");
    let sources = profile_sources("default", profile).expect("profile sources");
    assert_eq!(sources.catalogs[0].kind, CatalogKind::Memory);
    assert_eq!(sources.catalogs[0].refusal, None);
}

#[test]
fn a_non_memory_impl_beside_type_keeps_both_keys_and_refuses() {
    let class = "org.apache.iceberg.aws.glue.GlueCatalog";
    let text = format!(
        "[default.catalog.m]\ntype = \"memory\"\ncatalog-impl = \"{class}\"\nwarehouse = \"s3://bucket/wh\"\n"
    );
    let file = try_loaded_file(&text, &[]).expect("fixture loads");
    let pairs = pairs_of(&file);
    assert_eq!(
        pairs.get("repark.sql.catalog.m.type").map(String::as_str),
        Some("memory")
    );
    assert_eq!(
        pairs
            .get("repark.sql.catalog.m.catalog-impl")
            .map(String::as_str),
        Some(class)
    );
    let config = super::super::parse(&text).expect("config fixture");
    let (_, profile) = config.profiles.iter().next().expect("one profile");
    let sources = profile_sources("default", profile).expect("profile sources");
    assert_eq!(sources.catalogs[0].kind, CatalogKind::Refused);
    let refusal = sources.catalogs[0].refusal.as_ref().expect("a refusal");
    let error = refusal.error();
    assert_eq!(error.exception_class(), ErrorClass::IllegalArgument);
    assert_eq!(
        error.to_string(),
        format!(
            "Cannot create catalog m, both type and catalog-impl are set: \
             type=memory, catalog-impl={class}"
        )
    );
}

#[test]
fn session_default_catalog_emits_the_spark_default_catalog_key() {
    let file = try_loaded_file("[default.session]\ndefault_catalog = \"local\"\n", &[])
        .expect("fixture loads");
    let pairs = pairs_of(&file);
    assert_eq!(
        pairs
            .get(crate::CatalogRegistry::DEFAULT_CATALOG_KEY)
            .map(String::as_str),
        Some("local")
    );
}

#[test]
fn a_non_string_default_catalog_refuses_naming_the_key_path() {
    let error = try_loaded_file("[default.session]\ndefault_catalog = true\n", &[])
        .expect_err("a boolean default catalog must refuse");
    assert!(
        error
            .to_string()
            .contains("default.session.default_catalog"),
        "{error}"
    );
}

#[tokio::test]
async fn the_owner_toml_shape_loads_and_starts_in_spark_catalog() {
    let warehouse = TempDir::new().expect("warehouse fixture");
    let warehouse_text = warehouse.path().to_str().expect("utf8 warehouse");
    let text = format!(
        "[default.catalog.local]\nimpl = \"org.apache.iceberg.spark.SparkCatalog\"\ntype = \"memory\"\nwarehouse = \"{warehouse_text}\"\n"
    );
    let file = try_loaded_file(&text, &[]).expect("fixture loads");
    let pairs = pairs_of(&file);
    assert_eq!(
        pairs
            .get("repark.sql.catalog.local.catalog-impl")
            .map(String::as_str),
        Some(IN_MEMORY_CATALOG_CLASS)
    );
    assert!(!pairs.contains_key("repark.sql.catalog.local.type"));
    let (_directory, path) = staged_file(&text);
    let session = file_built_session(path).await;
    assert_eq!(
        session.current_catalog_checked().expect("current catalog"),
        ("spark_catalog".to_string(), "default".to_string())
    );
    assert!(
        session
            .catalogs_snapshot()
            .catalog_names()
            .contains(&"local".to_string())
    );
}

#[tokio::test]
async fn default_catalog_in_toml_moves_the_first_current_catalog() {
    let warehouse = TempDir::new().expect("warehouse fixture");
    let warehouse_text = warehouse.path().to_str().expect("utf8 warehouse");
    let (_directory, path) = staged_file(&format!(
        "[default.session]\ndefault_catalog = \"local\"\n[default.catalog.local]\ntype = \"memory\"\nwarehouse = \"{warehouse_text}\"\n"
    ));
    let session = file_built_session(path).await;
    assert_eq!(
        session.current_catalog_checked().expect("current catalog"),
        ("local".to_string(), String::new())
    );
}
