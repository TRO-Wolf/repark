use std::collections::HashMap;

use super::{CatalogKind, CatalogSpec, parse_catalog_specs};

const NAMING: &str = "metadata-naming";

fn memory_block(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    let mut config = HashMap::from([(
        "spark.sql.catalog.h.warehouse".to_string(),
        "/tmp/wh".to_string(),
    )]);
    for (prop, value) in pairs {
        config.insert(format!("spark.sql.catalog.h.{prop}"), (*value).to_string());
    }
    config
}

fn single_spec(config: &HashMap<String, String>) -> CatalogSpec {
    let mut specs = parse_catalog_specs(config).unwrap();
    assert_eq!(specs.len(), 1);
    specs.remove(0)
}

#[test]
fn hadoop_type_selects_hadoop_metadata_naming() {
    let spec = single_spec(&memory_block(&[("type", "hadoop")]));
    assert_eq!(spec.kind, CatalogKind::Memory);
    assert_eq!(spec.props.get(NAMING).map(String::as_str), Some("hadoop"));
}

#[test]
fn hadoop_type_is_trimmed_and_case_insensitive() {
    let spec = single_spec(&memory_block(&[("type", " Hadoop ")]));
    assert_eq!(spec.kind, CatalogKind::Memory);
    assert_eq!(spec.props.get(NAMING).map(String::as_str), Some("hadoop"));
}

#[test]
fn repark_spelling_of_hadoop_type_selects_hadoop_naming() {
    let config = HashMap::from([
        (
            "repark.sql.catalog.h.type".to_string(),
            "hadoop".to_string(),
        ),
        (
            "repark.sql.catalog.h.warehouse".to_string(),
            "/tmp/wh".to_string(),
        ),
    ]);
    let spec = single_spec(&config);
    assert_eq!(spec.props.get(NAMING).map(String::as_str), Some("hadoop"));
}

#[test]
fn memory_type_adds_no_metadata_naming() {
    let spec = single_spec(&memory_block(&[("type", "memory")]));
    assert_eq!(spec.kind, CatalogKind::Memory);
    assert!(!spec.props.contains_key(NAMING), "{spec:?}");
}

#[test]
fn in_memory_catalog_impl_adds_no_metadata_naming() {
    let spec = single_spec(&memory_block(&[(
        "catalog-impl",
        "org.apache.iceberg.inmemory.InMemoryCatalog",
    )]));
    assert_eq!(spec.kind, CatalogKind::Memory);
    assert!(!spec.props.contains_key(NAMING), "{spec:?}");
}

#[test]
fn explicit_metadata_naming_wins_over_hadoop_type() {
    let spec = single_spec(&memory_block(&[("type", "hadoop"), (NAMING, "uuid")]));
    assert_eq!(spec.props.get(NAMING).map(String::as_str), Some("uuid"));
}

#[test]
fn explicit_metadata_naming_passes_through_verbatim() {
    let spec = single_spec(&memory_block(&[("type", "memory"), (NAMING, "Hadoop")]));
    assert_eq!(spec.props.get(NAMING).map(String::as_str), Some("Hadoop"));
}

#[test]
fn near_miss_hadoop_types_are_still_refused() {
    for value in ["hadoopx", ""] {
        let message = parse_catalog_specs(&memory_block(&[("type", value)]))
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("spark.sql.catalog.h.type")
                && message.contains(&format!("unrecognized value '{value}'")),
            "{message}"
        );
    }
}

#[test]
fn bare_hadoop_catalog_value_adds_no_metadata_naming() {
    let config = HashMap::from([
        ("spark.sql.catalog.h".to_string(), "hadoop".to_string()),
        (
            "spark.sql.catalog.h.warehouse".to_string(),
            "/tmp/wh".to_string(),
        ),
    ]);
    let spec = single_spec(&config);
    assert_eq!(spec.kind, CatalogKind::Memory);
    assert!(!spec.props.contains_key(NAMING), "{spec:?}");
}
