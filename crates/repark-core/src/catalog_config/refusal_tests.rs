use std::collections::HashMap;

use crate::ErrorClass;

use super::{CatalogKind, CatalogSpec, parse_catalog_specs};

const IN_MEMORY_CLASS: &str = "org.apache.iceberg.inmemory.InMemoryCatalog";

fn config(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn single_spec(pairs: &[(&str, &str)]) -> CatalogSpec {
    let specs = parse_catalog_specs(&config(pairs)).unwrap();
    assert_eq!(specs.len(), 1);
    specs.into_iter().next().unwrap()
}

fn refusal_shape(pairs: &[(&str, &str)]) -> (CatalogKind, ErrorClass, String) {
    let spec = single_spec(pairs);
    let refusal = spec.refusal.as_ref().expect("a refusal");
    let error = refusal.error();
    (spec.kind, error.exception_class(), error.to_string())
}

#[test]
fn a_bare_memory_type_is_refused_at_first_use_with_sparks_text() {
    for value in ["memory", "MEMORY", "Memory"] {
        let (kind, class, message) = refusal_shape(&[
            (
                "spark.sql.catalog.c_mem",
                "org.apache.iceberg.spark.SparkCatalog",
            ),
            ("spark.sql.catalog.c_mem.type", value),
            ("spark.sql.catalog.c_mem.warehouse", "/tmp/wh"),
        ]);
        assert_eq!(kind, CatalogKind::Refused);
        assert_eq!(class, ErrorClass::Unsupported);
        assert_eq!(message, format!("Unknown catalog type: {value}"));
    }
}

#[test]
fn a_memory_type_without_a_warehouse_is_refused_not_a_build_error() {
    let (kind, class, message) = refusal_shape(&[("spark.sql.catalog.c_mem.type", "memory")]);
    assert_eq!(kind, CatalogKind::Refused);
    assert_eq!(class, ErrorClass::Unsupported);
    assert_eq!(message, "Unknown catalog type: memory");
}

#[test]
fn the_catalog_impl_long_form_and_hadoop_type_stay_catalogs() {
    let spec = single_spec(&[
        ("spark.sql.catalog.c.catalog-impl", IN_MEMORY_CLASS),
        ("spark.sql.catalog.c.warehouse", "/tmp/wh"),
    ]);
    assert_eq!(spec.kind, CatalogKind::Memory);
    assert_eq!(spec.refusal, None);
    let spec = single_spec(&[
        ("spark.sql.catalog.c.type", "hadoop"),
        ("spark.sql.catalog.c.warehouse", "/tmp/wh"),
    ]);
    assert_eq!(spec.kind, CatalogKind::Memory);
    assert_eq!(spec.refusal, None);
}

#[test]
fn both_type_and_catalog_impl_are_refused_with_sparks_text() {
    let (kind, class, message) = refusal_shape(&[
        ("spark.sql.catalog.c_both.type", "memory"),
        ("spark.sql.catalog.c_both.catalog-impl", IN_MEMORY_CLASS),
        ("spark.sql.catalog.c_both.warehouse", "/tmp/wh"),
    ]);
    assert_eq!(kind, CatalogKind::Refused);
    assert_eq!(class, ErrorClass::IllegalArgument);
    assert_eq!(
        message,
        "Cannot create catalog c_both, both type and catalog-impl are set: type=memory, \
         catalog-impl=org.apache.iceberg.inmemory.InMemoryCatalog"
    );
    let (_, _, message) = refusal_shape(&[
        ("spark.sql.catalog.c_gh.type", "hadoop"),
        (
            "spark.sql.catalog.c_gh.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        ),
        ("spark.sql.catalog.c_gh.warehouse", "s3://bucket/wh"),
    ]);
    assert_eq!(
        message,
        "Cannot create catalog c_gh, both type and catalog-impl are set: type=hadoop, \
         catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
    );
}

#[test]
fn the_catalog_extensions_opt_in_keeps_the_memory_type_and_the_agreeing_pair() {
    let spec = single_spec(&[
        ("repark.sql.catalogExtensions", "true"),
        ("spark.sql.catalog.c.type", "memory"),
        ("spark.sql.catalog.c.warehouse", "/tmp/wh"),
    ]);
    assert_eq!(spec.kind, CatalogKind::Memory);
    assert_eq!(spec.refusal, None);
    let spec = single_spec(&[
        ("repark.sql.catalogExtensions", "true"),
        ("spark.sql.catalog.c.type", "memory"),
        ("spark.sql.catalog.c.catalog-impl", IN_MEMORY_CLASS),
        ("spark.sql.catalog.c.warehouse", "/tmp/wh"),
    ]);
    assert_eq!(spec.kind, CatalogKind::Memory);
    assert_eq!(spec.refusal, None);
    let message = parse_catalog_specs(&config(&[
        ("repark.sql.catalogExtensions", "true"),
        ("spark.sql.catalog.c.type", "memory"),
        (
            "spark.sql.catalog.c.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        ),
        ("spark.sql.catalog.c.warehouse", "s3://bucket/wh"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(message.contains("different catalog kinds"), "{message}");
}

#[test]
fn the_opt_in_reads_only_a_true_value() {
    for value in ["True", "TRUE", "1", "yes", ""] {
        let (kind, _, _) = refusal_shape(&[
            ("repark.sql.catalogExtensions", value),
            ("spark.sql.catalog.c.type", "memory"),
            ("spark.sql.catalog.c.warehouse", "/tmp/wh"),
        ]);
        assert_eq!(kind, CatalogKind::Refused, "value {value:?}");
    }
    let spec = single_spec(&[
        ("Repark.SQL.CatalogExtensions", "true"),
        ("spark.sql.catalog.c.type", "memory"),
        ("spark.sql.catalog.c.warehouse", "/tmp/wh"),
    ]);
    assert_eq!(spec.kind, CatalogKind::Memory);
    assert_eq!(spec.refusal, None);
}
