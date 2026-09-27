use std::collections::HashMap;

use tempfile::TempDir;

use crate::{ErrorClass, ReparkSession};

const IN_MEMORY_CLASS: &str = "org.apache.iceberg.inmemory.InMemoryCatalog";

fn config(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

async fn session_with(pairs: &[(&str, &str)]) -> ReparkSession {
    let mut builder = ReparkSession::builder();
    for (key, value) in pairs {
        builder = builder.config(*key, *value);
    }
    let session = builder.build().unwrap();
    session.register_configured_catalogs().await.unwrap();
    session
}

#[test]
fn the_session_catalog_is_wanted_beside_other_catalog_blocks() {
    assert!(ReparkSession::auto_session_catalog_wanted(&config(&[])));
    assert!(ReparkSession::auto_session_catalog_wanted(&config(&[
        ("spark.sql.catalog.hc.type", "hadoop"),
        ("spark.sql.catalog.hc.warehouse", "/tmp/wh"),
        ("spark.sql.defaultCatalog", "hc"),
    ])));
    assert!(ReparkSession::auto_session_catalog_wanted(&config(&[(
        "spark.sql.catalog.spark_catalog_two.type",
        "hadoop",
    )])));
}

#[test]
fn a_user_session_catalog_block_or_the_switch_takes_the_auto_catalog_away() {
    assert!(!ReparkSession::auto_session_catalog_wanted(&config(&[(
        "spark.sql.catalog.spark_catalog.type",
        "hadoop",
    )])));
    assert!(!ReparkSession::auto_session_catalog_wanted(&config(&[(
        "repark.sql.catalog.spark_catalog",
        "org.apache.iceberg.spark.SparkSessionCatalog",
    )])));
    for value in ["false", "0", "No"] {
        assert!(!ReparkSession::auto_session_catalog_wanted(&config(&[(
            "repark.sql.autoMemoryCatalog",
            value,
        )])));
    }
}

#[tokio::test]
async fn a_fresh_session_starts_in_spark_catalog_with_catalogs_configured() {
    let warehouse = TempDir::new().unwrap();
    let warehouse = warehouse.path().to_str().unwrap();
    let session = session_with(&[
        ("spark.sql.catalog.hc.type", "hadoop"),
        ("spark.sql.catalog.hc.warehouse", warehouse),
    ])
    .await;
    assert_eq!(
        session.current_catalog_checked().unwrap(),
        ("spark_catalog".to_string(), "default".to_string())
    );
}

#[tokio::test]
async fn the_default_catalog_conf_sets_the_first_current_catalog() {
    let warehouse = TempDir::new().unwrap();
    let warehouse = warehouse.path().to_str().unwrap();
    let session = session_with(&[
        ("spark.sql.catalog.sc.catalog-impl", IN_MEMORY_CLASS),
        ("spark.sql.catalog.sc.warehouse", warehouse),
        ("spark.sql.defaultCatalog", "sc"),
    ])
    .await;
    assert_eq!(
        session.current_catalog_checked().unwrap(),
        ("sc".to_string(), String::new())
    );
}

#[tokio::test]
async fn a_default_catalog_that_names_no_catalog_is_catalog_not_found_at_first_use() {
    let session = session_with(&[("spark.sql.defaultCatalog", "nope")]).await;
    let error = session.current_catalog_checked().unwrap_err();
    assert_eq!(error.exception_class(), ErrorClass::Analysis);
    assert_eq!(
        error.to_string(),
        "[CATALOG_NOT_FOUND] The catalog `nope` not found. Consider to set the SQL config \
         \"spark.sql.catalog.nope\" to a catalog plugin. SQLSTATE: 42P08"
    );
}

#[tokio::test]
async fn a_bare_memory_type_builds_and_refuses_at_first_use() {
    let warehouse = TempDir::new().unwrap();
    let warehouse = warehouse.path().to_str().unwrap();
    let session = session_with(&[
        (
            "spark.sql.catalog.c_mem",
            "org.apache.iceberg.spark.SparkCatalog",
        ),
        ("spark.sql.catalog.c_mem.type", "memory"),
        ("spark.sql.catalog.c_mem.warehouse", warehouse),
    ])
    .await;
    assert!(
        !session
            .catalogs_snapshot()
            .catalog_names()
            .contains(&"c_mem".to_string())
    );
    for _ in 0..2 {
        let error = session.table_exists("c_mem.n1.t").await.unwrap_err();
        assert_eq!(error.exception_class(), ErrorClass::Unsupported);
        assert_eq!(error.to_string(), "Unknown catalog type: memory");
    }
    let error = session
        .create_namespace("c_mem", "n1", HashMap::new())
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "Unknown catalog type: memory");
    assert_eq!(
        session.current_catalog_checked().unwrap(),
        ("spark_catalog".to_string(), "default".to_string())
    );
}

#[tokio::test]
async fn a_runtime_memory_type_block_is_refused_and_a_later_long_form_registers() {
    let warehouse = TempDir::new().unwrap();
    let warehouse = warehouse.path().to_str().unwrap().to_string();
    let session = session_with(&[]).await;
    let refused = session
        .register_late_catalog_block(&config(&[
            ("spark.sql.catalog.c_mem.type", "memory"),
            ("spark.sql.catalog.c_mem.warehouse", warehouse.as_str()),
        ]))
        .await
        .unwrap();
    assert!(!refused);
    let error = session.table_exists("c_mem.n1.t").await.unwrap_err();
    assert_eq!(error.to_string(), "Unknown catalog type: memory");
    let registered = session
        .register_late_catalog_block(&config(&[
            ("spark.sql.catalog.c_mem.catalog-impl", IN_MEMORY_CLASS),
            ("spark.sql.catalog.c_mem.warehouse", warehouse.as_str()),
        ]))
        .await
        .unwrap();
    assert!(registered);
    assert!(!session.table_exists("c_mem.n1.t").await.unwrap());
    assert!(
        session
            .catalogs_snapshot()
            .catalog_names()
            .contains(&"c_mem".to_string())
    );
}

#[tokio::test]
async fn both_kind_keys_build_and_refuse_at_first_use_as_illegal_argument() {
    let warehouse = TempDir::new().unwrap();
    let warehouse = warehouse.path().to_str().unwrap();
    let session = session_with(&[
        ("spark.sql.catalog.c_both.type", "memory"),
        ("spark.sql.catalog.c_both.catalog-impl", IN_MEMORY_CLASS),
        ("spark.sql.catalog.c_both.warehouse", warehouse),
    ])
    .await;
    let error = session.table_exists("c_both.n1.t").await.unwrap_err();
    assert_eq!(error.exception_class(), ErrorClass::IllegalArgument);
    assert_eq!(
        error.to_string(),
        "Cannot create catalog c_both, both type and catalog-impl are set: type=memory, \
         catalog-impl=org.apache.iceberg.inmemory.InMemoryCatalog"
    );
}

#[tokio::test]
async fn the_catalog_extensions_opt_in_makes_the_memory_type_a_catalog_on_both_doors() {
    let warehouse = TempDir::new().unwrap();
    let warehouse = warehouse.path().to_str().unwrap().to_string();
    let session = session_with(&[
        ("repark.sql.catalogExtensions", "true"),
        ("spark.sql.catalog.c.type", "memory"),
        ("spark.sql.catalog.c.warehouse", warehouse.as_str()),
    ])
    .await;
    assert!(!session.table_exists("c.n1.t").await.unwrap());
    let registered = session
        .register_late_catalog_block(&config(&[
            ("repark.sql.catalogExtensions", "true"),
            ("spark.sql.catalog.r.type", "memory"),
            ("spark.sql.catalog.r.warehouse", warehouse.as_str()),
        ]))
        .await
        .unwrap();
    assert!(registered);
    assert!(!session.table_exists("r.n1.t").await.unwrap());
}

#[tokio::test]
async fn use_pins_the_current_catalog_and_the_default_conf_stops_moving_it() {
    let warehouse = TempDir::new().unwrap();
    let warehouse = warehouse.path().to_str().unwrap();
    let session = session_with(&[
        ("spark.sql.catalog.sc.catalog-impl", IN_MEMORY_CLASS),
        ("spark.sql.catalog.sc.warehouse", warehouse),
    ])
    .await;
    let catalogs = session.catalogs_snapshot();
    assert_eq!(
        catalogs.apply_default_catalog(Some("sc")),
        Some(("sc".to_string(), String::new()))
    );
    assert_eq!(
        catalogs.apply_default_catalog(None),
        Some(("spark_catalog".to_string(), "default".to_string()))
    );
    catalogs.set_defaults("sc", "ns");
    assert_eq!(catalogs.apply_default_catalog(Some("hc")), None);
    assert_eq!(catalogs.apply_default_catalog(None), None);
    assert_eq!(
        session.catalogs_snapshot().current_defaults(),
        ("sc".to_string(), "ns".to_string())
    );
}
