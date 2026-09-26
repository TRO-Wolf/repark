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
