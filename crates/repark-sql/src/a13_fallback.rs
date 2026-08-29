//! A13: `register_memory_catalog` location-less CREATE lands under the warehouse.

use std::sync::Arc;

use repark_core::ReparkSession;
use tempfile::TempDir;

use crate::AnsiDialect;

/// A13 product path uses the registration warehouse for location-less ANSI CREATE.
#[tokio::test]
async fn register_memory_catalog_location_less_create_lands_under_warehouse() {
    let warehouse_dir = TempDir::new().expect("tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let session = ReparkSession::builder()
        .with_sql_dialect(Arc::new(AnsiDialect))
        .build()
        .expect("session");
    session
        .register_memory_catalog("a13ice", &warehouse)
        .await
        .expect("register");
    session
        .create_namespace("a13ice", "a13ns", std::collections::HashMap::new())
        .await
        .expect("namespace");
    session
        .sql("CREATE TABLE a13ice.a13ns.t AS SELECT 1 AS id")
        .await
        .expect("create")
        .collect()
        .await
        .expect("collect");

    let under_warehouse = warehouse_dir
        .path()
        .join("repark_ansi_ctas")
        .join("a13ice")
        .join("a13ns")
        .join("t");
    assert!(
        under_warehouse.exists(),
        "location-less ANSI create must write under the warehouse, missing {under_warehouse:?}"
    );
    let global = std::env::temp_dir()
        .join("repark_ansi_ctas")
        .join("a13ice")
        .join("a13ns")
        .join("t");
    assert!(
        !global.exists(),
        "must not share the process-temp root, found {global:?}"
    );
}
