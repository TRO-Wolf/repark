//! Session `create_namespace` location-guard pins (G-6 Q1 / R-6).
//!
//! Four shapes: create-new, re-create-same, re-create-conflicting, re-create-without-location.
//! AWS-free memory catalog. Location preservation is read back through the test-owned handle.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::{Catalog, NamespaceIdent};
use tempfile::TempDir;

use super::super::*;

async fn session_with_catalog(warehouse: &str) -> (ReparkSession, Arc<dyn Catalog>) {
    let session = ReparkSession::builder().build().unwrap();
    let catalog = repark_iceberg::catalog::memory_catalog(warehouse)
        .await
        .unwrap();
    session
        .register_iceberg_catalog("ice", Arc::clone(&catalog))
        .await
        .unwrap();
    (session, catalog)
}

async fn stored_location(catalog: &dyn Catalog, namespace: &str) -> Option<String> {
    let existing = catalog
        .get_namespace(&NamespaceIdent::new(namespace.to_string()))
        .await
        .unwrap();
    repark_iceberg::catalog::resolve_namespace_location(existing.properties()).map(str::to_string)
}

/// Create-new: a first `create_namespace` with an explicit location stores it and is visible.
#[tokio::test]
async fn create_namespace_create_new_stores_location() {
    let warehouse = TempDir::new().unwrap();
    let warehouse_path = warehouse.path().to_str().unwrap();
    let (session, catalog) = session_with_catalog(warehouse_path).await;
    let location = format!("{warehouse_path}/silver");

    session
        .create_namespace(
            "ice",
            "silver",
            HashMap::from([("location".to_string(), location.clone())]),
        )
        .await
        .unwrap();

    assert_eq!(
        stored_location(catalog.as_ref(), "silver").await.as_deref(),
        Some(location.as_str())
    );
}

/// Re-create with the same location is idempotent and does not rewrite properties.
#[tokio::test]
async fn create_namespace_recreate_same_location_is_idempotent() {
    let warehouse = TempDir::new().unwrap();
    let warehouse_path = warehouse.path().to_str().unwrap();
    let (session, catalog) = session_with_catalog(warehouse_path).await;
    let location = format!("{warehouse_path}/silver");
    let properties = HashMap::from([("location".to_string(), location.clone())]);

    session
        .create_namespace("ice", "silver", properties.clone())
        .await
        .unwrap();
    session
        .create_namespace("ice", "silver", properties)
        .await
        .expect("matching location must be idempotent");

    assert_eq!(
        stored_location(catalog.as_ref(), "silver").await.as_deref(),
        Some(location.as_str())
    );
}

/// Re-create with a contradictory location fails loud, naming both paths; stored location stays.
#[tokio::test]
async fn create_namespace_recreate_conflicting_location_fails_loud() {
    let warehouse = TempDir::new().unwrap();
    let warehouse_path = warehouse.path().to_str().unwrap();
    let (session, catalog) = session_with_catalog(warehouse_path).await;
    let existing = format!("{warehouse_path}/existing");
    let requested = format!("{warehouse_path}/requested");

    session
        .create_namespace(
            "ice",
            "silver",
            HashMap::from([("location".to_string(), existing.clone())]),
        )
        .await
        .unwrap();

    let error = session
        .create_namespace(
            "ice",
            "silver",
            HashMap::from([("location".to_string(), requested.clone())]),
        )
        .await
        .expect_err("contradictory location must fail loud");
    assert!(
        matches!(error, Error::Analysis(_)),
        "conflict is Analysis-class, got {error:?}"
    );
    let message = error.to_string();
    assert!(
        message.contains(&existing),
        "must name the existing path: {message}"
    );
    assert!(
        message.contains(&requested),
        "must name the requested path: {message}"
    );
    assert!(
        message.contains("silver"),
        "must name the namespace: {message}"
    );

    assert_eq!(
        stored_location(catalog.as_ref(), "silver").await.as_deref(),
        Some(existing.as_str()),
        "a refused re-create must not rewrite the stored location"
    );
}

/// Re-create without a request location is idempotent (adopts the existing namespace).
#[tokio::test]
async fn create_namespace_recreate_without_location_is_idempotent() {
    let warehouse = TempDir::new().unwrap();
    let warehouse_path = warehouse.path().to_str().unwrap();
    let (session, catalog) = session_with_catalog(warehouse_path).await;
    let location = format!("{warehouse_path}/silver");

    session
        .create_namespace(
            "ice",
            "silver",
            HashMap::from([("location".to_string(), location.clone())]),
        )
        .await
        .unwrap();
    session
        .create_namespace("ice", "silver", HashMap::new())
        .await
        .expect("no-location re-create must be idempotent");

    assert_eq!(
        stored_location(catalog.as_ref(), "silver").await.as_deref(),
        Some(location.as_str())
    );
}

/// Trailing-slash-only difference is not a contradiction (same warehouse path).
#[tokio::test]
async fn create_namespace_recreate_trailing_slash_is_idempotent() {
    let warehouse = TempDir::new().unwrap();
    let warehouse_path = warehouse.path().to_str().unwrap();
    let (session, catalog) = session_with_catalog(warehouse_path).await;
    let location = format!("{warehouse_path}/silver");
    let location_slash = format!("{location}/");

    session
        .create_namespace(
            "ice",
            "silver",
            HashMap::from([("location".to_string(), location.clone())]),
        )
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "silver",
            HashMap::from([("location".to_string(), location_slash)]),
        )
        .await
        .expect("trailing-slash-only difference must be idempotent");

    assert_eq!(
        stored_location(catalog.as_ref(), "silver").await.as_deref(),
        Some(location.as_str())
    );
}
