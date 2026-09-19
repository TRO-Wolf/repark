//! ANSI-door `CREATE SCHEMA IF NOT EXISTS` location-guard pins.

use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::{Catalog, NamespaceIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

use std::collections::HashSet;

struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    warehouse: String,
    _warehouse_dir: TempDir,
}

impl Door {
    async fn new() -> Self {
        let warehouse_dir = TempDir::new().unwrap();
        let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
        let catalog = repark_iceberg::catalog::memory_catalog(&warehouse)
            .await
            .unwrap();
        let ctx = SessionContext::new_with_config(SessionConfig::new());
        repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", Arc::clone(&catalog))
            .await
            .unwrap();
        let mut catalogs = CatalogRegistry::new();
        catalogs.insert(
            "ice".to_string(),
            Arc::clone(&catalog),
            LocationPolicy::RequireExplicitLocation,
        );
        Self {
            ctx,
            catalogs,
            catalog,
            warehouse,
            _warehouse_dir: warehouse_dir,
        }
    }

    async fn sql(&self, sql: &str) -> datafusion::error::Result<()> {
        let read_only = HashSet::new();
        crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await?;
        Ok(())
    }

    async fn stored_location(&self, namespace: &str) -> Option<String> {
        let existing = self
            .catalog
            .get_namespace(&NamespaceIdent::new(namespace.to_string()))
            .await
            .unwrap();
        repark_iceberg::catalog::resolve_namespace_location(existing.properties())
            .map(str::to_string)
    }
}

#[tokio::test]
async fn ansi_create_schema_if_not_exists_create_new() {
    let door = Door::new().await;
    door.sql("CREATE SCHEMA IF NOT EXISTS ice.fresh_ns")
        .await
        .unwrap();
    assert!(
        door.catalog
            .namespace_exists(&NamespaceIdent::new("fresh_ns".to_string()))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn ansi_create_schema_if_not_exists_same_location_is_idempotent() {
    let door = Door::new().await;
    let location = format!("{}/same", door.warehouse);
    door.sql(&format!(
        "CREATE SCHEMA ice.silver WITH (location = '{location}')"
    ))
    .await
    .unwrap();
    door.sql(&format!(
        "CREATE SCHEMA IF NOT EXISTS ice.silver WITH (location = '{location}')"
    ))
    .await
    .unwrap();
    assert_eq!(
        door.stored_location("silver").await.as_deref(),
        Some(location.as_str())
    );
}

#[tokio::test]
async fn ansi_create_schema_if_not_exists_conflicting_location_fails_loud() {
    let door = Door::new().await;
    let existing = format!("{}/existing", door.warehouse);
    let requested = format!("{}/requested", door.warehouse);
    door.sql(&format!(
        "CREATE SCHEMA ice.silver WITH (location = '{existing}')"
    ))
    .await
    .unwrap();
    let error = door
        .sql(&format!(
            "CREATE SCHEMA IF NOT EXISTS ice.silver WITH (location = '{requested}')"
        ))
        .await
        .expect_err("contradictory IF NOT EXISTS location must fail loud");
    let message = error.to_string();
    assert!(
        message.contains(&existing),
        "must name the existing path: {message}"
    );
    assert!(
        message.contains(&requested),
        "must name the requested path: {message}"
    );
    assert_eq!(
        door.stored_location("silver").await.as_deref(),
        Some(existing.as_str())
    );
}

#[tokio::test]
async fn ansi_create_schema_if_not_exists_without_location_is_idempotent() {
    let door = Door::new().await;
    let location = format!("{}/kept", door.warehouse);
    door.sql(&format!(
        "CREATE SCHEMA ice.silver WITH (location = '{location}')"
    ))
    .await
    .unwrap();
    door.sql("CREATE SCHEMA IF NOT EXISTS ice.silver")
        .await
        .unwrap();
    assert_eq!(
        door.stored_location("silver").await.as_deref(),
        Some(location.as_str())
    );
}
