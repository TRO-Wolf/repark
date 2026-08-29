//! V3-2 — ANSI CREATE/CTAS `format_version = 3` behind the session opt-in.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

use crate::execute;

struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    warehouse: String,
    _warehouse_dir: TempDir,
}

impl Door {
    /// Model: Grok 4.6 xHigh
    async fn sql(
        &self,
        sql: &str,
    ) -> datafusion::error::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        let read_only = HashSet::new();
        let frame = execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await?;
        frame.collect().await
    }

    /// Model: Grok 4.6 xHigh
    async fn ok(&self, sql: &str) {
        self.sql(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"));
    }

    /// Model: Grok 4.6 xHigh
    async fn err(&self, sql: &str) -> String {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(err) => err.to_string(),
        }
    }

    /// Model: Grok 4.6 xHigh
    async fn table(&self, namespace: &str, table: &str) -> iceberg::table::Table {
        self.catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new(namespace.to_string()),
                table.to_string(),
            ))
            .await
            .unwrap_or_else(|err| panic!("`{namespace}.{table}` must load: {err}"))
    }

    /// Model: Grok 4.6 xHigh
    async fn table_exists(&self, namespace: &str, table: &str) -> bool {
        self.catalog
            .table_exists(&TableIdent::new(
                NamespaceIdent::new(namespace.to_string()),
                table.to_string(),
            ))
            .await
            .expect("table_exists")
    }
}

/// Model: Grok 4.6 xHigh
async fn door_with_config(config: SessionConfig) -> Door {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir
        .path()
        .to_str()
        .expect("utf8 warehouse")
        .to_string();

    let catalog: Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .expect("memory catalog");
    let ctx = SessionContext::new_with_config(config);
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", Arc::clone(&catalog))
        .await
        .expect("register catalog");

    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "ice".to_string(),
        Arc::clone(&catalog),
        LocationPolicy::TempFallbackAllowed {
            root: warehouse_dir.path().to_path_buf(),
        },
    );
    catalogs.note_local_warehouse_root(&warehouse);

    Door {
        ctx,
        catalogs,
        catalog,
        warehouse,
        _warehouse_dir: warehouse_dir,
    }
}

/// Model: Grok 4.6 xHigh
async fn door_with_schema() -> Door {
    let door = door_with_config(SessionConfig::new().with_information_schema(true)).await;
    let location = format!("{}/sales", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.sales WITH (location = '{location}')"
    ))
    .await;
    door
}

/// Stand-in for `SparkExtension`'s `ReparkSqlConfig` (SEC-02 pattern — no product functions edge).
#[derive(Debug, Clone, Default)]
struct TestAllowCreateV3Config {
    allow: bool,
}

impl ConfigExtension for TestAllowCreateV3Config {
    const PREFIX: &'static str = "repark.sql";
}

impl ExtensionOptions for TestAllowCreateV3Config {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }
    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        if key == "allow_create_format_version_3" {
            self.allow = value.eq_ignore_ascii_case("true");
        }
        Ok(())
    }
    fn entries(&self) -> Vec<ConfigEntry> {
        vec![ConfigEntry {
            key: "repark.sql.allow_create_format_version_3".to_string(),
            value: Some(self.allow.to_string()),
            description: "test stand-in for V3-2 CREATE opt-in",
        }]
    }
}

/// Model: Grok 4.6 xHigh
async fn door_with_v3_opt_in() -> Door {
    let mut config = SessionConfig::new().with_information_schema(true);
    config
        .options_mut()
        .extensions
        .insert(TestAllowCreateV3Config { allow: true });
    let door = door_with_config(config).await;
    let location = format!("{}/sales", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.sales WITH (location = '{location}')"
    ))
    .await;
    door
}

/// pins: v3-2-create-v3-opt-in/C-004
/// Model: Grok 4.6 xHigh
#[tokio::test]
async fn format_version_three_without_opt_in_refuses() {
    let door = door_with_schema().await;
    let err = door
        .err("CREATE TABLE ice.sales.v3 WITH (format_version = 3) AS SELECT 1 AS id")
        .await;
    assert!(
        err.contains("repark.sql.allowCreateFormatVersion3") && err.contains("format_version"),
        "opt-in refuse must name conf and property: {err}"
    );
    assert!(!door.table_exists("sales", "v3").await, "nothing created");

    let err = door
        .err("CREATE TABLE ice.sales.v3c (id BIGINT) WITH (format_version = 3)")
        .await;
    assert!(
        err.contains("repark.sql.allowCreateFormatVersion3"),
        "column-def must refuse too: {err}"
    );
}

/// pins: v3-2-create-v3-opt-in/C-002, C-006, C-013
/// Model: Grok 4.6 xHigh
#[tokio::test]
async fn format_version_three_opt_in_creates_v3() {
    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.v3 WITH (format_version = 3) AS SELECT 1 AS id")
        .await;
    let table = door.table("sales", "v3").await;
    assert_eq!(
        table.metadata().format_version() as u8,
        3,
        "opt-in CTAS must create format v3"
    );

    door.ok("CREATE TABLE ice.sales.v3c (id BIGINT) WITH (format_version = 3)")
        .await;
    let column_def = door.table("sales", "v3c").await;
    assert_eq!(
        column_def.metadata().format_version() as u8,
        3,
        "opt-in column-def CREATE must create format v3"
    );

    door.ok("CREATE TABLE ice.sales.still_v2 AS SELECT 1 AS id")
        .await;
    let still = door.table("sales", "still_v2").await;
    assert_eq!(
        still.metadata().format_version() as u8,
        2,
        "opt-in must not change the unspecified default"
    );
}

/// pins: v3-2-create-v3-opt-in/C-006
/// Model: Grok 4.6 xHigh
#[tokio::test]
async fn or_replace_applies_requested_v3() {
    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.up (id BIGINT)").await;
    door.ok("CREATE OR REPLACE TABLE ice.sales.up (id BIGINT) WITH (format_version = 3)")
        .await;
    let upgraded = door.table("sales", "up").await;
    assert_eq!(upgraded.metadata().format_version() as u8, 3);
    door.ok("CREATE OR REPLACE TABLE ice.sales.up (id BIGINT)")
        .await;
    let kept = door.table("sales", "up").await;
    assert_eq!(
        kept.metadata().format_version() as u8,
        3,
        "unspecified OR REPLACE must not force v2 onto an existing v3 table"
    );
}
