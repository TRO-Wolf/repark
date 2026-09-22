use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    _warehouse_dir: TempDir,
}

impl Door {
    async fn sql(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        let read_only = HashSet::new();
        let frame = repark_sql::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await?;
        frame.collect().await
    }

    async fn ok(&self, sql: &str) -> Vec<RecordBatch> {
        self.sql(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"))
    }

    async fn err(&self, sql: &str) -> String {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(err) => err.to_string(),
        }
    }
}

fn update_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema")
}

async fn door_with_table() -> Door {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir
        .path()
        .to_str()
        .expect("utf8 warehouse")
        .to_string();
    let catalog: Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .expect("memory catalog");
    let namespace = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(update_schema())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
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
    Door {
        ctx,
        catalogs,
        _warehouse_dir: warehouse_dir,
    }
}

fn assert_cannot_safely_cast(err: &str) {
    assert!(
        err.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"),
        "{err}"
    );
    assert!(err.contains("SQLSTATE: KD000"), "{err}");
    assert!(
        err.contains("Cannot safely cast `id` \"STRING\" to \"BIGINT\""),
        "{err}"
    );
}

#[tokio::test]
async fn ansi_update_string_literal_into_bigint_stamps_cannot_safely_cast() {
    let door = door_with_table().await;
    let err = door.err("UPDATE ice.sales.t SET id = 'notanumber'").await;
    assert_cannot_safely_cast(&err);
}

#[tokio::test]
async fn ansi_update_int_literal_into_bigint_succeeds() {
    let door = door_with_table().await;
    door.ok("UPDATE ice.sales.t SET id = 7").await;
}

#[tokio::test]
async fn ansi_update_string_literal_into_string_succeeds() {
    let door = door_with_table().await;
    door.ok("UPDATE ice.sales.t SET data = 'z'").await;
}

#[tokio::test]
async fn ansi_update_missing_table_keeps_its_own_error() {
    let door = door_with_table().await;
    let err = door
        .err("UPDATE ice.sales.missing SET id = 'notanumber'")
        .await;
    assert!(!err.contains("CANNOT_SAFELY_CAST"), "{err}");
}
