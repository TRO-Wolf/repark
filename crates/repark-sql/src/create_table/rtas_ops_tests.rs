use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::Snapshot;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

struct NativeDoor {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    _warehouse: TempDir,
}

impl NativeDoor {
    async fn new(policy: LocationPolicy) -> Self {
        let warehouse = TempDir::new().expect("warehouse");
        let root = warehouse.path().to_str().expect("utf8").to_string();
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), root.clone())]),
                )
                .await
                .expect("memory catalog"),
        );
        let namespace_properties = if policy == LocationPolicy::ServiceManagedLocation {
            HashMap::new()
        } else {
            HashMap::from([("location".to_string(), format!("{root}/sales"))])
        };
        catalog
            .create_namespace(
                &NamespaceIdent::new("sales".to_string()),
                namespace_properties,
            )
            .await
            .expect("create namespace");
        let ctx = SessionContext::new();
        repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", Arc::clone(&catalog))
            .await
            .expect("register catalog");
        let mut catalogs = CatalogRegistry::new();
        catalogs.insert("ice".to_string(), Arc::clone(&catalog), policy);
        Self {
            ctx,
            catalogs,
            catalog,
            _warehouse: warehouse,
        }
    }

    async fn ok(&self, sql: &str) {
        let read_only = HashSet::new();
        crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await
        .unwrap_or_else(|err| panic!("`{sql}` must plan: {err}"))
        .collect()
        .await
        .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"));
    }

    async fn ops(&self, table: &str) -> Vec<String> {
        let loaded = self
            .catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.to_string(),
            ))
            .await
            .unwrap_or_else(|err| panic!("`{table}` must load: {err}"));
        let mut snapshots: Vec<_> = loaded.metadata().snapshots().cloned().collect();
        snapshots.sort_by_key(|snapshot| snapshot.sequence_number());
        snapshots
            .iter()
            .map(|snapshot: &Arc<Snapshot>| snapshot.summary().operation.as_str().to_string())
            .collect()
    }

    async fn row_count(&self, table: &str) -> usize {
        let read_only = HashSet::new();
        crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            &format!("SELECT * FROM ice.sales.{table}"),
        )
        .await
        .expect("read plans")
        .collect()
        .await
        .expect("read collects")
        .iter()
        .map(datafusion::arrow::record_batch::RecordBatch::num_rows)
        .sum()
    }
}

const RTAS_ROWS: &str = "SELECT 9 AS id UNION ALL SELECT 10 AS id";
const EMPTY_ROWS: &str = "SELECT 1 AS id WHERE false";

async fn staged() -> NativeDoor {
    NativeDoor::new(LocationPolicy::RequireExplicitLocation).await
}

async fn service_managed() -> NativeDoor {
    NativeDoor::new(LocationPolicy::ServiceManagedLocation).await
}

#[tokio::test]
async fn native_ctas_then_rtas_records_append_then_overwrite() {
    let door = staged().await;
    door.ok("CREATE TABLE ice.sales.rt AS SELECT 1 AS id").await;
    door.ok(&format!(
        "CREATE OR REPLACE TABLE ice.sales.rt AS {RTAS_ROWS}"
    ))
    .await;
    assert_eq!(door.ops("rt").await, ["append", "overwrite"]);
    assert_eq!(door.row_count("rt").await, 2);
}

#[tokio::test]
async fn native_rtas_creating_the_table_records_overwrite() {
    let door = staged().await;
    door.ok(&format!(
        "CREATE OR REPLACE TABLE ice.sales.rt2 AS {RTAS_ROWS}"
    ))
    .await;
    assert_eq!(door.ops("rt2").await, ["overwrite"]);
}

#[tokio::test]
async fn native_empty_rtas_on_new_table_records_delete() {
    let door = staged().await;
    door.ok(&format!(
        "CREATE OR REPLACE TABLE ice.sales.rt3 AS {EMPTY_ROWS}"
    ))
    .await;
    assert_eq!(door.ops("rt3").await, ["delete"]);
}

#[tokio::test]
async fn native_empty_rtas_twice_records_two_deletes() {
    let door = staged().await;
    for _ in 0..2 {
        door.ok(&format!(
            "CREATE OR REPLACE TABLE ice.sales.rt3 AS {EMPTY_ROWS}"
        ))
        .await;
    }
    assert_eq!(door.ops("rt3").await, ["delete", "delete"]);
    assert_eq!(door.row_count("rt3").await, 0);
}

#[tokio::test]
async fn native_plain_ctas_records_append() {
    let door = staged().await;
    door.ok(&format!("CREATE TABLE ice.sales.plain AS {RTAS_ROWS}"))
        .await;
    assert_eq!(door.ops("plain").await, ["append"]);
}

#[tokio::test]
async fn native_coldef_replace_commits_no_snapshot() {
    let door = staged().await;
    door.ok("CREATE TABLE ice.sales.cd (id BIGINT)").await;
    door.ok("INSERT INTO ice.sales.cd VALUES (1), (2), (3)")
        .await;
    assert_eq!(door.ops("cd").await, ["append"]);
    door.ok("CREATE OR REPLACE TABLE ice.sales.cd (id BIGINT)")
        .await;
    assert_eq!(door.ops("cd").await, ["append"]);
    assert_eq!(door.row_count("cd").await, 0);
}

#[tokio::test]
async fn native_service_managed_rtas_creating_the_table_records_overwrite() {
    let door = service_managed().await;
    door.ok(&format!(
        "CREATE OR REPLACE TABLE ice.sales.sm AS {RTAS_ROWS}"
    ))
    .await;
    assert_eq!(door.ops("sm").await, ["overwrite"]);
    assert_eq!(door.row_count("sm").await, 2);
}

#[tokio::test]
async fn native_service_managed_empty_rtas_records_delete_then_delete() {
    let door = service_managed().await;
    door.ok(&format!(
        "CREATE OR REPLACE TABLE ice.sales.sm_empty AS {EMPTY_ROWS}"
    ))
    .await;
    assert_eq!(door.ops("sm_empty").await, ["delete"]);
    door.ok(&format!(
        "CREATE OR REPLACE TABLE ice.sales.sm_empty AS {EMPTY_ROWS}"
    ))
    .await;
    assert_eq!(door.ops("sm_empty").await, ["delete", "delete"]);
}

#[tokio::test]
async fn native_service_managed_plain_ctas_records_append() {
    let door = service_managed().await;
    door.ok(&format!("CREATE TABLE ice.sales.sm_plain AS {RTAS_ROWS}"))
        .await;
    assert_eq!(door.ops("sm_plain").await, ["append"]);
    door.ok("CREATE TABLE ice.sales.sm_plain_empty AS SELECT 1 AS id WHERE false")
        .await;
    assert_eq!(door.ops("sm_plain_empty").await, Vec::<String>::new());
}

impl NativeDoor {
    async fn field_ids(&self, table: &str) -> (Vec<(i32, String)>, i32) {
        let loaded = self
            .catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.to_string(),
            ))
            .await
            .unwrap_or_else(|err| panic!("`{table}` must load: {err}"));
        let metadata = loaded.metadata();
        let ids = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.id, field.name.clone()))
            .collect();
        (ids, metadata.last_column_id())
    }
}

fn named(pairs: &[(i32, &str)]) -> Vec<(i32, String)> {
    pairs
        .iter()
        .map(|(id, name)| (*id, (*name).to_string()))
        .collect()
}

#[tokio::test]
async fn native_column_def_replace_keeps_field_ids_by_name() {
    let door = staged().await;
    door.ok("CREATE TABLE ice.sales.ids (id BIGINT, data VARCHAR, cat VARCHAR)")
        .await;
    door.ok("CREATE OR REPLACE TABLE ice.sales.ids (cat VARCHAR, data VARCHAR, id BIGINT)")
        .await;
    assert_eq!(
        door.field_ids("ids").await,
        (named(&[(3, "cat"), (2, "data"), (1, "id")]), 3)
    );
    door.ok("CREATE OR REPLACE TABLE ice.sales.ids (cat VARCHAR, payload VARCHAR, id BIGINT)")
        .await;
    assert_eq!(
        door.field_ids("ids").await,
        (named(&[(3, "cat"), (4, "payload"), (1, "id")]), 4)
    );
}

#[tokio::test]
async fn native_rtas_keeps_field_ids_by_name() {
    let door = staged().await;
    door.ok("CREATE TABLE ice.sales.rids AS SELECT 1 AS id, 'a' AS data, 'x' AS cat")
        .await;
    door.ok("CREATE OR REPLACE TABLE ice.sales.rids AS SELECT 'x' AS cat, 2 AS extra, 1 AS id")
        .await;
    assert_eq!(
        door.field_ids("rids").await,
        (named(&[(3, "cat"), (4, "extra"), (1, "id")]), 4)
    );
}
