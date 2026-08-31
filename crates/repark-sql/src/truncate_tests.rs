//! pins: dml-c-truncate/C-001, C-003, C-006, C-007
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::spec::Operation;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

use crate::execute;

struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    warehouse: String,
    _warehouse_dir: TempDir,
}

impl Door {
    async fn sql(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        let read_only = HashSet::new();
        let frame = execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await?;
        frame.collect().await
    }

    async fn ok(&self, sql: &str) {
        self.sql(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"));
    }

    async fn err(&self, sql: &str) -> String {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(err) => err.to_string(),
        }
    }

    async fn live_rows(&self, table: &str) -> usize {
        let batches = self
            .sql(&format!("SELECT * FROM ice.sales.{table}"))
            .await
            .unwrap_or_else(|err| panic!("select: {err}"));
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    async fn load(&self, table: &str) -> iceberg::table::Table {
        self.catalogs["ice"]
            .load_table(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.to_string(),
            ))
            .await
            .unwrap_or_else(|err| panic!("load {table}: {err}"))
    }
}

async fn door_with_schema() -> Door {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir
        .path()
        .to_str()
        .expect("utf8 warehouse")
        .to_string();
    let catalog: Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .expect("memory catalog");
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
    catalogs.note_local_warehouse_root(&warehouse);
    let door = Door {
        ctx,
        catalogs,
        warehouse,
        _warehouse_dir: warehouse_dir,
    };
    let location = format!("{}/sales", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.sales WITH (location = '{location}')"
    ))
    .await;
    door
}

#[tokio::test]
async fn truncate_table_wipes_rows_stamps_delete_and_preserves_history() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.t AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    let before = door.load("t").await;
    let pre_id = before
        .metadata()
        .current_snapshot_id()
        .expect("pre-truncate snapshot");
    let pre_count = before.metadata().snapshots().len();
    assert_eq!(door.live_rows("t").await, 3);

    door.ok("TRUNCATE TABLE ice.sales.t").await;

    assert_eq!(door.live_rows("t").await, 0);
    let after = door.load("t").await;
    assert_eq!(after.metadata().snapshots().len(), pre_count + 1);
    assert_eq!(
        after
            .metadata()
            .current_snapshot()
            .expect("truncate snapshot")
            .summary()
            .operation
            .clone(),
        Operation::Delete,
    );
    let travelled = door
        .sql(&format!(
            "SELECT id FROM ice.sales.t FOR VERSION AS OF {pre_id}"
        ))
        .await
        .unwrap_or_else(|err| panic!("time travel: {err}"));
    let travelled_rows: usize = travelled.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(travelled_rows, 3);
}

#[tokio::test]
async fn truncate_missing_table_is_table_or_view_not_found() {
    let door = door_with_schema().await;
    let error = door.err("TRUNCATE TABLE ice.sales.does_not_exist").await;
    assert!(
        error.contains("TABLE_OR_VIEW_NOT_FOUND"),
        "spark class: {error}"
    );
}

#[tokio::test]
async fn truncate_view_is_expect_table_not_view() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.t AS SELECT 1 AS id").await;
    door.ok("CREATE VIEW v_trunc AS SELECT * FROM ice.sales.t")
        .await;
    let error = door.err("TRUNCATE TABLE v_trunc").await;
    assert!(
        error.contains("EXPECT_TABLE_NOT_VIEW"),
        "spark class: {error}"
    );
    assert_eq!(door.live_rows("t").await, 1);
}

#[tokio::test]
async fn truncate_partition_form_refuses_without_wiping() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.t AS SELECT 1 AS id").await;
    let error = door
        .err("TRUNCATE TABLE ice.sales.t PARTITION (id = 1)")
        .await;
    assert!(
        error.contains("INVALID_PARTITION_OPERATION") || error.contains("PARTITION"),
        "partition refuse: {error}"
    );
    assert_eq!(door.live_rows("t").await, 1);
}
