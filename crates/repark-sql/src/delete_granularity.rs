//! Model: Grok 4.6 xHigh
//! ANSI-door pins for Iceberg `write.delete.granularity`.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::Catalog;
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

    async fn delete_files(&self, table: &str) -> usize {
        let batches = self
            .sql(&format!(
                "SELECT * FROM ice.sales.{table}$files WHERE content = 1"
            ))
            .await
            .unwrap_or_else(|err| panic!("files query: {err}"));
        batches
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum()
    }

    async fn live_rows(&self, table: &str) -> usize {
        let batches = self
            .sql(&format!("SELECT * FROM ice.sales.{table}"))
            .await
            .unwrap_or_else(|err| panic!("select: {err}"));
        batches
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum()
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

const MOR: &str = "extra_properties = MAP(\
    ARRAY['write.merge.mode', 'write.delete.mode', 'write.update.mode'], \
    ARRAY['merge-on-read', 'merge-on-read', 'merge-on-read'])";

async fn seed_six(door: &Door, table: &str) {
    for id in 1..=6 {
        door.ok(&format!(
            "INSERT INTO ice.sales.{table} VALUES ({id}, 'v{id}')"
        ))
        .await;
    }
}

async fn merge_all_six(door: &Door, table: &str) {
    door.ok(&format!(
        "MERGE INTO ice.sales.{table} AS t USING (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL \
         SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6) AS s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = 'merged'"
    ))
    .await;
}

/// pins: mw-9-delete-granularity/C-001, C-006
#[tokio::test]
async fn unset_default_is_file_granularity() {
    let door = door_with_schema().await;
    door.ok(&format!(
        "CREATE TABLE ice.sales.g (id INT, v VARCHAR) WITH ({MOR})"
    ))
    .await;
    seed_six(&door, "g").await;
    merge_all_six(&door, "g").await;
    assert_eq!(door.delete_files("g").await, 6);
    assert_eq!(door.live_rows("g").await, 6);
}

/// pins: mw-9-delete-granularity/C-003
#[tokio::test]
async fn explicit_partition_granularity_writes_one_delete_file() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.p (id INT, v VARCHAR) WITH (\
         extra_properties = MAP(\
           ARRAY['write.merge.mode', 'write.delete.granularity'], \
           ARRAY['merge-on-read', 'partition']))")
        .await;
    seed_six(&door, "p").await;
    merge_all_six(&door, "p").await;
    assert_eq!(door.delete_files("p").await, 1);
}

/// pins: mw-9-delete-granularity/C-004
#[tokio::test]
async fn unknown_delete_granularity_refuses() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.bad (id INT, v VARCHAR) WITH (\
         extra_properties = MAP(\
           ARRAY['write.merge.mode', 'write.delete.granularity'], \
           ARRAY['merge-on-read', 'banana']))")
        .await;
    door.ok("INSERT INTO ice.sales.bad VALUES (1, 'a')").await;
    let err = door
        .err(
            "MERGE INTO ice.sales.bad AS t USING (SELECT 1 AS id) AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = 'x'",
        )
        .await;
    assert!(
        err.contains("write.delete.granularity")
            && err.contains("'file'")
            && err.contains("'partition'")
            && err.contains("banana"),
        "must name the property and both legal values: {err}"
    );
    assert_eq!(door.delete_files("bad").await, 0);
    assert_eq!(door.live_rows("bad").await, 1);
}

/// pins: mw-9-delete-granularity/C-007
#[tokio::test]
async fn alter_set_granularity_is_honored_on_the_next_merge() {
    let door = door_with_schema().await;
    door.ok(&format!(
        "CREATE TABLE ice.sales.flip (id INT, v VARCHAR) WITH ({MOR})"
    ))
    .await;
    seed_six(&door, "flip").await;
    door.ok(
        "ALTER TABLE ice.sales.flip SET PROPERTIES (extra_properties = MAP(\
         ARRAY['write.delete.granularity'], ARRAY['partition']))",
    )
    .await;
    merge_all_six(&door, "flip").await;
    assert_eq!(
        door.delete_files("flip").await,
        1,
        "ANSI SET PROPERTIES must feed the next MERGE's write.delete.granularity"
    );
}
