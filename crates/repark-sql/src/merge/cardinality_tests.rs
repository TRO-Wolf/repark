//! Native-door execute pins for the lone-unconditional-DELETE cardinality exemption.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::Catalog;
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

/// A native session with one registered in-memory Iceberg catalog (`ice`) over a temp warehouse.
struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    warehouse: String,
    _warehouse_dir: TempDir,
}

impl Door {
    /// Run one statement through the ANSI door.
    async fn sql(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        let read_only = HashSet::new();
        let frame = crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await?;
        frame.collect().await
    }

    /// Run a statement that must succeed.
    async fn ok(&self, sql: &str) -> Vec<RecordBatch> {
        self.sql(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"))
    }

    /// Run a statement that must succeed, returning its schema alongside the batches.
    async fn ok_typed(
        &self,
        sql: &str,
    ) -> (datafusion::arrow::datatypes::SchemaRef, Vec<RecordBatch>) {
        let read_only = HashSet::new();
        let frame = crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await
        .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"));
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let batches = frame
            .collect()
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must collect: {err}"));
        (schema, batches)
    }

    /// Run a statement that must fail, returning the message.
    async fn err(&self, sql: &str) -> String {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(err) => err.to_string(),
        }
    }
}

/// Build the door session with the `sales` schema at a real warehouse location.
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

/// Read `(id, name)` pairs out of an Int64 + Utf8 result, in batch order.
fn id_name_rows(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 id");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 name");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), names.value(row).to_string()));
        }
    }
    rows
}

/// Duplicate source keys plus lone unconditional `WHEN MATCHED THEN DELETE` must commit; the survivor is checked.
#[tokio::test]
async fn merge_dup_source_keys_unconditional_delete_succeeds() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.card_del AS \
         SELECT CAST(1 AS BIGINT) AS id, 'a' AS name \
         UNION ALL SELECT CAST(2 AS BIGINT), 'b'")
        .await;
    door.ok("CREATE TABLE ice.sales.card_del_src AS \
         SELECT CAST(2 AS BIGINT) AS id, 'x' AS name \
         UNION ALL SELECT CAST(2 AS BIGINT), 'y'")
        .await;

    door.ok(
        "MERGE INTO ice.sales.card_del AS t USING ice.sales.card_del_src AS s ON t.id = s.id \
         WHEN MATCHED THEN DELETE",
    )
    .await;

    let (schema, batches) = door
        .ok_typed("SELECT id, name FROM ice.sales.card_del ORDER BY id")
        .await;
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "id type");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "name type");
    assert_eq!(id_name_rows(&batches), vec![(1, "a".to_string())]);
}

/// Same dup keys + `WHEN MATCHED THEN UPDATE SET` still raise `MERGE_CARDINALITY_VIOLATION`.
#[tokio::test]
async fn merge_dup_source_keys_update_still_raises_cardinality() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.card_upd AS \
         SELECT CAST(1 AS BIGINT) AS id, 'a' AS name \
         UNION ALL SELECT CAST(2 AS BIGINT), 'b'")
        .await;
    door.ok("CREATE TABLE ice.sales.card_upd_src AS \
         SELECT CAST(2 AS BIGINT) AS id, 'x' AS name \
         UNION ALL SELECT CAST(2 AS BIGINT), 'y'")
        .await;

    let err = door
        .err(
            "MERGE INTO ice.sales.card_upd AS t USING ice.sales.card_upd_src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .await;
    assert!(
        err.contains("MERGE_CARDINALITY_VIOLATION"),
        "UPDATE arm must still raise cardinality, got: {err}"
    );
}

/// Same dup keys + conditional `WHEN MATCHED AND … THEN DELETE` still raise the check.
#[tokio::test]
async fn merge_dup_source_keys_conditional_delete_still_raises_cardinality() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.card_cdel AS \
         SELECT CAST(1 AS BIGINT) AS id, 'a' AS name \
         UNION ALL SELECT CAST(2 AS BIGINT), 'b'")
        .await;
    door.ok("CREATE TABLE ice.sales.card_cdel_src AS \
         SELECT CAST(2 AS BIGINT) AS id, 'x' AS name \
         UNION ALL SELECT CAST(2 AS BIGINT), 'y'")
        .await;

    let err = door
        .err(
            "MERGE INTO ice.sales.card_cdel AS t USING ice.sales.card_cdel_src AS s \
             ON t.id = s.id WHEN MATCHED AND t.name = 'b' THEN DELETE",
        )
        .await;
    assert!(
        err.contains("MERGE_CARDINALITY_VIOLATION"),
        "conditional DELETE must still raise cardinality, got: {err}"
    );
}
