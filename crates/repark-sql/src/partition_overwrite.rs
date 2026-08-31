//! ANSI-door PARTITION overwrite pins (DML-B). Q9 still omits whole-table overwrite.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::spec::Operation;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    warehouse: String,
    _warehouse_dir: TempDir,
}

impl Door {
    async fn sql(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        let read_only = HashSet::new();
        let frame = crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await?;
        frame.collect().await
    }

    async fn ok(&self, sql: &str) -> Vec<RecordBatch> {
        self.sql(sql)
            .await
            .unwrap_or_else(|error| panic!("`{sql}` must succeed: {error}"))
    }

    async fn err(&self, sql: &str) -> String {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(error) => error.to_string(),
        }
    }

    async fn table(&self, namespace: &str, table: &str) -> iceberg::table::Table {
        self.catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new(namespace.to_string()),
                table.to_string(),
            ))
            .await
            .unwrap_or_else(|error| panic!("load `{namespace}.{table}`: {error}"))
    }
}

async fn door_with_schema() -> Door {
    let warehouse_dir = TempDir::new().expect("warehouse");
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
        .expect("register");
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
        catalog,
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

fn id_name(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name");
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), names.value(index).to_string()));
        }
    }
    rows.sort();
    rows
}

/// pins: dml-b-insert-overwrite/C-006
#[tokio::test]
async fn whole_table_insert_overwrite_stays_q9() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.t AS SELECT 1 AS id, 'a' AS name")
        .await;
    let error = door
        .err("INSERT OVERWRITE ice.sales.t SELECT 2 AS id, 'b' AS name")
        .await;
    assert!(
        error.contains("INSERT OVERWRITE is not supported"),
        "{error}"
    );
}

/// pins: dml-b-insert-overwrite/C-001, C-005
#[tokio::test]
async fn static_partition_overwrite_keeps_siblings() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.t WITH (partitioning = ARRAY['id']) AS \
         SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2 AS id, 'b' AS name \
         UNION ALL SELECT 3 AS id, 'c' AS name",
    )
    .await;
    door.ok("INSERT OVERWRITE ice.sales.t PARTITION (id = 1) SELECT 'z'")
        .await;
    let batches = door
        .ok("SELECT id, name FROM ice.sales.t ORDER BY id")
        .await;
    assert_eq!(
        id_name(&batches),
        vec![(1, "z".into()), (2, "b".into()), (3, "c".into())]
    );
    let snapshot = door
        .table("sales", "t")
        .await
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .clone();
    assert_eq!(snapshot.summary().operation, Operation::Overwrite);
}

/// pins: dml-b-insert-overwrite/C-002, C-004
#[tokio::test]
async fn empty_dynamic_partition_overwrite_refuses() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.t WITH (partitioning = ARRAY['id']) AS \
         SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2 AS id, 'b' AS name",
    )
    .await;
    let error = door
        .err("INSERT OVERWRITE ice.sales.t PARTITION (id) SELECT * FROM ice.sales.t WHERE false")
        .await;
    assert!(
        error.contains(repark_iceberg::write::EMPTY_DYNAMIC_OVERWRITE_NEEDLE),
        "{error}"
    );
    let batches = door
        .ok("SELECT id, name FROM ice.sales.t ORDER BY id")
        .await;
    assert_eq!(id_name(&batches), vec![(1, "a".into()), (2, "b".into())]);
}
