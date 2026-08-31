//! ANSI-door PARTITION overwrite pins (DML-B). Q9 still omits whole-table overwrite.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
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
    assert!(
        !snapshot
            .summary()
            .additional_properties
            .contains_key("replace-partitions"),
        "static overwrite must not stamp replace-partitions"
    );
}

/// pins: dml-b-insert-overwrite/C-001, C-004, C-005
#[tokio::test]
async fn empty_static_partition_overwrite_stamps_delete() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.t WITH (partitioning = ARRAY['id']) AS \
         SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2 AS id, 'b' AS name \
         UNION ALL SELECT 3 AS id, 'c' AS name",
    )
    .await;
    door.ok(
        "INSERT OVERWRITE ice.sales.t PARTITION (id = 1) SELECT name FROM ice.sales.t WHERE false",
    )
    .await;
    let batches = door
        .ok("SELECT id, name FROM ice.sales.t ORDER BY id")
        .await;
    assert_eq!(id_name(&batches), vec![(2, "b".into()), (3, "c".into())]);
    let snapshot = door
        .table("sales", "t")
        .await
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .clone();
    assert_eq!(snapshot.summary().operation, Operation::Delete);
}

/// pins: dml-b-insert-overwrite/C-002
#[tokio::test]
async fn dynamic_partition_overwrite_keeps_absent_partitions() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.t WITH (partitioning = ARRAY['id']) AS \
         SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2 AS id, 'b' AS name \
         UNION ALL SELECT 3 AS id, 'c' AS name",
    )
    .await;
    door.ok("INSERT OVERWRITE ice.sales.t PARTITION (id) SELECT 1 AS id, 'z' AS name")
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
    assert_eq!(
        snapshot
            .summary()
            .additional_properties
            .get("replace-partitions")
            .map(String::as_str),
        Some("true")
    );
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

fn id_cat_payload(batches: &[RecordBatch]) -> Vec<(i64, String, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let cats = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cat");
        let payloads = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload");
        for index in 0..batch.num_rows() {
            rows.push((
                ids.value(index),
                cats.value(index).to_string(),
                payloads.value(index).to_string(),
            ));
        }
    }
    rows.sort();
    rows
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_two_key_partition_overwrite_replaces_only_the_tuple() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.two WITH (partitioning = ARRAY['id', 'cat']) AS \
         SELECT 1 AS id, 'west' AS cat, 'a' AS payload \
         UNION ALL SELECT 1, 'east', 'b' UNION ALL SELECT 2, 'west', 'c'",
    )
    .await;
    door.ok("INSERT OVERWRITE ice.sales.two PARTITION (id = 1, cat = 'west') SELECT 'z'")
        .await;
    let batches = door
        .ok("SELECT id, cat, payload FROM ice.sales.two ORDER BY id, cat")
        .await;
    assert_eq!(
        id_cat_payload(&batches),
        vec![
            (1, "east".into(), "b".into()),
            (1, "west".into(), "z".into()),
            (2, "west".into(), "c".into()),
        ]
    );
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_incomplete_two_key_partition_replaces_all_k2_under_k1() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.two WITH (partitioning = ARRAY['id', 'cat']) AS \
         SELECT 1 AS id, 'west' AS cat, 'a' AS payload \
         UNION ALL SELECT 1, 'east', 'b' UNION ALL SELECT 2, 'west', 'c'",
    )
    .await;
    door.ok(
        "INSERT OVERWRITE ice.sales.two PARTITION (id = 1) SELECT 'north' AS cat, 'z' AS payload",
    )
    .await;
    let batches = door
        .ok("SELECT id, cat, payload FROM ice.sales.two ORDER BY id, cat")
        .await;
    assert_eq!(
        id_cat_payload(&batches),
        vec![
            (1, "north".into(), "z".into()),
            (2, "west".into(), "c".into()),
        ]
    );
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_string_partition_overwrite_keeps_siblings() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.s WITH (partitioning = ARRAY['name']) AS \
         SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2 AS id, 'b' AS name \
         UNION ALL SELECT 3 AS id, 'c' AS name",
    )
    .await;
    door.ok("INSERT OVERWRITE ice.sales.s PARTITION (name = 'a') SELECT 9")
        .await;
    let batches = door
        .ok("SELECT id, name FROM ice.sales.s ORDER BY id")
        .await;
    assert_eq!(
        id_name(&batches),
        vec![(2, "b".into()), (3, "c".into()), (9, "a".into())]
    );
}

/// pins: dml-b-insert-overwrite/C-001
#[tokio::test]
async fn static_null_partition_overwrite_keeps_siblings() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.n WITH (partitioning = ARRAY['id']) AS \
         SELECT CAST(NULL AS BIGINT) AS id, 'n' AS name UNION ALL SELECT 1, 'a' \
         UNION ALL SELECT 2, 'b'",
    )
    .await;
    door.ok("INSERT OVERWRITE ice.sales.n PARTITION (id = NULL) SELECT 'z'")
        .await;
    let batches = door
        .ok("SELECT id, name FROM ice.sales.n ORDER BY id NULLS FIRST")
        .await;
    let mut rows = Vec::new();
    for batch in &batches {
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
            let id = if ids.is_null(index) {
                None
            } else {
                Some(ids.value(index))
            };
            rows.push((id, names.value(index).to_string()));
        }
    }
    rows.sort_by_key(|row| row.0);
    assert_eq!(
        rows,
        vec![
            (None, "z".into()),
            (Some(1), "a".into()),
            (Some(2), "b".into()),
        ]
    );
}
