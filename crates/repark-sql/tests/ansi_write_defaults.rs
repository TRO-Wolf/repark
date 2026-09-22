use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::spec::{
    Literal, NestedField, PrimitiveType, Schema as IcebergSchema, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
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

fn defaulted_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Int))
                .with_write_default(Literal::int(5))
                .into(),
        ])
        .build()
        .expect("schema")
}

fn required_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "req", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema")
}

async fn door_with_tables() -> Door {
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
    for (name, schema) in [("t", defaulted_schema()), ("r", required_schema())] {
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(schema)
            .properties(HashMap::new())
            .build();
        catalog
            .create_table(&namespace, creation)
            .await
            .expect("create table");
    }
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "id", Transform::Identity)
        .expect("identity partition field")
        .build();
    let partitioned = TableCreation::builder()
        .name("p".to_string())
        .schema(defaulted_schema())
        .partition_spec(spec)
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, partitioned)
        .await
        .expect("create partitioned table");
    let by_default = UnboundPartitionSpec::builder()
        .add_partition_field(3, "c", Transform::Identity)
        .expect("identity partition field")
        .build();
    let defaulted_partition = TableCreation::builder()
        .name("q".to_string())
        .schema(defaulted_schema())
        .partition_spec(by_default)
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&namespace, defaulted_partition)
        .await
        .expect("create default-partitioned table");
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
        catalog,
        _warehouse_dir: warehouse_dir,
    }
}

fn one_row_strings(batches: &[RecordBatch]) -> (i32, String, i32) {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1, "expected one row, got {rows}");
    let batch = &batches[0];
    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id Int32")
        .value(0);
    let name = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name Utf8")
        .value(0)
        .to_string();
    let fill = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("c Int32")
        .value(0);
    (id, name, fill)
}

#[tokio::test]
async fn ansi_insert_column_list_fills_write_default() {
    let door = door_with_tables().await;
    door.ok("INSERT INTO ice.sales.t (id, name) VALUES (4, 'd')")
        .await;
    let batches = door.ok("SELECT id, name, c FROM ice.sales.t").await;
    assert_eq!(one_row_strings(&batches), (4, "d".to_string(), 5));
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "t".to_string());
    assert!(door.catalog.table_exists(&ident).await.expect("exists"));
}

#[tokio::test]
async fn ansi_insert_missing_required_column_refuses() {
    let door = door_with_tables().await;
    let err = door.err("INSERT INTO ice.sales.r (id) VALUES (1)").await;
    assert!(
        err.contains("non-nullable but contains null"),
        "a missing required column must refuse like the Spark door: {err}"
    );
}

#[tokio::test]
async fn ansi_dynamic_partition_column_list_fills_write_default() {
    let door = door_with_tables().await;
    door.ok("INSERT OVERWRITE ice.sales.p (id, name) PARTITION (id) SELECT 10, 'x'")
        .await;
    let batches = door.ok("SELECT id, name, c FROM ice.sales.p").await;
    assert_eq!(one_row_strings(&batches), (10, "x".to_string(), 5));
}

#[tokio::test]
async fn ansi_static_partition_column_list_fills_write_default() {
    let door = door_with_tables().await;
    door.ok("INSERT OVERWRITE ice.sales.p (name) PARTITION (id = 10) SELECT 'x'")
        .await;
    let batches = door.ok("SELECT id, name, c FROM ice.sales.p").await;
    assert_eq!(one_row_strings(&batches), (10, "x".to_string(), 5));
}

#[tokio::test]
async fn ansi_partition_overwrite_default_keyword_fills_write_default() {
    let door = door_with_tables().await;
    door.ok("INSERT OVERWRITE ice.sales.p (id, name, c) PARTITION (id) SELECT 10, 'x', DEFAULT")
        .await;
    let batches = door.ok("SELECT id, name, c FROM ice.sales.p").await;
    assert_eq!(one_row_strings(&batches), (10, "x".to_string(), 5));
}

#[tokio::test]
async fn ansi_static_partition_value_wins_over_its_write_default() {
    let door = door_with_tables().await;
    door.ok("INSERT OVERWRITE ice.sales.q (id, name) PARTITION (c = 7) SELECT 1, 'x'")
        .await;
    let batches = door.ok("SELECT id, name, c FROM ice.sales.q").await;
    assert_eq!(one_row_strings(&batches), (1, "x".to_string(), 7));
}

#[tokio::test]
async fn ansi_static_partition_column_list_refusals() {
    let door = door_with_tables().await;
    let listed_static = door
        .err("INSERT OVERWRITE ice.sales.p (id, name) PARTITION (id = 10) SELECT 10, 'x'")
        .await;
    assert!(
        listed_static.contains("STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST"),
        "{listed_static}"
    );
    let arity = door
        .err("INSERT OVERWRITE ice.sales.p (name) PARTITION (id = 10) SELECT 'x', 1")
        .await;
    assert!(arity.contains("INSERT_COLUMN_ARITY_MISMATCH"), "{arity}");
    let duplicate = door
        .err("INSERT OVERWRITE ice.sales.p (name, name) PARTITION (id = 10) SELECT 'x', 'y'")
        .await;
    assert!(duplicate.contains("more than once"), "{duplicate}");
    let empty = door.ok("SELECT id FROM ice.sales.p").await;
    assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

#[tokio::test]
async fn ansi_short_values_insert_stamps_not_enough_data_columns() {
    let door = door_with_tables().await;
    let err = door.err("INSERT INTO ice.sales.t VALUES (8, 'h')").await;
    assert!(
        err.contains("[INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS]"),
        "{err}"
    );
    assert!(err.contains("SQLSTATE: 21S01"), "{err}");
    assert!(err.contains("Table columns: `id`, `name`, `c`."), "{err}");
    assert!(err.contains("Data columns: `col1`, `col2`."), "{err}");
    assert!(err.contains("`ice`.`sales`.`t`"), "{err}");
    let empty = door.ok("SELECT id FROM ice.sales.t").await;
    assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

#[tokio::test]
async fn ansi_correct_arity_values_insert_succeeds() {
    let door = door_with_tables().await;
    door.ok("INSERT INTO ice.sales.t VALUES (1, 'a', 5)").await;
    let batches = door.ok("SELECT id, name, c FROM ice.sales.t").await;
    assert_eq!(one_row_strings(&batches), (1, "a".to_string(), 5));
}

#[tokio::test]
async fn ansi_named_column_list_short_payload_still_fills_write_default() {
    let door = door_with_tables().await;
    door.ok("INSERT INTO ice.sales.t (id, name) VALUES (8, 'h')")
        .await;
    let batches = door.ok("SELECT id, name, c FROM ice.sales.t").await;
    assert_eq!(one_row_strings(&batches), (8, "h".to_string(), 5));
}

#[tokio::test]
async fn ansi_short_insert_select_keeps_column_count_mismatch() {
    let door = door_with_tables().await;
    let err = door.err("INSERT INTO ice.sales.t SELECT 9, 'i'").await;
    assert!(err.contains("Column count doesn't match"), "{err}");
    assert!(!err.contains("NOT_ENOUGH_DATA_COLUMNS"), "{err}");
    let empty = door.ok("SELECT id FROM ice.sales.t").await;
    assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

#[tokio::test]
async fn ansi_wide_and_mixed_values_have_no_arity_condition() {
    let door = door_with_tables().await;
    for sql in [
        "INSERT INTO ice.sales.t VALUES (1, 'a', 5, 'extra')",
        "INSERT INTO ice.sales.t VALUES (1, 'a', 5), (2, 'b')",
    ] {
        match door.sql(sql).await {
            Ok(_) => {}
            Err(err) => assert!(
                !err.to_string().contains("NOT_ENOUGH_DATA_COLUMNS"),
                "{sql}: {err}"
            ),
        }
    }
}

#[tokio::test]
async fn ansi_missing_table_short_values_has_no_arity_condition() {
    let door = door_with_tables().await;
    let err = door
        .err("INSERT INTO ice.sales.never_there VALUES (1, 'a')")
        .await;
    assert!(!err.is_empty());
    assert!(!err.contains("NOT_ENOUGH_DATA_COLUMNS"), "{err}");
}

#[tokio::test]
async fn ansi_short_overwrite_values_has_no_arity_condition() {
    let door = door_with_tables().await;
    match door
        .sql("INSERT OVERWRITE ice.sales.t VALUES (8, 'h')")
        .await
    {
        Ok(_) => {}
        Err(err) => assert!(
            !err.to_string().contains("NOT_ENOUGH_DATA_COLUMNS"),
            "{err}"
        ),
    }
}

#[tokio::test]
async fn ansi_default_in_outer_select_under_with_refuses_unresolved() {
    let door = door_with_tables().await;
    for sql in [
        "INSERT INTO ice.sales.t WITH x AS (SELECT 20 AS id, 'z' AS name) \
         SELECT id, name, DEFAULT FROM x",
        "INSERT OVERWRITE ice.sales.p (id, name, c) PARTITION (id) \
         WITH x AS (SELECT 20 AS id, 'z' AS name) SELECT id, name, DEFAULT FROM x",
    ] {
        let err = door.err(sql).await;
        assert!(
            err.contains("UNRESOLVED_COLUMN") && err.contains("`DEFAULT`") && err.contains("42703"),
            "{sql}: {err}"
        );
    }
    for table in ["t", "p"] {
        let empty = door.ok(&format!("SELECT id FROM ice.sales.{table}")).await;
        assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }
}
