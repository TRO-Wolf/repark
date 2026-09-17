use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema as IcebergSchema, Type};
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
