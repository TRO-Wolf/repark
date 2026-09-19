//! Model: GLM (glm-5.3-flash)
//! ANSI-door pins for column DEFAULT DDL: Spark-equal refuse on every form.
//! pins: v3-6-v3-types/C-005

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
    catalog: Arc<dyn Catalog>,
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

    async fn ok(&self, sql: &str) -> Vec<datafusion::arrow::record_batch::RecordBatch> {
        self.sql(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"))
    }

    async fn ok_typed(
        &self,
        sql: &str,
    ) -> (
        datafusion::arrow::datatypes::SchemaRef,
        Vec<datafusion::arrow::record_batch::RecordBatch>,
    ) {
        let read_only = HashSet::new();
        let frame = execute(
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

    async fn err(&self, sql: &str) -> String {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(err) => err.to_string(),
        }
    }

    async fn table_exists(&self, namespace: &str, table: &str) -> bool {
        self.catalog
            .table_exists(&iceberg::TableIdent::new(
                iceberg::NamespaceIdent::new(namespace.to_string()),
                table.to_string(),
            ))
            .await
            .expect("table_exists")
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

/// A column DEFAULT refuses Spark-equal (Spark 4.1.2 refuses column default values) and leaves
/// no table behind. Mutation: dropping the `ColumnOption::Default` check silently creates the
/// table with the default dropped — the no-table assert below reds.
#[tokio::test]
async fn create_table_column_default_refuses_naming_the_column() {
    let door = door_with_schema().await;
    let err = door
        .err("CREATE TABLE ice.sales.defcol (id INT, tag STRING DEFAULT 'x')")
        .await;
    assert!(err.contains("DEFAULT"), "must name the option: {err}");
    assert!(err.contains("tag"), "must name the column: {err}");
    assert!(
        !door.table_exists("sales", "defcol").await,
        "a refused CREATE must leave no table behind"
    );
}

/// Every DEFAULT DDL form refuses (Spark 4.1.2 refuses the same DDL), while a plain ADD COLUMN
/// leaves NULL on old and new rows — the Spark-measured incidental control.
#[tokio::test]
async fn column_default_ddl_refuses_and_plain_add_stays_null() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.nodef (id INT)").await;

    let add = door
        .err("ALTER TABLE ice.sales.nodef ADD COLUMN tag STRING DEFAULT 'x'")
        .await;
    assert!(
        add.contains("not supported") && add.contains("option"),
        "ADD COLUMN DEFAULT must refuse naming the option: {add}"
    );
    let set = door
        .err("ALTER TABLE ice.sales.nodef ALTER COLUMN id SET DEFAULT 3")
        .await;
    assert!(
        set.contains("not supported") || set.contains("SET DATA TYPE"),
        "ALTER COLUMN SET DEFAULT must refuse: {set}"
    );

    door.ok("INSERT INTO ice.sales.nodef VALUES (1)").await;
    door.ok("ALTER TABLE ice.sales.nodef ADD COLUMN tag STRING")
        .await;
    door.ok("INSERT INTO ice.sales.nodef (id) VALUES (2)").await;
    let (_, batches) = door
        .ok_typed("SELECT id, tag FROM ice.sales.nodef ORDER BY id")
        .await;
    for row in [0, 1] {
        assert!(
            batches[0].column(1).is_null(row),
            "a column added with no default must read NULL on old and new rows"
        );
    }
}
