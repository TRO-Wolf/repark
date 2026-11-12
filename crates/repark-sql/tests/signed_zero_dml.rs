use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use repark_core::{DataFusionDialect, ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

const KINDS: [(&str, u8); 3] = [
    ("copy-on-write", 2),
    ("merge-on-read", 2),
    ("copy-on-write", 3),
];

const ZERO_PREDICATES: [&str; 5] = [
    "d = 0.0",
    "d = -0.0",
    "d IN (0.0, 1.0)",
    "0.0 = d",
    "d = CAST('-0.0' AS DOUBLE)",
];

struct Door {
    session: ReparkSession,
    dialect: Arc<dyn SqlDialect>,
    spark: bool,
    root: String,
    _warehouse: TempDir,
}

async fn open(spark: bool) -> Door {
    let warehouse = TempDir::new().expect("warehouse");
    let session = if spark {
        let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
        ReparkSession::builder()
            .with_extension(Arc::new(SparkExtension))
            .with_sql_dialect(dialect)
            .config("repark.sql.allowCreateFormatVersion3", "true")
            .build()
            .expect("spark session")
    } else {
        let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
        ReparkSession::builder()
            .with_sql_dialect(dialect)
            .build()
            .expect("native session")
    };
    let root = warehouse.path().to_str().expect("utf8").to_string();
    session
        .register_memory_catalog("ice", &root)
        .await
        .expect("catalog");
    let dialect: Arc<dyn SqlDialect> = if spark {
        Arc::new(SparkDialect)
    } else {
        Arc::new(DataFusionDialect)
    };
    let door = Door {
        session,
        dialect,
        spark,
        root,
        _warehouse: warehouse,
    };
    door.session
        .sql("CREATE SCHEMA ice.ns")
        .await
        .expect("create schema")
        .collect()
        .await
        .expect("create schema");
    door
}

impl Door {
    async fn run(&self, sql: &str) -> Vec<RecordBatch> {
        self.session
            .sql_with(&self.dialect, sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
    }

    async fn create(&self, table: &str, mode: &str, version: u8) {
        if self.spark {
            self.run(&format!(
                "CREATE TABLE ice.ns.{table} (id BIGINT, d DOUBLE) USING iceberg TBLPROPERTIES \
                 ('format-version' = '{version}', 'write.delete.mode' = '{mode}', \
                 'write.update.mode' = '{mode}')"
            ))
            .await;
        } else {
            let schema = Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "d", Type::Primitive(PrimitiveType::Double)).into(),
                ])
                .build()
                .expect("schema");
            let creation = TableCreation::builder()
                .name(table.to_string())
                .location(format!("{}/ns/{table}", self.root))
                .schema(schema)
                .format_version(if version == 3 {
                    FormatVersion::V3
                } else {
                    FormatVersion::V2
                })
                .properties(HashMap::from([
                    ("write.delete.mode".to_string(), mode.to_string()),
                    ("write.update.mode".to_string(), mode.to_string()),
                ]))
                .build();
            let catalog: Arc<dyn Catalog> = self
                .session
                .catalogs_snapshot()
                .get("ice")
                .expect("ice catalog")
                .clone();
            catalog
                .create_table(&NamespaceIdent::new("ns".to_string()), creation)
                .await
                .expect("create through the catalog");
            self.session
                .refresh_catalog_provider("ice")
                .await
                .expect("refresh the provider");
        }
        for row in [
            "(1, CAST('-0.0' AS DOUBLE))",
            "(2, CAST('0.0' AS DOUBLE))",
            "(3, 1.5)",
        ] {
            self.run(&format!("INSERT INTO ice.ns.{table} VALUES {row}"))
                .await;
        }
    }

    async fn ids(&self, table: &str) -> Vec<i64> {
        let batches = self.run(&format!("SELECT id FROM ice.ns.{table}")).await;
        let mut ids = Vec::new();
        for batch in &batches {
            let column = cast(batch.column(0), &DataType::Int64).expect("id as Int64");
            let column = column.as_any().downcast_ref::<Int64Array>().expect("Int64");
            ids.extend(column.values().iter().copied());
        }
        ids.sort_unstable();
        ids
    }
}

#[tokio::test]
async fn a_float_zero_delete_removes_both_zero_files_on_both_doors_and_three_kinds() {
    for spark in [true, false] {
        let door = open(spark).await;
        for (kind_index, (mode, version)) in KINDS.into_iter().enumerate() {
            for (form_index, predicate) in ZERO_PREDICATES.into_iter().enumerate() {
                let table = format!("del_{kind_index}_{form_index}");
                door.create(&table, mode, version).await;
                door.run(&format!("DELETE FROM ice.ns.{table} WHERE {predicate}"))
                    .await;
                assert_eq!(
                    door.ids(&table).await,
                    vec![3],
                    "spark={spark} {mode} v{version}: DELETE WHERE {predicate}"
                );
            }
        }
    }
}

#[tokio::test]
async fn a_float_zero_update_touches_both_zero_rows_on_both_doors_and_three_kinds() {
    for spark in [true, false] {
        let door = open(spark).await;
        for (kind_index, (mode, version)) in KINDS.into_iter().enumerate() {
            let table = format!("upd_{kind_index}");
            door.create(&table, mode, version).await;
            door.run(&format!(
                "UPDATE ice.ns.{table} SET id = id + 10 WHERE d = CAST('-0.0' AS DOUBLE)"
            ))
            .await;
            assert_eq!(
                door.ids(&table).await,
                vec![3, 11, 12],
                "spark={spark} {mode} v{version}"
            );
        }
    }
}

#[tokio::test]
async fn insert_select_ctas_and_merge_conditions_carry_both_zero_rows() {
    for spark in [true, false] {
        let door = open(spark).await;
        for (kind_index, (mode, version)) in KINDS.into_iter().enumerate() {
            let source = format!("src_{kind_index}");
            let target = format!("dst_{kind_index}");
            door.create(&source, mode, version).await;
            door.create(&target, mode, version).await;
            door.run(&format!("DELETE FROM ice.ns.{target} WHERE id > 0"))
                .await;
            door.run(&format!(
                "INSERT INTO ice.ns.{target} SELECT id, d FROM ice.ns.{source} WHERE d = 0.0"
            ))
            .await;
            assert_eq!(
                door.ids(&target).await,
                vec![1, 2],
                "spark={spark} {mode} v{version}"
            );
            if !spark {
                continue;
            }
            let copy = format!("ctas_{kind_index}");
            door.run(&format!(
                "CREATE TABLE ice.ns.{copy} USING iceberg AS SELECT id, d FROM ice.ns.{source} \
                 WHERE d <=> -0.0"
            ))
            .await;
            assert_eq!(door.ids(&copy).await, vec![1, 2], "{mode} v{version}");
            door.run(&format!(
                "MERGE INTO ice.ns.{source} t USING (SELECT id FROM ice.ns.{source}) u \
                 ON t.id = u.id WHEN MATCHED AND t.d = 0.0 THEN UPDATE SET id = t.id + 10"
            ))
            .await;
            assert_eq!(
                door.ids(&source).await,
                vec![3, 11, 12],
                "{mode} v{version}"
            );
        }
    }
}
