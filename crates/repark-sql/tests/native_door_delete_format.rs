use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::Int32Type;
use iceberg::spec::{DataContentType, DataFileFormat};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

const READ: &str = "SELECT id FROM ice.ns.t ORDER BY id";

struct Shape {
    sql: &'static str,
    survivors: &'static [i32],
    position_deletes: u64,
}

const SHAPES: [Shape; 5] = [
    Shape {
        sql: "DELETE FROM ice.ns.t WHERE id BETWEEN 1 AND 3",
        survivors: &[0, 4],
        position_deletes: 3,
    },
    Shape {
        sql: "DELETE FROM ice.ns.t WHERE id = 2",
        survivors: &[0, 1, 3, 4],
        position_deletes: 1,
    },
    Shape {
        sql: "UPDATE ice.ns.t SET id = id + 10 WHERE id BETWEEN 1 AND 3",
        survivors: &[0, 4, 11, 12, 13],
        position_deletes: 3,
    },
    Shape {
        sql: "MERGE INTO ice.ns.t t USING (SELECT 2 AS id) s ON t.id = s.id \
              WHEN MATCHED THEN DELETE",
        survivors: &[0, 1, 3, 4],
        position_deletes: 1,
    },
    Shape {
        sql: "MERGE INTO ice.ns.t t USING (SELECT 2 AS id) s ON t.id = s.id \
              WHEN MATCHED THEN UPDATE SET id = 12",
        survivors: &[0, 1, 3, 4, 12],
        position_deletes: 1,
    },
];

fn ident() -> TableIdent {
    TableIdent::from_strs(["ns", "t"]).expect("ident")
}

fn native_session() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("native session")
}

async fn attempt(session: &ReparkSession, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let frame = session.sql(sql).await.map_err(|error| error.to_string())?;
    frame.collect().await.map_err(|error| error.to_string())
}

async fn run(session: &ReparkSession, sql: &str) {
    attempt(session, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` must succeed: {error}"));
}

async fn ids(session: &ReparkSession) -> Result<Vec<i32>, String> {
    let batches = attempt(session, READ).await?;
    Ok(batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .to_vec()
        })
        .collect())
}

fn catalog(session: &ReparkSession) -> Arc<dyn Catalog> {
    session
        .catalogs_snapshot()
        .get("ice")
        .expect("ice catalog")
        .clone()
}

async fn load(session: &ReparkSession) -> Table {
    catalog(session)
        .load_table(&ident())
        .await
        .expect("load table")
}

async fn seeded(properties: &[(&str, &str)]) -> (ReparkSession, TempDir) {
    let warehouse = TempDir::new().expect("warehouse");
    let session = native_session();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    let mut keys = vec![
        "'write.delete.mode'",
        "'write.update.mode'",
        "'write.merge.mode'",
    ];
    let mut values = vec!["'merge-on-read'"; 3];
    let quoted: Vec<(String, String)> = properties
        .iter()
        .map(|(key, value)| (format!("'{key}'"), format!("'{value}'")))
        .collect();
    for (key, value) in &quoted {
        keys.push(key);
        values.push(value);
    }
    run(&session, "CREATE SCHEMA ice.ns").await;
    run(
        &session,
        &format!(
            "CREATE TABLE ice.ns.t (id INT) WITH (format_version = 2, extra_properties = \
             MAP(ARRAY[{}], ARRAY[{}]))",
            keys.join(", "),
            values.join(", ")
        ),
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.t VALUES (0), (1), (2), (3), (4)",
    )
    .await;
    (session, warehouse)
}

async fn live_delete_files(table: &Table) -> Vec<(DataFileFormat, u64)> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Vec::new();
    };
    let manifests = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .expect("manifest list");
    let mut found = Vec::new();
    for manifest_file in manifests.entries() {
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("manifest");
        for entry in manifest.entries() {
            let file = entry.data_file();
            if entry.is_alive() && file.content_type() == DataContentType::PositionDeletes {
                found.push((file.file_format(), file.record_count()));
            }
        }
    }
    found
}

async fn fresh_session(table: &Table, warehouse: &TempDir) -> ReparkSession {
    let handle = repark_iceberg::catalog::memory_catalog(warehouse.path().to_str().expect("path"))
        .await
        .expect("memory catalog");
    handle
        .create_namespace(&NamespaceIdent::new("ns".to_string()), HashMap::new())
        .await
        .expect("namespace");
    handle
        .register_table(
            &ident(),
            table
                .metadata_location()
                .expect("metadata location")
                .to_string(),
        )
        .await
        .expect("register table");
    let session = native_session();
    session
        .register_iceberg_catalog("ice", handle)
        .await
        .expect("catalog");
    session
}

async fn assert_shape(properties: &[(&str, &str)], shape: &Shape, delete_format: DataFileFormat) {
    let (session, warehouse) = seeded(properties).await;
    let sql = shape.sql;
    let outcome = attempt(&session, sql).await;
    assert!(
        outcome.is_ok(),
        "`{sql}` on {properties:?} must complete: {outcome:?}"
    );
    let table = load(&session).await;
    assert_eq!(
        live_delete_files(&table).await,
        vec![(delete_format, shape.position_deletes)],
        "`{sql}` on {properties:?}: one position-delete file in {delete_format:?}"
    );
    assert_eq!(
        ids(&session).await,
        Ok(shape.survivors.to_vec()),
        "`{sql}` on {properties:?}: same-session survivors"
    );
    let fresh = fresh_session(&table, &warehouse).await;
    assert_eq!(
        ids(&fresh).await,
        Ok(shape.survivors.to_vec()),
        "`{sql}` on {properties:?}: fresh-session survivors"
    );
}

#[tokio::test]
async fn native_dml_and_merge_on_inherited_avro_write_and_read_back_an_avro_delete() {
    for shape in &SHAPES {
        assert_shape(
            &[("write.format.default", "avro")],
            shape,
            DataFileFormat::Avro,
        )
        .await;
    }
}

#[tokio::test]
async fn native_dml_and_merge_on_inherited_orc_write_and_read_back_an_orc_delete() {
    for shape in &SHAPES {
        assert_shape(
            &[("write.format.default", "orc")],
            shape,
            DataFileFormat::Orc,
        )
        .await;
    }
}

#[tokio::test]
async fn native_dml_and_merge_with_an_explicit_parquet_delete_format_on_avro_data_write_parquet() {
    for shape in &SHAPES {
        assert_shape(
            &[
                ("write.format.default", "avro"),
                ("write.delete.format.default", "parquet"),
            ],
            shape,
            DataFileFormat::Parquet,
        )
        .await;
    }
}
