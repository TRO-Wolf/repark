use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::spec::ManifestContentType;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

const KEY: &str = "review-test-key";

struct State {
    snapshots: Vec<i64>,
    files: HashSet<String>,
    objects: usize,
}

struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    warehouse: TempDir,
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

    async fn err(&self, sql: &str) -> datafusion::error::DataFusionError {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(err) => err,
        }
    }

    async fn table(&self, table: &str) -> iceberg::table::Table {
        let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
        self.catalog.load_table(&ident).await.expect("load table")
    }

    async fn capture(&self, table: &str) -> State {
        let loaded = self.table(table).await;
        let mut snapshots: Vec<i64> = loaded
            .metadata()
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .collect();
        snapshots.sort_unstable();
        let mut files = HashSet::new();
        if let Some(snapshot) = loaded.metadata().current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(loaded.file_io(), loaded.metadata())
                .await
                .expect("manifest list");
            for manifest_file in manifest_list.entries() {
                if manifest_file.content != ManifestContentType::Data {
                    continue;
                }
                let manifest = manifest_file
                    .load_manifest(loaded.file_io())
                    .await
                    .expect("manifest");
                for entry in manifest.entries() {
                    if entry.is_alive() {
                        files.insert(entry.data_file().file_path().to_string());
                    }
                }
            }
        }
        State {
            snapshots,
            files,
            objects: count_objects(self.warehouse.path()),
        }
    }

    async fn assert_unchanged(&self, table: &str, before: &State) {
        let after = self.capture(table).await;
        assert_eq!(after.snapshots, before.snapshots);
        assert_eq!(after.files, before.files);
        assert_eq!(after.objects, before.objects);
    }

    async fn rows(&self, table: &str) -> Vec<(i64, String)> {
        let batches = self
            .ok(&format!(
                "SELECT id, name FROM ice.sales.{table} ORDER BY id"
            ))
            .await;
        let mut rows = Vec::new();
        for batch in &batches {
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name Utf8");
            if let Some(ids) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for row in 0..batch.num_rows() {
                    rows.push((ids.value(row), names.value(row).to_string()));
                }
            } else {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int32Array>()
                    .expect("id Int32 or Int64");
                for row in 0..batch.num_rows() {
                    rows.push((i64::from(ids.value(row)), names.value(row).to_string()));
                }
            }
        }
        rows
    }
}

fn count_objects(root: &std::path::Path) -> usize {
    let mut pending = vec![root.to_path_buf()];
    let mut count = 0usize;
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                count += 1;
            }
        }
    }
    count
}

fn assert_refusal(error: datafusion::error::DataFusionError, table: &str) {
    let mapped = repark_core::engine_err(error);
    assert!(
        matches!(&mapped, repark_core::Error::NotImplemented(_)),
        "refusal must map to Unsupported, got: {mapped:?}"
    );
    assert_eq!(
        mapped.exception_class(),
        repark_core::ErrorClass::Unsupported
    );
    let message = mapped.to_string();
    for needle in [
        table,
        "encryption.key-id",
        "no table encryption",
        "plaintext",
        "ENC-1",
    ] {
        assert!(
            message.contains(needle),
            "refusal must name {needle}, got: {message}"
        );
    }
    assert!(
        !message.contains(KEY),
        "refusal must never echo the key value, got: {message}"
    );
}

async fn door() -> Door {
    let warehouse = TempDir::new().expect("warehouse tempdir");
    let root = warehouse
        .path()
        .to_str()
        .expect("utf8 warehouse")
        .to_string();
    let catalog: Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&root)
        .await
        .expect("memory catalog");
    catalog
        .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
        .await
        .expect("namespace");
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", Arc::clone(&catalog))
        .await
        .expect("register catalog");
    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "ice".to_string(),
        Arc::clone(&catalog),
        LocationPolicy::TempFallbackAllowed {
            root: warehouse.path().to_path_buf(),
        },
    );
    Door {
        ctx,
        catalogs,
        catalog,
        warehouse,
    }
}

fn keyed_assignment() -> String {
    format!("extra_properties = MAP(ARRAY['encryption.key-id'], ARRAY['{KEY}'])")
}

async fn create_keyed(door: &Door, table: &str) {
    door.ok(&format!(
        "CREATE TABLE ice.sales.{table} (id INT, name VARCHAR) WITH ({})",
        keyed_assignment()
    ))
    .await;
}

async fn seed_keyed(door: &Door, table: &str) {
    door.ok(&format!(
        "CREATE TABLE ice.sales.{table} AS SELECT CAST(1 AS BIGINT) AS id, 'a' AS name \
         UNION ALL SELECT CAST(2 AS BIGINT) AS id, 'b' AS name"
    ))
    .await;
    door.ok(&format!(
        "ALTER TABLE ice.sales.{table} SET PROPERTIES ({})",
        keyed_assignment()
    ))
    .await;
}

#[tokio::test]
async fn encrypted_table_insert_values_refuses() {
    let door = door().await;
    create_keyed(&door, "enc").await;
    let before = door.capture("enc").await;
    let error = door.err("INSERT INTO ice.sales.enc VALUES (1, 'a')").await;
    assert_refusal(error, "sales.enc");
    door.assert_unchanged("enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_insert_select_refuses() {
    let door = door().await;
    create_keyed(&door, "enc").await;
    door.ok("CREATE TABLE ice.sales.src AS SELECT CAST(3 AS BIGINT) AS id, 'c' AS name")
        .await;
    let before = door.capture("enc").await;
    let error = door
        .err("INSERT INTO ice.sales.enc SELECT id, name FROM ice.sales.src")
        .await;
    assert_refusal(error, "sales.enc");
    door.assert_unchanged("enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_insert_overwrite_partition_refuses() {
    let door = door().await;
    door.ok("CREATE TABLE ice.sales.enc (id INT, name VARCHAR) WITH (partitioning = ARRAY['id'])")
        .await;
    door.ok("INSERT INTO ice.sales.enc VALUES (1, 'a'), (2, 'b')")
        .await;
    door.ok(&format!(
        "ALTER TABLE ice.sales.enc SET PROPERTIES ({})",
        keyed_assignment()
    ))
    .await;
    let before = door.capture("enc").await;
    let error = door
        .err("INSERT OVERWRITE ice.sales.enc (name) PARTITION (id = 1) SELECT 'z'")
        .await;
    assert_refusal(error, "sales.enc");
    door.assert_unchanged("enc", &before).await;
}

#[tokio::test]
async fn encrypted_ctas_with_key_refuses_without_leaving_a_table() {
    let door = door().await;
    let objects_before = count_objects(door.warehouse.path());
    let error = door
        .err(&format!(
            "CREATE TABLE ice.sales.ctas_enc WITH ({}) AS SELECT CAST(1 AS BIGINT) AS id, 'a' AS name",
            keyed_assignment()
        ))
        .await;
    assert_refusal(error, "ctas_enc");
    let ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "ctas_enc".to_string(),
    );
    assert!(door.catalog.load_table(&ident).await.is_err());
    assert_eq!(count_objects(door.warehouse.path()), objects_before);
}

#[tokio::test]
async fn replace_without_key_onto_keyed_table_refuses() {
    let door = door().await;
    seed_keyed(&door, "enc").await;
    let before = door.capture("enc").await;
    let error = door
        .err("CREATE OR REPLACE TABLE ice.sales.enc AS SELECT CAST(9 AS BIGINT) AS id, 'z' AS name")
        .await;
    assert_refusal(error, "sales.enc");
    door.assert_unchanged("enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_merge_refuses() {
    let door = door().await;
    seed_keyed(&door, "enc").await;
    door.ok("CREATE TABLE ice.sales.src AS SELECT CAST(1 AS BIGINT) AS id, 'z' AS name")
        .await;
    let before = door.capture("enc").await;
    let error = door
        .err(
            "MERGE INTO ice.sales.enc AS t USING ice.sales.src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
        )
        .await;
    assert_refusal(error, "ice.sales.enc");
    door.assert_unchanged("enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_update_refuses() {
    let door = door().await;
    seed_keyed(&door, "enc").await;
    let before = door.capture("enc").await;
    let error = door
        .err("UPDATE ice.sales.enc SET name = 'z' WHERE id = 1")
        .await;
    assert_refusal(error, "sales.enc");
    door.assert_unchanged("enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_delete_refuses() {
    let door = door().await;
    seed_keyed(&door, "enc").await;
    let before = door.capture("enc").await;
    let error = door.err("DELETE FROM ice.sales.enc WHERE id = 1").await;
    assert_refusal(error, "sales.enc");
    door.assert_unchanged("enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_whole_delete_refuses() {
    let door = door().await;
    seed_keyed(&door, "enc").await;
    let before = door.capture("enc").await;
    let error = door.err("DELETE FROM ice.sales.enc").await;
    assert_refusal(error, "sales.enc");
    door.assert_unchanged("enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_truncate_refuses() {
    let door = door().await;
    seed_keyed(&door, "enc").await;
    let before = door.capture("enc").await;
    let error = door.err("TRUNCATE TABLE ice.sales.enc").await;
    assert_refusal(error, "sales.enc");
    door.assert_unchanged("enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_alter_added_key_then_insert_refuses() {
    let door = door().await;
    door.ok("CREATE TABLE ice.sales.enc AS SELECT CAST(1 AS BIGINT) AS id, 'a' AS name")
        .await;
    door.ok("INSERT INTO ice.sales.enc VALUES (2, 'b')").await;
    door.ok(&format!(
        "ALTER TABLE ice.sales.enc SET PROPERTIES ({})",
        keyed_assignment()
    ))
    .await;
    let before = door.capture("enc").await;
    let error = door.err("INSERT INTO ice.sales.enc VALUES (3, 'c')").await;
    assert_refusal(error, "sales.enc");
    door.assert_unchanged("enc", &before).await;
}

#[tokio::test]
async fn encrypted_table_empty_key_value_refuses() {
    let door = door().await;
    door.ok("CREATE TABLE ice.sales.enc (id INT, name VARCHAR) \
         WITH (extra_properties = MAP(ARRAY['encryption.key-id'], ARRAY['']))")
        .await;
    let before = door.capture("enc").await;
    let error = door.err("INSERT INTO ice.sales.enc VALUES (1, 'a')").await;
    assert_refusal(error, "sales.enc");
    door.assert_unchanged("enc", &before).await;
}

#[tokio::test]
async fn encrypted_lookalike_keys_stay_writable() {
    for (table, property) in [
        ("look_a", "encryption.keyid"),
        ("look_b", "encryption.key-id-x"),
    ] {
        let door = door().await;
        door.ok(&format!(
            "CREATE TABLE ice.sales.{table} (id INT, name VARCHAR) WITH (extra_properties = MAP(ARRAY['{property}'], ARRAY['{KEY}']))"
        ))
        .await;
        door.ok(&format!("INSERT INTO ice.sales.{table} VALUES (1, 'a')"))
            .await;
        assert_eq!(
            door.rows(table).await,
            vec![(1, "a".to_string())],
            "{property} must not refuse"
        );
    }
}

#[tokio::test]
async fn encrypted_table_create_and_select_run() {
    let door = door().await;
    create_keyed(&door, "enc").await;
    assert_eq!(door.rows("enc").await, Vec::<(i64, String)>::new());
    let table = door.table("enc").await;
    assert_eq!(
        table
            .metadata()
            .properties()
            .get("encryption.key-id")
            .map(String::as_str),
        Some(KEY)
    );
}

#[tokio::test]
async fn table_without_key_stays_writable() {
    let door = door().await;
    door.ok(
        "CREATE TABLE ice.sales.twin AS SELECT CAST(1 AS BIGINT) AS id, 'a' AS name \
         UNION ALL SELECT CAST(2 AS BIGINT) AS id, 'b' AS name",
    )
    .await;
    door.ok("INSERT INTO ice.sales.twin VALUES (3, 'c')").await;
    door.ok("UPDATE ice.sales.twin SET name = 'z' WHERE id = 1")
        .await;
    door.ok("DELETE FROM ice.sales.twin WHERE id = 2").await;
    assert_eq!(
        door.rows("twin").await,
        vec![(1, "z".to_string()), (3, "c".to_string())]
    );
    door.ok("TRUNCATE TABLE ice.sales.twin").await;
    assert_eq!(door.rows("twin").await, Vec::<(i64, String)>::new());
}
