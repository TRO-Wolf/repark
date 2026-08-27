//! Model: Grok 4.6 xHigh
//!
//! ANSI-door pins for Spark-written partitioned v3 DV and equality-delete
//! fixtures. This door refuses `CALL`; adoption uses `Catalog::register_table`.
//!
//! pins: v3e-3-partitioned-eqdel-fixtures/C-007, C-008, C-010

use std::collections::HashSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::spec::FormatVersion;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

use crate::execute;

const PART_DV_TABLE: &str = "/tmp/repark-v3e3-partdv/ns/v3part";
const EQ_DV_TABLE: &str = "/tmp/repark-v3e3-eqdel/ns/v3eq";

static PART_DV_LOCK: Mutex<()> = Mutex::new(());
static EQ_DV_LOCK: Mutex<()> = Mutex::new(());

struct DirLock {
    path: PathBuf,
}

impl DirLock {
    fn acquire(path: &str) -> Self {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("fixture lock parent");
        }
        let started = Instant::now();
        loop {
            match fs::create_dir(&path) {
                Ok(()) => return Self { path },
                Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                    assert!(
                        started.elapsed() <= Duration::from_mins(2),
                        "fixture lock {}: held for 2 minutes (no steal)",
                        path.display()
                    );
                    std::thread::sleep(Duration::from_millis(25));
                }
                Err(err) => panic!("fixture lock {}: {err}", path.display()),
            }
        }
    }
}

impl Drop for DirLock {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.path);
    }
}

struct SparkFixture {
    _thread: MutexGuard<'static, ()>,
    _cross_process: DirLock,
    metadata_file: String,
}

fn copy_dir_all(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("create dest");
    for entry in fs::read_dir(from).expect("read src") {
        let entry = entry.expect("dirent");
        let dest = to.join(entry.file_name());
        if entry.file_type().expect("ft").is_dir() {
            copy_dir_all(&entry.path(), &dest);
        } else {
            fs::copy(entry.path(), dest).expect("copy file");
        }
    }
}

fn materialize(
    lock: &'static Mutex<()>,
    src_name: &str,
    dest: &str,
    metadata: &str,
) -> SparkFixture {
    let held = lock
        .lock()
        .expect("ansi partitioned-equality-delete fixture lock");
    let cross_process = DirLock::acquire(&format!("{dest}.lock"));
    let dest_path = PathBuf::from(dest);
    if dest_path.exists() {
        fs::remove_dir_all(&dest_path).expect("clear previous fixture");
    }
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .join("repark-spark/src/tests/fixtures")
        .join(src_name);
    copy_dir_all(&src, &dest_path);
    let metadata_file = dest_path.join("metadata").join(metadata);
    assert!(
        metadata_file.is_file(),
        "Spark fixture must include Hadoop-named {metadata}"
    );
    SparkFixture {
        _thread: held,
        _cross_process: cross_process,
        metadata_file: metadata_file.to_string_lossy().into_owned(),
    }
}

struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: std::sync::Arc<dyn Catalog>,
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

    async fn live_triples(&self, table: &str) -> Vec<(i32, String, i32)> {
        let batches = self
            .sql(&format!(
                "SELECT id, name, part FROM ice.sales.{table} ORDER BY id"
            ))
            .await
            .unwrap_or_else(|err| panic!("select: {err}"));
        let mut rows = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap_or_else(|| panic!("id Int32, got {:?}", batch.schema()));
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("name Utf8, got {:?}", batch.schema()));
            let parts = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap_or_else(|| panic!("part Int32, got {:?}", batch.schema()));
            for index in 0..batch.num_rows() {
                rows.push((
                    ids.value(index),
                    names.value(index).to_string(),
                    parts.value(index),
                ));
            }
        }
        rows
    }
}

async fn door() -> Door {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir
        .path()
        .to_str()
        .expect("utf8 warehouse")
        .to_string();
    let catalog: std::sync::Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .expect("memory catalog");
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", std::sync::Arc::clone(&catalog))
        .await
        .expect("register catalog");
    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "ice".to_string(),
        std::sync::Arc::clone(&catalog),
        LocationPolicy::TempFallbackAllowed {
            root: warehouse_dir.path().to_path_buf(),
        },
    );
    catalogs.note_local_warehouse_root(&warehouse);
    let location = format!("{warehouse}/sales");
    let read_only = HashSet::new();
    execute(
        EngineContext::new(&ctx, &catalogs, &read_only),
        &format!("CREATE SCHEMA ice.sales WITH (location = '{location}')"),
    )
    .await
    .expect("create schema")
    .collect()
    .await
    .expect("collect create schema");
    Door {
        ctx,
        catalogs,
        catalog,
        _warehouse_dir: warehouse_dir,
    }
}

async fn adopt(door: &Door, ident: &str, metadata_file: &str) {
    let table_ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), ident.to_string());
    door.catalog
        .register_table(&table_ident, metadata_file.to_string())
        .await
        .unwrap_or_else(|err| panic!("register_table {ident}: {err}"));
    repark_iceberg::catalog::invalidate_catalog_namespaces(
        &door.ctx,
        std::sync::Arc::clone(&door.catalog),
        "ice",
        &["sales"],
    )
    .await
    .expect("invalidate after register_table");
    let loaded = door
        .catalog
        .load_table(&table_ident)
        .await
        .expect("load adopted");
    assert_eq!(loaded.metadata().format_version(), FormatVersion::V3);
}

#[tokio::test]
async fn ansi_partitioned_v3_dv_live_rows_match_spark() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    assert_eq!(
        door.live_triples("partdv").await,
        vec![
            (1, "a".to_string(), 0),
            (3, "c".to_string(), 0),
            (4, "d".to_string(), 1),
            (6, "f".to_string(), 1),
        ]
    );
}

#[tokio::test]
async fn ansi_equality_delete_alongside_dv_live_rows_match_spark() {
    let fixture = materialize(
        &EQ_DV_LOCK,
        "v3-spark-eq-dv",
        EQ_DV_TABLE,
        "v4.metadata.json",
    );
    let door = door().await;
    adopt(&door, "eqdv", &fixture.metadata_file).await;
    assert_eq!(
        door.live_triples("eqdv").await,
        vec![(2, "b".to_string(), 0), (3, "c".to_string(), 1)]
    );
}

#[tokio::test]
async fn ansi_equality_delete_delete_files_name_both_kinds() {
    let fixture = materialize(
        &EQ_DV_LOCK,
        "v3-spark-eq-dv",
        EQ_DV_TABLE,
        "v4.metadata.json",
    );
    let door = door().await;
    adopt(&door, "eqdv", &fixture.metadata_file).await;
    let batches = door
        .sql("SELECT content FROM ice.sales.eqdv$delete_files")
        .await
        .unwrap_or_else(|err| panic!("delete_files: {err}"));
    let mut contents = Vec::new();
    for batch in &batches {
        let column = batch.column(0);
        for index in 0..batch.num_rows() {
            let value = if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
                values.value(index)
            } else if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
                i32::try_from(values.value(index)).expect("content fits i32")
            } else {
                panic!("content Int32/Int64, got {:?}", batch.schema());
            };
            contents.push(value);
        }
    }
    assert!(
        contents.contains(&1) && contents.contains(&2),
        "ANSI .delete_files must list Puffin (1) and equality-delete (2): {contents:?}"
    );
}
