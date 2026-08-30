//! Model: Grok 4.6 xHigh
//! ANSI-door twins for V3E-4: branch/tag DDL and `FOR VERSION AS OF` over a v3 fixture.
//! pins: v3e-4-refs-time-travel/C-002, C-005, C-006, C-007, C-013, C-014
//! pins: rp-3-fork-repin/C-004

use std::collections::HashSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::spec::FormatVersion;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

use crate::execute;

const PART_DV_TABLE: &str = "/tmp/repark-v3e3-partdv/ns/v3part";
static PART_DV_LOCK: Mutex<()> = Mutex::new(());
const DV_LIVE: [(i32, &str, i32); 4] = [(1, "a", 0), (3, "c", 0), (4, "d", 1), (6, "f", 1)];

struct DirLock {
    path: PathBuf,
}

impl DirLock {
    fn acquire(path: &str) -> Self {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("lock parent");
        }
        let started = Instant::now();
        loop {
            match fs::create_dir(&path) {
                Ok(()) => return Self { path },
                Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                    assert!(
                        started.elapsed() <= Duration::from_mins(2),
                        "lock {} held 2 minutes",
                        path.display()
                    );
                    std::thread::sleep(Duration::from_millis(25));
                }
                Err(err) => panic!("lock {}: {err}", path.display()),
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
            fs::copy(entry.path(), dest).expect("copy");
        }
    }
}

fn materialize_writable() -> SparkFixture {
    let held = PART_DV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let cross_process = DirLock::acquire(&format!("{PART_DV_TABLE}.lock"));
    let dest_path = PathBuf::from(PART_DV_TABLE);
    if dest_path.exists() {
        fs::remove_dir_all(&dest_path).expect("clear fixture");
    }
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .join("repark-spark/src/tests/fixtures/v3-spark-part-dv");
    copy_dir_all(&src, &dest_path);
    let hadoop = dest_path.join("metadata").join("v3.metadata.json");
    let rewritten = dest_path
        .join("metadata")
        .join("3-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.metadata.json");
    fs::copy(&hadoop, &rewritten).expect("V3-ADOPT-1 rewrite");
    SparkFixture {
        _thread: held,
        _cross_process: cross_process,
        metadata_file: rewritten.to_string_lossy().into_owned(),
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

    async fn ok(&self, sql: &str) {
        self.sql(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"));
    }

    async fn live_triples(&self, sql: &str) -> Vec<(i32, String, i32)> {
        let batches = self
            .sql(sql)
            .await
            .unwrap_or_else(|err| panic!("{sql}: {err}"));
        let mut rows = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id Int32");
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name Utf8");
            let parts = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("part Int32");
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

    async fn snapshot_id(&self, table: &str) -> i64 {
        self.catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.to_string(),
            ))
            .await
            .expect("load")
            .metadata()
            .current_snapshot_id()
            .expect("snapshot")
    }
}

async fn door() -> Door {
    let warehouse_dir = TempDir::new().expect("warehouse");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
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
    .expect("schema")
    .collect()
    .await
    .expect("collect schema");
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
        .unwrap_or_else(|err| panic!("register_table: {err}"));
    repark_iceberg::catalog::invalidate_catalog_namespaces(
        &door.ctx,
        std::sync::Arc::clone(&door.catalog),
        "ice",
        &["sales"],
    )
    .await
    .expect("invalidate");
    let loaded = door.catalog.load_table(&table_ident).await.expect("load");
    assert_eq!(loaded.metadata().format_version(), FormatVersion::V3);
}

fn spark_dv_rows() -> Vec<(i32, String, i32)> {
    DV_LIVE
        .into_iter()
        .map(|(id, name, part)| (id, name.to_string(), part))
        .collect()
}

#[tokio::test]
async fn ansi_create_branch_on_v3_does_not_move_main() {
    let fixture = materialize_writable();
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let s_dv = door.snapshot_id("partdv").await;
    door.ok("INSERT INTO ice.sales.partdv SELECT 7 AS id, 'g' AS name, 0 AS part")
        .await;
    let s_head = door.snapshot_id("partdv").await;
    door.ok("ALTER TABLE ice.sales.partdv CREATE BRANCH audit")
        .await;
    door.ok("ALTER TABLE ice.sales.partdv CREATE TAG freeze")
        .await;
    door.ok(&format!(
        "ALTER TABLE ice.sales.partdv CREATE BRANCH old AS OF VERSION {s_dv}"
    ))
    .await;
    let table = door
        .catalog
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "partdv".to_string(),
        ))
        .await
        .expect("load");
    assert_eq!(table.metadata().current_snapshot_id(), Some(s_head));
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref("audit")
            .map(|snap| snap.snapshot_id()),
        Some(s_head)
    );
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref("freeze")
            .map(|snap| snap.snapshot_id()),
        Some(s_head)
    );
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref("old")
            .map(|snap| snap.snapshot_id()),
        Some(s_dv)
    );
    door.ok("ALTER TABLE ice.sales.partdv DROP BRANCH audit")
        .await;
    let table = door
        .catalog
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "partdv".to_string(),
        ))
        .await
        .expect("load");
    assert!(table.metadata().snapshot_for_ref("audit").is_none());
    assert_eq!(table.metadata().current_snapshot_id(), Some(s_head));
}

#[tokio::test]
async fn ansi_for_version_as_of_over_dvs_matches_spark_live_set() {
    let fixture = materialize_writable();
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let s_dv = door.snapshot_id("partdv").await;
    door.ok("INSERT INTO ice.sales.partdv SELECT 7 AS id, 'g' AS name, 0 AS part")
        .await;
    let at_dv = door
        .live_triples(&format!(
            "SELECT id, name, part FROM ice.sales.partdv FOR VERSION AS OF {s_dv} ORDER BY id"
        ))
        .await;
    assert_eq!(at_dv, spark_dv_rows());
}

#[tokio::test]
async fn ansi_for_version_as_of_branch_name_matches_that_snapshot() {
    let fixture = materialize_writable();
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let s_dv = door.snapshot_id("partdv").await;
    door.ok("INSERT INTO ice.sales.partdv SELECT 7 AS id, 'g' AS name, 0 AS part")
        .await;
    door.ok(&format!(
        "ALTER TABLE ice.sales.partdv CREATE BRANCH old AS OF VERSION {s_dv}"
    ))
    .await;
    let at_branch = door
        .live_triples(
            "SELECT id, name, part FROM ice.sales.partdv FOR VERSION AS OF 'old' ORDER BY id",
        )
        .await;
    assert_eq!(at_branch, spark_dv_rows());
}

#[tokio::test]
async fn ansi_mor_delete_on_a_shared_puffin_keeps_the_untouched_sibling() {
    let fixture = materialize_writable();
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let snapshot = door.snapshot_id("partdv").await;
    door.ok("DELETE FROM ice.sales.partdv WHERE id = 1").await;
    assert_ne!(
        door.snapshot_id("partdv").await,
        snapshot,
        "the shared-Puffin DELETE commits"
    );
    assert_eq!(
        door.live_triples("SELECT id, name, part FROM ice.sales.partdv ORDER BY id")
            .await,
        vec![
            (3, "c".to_string(), 0),
            (4, "d".to_string(), 1),
            (6, "f".to_string(), 1),
        ],
        "id 5 stays deleted"
    );
}

#[tokio::test]
async fn ansi_mor_delete_on_a_spark_written_dv_merges_into_that_file() {
    let fixture = materialize_writable();
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    door.ok("DELETE FROM ice.sales.partdv WHERE id = 3").await;
    assert_eq!(
        door.live_triples("SELECT id, name, part FROM ice.sales.partdv ORDER BY id")
            .await,
        vec![
            (1, "a".to_string(), 0),
            (4, "d".to_string(), 1),
            (6, "f".to_string(), 1),
        ],
        "DELETE id = 3 on the Spark-written part=0 DV"
    );
}
