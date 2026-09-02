//! Model: Grok 4.6 xHigh
//! pins: v3e-4-refs-time-travel/C-001, C-002, C-003, C-004, C-006, C-007, C-008
//! pins: v3e-4-refs-time-travel/C-009, C-010, C-011, C-013, C-014, C-015, C-016
//! Pins format-v3 snapshot refs, time travel over deletion vectors, and maintenance.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use super::super::*;
use super::common::*;

use datafusion::arrow::array::{Int32Array, StringArray};

/// Baked Hadoop location of the V3E-3 partitioned-DV fixture.
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

fn files_with_bytes(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut files = Vec::new();
    for entry in fs::read_dir(root).expect("read fixture tree") {
        let entry = entry.expect("fixture dirent");
        let path = entry.path();
        if entry.file_type().expect("fixture file type").is_dir() {
            files.extend(files_with_bytes(&path));
        } else {
            files.push((path.clone(), fs::read(path).expect("read fixture file")));
        }
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn materialize_part_dv() -> SparkFixture {
    let held = PART_DV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let cross_process = DirLock::acquire(&format!("{PART_DV_TABLE}.lock"));
    let dest_path = PathBuf::from(PART_DV_TABLE);
    if dest_path.exists() {
        fs::remove_dir_all(&dest_path).expect("clear previous fixture");
    }
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/fixtures/v3-spark-part-dv");
    copy_dir_all(&src, &dest_path);
    let hadoop = dest_path.join("metadata").join("v3.metadata.json");
    assert!(hadoop.is_file(), "partitioned-DV fixture metadata");
    // Keep a version-uuid pointer so this fixture's writes do not depend on Hadoop vN bump math.
    let metadata_file = dest_path
        .join("metadata")
        .join("3-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.metadata.json");
    fs::copy(&hadoop, &metadata_file).expect("copy Hadoop metadata onto version-uuid name");
    SparkFixture {
        _thread: held,
        _cross_process: cross_process,
        metadata_file: metadata_file.to_string_lossy().into_owned(),
    }
}

async fn register_adopted(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ident: &str,
    metadata_file: &str,
) {
    execute(
        ctx,
        catalogs,
        &format!(
            "CALL ice.system.register_table(table => '{ident}', \
             metadata_file => '{metadata_file}')"
        ),
    )
    .await
    .expect("register Spark-written v3 fixture")
    .collect()
    .await
    .expect("collect register_table");
}

fn ident() -> TableIdent {
    TableIdent::new(NamespaceIdent::new("sales".into()), "partdv".into())
}

async fn snapshot_id(catalogs: &CatalogRegistry) -> i64 {
    catalogs["ice"]
        .load_table(&ident())
        .await
        .expect("load")
        .metadata()
        .current_snapshot_id()
        .expect("current snapshot")
}

async fn live_triples(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<(i32, String, i32)> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
        .collect()
        .await
        .expect("collect");
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

async fn adopt_and_append(ctx: &SessionContext, catalogs: &CatalogRegistry, metadata_file: &str) {
    register_adopted(ctx, catalogs, "sales.partdv", metadata_file).await;
}

/// Spark-written DV live set at the adopted snapshot (V3E-3 / C-006 ground truth).
#[tokio::test]
async fn adopted_partitioned_dv_then_append_has_two_snapshots() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    adopt_and_append(&ctx, &catalogs, &fixture.metadata_file).await;

    let s_dv = snapshot_id(&catalogs).await;
    assert_eq!(
        live_triples(
            &ctx,
            &catalogs,
            "SELECT id, name, part FROM ice.sales.partdv ORDER BY id"
        )
        .await,
        DV_LIVE
            .into_iter()
            .map(|(id, name, part)| (id, name.to_string(), part))
            .collect::<Vec<_>>(),
    );

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.partdv SELECT 7 AS id, 'g' AS name, 0 AS part",
    )
    .await;
    let s_append = snapshot_id(&catalogs).await;
    assert_ne!(s_dv, s_append, "append must produce a new snapshot (C-003)");
    let table = catalogs["ice"].load_table(&ident()).await.expect("load");
    assert!(
        table.metadata().snapshots().count() >= 2,
        "DV snapshot + append snapshot"
    );
    let mut after = DV_LIVE
        .into_iter()
        .map(|(id, name, part)| (id, name.to_string(), part))
        .collect::<Vec<_>>();
    after.push((7, "g".to_string(), 0));
    after.sort_by_key(|row| row.0);
    assert_eq!(
        live_triples(
            &ctx,
            &catalogs,
            "SELECT id, name, part FROM ice.sales.partdv ORDER BY id"
        )
        .await,
        after
    );
}

#[tokio::test]
async fn create_branch_and_tag_on_v3_do_not_move_main() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    adopt_and_append(&ctx, &catalogs, &fixture.metadata_file).await;
    let s_dv = snapshot_id(&catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.partdv SELECT 7 AS id, 'g' AS name, 0 AS part",
    )
    .await;
    let s_head = snapshot_id(&catalogs).await;

    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.partdv CREATE BRANCH audit",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.partdv CREATE TAG freeze",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.partdv CREATE BRANCH old AS OF VERSION {s_dv}"),
    )
    .await;

    let table = catalogs["ice"].load_table(&ident()).await.expect("load");
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

    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.partdv DROP BRANCH audit",
    )
    .await;
    let table = catalogs["ice"].load_table(&ident()).await.expect("load");
    assert!(table.metadata().snapshot_for_ref("audit").is_none());
    assert_eq!(table.metadata().current_snapshot_id(), Some(s_head));
}

#[tokio::test]
async fn version_as_of_snapshot_id_over_dvs_matches_spark_live_set() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    adopt_and_append(&ctx, &catalogs, &fixture.metadata_file).await;
    let s_dv = snapshot_id(&catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.partdv SELECT 7 AS id, 'g' AS name, 0 AS part",
    )
    .await;

    let at_dv = live_triples(
        &ctx,
        &catalogs,
        &format!("SELECT id, name, part FROM ice.sales.partdv VERSION AS OF {s_dv} ORDER BY id"),
    )
    .await;
    assert_eq!(
        at_dv,
        DV_LIVE
            .into_iter()
            .map(|(id, name, part)| (id, name.to_string(), part))
            .collect::<Vec<_>>(),
        "VERSION AS OF the DV snapshot is Spark's live set (C-006)"
    );
    let mut current = DV_LIVE
        .into_iter()
        .map(|(id, name, part)| (id, name.to_string(), part))
        .collect::<Vec<_>>();
    current.push((7, "g".to_string(), 0));
    current.sort_by_key(|row| row.0);
    assert_eq!(
        live_triples(
            &ctx,
            &catalogs,
            "SELECT id, name, part FROM ice.sales.partdv ORDER BY id"
        )
        .await,
        current,
        "current snapshot includes the RePark append"
    );
}

#[tokio::test]
async fn version_as_of_branch_name_matches_that_snapshot() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    adopt_and_append(&ctx, &catalogs, &fixture.metadata_file).await;
    let s_dv = snapshot_id(&catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.partdv SELECT 7 AS id, 'g' AS name, 0 AS part",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.partdv CREATE BRANCH old AS OF VERSION {s_dv}"),
    )
    .await;

    let at_branch = live_triples(
        &ctx,
        &catalogs,
        "SELECT id, name, part FROM ice.sales.partdv VERSION AS OF 'old' ORDER BY id",
    )
    .await;
    assert_eq!(
        at_branch,
        DV_LIVE
            .into_iter()
            .map(|(id, name, part)| (id, name.to_string(), part))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn rollback_to_dv_snapshot_restores_spark_live_set() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    adopt_and_append(&ctx, &catalogs, &fixture.metadata_file).await;
    let s_dv = snapshot_id(&catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.partdv SELECT 7 AS id, 'g' AS name, 0 AS part",
    )
    .await;

    run(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.rollback_to_snapshot(table => 'sales.partdv', snapshot_id => {s_dv})"
        ),
    )
    .await;
    assert_eq!(snapshot_id(&catalogs).await, s_dv);
    assert_eq!(
        live_triples(
            &ctx,
            &catalogs,
            "SELECT id, name, part FROM ice.sales.partdv ORDER BY id"
        )
        .await,
        DV_LIVE
            .into_iter()
            .map(|(id, name, part)| (id, name.to_string(), part))
            .collect::<Vec<_>>()
    );
}

fn assert_expire_schema_is_sparks(batch: &datafusion::arrow::array::RecordBatch) {
    let names: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        names,
        vec![
            "deleted_data_files_count",
            "deleted_position_delete_files_count",
            "deleted_equality_delete_files_count",
            "deleted_manifest_files_count",
            "deleted_manifest_lists_count",
            "deleted_statistics_files_count",
        ]
    );
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| field.is_nullable()),
        "Spark declares all six expire columns nullable"
    );
}

#[tokio::test]
async fn expire_snapshots_on_v3_keeps_tagged_dv_snapshot_and_drops_untagged_intermediate() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    adopt_and_append(&ctx, &catalogs, &fixture.metadata_file).await;
    let s_dv = snapshot_id(&catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.partdv SELECT 7 AS id, 'g' AS name, 0 AS part",
    )
    .await;
    let s_mid = snapshot_id(&catalogs).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.partdv SELECT 8 AS id, 'h' AS name, 1 AS part",
    )
    .await;
    let s_head = snapshot_id(&catalogs).await;

    run(
        &ctx,
        &catalogs,
        &format!("ALTER TABLE ice.sales.partdv CREATE TAG keep_dv AS OF VERSION {s_dv}"),
    )
    .await;

    let older_than_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
    let result = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.expire_snapshots(\
                 table => 'sales.partdv', older_than => {older_than_ms}, retain_last => 1)"
        ),
    )
    .await
    .expect("expire on v3");
    let batches = result.collect().await.expect("collect expire");
    assert_expire_schema_is_sparks(&batches[0]);

    let table = catalogs["ice"].load_table(&ident()).await.expect("load");
    assert!(
        table.metadata().snapshot_by_id(s_dv).is_some(),
        "tag-reachable DV snapshot must survive expire (C-010)"
    );
    assert!(
        table.metadata().snapshot_by_id(s_mid).is_none(),
        "untagged intermediate must expire — proves expire ran (C-009)"
    );
    assert!(
        table.metadata().snapshot_by_id(s_head).is_some(),
        "main head retained by retain_last=1"
    );
    let mut after_expire = DV_LIVE
        .into_iter()
        .map(|(id, name, part)| (id, name.to_string(), part))
        .collect::<Vec<_>>();
    after_expire.push((7, "g".to_string(), 0));
    after_expire.push((8, "h".to_string(), 1));
    after_expire.sort_by_key(|row| row.0);
    assert_eq!(
        live_triples(
            &ctx,
            &catalogs,
            "SELECT id, name, part FROM ice.sales.partdv ORDER BY id"
        )
        .await,
        after_expire,
        "expire must not change live rows at main (C-009)"
    );
    assert_eq!(
        live_triples(
            &ctx,
            &catalogs,
            &format!(
                "SELECT id, name, part FROM ice.sales.partdv VERSION AS OF {s_dv} ORDER BY id"
            )
        )
        .await,
        DV_LIVE
            .into_iter()
            .map(|(id, name, part)| (id, name.to_string(), part))
            .collect::<Vec<_>>(),
        "tag-reachable DV snapshot still readable after expire (C-010)"
    );
}

#[tokio::test]
async fn remove_orphan_files_on_v3_refuses_inside_twenty_four_hours() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    adopt_and_append(&ctx, &catalogs, &fixture.metadata_file).await;

    let table_dir = PathBuf::from(PART_DV_TABLE);
    let planted = table_dir.join("data").join("orphan-v3e4.parquet");
    fs::create_dir_all(planted.parent().expect("data dir")).expect("data");
    fs::write(&planted, b"not really parquet").expect("plant orphan");
    assert!(planted.is_file());

    let now_ms = chrono::Utc::now().timestamp_millis();
    let err = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.remove_orphan_files(\
                 table => 'sales.partdv', older_than => {now_ms}, dry_run => false)"
        ),
    )
    .await
    .expect_err("inside 24h must refuse");
    assert!(
        err.to_string().contains("less than 24 hours"),
        "floor message, got: {err}"
    );
    assert!(
        planted.is_file(),
        "refusal must not delete the planted orphan"
    );
}

#[tokio::test]
async fn update_on_the_appended_v3_table_commits() {
    let _: &str = "pins: rp-6-fork-repin/C-003";
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    adopt_and_append(&ctx, &catalogs, &fixture.metadata_file).await;

    let before_snapshot = snapshot_id(&catalogs).await;
    let before_rows = live_triples(
        &ctx,
        &catalogs,
        "SELECT id, name, part FROM ice.sales.partdv ORDER BY id",
    )
    .await;
    let before_files = files_with_bytes(Path::new(PART_DV_TABLE));

    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.partdv SET name = 'x' WHERE id = 1",
    )
    .await;
    assert_ne!(snapshot_id(&catalogs).await, before_snapshot);
    let after_update = live_triples(
        &ctx,
        &catalogs,
        "SELECT id, name, part FROM ice.sales.partdv ORDER BY id",
    )
    .await;
    assert_eq!(after_update[0], (1, "x".into(), 0));
    assert_eq!(&after_update[1..], &before_rows[1..]);
    let _ = before_files;
}
