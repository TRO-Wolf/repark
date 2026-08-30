//! Model: Grok 4.6 xHigh
//! pins: v3e-3-partitioned-eqdel-fixtures/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-010, C-011, C-012, C-013
//! pins: rp-3-fork-repin/C-004, C-007, C-011
//! Pins partitioned and equality-delete Spark-written format-v3 fixtures.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use super::super::*;
use super::common::*;

use datafusion::arrow::array::{Int32Array, Int64Array, ListArray, StringArray};
use iceberg::spec::Literal;

/// Table location baked into the partitioned-DV fixture.
const PART_DV_TABLE: &str = "/tmp/repark-v3e3-partdv/ns/v3part";

/// Table location baked into the equality-delete + DV fixture.
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
    let held = lock.lock().expect("spark v3e3 fixture lock");
    let cross_process = DirLock::acquire(&format!("{dest}.lock"));
    let dest_path = PathBuf::from(dest);
    if dest_path.exists() {
        fs::remove_dir_all(&dest_path).expect("clear previous fixture");
    }
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/tests/fixtures")
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

fn materialize_part_dv() -> SparkFixture {
    materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    )
}

fn materialize_eq_dv() -> SparkFixture {
    materialize(
        &EQ_DV_LOCK,
        "v3-spark-eq-dv",
        EQ_DV_TABLE,
        "v4.metadata.json",
    )
}

fn i64_cell(batch: &datafusion::arrow::array::RecordBatch, column: usize) -> Option<i64> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("register_table result column is Int64")
        .iter()
        .next()
        .flatten()
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

async fn live_triples(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(i32, String, i32)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id, name, part FROM {table} ORDER BY id"),
    )
    .await
    .expect("select live rows")
    .collect()
    .await
    .expect("collect live rows");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("id must be Int32, got {:?}", batch.schema()));
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("name must be Utf8, got {:?}", batch.schema()));
        let parts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("part must be Int32, got {:?}", batch.schema()));
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

async fn live_pairs_pruned(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    part: i32,
) -> Vec<(i32, String)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id, name FROM {table} WHERE part = {part} ORDER BY id"),
    )
    .await
    .expect("partition prune select")
    .collect()
    .await
    .expect("collect prune");
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
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), names.value(index).to_string()));
        }
    }
    rows
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeleteFileRow {
    content: i32,
    file_format: String,
    record_count: i64,
    equality_ids: Option<Vec<i32>>,
}

fn i32_or_i64(array: &dyn datafusion::arrow::array::Array, index: usize) -> i64 {
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        i64::from(values.value(index))
    } else if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        values.value(index)
    } else {
        panic!("expected Int32 or Int64, got {array:?}")
    }
}

fn equality_ids_at(array: &dyn datafusion::arrow::array::Array, index: usize) -> Option<Vec<i32>> {
    if array.is_null(index) {
        return None;
    }
    let list = array
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap_or_else(|| panic!("equality_ids must be List, got {array:?}"));
    let values = list.value(index);
    let mut ids = Vec::with_capacity(values.len());
    for inner in 0..values.len() {
        ids.push(i32::try_from(i32_or_i64(values.as_ref(), inner)).expect("equality field id"));
    }
    Some(ids)
}

async fn delete_file_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<DeleteFileRow> {
    let batches = execute(
        ctx,
        catalogs,
        &format!(
            "SELECT content, file_format, record_count, equality_ids \
             FROM {table}.delete_files"
        ),
    )
    .await
    .expect("select delete_files")
    .collect()
    .await
    .expect("collect delete_files");
    let mut rows = Vec::new();
    for batch in &batches {
        let content = batch.column(0);
        let formats = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("file_format Utf8, got {:?}", batch.schema()));
        let counts = batch.column(2);
        let equality = batch.column(3);
        for index in 0..batch.num_rows() {
            rows.push(DeleteFileRow {
                content: i32::try_from(i32_or_i64(content.as_ref(), index)).expect("content"),
                file_format: formats.value(index).to_string(),
                record_count: i32_or_i64(counts.as_ref(), index),
                equality_ids: equality_ids_at(equality.as_ref(), index),
            });
        }
    }
    rows.sort_by_key(|row| (row.content, row.file_format.clone(), row.record_count));
    rows
}

// Partitioned DV fixture

#[tokio::test]
async fn partitioned_v3_dv_fixture_adopts_and_matches_spark_live_rows() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;

    let batches = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.partdv', \
             metadata_file => '{}')",
            fixture.metadata_file
        ),
    )
    .await
    .expect("register partitioned DV fixture")
    .collect()
    .await
    .expect("collect");
    let batch = &batches[0];
    assert_eq!(i64_cell(batch, 0), Some(8_850_248_918_634_954_095));
    assert_eq!(i64_cell(batch, 1), Some(6), "Spark summary total-records");
    assert_eq!(
        i64_cell(batch, 2),
        Some(2),
        "Spark summary total-data-files"
    );

    assert_eq!(
        live_triples(&ctx, &catalogs, "ice.sales.partdv").await,
        vec![
            (1, "a".to_string(), 0),
            (3, "c".to_string(), 0),
            (4, "d".to_string(), 1),
            (6, "f".to_string(), 1),
        ],
        "partitioned Puffin vectors must apply; Spark live set"
    );
}

#[tokio::test]
async fn partitioned_v3_dv_partition_predicate_matches_spark() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.partdv", &fixture.metadata_file).await;

    assert_eq!(
        live_pairs_pruned(&ctx, &catalogs, "ice.sales.partdv", 0).await,
        vec![(1, "a".to_string()), (3, "c".to_string())]
    );
    assert_eq!(
        live_pairs_pruned(&ctx, &catalogs, "ice.sales.partdv", 1).await,
        vec![(4, "d".to_string()), (6, "f".to_string())]
    );
}

#[tokio::test]
async fn partitioned_v3_dv_delete_files_are_puffin_content_one() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.partdv", &fixture.metadata_file).await;

    let deletes = delete_file_rows(&ctx, &catalogs, "ice.sales.partdv").await;
    assert!(
        !deletes.is_empty(),
        "partitioned DV fixture must expose .delete_files rows"
    );
    assert!(
        deletes
            .iter()
            .all(|row| row.content == 1 && row.file_format.eq_ignore_ascii_case("PUFFIN")),
        "every delete_files row is a Puffin vector (content=1): {deletes:?}"
    );
    assert!(
        deletes.iter().all(|row| row.equality_ids.is_none()),
        "DV rows carry null equality_ids: {deletes:?}"
    );
    let records: i64 = deletes.iter().map(|row| row.record_count).sum();
    assert_eq!(records, 2, "Spark added-position-deletes / added-dvs = 2");
}

#[tokio::test]
async fn partitioned_v3_dv_rewrite_position_delete_files_still_refuses() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.partdv", &fixture.metadata_file).await;

    let err = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.partdv')",
    )
    .await
    .expect_err("B-MOR-3: live Puffin vectors must refuse")
    .to_string();
    assert!(
        err.contains("live Puffin deletion vector"),
        "refusal must name Puffin vectors: {err}"
    );
}

#[tokio::test]
async fn partitioned_v3_dv_fork_rewrite_position_delete_files_measurement() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.partdv", &fixture.metadata_file).await;
    let before_rows = live_triples(&ctx, &catalogs, "ice.sales.partdv").await;
    let before_sum: i32 = before_rows.iter().map(|row| row.0).sum();
    let before_deletes = delete_file_rows(&ctx, &catalogs, "ice.sales.partdv").await;
    let ident = TableIdent::from_strs(["sales", "partdv"]).expect("ident");
    let catalog = Arc::clone(catalogs.get("ice").expect("ice catalog"));
    let table = catalog
        .load_table(&ident)
        .await
        .expect("load adopted partitioned DV table");
    let first_result = iceberg::maintenance::RewritePositionDeleteFiles::new(table)
        .execute(catalog.as_ref())
        .await
        .expect("fork v3 RewritePositionDeleteFiles must run");
    reregister(&ctx, Arc::clone(&catalog), "ice", "sales")
        .await
        .expect("invalidate after first rewrite");
    let after_first = live_triples(&ctx, &catalogs, "ice.sales.partdv").await;
    let after_first_sum: i32 = after_first.iter().map(|row| row.0).sum();
    let after_first_deletes = delete_file_rows(&ctx, &catalogs, "ice.sales.partdv").await;
    assert_eq!(
        after_first, before_rows,
        "first rewrite must keep live rows"
    );
    assert_eq!(
        after_first_sum, before_sum,
        "first rewrite must keep sum(id)"
    );
    let table = catalog
        .load_table(&ident)
        .await
        .expect("reload after first rewrite");
    let second = iceberg::maintenance::RewritePositionDeleteFiles::new(table)
        .execute(catalog.as_ref())
        .await
        .expect("second rewrite");
    reregister(&ctx, Arc::clone(&catalog), "ice", "sales")
        .await
        .expect("invalidate after second rewrite");
    let after_second = live_triples(&ctx, &catalogs, "ice.sales.partdv").await;
    assert_eq!(
        after_second, after_first,
        "second rewrite must converge on the same live rows"
    );
    let summary = format!(
        "C-007 first rewritten={} added={} bytes_in={} bytes_out={} deletes {} -> {}; second rewritten={} added={}",
        first_result.rewritten_delete_files_count,
        first_result.added_delete_files_count,
        first_result.rewritten_bytes_count,
        first_result.added_bytes_count,
        before_deletes.len(),
        after_first_deletes.len(),
        second.rewritten_delete_files_count,
        second.added_delete_files_count,
    );
    assert_eq!(first_result.rewritten_delete_files_count, 0, "{summary}");
    assert_eq!(first_result.added_delete_files_count, 0, "{summary}");
    assert_eq!(second.rewritten_delete_files_count, 0, "{summary}");
    assert_eq!(after_first_deletes.len(), before_deletes.len(), "{summary}");
}

#[tokio::test]
async fn partitioned_v3_dv_delete_id_3_merges_into_the_touched_file() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.partdv", &fixture.metadata_file).await;
    execute(&ctx, &catalogs, "DELETE FROM ice.sales.partdv WHERE id = 3")
        .await
        .expect("DELETE id = 3")
        .collect()
        .await
        .expect("collect DELETE id = 3");
    assert_eq!(
        live_triples(&ctx, &catalogs, "ice.sales.partdv").await,
        vec![
            (1, "a".to_string(), 0),
            (4, "d".to_string(), 1),
            (6, "f".to_string(), 1),
        ]
    );
    let deletes = delete_file_rows(&ctx, &catalogs, "ice.sales.partdv").await;
    assert_eq!(deletes.len(), 2, "still one DV per data file");
    let records: i64 = deletes.iter().map(|row| row.record_count).sum();
    assert_eq!(records, 3, "part=0 gained the new position");
}

#[tokio::test]
async fn partitioned_v3_dv_delete_id_1_keeps_the_untouched_sibling() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.partdv", &fixture.metadata_file).await;
    execute(&ctx, &catalogs, "DELETE FROM ice.sales.partdv WHERE id = 1")
        .await
        .expect("DELETE id = 1")
        .collect()
        .await
        .expect("collect DELETE id = 1");
    assert_eq!(
        live_triples(&ctx, &catalogs, "ice.sales.partdv").await,
        vec![
            (3, "c".to_string(), 0),
            (4, "d".to_string(), 1),
            (6, "f".to_string(), 1),
        ],
        "id 5 stays deleted"
    );
    let deletes = delete_file_rows(&ctx, &catalogs, "ice.sales.partdv").await;
    assert_eq!(deletes.len(), 2, "untouched sibling stays live");
}

#[tokio::test]
async fn partitioned_v3_dv_delete_across_files_keeps_per_file_partitions() {
    let fixture = materialize_part_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.partdv", &fixture.metadata_file).await;
    execute(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.partdv WHERE id IN (1, 4)",
    )
    .await
    .expect("DELETE one row from each partition")
    .collect()
    .await
    .expect("collect partition-spanning DELETE");
    assert_eq!(
        live_triples(&ctx, &catalogs, "ice.sales.partdv").await,
        vec![(3, "c".to_string(), 0), (6, "f".to_string(), 1)]
    );
    let deletes = delete_file_rows(&ctx, &catalogs, "ice.sales.partdv").await;
    assert_eq!(deletes.len(), 2, "one live DV for each data file");
    assert!(
        deletes.iter().all(|file| {
            file.content == 1
                && file.file_format.eq_ignore_ascii_case("PUFFIN")
                && file.record_count == 2
        }),
        "each replacement DV carries two deleted positions: {deletes:?}"
    );
    let table = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".into()),
            "partdv".into(),
        ))
        .await
        .expect("load partitioned DV table");
    let vectors = iceberg::live_deletion_vectors_by_data_file(&table)
        .await
        .expect("discover live DVs by referenced data file");
    assert_eq!(vectors.len(), 2, "R114 maps one DV to each data file");
    let mut partitions: Vec<_> = vectors
        .values()
        .map(|file| {
            assert_eq!(file.partition_spec_id(), 0, "the fixture uses spec 0");
            file.partition().fields().to_vec()
        })
        .collect();
    partitions.sort_by_key(|partition| format!("{partition:?}"));
    assert_eq!(
        partitions,
        vec![vec![Some(Literal::int(0))], vec![Some(Literal::int(1))],]
    );
}

// Equality-delete + DV fixture

#[tokio::test]
async fn equality_delete_alongside_dv_adopts_and_matches_spark_live_rows() {
    let fixture = materialize_eq_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;

    let batches = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.eqdv', \
             metadata_file => '{}')",
            fixture.metadata_file
        ),
    )
    .await
    .expect("register eq+DV fixture")
    .collect()
    .await
    .expect("collect");
    let batch = &batches[0];
    assert_eq!(i64_cell(batch, 0), Some(5_751_120_093_798_556_354));
    assert_eq!(i64_cell(batch, 1), Some(4), "Spark summary total-records");
    assert_eq!(
        i64_cell(batch, 2),
        Some(2),
        "Spark summary total-data-files"
    );

    assert_eq!(
        live_triples(&ctx, &catalogs, "ice.sales.eqdv").await,
        vec![(2, "b".to_string(), 0), (3, "c".to_string(), 1)],
        "Puffin DV (id=1) and equality-delete (id=4) must both apply"
    );
}

#[tokio::test]
async fn equality_delete_alongside_dv_delete_files_name_both_kinds() {
    let fixture = materialize_eq_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.eqdv", &fixture.metadata_file).await;

    let deletes = delete_file_rows(&ctx, &catalogs, "ice.sales.eqdv").await;
    let puffin = deletes
        .iter()
        .find(|row| row.content == 1)
        .expect("Puffin DV row (content=1)");
    assert!(
        puffin.file_format.eq_ignore_ascii_case("PUFFIN"),
        "content=1 is PUFFIN: {puffin:?}"
    );
    assert_eq!(puffin.record_count, 1);
    assert!(puffin.equality_ids.is_none());

    let equality = deletes
        .iter()
        .find(|row| row.content == 2)
        .expect("equality-delete row (content=2)");
    assert!(
        equality.file_format.eq_ignore_ascii_case("PARQUET"),
        "content=2 is PARQUET: {equality:?}"
    );
    assert_eq!(equality.record_count, 1);
    assert_eq!(equality.equality_ids.as_deref(), Some(&[1][..]));
}

#[tokio::test]
async fn equality_delete_and_dv_keep_both_delete_classes_after_delete() {
    let fixture = materialize_eq_dv();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.eqdv", &fixture.metadata_file).await;
    execute(&ctx, &catalogs, "DELETE FROM ice.sales.eqdv WHERE id = 2")
        .await
        .expect("DELETE against the data file with the live DV")
        .collect()
        .await
        .expect("collect equality-delete plus DV DELETE");
    assert_eq!(
        live_triples(&ctx, &catalogs, "ice.sales.eqdv").await,
        vec![(3, "c".to_string(), 1)]
    );
    let deletes = delete_file_rows(&ctx, &catalogs, "ice.sales.eqdv").await;
    assert_eq!(
        deletes.len(),
        2,
        "the DV and equality delete both stay live"
    );
    let vector = deletes
        .iter()
        .find(|file| file.content == 1)
        .expect("Puffin DV remains live");
    assert!(vector.file_format.eq_ignore_ascii_case("PUFFIN"));
    assert_eq!(vector.record_count, 2);
    let equality = deletes
        .iter()
        .find(|file| file.content == 2)
        .expect("Parquet equality delete remains live");
    assert!(equality.file_format.eq_ignore_ascii_case("PARQUET"));
    assert_eq!(equality.record_count, 1);
    assert_eq!(equality.equality_ids.as_deref(), Some(&[1][..]));
}
