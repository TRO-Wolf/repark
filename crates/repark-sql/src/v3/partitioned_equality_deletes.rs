//! Model: Grok 4.6 xHigh
//! ANSI-door pins for Spark-written partitioned v3 DV and equality-delete
//! pins: v3e-3-partitioned-eqdel-fixtures/C-007, C-008, C-010
//! pins: rp-3-fork-repin/C-007
//! pins: v3-4-serve-lineage-columns/C-003, C-005, C-007, C-008
//! pins: v3-4-serve-lineage-columns/C-011, C-012, C-013, C-014, C-015, C-016, C-018, C-020

use std::collections::HashSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::spec::{FormatVersion, Literal};
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

#[tokio::test]
async fn ansi_partitioned_v3_dv_delete_across_files_keeps_per_file_partitions() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    door.sql("DELETE FROM ice.sales.partdv WHERE id IN (1, 4)")
        .await
        .expect("DELETE one row from each partition");
    assert_eq!(
        door.live_triples("partdv").await,
        vec![(3, "c".to_string(), 0), (6, "f".to_string(), 1)]
    );
    let table = door
        .catalog
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

#[tokio::test]
async fn ansi_equality_delete_and_dv_keep_both_delete_classes_after_delete() {
    let fixture = materialize(
        &EQ_DV_LOCK,
        "v3-spark-eq-dv",
        EQ_DV_TABLE,
        "v4.metadata.json",
    );
    let door = door().await;
    adopt(&door, "eqdv", &fixture.metadata_file).await;
    door.sql("DELETE FROM ice.sales.eqdv WHERE id = 2")
        .await
        .expect("DELETE against the data file with the live DV");
    assert_eq!(
        door.live_triples("eqdv").await,
        vec![(3, "c".to_string(), 1)]
    );
    let batches = door
        .sql("SELECT content FROM ice.sales.eqdv$delete_files")
        .await
        .expect("read equality-delete plus DV metadata");
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
    contents.sort_unstable();
    assert_eq!(
        contents,
        vec![1, 2],
        "the DV and equality delete both stay live"
    );
}

#[tokio::test]
async fn ansi_partitioned_dv_update_refuses_before_writing() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "partdv".into());
    let before_snapshot = door
        .catalog
        .load_table(&ident)
        .await
        .expect("load before refused update")
        .metadata()
        .current_snapshot_id();
    let before_rows = door.live_triples("partdv").await;
    let before_files = files_with_bytes(Path::new(PART_DV_TABLE));
    let error = door
        .sql("UPDATE ice.sales.partdv SET name = 'x' WHERE id = 1")
        .await
        .expect_err("live-DV UPDATE must refuse before writing")
        .to_string();
    assert!(
        error.contains("V3-COW-1"),
        "refusal must name V3-COW-1: {error}"
    );
    assert_eq!(
        door.catalog
            .load_table(&ident)
            .await
            .expect("load after refused update")
            .metadata()
            .current_snapshot_id(),
        before_snapshot
    );
    assert_eq!(door.live_triples("partdv").await, before_rows);
    assert_eq!(files_with_bytes(Path::new(PART_DV_TABLE)), before_files);
}

#[tokio::test]
async fn ansi_partitioned_dv_rewrite_position_delete_files_call_is_spark_only() {
    let door = door().await;
    let error = door
        .sql("CALL ice.system.rewrite_position_delete_files(table => 'sales.partdv')")
        .await
        .expect_err("ANSI CALL is Q7")
        .to_string();
    assert!(
        error.contains("CALLABLE OPERATION") || error.contains("not supported"),
        "ANSI must refuse CALL, not run B-MOR-3: {error}"
    );
}

#[tokio::test]
async fn ansi_partitioned_dv_fork_rewrite_position_delete_files_is_a_conversion_noop() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let before = door.live_triples("partdv").await;
    let before_sum: i32 = before.iter().map(|row| row.0).sum();
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "partdv".into());
    let table = door
        .catalog
        .load_table(&ident)
        .await
        .expect("load adopted partitioned DV");
    let first = iceberg::maintenance::RewritePositionDeleteFiles::new(table)
        .execute(door.catalog.as_ref())
        .await
        .expect("fork v3 arm must run");
    repark_iceberg::catalog::invalidate_catalog_namespaces(
        &door.ctx,
        std::sync::Arc::clone(&door.catalog),
        "ice",
        &["sales"],
    )
    .await
    .expect("invalidate after first rewrite");
    let after = door.live_triples("partdv").await;
    assert_eq!(after, before, "read identity");
    assert_eq!(
        after.iter().map(|row| row.0).sum::<i32>(),
        before_sum,
        "sum(id)"
    );
    assert_eq!(first.rewritten_delete_files_count, 0);
    assert_eq!(first.added_delete_files_count, 0);
    let table = door.catalog.load_table(&ident).await.expect("reload");
    let second = iceberg::maintenance::RewritePositionDeleteFiles::new(table)
        .execute(door.catalog.as_ref())
        .await
        .expect("second rewrite");
    assert_eq!(second.rewritten_delete_files_count, 0);
    assert_eq!(door.live_triples("partdv").await, after);
}

async fn lineage_triples(door: &Door, table: &str) -> Vec<(i32, Option<i64>, Option<i64>)> {
    let batches = door
        .sql(&format!(
            "SELECT id, _row_id, _last_updated_sequence_number FROM ice.sales.{table} ORDER BY id"
        ))
        .await
        .unwrap_or_else(|err| panic!("lineage select: {err}"));
    let schema = batches[0].schema();
    assert_eq!(schema.field(1).name(), "_row_id");
    assert!(schema.field(1).is_nullable());
    assert_eq!(schema.field(2).name(), "_last_updated_sequence_number");
    assert!(schema.field(2).is_nullable());
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id Int32");
        let row_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_row_id Int64");
        let seqs = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("seq Int64");
        for index in 0..batch.num_rows() {
            rows.push((
                ids.value(index),
                (!row_ids.is_null(index)).then(|| row_ids.value(index)),
                (!seqs.is_null(index)).then(|| seqs.value(index)),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn ansi_partitioned_v3_dv_serves_spark_equal_lineage() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    assert_eq!(
        lineage_triples(&door, "partdv").await,
        vec![
            (1, Some(0), Some(1)),
            (3, Some(2), Some(1)),
            (4, Some(3), Some(1)),
            (6, Some(5), Some(1)),
        ]
    );
}

#[tokio::test]
async fn ansi_equality_delete_v3_serves_spark_equal_lineage() {
    let fixture = materialize(
        &EQ_DV_LOCK,
        "v3-spark-eq-dv",
        EQ_DV_TABLE,
        "v4.metadata.json",
    );
    let door = door().await;
    adopt(&door, "eqdv", &fixture.metadata_file).await;
    assert_eq!(
        lineage_triples(&door, "eqdv").await,
        vec![(2, Some(1), Some(1)), (3, Some(2), Some(1))]
    );
}

#[tokio::test]
async fn ansi_partitioned_v3_select_star_hides_lineage_columns() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let batches = door
        .sql("SELECT * FROM ice.sales.partdv ORDER BY id")
        .await
        .expect("select *");
    let names: Vec<_> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, vec!["id", "name", "part"]);
}

#[tokio::test]
async fn ansi_v2_table_lineage_columns_are_unresolved() {
    let door = door().await;
    door.sql("CREATE TABLE ice.sales.lin2 (id INT, name VARCHAR)")
        .await
        .expect("create v2");
    door.sql("INSERT INTO ice.sales.lin2 VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .expect("insert v2");
    let error = door
        .sql("SELECT id, _row_id FROM ice.sales.lin2")
        .await
        .expect_err("v2 must not plan lineage columns")
        .to_string();
    assert!(
        error.contains("No field named") && error.contains("_row_id"),
        "pre-v3 must fail as the engine Schema class, got: {error}"
    );
}

fn assert_v3_rowid2(message: &str, kind: &str) {
    assert!(
        message.contains("[V3-ROWID-2]")
            && message.contains(kind)
            && message.contains("single-table reads are"),
        "expected V3-ROWID-2 over {kind}, got: {message}"
    );
}

async fn row_id_values(door: &Door, sql: &str) -> Vec<i64> {
    let batches = door
        .sql(sql)
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"));
    let mut values = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_row_id Int64");
        for index in 0..batch.num_rows() {
            assert!(!column.is_null(index));
            values.push(column.value(index));
        }
    }
    values
}

#[tokio::test]
async fn ansi_join_naming_lineage_refuses_v3_rowid2() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let error = door
        .sql(
            "SELECT * FROM ice.sales.partdv a JOIN ice.sales.partdv b ON a.id = b.id \
             WHERE a._row_id IS NOT NULL",
        )
        .await
        .expect_err("join plus lineage must refuse");
    assert_v3_rowid2(&error.to_string(), "joins");
}

#[tokio::test]
async fn ansi_qualified_and_aliased_single_table_lineage_selects() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let expected = vec![0, 2, 3, 5];
    assert_eq!(
        row_id_values(
            &door,
            "SELECT t._row_id FROM ice.sales.partdv t ORDER BY t._row_id"
        )
        .await,
        expected
    );
    assert_eq!(
        row_id_values(
            &door,
            "SELECT partdv._row_id FROM ice.sales.partdv ORDER BY partdv._row_id"
        )
        .await,
        expected
    );
    assert_eq!(
        row_id_values(
            &door,
            "SELECT ice.sales.partdv._row_id FROM ice.sales.partdv ORDER BY 1"
        )
        .await,
        expected
    );
}

#[tokio::test]
async fn ansi_cte_and_subquery_naming_lineage_refuse_v3_rowid2() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let cte = door
        .sql("WITH x AS (SELECT _row_id FROM ice.sales.partdv) SELECT * FROM x")
        .await
        .expect_err("CTE plus lineage must refuse");
    assert_v3_rowid2(&cte.to_string(), "CTEs");
    let subquery = door
        .sql("SELECT _row_id FROM (SELECT _row_id FROM ice.sales.partdv) s")
        .await
        .expect_err("subquery plus lineage must refuse");
    assert_v3_rowid2(&subquery.to_string(), "subqueries");
}

#[tokio::test]
async fn ansi_version_as_of_naming_lineage_refuses_v3_rowid2() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "partdv".into());
    let snapshot = door
        .catalog
        .load_table(&ident)
        .await
        .expect("load")
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .snapshot_id();
    let error = door
        .sql(&format!(
            "SELECT _row_id FROM ice.sales.partdv FOR VERSION AS OF {snapshot}"
        ))
        .await
        .expect_err("time-travel plus lineage must refuse");
    assert_v3_rowid2(&error.to_string(), "time-travel");
}

#[tokio::test]
async fn ansi_unquoted_row_id_folds_quoted_mixed_case_stays_exact() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    assert_eq!(
        row_id_values(&door, "SELECT _ROW_ID FROM ice.sales.partdv ORDER BY 1").await,
        vec![0, 2, 3, 5]
    );
    let quoted = door
        .sql(r#"SELECT "_Row_Id" FROM ice.sales.partdv"#)
        .await
        .expect_err("quoted mixed-case must stay exact");
    let message = quoted.to_string();
    assert!(
        message.contains("_Row_Id") || message.contains("No field named"),
        "quoted mixed-case must not fold, got: {message}"
    );
}

#[tokio::test]
async fn ansi_select_star_plus_row_id_expands_user_columns_only() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    let batches = door
        .sql("SELECT *, _row_id FROM ice.sales.partdv ORDER BY id")
        .await
        .expect("select *, _row_id");
    let names: Vec<_> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, vec!["id", "name", "part", "_row_id"]);
}

#[tokio::test]
async fn ansi_filtered_lineage_select_returns_matching_rows() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let door = door().await;
    adopt(&door, "partdv", &fixture.metadata_file).await;
    assert_eq!(
        row_id_values(&door, "SELECT _row_id FROM ice.sales.partdv WHERE id = 1").await,
        vec![0]
    );
}
