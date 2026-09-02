//! pins: v3-4-serve-lineage-columns/C-001, C-002, C-005, C-006, C-007, C-008, C-009, C-010
//! pins: v3-4-serve-lineage-columns/C-011, C-012, C-013, C-014, C-015, C-016, C-018, C-020
//! Spark-door lineage metadata columns on v3 reads.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use super::super::*;
use super::common::*;

use datafusion::arrow::array::{Int32Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use iceberg::spec::FormatVersion;
use iceberg::{NamespaceIdent, TableCreation, TableIdent};

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
                        "fixture lock {}: held for 2 minutes",
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
    let held = lock.lock().expect("v3 lineage fixture lock");
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

fn lineage_triples(batches: &[RecordBatch]) -> Vec<(i32, Option<i64>, Option<i64>)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("id Int32, got {:?}", batch.schema()));
        let row_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("_row_id Int64, got {:?}", batch.schema()));
        let seqs = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| {
                panic!(
                    "_last_updated_sequence_number Int64, got {:?}",
                    batch.schema()
                )
            });
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

async fn select_lineage(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(i32, Option<i64>, Option<i64>)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id, _row_id, _last_updated_sequence_number FROM {table} ORDER BY id"),
    )
    .await
    .unwrap_or_else(|err| panic!("lineage select: {err}"))
    .collect()
    .await
    .unwrap_or_else(|err| panic!("lineage collect: {err}"));
    let schema = batches[0].schema();
    assert_eq!(schema.field(1).name(), "_row_id");
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    assert!(schema.field(1).is_nullable());
    assert_eq!(schema.field(2).name(), "_last_updated_sequence_number");
    assert_eq!(schema.field(2).data_type(), &DataType::Int64);
    assert!(schema.field(2).is_nullable());
    lineage_triples(&batches)
}

fn find_ledger(dir: &Path, suffix: &str) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_ledger(&path, suffix) {
                return Some(found);
            }
        } else if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with(suffix))
        {
            return Some(path);
        }
    }
    None
}

#[tokio::test]
async fn v3_lineage_oracle_matrix_is_the_c001_record() {
    let ledgers = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("repo")
        .join("task/ledgers");
    let ledger = find_ledger(&ledgers, "v3-4-serve-lineage-columns-ledger.md")
        .expect("the V3-4 ledger lives somewhere under task/ledgers/");
    let text = fs::read_to_string(&ledger).expect("C-001 ledger");
    assert!(
        text.contains("UNRESOLVED_COLUMN.WITH_SUGGESTION")
            && text.contains("(1,0,1),(3,2,1),(4,3,1),(6,5,1)")
            && text.contains("(2,1,1),(3,2,1)"),
        "C-001 matrix must stay in the ledger"
    );
}

#[tokio::test]
async fn partitioned_v3_dv_select_star_hides_lineage_columns() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.partdv", &fixture.metadata_file).await;
    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.partdv ORDER BY id",
    )
    .await
    .expect("select *")
    .collect()
    .await
    .expect("collect *");
    let names: Vec<_> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, vec!["id", "name", "part"]);
}

#[tokio::test]
async fn partitioned_v3_dv_serves_spark_equal_lineage_for_surviving_rows() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.partdv", &fixture.metadata_file).await;
    assert_eq!(
        select_lineage(&ctx, &catalogs, "ice.sales.partdv").await,
        vec![
            (1, Some(0), Some(1)),
            (3, Some(2), Some(1)),
            (4, Some(3), Some(1)),
            (6, Some(5), Some(1)),
        ]
    );
}

#[tokio::test]
async fn equality_delete_v3_serves_spark_equal_lineage_for_surviving_rows() {
    let fixture = materialize(
        &EQ_DV_LOCK,
        "v3-spark-eq-dv",
        EQ_DV_TABLE,
        "v4.metadata.json",
    );
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.eqdv", &fixture.metadata_file).await;
    assert_eq!(
        select_lineage(&ctx, &catalogs, "ice.sales.eqdv").await,
        vec![(2, Some(1), Some(1)), (3, Some(2), Some(1))]
    );
}

#[tokio::test]
async fn created_v3_table_serves_derived_row_ids() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.lin3 (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.lin3 SELECT * FROM src",
    )
    .await;
    let rows = select_lineage(&ctx, &catalogs, "ice.sales.lin3").await;
    assert_eq!(
        rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let ids: Vec<i64> = rows.iter().filter_map(|(_, row_id, _)| *row_id).collect();
    assert_eq!(ids, vec![0, 1, 2]);
    assert!(rows.iter().all(|(_, _, seq)| *seq == Some(1)));
}

#[tokio::test]
async fn v2_table_lineage_columns_are_unresolved() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.lin2 (id INT, name STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.lin2 SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::from_strs(["sales", "lin2"]).expect("ident");
    let table = catalogs
        .get("ice")
        .expect("ice")
        .load_table(&ident)
        .await
        .expect("load");
    assert_eq!(table.metadata().format_version(), FormatVersion::V2);
    let err = execute(
        &ctx,
        &catalogs,
        "SELECT id, _row_id, _last_updated_sequence_number FROM ice.sales.lin2",
    )
    .await
    .expect_err("v2 must not plan lineage columns");
    let message = err.to_string();
    assert!(
        message.contains("No field named") && message.contains("_row_id"),
        "pre-v3 must fail as the engine Schema class, got: {message}"
    );
}

#[tokio::test]
async fn v1_table_lineage_columns_are_unresolved() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let catalog = catalogs.get("ice").expect("ice").clone();
    let schema = iceberg::spec::Schema::builder()
        .with_fields(vec![
            iceberg::spec::NestedField::optional(
                1,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
            )
            .into(),
            iceberg::spec::NestedField::optional(
                2,
                "name",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
            .into(),
        ])
        .build()
        .expect("v1 schema");
    catalog
        .create_table(
            &NamespaceIdent::new("sales".into()),
            TableCreation::builder()
                .name("lin1".to_string())
                .schema(schema)
                .format_version(FormatVersion::V1)
                .build(),
        )
        .await
        .expect("create v1");
    repark_iceberg::catalog::invalidate_catalog_namespaces(&ctx, catalog, "ice", &["sales"])
        .await
        .expect("refresh");
    let err = execute(&ctx, &catalogs, "SELECT id, _row_id FROM ice.sales.lin1")
        .await
        .expect_err("v1 must not plan lineage columns");
    let message = err.to_string();
    assert!(
        message.contains("No field named") && message.contains("_row_id"),
        "v1 must fail as the engine Schema class, got: {message}"
    );
}

#[test]
fn v3_rowid_1_is_fixed_in_the_registry() {
    let registry = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("repo")
        .join("docs/spark-sql-iceberg-parity.md");
    let text = fs::read_to_string(&registry).expect("registry");
    assert!(
        text.contains("**V3-ROWID-1** — **FIXED (V3-4, 2026-08-31).**"),
        "V3-ROWID-1 must close as FIXED"
    );
    assert!(
        text.contains("**V3-ROWID-2** — **DECLARED (V3-4, 2026-08-31).**")
            && text.contains("No field named _row_id"),
        "V3-ROWID-2 must document the composed refuse; v1/v2 class is the engine Schema error"
    );
}

#[test]
fn cow_keep_refusal_files_are_byte_untouched() {
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-005";
    let _: &str = "pins: v3-11-row-id-determinism/C-004";
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("repo")
        .to_path_buf();
    let pinned: [(&str, u64); 4] = [
        (
            "crates/repark-spark/src/tests/v3_subquery_dml.rs",
            0x4e89_f691_b357_c970,
        ),
        (
            "crates/repark-spark/src/tests/v3_cow.rs",
            0x9339_d979_508a_32b0,
        ),
        ("crates/repark-sql/src/v3/cow.rs", 0xf0a7_9f70_6d31_c7ad),
        (
            "python/repark/tests/test_v3_cow_dml.py",
            0xce7c_19f9_d69d_4f7c,
        ),
    ];
    for (path, expected) in pinned {
        let bytes = std::fs::read(repo.join(path)).expect(path);
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        assert_eq!(
            hash, expected,
            "V3-COW-1 lift file {path} changed; V3-11 \
             (feat/v3-11-row-id-determinism) re-records the `v3/cow.rs` hash for the two ANSI \
             same-commit file-order twins it adds there; later units re-record only for a \
             change they themselves made"
        );
    }
}

fn assert_v3_rowid2(message: &str, kind: &str) {
    assert!(
        message.contains("[V3-ROWID-2]")
            && message.contains(kind)
            && message.contains("single-table reads are"),
        "expected V3-ROWID-2 over {kind}, got: {message}"
    );
}

async fn row_id_values(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<i64> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
        .collect()
        .await
        .unwrap_or_else(|err| panic!("{sql} collect: {err}"));
    let mut values = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("_row_id Int64, got {:?}", batch.schema()));
        for index in 0..batch.num_rows() {
            assert!(!column.is_null(index), "derived _row_id is present");
            values.push(column.value(index));
        }
    }
    values
}

#[tokio::test]
async fn join_naming_lineage_refuses_v3_rowid2() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.lin3 (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.lin3 SELECT * FROM src",
    )
    .await;
    let err = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.lin3 a JOIN ice.sales.lin3 b ON a.id = b.id \
         WHERE a._row_id IS NOT NULL",
    )
    .await
    .expect_err("join plus lineage must refuse, not emit HashMap-ordered columns");
    assert_v3_rowid2(&err.to_string(), "joins");
}

#[tokio::test]
async fn qualified_and_aliased_single_table_lineage_selects() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.lin3 (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.lin3 SELECT * FROM src",
    )
    .await;
    assert_eq!(
        row_id_values(
            &ctx,
            &catalogs,
            "SELECT t._row_id FROM ice.sales.lin3 t ORDER BY t._row_id"
        )
        .await,
        vec![0, 1, 2]
    );
    assert_eq!(
        row_id_values(
            &ctx,
            &catalogs,
            "SELECT lin3._row_id FROM ice.sales.lin3 ORDER BY lin3._row_id"
        )
        .await,
        vec![0, 1, 2]
    );
    assert_eq!(
        row_id_values(
            &ctx,
            &catalogs,
            "SELECT ice.sales.lin3._row_id FROM ice.sales.lin3 ORDER BY 1"
        )
        .await,
        vec![0, 1, 2]
    );
}

#[tokio::test]
async fn cte_and_subquery_naming_lineage_refuse_v3_rowid2() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.lin3 (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.lin3 SELECT * FROM src",
    )
    .await;
    let cte = execute(
        &ctx,
        &catalogs,
        "WITH x AS (SELECT _row_id FROM ice.sales.lin3) SELECT * FROM x",
    )
    .await
    .expect_err("CTE plus lineage must refuse");
    assert_v3_rowid2(&cte.to_string(), "CTEs");
    let subquery = execute(
        &ctx,
        &catalogs,
        "SELECT _row_id FROM (SELECT _row_id FROM ice.sales.lin3) s",
    )
    .await
    .expect_err("subquery plus lineage must refuse");
    assert_v3_rowid2(&subquery.to_string(), "subqueries");
}

#[tokio::test]
async fn version_as_of_naming_lineage_refuses_v3_rowid2() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.lin3 (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.lin3 SELECT * FROM src",
    )
    .await;
    let snapshot = load_sales_table(&catalogs, "lin3")
        .await
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .snapshot_id();
    let err = execute(
        &ctx,
        &catalogs,
        &format!("SELECT _row_id FROM ice.sales.lin3 VERSION AS OF {snapshot}"),
    )
    .await
    .expect_err("time-travel plus lineage must refuse");
    assert_v3_rowid2(&err.to_string(), "time-travel");
}

#[tokio::test]
async fn unquoted_row_id_folds_quoted_mixed_case_stays_exact() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.lin3 (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.lin3 SELECT * FROM src",
    )
    .await;
    assert_eq!(
        row_id_values(
            &ctx,
            &catalogs,
            "SELECT _ROW_ID FROM ice.sales.lin3 ORDER BY 1"
        )
        .await,
        vec![0, 1, 2]
    );
    let quoted = execute(&ctx, &catalogs, "SELECT `_Row_Id` FROM ice.sales.lin3")
        .await
        .expect_err("quoted mixed-case must stay exact");
    let message = quoted.to_string();
    assert!(
        message.contains("_Row_Id") || message.contains("No field named"),
        "quoted mixed-case must not fold, got: {message}"
    );
}

#[tokio::test]
async fn select_star_plus_row_id_expands_user_columns_only() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.partdv", &fixture.metadata_file).await;
    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT *, _row_id FROM ice.sales.partdv ORDER BY id",
    )
    .await
    .expect("select *, _row_id")
    .collect()
    .await
    .expect("collect");
    let names: Vec<_> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        names,
        vec!["id", "name", "part", "_row_id"],
        "star must expand to user columns only; leaking lineage into * must red"
    );
    assert!(!names.contains(&"_last_updated_sequence_number".to_string()));
}

#[tokio::test]
async fn filtered_lineage_select_returns_matching_rows() {
    let fixture = materialize(
        &PART_DV_LOCK,
        "v3-spark-part-dv",
        PART_DV_TABLE,
        "v3.metadata.json",
    );
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    register_adopted(&ctx, &catalogs, "sales.partdv", &fixture.metadata_file).await;
    assert_eq!(
        row_id_values(
            &ctx,
            &catalogs,
            "SELECT _row_id FROM ice.sales.partdv WHERE id = 1"
        )
        .await,
        vec![0]
    );
    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT id FROM ice.sales.partdv WHERE id = 1",
    )
    .await
    .expect("filtered id")
    .collect()
    .await
    .expect("collect");
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id");
    assert_eq!(ids.values(), &[1]);
}
