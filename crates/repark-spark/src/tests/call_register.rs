//! Pins `CALL system.register_table` and adoption of the Spark-written format-v3 fixture.
//! pins: rp-3-fork-repin/C-007, C-008

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use super::super::*;
use super::common::*;

use datafusion::arrow::array::Int64Array;

/// Warehouse prefix baked into the checked-in Spark fixture (same length as the original
/// `/tmp/mw2-v3-atl_i0w4/ns/v3` so Avro length-prefixed paths stay valid).
const SPARK_V3_WAREHOUSE: &str = "/tmp/repark-v3-1-spark-mor";

/// Serializes tests that materialize the Spark fixture at a fixed `/tmp` path.
static SPARK_V3_LOCK: Mutex<()> = Mutex::new(());

struct SparkV3Fixture {
    _lock: MutexGuard<'static, ()>,
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

/// Copy the checked-in Spark-written v3 table onto the warehouse path its metadata names.
///
/// The fixture's Avro/Puffin/JSON all point at [`SPARK_V3_WAREHOUSE`]. Tests hold the lock for
/// the whole case so parallel `cargo test` threads do not clobber each other. The path is under
/// `/tmp` because that is where Spark wrote it; rewriting every Avro block to a `TempDir` of a
/// different length would corrupt length-prefixed paths.
fn materialize_spark_v3_fixture() -> SparkV3Fixture {
    let lock = SPARK_V3_LOCK.lock().expect("spark v3 fixture lock");
    let dest = PathBuf::from(SPARK_V3_WAREHOUSE);
    if dest.exists() {
        fs::remove_dir_all(&dest).expect("clear previous fixture");
    }
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/fixtures/v3-spark-mor");
    copy_dir_all(&src, &dest);
    let metadata_file = dest.join("metadata/v8.metadata.json");
    assert!(
        metadata_file.is_file(),
        "Spark fixture must include Hadoop-named v8.metadata.json"
    );
    SparkV3Fixture {
        _lock: lock,
        metadata_file: metadata_file.to_string_lossy().into_owned(),
    }
}

fn register_schema_names(batch: &datafusion::arrow::array::RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
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

#[tokio::test]
async fn call_register_table_adopts_an_engine_written_table_and_returns_sparks_three_columns() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.src AS SELECT * FROM src",
    )
    .await;

    let ident = TableIdent::from_strs(["sales", "src"]).unwrap();
    let source = catalogs
        .get("ice")
        .unwrap()
        .load_table(&ident)
        .await
        .unwrap();
    let metadata_file = source
        .metadata_location()
        .expect("engine-created table has a metadata pointer")
        .to_string();
    let expected_snapshot = source.metadata().current_snapshot_id();
    let expected_records = source.metadata().current_snapshot().and_then(|snap| {
        snap.summary()
            .additional_properties
            .get("total-records")
            .and_then(|raw| raw.parse::<i64>().ok())
    });
    let expected_files = source.metadata().current_snapshot().and_then(|snap| {
        snap.summary()
            .additional_properties
            .get("total-data-files")
            .and_then(|raw| raw.parse::<i64>().ok())
    });

    let batches = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.adopted', \
             metadata_file => '{metadata_file}')"
        ),
    )
    .await
    .expect("register_table of an engine-written table")
    .collect()
    .await
    .expect("collect register_table");
    let batch = &batches[0];
    assert_eq!(
        register_schema_names(batch),
        [
            "current_snapshot_id",
            "total_records_count",
            "total_data_files_count"
        ]
    );
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| field.is_nullable()),
        "Spark declares all three register_table columns nullable"
    );
    assert_eq!(i64_cell(batch, 0), expected_snapshot);
    assert_eq!(i64_cell(batch, 1), expected_records);
    assert_eq!(i64_cell(batch, 2), expected_files);

    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.adopted").await,
        3,
        "adopted table must serve the same rows the source wrote"
    );
}

#[tokio::test]
async fn call_register_table_of_a_table_with_no_snapshot_returns_three_nulls() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.empty (id BIGINT, name STRING) USING iceberg",
    )
    .await;
    let ident = TableIdent::from_strs(["sales", "empty"]).unwrap();
    let empty = catalogs
        .get("ice")
        .unwrap()
        .load_table(&ident)
        .await
        .unwrap();
    assert!(
        empty.metadata().current_snapshot_id().is_none(),
        "schema-only CREATE is the no-snapshot fixture; if this stamps a snapshot the pin is testing the wrong thing"
    );
    let metadata_file = empty
        .metadata_location()
        .expect("schema-only CREATE still has a metadata pointer")
        .to_string();

    let batches = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.empty_adopted', \
             metadata_file => '{metadata_file}')"
        ),
    )
    .await
    .expect("register of a no-snapshot table")
    .collect()
    .await
    .expect("collect");
    let batch = &batches[0];
    assert_eq!(
        batch.num_rows(),
        1,
        "Spark returns one row of nulls, not an empty batch"
    );
    assert_eq!(i64_cell(batch, 0), None, "current_snapshot_id must be null");
    assert_eq!(i64_cell(batch, 1), None, "total_records_count must be null");
    assert_eq!(
        i64_cell(batch, 2),
        None,
        "total_data_files_count must be null — never a fabricated zero or file walk"
    );
}

#[tokio::test]
async fn call_register_table_accepts_positional_arguments() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pos AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::from_strs(["sales", "pos"]).unwrap();
    let metadata_file = catalogs
        .get("ice")
        .unwrap()
        .load_table(&ident)
        .await
        .unwrap()
        .metadata_location()
        .expect("pointer")
        .to_string();

    execute(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.register_table('sales.pos_adopted', '{metadata_file}')"),
    )
    .await
    .expect("positional register_table");
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.pos_adopted").await,
        3
    );
}

#[tokio::test]
async fn call_register_table_refuses_an_empty_metadata_file() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let err = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.register_table(table => 'sales.t', metadata_file => '')",
    )
    .await
    .expect_err("empty metadata_file must refuse")
    .to_string();
    assert!(
        err.contains("non-empty") && err.contains("metadata_file"),
        "empty metadata_file refusal must name the argument: {err}"
    );
}

#[tokio::test]
async fn call_register_table_refuses_a_missing_metadata_file_argument() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let err = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.register_table(table => 'sales.t')",
    )
    .await
    .expect_err("metadata_file is required")
    .to_string();
    assert!(
        err.contains("metadata_file"),
        "missing-argument refusal must name metadata_file: {err}"
    );
}

#[tokio::test]
async fn call_register_table_of_hadoop_named_metadata_writes_name_the_convention() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.hadoop AS SELECT * FROM src",
    )
    .await;
    let ident = TableIdent::from_strs(["sales", "hadoop"]).unwrap();
    let catalog = catalogs.get("ice").unwrap();
    let table = catalog.load_table(&ident).await.unwrap();
    let original = PathBuf::from(table.metadata_location().expect("pointer"));
    let hadoop = original
        .parent()
        .expect("metadata dir")
        .join("v1.metadata.json");
    fs::copy(&original, &hadoop).expect("copy to Hadoop name");
    catalog.drop_table(&ident).await.expect("drop pointer");

    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.hadoop', \
             metadata_file => '{}')",
            hadoop.display()
        ),
    )
    .await
    .expect("Hadoop-named metadata must register — reads work");
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.hadoop").await,
        3,
        "Hadoop-named adopt must still read"
    );

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.hadoop SELECT 4 AS id, 'd' AS name",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.hadoop").await,
        4,
        "a Hadoop vN pointer must take a write"
    );
    let written = catalog
        .load_table(&ident)
        .await
        .expect("reload after Hadoop write");
    let pointer = PathBuf::from(written.metadata_location().expect("pointer after write"));
    assert_eq!(
        pointer.file_name().and_then(|name| name.to_str()),
        Some("v2.metadata.json"),
        "fork #235 bumps Hadoop vN to uncompressed v(N+1): {}",
        pointer.display()
    );
}

#[tokio::test]
async fn call_register_table_on_s3_tables_names_the_dated_service_gap() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&warehouse).await;
    let s3_tables = repark_iceberg::catalog::s3tables_catalog(&HashMap::from([
        (
            "table_bucket_arn".to_string(),
            "arn:aws:s3tables:us-east-2:123456789012:bucket/example".to_string(),
        ),
        ("region_name".to_string(), "us-east-2".to_string()),
    ]))
    .await
    .expect("s3tables catalog constructs offline");
    catalogs.insert(
        "s3t".to_string(),
        s3_tables,
        LocationPolicy::ServiceManagedLocation,
    );
    let err = execute(
        &ctx,
        &catalogs,
        "CALL s3t.system.register_table(table => 'ns.t', metadata_file => '/tmp/x.metadata.json')",
    )
    .await
    .expect_err("S3 Tables has no register-by-metadata-location API")
    .to_string();
    assert!(
        err.contains("R126"),
        "refusal must cite fork gap R126: {err}"
    );
    assert!(
        err.contains("no register-by-metadata-location"),
        "refusal must name the missing operation: {err}"
    );
}

#[tokio::test]
async fn call_register_table_adopts_a_spark_written_v3_table_with_puffin_vectors() {
    let fixture = materialize_spark_v3_fixture();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;

    let batches = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.sparkv3', \
             metadata_file => '{}')",
            fixture.metadata_file
        ),
    )
    .await
    .expect("register Spark-written v3 table")
    .collect()
    .await
    .expect("collect");
    let batch = &batches[0];
    assert_eq!(i64_cell(batch, 0), Some(4_803_484_336_433_650_168));
    assert_eq!(i64_cell(batch, 1), Some(40), "Spark summary total-records");
    assert_eq!(
        i64_cell(batch, 2),
        Some(4),
        "Spark summary total-data-files"
    );

    let live = rows(&ctx, &catalogs, "SELECT * FROM ice.sales.sparkv3").await;
    assert_eq!(
        live, 37,
        "four appends of 10 minus three position deletes; deletion vectors must apply"
    );
}

#[tokio::test]
async fn call_rewrite_position_delete_files_refuses_spark_written_puffin_vectors() {
    let fixture = materialize_spark_v3_fixture();
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.sparkv3', \
             metadata_file => '{}')",
            fixture.metadata_file
        ),
    )
    .await
    .expect("register");

    let err = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.rewrite_position_delete_files(table => 'sales.sparkv3')",
    )
    .await
    .expect_err("B-MOR-3: live Puffin vectors must refuse, not return zeros")
    .to_string();
    assert!(
        err.contains("3 live Puffin deletion vector"),
        "refusal must count the Spark-written vectors: {err}"
    );
}

#[tokio::test]
async fn call_register_table_of_an_occupied_ident_refuses_and_keeps_the_original_rows() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.keep AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.other AS SELECT 9 AS id, 'z' AS name",
    )
    .await;
    let other = catalogs
        .get("ice")
        .unwrap()
        .load_table(&TableIdent::from_strs(["sales", "other"]).unwrap())
        .await
        .unwrap();
    let other_meta = other
        .metadata_location()
        .expect("other table pointer")
        .to_string();

    let err = execute(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.keep', \
             metadata_file => '{other_meta}')"
        ),
    )
    .await
    .expect_err("occupied ident must refuse, not swap the pointer")
    .to_string();
    assert!(
        err.to_ascii_lowercase().contains("already")
            || err.contains("exists")
            || err.contains("Occupied"),
        "occupied-ident refusal must name already-exists, not succeed: {err}"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.keep").await,
        3,
        "failed register must leave the original table's rows in place"
    );
}

#[tokio::test]
async fn call_register_table_refuses_an_unknown_named_argument() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let err = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.register_table(table => 'sales.t', metadata_file => '/x', extra => 'y')",
    )
    .await
    .expect_err("unknown named argument must refuse")
    .to_string();
    assert!(
        err.contains("extra") && err.contains("unknown CALL argument"),
        "unknown-argument refusal must name the key: {err}"
    );
}

#[tokio::test]
async fn call_unknown_procedure_lists_register_table() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let message = execute(
        &ctx,
        &catalogs,
        "CALL ice.system.not_a_real_proc(table => 'sales.t')",
    )
    .await
    .expect_err("unknown CALL")
    .to_string();
    assert!(
        message.contains("register_table"),
        "supported-procedure list must include register_table: {message}"
    );
}
