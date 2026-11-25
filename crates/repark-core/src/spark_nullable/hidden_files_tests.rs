use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::SessionContext;
use object_store::path::Path as ObjectPath;

use super::hidden_files::{SparkListingTable, spark_hidden_below, spark_hidden_name};
use crate::plan_introspect::{input_files, plan_reads_s3_prefix};
use crate::session::ReparkSession;

fn write_ids(path: &Path, ids: &[i64]) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("an id batch");
    let file = std::fs::File::create(path).expect("create the parquet file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("open the parquet writer");
    writer.write(&batch).expect("write the batch");
    writer.close().expect("close the parquet writer");
}

fn write_bytes(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).expect("write the file");
}

async fn read_ids(session: &ReparkSession, path: &Path) -> Result<Vec<i64>, String> {
    let frame = session
        .read_parquet(path.to_str().expect("a utf-8 path"))
        .await
        .map_err(|error| error.to_string())?;
    let batches = frame.collect().await.map_err(|error| error.to_string())?;
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("an id column");
        ids.extend((0..column.len()).map(|row| column.value(row)));
    }
    ids.sort_unstable();
    Ok(ids)
}

async fn read_files(session: &ReparkSession, path: &Path) -> Vec<String> {
    let frame = session
        .read_parquet(path.to_str().expect("a utf-8 path"))
        .await
        .expect("read the directory");
    let plan = frame.create_physical_plan().await.expect("plan the scan");
    let root = path.to_str().expect("a utf-8 path");
    let mut names = input_files(&plan)
        .into_iter()
        .map(|uri| {
            uri.trim_start_matches("file://")
                .trim_start_matches(root)
                .trim_start_matches('/')
                .to_owned()
        })
        .collect::<Vec<String>>();
    names.sort();
    names
}

#[test]
fn the_name_rule_is_sparks() {
    let hidden = [
        "_SUCCESS",
        "_committed_123",
        "_started_123",
        "_metadata",
        "_common_metadata",
        "_temporary",
        "_b.parquet",
        ".b.parquet",
        ".part-00000.parquet.crc",
        "._SUCCESS.crc",
        ".hidden",
        ".p=1",
        "b.parquet._COPYING_",
        "b._COPYING_",
    ];
    let visible = [
        "a.parquet",
        "b",
        "b.PARQUET",
        "b.snappy.parquet",
        "b.parq",
        "b.bin",
        "b.x.y.z",
        "README.txt",
        "_k=1.parquet",
        "_p=1",
        "p=1",
        "b_COPYING_",
        "part-00000",
    ];
    for name in hidden {
        assert!(spark_hidden_name(name), "{name} is hidden");
    }
    for name in visible {
        assert!(!spark_hidden_name(name), "{name} is visible");
    }
}

#[test]
fn the_rule_reads_every_segment_below_the_root() {
    let cases = [
        ("d", "d/a.parquet", false),
        ("d", "d/b", false),
        ("d", "d/_SUCCESS", true),
        ("d", "d/.a.parquet.crc", true),
        ("d", "d/p=1/b", false),
        ("d", "d/p=1/_SUCCESS", true),
        ("d", "d/_p=2/b.parquet", false),
        ("d", "d/.p=3/c.parquet", true),
        ("d", "d/_temporary/0/x.parquet", true),
        ("_root", "_root/a.parquet", false),
        (".root", ".root/b", false),
        ("d/b", "d/b", false),
        ("d/_b.parquet", "d/_b.parquet", true),
        ("d/.b.parquet", "d/.b.parquet", true),
        ("d/b.parquet._COPYING_", "d/b.parquet._COPYING_", true),
        ("", "_SUCCESS", true),
        ("", "a.parquet", false),
        ("d", "e/_SUCCESS", false),
    ];
    for (root, location, hidden) in cases {
        assert_eq!(
            spark_hidden_below(&ObjectPath::from(root), &ObjectPath::from(location)),
            hidden,
            "{root} / {location}"
        );
    }
}

#[tokio::test]
async fn a_directory_read_lists_every_name_spark_lists() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_ids(&dir.path().join("a.parquet"), &[10]);
    write_ids(&dir.path().join("b"), &[11]);
    write_ids(&dir.path().join("c.PARQUET"), &[12]);
    write_ids(&dir.path().join("d.bin"), &[13]);
    write_ids(&dir.path().join("_b.parquet"), &[90]);
    write_ids(&dir.path().join(".b.parquet"), &[91]);
    write_ids(&dir.path().join("e.parquet._COPYING_"), &[92]);
    write_bytes(&dir.path().join("_SUCCESS"), b"");
    write_bytes(&dir.path().join("_committed_1"), b"{}");
    write_bytes(&dir.path().join("_started_1"), b"");
    write_bytes(&dir.path().join(".a.parquet.crc"), b"crc");
    let session = ReparkSession::new().expect("a session");
    assert_eq!(
        read_ids(&session, dir.path()).await,
        Ok(vec![10, 11, 12, 13])
    );
    assert_eq!(
        read_files(&session, dir.path()).await,
        vec!["a.parquet", "b", "c.PARQUET", "d.bin"]
    );
}

#[tokio::test]
async fn partition_directories_keep_the_rule_on_each_level() {
    let dir = tempfile::tempdir().expect("a temp dir");
    for sub in ["p=1", "p=2", "p=3", ".p=4"] {
        std::fs::create_dir(dir.path().join(sub)).expect("a partition dir");
    }
    write_ids(&dir.path().join("p=1/part-00000"), &[1]);
    write_ids(&dir.path().join("p=2/data.bin"), &[2]);
    write_ids(&dir.path().join("p=3/_k=5"), &[5]);
    write_ids(&dir.path().join(".p=4/x.parquet"), &[94]);
    write_bytes(&dir.path().join("p=1/_SUCCESS"), b"x");
    write_bytes(&dir.path().join("p=2/.data.bin.crc"), b"crc");
    let session = ReparkSession::new().expect("a session");
    assert_eq!(read_ids(&session, dir.path()).await, Ok(vec![1, 2, 5]));
    std::fs::create_dir(dir.path().join("_q=3")).expect("a hidden-looking partition dir");
    write_ids(&dir.path().join("_q=3/x.parquet"), &[3]);
    let conflict = read_ids(&session, dir.path()).await;
    assert!(
        conflict
            .as_ref()
            .is_err_and(|error| error.contains("CONFLICTING_PARTITION_COLUMN_NAMES")),
        "{conflict:?}"
    );
}

#[tokio::test]
async fn a_single_file_path_reads_any_extension_but_not_a_hidden_name() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_ids(&dir.path().join("b"), &[11]);
    write_ids(&dir.path().join("c.PARQUET"), &[12]);
    write_ids(&dir.path().join("_b.parquet"), &[90]);
    write_ids(&dir.path().join(".b.parquet"), &[91]);
    let session = ReparkSession::new().expect("a session");
    assert_eq!(
        read_ids(&session, &dir.path().join("b")).await,
        Ok(vec![11])
    );
    assert_eq!(
        read_ids(&session, &dir.path().join("c.PARQUET")).await,
        Ok(vec![12])
    );
    for hidden in ["_b.parquet", ".b.parquet"] {
        let read = read_ids(&session, &dir.path().join(hidden)).await;
        assert!(read.is_err(), "{hidden} read as {read:?}");
    }
}

#[tokio::test]
async fn a_hidden_root_directory_is_read() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let root = dir.path().join("_root");
    std::fs::create_dir(&root).expect("a root dir");
    write_ids(&root.join("a.parquet"), &[10]);
    write_ids(&root.join("b"), &[11]);
    let session = ReparkSession::new().expect("a session");
    assert_eq!(read_ids(&session, &root).await, Ok(vec![10, 11]));
}

#[tokio::test]
async fn a_glob_keeps_the_rule_below_its_prefix() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_ids(&dir.path().join("a.parquet"), &[10]);
    write_ids(&dir.path().join("b"), &[11]);
    write_ids(&dir.path().join("_b.parquet"), &[90]);
    write_bytes(&dir.path().join(".b.crc"), b"crc");
    let session = ReparkSession::new().expect("a session");
    assert_eq!(
        read_ids(&session, &dir.path().join("*")).await,
        Ok(vec![10, 11])
    );
    let hidden_only = read_ids(&session, &dir.path().join("_*")).await;
    assert!(hidden_only.is_err(), "_* read as {hidden_only:?}");
}

#[tokio::test]
async fn the_wrapped_listing_keeps_the_overwrite_check() {
    let context = SessionContext::new();
    let url = ListingTableUrl::parse("s3://bucket/data/").expect("an s3 url");
    let root = url.prefix().clone();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
    let config = ListingTableConfig::new(url)
        .with_listing_options(ListingOptions::new(Arc::new(ParquetFormat::default())))
        .with_schema(schema);
    let table = ListingTable::try_new(config).expect("a listing table");
    let wrapped = SparkListingTable::new(table, root);
    let frame = context
        .read_table(Arc::new(wrapped))
        .expect("read the table");
    let plan = frame.logical_plan();
    assert!(plan_reads_s3_prefix(plan, "bucket", "data"));
    assert!(!plan_reads_s3_prefix(plan, "bucket", "other"));
}
