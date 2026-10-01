use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::NaiveDate;
use datafusion::datasource::MemTable;
use datafusion::prelude::DataFrame;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;

use crate::ReparkSession;

fn options_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn write_session(bucket: &str) -> (ReparkSession, Arc<InMemory>) {
    let session = ReparkSession::new().unwrap();
    let memory = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = memory.clone();
    session
        .register_s3_bucket_store_for_test(bucket, &store)
        .unwrap();
    (session, memory)
}

async fn listed_names(memory: &InMemory, prefix: &str) -> Vec<String> {
    let scope = if prefix.is_empty() {
        None
    } else {
        Some(ObjectPath::parse(prefix).unwrap())
    };
    let mut listed = memory.list(scope.as_ref());
    let mut names = Vec::new();
    while let Some(meta) = listed.next().await {
        names.push(meta.unwrap().location.to_string());
    }
    names.sort();
    names
}

async fn frame_of(session: &ReparkSession, sql: &str) -> DataFrame {
    session.sql(sql).await.unwrap()
}

fn has_extension(name: &str, extension: &str) -> bool {
    ObjectPath::from(name).extension() == Some(extension)
}

fn has_filename(name: &str, filename: &str) -> bool {
    ObjectPath::from(name).filename() == Some(filename)
}

async fn object_bytes(memory: &InMemory, key: &str) -> Vec<u8> {
    memory
        .get(&ObjectPath::from(key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec()
}

async fn assert_frame_rows(frame: DataFrame, expected_rows: usize, fragments: &[&str]) {
    let batches = frame.collect().await.unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, expected_rows);
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap();
    let rendered = text.to_string();
    for fragment in fragments {
        assert!(rendered.contains(fragment), "got:\n{rendered}");
    }
}

#[tokio::test]
async fn error_write_lands_parts_plus_success_and_refuses_twice() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let url = "s3://write-bucket/cell/p";
    let count = session
        .write_path(&frame, url, "parquet", "error", &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(count, 1);
    let names = listed_names(&memory, "cell/p").await;
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|name| name.ends_with("_SUCCESS")));
    let part = names
        .iter()
        .find(|name| has_extension(name, "parquet"))
        .unwrap()
        .clone();
    assert!(names.iter().all(|name| !name.contains("_temporary")));
    assert!(object_bytes(&memory, "cell/p/_SUCCESS").await.is_empty());
    let back = session
        .read_parquet(&format!("s3://write-bucket/{part}"))
        .await
        .unwrap();
    assert_frame_rows(back, 1, &["1", "a"]).await;
    let views = session.list_temp_view_names().unwrap();
    assert!(views.iter().all(|name| !name.contains("_repark_s3_write_")));
    let error = session
        .write_path(&frame, url, "parquet", "error", &HashMap::new(), &[])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("[PATH_ALREADY_EXISTS]"));
    assert!(error.to_string().contains(url));
}

#[tokio::test]
async fn errorifexists_spelling_matches_error() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id").await;
    let url = "s3a://write-bucket/cell/p";
    session
        .write_path(
            &frame,
            url,
            "parquet",
            "errorifexists",
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(listed_names(&memory, "cell/p").await.len(), 2);
    let error = session
        .write_path(
            &frame,
            url,
            "parquet",
            "errorifexists",
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("[PATH_ALREADY_EXISTS]"));
}

#[tokio::test]
async fn ignore_writes_once_then_noops() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let url = "s3://write-bucket/cell/p";
    let first = session
        .write_path(&frame, url, "csv", "ignore", &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(first, 1);
    let before = listed_names(&memory, "cell/p").await;
    let second = session
        .write_path(&frame, url, "csv", "ignore", &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(second, 1);
    assert_eq!(listed_names(&memory, "cell/p").await, before);
}

#[tokio::test]
async fn overwrite_replaces_parts_and_foreign_objects() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let url = "s3://write-bucket/cell/p";
    session
        .write_path(&frame, url, "json", "error", &HashMap::new(), &[])
        .await
        .unwrap();
    memory
        .put(
            &ObjectPath::from("cell/p/foreign.txt"),
            object_store::PutPayload::from("not-a-part-file"),
        )
        .await
        .unwrap();
    assert_eq!(listed_names(&memory, "cell/p").await.len(), 3);
    let count = session
        .write_path(&frame, url, "json", "overwrite", &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(count, 1);
    let names = listed_names(&memory, "cell/p").await;
    assert_eq!(names.len(), 2);
    assert!(names.iter().any(|name| name.ends_with("_SUCCESS")));
    assert!(names.iter().any(|name| has_extension(name, "json")));
}

#[tokio::test]
async fn append_adds_a_fresh_part_and_preserves_foreign_objects() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let url = "s3://write-bucket/cell/p";
    session
        .write_path(&frame, url, "parquet", "error", &HashMap::new(), &[])
        .await
        .unwrap();
    memory
        .put(
            &ObjectPath::from("cell/p/foreign.txt"),
            object_store::PutPayload::from("not-a-part-file"),
        )
        .await
        .unwrap();
    let count = session
        .write_path(&frame, url, "parquet", "append", &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(count, 2);
    let names = listed_names(&memory, "cell/p").await;
    assert!(names.iter().any(|name| has_filename(name, "foreign.txt")));
    assert!(names.iter().any(|name| name.ends_with("_SUCCESS")));
    let parts: Vec<&String> = names
        .iter()
        .filter(|name| has_extension(name, "parquet"))
        .collect();
    assert_eq!(parts.len(), 2);
    for part in parts {
        let back = session
            .read_parquet(&format!("s3://write-bucket/{part}"))
            .await
            .unwrap();
        assert_frame_rows(back, 1, &["1", "a"]).await;
    }
}

#[tokio::test]
async fn success_only_prefix_counts_as_existing() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id").await;
    let url = "s3://write-bucket/cell/p";
    memory
        .put(
            &ObjectPath::from("cell/p/_SUCCESS"),
            object_store::PutPayload::from(Vec::<u8>::new()),
        )
        .await
        .unwrap();
    let error = session
        .write_path(&frame, url, "parquet", "error", &HashMap::new(), &[])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("[PATH_ALREADY_EXISTS]"));
    let count = session
        .write_path(&frame, url, "parquet", "ignore", &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn foreign_only_prefix_counts_as_existing() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id").await;
    let url = "s3://write-bucket/cell/p";
    memory
        .put(
            &ObjectPath::from("cell/p/foreign.txt"),
            object_store::PutPayload::from("not-a-part-file"),
        )
        .await
        .unwrap();
    let error = session
        .write_path(&frame, url, "parquet", "error", &HashMap::new(), &[])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("[PATH_ALREADY_EXISTS]"));
    let count = session
        .write_path(&frame, url, "parquet", "append", &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert!(
        listed_names(&memory, "cell/p")
            .await
            .iter()
            .any(|name| has_filename(name, "foreign.txt"))
    );
}

#[tokio::test]
async fn append_refuses_mismatched_parquet_columns_and_types() {
    let (session, _) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let url = "s3://write-bucket/cell/p";
    session
        .write_path(&frame, url, "parquet", "error", &HashMap::new(), &[])
        .await
        .unwrap();
    let renamed = frame_of(&session, "SELECT 1 AS id, 'a' AS other").await;
    let error = session
        .write_path(&renamed, url, "parquet", "append", &HashMap::new(), &[])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("column sets differ"));
    let retyped = frame_of(&session, "SELECT 'x' AS id, 'a' AS grp").await;
    let error = session
        .write_path(&retyped, url, "parquet", "append", &HashMap::new(), &[])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("type mismatch"));
}

#[tokio::test]
async fn empty_frame_still_writes_a_part_per_format() {
    for format in ["parquet", "csv", "json"] {
        let (session, memory) = write_session("write-bucket");
        let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
        let empty = frame.limit(0, Some(0)).unwrap();
        let url = format!("s3://write-bucket/cell-{format}/p");
        let count = session
            .write_path(&empty, &url, format, "overwrite", &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(count, 1, "format {format}");
        let names = listed_names(&memory, &format!("cell-{format}/p")).await;
        assert!(names.iter().any(|name| name.ends_with("_SUCCESS")));
        let part = names
            .iter()
            .find(|name| has_extension(name, format))
            .unwrap()
            .clone();
        assert!(part.ends_with(&format!("part-00000.{format}")));
        let file_url = format!("s3://write-bucket/{part}");
        if format == "parquet" {
            assert!(!object_bytes(&memory, &part).await.is_empty());
            let back = session.read_parquet(&file_url).await.unwrap();
            assert_frame_rows(back, 0, &[]).await;
        } else if format == "csv" {
            let back = session
                .read_csv(&file_url, &options_map(&[("header", "true")]))
                .await
                .unwrap();
            assert_frame_rows(back, 0, &[]).await;
        } else {
            assert!(object_bytes(&memory, &part).await.is_empty());
        }
    }
}

#[tokio::test]
async fn partition_by_writes_hive_dirs() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let url = "s3://write-bucket/cell/p";
    let count = session
        .write_path(
            &frame,
            url,
            "parquet",
            "overwrite",
            &HashMap::new(),
            &["grp".to_string()],
        )
        .await
        .unwrap();
    assert_eq!(count, 1);
    let names = listed_names(&memory, "cell/p").await;
    assert!(names.iter().any(|name| name.contains("grp=a/")));
    assert!(names.iter().any(|name| name.ends_with("_SUCCESS")));
    let part = names
        .iter()
        .find(|name| has_extension(name, "parquet"))
        .unwrap();
    let back = session
        .read_parquet(&format!("s3://write-bucket/{part}"))
        .await
        .unwrap();
    assert_frame_rows(back, 1, &["1"]).await;
}

#[tokio::test]
async fn csv_separator_option_reaches_the_copy() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let url = "s3://write-bucket/cell/p";
    session
        .write_path(
            &frame,
            url,
            "csv",
            "error",
            &options_map(&[("sep", "|")]),
            &[],
        )
        .await
        .unwrap();
    let names = listed_names(&memory, "cell/p").await;
    let part = names
        .iter()
        .find(|name| has_extension(name, "csv"))
        .unwrap();
    let text = String::from_utf8(object_bytes(&memory, part).await).unwrap();
    assert!(text.contains("id|grp"), "got: {text}");
    assert!(text.contains("1|a"), "got: {text}");
}

#[tokio::test]
async fn invalid_format_mode_partition_and_option_refuse_loud() {
    let (session, _) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let url = "s3://write-bucket/cell/p";
    let error = session
        .write_path(&frame, url, "avro", "error", &HashMap::new(), &[])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unknown path write format"));
    let error = session
        .write_path(&frame, url, "parquet", "merge", &HashMap::new(), &[])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("path write mode must be one of"));
    let error = session
        .write_path(
            &frame,
            url,
            "parquet",
            "error",
            &HashMap::new(),
            &["missing".to_string()],
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("is not in the DataFrame columns")
    );
}

#[tokio::test]
async fn temporal_write_options_are_honored_on_csv_path_write() {
    let (session, _) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    session
        .write_path(
            &frame,
            "s3://write-bucket/cell/p",
            "csv",
            "error",
            &options_map(&[("dateFormat", "yyyy")]),
            &[],
        )
        .await
        .expect("temporal write options are honored, not refused");
}

#[tokio::test]
async fn non_s3_destination_refuses_before_any_store_touch() {
    let (session, _) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id").await;
    for url in [
        "/tmp/cell/p",
        "file:///tmp/cell/p",
        "gs://write-bucket/cell/p",
    ] {
        let error = session
            .write_path(&frame, url, "parquet", "error", &HashMap::new(), &[])
            .await
            .unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("not an s3:// or s3a:// URL")
                || message.contains("invalid path write destination"),
            "got: {message}"
        );
    }
}

#[tokio::test]
async fn overwrite_at_bucket_root_refuses() {
    let (session, _) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id").await;
    let error = session
        .write_path(
            &frame,
            "s3://write-bucket",
            "parquet",
            "overwrite",
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("refusing to delete a whole bucket")
    );
}

#[tokio::test]
async fn unregistered_bucket_write_hits_the_finalize_gate() {
    let session = ReparkSession::new().unwrap();
    let frame = frame_of(&session, "SELECT 1 AS id").await;
    let error = session
        .write_path(
            &frame,
            "s3://nope-bucket/cell/p",
            "parquet",
            "error",
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("register_configured_catalogs"));
}

#[tokio::test]
async fn extension_destination_writes_a_directory_for_append() {
    for format in ["parquet", "csv", "json"] {
        let (session, memory) = write_session("write-bucket");
        let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
        let url = format!("s3://write-bucket/cell/out.{format}");
        session
            .write_path(&frame, &url, format, "overwrite", &HashMap::new(), &[])
            .await
            .unwrap();
        let count = session
            .write_path(&frame, &url, format, "append", &HashMap::new(), &[])
            .await
            .unwrap();
        assert_eq!(count, 2, "format {format}");
        let names = listed_names(&memory, &format!("cell/out.{format}")).await;
        assert_eq!(names.len(), 3, "format {format}: {names:?}");
        assert!(
            names
                .iter()
                .all(|name| name.starts_with(&format!("cell/out.{format}/"))),
            "format {format}: {names:?}"
        );
    }
}

#[tokio::test]
async fn exact_key_object_is_visible_to_error_ignore_and_overwrite() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let url = "s3://write-bucket/cell/solo.parquet";
    memory
        .put(
            &ObjectPath::from("cell/solo.parquet"),
            object_store::PutPayload::from("seed-bytes"),
        )
        .await
        .unwrap();
    let error = session
        .write_path(&frame, url, "parquet", "error", &HashMap::new(), &[])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("[PATH_ALREADY_EXISTS]"));
    assert_eq!(
        object_bytes(&memory, "cell/solo.parquet").await,
        b"seed-bytes"
    );
    session
        .write_path(&frame, url, "parquet", "ignore", &HashMap::new(), &[])
        .await
        .unwrap();
    assert_eq!(
        object_bytes(&memory, "cell/solo.parquet").await,
        b"seed-bytes"
    );
    session
        .write_path(&frame, url, "parquet", "overwrite", &HashMap::new(), &[])
        .await
        .unwrap();
    let names = listed_names(&memory, "cell/solo.parquet").await;
    assert!(
        !names.iter().any(|name| name == "cell/solo.parquet"),
        "exact object must go: {names:?}"
    );
    assert!(names.iter().any(|name| name.ends_with("_SUCCESS")));
    assert!(names.iter().any(|name| has_extension(name, "parquet")));
}

#[tokio::test]
async fn overwrite_of_prefix_the_frame_reads_refuses() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let url = "s3://write-bucket/cell/p";
    session
        .write_path(&frame, url, "parquet", "overwrite", &HashMap::new(), &[])
        .await
        .unwrap();
    let back = session.read_parquet(url).await.unwrap();
    let error = session
        .write_path(&back, url, "parquet", "overwrite", &HashMap::new(), &[])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("UNSUPPORTED_OVERWRITE"));
    let names = listed_names(&memory, "cell/p").await;
    assert_eq!(names.len(), 2);
    let again = session.read_parquet(url).await.unwrap();
    assert_frame_rows(again, 1, &["1", "a"]).await;
}

const ROLLBACK_ROWS: usize = 400_000;
const ROLLBACK_PARTITIONS: usize = 8;
const ROLLBACK_FAILING_ROW: i64 = 99_999;

fn write_session_in(bucket: &str, zone: &str) -> (ReparkSession, Arc<InMemory>) {
    let session = ReparkSession::builder()
        .configs(HashMap::from([(
            "spark.sql.session.timeZone".to_string(),
            zone.to_string(),
        )]))
        .build()
        .unwrap();
    let memory = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = memory.clone();
    session
        .register_s3_bucket_store_for_test(bucket, &store)
        .unwrap();
    (session, memory)
}

fn rollback_noon_micros() -> i64 {
    NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_micros()
}

fn rollback_frame_schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("n", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("s", DataType::Utf8, false),
    ]))
}

fn rollback_seed_batch() -> RecordBatch {
    let micros = rollback_noon_micros();
    RecordBatch::try_new(
        rollback_frame_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(TimestampMicrosecondArray::from(vec![micros; 3])),
            Arc::new(TimestampMicrosecondArray::from(vec![micros; 3]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec!["a", "b", "a"])),
        ],
    )
    .unwrap()
}

fn rollback_big_partitions() -> (arrow::datatypes::SchemaRef, Vec<Vec<RecordBatch>>) {
    let micros = rollback_noon_micros();
    let schema = rollback_frame_schema();
    let rows_per_partition = ROLLBACK_ROWS / ROLLBACK_PARTITIONS;
    let mut partitions = Vec::with_capacity(ROLLBACK_PARTITIONS);
    for partition in 0..ROLLBACK_PARTITIONS {
        let base = i64::try_from(partition * rows_per_partition).unwrap();
        let span = i64::try_from(rows_per_partition).unwrap();
        let ids: Vec<i64> = (0..span).map(|offset| base + offset).collect();
        let ntz: Vec<Option<i64>> = ids
            .iter()
            .map(|id| {
                if *id == ROLLBACK_FAILING_ROW {
                    Some(micros)
                } else {
                    None
                }
            })
            .collect();
        let ltz = vec![micros; rows_per_partition];
        let groups: Vec<&str> = ids
            .iter()
            .map(|id| if id % 2 == 0 { "a" } else { "b" })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(TimestampMicrosecondArray::from(ntz)),
                Arc::new(TimestampMicrosecondArray::from(ltz).with_timezone("UTC")),
                Arc::new(StringArray::from(groups)),
            ],
        )
        .unwrap();
        partitions.push(vec![batch]);
    }
    (schema, partitions)
}

async fn frame_of_rollback_big(session: &ReparkSession, name: &str) -> DataFrame {
    let (schema, partitions) = rollback_big_partitions();
    let table = MemTable::try_new(schema, partitions).unwrap();
    session
        .context()
        .register_table(name, Arc::new(table))
        .unwrap();
    session.sql(&format!("SELECT * FROM {name}")).await.unwrap()
}

fn rollback_pattern_options() -> HashMap<String, String> {
    options_map(&[("timestampNTZFormat", "yyyy-MM-dd VV")])
}

#[tokio::test]
async fn failed_render_overwrite_into_empty_leaves_no_objects() {
    let (session, memory) = write_session_in("rollback-bucket", "America/New_York");
    let frame = frame_of_rollback_big(&session, "big_over").await;
    let url = "s3://rollback-bucket/cell/over";
    assert!(listed_names(&memory, "cell/over").await.is_empty());
    let error = session
        .write_path(
            &frame,
            url,
            "json",
            "overwrite",
            &rollback_pattern_options(),
            &[],
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Unable to extract ZoneId"),
        "got: {error}"
    );
    assert!(listed_names(&memory, "cell/over").await.is_empty());
}

#[tokio::test]
async fn failed_render_append_preserves_existing_objects() {
    let (session, memory) = write_session_in("rollback-bucket", "America/New_York");
    session
        .context()
        .register_batch("seed_append", rollback_seed_batch())
        .unwrap();
    let seed = session.sql("SELECT * FROM seed_append").await.unwrap();
    let frame = frame_of_rollback_big(&session, "big_append").await;
    let url = "s3://rollback-bucket/cell/append";
    session
        .write_path(&seed, url, "json", "error", &HashMap::new(), &[])
        .await
        .unwrap();
    let before = listed_names(&memory, "cell/append").await;
    assert!(!before.is_empty());
    let mut before_bytes = Vec::with_capacity(before.len());
    for name in &before {
        before_bytes.push(object_bytes(&memory, name).await);
    }
    let error = session
        .write_path(
            &frame,
            url,
            "json",
            "append",
            &rollback_pattern_options(),
            &[],
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Unable to extract ZoneId"),
        "got: {error}"
    );
    let after = listed_names(&memory, "cell/append").await;
    assert_eq!(after, before);
    for (name, bytes) in before.iter().zip(before_bytes.iter()) {
        assert_eq!(&object_bytes(&memory, name).await, bytes);
    }
}

#[tokio::test]
async fn failed_render_partitioned_overwrite_leaves_no_objects() {
    let (session, memory) = write_session_in("rollback-bucket", "America/New_York");
    let frame = frame_of_rollback_big(&session, "big_part").await;
    let url = "s3://rollback-bucket/cell/part";
    assert!(listed_names(&memory, "cell/part").await.is_empty());
    let error = session
        .write_path(
            &frame,
            url,
            "json",
            "overwrite",
            &rollback_pattern_options(),
            &["s".to_string()],
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Unable to extract ZoneId"),
        "got: {error}"
    );
    assert!(listed_names(&memory, "cell/part").await.is_empty());
}

const CONCURRENT_FOREIGN_LINE: &str =
    "{\"id\":1,\"n\":\"2024-01-01T00:00:00.000\",\"t\":\"2024-06-15T12:00:00.000\"}\n";

async fn put_foreign_key(memory: &InMemory, key: &str) {
    memory
        .put(
            &ObjectPath::from(key),
            object_store::PutPayload::from(CONCURRENT_FOREIGN_LINE),
        )
        .await
        .unwrap();
}

async fn put_foreign_keys(memory: &InMemory, keys: &[String]) {
    for key in keys {
        put_foreign_key(memory, key).await;
        tokio::task::yield_now().await;
    }
}

fn foreign_keys(prefix: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{prefix}zz-foreign-{index:02}.json"))
        .collect()
}

#[tokio::test]
async fn failed_render_append_leaves_concurrent_objects_alone() {
    let (session, memory) = write_session_in("conc-bucket", "America/New_York");
    session
        .context()
        .register_batch("seed_conc", rollback_seed_batch())
        .unwrap();
    let seed = session.sql("SELECT * FROM seed_conc").await.unwrap();
    let frame = frame_of_rollback_big(&session, "big_conc").await;
    let url = "s3://conc-bucket/cell/conc";
    session
        .write_path(&seed, url, "json", "error", &HashMap::new(), &[])
        .await
        .unwrap();
    let before = listed_names(&memory, "cell/conc").await;
    assert!(!before.is_empty());
    let same = foreign_keys("cell/conc/", 16);
    let sibling = foreign_keys("cell/conc-sibling/", 4);
    let elsewhere = foreign_keys("elsewhere/", 4);
    let poke = async {
        put_foreign_keys(&memory, &same).await;
        put_foreign_keys(&memory, &sibling).await;
        put_foreign_keys(&memory, &elsewhere).await;
    };
    let options = rollback_pattern_options();
    let write = session.write_path(&frame, url, "json", "append", &options, &[]);
    let (result, ()) = futures::join!(write, poke);
    let error = result.unwrap_err();
    assert!(
        error.to_string().contains("Unable to extract ZoneId"),
        "got: {error}"
    );
    let mut expected = before.clone();
    expected.extend(same.iter().cloned());
    expected.extend(sibling.iter().cloned());
    expected.extend(elsewhere.iter().cloned());
    expected.sort();
    let after = listed_names(&memory, "").await;
    assert_eq!(after, expected);
    for key in same.iter().chain(sibling.iter()).chain(elsewhere.iter()) {
        assert_eq!(
            object_bytes(&memory, key).await,
            CONCURRENT_FOREIGN_LINE.as_bytes()
        );
    }
}

#[tokio::test]
async fn failed_render_root_append_leaves_foreign_prefixes_alone() {
    let (session, memory) = write_session_in("conc-root", "America/New_York");
    let frame = frame_of_rollback_big(&session, "big_root").await;
    let url = "s3://conc-root";
    put_foreign_key(&memory, "unrelated/keep.txt").await;
    let foreign = foreign_keys("unrelated/", 16);
    let poke = put_foreign_keys(&memory, &foreign);
    let options = rollback_pattern_options();
    let write = session.write_path(&frame, url, "json", "append", &options, &[]);
    let (result, ()) = futures::join!(write, poke);
    let error = result.unwrap_err();
    assert!(
        error.to_string().contains("Unable to extract ZoneId"),
        "got: {error}"
    );
    let after = listed_names(&memory, "").await;
    let mut expected = vec!["unrelated/keep.txt".to_string()];
    expected.extend(foreign.iter().cloned());
    expected.sort();
    assert_eq!(after, expected);
}

#[tokio::test]
async fn hash_key_writes_beside_the_plain_prefix() {
    let (session, memory) = write_session("write-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let grown = frame_of(&session, "SELECT 10 AS id, 'x' AS grp").await;
    session
        .write_path(
            &frame,
            "s3://write-bucket/cell/data",
            "parquet",
            "overwrite",
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    session
        .write_path(
            &grown,
            "s3://write-bucket/cell/data#v2",
            "parquet",
            "overwrite",
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    let plain = session
        .read_parquet("s3://write-bucket/cell/data")
        .await
        .unwrap();
    assert_frame_rows(plain, 1, &["1", "a"]).await;
    let names = listed_names(&memory, "cell/data#v2").await;
    assert_eq!(names.len(), 2, "hash keys: {names:?}");
    assert!(
        names.iter().all(|name| name.starts_with("cell/data#v2/")),
        "hash keys: {names:?}"
    );
}
