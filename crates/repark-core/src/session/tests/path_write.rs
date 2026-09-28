use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
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
        Some(ObjectPath::from(prefix))
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
    let error = session
        .write_path(
            &frame,
            url,
            "csv",
            "error",
            &options_map(&[("dateFormat", "yyyy")]),
            &[],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not supported yet"));
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
