use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::prelude::DataFrame;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;

use crate::ReparkSession;

fn write_session(bucket: &str) -> (ReparkSession, Arc<InMemory>) {
    let session = ReparkSession::new().unwrap();
    let memory = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = memory.clone();
    session
        .register_s3_bucket_store_for_test(bucket, &store)
        .unwrap();
    (session, memory)
}

async fn frame_of(session: &ReparkSession, sql: &str) -> DataFrame {
    session.sql(sql).await.unwrap()
}

async fn row_count(frame: DataFrame) -> usize {
    frame
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

async fn part_key(memory: &InMemory, prefix: &str, extension: &str) -> String {
    let scope = ObjectPath::from(prefix);
    let mut listed = memory.list(Some(&scope));
    while let Some(meta) = listed.next().await {
        let location = meta.unwrap().location.to_string();
        if ObjectPath::from(location.as_str()).extension() == Some(extension) {
            return location;
        }
    }
    panic!("no .{extension} part under {prefix}");
}

async fn read_format(
    session: &ReparkSession,
    url: &str,
    format: &str,
    options: &HashMap<String, String>,
) -> DataFrame {
    if format == "parquet" {
        session.read_parquet(url).await.unwrap()
    } else if format == "csv" {
        session.read_csv(url, options).await.unwrap()
    } else {
        session.read_json(url, &HashMap::new()).await.unwrap()
    }
}

#[tokio::test]
async fn slashless_prefix_reads_written_parts_for_every_format() {
    for format in ["parquet", "csv", "json"] {
        let (session, _) = write_session("prefix-bucket");
        let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
        let url = format!("s3://prefix-bucket/cell-{format}/p");
        session
            .write_path(&frame, &url, format, "error", &HashMap::new(), &[])
            .await
            .unwrap();
        let options = HashMap::from([("header".to_string(), "true".to_string())]);
        let back = read_format(&session, &url, format, &options).await;
        assert_eq!(row_count(back).await, 1, "format {format}");
    }
}

#[tokio::test]
async fn exact_part_urls_keep_single_file_reads_for_every_format() {
    for format in ["parquet", "csv", "json"] {
        let (session, memory) = write_session("prefix-bucket");
        let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
        let url = format!("s3://prefix-bucket/exact-{format}/p");
        session
            .write_path(&frame, &url, format, "error", &HashMap::new(), &[])
            .await
            .unwrap();
        let part = part_key(&memory, &format!("exact-{format}/p"), format).await;
        let file_url = format!("s3://prefix-bucket/{part}");
        let options = HashMap::from([("header".to_string(), "true".to_string())]);
        let back = read_format(&session, &file_url, format, &options).await;
        assert_eq!(row_count(back).await, 1, "format {format}");
    }
}

#[tokio::test]
async fn trailing_slash_and_s3a_spelling_read_the_same_prefix() {
    let (session, _) = write_session("prefix-bucket");
    let frame = frame_of(&session, "SELECT 1 AS id, 'a' AS grp").await;
    let url = "s3://prefix-bucket/cell/p";
    session
        .write_path(&frame, url, "parquet", "error", &HashMap::new(), &[])
        .await
        .unwrap();
    let slashed = session
        .read_parquet("s3://prefix-bucket/cell/p/")
        .await
        .unwrap();
    assert_eq!(row_count(slashed).await, 1);
    let aliased = session
        .read_parquet("s3a://prefix-bucket/cell/p")
        .await
        .unwrap();
    assert_eq!(row_count(aliased).await, 1);
}
