use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Date32Array, Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{Datelike, NaiveDate};
use datafusion::prelude::DataFrame;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;

use crate::ReparkSession;
use crate::session::text_write_format::spec::TextWriteSpec;
use crate::session::text_write_format::udf::format_batch_for_sink;

fn options_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn write_session(bucket: &str) -> (ReparkSession, Arc<InMemory>) {
    let session = ReparkSession::builder()
        .configs(HashMap::from([(
            "spark.sql.session.timeZone".to_string(),
            "America/New_York".to_string(),
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

async fn object_text(memory: &InMemory, key: &str) -> String {
    let bytes = memory
        .get(&ObjectPath::from(key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn instant_micros() -> i64 {
    NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_milli_opt(12, 34, 56, 789)
        .unwrap()
        .and_utc()
        .timestamp_micros()
}

fn temporal_batch() -> RecordBatch {
    let micros = instant_micros();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("n", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("d", DataType::Date32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(vec![micros])),
            Arc::new(Date32Array::from(vec![
                NaiveDate::from_ymd_opt(2024, 6, 15)
                    .unwrap()
                    .num_days_from_ce()
                    - 719_163,
            ])),
        ],
    )
    .unwrap()
}

async fn frame_of_batch(session: &ReparkSession, name: &str, batch: RecordBatch) -> DataFrame {
    session.context().register_batch(name, batch).unwrap();
    session.sql(&format!("SELECT * FROM {name}")).await.unwrap()
}

#[tokio::test]
async fn sink_csv_formats_temporal_in_session_zone() {
    let (session, memory) = write_session("sink-bucket");
    let frame = frame_of_batch(&session, "v", temporal_batch()).await;
    session
        .write_path(
            &frame,
            "s3://sink-bucket/cell/p",
            "csv",
            "error",
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    let names = listed_names(&memory, "cell/p").await;
    assert_eq!(names.len(), 2, "part plus _SUCCESS, got {names:?}");
    let part = names.iter().find(|name| name.ends_with(".csv")).unwrap();
    assert_eq!(
        object_text(&memory, part).await,
        "id,t,n,d\n1,2024-06-15T08:34:56.789-04:00,2024-06-15T12:34:56.789,2024-06-15\n"
    );
}

#[tokio::test]
async fn sink_json_formats_temporal_in_session_zone() {
    let (session, memory) = write_session("sink-bucket");
    let frame = frame_of_batch(&session, "v", temporal_batch()).await;
    session
        .write_path(
            &frame,
            "s3://sink-bucket/cell/p",
            "json",
            "error",
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    let names = listed_names(&memory, "cell/p").await;
    assert_eq!(names.len(), 2, "part plus _SUCCESS, got {names:?}");
    let part = names.iter().find(|name| name.ends_with(".json")).unwrap();
    assert_eq!(
        object_text(&memory, part).await,
        "{\"id\":1,\"t\":\"2024-06-15T08:34:56.789-04:00\",\"n\":\"2024-06-15T12:34:56.789\",\"d\":\"2024-06-15\"}\n"
    );
}

#[tokio::test]
async fn sink_user_pattern_renders_through_serializer() {
    let (session, memory) = write_session("sink-bucket");
    let frame = frame_of_batch(&session, "v", temporal_batch()).await;
    session
        .write_path(
            &frame,
            "s3://sink-bucket/cell/p",
            "csv",
            "error",
            &options_map(&[("timestampFormat", "yyyy/MM/dd HH:mm")]),
            &[],
        )
        .await
        .unwrap();
    let names = listed_names(&memory, "cell/p").await;
    let part = names.iter().find(|name| name.ends_with(".csv")).unwrap();
    assert_eq!(
        object_text(&memory, part).await,
        "id,t,n,d\n1,2024/06/15 08:34,2024-06-15T12:34:56.789,2024-06-15\n"
    );
}

#[tokio::test]
async fn sink_partition_columns_keep_raw_directory_names() {
    let (session, memory) = write_session("sink-bucket");
    let frame = frame_of_batch(&session, "v", temporal_batch()).await;
    session
        .write_path(
            &frame,
            "s3://sink-bucket/cell/p",
            "csv",
            "error",
            &HashMap::new(),
            &["d".to_string()],
        )
        .await
        .unwrap();
    let names = listed_names(&memory, "cell/p").await;
    assert!(
        names
            .iter()
            .any(|name| name.starts_with("cell/p/d=2024-06-15/") && name.ends_with(".csv")),
        "raw date directory name, got {names:?}"
    );
    let part = names.iter().find(|name| name.ends_with(".csv")).unwrap();
    assert_eq!(
        object_text(&memory, part).await,
        "id,t,n\n1,2024-06-15T08:34:56.789-04:00,2024-06-15T12:34:56.789\n"
    );
}

#[tokio::test]
async fn sink_lazy_pattern_error_keeps_spark_message() {
    let (session, _) = write_session("sink-bucket");
    let frame = frame_of_batch(&session, "v", temporal_batch()).await;
    let error = session
        .write_path(
            &frame,
            "s3://sink-bucket/cell/p",
            "csv",
            "error",
            &options_map(&[("timestampNTZFormat", "xxx")]),
            &[],
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Unsupported field: OffsetSeconds"),
        "spark lazy message intact, got {error}"
    );
}

#[tokio::test]
async fn sink_eager_bad_pattern_refuses_before_any_write() {
    let (session, memory) = write_session("sink-bucket");
    let frame = frame_of_batch(&session, "v", temporal_batch()).await;
    let error = session
        .write_path(
            &frame,
            "s3://sink-bucket/cell/p",
            "csv",
            "error",
            &options_map(&[("timestampFormat", "vv")]),
            &[],
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("INVALID_DATETIME_PATTERN.WITH_SUGGESTION"),
        "spark class surfaces: {error}"
    );
    assert!(
        listed_names(&memory, "cell/p").await.is_empty(),
        "no objects written"
    );
}

#[tokio::test]
async fn sink_empty_frame_writes_header_only_csv() {
    let (session, memory) = write_session("sink-bucket");
    let empty = temporal_batch().slice(0, 0);
    let frame = frame_of_batch(&session, "v", empty).await;
    session
        .write_path(
            &frame,
            "s3://sink-bucket/cell/p",
            "csv",
            "error",
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    let names = listed_names(&memory, "cell/p").await;
    let part = names.iter().find(|name| name.ends_with(".csv")).unwrap();
    assert_eq!(object_text(&memory, part).await, "id,t,n,d\n");
}

#[tokio::test]
async fn sink_null_temporal_writes_empty_fields() {
    let (session, memory) = write_session("sink-bucket");
    let schema = temporal_batch().schema();
    let nulls = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(TimestampMicrosecondArray::from(vec![None]).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(vec![None])),
            Arc::new(Date32Array::from(vec![None])),
        ],
    )
    .unwrap();
    let frame = frame_of_batch(&session, "v", nulls).await;
    session
        .write_path(
            &frame,
            "s3://sink-bucket/cell/p",
            "csv",
            "error",
            &HashMap::new(),
            &[],
        )
        .await
        .unwrap();
    let names = listed_names(&memory, "cell/p").await;
    let part = names.iter().find(|name| name.ends_with(".csv")).unwrap();
    assert_eq!(object_text(&memory, part).await, "id,t,n,d\n1,,,\n");
}

#[tokio::test]
async fn sink_keep_partition_columns_pass_temporal_native() {
    let session = ReparkSession::builder()
        .configs(HashMap::from([(
            "spark.sql.session.timeZone".to_string(),
            "America/New_York".to_string(),
        )]))
        .build()
        .unwrap();
    let micros = instant_micros();
    let days = NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .num_days_from_ce()
        - 719_163;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("d", DataType::Date32, true),
        Field::new("p", DataType::Date32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC")),
            Arc::new(Date32Array::from(vec![days])),
            Arc::new(Date32Array::from(vec![days])),
        ],
    )
    .unwrap();
    session.context().register_batch("v", batch).unwrap();
    session
        .sql("SET datafusion.execution.keep_partition_by_columns = true")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let custom = dir.path().join("custom");
    let plain = dir.path().join("plain");
    let custom_sql = format!(
        "COPY (SELECT * FROM v) TO '{}' STORED AS repark_text_csv PARTITIONED BY (p) \
         OPTIONS ('repark.text.zone' 'America/New_York', 'repark.text.date_format_hex' \
         '64642f4d4d2f79797979')",
        custom.to_string_lossy()
    );
    session
        .sql(&custom_sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plain_sql = format!(
        "COPY (SELECT * FROM v) TO '{}' STORED AS CSV PARTITIONED BY (p)",
        plain.to_string_lossy()
    );
    session
        .sql(&plain_sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let read_dirs = |root: &std::path::Path| {
        let mut entries: Vec<String> = std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        entries.sort();
        entries
    };
    let custom_dirs = read_dirs(&custom);
    let plain_dirs = read_dirs(&plain);
    assert_eq!(custom_dirs, plain_dirs, "directory names match");
    assert_eq!(custom_dirs.len(), 1);
    let read_part = |root: &std::path::Path| {
        let part_dir = root.join(&custom_dirs[0]);
        let mut entries: Vec<String> = std::fs::read_dir(&part_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        entries.sort();
        assert_eq!(entries.len(), 1, "one part file in {part_dir:?}");
        std::fs::read_to_string(part_dir.join(&entries[0])).unwrap()
    };
    let custom_body = read_part(&custom);
    let plain_body = read_part(&plain);
    let custom_fields: Vec<&str> = custom_body.lines().nth(1).unwrap().split(',').collect();
    let plain_fields: Vec<&str> = plain_body.lines().nth(1).unwrap().split(',').collect();
    assert_eq!(custom_body.lines().next(), plain_body.lines().next());
    assert_eq!(
        custom_fields[3], plain_fields[3],
        "partition column passes through native: {custom_body:?} vs {plain_body:?}"
    );
    assert_eq!(
        custom_fields[2], "15/06/2024",
        "non-partition date takes the user pattern: {custom_body:?}"
    );
    assert!(
        custom_fields[1].contains("-04:00"),
        "zone offset rendered: {custom_body:?}"
    );
}

#[test]
fn sink_format_batch_skips_partition_columns() {
    let spec = TextWriteSpec::from_parts("America/New_York", None, None, None).unwrap();
    let micros = instant_micros();
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "p",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC")),
        ],
    )
    .unwrap();
    let skip = HashSet::from(["p".to_string()]);
    let formatted = format_batch_for_sink(&batch, &spec, &skip).unwrap();
    assert_eq!(formatted.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        formatted.schema().field(1).data_type(),
        batch.schema().field(1).data_type()
    );
    assert_eq!(formatted.column(1).as_ref(), batch.column(1).as_ref());
}
