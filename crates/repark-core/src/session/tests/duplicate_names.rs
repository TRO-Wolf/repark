use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::DataFrame;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;

use crate::ReparkSession;
use crate::frame_names::{
    DISPLAY_NAME_KEY, duplicate_tolerant_names, recorded_display_names, rename_duplicate_tolerant,
};
use crate::session::text_write_format::select::build_text_write_copy_parts;
use crate::session::text_write_format::serializer::with_display_header;
use crate::session::text_write_format::spec::TextWriteSpec;

fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

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
            "UTC".to_string(),
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

async fn written_parts(memory: &InMemory, prefix: &str) -> Vec<(String, String)> {
    let scope = ObjectPath::parse(prefix).unwrap();
    let mut listed = memory.list(Some(&scope));
    let mut keys = Vec::new();
    while let Some(meta) = listed.next().await {
        let location = meta.unwrap().location;
        if location.extension() == Some("csv") || location.extension() == Some("json") {
            keys.push(location);
        }
    }
    keys.sort();
    let mut parts = Vec::new();
    for key in keys {
        let bytes = memory.get(&key).await.unwrap().bytes().await.unwrap();
        parts.push((key.to_string(), String::from_utf8(bytes.to_vec()).unwrap()));
    }
    parts
}

fn left_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap()
}

fn right_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("t", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap()
}

async fn self_join(session: &ReparkSession) -> DataFrame {
    session.context().register_batch("l", left_batch()).unwrap();
    let joined = session
        .sql("SELECT a.id, a.s, a.v, b.id, b.s, b.v FROM l a JOIN l b ON a.id = b.id ORDER BY a.id")
        .await
        .unwrap();
    rename_duplicate_tolerant(joined, &names(&["id", "s", "v", "id", "s", "v"])).unwrap()
}

async fn mixed_join(session: &ReparkSession) -> DataFrame {
    session.context().register_batch("l", left_batch()).unwrap();
    session
        .context()
        .register_batch("r", right_batch())
        .unwrap();
    let joined = session
        .sql("SELECT a.id, a.s, a.v, b.id, b.t FROM l a JOIN r b ON a.id = b.id")
        .await
        .unwrap();
    rename_duplicate_tolerant(joined, &names(&["id", "s", "v", "id", "t"])).unwrap()
}

#[test]
fn names_without_an_exact_duplicate_answer_none() {
    assert_eq!(duplicate_tolerant_names(&names(&["id", "s", "v"])), None);
    assert_eq!(duplicate_tolerant_names(&names(&["id", "ID", "Id"])), None);
    assert_eq!(duplicate_tolerant_names(&[]), None);
}

#[test]
fn repeated_names_take_their_position_and_unique_names_stay() {
    assert_eq!(
        duplicate_tolerant_names(&names(&["id", "s", "v", "id", "t"])),
        Some(names(&[
            "__repark_dup_0_id",
            "s",
            "v",
            "__repark_dup_3_id",
            "t"
        ]))
    );
    assert_eq!(
        duplicate_tolerant_names(&names(&["a", "a", "a", "A"])),
        Some(names(&[
            "__repark_dup_0_a",
            "__repark_dup_1_a",
            "__repark_dup_2_a",
            "A"
        ]))
    );
}

#[test]
fn generated_names_step_around_a_user_column_of_the_same_shape() {
    assert_eq!(
        duplicate_tolerant_names(&names(&["id", "id", "__repark_dup_0_id"])),
        Some(names(&[
            "__repark_dup_0_id_",
            "__repark_dup_1_id",
            "__repark_dup_0_id"
        ]))
    );
    assert_eq!(
        duplicate_tolerant_names(&names(&["__repark_dup_0_id", "s", "__repark_dup_7_"])),
        None
    );
}

#[test]
fn display_names_are_read_from_the_record_never_from_a_name() {
    let unrecorded = Schema::new(vec![
        Field::new("__repark_dup_0_id", DataType::Int64, true),
        Field::new("__repark_dup_7_", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]);
    assert_eq!(recorded_display_names(&unrecorded), None);
    let recorded = Schema::new(vec![
        Field::new("x0", DataType::Int64, true).with_metadata(HashMap::from([(
            DISPLAY_NAME_KEY.to_string(),
            "id".to_string(),
        )])),
        Field::new("__repark_dup_3_x", DataType::Int64, true),
    ]);
    assert_eq!(
        recorded_display_names(&recorded),
        Some(names(&["id", "__repark_dup_3_x"]))
    );
}

#[test]
fn the_display_header_option_round_trips_any_name() {
    let header = names(&["id", "", "a,b", "q\"t", "\u{e9}", "id"]);
    let sql = TextWriteSpec::options_sql("UTC", None, None, None, Some(&header));
    let encoded = sql
        .split("'repark.text.display_header_hex' '")
        .nth(1)
        .and_then(|rest| rest.strip_suffix('\''))
        .unwrap();
    let options = options_map(&[
        ("repark.text.zone", "UTC"),
        ("repark.text.display_header_hex", encoded),
        ("format.has_header", "true"),
    ]);
    let (spec, rest) = TextWriteSpec::from_format_options(&options).unwrap();
    assert_eq!(spec.display_header, Some(header));
    assert_eq!(rest, options_map(&[("format.has_header", "true")]));
    let plain = options_map(&[("repark.text.zone", "UTC")]);
    let (spec, _) = TextWriteSpec::from_format_options(&plain).unwrap();
    assert_eq!(spec.display_header, None);
    let broken = options_map(&[
        ("repark.text.zone", "UTC"),
        ("repark.text.display_header_hex", "zz"),
    ]);
    assert!(TextWriteSpec::from_format_options(&broken).is_err());
}

#[tokio::test]
async fn rename_gives_unique_engine_names_and_keeps_a_plain_frame() {
    let (session, _) = write_session("dup-bucket");
    let frame = self_join(&session).await;
    let engine = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        engine,
        names(&[
            "__repark_dup_0_id",
            "__repark_dup_1_s",
            "__repark_dup_2_v",
            "__repark_dup_3_id",
            "__repark_dup_4_s",
            "__repark_dup_5_v"
        ])
    );
    assert_eq!(
        recorded_display_names(frame.schema().inner()),
        Some(names(&["id", "s", "v", "id", "s", "v"]))
    );
    session
        .create_or_replace_temp_view_from("dup_view", &frame)
        .unwrap();
    let plain = session.sql("SELECT * FROM l").await.unwrap();
    let before = plain.logical_plan().clone();
    let kept = rename_duplicate_tolerant(plain, &names(&["id", "s", "v"])).unwrap();
    assert_eq!(kept.logical_plan(), &before);
}

fn recorded(name: &str, display: &str) -> Field {
    Field::new(name, DataType::Int64, true).with_metadata(HashMap::from([(
        DISPLAY_NAME_KEY.to_string(),
        display.to_string(),
    )]))
}

#[test]
fn parts_route_recorded_csv_to_the_sink_and_leave_the_rest() {
    let duplicate = Arc::new(Schema::new(vec![
        recorded("x0", "id"),
        recorded("x1", "id"),
    ]));
    let options = HashMap::new();
    let csv = build_text_write_copy_parts(&duplicate, "v", "UTC", &options, &[], "CSV").unwrap();
    assert_eq!(csv.select_sql, "SELECT * FROM v");
    assert_eq!(csv.stored_as, "repark_text_csv");
    assert_eq!(
        csv.spec_options_sql,
        "'repark.text.zone' 'UTC', 'repark.text.display_header_hex' '6964,6964'"
    );
    let json = build_text_write_copy_parts(&duplicate, "v", "UTC", &options, &[], "JSON").unwrap();
    assert_eq!(json.stored_as, "JSON");
    assert!(json.spec_options_sql.is_empty());
    let shaped = Arc::new(Schema::new(vec![
        Field::new("__repark_dup_0_id", DataType::Int64, true),
        Field::new("__repark_dup_7_", DataType::Int64, true),
    ]));
    let untouched = build_text_write_copy_parts(&shaped, "v", "UTC", &options, &[], "CSV").unwrap();
    assert_eq!(untouched.stored_as, "CSV");
    assert!(untouched.spec_options_sql.is_empty());
    let temporal = Arc::new(Schema::new(vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    )]));
    let formatted =
        build_text_write_copy_parts(&temporal, "v", "UTC", &options, &[], "CSV").unwrap();
    assert_eq!(formatted.stored_as, "repark_text_csv");
    assert_eq!(formatted.spec_options_sql, "'repark.text.zone' 'UTC'");
}

#[test]
fn display_header_renames_the_mapped_fields_only() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("x0", DataType::Int64, true),
        Field::new("__repark_dup_9_s", DataType::Utf8, false),
        Field::new("x2", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Int64Array::from(vec![2])),
        ],
    )
    .unwrap();
    let header = HashMap::from([
        ("x0".to_string(), "id".to_string()),
        ("x2".to_string(), "id".to_string()),
    ]);
    let shown = with_display_header(&batch, &header).unwrap();
    let written = shown
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(written, names(&["id", "__repark_dup_9_s", "id"]));
    assert!(!shown.schema().field(1).is_nullable());
    assert_eq!(shown.columns(), batch.columns());
}

async fn csv_text(
    session: &ReparkSession,
    memory: &InMemory,
    frame: &DataFrame,
    cell: &str,
) -> String {
    session
        .write_path(
            frame,
            &format!("s3://dup-bucket/{cell}"),
            "csv",
            "error",
            &options_map(&[("header", "true")]),
            &[],
        )
        .await
        .unwrap();
    let parts = written_parts(memory, cell).await;
    assert_eq!(parts.len(), 1, "{parts:?}");
    parts[0].1.clone()
}

#[tokio::test]
async fn csv_keeps_a_user_column_named_like_a_generated_one() {
    let (session, memory) = write_session("dup-bucket");
    session.context().register_batch("l", left_batch()).unwrap();
    let unique = session
        .sql("SELECT id AS \"__repark_dup_0_id\", s AS \"__repark_dup_7_\", v FROM l ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        csv_text(&session, &memory, &unique, "cell/unique").await,
        "__repark_dup_0_id,__repark_dup_7_,v\n1,a,10\n2,b,20\n"
    );
    let mixed = session
        .sql("SELECT a.id, b.id, a.v AS \"__repark_dup_0_id\" FROM l a JOIN l b ON a.id = b.id ORDER BY a.id")
        .await
        .unwrap();
    let mixed =
        rename_duplicate_tolerant(mixed, &names(&["id", "id", "__repark_dup_0_id"])).unwrap();
    assert_eq!(
        csv_text(&session, &memory, &mixed, "cell/mixed").await,
        "id,id,__repark_dup_0_id\n1,1,10\n2,2,20\n"
    );
}

#[tokio::test]
async fn csv_writes_the_duplicate_display_header() {
    let (session, memory) = write_session("dup-bucket");
    let frame = self_join(&session).await;
    session
        .write_path(
            &frame,
            "s3://dup-bucket/cell/header",
            "csv",
            "error",
            &options_map(&[("header", "true")]),
            &[],
        )
        .await
        .unwrap();
    let parts = written_parts(&memory, "cell/header").await;
    assert_eq!(parts.len(), 1, "{parts:?}");
    assert_eq!(parts[0].1, "id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n");
}

#[tokio::test]
async fn csv_without_header_writes_rows_only() {
    let (session, memory) = write_session("dup-bucket");
    let frame = self_join(&session).await;
    session
        .write_path(
            &frame,
            "s3://dup-bucket/cell/rows",
            "csv",
            "error",
            &options_map(&[("header", "false")]),
            &[],
        )
        .await
        .unwrap();
    let parts = written_parts(&memory, "cell/rows").await;
    assert_eq!(parts.len(), 1, "{parts:?}");
    assert_eq!(parts[0].1, "1,a,10,1,a,10\n2,b,20,2,b,20\n");
}

#[tokio::test]
async fn csv_separator_applies_to_the_display_header() {
    let (session, memory) = write_session("dup-bucket");
    let frame = self_join(&session).await;
    session
        .write_path(
            &frame,
            "s3://dup-bucket/cell/sep",
            "csv",
            "error",
            &options_map(&[("header", "true"), ("sep", "|")]),
            &[],
        )
        .await
        .unwrap();
    let parts = written_parts(&memory, "cell/sep").await;
    assert_eq!(parts[0].1, "id|s|v|id|s|v\n1|a|10|1|a|10\n2|b|20|2|b|20\n");
}

#[tokio::test]
async fn csv_partition_on_a_unique_name_drops_it_from_the_header() {
    let (session, memory) = write_session("dup-bucket");
    let frame = mixed_join(&session).await;
    session
        .write_path(
            &frame,
            "s3://dup-bucket/cell/part",
            "csv",
            "error",
            &options_map(&[("header", "true")]),
            &names(&["t"]),
        )
        .await
        .unwrap();
    let parts = written_parts(&memory, "cell/part").await;
    assert_eq!(parts.len(), 1, "{parts:?}");
    assert!(parts[0].0.starts_with("cell/part/t=x/"), "{}", parts[0].0);
    assert_eq!(parts[0].1, "id,s,v,id\n1,a,10,1\n");
}

#[tokio::test]
async fn csv_empty_duplicate_frame_writes_the_display_header() {
    let (session, memory) = write_session("dup-bucket");
    session.context().register_batch("l", left_batch()).unwrap();
    let joined = session
        .sql("SELECT a.id, a.s, b.id, b.s FROM l a JOIN l b ON a.id = b.id WHERE a.id > 99")
        .await
        .unwrap();
    let frame = rename_duplicate_tolerant(joined, &names(&["id", "s", "id", "s"])).unwrap();
    session
        .write_path(
            &frame,
            "s3://dup-bucket/cell/empty",
            "csv",
            "error",
            &options_map(&[("header", "true")]),
            &[],
        )
        .await
        .unwrap();
    let parts = written_parts(&memory, "cell/empty").await;
    assert_eq!(parts.len(), 1, "{parts:?}");
    assert_eq!(parts[0].1, "id,s,id,s\n");
}

#[tokio::test]
async fn csv_duplicate_header_keeps_temporal_formatting() {
    let (session, memory) = write_session("dup-bucket");
    let micros = 1_704_164_645_000_000_i64;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC")),
        ],
    )
    .unwrap();
    session.context().register_batch("t", batch).unwrap();
    let joined = session
        .sql("SELECT a.id, a.ts, b.id, b.ts FROM t a JOIN t b ON a.id = b.id")
        .await
        .unwrap();
    let frame = rename_duplicate_tolerant(joined, &names(&["id", "ts", "id", "ts"])).unwrap();
    session
        .write_path(
            &frame,
            "s3://dup-bucket/cell/temporal",
            "csv",
            "error",
            &options_map(&[("header", "true")]),
            &[],
        )
        .await
        .unwrap();
    let parts = written_parts(&memory, "cell/temporal").await;
    assert_eq!(
        parts[0].1,
        "id,ts,id,ts\n1,2024-01-02T03:04:05.000Z,1,2024-01-02T03:04:05.000Z\n"
    );
}
