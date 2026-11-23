use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::parquet::data_type::{Int32Type, Int64Type, Int96, Int96Type};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::writer::SerializedFileWriter;
use datafusion::parquet::schema::parser::parse_message_type;
use datafusion::prelude::SessionConfig;

use super::with_int96_as_instants;
use crate::session::ReparkSession;

const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const INSTANTS: [i64; 3] = [
    1_736_902_800_000_000,
    32_503_672_800_123_456,
    -310_415_399_750_000,
];

fn int96_of(micros: i64) -> Int96 {
    let days = micros.div_euclid(MICROS_PER_DAY) + JULIAN_DAY_OF_EPOCH;
    let nanos = micros.rem_euclid(MICROS_PER_DAY) * 1_000;
    let mut value = Int96::new();
    value.set_data(
        u32::try_from(nanos & 0xFFFF_FFFF).expect("low word"),
        u32::try_from(nanos >> 32).expect("high word"),
        u32::try_from(days).expect("julian day"),
    );
    value
}

fn write_int96_file(dir: &Path) {
    let schema = Arc::new(
        parse_message_type(
            "message spark_schema { optional int32 id; optional int96 ts; \
             optional int64 ntz (TIMESTAMP(MICROS,false)); }",
        )
        .expect("the message type"),
    );
    let file = File::create(dir.join("part-0.parquet")).expect("create the file");
    let mut writer =
        SerializedFileWriter::new(file, schema, Arc::new(WriterProperties::builder().build()))
            .expect("a writer");
    let mut group = writer.next_row_group().expect("a row group");
    let ids = [1, 2, 3, 4];
    let levels = [1, 1, 0, 1];
    let mut column = group.next_column().expect("id").expect("id column");
    column
        .typed::<Int32Type>()
        .write_batch(&ids, Some(&[1, 1, 1, 1]), None)
        .expect("write ids");
    column.close().expect("close ids");
    let mut column = group.next_column().expect("ts").expect("ts column");
    let stamps = INSTANTS.map(int96_of);
    column
        .typed::<Int96Type>()
        .write_batch(&stamps, Some(&levels), None)
        .expect("write ts");
    column.close().expect("close ts");
    let mut column = group.next_column().expect("ntz").expect("ntz column");
    column
        .typed::<Int64Type>()
        .write_batch(&INSTANTS, Some(&levels), None)
        .expect("write ntz");
    column.close().expect("close ntz");
    group.close().expect("close the row group");
    writer.close().expect("close the file");
}

async fn micros_column(session: &ReparkSession, query: &str) -> (DataType, Vec<Option<i64>>) {
    let batches = session
        .sql(query)
        .await
        .expect("plan the query")
        .collect()
        .await
        .expect("run the query");
    let mut kind = DataType::Null;
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        kind = column.data_type().clone();
        let values = column
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("microsecond timestamps");
        out.extend(values.iter());
    }
    (kind, out)
}

fn expected() -> Vec<Option<i64>> {
    vec![
        Some(INSTANTS[0]),
        Some(INSTANTS[1]),
        None,
        Some(INSTANTS[2]),
    ]
}

#[test]
fn a_session_config_reads_int96_as_utc_microsecond_instants() {
    let config = with_int96_as_instants(SessionConfig::new());
    let parquet = &config.options().execution.parquet;
    assert_eq!(parquet.coerce_int96.as_deref(), Some("us"));
    assert_eq!(parquet.coerce_int96_tz.as_deref(), Some("UTC"));
}

#[test]
fn an_explicit_int96_setting_is_kept() {
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.coerce_int96 = Some("ms".to_owned());
    config.options_mut().execution.parquet.coerce_int96_tz = Some("Asia/Kolkata".to_owned());
    let config = with_int96_as_instants(config);
    let parquet = &config.options().execution.parquet;
    assert_eq!(parquet.coerce_int96.as_deref(), Some("ms"));
    assert_eq!(parquet.coerce_int96_tz.as_deref(), Some("Asia/Kolkata"));
}

#[tokio::test]
async fn read_parquet_decodes_int96_as_an_instant_and_keeps_int64_ntz() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_int96_file(dir.path());
    let session = ReparkSession::new().expect("a session");
    let frame = session
        .read_parquet(dir.path().to_str().expect("a utf-8 path"))
        .await
        .expect("read the int96 file");
    session
        .context()
        .register_table("int96_read", frame.into_view())
        .expect("register the view");
    let instant = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let wall = DataType::Timestamp(TimeUnit::Microsecond, None);
    let (kind, values) = micros_column(&session, "SELECT ts FROM int96_read ORDER BY id").await;
    assert_eq!((kind, values), (instant, expected()));
    let (kind, values) = micros_column(&session, "SELECT ntz FROM int96_read ORDER BY id").await;
    assert_eq!((kind, values), (wall, expected()));
}

#[tokio::test]
async fn an_external_table_decodes_int96_as_an_instant() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_int96_file(dir.path());
    let session = ReparkSession::new().expect("a session");
    let location = format!("{}/", dir.path().to_str().expect("a utf-8 path"));
    session
        .sql(&format!(
            "CREATE EXTERNAL TABLE int96_ext STORED AS PARQUET LOCATION '{location}'"
        ))
        .await
        .expect("plan the external table")
        .collect()
        .await
        .expect("create the external table");
    let instant = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let (kind, values) = micros_column(&session, "SELECT ts FROM int96_ext ORDER BY id").await;
    assert_eq!((kind, values), (instant, expected()));
}
