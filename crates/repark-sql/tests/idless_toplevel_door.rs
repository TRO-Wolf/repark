use std::collections::HashMap;
use std::fmt::Write;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, Decimal128Array, Int32Array, RecordBatch, StringArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

const UNSET_MAPPING: &str =
    "ALTER TABLE ice.ns.t UNSET TBLPROPERTIES ('schema.name-mapping.default')";
const COUNTS_ONLY: &str = ", 'write.metadata.metrics.default'='counts'";
const NO_IDS_HINT: &str = "the data file has no field ids and the table has no name mapping, so its columns are read by position";

#[derive(Clone, Copy, PartialEq)]
enum Route {
    NoMapping,
    Mapping,
}

struct Doors {
    spark: ReparkSession,
    native: ReparkSession,
    _warehouse: TempDir,
    _source: TempDir,
}

impl Doors {
    fn each(&self) -> [(&'static str, &ReparkSession); 2] {
        [("spark door", &self.spark), ("native door", &self.native)]
    }
}

fn ident() -> TableIdent {
    TableIdent::from_strs(["ns", "t"]).expect("ident")
}

fn bare_session(dialect: Arc<dyn SqlDialect>, spark: bool) -> ReparkSession {
    let mut builder = ReparkSession::builder().with_sql_dialect(dialect);
    if spark {
        builder = builder.with_extension(Arc::new(SparkExtension));
    }
    builder.build().expect("session")
}

async fn adopted_native(warehouse: &Path, metadata_location: &str) -> ReparkSession {
    let handle = repark_iceberg::catalog::memory_catalog(warehouse.to_str().expect("path"))
        .await
        .expect("memory catalog");
    handle
        .create_namespace(&NamespaceIdent::new("ns".to_string()), HashMap::new())
        .await
        .expect("namespace");
    handle
        .register_table(&ident(), metadata_location.to_string())
        .await
        .expect("register table");
    let session = bare_session(Arc::new(AnsiDialect), false);
    session
        .register_iceberg_catalog("ice", handle)
        .await
        .expect("catalog");
    session
}

async fn attempt(session: &ReparkSession, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let frame = session.sql(sql).await.map_err(|error| error.to_string())?;
    frame.collect().await.map_err(|error| error.to_string())
}

async fn run(session: &ReparkSession, sql: &str) {
    attempt(session, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` must succeed: {error}"));
}

fn cell(column: &ArrayRef, row: usize) -> String {
    if column.is_null(row) {
        return "NULL".to_string();
    }
    match column.data_type() {
        DataType::Binary => hex(column.as_binary::<i32>().value(row)),
        DataType::LargeBinary => hex(column.as_binary::<i64>().value(row)),
        DataType::BinaryView => hex(column.as_binary_view().value(row)),
        _ => {
            let text = cast(column, &DataType::Utf8).expect("cell as utf8");
            text.as_string::<i32>().value(row).to_string()
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

fn rendered(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| cell(column, row))
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect()
}

async fn rows(session: &ReparkSession, sql: &str) -> Result<Vec<String>, String> {
    attempt(session, sql)
        .await
        .map(|batches| rendered(&batches))
}

fn write_idless(directory: &Path, batch: &RecordBatch) {
    let path = directory.join("part-00000.parquet");
    let file = File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
    let reader = SerializedFileReader::new(File::open(&path).expect("open")).expect("reader");
    let root = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema();
    assert!(
        root.get_fields()
            .iter()
            .all(|column| !column.get_basic_info().has_id()),
        "the file must embed no field id"
    );
}

async fn doors(columns: &str, batch: &RecordBatch, route: Route) -> Doors {
    doors_with(columns, "", batch, route).await
}

async fn doors_with(columns: &str, properties: &str, batch: &RecordBatch, route: Route) -> Doors {
    let warehouse = TempDir::new().expect("warehouse");
    let source = TempDir::new().expect("source");
    write_idless(source.path(), batch);
    let spark = bare_session(Arc::new(SparkDialect), true);
    spark
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&spark, "CREATE NAMESPACE ice.ns").await;
    run(
        &spark,
        &format!(
            "CREATE TABLE ice.ns.t ({columns}) USING iceberg TBLPROPERTIES ('format-version'='2'{properties})"
        ),
    )
    .await;
    run(
        &spark,
        &format!(
            "CALL ice.system.add_files(table => 'ns.t', source_table => '`parquet`.`{}`')",
            source.path().display()
        ),
    )
    .await;
    if route == Route::NoMapping {
        run(&spark, UNSET_MAPPING).await;
    }
    let catalog = spark
        .catalogs_snapshot()
        .get("ice")
        .expect("ice catalog")
        .clone();
    let table = catalog.load_table(&ident()).await.expect("load table");
    assert_eq!(
        table
            .metadata()
            .properties()
            .contains_key("schema.name-mapping.default"),
        route == Route::Mapping,
        "the name mapping is present only on the mapping route"
    );
    let metadata_location = table.metadata_location().expect("metadata location");
    let native = adopted_native(warehouse.path(), metadata_location).await;
    Doors {
        spark,
        native,
        _warehouse: warehouse,
        _source: source,
    }
}

fn string_file() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, true),
        Field::new("v", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("abc"),
                Some("xyz"),
                None,
                Some("\u{e9}a"),
            ])) as ArrayRef,
        ],
    )
    .expect("string batch")
}

fn reordered_string_file() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Utf8, true),
        Field::new("k", DataType::Int32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("abc"), None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        ],
    )
    .expect("reordered batch")
}

fn binary_file() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, true),
        Field::new("v", DataType::Binary, true),
    ]));
    let values: Vec<Option<&[u8]>> = vec![Some(b"abc"), Some(b"xyz"), None];
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(BinaryArray::from(values)) as ArrayRef,
        ],
    )
    .expect("binary batch")
}

fn wide_decimal_file() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, true),
        Field::new("d", DataType::Decimal128(12, 2), true),
    ]));
    let decimals = Decimal128Array::from(vec![Some(123_456_789_012_i128), Some(12_345_i128)])
        .with_precision_and_scale(12, 2)
        .expect("decimal(12,2)");
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(decimals) as ArrayRef,
        ],
    )
    .expect("decimal batch")
}

fn owned(expected: &[&str]) -> Vec<String> {
    expected.iter().map(ToString::to_string).collect()
}

async fn assert_answers(doors: &Doors, sql: &str, expected: &[&str]) {
    for (door, session) in doors.each() {
        assert_eq!(
            rows(session, sql).await,
            Ok(owned(expected)),
            "{door}: `{sql}`"
        );
    }
}

async fn assert_refuses(doors: &Doors, sql: &str, needles: &[&str]) {
    for (door, session) in doors.each() {
        let outcome = rows(session, sql).await;
        let Err(error) = outcome else {
            panic!("{door}: `{sql}` must refuse, read {outcome:?}");
        };
        assert!(
            error.contains("DataInvalid"),
            "{door}: `{sql}` must refuse with DataInvalid: {error}"
        );
        for needle in needles {
            assert!(
                error.contains(needle),
                "{door}: `{sql}` refusal must name `{needle}`: {error}"
            );
        }
    }
}

fn filter_refusal(
    file_type: &'static str,
    op: &'static str,
    table_type: &'static str,
) -> [String; 3] {
    [
        format!("data file column 'v' at position 1 has type {file_type}"),
        format!("cannot be filtered with `{op}` as table field 'v' (id 2) of type {table_type}"),
        NO_IDS_HINT.to_string(),
    ]
}

async fn assert_filter_refuses(
    doors: &Doors,
    sql: &str,
    file_type: &'static str,
    op: &'static str,
    table_type: &'static str,
) {
    let needles = filter_refusal(file_type, op, table_type);
    let needles: Vec<&str> = needles.iter().map(String::as_str).collect();
    assert_refuses(doors, sql, &needles).await;
}

#[tokio::test]
async fn a_string_column_read_as_binary_reads_its_raw_bytes_through_both_doors() {
    let doors = doors("k INT, v BINARY", &string_file(), Route::NoMapping).await;
    assert_answers(
        &doors,
        "SELECT k, v FROM ice.ns.t ORDER BY k",
        &["1|616263", "2|78797a", "3|NULL", "4|c3a961"],
    )
    .await;
}

#[tokio::test]
async fn a_string_column_read_as_binary_answers_a_null_test_an_inequality_and_a_prefix() {
    let doors = doors("k INT, v BINARY", &string_file(), Route::NoMapping).await;
    assert_answers(
        &doors,
        "SELECT k FROM ice.ns.t WHERE v IS NOT NULL ORDER BY k",
        &["1", "2", "4"],
    )
    .await;
    assert_answers(
        &doors,
        "SELECT k FROM ice.ns.t WHERE v != X'616263' ORDER BY k",
        &["2", "4"],
    )
    .await;
    assert_answers(
        &doors,
        "SELECT k FROM ice.ns.t WHERE v LIKE 'a%' ORDER BY k",
        &["1"],
    )
    .await;
}

#[tokio::test]
async fn a_string_column_read_as_binary_refuses_equality_ordering_and_in_as_equality() {
    let doors = doors("k INT, v BINARY", &string_file(), Route::NoMapping).await;
    assert_filter_refuses(
        &doors,
        "SELECT k FROM ice.ns.t WHERE v = X'616263'",
        "string",
        "=",
        "binary",
    )
    .await;
    assert_filter_refuses(
        &doors,
        "SELECT k FROM ice.ns.t WHERE v < X'62'",
        "string",
        "<",
        "binary",
    )
    .await;
    assert_filter_refuses(
        &doors,
        "SELECT k FROM ice.ns.t WHERE v IN (X'616263', X'78797a')",
        "string",
        "=",
        "binary",
    )
    .await;
}

#[tokio::test]
async fn a_binary_column_read_as_string_answers_null_tests_an_inequality_and_both_prefixes() {
    let doors = doors("k INT, v STRING", &binary_file(), Route::NoMapping).await;
    assert_answers(
        &doors,
        "SELECT k, v FROM ice.ns.t ORDER BY k",
        &["1|abc", "2|xyz", "3|NULL"],
    )
    .await;
    assert_answers(&doors, "SELECT k FROM ice.ns.t WHERE v IS NULL", &["3"]).await;
    assert_answers(
        &doors,
        "SELECT k FROM ice.ns.t WHERE v != 'abc' ORDER BY k",
        &["2"],
    )
    .await;
    assert_answers(&doors, "SELECT k FROM ice.ns.t WHERE v LIKE 'a%'", &["1"]).await;
    assert_answers(
        &doors,
        "SELECT k FROM ice.ns.t WHERE v NOT LIKE 'a%' ORDER BY k",
        &["2"],
    )
    .await;
}

#[tokio::test]
async fn a_binary_column_read_as_string_refuses_equality_and_ordering() {
    let doors = doors("k INT, v STRING", &binary_file(), Route::NoMapping).await;
    assert_filter_refuses(
        &doors,
        "SELECT k FROM ice.ns.t WHERE v = 'abc'",
        "binary",
        "=",
        "string",
    )
    .await;
    assert_filter_refuses(
        &doors,
        "SELECT k FROM ice.ns.t WHERE v > 'b'",
        "binary",
        ">",
        "string",
    )
    .await;
}

#[tokio::test]
async fn an_overflowing_narrowed_decimal_is_loud_under_a_filter() {
    let doors = doors_with(
        "k INT, d DECIMAL(10,2)",
        COUNTS_ONLY,
        &wide_decimal_file(),
        Route::NoMapping,
    )
    .await;
    for sql in [
        "SELECT k, d FROM ice.ns.t",
        "SELECT k FROM ice.ns.t WHERE d > 0",
        "SELECT k FROM ice.ns.t WHERE d IS NOT NULL",
    ] {
        assert_refuses(
            &doors,
            sql,
            &["cannot read a data file column of type Decimal128(12, 2) as Decimal128(10, 2)"],
        )
        .await;
    }
}

#[tokio::test]
async fn the_same_files_with_a_name_mapping_read_the_pair_by_name() {
    let doors_string = doors("k INT, v BINARY", &string_file(), Route::Mapping).await;
    assert_answers(
        &doors_string,
        "SELECT k, v FROM ice.ns.t ORDER BY k",
        &["1|616263", "2|78797a", "3|NULL", "4|c3a961"],
    )
    .await;
    assert_answers(
        &doors_string,
        "SELECT k FROM ice.ns.t WHERE v IS NOT NULL ORDER BY k",
        &["1", "2", "4"],
    )
    .await;
    let doors_binary = doors("k INT, v STRING", &binary_file(), Route::Mapping).await;
    assert_answers(
        &doors_binary,
        "SELECT k, v FROM ice.ns.t ORDER BY k",
        &["1|abc", "2|xyz", "3|NULL"],
    )
    .await;
    assert_answers(
        &doors_binary,
        "SELECT k FROM ice.ns.t WHERE v LIKE 'a%'",
        &["1"],
    )
    .await;
}

#[tokio::test]
async fn a_reordered_same_type_file_reads_by_name_with_a_mapping_control() {
    let doors = doors("k INT, v STRING", &reordered_string_file(), Route::Mapping).await;
    assert_answers(
        &doors,
        "SELECT k, v FROM ice.ns.t ORDER BY k",
        &["1|abc", "2|NULL"],
    )
    .await;
    assert_answers(&doors, "SELECT k FROM ice.ns.t WHERE v = 'abc'", &["1"]).await;
}

#[tokio::test]
async fn a_reordered_file_refuses_by_position_without_a_mapping() {
    let doors = doors(
        "k INT, v STRING",
        &reordered_string_file(),
        Route::NoMapping,
    )
    .await;
    assert_refuses(
        &doors,
        "SELECT k, v FROM ice.ns.t ORDER BY k",
        &[
            "data file column 'v' at position 0 has type string and cannot be read as table field 'k' (id 1) of type int",
            NO_IDS_HINT,
        ],
    )
    .await;
}
