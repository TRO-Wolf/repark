use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use repark_connect::postgres::PostgresMapping;

use crate::Error;
use crate::session::ReparkSessionBuilder;
use crate::session::write_postgres::{
    PostgresWrite, PostgresWritePath, PostgresWriteReport, PostgresWriteTarget, ShapedColumn,
    execute_postgres_write, record_postgres_write_report, shape_batch, take_postgres_write_report,
};

const REFUSED_URL: &str = "postgresql://127.0.0.1:1/postgres";

fn frame() -> datafusion::prelude::DataFrame {
    SessionContext::new()
        .read_empty()
        .expect("an empty frame plans")
}

fn properties(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn url_write(dbtable: &str, props: BTreeMap<String, String>) -> PostgresWrite {
    PostgresWrite {
        target: PostgresWriteTarget::Url {
            url: REFUSED_URL.to_string(),
            dbtable: dbtable.to_string(),
            properties: props,
        },
        columns: None,
        case_insensitive: false,
        path: PostgresWritePath::Bulk,
    }
}

fn catalogs() -> crate::catalog_state::CatalogRegistry {
    ReparkSessionBuilder::default()
        .build()
        .expect("a session builds")
        .catalogs_snapshot()
}

#[tokio::test]
async fn an_unknown_mounted_source_refuses_before_any_connection() {
    let write = PostgresWrite {
        target: PostgresWriteTarget::Mounted {
            source: "nosuch".to_string(),
            schema: "public".to_string(),
            table: "t".to_string(),
        },
        columns: None,
        case_insensitive: false,
        path: PostgresWritePath::Bulk,
    };
    let error = execute_postgres_write(&catalogs(), frame(), write, "UTC")
        .await
        .expect_err("an unknown source refuses");
    assert!(matches!(error, DataFusionError::Plan(_)), "{error:?}");
    assert!(
        error
            .to_string()
            .contains("unknown database source `nosuch`")
    );
}

#[tokio::test]
async fn a_non_postgres_mount_refuses() {
    let (directory, path) = {
        let directory = tempfile::TempDir::new().expect("config fixture directory");
        let path = directory.path().join("repark.toml");
        std::fs::write(
            &path,
            "[default.database.sqlserver.ms_db]\nhost = \"203.0.113.1\"\n",
        )
        .expect("fixture file");
        (directory, path)
    };
    let session = ReparkSessionBuilder::default()
        .from_config_file(Some(path))
        .build()
        .expect("file-built session");
    session
        .register_configured_sources()
        .expect("source registration");
    let _directory = directory;
    let write = PostgresWrite {
        target: PostgresWriteTarget::Mounted {
            source: "ms_db".to_string(),
            schema: "dbo".to_string(),
            table: "t".to_string(),
        },
        columns: None,
        case_insensitive: false,
        path: PostgresWritePath::Bulk,
    };
    let error = execute_postgres_write(&session.catalogs_snapshot(), frame(), write, "UTC")
        .await
        .expect_err("a SQL Server mount refuses");
    assert!(
        error.to_string().contains("not a Postgres source"),
        "{error}"
    );
}

#[tokio::test]
async fn an_unknown_property_refuses_as_config_naming_the_source() {
    let props = properties(&[
        ("user", "postgres"),
        ("password", "secret-test"),
        ("sslmode", "disable"),
        ("bogus", "1"),
    ]);
    let error = execute_postgres_write(&catalogs(), frame(), url_write("public.t", props), "UTC")
        .await
        .expect_err("an unknown key refuses");
    let message = error.to_string();
    assert!(message.contains("bogus"), "{message}");
    assert!(message.contains("database source `jdbc`"), "{message}");
    assert!(!message.contains("secret-test"), "{message}");
    let folded = crate::error_map::engine_err(error);
    assert!(matches!(folded, Error::Config(_)), "{folded:?}");
}

#[tokio::test]
async fn an_unreachable_host_refuses_as_operational_without_the_password() {
    let props = properties(&[
        ("user", "postgres"),
        ("password", "secret-test"),
        ("sslmode", "disable"),
    ]);
    let error = execute_postgres_write(&catalogs(), frame(), url_write("public.t", props), "UTC")
        .await
        .expect_err("a refused port refuses");
    let message = error.to_string();
    assert!(message.contains("database source `jdbc`"), "{message}");
    assert!(!message.contains("secret-test"), "{message}");
    let folded = crate::error_map::engine_err(error);
    assert!(matches!(folded, Error::DataFusion(_)), "{folded:?}");
}

#[tokio::test]
async fn partition_options_on_a_write_are_ignored_not_refused() {
    let props = properties(&[
        ("user", "postgres"),
        ("password", "postgres"),
        ("sslmode", "disable"),
        ("partitionColumn", "id"),
        ("lowerBound", "1"),
        ("upperBound", "9"),
        ("numPartitions", "4"),
        ("predicates", "id > 1"),
    ]);
    let error = execute_postgres_write(&catalogs(), frame(), url_write("public.t", props), "UTC")
        .await
        .expect_err("a refused port refuses");
    let message = error.to_string().to_lowercase();
    assert!(!message.contains("partitioncolumn"), "{message}");
    assert!(!message.contains("predicates"), "{message}");
    assert!(!message.contains("unknown"), "{message}");
    assert!(!message.contains("declared"), "{message}");
}

#[test]
fn write_path_option_defaults_to_bulk_and_names_both_values_on_refusal() {
    use crate::session::write_postgres::{PostgresWritePath, parse_write_path_option};
    assert_eq!(
        parse_write_path_option(None).expect("absent means bulk"),
        PostgresWritePath::Bulk
    );
    assert_eq!(
        parse_write_path_option(Some("bulk")).expect("bulk parses"),
        PostgresWritePath::Bulk
    );
    assert_eq!(
        parse_write_path_option(Some("ROW")).expect("row parses case-insensitively"),
        PostgresWritePath::Row
    );
    let error = parse_write_path_option(Some("columnar")).expect_err("a bad value refuses");
    assert!(
        matches!(error, DataFusionError::Configuration(_)),
        "{error:?}"
    );
    let message = error.to_string();
    assert!(message.contains("'bulk'"), "{message}");
    assert!(message.contains("'row'"), "{message}");
}

#[test]
fn last_write_report_records_on_builder_sessions_and_nowhere_else() {
    let session = ReparkSessionBuilder::default()
        .build()
        .expect("a session builds");
    let context = session.context();
    assert!(take_postgres_write_report(context).is_none());
    record_postgres_write_report(
        context,
        PostgresWriteReport {
            path: PostgresWritePath::Row,
            rows: 7,
            fallback: Some("the target is a view, which COPY cannot write".to_string()),
        },
    );
    let taken = take_postgres_write_report(context).expect("a recorded report reads back once");
    assert_eq!(taken.path, PostgresWritePath::Row);
    assert_eq!(taken.rows, 7);
    assert_eq!(
        taken.fallback.as_deref(),
        Some("the target is a view, which COPY cannot write")
    );
    assert!(take_postgres_write_report(context).is_none());
    let bare = SessionContext::new();
    record_postgres_write_report(
        &bare,
        PostgresWriteReport {
            path: PostgresWritePath::Bulk,
            rows: 1,
            fallback: None,
        },
    );
    assert!(take_postgres_write_report(&bare).is_none());
}

fn shaped_one(mapping: PostgresMapping, expected: DataType) -> Vec<ShapedColumn> {
    vec![ShapedColumn::for_test("v", mapping, expected)]
}

fn single_batch(array: ArrayRef) -> RecordBatch {
    let field = Field::new("v", array.data_type().clone(), true);
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array])
        .expect("a single-column batch builds")
}

fn shape_single(
    mapping: PostgresMapping,
    expected: DataType,
    array: ArrayRef,
) -> Result<RecordBatch, DataFusionError> {
    shape_batch(
        &single_batch(array),
        &shaped_one(mapping, expected),
        false,
        "UTC",
        "pg",
    )
}

fn utf8(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn int64(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn float64(values: &[f64]) -> ArrayRef {
    Arc::new(Float64Array::from(values.to_vec()))
}

fn decimal(values: &[i128], precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(precision, scale)
            .expect("a valid decimal width"),
    )
}

fn overflow_error(
    mapping: PostgresMapping,
    expected: DataType,
    array: ArrayRef,
) -> DataFusionError {
    shape_single(mapping, expected, array).expect_err("an unstorable value refuses")
}

fn assert_overflow(
    mapping: PostgresMapping,
    expected: DataType,
    array: ArrayRef,
    from: &str,
    to: &str,
) {
    let error = overflow_error(mapping, expected, array);
    let DataFusionError::Execution(message) = error else {
        panic!("a cast refusal is an execution error, got {error:?}");
    };
    assert!(
        message.starts_with("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
        "{message}"
    );
    assert!(message.contains("`v`"), "{message}");
    assert!(message.contains(from), "{message}");
    assert!(message.contains(to), "{message}");
    assert!(!message.contains("Cast error"), "{message}");
    assert!(!message.contains("Invalid argument"), "{message}");
}

fn assert_invalid_input(
    mapping: PostgresMapping,
    expected: DataType,
    array: ArrayRef,
    shown: &str,
    from: &str,
    to: &str,
) {
    let error = overflow_error(mapping, expected, array);
    let DataFusionError::Execution(message) = error else {
        panic!("a cast refusal is an execution error, got {error:?}");
    };
    assert!(
        message.starts_with("column `v`: [CAST_INVALID_INPUT]"),
        "{message}"
    );
    assert!(message.contains(shown), "{message}");
    assert!(message.contains(from), "{message}");
    assert!(message.contains(to), "{message}");
    assert!(!message.contains("Cast error"), "{message}");
}

#[test]
fn integer_overflows_refuse_with_the_overflow_class() {
    assert_overflow(
        PostgresMapping::Int32,
        DataType::Int32,
        int64(&[3_000_000_000]),
        "BIGINT",
        "INT",
    );
    assert_overflow(
        PostgresMapping::Int16,
        DataType::Int16,
        int64(&[40_000]),
        "BIGINT",
        "SMALLINT",
    );
    assert_overflow(
        PostgresMapping::Int64,
        DataType::Int64,
        float64(&[1e19]),
        "DOUBLE",
        "BIGINT",
    );
    assert_overflow(
        PostgresMapping::Int32,
        DataType::Int32,
        float64(&[f64::NAN]),
        "DOUBLE",
        "INT",
    );
}

#[test]
fn malformed_strings_refuse_with_the_invalid_input_class() {
    assert_invalid_input(
        PostgresMapping::Int32,
        DataType::Int32,
        utf8(&["abc"]),
        "'abc'",
        "STRING",
        "INT",
    );
    assert_invalid_input(
        PostgresMapping::Int32,
        DataType::Int32,
        utf8(&[" 12 "]),
        "' 12 '",
        "STRING",
        "INT",
    );
    assert_invalid_input(
        PostgresMapping::Date,
        DataType::Date32,
        utf8(&["garbage"]),
        "'garbage'",
        "STRING",
        "DATE",
    );
    assert_invalid_input(
        PostgresMapping::Date,
        DataType::Date32,
        utf8(&["2024-02-30"]),
        "'2024-02-30'",
        "STRING",
        "DATE",
    );
    assert_invalid_input(
        PostgresMapping::Boolean,
        DataType::Boolean,
        utf8(&["maybe"]),
        "'maybe'",
        "STRING",
        "BOOLEAN",
    );
}

#[test]
fn decimal_overflows_refuse_with_the_overflow_class() {
    assert_overflow(
        PostgresMapping::Numeric(repark_connect::postgres::DecimalTarget::UNCONSTRAINED),
        DataType::Decimal128(10, 2),
        decimal(&[123_456_789_012_345], 14, 3),
        "DECIMAL(14,3)",
        "DECIMAL(10,2)",
    );
    assert_overflow(
        PostgresMapping::Numeric(repark_connect::postgres::DecimalTarget::UNCONSTRAINED),
        DataType::Decimal128(5, 0),
        decimal(&[999_995], 6, 1),
        "DECIMAL(6,1)",
        "DECIMAL(5,0)",
    );
    assert_overflow(
        PostgresMapping::Numeric(repark_connect::postgres::DecimalTarget::UNCONSTRAINED),
        DataType::Decimal128(38, 18),
        decimal(&[12_345_678_901_234_567_890_123_456_789_012_345_678], 38, 0),
        "DECIMAL(38,0)",
        "DECIMAL(38,18)",
    );
    assert_overflow(
        PostgresMapping::Numeric(repark_connect::postgres::DecimalTarget::UNCONSTRAINED),
        DataType::Decimal128(10, 2),
        float64(&[f64::NAN]),
        "DOUBLE",
        "DECIMAL(10,2)",
    );
    assert_overflow(
        PostgresMapping::Numeric(repark_connect::postgres::DecimalTarget::UNCONSTRAINED),
        DataType::Decimal128(10, 2),
        float64(&[f64::INFINITY]),
        "DOUBLE",
        "DECIMAL(10,2)",
    );
}

#[test]
fn invalid_bytes_into_text_refuse_naming_the_bytes() {
    let bytes: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![Some(&[0xFF, 0xFE][..])]));
    assert_invalid_input(
        PostgresMapping::Utf8,
        DataType::Utf8,
        bytes,
        "X'FFFE'",
        "BINARY",
        "STRING",
    );
}

#[test]
fn a_numeric_string_too_large_for_the_column_is_overflow_not_malformed() {
    assert_overflow(
        PostgresMapping::Numeric(repark_connect::postgres::DecimalTarget::UNCONSTRAINED),
        DataType::Decimal128(5, 0),
        utf8(&["999999.99"]),
        "STRING",
        "DECIMAL(5,0)",
    );
    assert_invalid_input(
        PostgresMapping::Numeric(repark_connect::postgres::DecimalTarget::UNCONSTRAINED),
        DataType::Decimal128(5, 0),
        utf8(&["abc"]),
        "'abc'",
        "STRING",
        "DECIMAL(5,0)",
    );
}

#[test]
fn a_batch_names_its_leftmost_bad_value() {
    assert_invalid_input(
        PostgresMapping::Int32,
        DataType::Int32,
        utf8(&["1", "abc", "xyz"]),
        "'abc'",
        "STRING",
        "INT",
    );
    assert_invalid_input(
        PostgresMapping::Int32,
        DataType::Int32,
        utf8(&["1", "2", "xyz"]),
        "'xyz'",
        "STRING",
        "INT",
    );
}

#[test]
fn a_long_bad_value_is_capped_in_the_message() {
    let long = "z".repeat(300);
    let error = overflow_error(
        PostgresMapping::Int32,
        DataType::Int32,
        utf8(&[long.as_str()]),
    );
    let message = error.to_string();
    assert!(message.contains('…'), "{message}");
    assert!(!message.contains(long.as_str()), "{message}");
}

#[test]
fn fitting_values_cast_exactly_as_before() {
    let cases: Vec<(PostgresMapping, DataType, ArrayRef, &str)> = vec![
        (
            PostgresMapping::Int32,
            DataType::Int32,
            utf8(&["12"]),
            "PrimitiveArray<Int32>\n[\n  12,\n]",
        ),
        (
            PostgresMapping::Boolean,
            DataType::Boolean,
            utf8(&["yes"]),
            "BooleanArray\n[\n  true,\n]",
        ),
        (
            PostgresMapping::Int32,
            DataType::Int32,
            float64(&[2.7]),
            "PrimitiveArray<Int32>\n[\n  2,\n]",
        ),
        (
            PostgresMapping::Int32,
            DataType::Int32,
            float64(&[2.5]),
            "PrimitiveArray<Int32>\n[\n  2,\n]",
        ),
        (
            PostgresMapping::Int32,
            DataType::Int32,
            decimal(&[25], 2, 1),
            "PrimitiveArray<Int32>\n[\n  2,\n]",
        ),
        (
            PostgresMapping::Float32,
            DataType::Float32,
            float64(&[1e40]),
            "PrimitiveArray<Float32>\n[\n  inf,\n]",
        ),
        (
            PostgresMapping::Float32,
            DataType::Float32,
            float64(&[1e-46]),
            "PrimitiveArray<Float32>\n[\n  0.0,\n]",
        ),
        (
            PostgresMapping::Numeric(repark_connect::postgres::DecimalTarget::UNCONSTRAINED),
            DataType::Decimal128(10, 2),
            float64(&[1.005]),
            "PrimitiveArray<Decimal128(10, 2)>\n[\n  100,\n]",
        ),
        (
            PostgresMapping::Numeric(repark_connect::postgres::DecimalTarget::UNCONSTRAINED),
            DataType::Decimal128(5, 0),
            decimal(&[5], 2, 1),
            "PrimitiveArray<Decimal128(5, 0)>\n[\n  1,\n]",
        ),
        (
            PostgresMapping::Int32,
            DataType::Int32,
            Arc::new(BooleanArray::from(vec![true])),
            "PrimitiveArray<Int32>\n[\n  1,\n]",
        ),
        (
            PostgresMapping::Boolean,
            DataType::Boolean,
            int64(&[1]),
            "BooleanArray\n[\n  true,\n]",
        ),
        (
            PostgresMapping::Utf8,
            DataType::Utf8,
            decimal(&[150], 3, 2),
            "StringArray\n[\n  \"1.50\",\n]",
        ),
    ];
    for (mapping, expected, array, rendered) in cases {
        let shaped = shape_single(mapping, expected, array).expect("a fitting value stores");
        assert_eq!(shaped.num_rows(), 1);
        assert_eq!(format!("{:?}", shaped.column(0)), rendered);
    }
}

#[test]
fn nulls_still_store_as_null() {
    let nulls: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    let shaped =
        shape_single(PostgresMapping::Int32, DataType::Int32, nulls).expect("a null stores");
    assert_eq!(shaped.num_rows(), 1);
    assert!(shaped.column(0).is_null(0));
    let nulls: ArrayRef = Arc::new(StringArray::from(vec![None as Option<&str>]));
    let shaped =
        shape_single(PostgresMapping::Int32, DataType::Int32, nulls).expect("a null stores");
    assert!(shaped.column(0).is_null(0));
}

#[test]
fn garbage_into_a_timestamp_column_refuses_while_fitting_text_parses() {
    assert_invalid_input(
        PostgresMapping::Timestamp,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        utf8(&["garbage"]),
        "'garbage'",
        "STRING",
        "TIMESTAMP_NTZ",
    );
    let shaped = shape_single(
        PostgresMapping::Timestamp,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        utf8(&["2024-07-01 12:00:00"]),
    )
    .expect("a fitting timestamp string parses");
    assert_eq!(shaped.num_rows(), 1);
}
