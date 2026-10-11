use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float64Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::error::DataFusionError;
use repark_connect::postgres::PostgresMapping;

use crate::session::write_postgres::{ShapedColumn, shape_batch};

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
    shape_single_zone(mapping, expected, array, "UTC")
}

fn shape_single_zone(
    mapping: PostgresMapping,
    expected: DataType,
    array: ArrayRef,
    zone: &str,
) -> Result<RecordBatch, DataFusionError> {
    shape_batch(
        &single_batch(array),
        &shaped_one(mapping, expected),
        false,
        zone,
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
fn fitting_values_cast_exactly_as_before() {
    let cases: Vec<(PostgresMapping, DataType, ArrayRef, &str)> = vec![
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
}

fn int32(values: &[i32]) -> ArrayRef {
    Arc::new(arrow::array::Int32Array::from(values.to_vec()))
}

fn int16(values: &[i16]) -> ArrayRef {
    Arc::new(arrow::array::Int16Array::from(values.to_vec()))
}

fn float32(values: &[f32]) -> ArrayRef {
    Arc::new(arrow::array::Float32Array::from(values.to_vec()))
}

fn boolean(values: &[bool]) -> ArrayRef {
    Arc::new(BooleanArray::from(values.to_vec()))
}

fn date32(values: &[i32]) -> ArrayRef {
    Arc::new(arrow::array::Date32Array::from(values.to_vec()))
}

fn untyped_null() -> ArrayRef {
    arrow::array::new_null_array(&DataType::Null, 1)
}

fn assert_store_refusal(
    mapping: PostgresMapping,
    expected: DataType,
    array: ArrayRef,
    source_needle: &str,
    target_needle: &str,
) {
    let error = shape_single(mapping, expected, array).expect_err("a cross-family cast refuses");
    let DataFusionError::Plan(message) = error else {
        panic!("a store-assignment refusal is a plan error");
    };
    assert!(
        message.contains("INSERT INTO cannot store-assign column `v`"),
        "{message}"
    );
    assert!(message.contains("not ANSI-store-assignable"), "{message}");
    assert!(
        message.contains("INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST"),
        "{message}"
    );
    assert!(message.contains(source_needle), "{message}");
    assert!(message.contains(target_needle), "{message}");
}

#[test]
fn cross_family_sources_refuse_with_the_store_assignment_class() {
    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
    let zoned = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let cases: Vec<(PostgresMapping, DataType, ArrayRef, &str, &str)> = vec![
        (
            PostgresMapping::Date,
            DataType::Date32,
            int64(&[20_240_701]),
            "Int64",
            "Date32",
        ),
        (
            PostgresMapping::Timestamp,
            naive.clone(),
            int64(&[12_345]),
            "Int64",
            "Timestamp",
        ),
        (
            PostgresMapping::Timestamptz,
            zoned.clone(),
            int64(&[12_345]),
            "Int64",
            "Timestamp",
        ),
        (
            PostgresMapping::Binary,
            DataType::Binary,
            int64(&[12]),
            "Int64",
            "Binary",
        ),
        (
            PostgresMapping::Int32,
            DataType::Int32,
            date32(&[19_905]),
            "Date32",
            "Int32",
        ),
        (
            PostgresMapping::Int64,
            DataType::Int64,
            naive_ts(&[1_719_806_400_000_000]),
            "Timestamp",
            "Int64",
        ),
        (
            PostgresMapping::Numeric(repark_connect::postgres::DecimalTarget::UNCONSTRAINED),
            DataType::Decimal128(10, 2),
            utf8(&[""]),
            "Utf8",
            "Decimal128",
        ),
        (
            PostgresMapping::Boolean,
            DataType::Boolean,
            int64(&[2]),
            "Int64",
            "Boolean",
        ),
        (
            PostgresMapping::Boolean,
            DataType::Boolean,
            float64(&[0.5]),
            "Float64",
            "Boolean",
        ),
        (
            PostgresMapping::Int32,
            DataType::Int32,
            boolean(&[true]),
            "Boolean",
            "Int32",
        ),
        (
            PostgresMapping::Float64,
            DataType::Float64,
            boolean(&[true]),
            "Boolean",
            "Float64",
        ),
        (
            PostgresMapping::ServerText,
            DataType::Utf8,
            int64(&[12]),
            "Int64",
            "interval",
        ),
        (
            PostgresMapping::Int32,
            DataType::Int32,
            utf8(&["12"]),
            "Utf8",
            "Int32",
        ),
        (
            PostgresMapping::Date,
            DataType::Date32,
            utf8(&["2024-07-01"]),
            "Utf8",
            "Date32",
        ),
        (
            PostgresMapping::Boolean,
            DataType::Boolean,
            utf8(&["true"]),
            "Utf8",
            "Boolean",
        ),
        (
            PostgresMapping::Timestamp,
            naive,
            utf8(&["2024-07-01 12:00:00"]),
            "Utf8",
            "Timestamp",
        ),
        (
            PostgresMapping::Numeric(repark_connect::postgres::DecimalTarget::UNCONSTRAINED),
            DataType::Decimal128(10, 2),
            utf8(&["1.5"]),
            "Utf8",
            "Decimal128",
        ),
        (
            PostgresMapping::Timestamptz,
            zoned,
            utf8(&["2024-07-01 12:00:00+00"]),
            "Utf8",
            "Timestamp",
        ),
        (
            PostgresMapping::Binary,
            DataType::Binary,
            utf8(&["abc"]),
            "Utf8",
            "Binary",
        ),
    ];
    for (mapping, expected, array, source_needle, target_needle) in cases {
        assert_store_refusal(mapping, expected, array, source_needle, target_needle);
    }
}

#[test]
fn server_text_columns_take_only_strings_and_null() {
    for array in [
        utf8(&["1 day"]),
        Arc::new(arrow::array::LargeStringArray::from(vec!["1 day"])),
        Arc::new(arrow::array::StringViewArray::from(vec!["1 day"])),
        untyped_null(),
    ] {
        let shaped = shape_single(PostgresMapping::ServerText, DataType::Utf8, array)
            .expect("a string or null stores into an interval column");
        assert_eq!(shaped.num_rows(), 1);
    }
    for array in [
        int64(&[12]),
        float64(&[1.5]),
        boolean(&[true]),
        date32(&[19_905]),
        naive_ts(&[1_719_806_400_000_000]),
        decimal(&[150], 3, 2),
        Arc::new(BinaryArray::from_opt_vec(vec![Some(&[0x31][..])])),
    ] {
        assert_store_refusal(
            PostgresMapping::ServerText,
            DataType::Utf8,
            array,
            "is not ANSI-store-assignable",
            "interval",
        );
    }
}

#[test]
fn untyped_null_stores_into_every_column_type() {
    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
    let zoned = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let unconstrained = repark_connect::postgres::DecimalTarget::UNCONSTRAINED;
    let cases: Vec<(PostgresMapping, DataType)> = vec![
        (PostgresMapping::Boolean, DataType::Boolean),
        (PostgresMapping::Int16, DataType::Int16),
        (PostgresMapping::Int32, DataType::Int32),
        (PostgresMapping::Int64, DataType::Int64),
        (PostgresMapping::Float32, DataType::Float32),
        (PostgresMapping::Float64, DataType::Float64),
        (PostgresMapping::Utf8, DataType::Utf8),
        (PostgresMapping::Binary, DataType::Binary),
        (
            PostgresMapping::Numeric(unconstrained),
            DataType::Decimal128(10, 2),
        ),
        (PostgresMapping::Date, DataType::Date32),
        (PostgresMapping::Timestamp, naive),
        (PostgresMapping::Timestamptz, zoned),
        (PostgresMapping::Uuid, DataType::Utf8),
        (PostgresMapping::Json, DataType::Utf8),
        (PostgresMapping::Jsonb, DataType::Utf8),
        (PostgresMapping::ServerText, DataType::Utf8),
    ];
    for (mapping, expected) in cases {
        let shaped = shape_single(mapping, expected, untyped_null()).expect("a null stores");
        assert_eq!(shaped.num_rows(), 1);
        assert!(shaped.column(0).is_null(0));
    }
}

#[test]
fn assignable_widening_and_narrowing_still_store() {
    let unconstrained = repark_connect::postgres::DecimalTarget::UNCONSTRAINED;
    let cases: Vec<(PostgresMapping, DataType, ArrayRef, &str)> = vec![
        (
            PostgresMapping::Int32,
            DataType::Int32,
            int16(&[7]),
            "PrimitiveArray<Int32>\n[\n  7,\n]",
        ),
        (
            PostgresMapping::Int16,
            DataType::Int16,
            int32(&[7]),
            "PrimitiveArray<Int16>\n[\n  7,\n]",
        ),
        (
            PostgresMapping::Int64,
            DataType::Int64,
            int32(&[-7]),
            "PrimitiveArray<Int64>\n[\n  -7,\n]",
        ),
        (
            PostgresMapping::Int32,
            DataType::Int32,
            int64(&[-7]),
            "PrimitiveArray<Int32>\n[\n  -7,\n]",
        ),
        (
            PostgresMapping::Numeric(unconstrained),
            DataType::Decimal128(10, 2),
            int32(&[12]),
            "PrimitiveArray<Decimal128(10, 2)>\n[\n  1200,\n]",
        ),
        (
            PostgresMapping::Float64,
            DataType::Float64,
            float32(&[1.5]),
            "PrimitiveArray<Float64>\n[\n  1.5,\n]",
        ),
        (
            PostgresMapping::Boolean,
            DataType::Boolean,
            boolean(&[true]),
            "BooleanArray\n[\n  true,\n]",
        ),
        (
            PostgresMapping::Utf8,
            DataType::Utf8,
            utf8(&["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]),
            "StringArray\n[\n  \"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\",\n]",
        ),
    ];
    for (mapping, expected, array, rendered) in cases {
        let shaped = shape_single(mapping, expected, array).expect("an assignable value stores");
        assert_eq!(shaped.num_rows(), 1);
        assert_eq!(format!("{:?}", shaped.column(0)), rendered);
    }
}

fn instant_micros(rfc3339: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .expect("a valid instant")
        .timestamp_micros()
}

fn wall_micros(text: &str) -> i64 {
    chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .expect("a valid wall clock")
        .and_utc()
        .timestamp_micros()
}

fn zoned_ts(micros: &[i64]) -> ArrayRef {
    Arc::new(arrow::array::TimestampMicrosecondArray::from(micros.to_vec()).with_timezone("UTC"))
}

fn naive_ts(micros: &[i64]) -> ArrayRef {
    Arc::new(arrow::array::TimestampMicrosecondArray::from(
        micros.to_vec(),
    ))
}

fn shaped_text(shaped: &RecordBatch) -> Vec<Option<String>> {
    let column = shaped
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a text column");
    (0..column.len())
        .map(|index| {
            if column.is_null(index) {
                None
            } else {
                Some(column.value(index).to_string())
            }
        })
        .collect()
}

fn shaped_days(shaped: &RecordBatch) -> Vec<Option<i32>> {
    let column = shaped
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Date32Array>()
        .expect("a date column");
    (0..column.len())
        .map(|index| {
            if column.is_null(index) {
                None
            } else {
                Some(column.value(index))
            }
        })
        .collect()
}

fn days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
    chrono::NaiveDate::from_ymd_opt(year, month, day)
        .expect("a valid date")
        .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch"))
        .num_days()
        .try_into()
        .expect("a date near the epoch")
}

#[test]
fn zoned_timestamps_into_a_date_column_take_the_session_zone_day() {
    let instant = instant_micros("2024-07-02T03:30:00Z");
    for (zone, year, month, day) in [
        ("UTC", 2024, 7, 2),
        ("America/New_York", 2024, 7, 1),
        ("Asia/Kolkata", 2024, 7, 2),
    ] {
        let shaped = shape_single_zone(
            PostgresMapping::Date,
            DataType::Date32,
            zoned_ts(&[instant]),
            zone,
        )
        .expect("an instant dates");
        assert_eq!(
            shaped_days(&shaped),
            vec![Some(days_since_epoch(year, month, day))],
            "{zone}"
        );
    }
}

#[test]
fn zoned_timestamps_into_a_text_column_render_the_session_wall() {
    let instant = instant_micros("2024-07-02T03:30:00Z");
    for (zone, text) in [
        ("UTC", "2024-07-02 03:30:00"),
        ("America/New_York", "2024-07-01 23:30:00"),
        ("Asia/Kolkata", "2024-07-02 09:00:00"),
    ] {
        let shaped = shape_single_zone(
            PostgresMapping::Utf8,
            DataType::Utf8,
            zoned_ts(&[instant]),
            zone,
        )
        .expect("an instant renders");
        assert_eq!(shaped_text(&shaped), vec![Some(text.to_string())], "{zone}");
    }
}

#[test]
fn text_rendering_trims_the_fraction_like_spark() {
    let walls = [
        "2024-07-01 12:00:00",
        "2024-07-01 12:00:00.123456",
        "2024-07-01 12:00:00.123000",
        "2024-07-01 12:00:00.000001",
    ];
    let micros: Vec<i64> = walls.iter().map(|wall| wall_micros(wall)).collect();
    let shaped = shape_single(PostgresMapping::Utf8, DataType::Utf8, naive_ts(&micros))
        .expect("walls render");
    assert_eq!(
        shaped_text(&shaped),
        vec![
            Some("2024-07-01 12:00:00".to_string()),
            Some("2024-07-01 12:00:00.123456".to_string()),
            Some("2024-07-01 12:00:00.123".to_string()),
            Some("2024-07-01 12:00:00.000001".to_string()),
        ]
    );
}

#[test]
fn naive_timestamps_ignore_the_session_zone() {
    let micros = wall_micros("2024-07-01 12:00:00");
    for zone in ["UTC", "America/New_York", "Asia/Kolkata"] {
        let shaped = shape_single_zone(
            PostgresMapping::Utf8,
            DataType::Utf8,
            naive_ts(&[micros]),
            zone,
        )
        .expect("a wall renders");
        assert_eq!(
            shaped_text(&shaped),
            vec![Some("2024-07-01 12:00:00".to_string())],
            "{zone}"
        );
        let shaped = shape_single_zone(
            PostgresMapping::Date,
            DataType::Date32,
            naive_ts(&[micros]),
            zone,
        )
        .expect("a wall dates");
        assert_eq!(
            shaped_days(&shaped),
            vec![Some(days_since_epoch(2024, 7, 1))],
            "{zone}"
        );
    }
}

#[test]
fn text_rendering_pads_the_year_to_four_digits() {
    let shaped = shape_single(
        PostgresMapping::Utf8,
        DataType::Utf8,
        naive_ts(&[wall_micros("0001-01-01 00:00:00")]),
    )
    .expect("year one renders");
    assert_eq!(
        shaped_text(&shaped),
        vec![Some("0001-01-01 00:00:00".to_string())]
    );
}

#[test]
fn null_timestamps_stay_null_in_date_and_text_columns() {
    let nulls: ArrayRef = Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![None]));
    let shaped = shape_single(PostgresMapping::Utf8, DataType::Utf8, Arc::clone(&nulls))
        .expect("a null renders");
    assert_eq!(shaped_text(&shaped), vec![None]);
    let shaped =
        shape_single(PostgresMapping::Date, DataType::Date32, nulls).expect("a null dates");
    assert_eq!(shaped_days(&shaped), vec![None]);
}

#[test]
fn timestamp_and_timestamptz_columns_keep_their_placement() {
    let instant = instant_micros("2024-07-02T03:30:00Z");
    let wall = wall_micros("2024-07-01 23:30:00");
    let shaped = shape_single_zone(
        PostgresMapping::Timestamp,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        zoned_ts(&[instant]),
        "America/New_York",
    )
    .expect("an instant places");
    let placed = shaped
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .expect("a timestamp column");
    assert_eq!(placed.value(0), wall);
    let shaped = shape_single_zone(
        PostgresMapping::Timestamp,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        naive_ts(&[wall]),
        "America/New_York",
    )
    .expect("a wall passes through");
    let placed = shaped
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .expect("a timestamp column");
    assert_eq!(placed.value(0), wall);
    let shaped = shape_single_zone(
        PostgresMapping::Timestamptz,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        zoned_ts(&[instant]),
        "America/New_York",
    )
    .expect("an instant passes through");
    let placed = shaped
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .expect("a timestamptz column");
    assert_eq!(placed.value(0), instant);
}

#[test]
fn a_wall_past_the_calendar_refuses_named() {
    let error = shape_single(PostgresMapping::Utf8, DataType::Utf8, naive_ts(&[i64::MAX]))
        .expect_err("an unformattable wall refuses");
    let message = error.to_string();
    assert!(
        message.contains("[CAST_OVERFLOW_IN_TABLE_INSERT]"),
        "{message}"
    );
    assert!(message.contains("`v`"), "{message}");
}
