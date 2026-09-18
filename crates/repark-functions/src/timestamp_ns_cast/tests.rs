use std::str::FromStr;

use datafusion::arrow::array::{StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::TimestampNanosecondType;
use datafusion::logical_expr::{LogicalPlanBuilder, lit};

use super::*;

fn conversion(zoned: bool, zone: &str, ansi: bool) -> Conversion {
    Conversion {
        zoned,
        zone: Tz::from_str(zone).unwrap(),
        ansi,
    }
}

fn nanos(array: &ArrayRef) -> Vec<Option<i64>> {
    array
        .as_primitive::<TimestampNanosecondType>()
        .iter()
        .collect()
}

fn strings(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

#[test]
fn strings_keep_all_nine_fraction_digits() {
    let out = conversion(false, "UTC", true)
        .convert(&strings(&[
            "2026-01-02 03:04:05.123456789",
            "2026-01-03 00:00:00.000000001",
            "2026-01-02 03:04:05",
        ]))
        .unwrap();
    assert_eq!(
        out.data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert_eq!(
        nanos(&out),
        vec![
            Some(1_767_323_045_123_456_789),
            Some(1_767_398_400_000_000_001),
            Some(1_767_323_045_000_000_000)
        ]
    );
}

#[test]
fn zoned_strings_honour_an_offset_and_else_the_session_zone() {
    let out = conversion(true, "America/Los_Angeles", true)
        .convert(&strings(&[
            "2026-01-02 03:04:05.123456789+01:00",
            "2026-01-02 03:04:05.123456789",
        ]))
        .unwrap();
    assert_eq!(
        out.data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
    );
    assert_eq!(
        nanos(&out),
        vec![
            Some(1_767_319_445_123_456_789),
            Some(1_767_351_845_123_456_789)
        ]
    );
}

#[test]
fn malformed_strings_raise_under_ansi_and_are_null_without() {
    let error = conversion(false, "UTC", true)
        .convert(&strings(&["not a time"]))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("[CAST_INVALID_INPUT]") && error.contains("\"TIMESTAMP_NS\""),
        "{error}"
    );
    let out = conversion(true, "UTC", false)
        .convert(&strings(&["not a time"]))
        .unwrap();
    assert_eq!(nanos(&out), vec![None]);
}

#[test]
fn microsecond_instants_widen_exactly() {
    let micros: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(1_767_484_799_999_999), None])
            .with_timezone("UTC"),
    );
    let zoned = conversion(true, "America/Los_Angeles", true)
        .convert(&micros)
        .unwrap();
    assert_eq!(nanos(&zoned), vec![Some(1_767_484_799_999_999_000), None]);
    let naive = conversion(false, "UTC", true).convert(&micros).unwrap();
    assert_eq!(nanos(&naive), vec![Some(1_767_484_799_999_999_000), None]);
}

#[test]
fn an_instant_into_a_naive_target_is_the_session_wall() {
    let micros: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(1_767_484_799_999_999)]).with_timezone("UTC"),
    );
    let out = conversion(false, "America/Los_Angeles", true)
        .convert(&micros)
        .unwrap();
    assert_eq!(nanos(&out), vec![Some(1_767_455_999_999_999_000)]);
}

#[test]
fn a_wall_into_a_zoned_target_is_localized() {
    let wall: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(
        1_767_323_045_000_000,
    )]));
    let out = conversion(true, "America/Los_Angeles", true)
        .convert(&wall)
        .unwrap();
    assert_eq!(nanos(&out), vec![Some(1_767_351_845_000_000_000)]);
}

#[test]
fn out_of_range_widening_overflows_loud_under_ansi() {
    let micros: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(i64::MAX / 10)]));
    let error = conversion(false, "UTC", true)
        .convert(&micros)
        .unwrap_err()
        .to_string();
    assert!(error.contains("[CAST_OVERFLOW]"), "{error}");
    let out = conversion(false, "UTC", false).convert(&micros).unwrap();
    assert_eq!(nanos(&out), vec![None]);
}

#[test]
fn non_temporal_sources_refuse_at_planning() {
    let error = SparkTimestampNsCast::new(false)
        .return_type(&[DataType::Int64])
        .unwrap_err()
        .to_string();
    assert!(error.contains("CAST_WITHOUT_SUGGESTION"), "{error}");
}

#[test]
fn values_retype_microsecond_columns_and_cast_mixed_ones() {
    let micro = lit(ScalarValue::TimestampMicrosecond(
        Some(1_767_484_799_999_999),
        Some(Arc::from("UTC")),
    ));
    let naive_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let schema = DFSchema::try_from(datafusion::arrow::datatypes::Schema::new(vec![
        Field::new("a", naive_ns.clone(), false),
        Field::new("b", naive_ns.clone(), false),
    ]))
    .unwrap();
    let ns_value = timestamp_ns_cast_expr(lit("2026-01-02 03:04:05.123456789"), false);
    let plan = LogicalPlan::Values(Values {
        schema: Arc::new(schema),
        values: vec![
            vec![micro.clone(), micro.clone()],
            vec![micro.clone(), ns_value],
        ],
    });
    let LogicalPlan::Values(conformed) = conform_values_timestamp_columns(plan).unwrap() else {
        panic!("values stay values");
    };
    assert_eq!(
        conformed.schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
    );
    assert_eq!(conformed.schema.field(1).data_type(), &naive_ns);
    assert!(is_timestamp_ns_cast(&conformed.values[0][1]));
    assert!(!is_timestamp_ns_cast(&conformed.values[0][0]));
    let untouched = LogicalPlanBuilder::empty(false).build().unwrap();
    assert!(matches!(
        conform_values_timestamp_columns(untouched).unwrap(),
        LogicalPlan::EmptyRelation(_)
    ));
}

fn micros(array: &ArrayRef) -> Vec<Option<i64>> {
    array
        .as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>()
        .iter()
        .collect()
}

#[test]
fn narrowing_floors_instants_to_microseconds() {
    let instants: ArrayRef = Arc::new(
        datafusion::arrow::array::TimestampNanosecondArray::from(vec![
            Some(1_767_323_045_123_456_789),
            Some(-1),
            None,
        ])
        .with_timezone("UTC"),
    );
    let out = narrow(&instants, Tz::from_str("America/New_York").unwrap()).unwrap();
    assert_eq!(
        out.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
    );
    assert_eq!(
        micros(&out),
        vec![Some(1_767_323_045_123_456), Some(-1), None]
    );
}

#[test]
fn narrowing_localizes_walls_in_the_session_zone() {
    let walls: ArrayRef = Arc::new(datafusion::arrow::array::TimestampNanosecondArray::from(
        vec![Some(1_767_323_045_123_456_789), Some(-1)],
    ));
    let utc = narrow(&walls, Tz::from_str("UTC").unwrap()).unwrap();
    assert_eq!(micros(&utc), vec![Some(1_767_323_045_123_456), Some(-1)]);
    let new_york = narrow(&walls, Tz::from_str("America/New_York").unwrap()).unwrap();
    assert_eq!(
        micros(&new_york),
        vec![Some(1_767_341_045_123_456), Some(17_999_999_999)]
    );
}

#[test]
fn same_kind_instants_widen_and_overflow_like_the_row_path() {
    let instants: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(-1), None, Some(i64::MAX / 10)])
            .with_timezone("UTC"),
    );
    let error = conversion(true, "UTC", true)
        .convert(&instants)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("[CAST_OVERFLOW]") && error.contains(&(i64::MAX / 10).to_string()),
        "{error}"
    );
    let out = conversion(true, "UTC", false).convert(&instants).unwrap();
    assert_eq!(
        out.data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
    );
    assert_eq!(nanos(&out), vec![Some(-1_000), None, None]);
}

#[test]
fn values_without_ns_columns_are_left_alone() {
    let micro = lit(ScalarValue::TimestampMicrosecond(
        Some(1_767_484_799_999_999),
        Some(Arc::from("UTC")),
    ));
    let schema = DFSchema::try_from(datafusion::arrow::datatypes::Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new(
            "b",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            false,
        ),
    ]))
    .unwrap();
    let values = Values {
        schema: Arc::new(schema),
        values: vec![vec![lit(1_i64), micro]],
    };
    assert!(values_actions(&values).is_empty());
}

#[test]
fn narrowing_a_microsecond_input_keeps_its_ticks() {
    let instants: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(vec![Some(-1), None]).with_timezone("UTC"));
    let out = narrow(&instants, Tz::from_str("America/New_York").unwrap()).unwrap();
    assert_eq!(micros(&out), vec![Some(-1), None]);
    let walls: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(-1)]));
    let out = narrow(&walls, Tz::from_str("America/New_York").unwrap()).unwrap();
    assert_eq!(micros(&out), vec![Some(17_999_999_999)]);
}
