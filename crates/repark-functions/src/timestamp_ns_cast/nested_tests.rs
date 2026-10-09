use std::str::FromStr;

use datafusion::arrow::array::{
    Array, Int32Array, ListArray, MapArray, StringArray, StructArray, TimestampMicrosecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{Fields, TimestampNanosecondType};

use super::nested::{conform, conformed_type};
use super::*;

const INSTANT_MICROS: i64 = 1_767_323_045_123_456;
const NEW_YORK_WALL: i64 = 1_767_305_045_123_456_000;
const FAR_MICROS: i64 = 32_503_680_000_000_001;

fn conversion(zone: &str, ansi: bool) -> Conversion {
    Conversion {
        zoned: false,
        zone: Tz::from_str(zone).unwrap(),
        ansi,
    }
}

fn instant() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

fn wall_ns() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

fn pair(first: (&str, DataType, bool), second: (&str, DataType, bool)) -> Fields {
    Fields::from(vec![
        Field::new(first.0, first.1, first.2),
        Field::new(second.0, second.1, second.2),
    ])
}

fn instants(values: &[i64], nullable: bool) -> StructArray {
    let stamps = TimestampMicrosecondArray::from(values.to_vec()).with_timezone("UTC");
    let counts = Int32Array::from(vec![1; values.len()]);
    StructArray::new(
        pair(("v", instant(), nullable), ("n", DataType::Int32, true)),
        vec![Arc::new(stamps), Arc::new(counts)],
        None,
    )
}

fn leaf(array: &ArrayRef) -> Vec<Option<i64>> {
    match array.data_type() {
        DataType::Struct(_) => leaf(array.as_struct().column(0)),
        DataType::List(_) => leaf(array.as_list::<i32>().values()),
        DataType::Map(_, _) => leaf(array.as_map().entries().column(1)),
        _ => array
            .as_primitive::<TimestampNanosecondType>()
            .iter()
            .collect(),
    }
}

fn shape_of(value: &DataType) -> DataType {
    match value {
        DataType::Struct(_) => {
            DataType::Struct(pair(("v", wall_ns(), true), ("n", DataType::Int32, true)))
        }
        DataType::List(field) => {
            let element = field.as_ref().clone();
            let conformed = shape_of(element.data_type());
            DataType::List(Arc::new(element.with_data_type(conformed)))
        }
        DataType::Map(entries, sorted) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return value.clone();
            };
            let item = fields[1].as_ref().clone();
            let conformed = shape_of(item.data_type());
            let pairs = Fields::from(vec![
                Arc::clone(&fields[0]),
                Arc::new(item.with_data_type(conformed)),
            ]);
            let renamed = Field::new("key_value", DataType::Struct(pairs), false);
            DataType::Map(Arc::new(renamed), *sorted)
        }
        other => other.clone(),
    }
}

fn conformed(value: &ArrayRef, zone: &str, ansi: bool) -> Result<ArrayRef> {
    let out = conformed_type(value.data_type(), &shape_of(value.data_type()), false);
    conform(&conversion(zone, ansi), value, &out)
}

fn list_of(values: ArrayRef) -> ArrayRef {
    let field = Arc::new(Field::new("element", values.data_type().clone(), true));
    let offsets = OffsetBuffer::from_lengths([values.len()]);
    Arc::new(ListArray::new(field, offsets, values, None))
}

fn map_of(values: ArrayRef) -> ArrayRef {
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["k"; values.len()]));
    let fields = pair(
        ("key", DataType::Utf8, false),
        ("value", values.data_type().clone(), true),
    );
    let entries = StructArray::new(fields.clone(), vec![keys, values], None);
    let field = Arc::new(Field::new("entries", DataType::Struct(fields), false));
    let offsets = OffsetBuffer::from_lengths([entries.len()]);
    Arc::new(MapArray::new(field, offsets, entries, None, false))
}

#[test]
fn a_nested_instant_takes_the_session_wall_at_any_depth() {
    let row: ArrayRef = Arc::new(instants(&[INSTANT_MICROS], true));
    let shapes = [
        Arc::clone(&row),
        list_of(Arc::clone(&row)),
        map_of(Arc::clone(&row)),
        map_of(list_of(Arc::clone(&row))),
        list_of(map_of(row)),
    ];
    for value in shapes {
        let stored = conformed(&value, "America/New_York", true).unwrap();
        assert_eq!(
            leaf(&stored),
            vec![Some(NEW_YORK_WALL)],
            "{}",
            value.data_type()
        );
        let utc = conformed(&value, "UTC", true).unwrap();
        assert_eq!(leaf(&utc), vec![Some(INSTANT_MICROS * 1_000)]);
    }
}

#[test]
fn a_nested_overflow_answers_as_the_leaf_does() {
    let value: ArrayRef = Arc::new(instants(&[FAR_MICROS, INSTANT_MICROS], false));
    let stored = conformed(&value, "America/New_York", false).unwrap();
    assert_eq!(leaf(&stored), vec![None, Some(NEW_YORK_WALL)]);
    let refused = conformed(&value, "America/New_York", true).unwrap_err();
    assert!(refused.to_string().contains("[CAST_OVERFLOW]"), "{refused}");
}

#[test]
fn nested_fields_pair_by_name_and_else_by_position() {
    let target = DataType::Struct(pair(("v", wall_ns(), true), ("n", DataType::Int32, true)));
    let reordered = DataType::Struct(pair(("n", DataType::Int32, true), ("v", instant(), true)));
    assert_eq!(
        conformed_type(&reordered, &target, false),
        DataType::Struct(pair(("n", DataType::Int32, true), ("v", wall_ns(), true)))
    );
    let renamed = DataType::Struct(pair(("a", instant(), false), ("b", DataType::Int32, true)));
    assert_eq!(
        conformed_type(&renamed, &target, false),
        DataType::Struct(pair(("a", wall_ns(), true), ("b", DataType::Int32, true)))
    );
    let half = DataType::Struct(pair(("v", instant(), true), ("w", instant(), true)));
    let mixed = DataType::Struct(pair(("v", wall_ns(), true), ("w", instant(), true)));
    assert_eq!(conformed_type(&half, &mixed, false), mixed);
    let zoned = DataType::Struct(pair(
        (
            "v",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            true,
        ),
        ("n", DataType::Int32, true),
    ));
    assert_eq!(conformed_type(&half, &zoned, false), half);
}

#[test]
fn the_return_field_is_nullable_where_an_overflow_answers_null() {
    let udf = timestamp_ns_cast_udf(false);
    let nullable = |source: DataType| {
        let field = Arc::new(Field::new("x", source, false));
        udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[field],
            scalar_arguments: &[None],
        })
        .unwrap()
        .is_nullable()
    };
    assert!(nullable(instant()));
    assert!(nullable(DataType::Date32));
    assert!(nullable(DataType::Timestamp(TimeUnit::Microsecond, None)));
    assert!(!nullable(wall_ns()));
}
