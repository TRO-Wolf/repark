use std::str::FromStr;

use datafusion::arrow::array::{
    Array, FixedSizeListArray, Int32Array, Int64Array, ListArray, MapArray, RunArray, StringArray,
    StructArray, TimestampMicrosecondArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{
    Fields, Int32Type, TimestampNanosecondType, UnionFields, UnionMode,
};

use super::nested::Store;
use super::*;

const INSTANT_MICROS: i64 = 1_767_323_045_123_456;
const NEW_YORK_WALL: i64 = 1_767_305_045_123_456_000;
const FAR_MICROS: i64 = 32_503_680_000_000_001;
const STORE: Store<'static> = Store {
    zoned: false,
    column: "st",
};

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

fn fields(named: &[(&str, DataType, bool)]) -> Fields {
    named
        .iter()
        .map(|(name, data_type, nullable)| Field::new(*name, data_type.clone(), *nullable))
        .collect()
}

fn pair_of(first: &str, second: &str) -> DataType {
    DataType::Struct(fields(&[
        (first, instant(), true),
        (second, instant(), true),
    ]))
}

fn target_pair() -> DataType {
    DataType::Struct(fields(&[("a", wall_ns(), true), ("b", instant(), true)]))
}

fn stamps(values: &[i64]) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(values.to_vec()).with_timezone("UTC"))
}

fn row_of(leaf: ArrayRef, nullable: bool) -> ArrayRef {
    let counts: ArrayRef = Arc::new(Int32Array::from(vec![1; leaf.len()]));
    let shape = fields(&[
        ("v", leaf.data_type().clone(), nullable),
        ("n", DataType::Int32, true),
    ]);
    Arc::new(StructArray::new(shape, vec![leaf, counts], None))
}

fn row_target(required: bool) -> DataType {
    DataType::Struct(fields(&[
        ("v", wall_ns(), !required),
        ("n", DataType::Int32, true),
    ]))
}

fn list_of(values: ArrayRef) -> ArrayRef {
    let field = Arc::new(Field::new("element", values.data_type().clone(), true));
    let offsets = OffsetBuffer::from_lengths([values.len()]);
    Arc::new(ListArray::new(field, offsets, values, None))
}

fn map_of(values: ArrayRef) -> ArrayRef {
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["k"; values.len()]));
    let shape = fields(&[
        ("key", DataType::Utf8, false),
        ("value", values.data_type().clone(), true),
    ]);
    let entries = StructArray::new(shape.clone(), vec![keys, values], None);
    let field = Arc::new(Field::new("entries", DataType::Struct(shape), false));
    let offsets = OffsetBuffer::from_lengths([entries.len()]);
    Arc::new(MapArray::new(field, offsets, entries, None, false))
}

fn target_of(source: &DataType) -> DataType {
    match source {
        DataType::Struct(_) => row_target(false),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            let item = Field::new("element", target_of(field.data_type()), true);
            DataType::List(Arc::new(item))
        }
        DataType::Map(entries, sorted) => {
            let DataType::Struct(pair) = entries.data_type() else {
                return source.clone();
            };
            let item = pair[1].as_ref().clone();
            let conformed = target_of(item.data_type());
            let renamed = Fields::from(vec![
                Arc::clone(&pair[0]),
                Arc::new(item.with_data_type(conformed)),
            ]);
            let field = Field::new("key_value", DataType::Struct(renamed), false);
            DataType::Map(Arc::new(field), *sorted)
        }
        DataType::Dictionary(_, values) => target_of(values),
        DataType::RunEndEncoded(_, values) => target_of(values.data_type()),
        DataType::Timestamp(_, _) => wall_ns(),
        other => other.clone(),
    }
}

fn leaf(array: &ArrayRef) -> Vec<Option<i64>> {
    match array.data_type() {
        DataType::Struct(_) => leaf(array.as_struct().column(0)),
        DataType::List(_) => leaf(array.as_list::<i32>().values()),
        DataType::LargeList(_) => leaf(array.as_list::<i64>().values()),
        DataType::Map(_, _) => leaf(array.as_map().entries().column(1)),
        _ => array
            .as_primitive::<TimestampNanosecondType>()
            .iter()
            .collect(),
    }
}

fn stored(value: &ArrayRef, zone: &str, ansi: bool, pairing: Pairing) -> Result<ArrayRef> {
    let target = target_of(value.data_type());
    let promised = STORE.conformed_type(value.data_type(), &target, pairing)?;
    let out = STORE.conform(&conversion(zone, ansi), value, &target, pairing)?;
    assert_eq!(out.data_type(), &promised, "{}", value.data_type());
    Ok(out)
}

fn refusal(source: &DataType, target: &DataType, pairing: Pairing) -> String {
    STORE
        .conformed_type(source, target, pairing)
        .unwrap_err()
        .to_string()
}

#[test]
fn a_nested_instant_takes_the_session_wall_at_any_depth() {
    let row = row_of(stamps(&[INSTANT_MICROS]), true);
    let shapes = [
        Arc::clone(&row),
        list_of(Arc::clone(&row)),
        map_of(Arc::clone(&row)),
        map_of(list_of(Arc::clone(&row))),
        list_of(map_of(row)),
    ];
    for value in shapes {
        for pairing in [Pairing::Cast, Pairing::Name, Pairing::Exact] {
            let new_york = stored(&value, "America/New_York", true, pairing).unwrap();
            assert_eq!(leaf(&new_york), vec![Some(NEW_YORK_WALL)], "{pairing:?}");
            let utc = stored(&value, "UTC", true, pairing).unwrap();
            assert_eq!(leaf(&utc), vec![Some(INSTANT_MICROS * 1_000)]);
        }
    }
}

#[test]
fn every_list_layout_is_stored_as_a_plain_list() {
    let plain = list_of(stamps(&[INSTANT_MICROS, 5]));
    let item = Arc::new(Field::new("element", instant(), true));
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::new(
        Arc::clone(&item),
        2,
        stamps(&[INSTANT_MICROS, 5]),
        None,
    ));
    let layouts = [
        (Arc::clone(&plain), false),
        (
            cast(&plain, &DataType::LargeList(Arc::clone(&item))).unwrap(),
            true,
        ),
        (
            cast(&plain, &DataType::ListView(Arc::clone(&item))).unwrap(),
            false,
        ),
        (
            cast(&plain, &DataType::LargeListView(Arc::clone(&item))).unwrap(),
            true,
        ),
        (fixed, false),
    ];
    for (value, large) in layouts {
        let out = stored(&value, "America/New_York", true, Pairing::Cast).unwrap();
        assert_eq!(
            matches!(out.data_type(), DataType::LargeList(_)),
            large,
            "{}",
            value.data_type()
        );
        assert!(matches!(
            out.data_type(),
            DataType::List(_) | DataType::LargeList(_)
        ));
        assert_eq!(
            leaf(&out),
            vec![Some(NEW_YORK_WALL), Some(-17_999_999_995_000)],
            "{}",
            value.data_type()
        );
    }
}

#[test]
fn an_encoded_child_is_decoded_and_stored() {
    let plain = stamps(&[INSTANT_MICROS, INSTANT_MICROS]);
    let keyed = DataType::Dictionary(Box::new(DataType::Int32), Box::new(instant()));
    let dictionary = cast(&plain, &keyed).unwrap();
    let ends = Int32Array::from(vec![2]);
    let runs: ArrayRef =
        Arc::new(RunArray::<Int32Type>::try_new(&ends, &stamps(&[INSTANT_MICROS])).unwrap());
    for encoded in [dictionary, runs] {
        let direct = stored(
            &row_of(Arc::clone(&encoded), true),
            "America/New_York",
            true,
            Pairing::Cast,
        );
        assert_eq!(leaf(&direct.unwrap()), vec![Some(NEW_YORK_WALL); 2]);
        let listed = stored(&list_of(encoded), "America/New_York", true, Pairing::Name);
        assert_eq!(leaf(&listed.unwrap()), vec![Some(NEW_YORK_WALL); 2]);
    }
    let whole = row_of(plain, true);
    let keyed_row = DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(whole.data_type().clone()),
    );
    let conformed = STORE
        .conformed_type(&keyed_row, &row_target(false), Pairing::Cast)
        .unwrap();
    assert_eq!(
        conformed,
        DataType::Struct(fields(&[
            ("v", wall_ns(), true),
            ("n", DataType::Int32, true)
        ]))
    );
}

#[test]
fn a_layout_that_cannot_carry_the_leaf_is_refused_by_name() {
    let union = DataType::Union(
        UnionFields::try_new(vec![0], vec![Field::new("x", instant(), true)]).unwrap(),
        UnionMode::Dense,
    );
    let leaf_of = |leaf: DataType| {
        DataType::Struct(fields(&[("v", leaf, true), ("n", DataType::Int32, true)]))
    };
    for (source, reason) in [
        (
            leaf_of(union.clone()),
            "the source layout cannot carry a timestamp",
        ),
        (
            leaf_of(DataType::Int64),
            "the source layout cannot carry a timestamp",
        ),
        (
            leaf_of(DataType::Binary),
            "the source layout cannot carry a timestamp",
        ),
        (
            leaf_of(pair_of("x", "y")),
            "the source nests differently from the target",
        ),
        (instant(), "the source nests differently from the target"),
        (union, "the source layout cannot carry a timestamp"),
    ] {
        let text = refusal(&source, &row_target(false), Pairing::Cast);
        assert!(
            text.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"),
            "{text}"
        );
        assert!(
            text.contains("cannot be stored into this timestamp_ns leaf"),
            "{text}"
        );
        assert!(text.contains(reason), "{source}: {text}");
        assert!(text.contains("`st`"), "{text}");
    }
    let untouched = DataType::Struct(fields(&[("v", DataType::Int64, true)]));
    let zoned_only = DataType::Struct(fields(&[(
        "v",
        DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
        true,
    )]));
    assert_eq!(
        STORE
            .conformed_type(&untouched, &zoned_only, Pairing::Exact)
            .unwrap(),
        untouched
    );
}

fn conformed_first(source: &DataType, pairing: Pairing) -> Result<Vec<bool>> {
    let DataType::Struct(out) = STORE.conformed_type(source, &target_pair(), pairing)? else {
        return Ok(Vec::new());
    };
    Ok(out
        .iter()
        .map(|field| field.data_type() == &wall_ns())
        .collect())
}

#[test]
fn struct_fields_pair_as_the_door_named_in_the_argument_pairs() {
    let in_order = pair_of("a", "b");
    let swapped = pair_of("b", "a");
    let renamed = pair_of("q", "b");
    let recased = pair_of("A", "b");
    let unrelated = pair_of("q", "r");
    let longer = DataType::Struct(fields(&[
        ("q", instant(), true),
        ("b", instant(), true),
        ("c", DataType::Int32, true),
    ]));
    let shorter = DataType::Struct(fields(&[("q", instant(), true)]));
    let only_a = DataType::Struct(fields(&[("a", instant(), true)]));
    let cases: [(&DataType, Pairing, Option<&[bool]>); 20] = [
        (&in_order, Pairing::Cast, Some(&[true, false])),
        (&swapped, Pairing::Cast, Some(&[false, true])),
        (&renamed, Pairing::Cast, Some(&[true, false])),
        (&recased, Pairing::Cast, Some(&[true, false])),
        (&unrelated, Pairing::Cast, Some(&[true, false])),
        (&longer, Pairing::Cast, Some(&[true, false, false])),
        (&shorter, Pairing::Cast, None),
        (&in_order, Pairing::Name, Some(&[true, false])),
        (&swapped, Pairing::Name, Some(&[false, true])),
        (&renamed, Pairing::Name, None),
        (&recased, Pairing::Name, None),
        (&unrelated, Pairing::Name, None),
        (&longer, Pairing::Name, None),
        (&only_a, Pairing::Name, Some(&[true])),
        (&in_order, Pairing::Exact, Some(&[true, false])),
        (&swapped, Pairing::Exact, None),
        (&renamed, Pairing::Exact, None),
        (&longer, Pairing::Exact, None),
        (&only_a, Pairing::Exact, None),
        (&only_a, Pairing::Cast, None),
    ];
    for (source, pairing, expected) in cases {
        let got = conformed_first(source, pairing);
        if let Some(expected) = expected {
            assert_eq!(got.unwrap(), expected.to_vec(), "{pairing:?} {source}");
            continue;
        }
        let text = got.unwrap_err().to_string();
        assert!(text.contains("`st`.`a`"), "{pairing:?} {source}: {text}");
        assert!(
            text.contains("cannot be stored into this timestamp_ns leaf"),
            "{text}"
        );
    }
}

#[test]
fn the_pairing_under_a_list_is_the_cast_the_door_runs_there() {
    let renamed = pair_of("q", "b");
    let item = |inner: DataType| Arc::new(Field::new("element", inner, true));
    let target = DataType::List(item(target_pair()));
    let plain = DataType::List(item(renamed.clone()));
    let large = DataType::LargeList(item(renamed.clone()));
    let view = DataType::ListView(item(renamed));
    assert!(
        STORE
            .conformed_type(&plain, &target, Pairing::Name)
            .is_err()
    );
    assert!(STORE.conformed_type(&view, &target, Pairing::Name).is_err());
    assert!(STORE.conformed_type(&large, &target, Pairing::Name).is_ok());
    assert!(STORE.conformed_type(&plain, &target, Pairing::Cast).is_ok());
    assert!(
        STORE
            .conformed_type(&large, &target, Pairing::Exact)
            .is_err()
    );
    let entries = |inner: DataType| {
        let pair = fields(&[("key", DataType::Utf8, false), ("value", inner, true)]);
        DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(pair), false)),
            false,
        )
    };
    let mapped = entries(pair_of("q", "b"));
    assert!(
        STORE
            .conformed_type(&mapped, &entries(target_pair()), Pairing::Name)
            .is_ok()
    );
    assert!(
        STORE
            .conformed_type(&mapped, &entries(target_pair()), Pairing::Exact)
            .is_err()
    );
}

#[test]
fn a_required_field_by_name_needs_a_source_that_cannot_be_null() {
    let required = DataType::Struct(fields(&[("a", wall_ns(), false), ("b", instant(), true)]));
    let nullable = pair_of("a", "b");
    let exact = DataType::Struct(fields(&[("a", instant(), false), ("b", instant(), true)]));
    assert!(
        STORE
            .conformed_type(&nullable, &required, Pairing::Name)
            .is_err()
    );
    assert_eq!(
        STORE
            .conformed_type(&exact, &required, Pairing::Name)
            .unwrap(),
        DataType::Struct(fields(&[("a", wall_ns(), false), ("b", instant(), true)]))
    );
    assert!(
        STORE
            .conformed_type(&nullable, &required, Pairing::Cast)
            .is_ok()
    );
}

#[test]
fn a_nested_overflow_answers_as_the_leaf_does() {
    let value = row_of(stamps(&[FAR_MICROS, INSTANT_MICROS]), false);
    let out = stored(&value, "America/New_York", false, Pairing::Cast).unwrap();
    assert_eq!(leaf(&out), vec![None, Some(NEW_YORK_WALL)]);
    let refused = stored(&value, "America/New_York", true, Pairing::Cast).unwrap_err();
    assert!(refused.to_string().contains("[CAST_OVERFLOW]"), "{refused}");
}

#[test]
fn a_null_in_a_required_leaf_is_refused_by_name() {
    let target = row_target(true);
    let run = |value: &ArrayRef, ansi: bool| {
        STORE.conform(
            &conversion("America/New_York", ansi),
            value,
            &target,
            Pairing::Cast,
        )
    };
    let overflow = row_of(stamps(&[FAR_MICROS, INSTANT_MICROS]), false);
    let text = run(&overflow, false).unwrap_err().to_string();
    assert!(
        text.contains("the leaf is required and the value is NULL"),
        "{text}"
    );
    assert!(text.contains("`st`.`v`"), "{text}");
    let holes: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(INSTANT_MICROS), None]).with_timezone("UTC"),
    );
    assert!(run(&row_of(holes, true), true).is_err());
    let whole = row_of(stamps(&[INSTANT_MICROS]), false);
    assert_eq!(leaf(&run(&whole, true).unwrap()), vec![Some(NEW_YORK_WALL)]);
}

#[test]
fn a_value_under_a_null_parent_is_not_read() {
    let leaf_values = stamps(&[INSTANT_MICROS, FAR_MICROS]);
    let counts: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let shape = fields(&[("v", instant(), true), ("n", DataType::Int32, true)]);
    let masked = NullBuffer::from(vec![true, false]);
    let value: ArrayRef = Arc::new(StructArray::new(
        shape,
        vec![leaf_values, counts],
        Some(masked),
    ));
    let out = stored(&value, "America/New_York", true, Pairing::Cast).unwrap();
    assert_eq!(leaf(&out), vec![Some(NEW_YORK_WALL), None]);
    assert_eq!(out.null_count(), 1);
}

#[test]
fn a_string_leaf_is_read_as_insert_reads_a_literal() {
    let texts: ArrayRef = Arc::new(StringArray::from(vec![
        "2026-01-02 03:04:05.123456789+00:00",
        "2026-01-02 03:04:05.123456789",
    ]));
    let out = stored(
        &row_of(texts, true),
        "America/New_York",
        true,
        Pairing::Name,
    )
    .unwrap();
    assert_eq!(
        leaf(&out),
        vec![Some(NEW_YORK_WALL + 789), Some(1_767_323_045_123_456_789)]
    );
    let numbers: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    assert!(stored(&row_of(numbers, true), "UTC", true, Pairing::Name).is_err());
}

#[test]
fn the_return_field_is_nullable_where_an_overflow_answers_null() {
    let nullable = |zoned: bool, source: DataType| {
        let field = Arc::new(Field::new("x", source, false));
        timestamp_ns_cast_udf(zoned)
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[field],
                scalar_arguments: &[None],
            })
            .unwrap()
            .is_nullable()
    };
    assert!(nullable(false, instant()));
    assert!(nullable(false, DataType::Date32));
    assert!(nullable(
        false,
        DataType::Timestamp(TimeUnit::Microsecond, None)
    ));
    assert!(!nullable(false, wall_ns()));
    assert!(!nullable(true, instant()));
    assert!(!nullable(
        true,
        DataType::Timestamp(TimeUnit::Microsecond, None)
    ));
    assert!(nullable(true, DataType::Utf8));
}

#[test]
fn the_nested_form_reads_its_pairing_and_column_from_literals() {
    let word = |text: &str| ScalarValue::Utf8(Some(text.to_string()));
    let typed = |source: DataType, pairing: &ScalarValue| {
        let column = word("st");
        let arguments = [
            Arc::new(Field::new("value", source, true)),
            Arc::new(Field::new("shape", target_pair(), true)),
            Arc::new(Field::new("pairing", DataType::Utf8, false)),
            Arc::new(Field::new("column", DataType::Utf8, false)),
        ];
        timestamp_ns_cast_udf(false).return_field_from_args(ReturnFieldArgs {
            arg_fields: &arguments,
            scalar_arguments: &[None, None, Some(pairing), Some(&column)],
        })
    };
    let by_position = typed(pair_of("q", "b"), &word("cast")).unwrap();
    assert_eq!(
        by_position.data_type(),
        &DataType::Struct(fields(&[("q", wall_ns(), true), ("b", instant(), true)]))
    );
    assert!(typed(pair_of("q", "b"), &word("name")).is_err());
    assert!(typed(pair_of("a", "b"), &word("positional")).is_err());
    assert_eq!(Pairing::parse("exact"), Some(Pairing::Exact));
}
