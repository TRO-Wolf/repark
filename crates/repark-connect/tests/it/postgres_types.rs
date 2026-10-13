use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, TimeUnit,
    TimestampMicrosecondType,
};
use repark_common::{Error, ErrorClass};
use repark_connect::postgres::{
    POSTGRES_EPOCH_DAYS, POSTGRES_EPOCH_MICROS, POSTGRES_TYPES, PgTypeKind, PlannedColumn,
    PostgresMapping, PostgresTypeRow, TypeMod, UTC_ZONE_LABEL, postgres_type,
};
use repark_connect::{ConnectError, ProtocolViolation, UNMAPPED_ROW, ValueRefusal};

const THIS_FILE: &str = include_str!("postgres_types.rs");

fn row(postgres_name: &str) -> &'static PostgresTypeRow {
    postgres_type(postgres_name).expect("a row in the Postgres type map")
}

fn round_trip(postgres_name: &str, array: &ArrayRef) -> ArrayRef {
    let row = row(postgres_name);
    assert_eq!(row.mapping.data_type().as_ref(), Some(array.data_type()));
    let wire = row.encode(array.as_ref()).expect("encode to the wire form");
    assert_eq!(wire.len(), array.len());
    let borrowed: Vec<Option<&[u8]>> = wire.iter().map(Option::as_deref).collect();
    let decoded = row.decode(&borrowed).expect("decode from the wire form");
    assert_eq!(decoded.data_type(), array.data_type());
    decoded
}

fn assert_round_trips(postgres_name: &str, array: impl Array + 'static) {
    let array: ArrayRef = Arc::new(array);
    let decoded = round_trip(postgres_name, &array);
    assert_eq!(&decoded, &array);
}

#[test]
fn bool_round_trips() {
    assert_round_trips(
        "bool",
        BooleanArray::from(vec![Some(true), None, Some(false)]),
    );
    let decoded = row("bool")
        .decode(&[Some(&[1]), Some(&[0]), Some(&[2]), None])
        .expect("boolrecv: any nonzero byte is true");
    assert_eq!(
        decoded.as_boolean(),
        &BooleanArray::from(vec![Some(true), Some(false), Some(true), None])
    );
}

#[test]
fn int2_round_trips() {
    assert_round_trips(
        "int2",
        Int16Array::from(vec![
            Some(i16::MIN),
            Some(-1),
            None,
            Some(0),
            Some(i16::MAX),
        ]),
    );
    let wire = row("int2")
        .encode(&Int16Array::from(vec![258]))
        .expect("encode");
    assert_eq!(wire, vec![Some(vec![0x01, 0x02])]);
}

#[test]
fn int4_round_trips() {
    assert_round_trips(
        "int4",
        Int32Array::from(vec![
            Some(i32::MIN),
            Some(-1),
            None,
            Some(0),
            Some(i32::MAX),
        ]),
    );
    let wire = row("int4")
        .encode(&Int32Array::from(vec![-2]))
        .expect("encode");
    assert_eq!(wire, vec![Some(vec![0xff, 0xff, 0xff, 0xfe])]);
}

#[test]
fn int8_round_trips() {
    assert_round_trips(
        "int8",
        Int64Array::from(vec![
            Some(i64::MIN),
            Some(-1),
            None,
            Some(0),
            Some(i64::MAX),
        ]),
    );
    let wire = row("int8")
        .encode(&Int64Array::from(vec![1]))
        .expect("encode");
    assert_eq!(wire, vec![Some(vec![0, 0, 0, 0, 0, 0, 0, 1])]);
}

#[test]
fn float4_round_trips() {
    let values = vec![
        Some(1.5_f32),
        Some(-0.0),
        None,
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        Some(f32::NAN),
        Some(f32::MIN_POSITIVE),
    ];
    let array: ArrayRef = Arc::new(Float32Array::from(values.clone()));
    let decoded = round_trip("float4", &array);
    let decoded = decoded.as_primitive::<Float32Type>();
    let bits = |value: Option<f32>| value.map(f32::to_bits);
    let actual: Vec<Option<u32>> = decoded.iter().map(bits).collect();
    let expected: Vec<Option<u32>> = values.into_iter().map(bits).collect();
    assert_eq!(actual, expected);
    let wire = row("float4")
        .encode(&Float32Array::from(vec![1.5_f32]))
        .expect("encode");
    assert_eq!(wire, vec![Some(vec![0x3f, 0xc0, 0x00, 0x00])]);
    let decoded = row("float4")
        .decode(&[Some(&[0x3f, 0xc0, 0x00, 0x00][..])])
        .expect("decode");
    assert_eq!(
        decoded.as_primitive::<Float32Type>().value(0).to_bits(),
        1.5_f32.to_bits()
    );
}

#[test]
fn float8_round_trips() {
    let values = vec![
        Some(1.5_f64),
        Some(-0.0),
        None,
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::NAN),
        Some(f64::MIN_POSITIVE),
    ];
    let array: ArrayRef = Arc::new(Float64Array::from(values.clone()));
    let decoded = round_trip("float8", &array);
    let decoded = decoded.as_primitive::<Float64Type>();
    let bits = |value: Option<f64>| value.map(f64::to_bits);
    let actual: Vec<Option<u64>> = decoded.iter().map(bits).collect();
    let expected: Vec<Option<u64>> = values.into_iter().map(bits).collect();
    assert_eq!(actual, expected);
    let wire = row("float8")
        .encode(&Float64Array::from(vec![1.5_f64]))
        .expect("encode");
    assert_eq!(
        wire,
        vec![Some(vec![0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])]
    );
    let decoded = row("float8")
        .decode(&[Some(&[0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00][..])])
        .expect("decode");
    assert_eq!(
        decoded.as_primitive::<Float64Type>().value(0).to_bits(),
        1.5_f64.to_bits()
    );
}

fn text_values() -> StringArray {
    StringArray::from(vec![
        Some("company"),
        Some(""),
        None,
        Some("naïve ☃ 東京"),
        Some("trailing   "),
    ])
}

#[test]
fn text_round_trips() {
    assert_round_trips("text", text_values());
}

#[test]
fn varchar_round_trips() {
    assert_round_trips("varchar", text_values());
}

#[test]
fn bpchar_round_trips() {
    assert_round_trips("bpchar", text_values());
    let decoded = row("bpchar")
        .decode(&[Some(b"ab   ".as_slice())])
        .expect("decode");
    assert_eq!(
        decoded.as_string::<i32>(),
        &StringArray::from(vec!["ab   "])
    );
}

#[test]
fn bytea_round_trips() {
    assert_round_trips(
        "bytea",
        BinaryArray::from(vec![
            Some(b"\x00\xff\x10".as_slice()),
            Some(b"".as_slice()),
            None,
            Some(b"\xc3\x28".as_slice()),
        ]),
    );
}

#[test]
fn declared_types_refuse_naming_their_row() {
    let declared: Vec<(&str, &str)> = POSTGRES_TYPES
        .iter()
        .filter_map(|row| match row.mapping {
            PostgresMapping::Declared { registry_row } => Some((row.postgres_name, registry_row)),
            _ => None,
        })
        .collect();
    assert_eq!(declared, vec![("time", "CONNECT-DECL-pg-time")]);
    for (postgres_name, registry_row) in declared {
        let row = row(postgres_name);
        assert_eq!(row.mapping.data_type(), None);
        let expected = ConnectError::Declared {
            postgres_name: row.postgres_name,
            registry_row,
        };
        let decode = row.decode(&[Some(b"x".as_slice())]).expect_err("declared");
        assert_eq!(decode, expected);
        assert!(decode.to_string().contains(registry_row), "{decode}");
        let encode = row
            .encode(&StringArray::from(vec!["x"]))
            .expect_err("declared");
        assert_eq!(encode, expected);
    }
}

#[test]
fn wire_values_of_the_wrong_length_refuse() {
    let error = row("int4")
        .decode(&[Some(&[0, 0, 0, 1]), Some(&[0, 1])])
        .expect_err("short int4");
    assert_eq!(
        error,
        ConnectError::WireLength {
            postgres_name: "int4",
            index: 1,
            expected: 4,
            actual: 2,
        }
    );
    let error = row("bool").decode(&[Some(&[])]).expect_err("empty bool");
    assert!(matches!(
        error,
        ConnectError::WireLength {
            expected: 1,
            actual: 0,
            ..
        }
    ));
}

#[test]
fn text_refuses_invalid_utf8() {
    let error = row("text")
        .decode(&[Some(b"ok".as_slice()), Some(b"\xc3\x28".as_slice())])
        .expect_err("invalid UTF-8");
    assert_eq!(
        error,
        ConnectError::InvalidUtf8 {
            postgres_name: "text",
            index: 1,
        }
    );
}

#[test]
fn encode_refuses_an_arrow_array_of_another_type() {
    let error = row("int4")
        .encode(&Int64Array::from(vec![1]))
        .expect_err("int8 values into int4");
    assert_eq!(
        error,
        ConnectError::ArrowType {
            postgres_name: "int4",
            expected: DataType::Int32,
            actual: DataType::Int64,
        }
    );
    for (postgres_name, expected) in [
        ("bool", DataType::Boolean),
        ("text", DataType::Utf8),
        ("bytea", DataType::Binary),
    ] {
        let error = row(postgres_name)
            .encode(&Int32Array::from(vec![1]))
            .expect_err("wrong Arrow type");
        assert_eq!(
            error,
            ConnectError::ArrowType {
                postgres_name: row(postgres_name).postgres_name,
                expected,
                actual: DataType::Int32,
            }
        );
    }
}

#[test]
fn type_map_has_one_row_per_type_and_a_live_pin_per_row() {
    let mut names: Vec<&str> = POSTGRES_TYPES.iter().map(|row| row.postgres_name).collect();
    let total = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), total, "a Postgres type has two rows");
    for row in POSTGRES_TYPES {
        assert!(
            THIS_FILE.contains(&format!("fn {}()", row.pin)),
            "row `{}` names pin `{}`, which is not a test here",
            row.postgres_name,
            row.pin
        );
    }
    assert_eq!(postgres_type("money"), None);
}

fn column(postgres_name: &str, typmod: TypeMod) -> PlannedColumn {
    PlannedColumn::resolve(
        Arc::from(postgres_name),
        postgres_name,
        PgTypeKind::Base,
        typmod,
    )
    .expect("a mapped column")
}

fn decode_one(column: &PlannedColumn, wire: &[u8]) -> Result<ArrayRef, ConnectError> {
    column.decode(&[Some(wire)])
}

fn refusal(error: &ConnectError) -> ValueRefusal {
    match error {
        ConnectError::UnrepresentableValue { reason, .. } => *reason,
        other => panic!("expected a refused value, got {other:?}"),
    }
}

fn numeric_wire(weight: i16, sign: u16, dscale: u16, digits: &[i16]) -> Vec<u8> {
    let count = i16::try_from(digits.len()).expect("digit count");
    let mut wire = Vec::new();
    for word in [count, weight] {
        wire.extend_from_slice(&word.to_be_bytes());
    }
    wire.extend_from_slice(&sign.to_be_bytes());
    wire.extend_from_slice(&dscale.to_be_bytes());
    for digit in digits {
        wire.extend_from_slice(&digit.to_be_bytes());
    }
    wire
}

fn decimal(column: &PlannedColumn, wire: &[u8]) -> i128 {
    let decoded = decode_one(column, wire).expect("a decimal value");
    decoded.as_primitive::<Decimal128Type>().value(0)
}

#[test]
fn date_anchors_round_trip() {
    let date = column("date", TypeMod::NONE);
    assert_eq!(date.field().data_type(), &DataType::Date32);
    let decoded = date
        .decode(&[
            Some(&[0x00, 0x00, 0x00, 0x00][..]),
            Some(&[0xff, 0xff, 0xd5, 0x33][..]),
            Some(&[0x00, 0x00, 0x22, 0x83][..]),
            None,
        ])
        .expect("finite dates");
    assert_eq!(
        decoded.as_primitive::<Date32Type>(),
        &Date32Array::from(vec![Some(10957), Some(0), Some(19792), None])
    );
    assert_eq!(POSTGRES_EPOCH_DAYS, 10957);
    for wire in [[0x7f, 0xff, 0xff, 0xff], [0x80, 0x00, 0x00, 0x00]] {
        let error = decode_one(&date, &wire).expect_err("an infinite date refuses");
        assert_eq!(refusal(&error), ValueRefusal::InfiniteDate);
        let message = error.to_string();
        assert!(
            message.contains("CONNECT-DECL-pg-infinite-datetime"),
            "{message}"
        );
        assert!(message.contains("column `date`"), "{message}");
    }
    let error = decode_one(&date, &[0x7f, 0xff, 0xd5, 0x33]).expect_err("past Date32");
    assert_eq!(refusal(&error), ValueRefusal::DateOutOfRange);
}

fn assert_timestamp_anchors(postgres_name: &str, zone: Option<&str>) {
    let timestamp = column(postgres_name, TypeMod::NONE);
    assert_eq!(
        timestamp.field().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, zone.map(Arc::from))
    );
    let last_wire = i64::MAX - POSTGRES_EPOCH_MICROS;
    let decoded = timestamp
        .decode(&[
            Some(&[0xff, 0xfc, 0xa2, 0xfe, 0xc4, 0xc8, 0x20, 0x00][..]),
            Some(&[0x00, 0x02, 0xb6, 0x4b, 0xee, 0xe1, 0xd0, 0x00][..]),
            None,
            Some(&last_wire.to_be_bytes()[..]),
        ])
        .expect("finite timestamps");
    let decoded = decoded.as_primitive::<TimestampMicrosecondType>();
    assert_eq!(
        decoded.iter().collect::<Vec<_>>(),
        vec![Some(0), Some(1_710_072_000_000_000), None, Some(i64::MAX)]
    );
    assert_eq!(
        decoded.timezone(),
        zone,
        "{postgres_name} carries the wrong zone"
    );
    let past = (last_wire + 1).to_be_bytes();
    let error = decode_one(&timestamp, &past).expect_err("after 294247-01-10");
    assert_eq!(refusal(&error), ValueRefusal::TimestampOutOfRange);
    assert!(
        error.to_string().contains("CONNECT-DECL-pg-out-of-range"),
        "{error}"
    );
    for wire in [i64::MAX, i64::MIN] {
        let error = decode_one(&timestamp, &wire.to_be_bytes()).expect_err("infinity refuses");
        assert_eq!(refusal(&error), ValueRefusal::InfiniteTimestamp);
        assert!(
            error
                .to_string()
                .contains("CONNECT-DECL-pg-infinite-datetime"),
            "{error}"
        );
    }
}

#[test]
fn timestamp_ntz_anchors_round_trip() {
    assert_timestamp_anchors("timestamp", None);
}

#[test]
fn timestamptz_anchors_round_trip() {
    assert_timestamp_anchors("timestamptz", Some(UTC_ZONE_LABEL));
    assert_eq!(UTC_ZONE_LABEL, "UTC");
}

#[test]
fn numeric_anchors_round_trip() {
    let eight_three = column("numeric", TypeMod::numeric(8, 3));
    assert_eq!(eight_three.field().data_type(), &DataType::Decimal128(8, 3));
    let anchor = [
        0x00, 0x03, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x09, 0x29, 0x1a, 0x7c,
    ];
    assert_eq!(decimal(&eight_three, &anchor), 12_345_678);
    let two_one = column("numeric", TypeMod::numeric(2, 1));
    let anchor = [0x00, 0x01, 0xff, 0xff, 0x40, 0x00, 0x00, 0x01, 0x13, 0x88];
    assert_eq!(decimal(&two_one, &anchor), -5);
    let widest = column("numeric", TypeMod::numeric(38, 0));
    let mut nines = vec![99];
    nines.extend([9999; 9]);
    let maximum = 10_i128.pow(38) - 1;
    assert_eq!(decimal(&widest, &numeric_wire(9, 0, 0, &nines)), maximum);
    assert_eq!(
        decimal(&widest, &numeric_wire(9, 0x4000, 0, &nines)),
        -maximum
    );
    let padded = column("numeric", TypeMod::numeric(10, 4));
    assert_eq!(decimal(&padded, &numeric_wire(0, 0, 1, &[1, 5000])), 15_000);
    assert_eq!(decimal(&padded, &numeric_wire(0, 0, 0, &[])), 0);
    assert_eq!(decimal(&padded, &numeric_wire(-1, 0, 4, &[7])), 7);
    let decoded = eight_three.decode(&[None]).expect("NULL numeric");
    assert!(decoded.is_null(0));
    let error = decode_one(&eight_three, &anchor[..9]).expect_err("short numeric");
    assert!(
        matches!(
            error,
            ConnectError::WireLength {
                postgres_name: "numeric",
                expected: 10,
                actual: 9,
                ..
            }
        ),
        "{error:?}"
    );
    let error = decode_one(&eight_three, &numeric_wire(0, 0, 0, &[10_000])).expect_err("digit");
    assert_eq!(
        error,
        ConnectError::Protocol {
            violation: ProtocolViolation::NumericDigit { digit: 10_000 }
        }
    );
}

#[test]
fn numeric_typmods_resolve_to_spark_decimal_types() {
    for (typmod, expected) in [
        (TypeMod::NONE, DataType::Decimal128(38, 18)),
        (TypeMod::numeric(1, 0), DataType::Decimal128(1, 0)),
        (TypeMod::numeric(38, 38), DataType::Decimal128(38, 38)),
        (TypeMod::numeric(39, 1), DataType::Decimal128(38, 0)),
        (TypeMod::numeric(50, 10), DataType::Decimal128(38, 0)),
        (TypeMod::numeric(50, 45), DataType::Decimal128(38, 33)),
        (TypeMod::numeric(1000, 40), DataType::Decimal128(38, 0)),
        (TypeMod::numeric(3, 5), DataType::Decimal128(5, 5)),
        (TypeMod::numeric(5, -2), DataType::Decimal128(38, 38)),
    ] {
        assert_eq!(column("numeric", typmod).field().data_type(), &expected);
    }
    let thousand = column("numeric", TypeMod::numeric(1000, 40));
    assert_eq!(decimal(&thousand, &numeric_wire(0, 0, 1, &[1, 5000])), 2);
    let over = column("numeric", TypeMod::numeric(39, 1));
    let wire = numeric_wire(
        9,
        0,
        1,
        &[
            12, 3456, 7890, 1234, 5678, 9012, 3456, 7890, 1234, 5678, 5000,
        ],
    );
    assert_eq!(
        decimal(&over, &wire),
        12_345_678_901_234_567_890_123_456_789_012_345_679
    );
    let narrow = column("numeric", TypeMod::numeric(3, 5));
    assert_eq!(decimal(&narrow, &numeric_wire(-1, 0, 5, &[12, 3000])), 123);
    let negative_scale = column("numeric", TypeMod::numeric(5, -2));
    let error = decode_one(&negative_scale, &numeric_wire(1, 0, 0, &[1, 2300]))
        .expect_err("12300 cannot fit Decimal128(38,38)");
    assert_eq!(refusal(&error), ValueRefusal::NumericOutOfRange);
    assert!(
        error.to_string().contains("CONNECT-DECL-pg-out-of-range"),
        "{error}"
    );
    let error = PlannedColumn::resolve(
        Arc::from("s.t.c"),
        "numeric",
        PgTypeKind::Base,
        TypeMod::numeric(1001, 0),
    )
    .expect_err("precision past 1000 refuses the column");
    assert_eq!(
        error,
        ConnectError::UnmappedType {
            column: Arc::from("s.t.c"),
            postgres_type: String::from("numeric(1001,0)"),
            row: UNMAPPED_ROW,
        }
    );
}

#[test]
fn numeric_special_values_refuse() {
    let numeric = column("numeric", TypeMod::NONE);
    let nan = [0x00, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00];
    let error = decode_one(&numeric, &nan).expect_err("NaN refuses");
    assert_eq!(refusal(&error), ValueRefusal::NumericNaN);
    assert!(
        error
            .to_string()
            .contains("CONNECT-DECL-pg-numeric-special"),
        "{error}"
    );
    for sign in [0xd000_u16, 0xf000] {
        let error =
            decode_one(&numeric, &numeric_wire(0, sign, 0, &[])).expect_err("infinity refuses");
        assert_eq!(refusal(&error), ValueRefusal::NumericInfinity);
        assert!(
            error
                .to_string()
                .contains("CONNECT-DECL-pg-numeric-special"),
            "{error}"
        );
    }
    let error = decode_one(&numeric, &numeric_wire(0, 0x1234, 0, &[1])).expect_err("sign word");
    assert_eq!(
        error,
        ConnectError::Protocol {
            violation: ProtocolViolation::NumericSign { sign: 0x1234 }
        }
    );
}

#[test]
fn unconstrained_numeric_rounds_half_up_at_scale_18() {
    let numeric = column("numeric", TypeMod::NONE);
    assert_eq!(decimal(&numeric, &numeric_wire(-5, 0, 19, &[50])), 1);
    assert_eq!(decimal(&numeric, &numeric_wire(-5, 0x4000, 19, &[50])), -1);
    assert_eq!(decimal(&numeric, &numeric_wire(-5, 0, 22, &[49, 9900])), 0);
    assert_eq!(decimal(&numeric, &numeric_wire(-5, 0, 20, &[150])), 2);
    assert_eq!(decimal(&numeric, &numeric_wire(-6, 0, 24, &[9999])), 0);
    assert_eq!(
        decimal(&numeric, &numeric_wire(0, 0, 20, &[2, 0, 0, 0, 0, 50])),
        2_000_000_000_000_000_001
    );
}

#[test]
fn numeric_group_at_exponent_minus_four_rounds_half_up() {
    let whole = column("numeric", TypeMod::numeric(8, 0));
    assert_eq!(decimal(&whole, &numeric_wire(0, 0, 1, &[7, 5000])), 8);
    assert_eq!(decimal(&whole, &numeric_wire(0, 0x4000, 1, &[7, 5000])), -8);
}

#[test]
fn bounded_numeric_overflow_refuses() {
    let numeric = column("numeric", TypeMod::NONE);
    let error = decode_one(&numeric, &numeric_wire(5, 0, 0, &[1])).expect_err("21 digits");
    assert_eq!(refusal(&error), ValueRefusal::NumericOutOfRange);
    assert!(
        error.to_string().contains("CONNECT-DECL-pg-out-of-range"),
        "{error}"
    );
    let twenty_nines = [9999; 5];
    assert_eq!(
        decimal(&numeric, &numeric_wire(4, 0, 0, &twenty_nines)),
        (10_i128.pow(20) - 1) * 10_i128.pow(18)
    );
    let mut almost = vec![9999; 9];
    almost.push(9950);
    let rounds_over = numeric_wire(4, 0, 19, &almost);
    let error = decode_one(&numeric, &rounds_over).expect_err("rounding carries past 38");
    assert_eq!(refusal(&error), ValueRefusal::NumericOutOfRange);
    let bounded = column("numeric", TypeMod::numeric(50, 10));
    let error = decode_one(&bounded, &numeric_wire(9, 0, 0, &[100])).expect_err("10^38 at scale 0");
    assert_eq!(refusal(&error), ValueRefusal::NumericOutOfRange);
    let error = decode_one(&numeric, &numeric_wire(i16::MAX, 0, 0, &[1])).expect_err("weight");
    assert_eq!(refusal(&error), ValueRefusal::NumericOutOfRange);
}

#[test]
fn uuid_renders_lowercase_canonical() {
    let uuid = column("uuid", TypeMod::NONE);
    let anchor = [
        0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a,
        0x11,
    ];
    let decoded = uuid
        .decode(&[Some(&anchor[..]), None, Some(&[0; 16][..])])
        .expect("uuids");
    assert_eq!(
        decoded.as_string::<i32>(),
        &StringArray::from(vec![
            Some("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
            None,
            Some("00000000-0000-0000-0000-000000000000"),
        ])
    );
    let error = decode_one(&uuid, &anchor[..15]).expect_err("15 bytes");
    assert!(
        matches!(
            error,
            ConnectError::WireLength {
                expected: 16,
                actual: 15,
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn jsonb_strips_version_one_and_refuses_others() {
    let jsonb = column("jsonb", TypeMod::NONE);
    let decoded = decode_one(
        &jsonb,
        &[0x01, 0x7b, 0x22, 0x61, 0x22, 0x3a, 0x20, 0x31, 0x7d],
    )
    .expect("version 1");
    assert_eq!(
        decoded.as_string::<i32>(),
        &StringArray::from(vec![r#"{"a": 1}"#])
    );
    let error = decode_one(&jsonb, b"\x02{}").expect_err("version 2");
    assert_eq!(
        error,
        ConnectError::Protocol {
            violation: ProtocolViolation::JsonbVersion { found: Some(2) }
        }
    );
    let error = decode_one(&jsonb, b"").expect_err("no version byte");
    assert_eq!(
        error,
        ConnectError::Protocol {
            violation: ProtocolViolation::JsonbVersion { found: None }
        }
    );
    let error = decode_one(&jsonb, b"\x01\xc3\x28").expect_err("invalid UTF-8");
    assert!(
        matches!(error, ConnectError::InvalidUtf8 { .. }),
        "{error:?}"
    );
}

#[test]
fn json_and_interval_are_text_verbatim() {
    for (postgres_name, text) in [
        ("json", "  {\"b\" : [1, 2] ,\"a\":1, \"a\": 2}\n"),
        ("interval", " 1 year 2 mons -3 days +04:05:06.789 "),
    ] {
        let planned = column(postgres_name, TypeMod::NONE);
        assert_eq!(planned.field().data_type(), &DataType::Utf8);
        let decoded = planned
            .decode(&[Some(text.as_bytes()), None])
            .expect("text");
        assert_eq!(
            decoded.as_string::<i32>(),
            &StringArray::from(vec![Some(text), None])
        );
        let error = decode_one(&planned, b"\xc3\x28").expect_err("invalid UTF-8");
        assert!(
            matches!(error, ConnectError::InvalidUtf8 { .. }),
            "{error:?}"
        );
    }
    assert_eq!(
        postgres_type("interval").map(|row| row.mapping),
        Some(PostgresMapping::ServerText)
    );
}

#[test]
fn unmapped_types_refuse_at_resolution() {
    let resolve = |typname: &str, kind: PgTypeKind| {
        PlannedColumn::resolve(Arc::from("public.t.c"), typname, kind, TypeMod::NONE)
    };
    for (typname, kind, row) in [
        ("money", PgTypeKind::Base, UNMAPPED_ROW),
        ("_int4", PgTypeKind::Base, UNMAPPED_ROW),
        ("int4range", PgTypeKind::Other, UNMAPPED_ROW),
        ("int4", PgTypeKind::Other, UNMAPPED_ROW),
        ("time", PgTypeKind::Base, "CONNECT-DECL-pg-time"),
    ] {
        let error = resolve(typname, kind).expect_err("outside the map");
        assert_eq!(
            error,
            ConnectError::UnmappedType {
                column: Arc::from("public.t.c"),
                postgres_type: typname.to_string(),
                row,
            }
        );
        let message = error.to_string();
        assert!(message.contains("`public.t.c`"), "{message}");
        assert!(message.contains(row), "{message}");
        assert!(message.contains("through `query` with a cast"), "{message}");
        assert_eq!(
            Error::from(error).exception_class(),
            ErrorClass::Unsupported
        );
    }
    let label = resolve("mood", PgTypeKind::Enum).expect("an enum reads as its label");
    assert_eq!(label.mapping(), PostgresMapping::Utf8);
    let decoded = label.decode(&[Some(b"happy".as_slice())]).expect("label");
    assert_eq!(
        decoded.as_string::<i32>(),
        &StringArray::from(vec!["happy"])
    );
    let not_null = resolve("int4", PgTypeKind::Base)
        .expect("int4")
        .with_nullable(false);
    assert!(!not_null.field().is_nullable());
}

#[test]
fn every_mapped_row_encodes_and_a_wrong_array_names_both_types() {
    for row in POSTGRES_TYPES {
        let Some(expected) = row.mapping.data_type() else {
            continue;
        };
        let wrong: ArrayRef = if expected == DataType::Boolean {
            Arc::new(Int32Array::from(vec![1]))
        } else {
            Arc::new(BooleanArray::from(vec![true]))
        };
        let error = row.encode(wrong.as_ref()).expect_err("a wrong array");
        assert_eq!(
            error,
            ConnectError::ArrowType {
                postgres_name: row.postgres_name,
                expected: expected.clone(),
                actual: wrong.data_type().clone(),
            }
        );
        let empty = arrow::array::new_empty_array(&expected);
        assert_eq!(
            row.encode(empty.as_ref()),
            Ok(Vec::new()),
            "{}",
            row.postgres_name
        );
    }
}

#[test]
fn decode_errors_fold_by_class() {
    let numeric = column("numeric", TypeMod::NONE);
    let nan = [0x00, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00];
    let refused = decode_one(&numeric, &nan).expect_err("NaN");
    assert_eq!(
        Error::from(refused).exception_class(),
        ErrorClass::Unsupported
    );
    let malformed = decode_one(&numeric, &numeric_wire(0, 0x1234, 0, &[])).expect_err("sign");
    assert_eq!(Error::from(malformed).exception_class(), ErrorClass::Base);
    assert_eq!(
        Error::from(ConnectError::Disconnected).exception_class(),
        ErrorClass::Base
    );
}
