use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use repark_connect::postgres::{
    POSTGRES_TYPES, PostgresMapping, PostgresTypeRow, TypeMapError, postgres_type,
};

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
    assert_eq!(
        declared,
        vec![
            ("numeric", "CONNECT-DECL-pg-numeric"),
            ("date", "CONNECT-DECL-pg-date"),
            ("time", "CONNECT-DECL-pg-time"),
            ("timestamp", "CONNECT-DECL-pg-timestamp"),
            ("timestamptz", "CONNECT-DECL-pg-timestamptz"),
            ("interval", "CONNECT-DECL-pg-interval"),
            ("uuid", "CONNECT-DECL-pg-uuid"),
            ("json", "CONNECT-DECL-pg-json"),
            ("jsonb", "CONNECT-DECL-pg-jsonb"),
        ]
    );
    for (postgres_name, registry_row) in declared {
        let row = row(postgres_name);
        assert_eq!(row.mapping.data_type(), None);
        let expected = TypeMapError::Declared {
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
        TypeMapError::WireLength {
            postgres_name: "int4",
            index: 1,
            expected: 4,
            actual: 2,
        }
    );
    let error = row("bool").decode(&[Some(&[])]).expect_err("empty bool");
    assert!(matches!(
        error,
        TypeMapError::WireLength {
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
        TypeMapError::InvalidUtf8 {
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
        TypeMapError::ArrowType {
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
            TypeMapError::ArrowType {
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
