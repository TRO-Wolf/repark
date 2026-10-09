use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use repark_connect::ConnectError;
use repark_connect::postgres::{PgTypeKind, PlannedColumn, TypeMod};

fn base(typname: &str) -> PlannedColumn {
    PlannedColumn::resolve(Arc::from("c"), typname, PgTypeKind::Base, TypeMod::NONE)
        .expect("a mapped column")
}

fn dictionary(keys: DataType, values: DataType) -> DataType {
    DataType::Dictionary(Box::new(keys), Box::new(values))
}

#[test]
fn another_encoding_of_the_same_values_encodes_to_the_same_bytes() {
    let texts: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some("é𝄞"),
        Some("a"),
        Some(""),
    ]));
    let uuids: ArrayRef = Arc::new(StringArray::from(vec![
        Some("123e4567-e89b-12d3-a456-426614174000"),
        None,
    ]));
    let bytes: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(&[0_u8, 255][..]),
        None,
        Some(&[][..]),
    ]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None, Some(7), Some(-1)]));
    let decimals: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(15_000), None, Some(-1)])
            .with_precision_and_scale(38, 18)
            .expect("a decimal type"),
    );
    let instants: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(0), None, Some(1)]).with_timezone("UTC"),
    );
    let narrow = DataType::Int8;
    let cases: Vec<(&str, &ArrayRef, Vec<DataType>)> = vec![
        (
            "text",
            &texts,
            vec![
                DataType::LargeUtf8,
                DataType::Utf8View,
                dictionary(DataType::Int32, DataType::Utf8),
                dictionary(narrow.clone(), DataType::LargeUtf8),
                dictionary(DataType::UInt16, DataType::Utf8View),
            ],
        ),
        ("varchar", &texts, vec![DataType::Utf8View]),
        ("json", &texts, vec![DataType::LargeUtf8]),
        ("interval", &texts, vec![DataType::Utf8View]),
        (
            "uuid",
            &uuids,
            vec![
                DataType::Utf8View,
                dictionary(narrow.clone(), DataType::Utf8),
            ],
        ),
        (
            "bytea",
            &bytes,
            vec![
                DataType::LargeBinary,
                DataType::BinaryView,
                dictionary(DataType::Int32, DataType::Binary),
            ],
        ),
        (
            "int4",
            &ints,
            vec![dictionary(narrow.clone(), DataType::Int32)],
        ),
        (
            "numeric",
            &decimals,
            vec![dictionary(DataType::Int32, decimals.data_type().clone())],
        ),
        (
            "timestamptz",
            &instants,
            vec![dictionary(narrow, instants.data_type().clone())],
        ),
    ];
    for (typname, plain, encodings) in cases {
        let column = base(typname);
        let expected = column.encode(plain.as_ref()).expect("the planned type");
        for encoding in encodings {
            let other = cast(plain, &encoding).expect("an encoding of the same values");
            assert_eq!(other.data_type(), &encoding);
            let wire = column.encode(other.as_ref());
            assert_eq!(wire.as_ref(), Ok(&expected), "{typname} as {encoding}");
        }
    }
}

#[test]
fn another_encoding_of_other_values_is_still_refused_naming_what_came() {
    let wide: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let packed = dictionary(DataType::Int8, DataType::Int64);
    let texts: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
    let cases = [
        ("int4", cast(&wide, &packed).expect("a dictionary")),
        ("int4", cast(&texts, &DataType::Utf8View).expect("a view")),
        (
            "bytea",
            cast(&texts, &DataType::LargeUtf8).expect("large text"),
        ),
        (
            "text",
            cast(&texts, &DataType::BinaryView).expect("a binary view"),
        ),
    ];
    for (typname, given) in cases {
        let column = base(typname);
        let error = column.encode(given.as_ref()).expect_err("another type");
        assert_eq!(
            error,
            ConnectError::ArrowType {
                postgres_name: column.postgres_type(),
                expected: column.field().data_type().clone(),
                actual: given.data_type().clone(),
            },
            "{typname}"
        );
    }
}
