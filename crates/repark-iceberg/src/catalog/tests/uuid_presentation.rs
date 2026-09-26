use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field};
use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Type};

use super::super::uuid_presentation::{
    convert_uuid_column, parse_uuid_text, presented_arrow_schema, stored_arrow_schema,
};

fn schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(
                1,
                "u",
                Type::Primitive(PrimitiveType::Uuid),
            )),
            Arc::new(NestedField::optional(
                2,
                "f",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
            Arc::new(NestedField::optional(
                3,
                "s",
                Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                    4,
                    "v",
                    Type::Primitive(PrimitiveType::Uuid),
                ))])),
            )),
        ])
        .build()
        .expect("schema")
}

#[test]
fn only_uuid_fields_present_as_text_and_store_back_as_bytes() {
    let iceberg = schema();
    let presented = Arc::new(presented_arrow_schema(&iceberg).expect("presented"));
    assert_eq!(presented.field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        presented.field(1).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    let DataType::Struct(children) = presented.field(2).data_type() else {
        panic!("struct expected");
    };
    assert_eq!(children[0].data_type(), &DataType::Utf8);
    let stored = stored_arrow_schema(&presented, &iceberg);
    let original = iceberg::arrow::schema_to_arrow_schema(&iceberg).expect("stored");
    assert_eq!(stored.as_ref(), &original);
}

#[test]
fn the_parser_is_java_uuid_from_string() {
    let upper = parse_uuid_text("123E4567-E89B-12D3-A456-4266141740FF").expect("upper case");
    let lower = parse_uuid_text("123e4567-e89b-12d3-a456-4266141740ff").expect("lower case");
    assert_eq!(upper, lower);
    let short = parse_uuid_text("1-2-3-4-5").expect("java accepts short groups");
    assert_eq!(
        uuid::Uuid::from_bytes(short).hyphenated().to_string(),
        "00000001-0002-0003-0004-000000000005"
    );
    assert_eq!(
        parse_uuid_text("nope").expect_err("no dashes"),
        "Invalid UUID string: nope"
    );
    assert_eq!(
        parse_uuid_text("123e4567e89b12d3a456426614174000").expect_err("simple form"),
        "Invalid UUID string: 123e4567e89b12d3a456426614174000"
    );
}

#[test]
fn a_nested_uuid_round_trips_text_to_bytes_to_lower_case_text() {
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("123E4567-E89B-12D3-A456-4266141740FF"),
        None,
    ]));
    let presented_struct = DataType::Struct(vec![Field::new("v", DataType::Utf8, true)].into());
    let stored_struct =
        DataType::Struct(vec![Field::new("v", DataType::FixedSizeBinary(16), true)].into());
    let column: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("v", DataType::Utf8, true)),
        text,
    )]));
    let stored = convert_uuid_column(&column, &stored_struct).expect("stores");
    assert_eq!(stored.data_type(), &stored_struct);
    let rendered = convert_uuid_column(&stored, &presented_struct).expect("renders");
    let values = rendered.as_struct().column(0).as_string::<i32>().clone();
    assert_eq!(values.value(0), "123e4567-e89b-12d3-a456-4266141740ff");
    assert!(values.is_null(1));
}

#[test]
fn an_invalid_text_refuses_with_the_fork_text() {
    let text: ArrayRef = Arc::new(StringArray::from(vec![Some("nope")]));
    let error = convert_uuid_column(&text, &DataType::FixedSizeBinary(16)).expect_err("refuses");
    assert_eq!(
        error.to_string(),
        "External error: DataInvalid => Invalid UUID string: nope"
    );
}
