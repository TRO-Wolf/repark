use std::collections::HashMap;

use iceberg::spec::{
    FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, StructType, TableMetadata,
    TableMetadataBuilder, Type, UnboundPartitionSpec,
};

use crate::write::replace_schema::replacement_schema;

fn long(id: i32, name: &str) -> NestedField {
    NestedField::optional(id, name, Type::Primitive(PrimitiveType::Long))
}

fn string(id: i32, name: &str) -> NestedField {
    NestedField::optional(id, name, Type::Primitive(PrimitiveType::String))
}

fn int(id: i32, name: &str) -> NestedField {
    NestedField::optional(id, name, Type::Primitive(PrimitiveType::Int))
}

fn schema(fields: Vec<NestedField>) -> Schema {
    Schema::builder()
        .with_fields(fields.into_iter().map(Into::into))
        .build()
        .expect("schema")
}

fn metadata(current: Schema) -> TableMetadata {
    TableMetadataBuilder::new(
        current,
        UnboundPartitionSpec::builder().build(),
        SortOrder::unsorted_order(),
        "/tmp/u7/t".to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .expect("builder")
    .build()
    .expect("metadata")
    .metadata
}

fn ids(schema: &Schema) -> Vec<(i32, String)> {
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.id, field.name.clone()))
        .collect()
}

fn named(pairs: &[(i32, &str)]) -> Vec<(i32, String)> {
    pairs
        .iter()
        .map(|(id, name)| (*id, (*name).to_string()))
        .collect()
}

#[test]
fn a_replacement_keeps_ids_by_name_and_takes_fresh_ids_above_the_last_column_id() {
    let current = metadata(schema(vec![
        long(1, "id"),
        string(2, "data"),
        string(3, "cat"),
    ]));
    let frame = schema(vec![
        string(1, "cat"),
        string(2, "payload"),
        long(3, "id"),
        int(4, "extra"),
    ]);
    let replaced = replacement_schema(&current, &frame).expect("replacement");
    assert_eq!(
        ids(&replaced),
        named(&[(3, "cat"), (4, "payload"), (1, "id"), (5, "extra")])
    );
    assert_eq!(replaced.highest_field_id(), 5);
}

#[test]
fn a_type_change_keeps_the_id_and_a_dropped_name_leaves_no_gap_reused() {
    let current = metadata(schema(vec![
        long(1, "id"),
        string(2, "data"),
        string(3, "cat"),
    ]));
    let frame = schema(vec![int(1, "id"), string(2, "data")]);
    let replaced = replacement_schema(&current, &frame).expect("replacement");
    assert_eq!(ids(&replaced), named(&[(1, "id"), (2, "data")]));
    let field = replaced.field_by_id(1).expect("id field");
    assert_eq!(*field.field_type, Type::Primitive(PrimitiveType::Int));
    let readded = replacement_schema(&current, &schema(vec![long(1, "id"), string(2, "new")]))
        .expect("replacement");
    assert_eq!(ids(&readded), named(&[(1, "id"), (4, "new")]));
}

#[test]
fn nested_fields_keep_their_ids_by_dotted_name() {
    let current = metadata(schema(vec![
        long(1, "id"),
        NestedField::optional(
            2,
            "s",
            Type::Struct(StructType::new(vec![
                int(3, "a").into(),
                string(4, "b").into(),
            ])),
        ),
    ]));
    let frame = schema(vec![
        long(1, "id"),
        NestedField::optional(
            2,
            "s",
            Type::Struct(StructType::new(vec![
                string(3, "b").into(),
                int(4, "a").into(),
                long(5, "c").into(),
            ])),
        ),
    ]);
    let replaced = replacement_schema(&current, &frame).expect("replacement");
    assert_eq!(replaced.field_id_by_name("s.b"), Some(4));
    assert_eq!(replaced.field_id_by_name("s.a"), Some(3));
    assert_eq!(replaced.field_id_by_name("s.c"), Some(5));
    assert_eq!(replaced.field_id_by_name("s"), Some(2));
}
