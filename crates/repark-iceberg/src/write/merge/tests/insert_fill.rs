use std::collections::HashMap;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::error::Result;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::spec::{
    FormatVersion, Literal, NestedField, PrimitiveType, Schema as IcebergSchema, SortOrder, Struct,
    StructType, TableMetadataBuilder, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableIdent};

use super::super::insert::insert_projection_with_defaults;
use super::super::{InsertAction, InsertClause};
use super::helpers::spec;
use super::merge::merge_sql;
use crate::write::insert_defaults::{ColumnDefaults, column_defaults};

pub(super) fn insert_projection(
    clause: &InsertClause,
    write_schema: &ArrowSchema,
    case_insensitive: bool,
) -> Result<String> {
    insert_projection_with_defaults(
        clause,
        write_schema,
        case_insensitive,
        &ColumnDefaults::new(),
    )
}

fn insert(columns: &[&str], values: &[&str]) -> InsertClause {
    InsertClause {
        predicate_sql: None,
        action: InsertAction::Explicit {
            columns: columns.iter().map(ToString::to_string).collect(),
            values_sql: values.iter().map(ToString::to_string).collect(),
        },
    }
}

pub(super) fn test_table(current: IcebergSchema) -> Table {
    let built = TableMetadataBuilder::new(
        current,
        UnboundPartitionSpec::builder().build(),
        SortOrder::unsorted_order(),
        "memory://insert-fill".to_string(),
        FormatVersion::V3,
        HashMap::new(),
    )
    .expect("metadata builder")
    .build()
    .expect("metadata");
    Table::builder()
        .file_io(FileIO::new_with_memory())
        .metadata(built.metadata)
        .identifier(TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t".to_string(),
        ))
        .build()
        .expect("table")
}

pub(super) fn target_schema() -> ArrowSchema {
    schema_to_arrow_schema(target().metadata().current_schema()).expect("arrow")
}

pub(super) fn target() -> Table {
    test_table(
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema"),
    )
}

#[test]
fn insert_projection_fills_write_default() {
    let current = IcebergSchema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Int))
                .with_write_default(Literal::int(5))
                .into(),
        ])
        .build()
        .expect("schema");
    let write_schema = schema_to_arrow_schema(&current).expect("arrow");
    let defaults = column_defaults(&current).expect("defaults");
    assert_eq!(
        insert_projection_with_defaults(
            &insert(&["id", "name"], &["s.id", "s.name"]),
            &write_schema,
            true,
            &defaults
        )
        .unwrap(),
        "(s.id) AS `id`, (s.name) AS `name`, (CAST(5 AS INT)) AS `c`"
    );
    let err = insert_projection_with_defaults(
        &insert(&["name"], &["s.name"]),
        &write_schema,
        true,
        &defaults,
    )
    .unwrap_err();
    assert!(err.to_string().contains("required column `id`"));
}

#[test]
fn column_defaults_ignores_struct_default() {
    let current = IcebergSchema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(
                2,
                "s",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(3, "x", Type::Primitive(PrimitiveType::Int)).into(),
                ])),
            )
            .with_write_default(Literal::Struct(Struct::empty()))
            .into(),
        ])
        .build()
        .expect("schema");
    let defaults = column_defaults(&current).expect("defaults");
    assert!(!defaults.has_any());
}

#[test]
fn insert_sql_fills_write_default() {
    let current = IcebergSchema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Int))
                .with_write_default(Literal::int(5))
                .into(),
        ])
        .build()
        .expect("schema");
    let table = test_table(current);
    let owned = spec(vec![], vec![insert(&["id", "name"], &["s.id", "s.name"])]);
    let sql = merge_sql(&owned);
    let schema = schema_to_arrow_schema(table.metadata().current_schema()).expect("arrow");
    let text = sql.insert_sql(0, &table, &schema).expect("insert");
    assert!(
        text.contains("(CAST(5 AS INT)) AS `c`"),
        "omitted defaulted column must fill, got: {text}"
    );
}
