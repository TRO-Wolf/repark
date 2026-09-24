use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Type};
use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation};
use tempfile::TempDir;

use super::*;

async fn seeded(
    warehouse: &TempDir,
    properties: HashMap<String, String>,
) -> (Arc<dyn Catalog>, Table) {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("memory catalog"),
    );
    let namespace = NamespaceIdent::new("ns".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .properties(properties)
        .build();
    let table = catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    (catalog, table)
}

fn arrow_with_extra() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("data", DataType::Utf8, true),
        Field::new("extra", DataType::Int32, true),
    ])
}

#[tokio::test]
async fn the_property_is_read_exactly_and_case_insensitively() {
    let warehouse = TempDir::new().expect("tempdir");
    let (_catalog, plain) = seeded(&warehouse, HashMap::new()).await;
    assert!(!accepts_any_schema(&plain));

    let other = TempDir::new().expect("tempdir");
    let (_catalog, opted) = seeded(
        &other,
        HashMap::from([(ACCEPT_ANY_SCHEMA_PROP.to_string(), " TRUE ".to_string())]),
    )
    .await;
    assert!(accepts_any_schema(&opted));

    let third = TempDir::new().expect("tempdir");
    let (_catalog, off) = seeded(
        &third,
        HashMap::from([(ACCEPT_ANY_SCHEMA_PROP.to_string(), "false".to_string())]),
    )
    .await;
    assert!(!accepts_any_schema(&off));
}

#[tokio::test]
async fn the_union_adds_the_new_column_last_optional_and_keeps_the_source_type() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = seeded(&warehouse, HashMap::new()).await;
    let incoming = incoming_schema(&arrow_with_extra()).expect("incoming");
    let evolved = evolve_schema(&catalog, &table, incoming)
        .await
        .expect("evolve");
    let fields = evolved.metadata().current_schema().as_struct().fields();
    assert_eq!(
        fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "data", "extra"]
    );
    let added = fields.last().expect("added field");
    assert_eq!(*added.field_type, Type::Primitive(PrimitiveType::Int));
    assert!(!added.required);
}

#[tokio::test]
async fn the_union_adds_no_snapshot() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = seeded(&warehouse, HashMap::new()).await;
    let before = table.metadata().snapshots().count();
    let incoming = incoming_schema(&arrow_with_extra()).expect("incoming");
    let evolved = evolve_schema(&catalog, &table, incoming)
        .await
        .expect("evolve");
    assert_eq!(evolved.metadata().snapshots().count(), before);
}

#[tokio::test]
async fn a_narrower_source_column_does_not_narrow_the_table() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = seeded(&warehouse, HashMap::new()).await;
    let narrower = ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("data", DataType::Utf8, true),
    ]);
    let incoming = incoming_schema(&narrower).expect("incoming");
    let evolved = evolve_schema(&catalog, &table, incoming)
        .await
        .expect("evolve");
    let fields = evolved.metadata().current_schema().as_struct().fields();
    assert_eq!(
        *fields[0].field_type,
        Type::Primitive(PrimitiveType::Long),
        "a narrower incoming type must not narrow the table"
    );
}

#[tokio::test]
async fn a_source_missing_a_column_keeps_it_in_the_schema() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = seeded(&warehouse, HashMap::new()).await;
    let partial = ArrowSchema::new(vec![Field::new("id", DataType::Int64, true)]);
    let incoming = incoming_schema(&partial).expect("incoming");
    let evolved = evolve_schema(&catalog, &table, incoming)
        .await
        .expect("evolve");
    assert_eq!(
        evolved
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "data"]
    );
}

#[tokio::test]
async fn an_identical_schema_leaves_the_table_where_it_was() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = seeded(&warehouse, HashMap::new()).await;
    let same = ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("data", DataType::Utf8, true),
    ]);
    let incoming = incoming_schema(&same).expect("incoming");
    let evolved = evolve_schema(&catalog, &table, incoming)
        .await
        .expect("evolve");
    assert_eq!(
        evolved.metadata().current_schema_id(),
        table.metadata().current_schema_id()
    );
}

fn illegal_argument_message(error: &datafusion::error::DataFusionError) -> String {
    let datafusion::error::DataFusionError::External(inner) = error else {
        panic!("expected an external IllegalArgumentMarker, got {error:?}");
    };
    inner
        .downcast_ref::<crate::write::IllegalArgumentMarker>()
        .expect("expected an IllegalArgumentMarker")
        .0
        .clone()
}

async fn reloaded_types(catalog: &Arc<dyn Catalog>, table: &Table) -> Vec<Type> {
    catalog
        .load_table(table.identifier())
        .await
        .expect("reload")
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.field_type.as_ref().clone())
        .collect()
}

#[tokio::test]
async fn merge_evolution_refuses_narrowing_with_the_java_message() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = seeded(&warehouse, HashMap::new()).await;
    let narrower = ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("data", DataType::Utf8, true),
        Field::new("extra", DataType::Int32, true),
    ]);
    let incoming = incoming_schema(&narrower).expect("incoming");
    let error = evolve_merge_schema(&catalog, &table, incoming)
        .await
        .expect_err("narrowing must refuse");
    assert_eq!(
        illegal_argument_message(&error),
        "Cannot change column type: id: long -> int"
    );
    assert_eq!(
        reloaded_types(&catalog, &table).await,
        vec![
            Type::Primitive(PrimitiveType::Long),
            Type::Primitive(PrimitiveType::String)
        ]
    );
}

#[tokio::test]
async fn merge_evolution_refuses_a_non_promotable_change() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = seeded(&warehouse, HashMap::new()).await;
    let other = ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("DATA", DataType::Int32, true),
    ]);
    let incoming = incoming_schema(&other).expect("incoming");
    let error = evolve_merge_schema(&catalog, &table, incoming)
        .await
        .expect_err("string to int must refuse");
    assert_eq!(
        illegal_argument_message(&error),
        "Cannot change column type: data: string -> int"
    );
}

#[tokio::test]
async fn merge_evolution_widens_a_promotable_column_and_adds_the_new_one() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = seeded(&warehouse, HashMap::new()).await;
    let with_int = incoming_schema(&arrow_with_extra()).expect("incoming");
    let table = evolve_schema(&catalog, &table, with_int)
        .await
        .expect("add int extra");
    let wider = ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("data", DataType::Utf8, true),
        Field::new("extra", DataType::Int64, true),
        Field::new("more", DataType::Utf8, true),
    ]);
    let incoming = incoming_schema(&wider).expect("incoming");
    let evolved = evolve_merge_schema(&catalog, &table, incoming)
        .await
        .expect("widen");
    assert_eq!(
        reloaded_types(&catalog, &evolved).await,
        vec![
            Type::Primitive(PrimitiveType::Long),
            Type::Primitive(PrimitiveType::String),
            Type::Primitive(PrimitiveType::Long),
            Type::Primitive(PrimitiveType::String)
        ]
    );
}

#[tokio::test]
async fn union_type_conflict_refuses_as_an_illegal_argument() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = seeded(&warehouse, HashMap::new()).await;
    let conflict = ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("data", DataType::Utf8, true),
    ]);
    let incoming = incoming_schema(&conflict).expect("incoming");
    let error = evolve_schema(&catalog, &table, incoming)
        .await
        .expect_err("long to string must refuse");
    assert_eq!(
        illegal_argument_message(&error),
        "Cannot change column type: id: long -> string"
    );
}
