use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Transform, Type,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::{PartitionSpecChange, apply_partition_spec_changes, java_struct_text};

fn primitive(kind: PrimitiveType) -> Type {
    Type::Primitive(kind)
}

fn measured_schema() -> Schema {
    let list = ListType::new(
        NestedField::list_element(12, primitive(PrimitiveType::String), false).into(),
    );
    let map = MapType::new(
        NestedField::map_key_element(13, primitive(PrimitiveType::String)).into(),
        NestedField::map_value_element(14, primitive(PrimitiveType::Int), false).into(),
    );
    let inner = StructType::new(vec![
        NestedField::optional(15, "a", primitive(PrimitiveType::Int)).into(),
        NestedField::optional(16, "b", primitive(PrimitiveType::String)).into(),
    ]);
    let decimal = PrimitiveType::Decimal {
        precision: 10,
        scale: 2,
    };
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", primitive(PrimitiveType::Long))
                .with_doc("the key")
                .into(),
            NestedField::optional(2, "d", primitive(decimal)).into(),
            NestedField::optional(3, "l", Type::List(list)).into(),
            NestedField::optional(4, "m", Type::Map(map)).into(),
            NestedField::optional(5, "s", Type::Struct(inner)).into(),
            NestedField::optional(6, "tn", primitive(PrimitiveType::Timestamp)).into(),
            NestedField::optional(7, "bi", primitive(PrimitiveType::Binary)).into(),
            NestedField::optional(8, "f", primitive(PrimitiveType::Float)).into(),
            NestedField::optional(9, "db", primitive(PrimitiveType::Double)).into(),
            NestedField::optional(10, "dt", primitive(PrimitiveType::Date)).into(),
            NestedField::optional(11, "bo", primitive(PrimitiveType::Boolean)).into(),
        ])
        .build()
        .unwrap()
}

#[test]
fn struct_text_renders_docs_and_decimal_like_java() {
    let expected = "struct<1: id: required long (the key), 2: d: optional decimal(10, 2), \
                    3: l: optional list<string>, 4: m: optional map<string, int>, \
                    5: s: optional struct<15: a: optional int, 16: b: optional string>, \
                    6: tn: optional timestamp, 7: bi: optional binary, 8: f: optional float, \
                    9: db: optional double, 10: dt: optional date, 11: bo: optional boolean>";
    assert_eq!(
        java_struct_text(measured_schema().as_struct().fields()),
        expected
    );
}

async fn partitioned_by_cat(wh: &TempDir) -> (Arc<dyn Catalog>, TableIdent) {
    let warehouse = wh.path().to_str().unwrap().to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await
            .unwrap(),
    );
    let ns = NamespaceIdent::new("sales".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "cat", primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .properties(HashMap::new())
        .build();
    catalog.create_table(&ns, creation).await.unwrap();
    let ident = TableIdent::new(ns, "t".to_string());
    apply_partition_spec_changes(
        catalog.as_ref(),
        &ident,
        &[PartitionSpecChange::AddField {
            source_name: "cat".into(),
            transform: Transform::Identity,
            name: None,
        }],
    )
    .await
    .unwrap();
    (catalog, ident)
}

async fn refuses_and_keeps_the_spec(change: PartitionSpecChange) {
    let wh = TempDir::new().unwrap();
    let (catalog, ident) = partitioned_by_cat(&wh).await;
    let error = apply_partition_spec_changes(catalog.as_ref(), &ident, &[change])
        .await
        .expect_err("a wrong-case source must refuse");
    let expected = "ValidationException: Cannot find field 'CAT' in struct: \
                    struct<1: id: optional int, 2: cat: optional string>";
    assert!(error.to_string().ends_with(expected), "{error}");
    let table = catalog.load_table(&ident).await.unwrap();
    let spec = table.metadata().default_partition_spec();
    let fields = spec
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.transform))
        .collect::<Vec<_>>();
    assert_eq!(fields, vec![("cat".to_string(), Transform::Identity)]);
}

#[tokio::test]
async fn replace_field_wrong_case_source_refuses_like_spark() {
    refuses_and_keeps_the_spec(PartitionSpecChange::ReplaceField {
        old_name: "cat".into(),
        source_name: "CAT".into(),
        transform: Transform::Bucket(4),
        new_name: None,
    })
    .await;
}

#[tokio::test]
async fn replace_by_transform_wrong_case_source_refuses_like_spark() {
    refuses_and_keeps_the_spec(PartitionSpecChange::ReplaceFieldByTransform {
        old_source_name: "cat".into(),
        old_transform: Transform::Identity,
        source_name: "CAT".into(),
        transform: Transform::Bucket(4),
        new_name: None,
    })
    .await;
}
