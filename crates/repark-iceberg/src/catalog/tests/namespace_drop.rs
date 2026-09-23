use super::super::*;

use crate::view::{ViewDefinition, ViewTarget, create_or_replace_view};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

fn single_column_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap_or_else(|error| panic!("fixture schema must build: {error}"))
}

async fn catalog_with_one_table(
    warehouse: &TempDir,
    namespace: &str,
    table: &str,
) -> std::sync::Arc<dyn Catalog> {
    let catalog = memory_catalog(warehouse.path().to_str().unwrap_or_else(|| {
        panic!("warehouse path must be utf8");
    }))
    .await
    .unwrap_or_else(|error| panic!("memory catalog must build: {error}"));
    catalog
        .create_namespace(&NamespaceIdent::new(namespace.to_string()), HashMap::new())
        .await
        .unwrap_or_else(|error| panic!("namespace must create: {error}"));
    let creation = TableCreation::builder()
        .name(table.to_string())
        .location(format!(
            "{}/{table}",
            warehouse.path().to_str().unwrap_or_else(|| {
                panic!("warehouse path must be utf8");
            })
        ))
        .schema(single_column_schema())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&NamespaceIdent::new(namespace.to_string()), creation)
        .await
        .unwrap_or_else(|error| panic!("table must create: {error}"));
    catalog
}

#[tokio::test]
async fn refuse_non_empty_namespace_drop_refuses_a_table_holding_namespace() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_one_table(&warehouse, "guarded", "t").await;
    let error = refuse_non_empty_namespace_drop(
        catalog.as_ref(),
        &NamespaceIdent::new("guarded".to_string()),
        "guarded",
    )
    .await
    .expect_err("a table-holding namespace must refuse the drop");
    let message = error.to_string();
    assert!(
        message.contains("Namespace guarded is not empty."),
        "the refusal must name the namespace like Spark, got: {message}"
    );
    assert!(
        message.contains("Contains 1 table(s)."),
        "the refusal must name the table count like Spark, got: {message}"
    );
}

#[tokio::test]
async fn refuse_non_empty_namespace_drop_passes_after_the_table_is_dropped() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = catalog_with_one_table(&warehouse, "emptied", "t").await;
    catalog
        .drop_table(&TableIdent::new(
            NamespaceIdent::new("emptied".to_string()),
            "t".to_string(),
        ))
        .await
        .unwrap_or_else(|error| panic!("table must drop: {error}"));
    refuse_non_empty_namespace_drop(
        catalog.as_ref(),
        &NamespaceIdent::new("emptied".to_string()),
        "emptied",
    )
    .await
    .unwrap_or_else(|error| panic!("an emptied namespace must pass: {error}"));
}

#[tokio::test]
async fn refuse_non_empty_namespace_drop_passes_on_an_empty_namespace() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = memory_catalog(warehouse.path().to_str().unwrap_or_else(|| {
        panic!("warehouse path must be utf8");
    }))
    .await
    .unwrap_or_else(|error| panic!("memory catalog must build: {error}"));
    catalog
        .create_namespace(&NamespaceIdent::new("bare".to_string()), HashMap::new())
        .await
        .unwrap_or_else(|error| panic!("namespace must create: {error}"));
    refuse_non_empty_namespace_drop(
        catalog.as_ref(),
        &NamespaceIdent::new("bare".to_string()),
        "bare",
    )
    .await
    .unwrap_or_else(|error| panic!("an empty namespace must pass: {error}"));
}

#[tokio::test]
async fn refuse_non_empty_namespace_drop_fails_on_a_missing_namespace() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = memory_catalog(warehouse.path().to_str().unwrap_or_else(|| {
        panic!("warehouse path must be utf8");
    }))
    .await
    .unwrap_or_else(|error| panic!("memory catalog must build: {error}"));
    refuse_non_empty_namespace_drop(
        catalog.as_ref(),
        &NamespaceIdent::new("no_such_ns".to_string()),
        "no_such_ns",
    )
    .await
    .expect_err("a missing namespace must fail loud, never pass as empty");
}

#[tokio::test]
async fn refuse_non_empty_namespace_drop_refuses_a_namespace_holding_a_child_namespace() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = memory_catalog(warehouse.path().to_str().unwrap_or_else(|| {
        panic!("warehouse path must be utf8");
    }))
    .await
    .unwrap_or_else(|error| panic!("memory catalog must build: {error}"));
    let parent = NamespaceIdent::new("parent".to_string());
    catalog
        .create_namespace(&parent, HashMap::new())
        .await
        .unwrap_or_else(|error| panic!("parent must create: {error}"));
    let child = NamespaceIdent::from_strs(["parent", "child"])
        .unwrap_or_else(|error| panic!("child ident: {error}"));
    catalog
        .create_namespace(&child, HashMap::new())
        .await
        .unwrap_or_else(|error| panic!("child must create: {error}"));
    let error = refuse_non_empty_namespace_drop(catalog.as_ref(), &parent, "parent")
        .await
        .expect_err("a namespace holding a child namespace must refuse");
    assert!(
        error
            .to_string()
            .contains("Namespace parent is not empty. Contains 1 child namespace(s)."),
        "got: {error}"
    );
}

#[tokio::test]
async fn refuse_non_empty_namespace_drop_refuses_a_view_only_namespace() {
    let warehouse = TempDir::new().unwrap_or_else(|error| panic!("tempdir: {error}"));
    let catalog = memory_catalog(warehouse.path().to_str().unwrap_or_else(|| {
        panic!("warehouse path must be utf8");
    }))
    .await
    .unwrap_or_else(|error| panic!("memory catalog must build: {error}"));
    let namespace = NamespaceIdent::new("guarded".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap_or_else(|error| panic!("namespace must create: {error}"));
    let target = ViewTarget {
        catalog_name: "sc",
        catalog: catalog.as_ref(),
        namespace: &namespace,
        namespace_name: "guarded",
        view_name: "vw",
    };
    create_or_replace_view(
        &target,
        false,
        false,
        ViewDefinition {
            sql: "SELECT id FROM t".to_string(),
            schema: single_column_schema(),
            properties: HashMap::new(),
            warehouse: warehouse
                .path()
                .to_str()
                .unwrap_or_else(|| panic!("warehouse path must be utf8"))
                .to_string(),
            location_override: None,
            default_catalog: "sc".to_string(),
        },
    )
    .await
    .unwrap_or_else(|error| panic!("view must create: {error}"));
    let error = refuse_non_empty_namespace_drop(catalog.as_ref(), &namespace, "guarded")
        .await
        .expect_err("a view-only namespace must refuse the drop");
    assert!(
        error
            .to_string()
            .contains("Namespace guarded is not empty. Contains 1 view(s)."),
        "pins: ice-views-1/C-013, got: {error}"
    );
}
