use super::super::*;
use super::common::*;
use std::collections::HashSet;
use tempfile::TempDir;

fn identifier_ids(table: &iceberg::table::Table) -> HashSet<i32> {
    table
        .metadata()
        .current_schema()
        .identifier_field_ids()
        .collect()
}

fn field_of(
    table: &iceberg::table::Table,
    name: &str,
) -> std::sync::Arc<iceberg::spec::NestedField> {
    table
        .metadata()
        .current_schema()
        .field_by_name(name)
        .expect("field must exist")
        .clone()
}

#[tokio::test]
async fn set_identifier_fields_records_the_set_without_changing_requiredness() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ids (id BIGINT NOT NULL, k STRING NOT NULL, v STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ids SET IDENTIFIER FIELDS id, k",
    )
    .await;
    let table = load_sales_table(&catalogs, "ids").await;
    let id = field_of(&table, "id");
    let k = field_of(&table, "k");
    let v = field_of(&table, "v");
    assert_eq!(
        identifier_ids(&table),
        HashSet::from([id.id, k.id]),
        "identifier fields must be id and k"
    );
    assert!(id.required, "id must stay required");
    assert!(k.required, "k must stay required");
    assert!(!v.required, "v must stay optional");
}

#[tokio::test]
async fn drop_identifier_fields_replaces_the_set_and_keeps_requiredness() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.dropped (id BIGINT NOT NULL, k STRING NOT NULL) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.dropped SET IDENTIFIER FIELDS id, k",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.dropped DROP IDENTIFIER FIELDS k",
    )
    .await;
    let table = load_sales_table(&catalogs, "dropped").await;
    let id = field_of(&table, "id");
    let k = field_of(&table, "k");
    assert_eq!(
        identifier_ids(&table),
        HashSet::from([id.id]),
        "identifier fields must be exactly id after dropping k"
    );
    assert!(id.required, "id must stay required");
    assert!(
        k.required,
        "dropping an identifier must not make k optional"
    );
}

#[tokio::test]
async fn set_identifier_fields_refuses_a_nullable_column_and_changes_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nullable (id BIGINT NOT NULL, k STRING NOT NULL, v STRING) USING iceberg",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nullable SET IDENTIFIER FIELDS v",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "External error: Cannot add field v as an identifier field: not a required field"
    );
    let table = load_sales_table(&catalogs, "nullable").await;
    let v = field_of(&table, "v");
    assert!(
        identifier_ids(&table).is_empty(),
        "the refused SET must commit nothing"
    );
    assert!(!v.required, "the refused SET must not make v required");
}

#[tokio::test]
async fn set_and_drop_identifier_fields_refuse_unknown_columns() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.unknown (id BIGINT NOT NULL, k STRING NOT NULL) USING iceberg",
    )
    .await;
    let set_error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.unknown SET IDENTIFIER FIELDS nope",
    )
    .await
    .unwrap_err();
    assert_eq!(
        set_error.to_string(),
        "External error: Cannot add field nope as an identifier field: not found in current schema or added columns"
    );
    let drop_error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.unknown DROP IDENTIFIER FIELDS nope",
    )
    .await
    .unwrap_err();
    assert_eq!(
        drop_error.to_string(),
        "External error: Cannot add field nope as an identifier field: not found in current schema or added columns"
    );
    let table = load_sales_table(&catalogs, "unknown").await;
    assert!(
        identifier_ids(&table).is_empty(),
        "the refused statements must commit nothing"
    );
}

#[tokio::test]
async fn set_identifier_fields_refuses_float_and_double_columns() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.floats (f FLOAT NOT NULL, d DOUBLE NOT NULL, id BIGINT NOT NULL) USING iceberg",
    )
    .await;
    for column in ["f", "d"] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!("ALTER TABLE ice.sales.floats SET IDENTIFIER FIELDS {column}"),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "External error: Cannot add field {column} as an identifier field: must not be \
                 float or double field"
            )
        );
    }
    let table = load_sales_table(&catalogs, "floats").await;
    assert!(
        identifier_ids(&table).is_empty(),
        "the refused SETs must commit nothing"
    );
}

#[tokio::test]
async fn set_identifier_fields_reports_required_before_float_or_double() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nullfloat (f FLOAT, id BIGINT NOT NULL) USING iceberg",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nullfloat SET IDENTIFIER FIELDS f",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "External error: Cannot add field f as an identifier field: not a required field"
    );
    let table = load_sales_table(&catalogs, "nullfloat").await;
    assert!(
        identifier_ids(&table).is_empty(),
        "the refused SET must commit nothing"
    );
}

#[tokio::test]
async fn set_identifier_fields_refuses_map_and_list_nesting() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nested (m MAP<STRING, STRING>, arr ARRAY<STRING>, id BIGINT NOT NULL) USING iceberg",
    )
    .await;
    let key_error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested SET IDENTIFIER FIELDS m.key",
    )
    .await
    .unwrap_err();
    assert_eq!(
        key_error.to_string(),
        "External error: Cannot add field key as an identifier field: must not be nested in \
         1: m: optional map<string, string>"
    );
    let value_error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested SET IDENTIFIER FIELDS m.value",
    )
    .await
    .unwrap_err();
    assert_eq!(
        value_error.to_string(),
        "External error: Cannot add field value as an identifier field: not a required field"
    );
    let element_error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested SET IDENTIFIER FIELDS arr.element",
    )
    .await
    .unwrap_err();
    assert_eq!(
        element_error.to_string(),
        "External error: Cannot add field element as an identifier field: not a required field"
    );
    let table = load_sales_table(&catalogs, "nested").await;
    assert!(
        identifier_ids(&table).is_empty(),
        "the refused SETs must commit nothing"
    );
}

#[tokio::test]
async fn set_identifier_fields_refuses_optional_struct_nesting_with_full_parent() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.optstruct (s STRUCT<a: BIGINT NOT NULL>, id BIGINT NOT NULL) \
         USING iceberg",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.optstruct SET IDENTIFIER FIELDS s.a",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "External error: Cannot add field a as an identifier field: must not be nested in an \
         optional field 1: s: optional struct<3: a: required long>"
    );
    let table = load_sales_table(&catalogs, "optstruct").await;
    assert!(
        identifier_ids(&table).is_empty(),
        "the refused SET must commit nothing"
    );
}

#[tokio::test]
async fn set_identifier_fields_refuses_non_primitive_columns() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.complex (m MAP<STRING, STRING>, id BIGINT NOT NULL) USING iceberg",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.complex SET IDENTIFIER FIELDS m",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "External error: Cannot add field m as an identifier field: not a primitive type field"
    );
    let table = load_sales_table(&catalogs, "complex").await;
    assert!(
        identifier_ids(&table).is_empty(),
        "the refused SET must commit nothing"
    );
}
