use std::sync::Arc;

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};

use super::super::plain::{selection_refs_non_primitive, try_allowed_plain_identity};

fn parse_statement(sql: &str) -> Statement {
    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|error| panic!("{sql:?} must parse: {error}"))
        .remove(0)
}

fn nested_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(
                3,
                "xs_list",
                Type::List(ListType::new(Arc::new(NestedField::optional(
                    4,
                    "element",
                    Type::Primitive(PrimitiveType::Int),
                )))),
            )
            .into(),
            NestedField::optional(
                5,
                "xs_map",
                Type::Map(MapType::new(
                    Arc::new(NestedField::map_key_element(
                        6,
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::map_value_element(
                        7,
                        Type::Primitive(PrimitiveType::Int),
                        false,
                    )),
                )),
            )
            .into(),
            NestedField::optional(
                8,
                "xs_struct",
                Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                    9,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                ))])),
            )
            .into(),
        ])
        .build()
        .expect("nested schema builds")
}

fn needs_fork(sql: &str, schema: &Schema) -> bool {
    let allowed = try_allowed_plain_identity(&parse_statement(sql))
        .unwrap_or_else(|error| panic!("{sql:?}: {error}"))
        .expect("plain delete must claim the predicate");
    selection_refs_non_primitive(
        &allowed.spec.selection_sql,
        &allowed.spec.target_alias,
        schema,
    )
}

#[test]
fn compound_and_over_list_column_needs_fork() {
    assert!(needs_fork(
        "DELETE FROM ice.sales.t WHERE id > 1 AND xs_list IS NULL",
        &nested_schema(),
    ));
}

#[test]
fn compound_or_over_map_column_needs_fork() {
    assert!(needs_fork(
        "DELETE FROM ice.sales.t WHERE xs_map IS NULL OR id = 1",
        &nested_schema(),
    ));
}

#[test]
fn compound_over_struct_column_needs_fork() {
    assert!(needs_fork(
        "DELETE FROM ice.sales.t WHERE id > 1 AND xs_struct IS NULL",
        &nested_schema(),
    ));
}

#[test]
fn bare_is_null_over_nested_column_is_not_plain_identity() {
    assert!(
        try_allowed_plain_identity(&parse_statement(
            "DELETE FROM ice.sales.t WHERE xs_list IS NULL",
        ))
        .expect("bare null test must parse")
        .is_none()
    );
}

#[test]
fn primitive_only_compound_stays_on_identity() {
    assert!(!needs_fork(
        "DELETE FROM ice.sales.t WHERE id > 1 AND name = 'a'",
        &nested_schema(),
    ));
}

#[test]
fn alias_qualified_nested_column_needs_fork() {
    assert!(needs_fork(
        "DELETE FROM ice.sales.t AS t WHERE t.id > 1 AND t.xs_list IS NULL",
        &nested_schema(),
    ));
}

#[test]
fn unknown_column_never_needs_fork() {
    assert!(!needs_fork(
        "DELETE FROM ice.sales.t WHERE id > 1 AND missing IS NULL",
        &nested_schema(),
    ));
}

#[test]
fn plain_where_delete_is_identity_dml() {
    let allowed = try_allowed_plain_identity(&parse_statement(
        "DELETE FROM ice.sales.puredv WHERE id = 0",
    ))
    .expect("plain delete must parse")
    .expect("plain delete must be identity DML");
    assert_eq!(allowed.catalog_name, "ice");
    assert_eq!(allowed.spec.target.name(), "puredv");
    assert!(allowed.spec.assignments.is_none());
}

#[test]
fn plain_where_update_is_not_plain_identity() {
    let allowed = try_allowed_plain_identity(&parse_statement(
        "UPDATE ice.sales.puredv SET name = 'z' WHERE id = 0",
    ))
    .expect("plain update must parse");
    assert!(allowed.is_none());
}

#[test]
fn literal_in_list_delete_is_not_plain_identity() {
    let allowed = try_allowed_plain_identity(&parse_statement(
        "DELETE FROM ice.sales.puredv WHERE id IN (1, 2, 3)",
    ))
    .expect("in-list delete must parse");
    assert!(allowed.is_none());
}

#[test]
fn branch_selector_delete_is_not_plain_identity() {
    let allowed = try_allowed_plain_identity(&parse_statement(
        "DELETE FROM ice.sales.t.branch_b WHERE id = 0",
    ))
    .expect("branch delete must parse");
    assert!(allowed.is_none());
}

#[test]
fn subquery_delete_is_not_plain_identity() {
    let allowed = try_allowed_plain_identity(&parse_statement(
        "DELETE FROM ice.sales.puredv WHERE id IN (SELECT id FROM ice.sales.src)",
    ))
    .expect("subquery delete must parse");
    assert!(allowed.is_none());
}
