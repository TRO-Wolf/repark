//! Catalog-DDL helper tests cover schema `WITH (…)` vocabulary, name qualification, and identifier hygiene.

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::*;

/// Parse `CREATE SCHEMA c.s WITH (…)` and hand back the option list.
fn options_of(with_clause: &str) -> Vec<SqlOption> {
    let sql = format!("CREATE SCHEMA c.s WITH ({with_clause})");
    let mut statements = Parser::parse_sql(&GenericDialect {}, &sql)
        .unwrap_or_else(|err| panic!("fixture must parse (`{sql}`): {err}"));
    match statements.remove(0) {
        Statement::CreateSchema { with, .. } => with.unwrap_or_default(),
        other => panic!("fixture must be a CREATE SCHEMA, got {other:?}"),
    }
}

fn object_name(sql_name: &str) -> ObjectName {
    let sql = format!("DROP TABLE {sql_name}");
    let mut statements = Parser::parse_sql(&GenericDialect {}, &sql).expect("fixture parses");
    match statements.remove(0) {
        Statement::Drop { mut names, .. } => names.remove(0),
        other => panic!("fixture must be a DROP, got {other:?}"),
    }
}

/// `location` is accepted, under either case, and normalized to the canonical key.
#[test]
fn location_property_is_accepted() {
    for spelling in ["location", "LOCATION", "Location"] {
        let properties = schema_properties(&options_of(&format!("{spelling} = 's3://bucket/s'")))
            .unwrap_or_else(|err| panic!("`{spelling}` must be accepted: {err}"));
        assert_eq!(
            properties.get("location").map(String::as_str),
            Some("s3://bucket/s"),
            "`{spelling}` must normalize to `location`"
        );
    }
}

/// A schema with no properties is legal.
#[test]
fn no_properties_is_legal() {
    assert!(schema_properties(&[]).expect("empty is fine").is_empty());
}

/// An unknown schema property refuses and lists the supported set.
#[test]
fn unknown_schema_property_refuses_listing_support() {
    let err = schema_properties(&options_of("owner = 'me'"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("owner"), "must name the key: {err}");
    assert!(err.contains("`location`"), "must list support: {err}");
}

/// A non-literal property value refuses.
#[test]
fn non_literal_property_value_refuses() {
    let err = schema_properties(&options_of("location = 42"))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("string literal"),
        "must say what is wanted: {err}"
    );
}

/// A duplicated property refuses rather than last-write-wins.
#[test]
fn duplicate_schema_property_refuses() {
    let err = schema_properties(&options_of("location = 'a', location = 'b'"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("more than once"), "must name the class: {err}");
}

/// `name_parts` reads dotted names, including quoted segments containing spaces.
#[test]
fn name_parts_reads_dotted_and_quoted_identifiers() {
    assert_eq!(name_parts(&object_name("a.b.c")), vec!["a", "b", "c"]);
    assert_eq!(
        name_parts(&object_name(r#""My Cat"."My Schema".t"#)),
        vec!["My Cat", "My Schema", "t"]
    );
}

/// Namespace resolution requires a catalog-qualified name.
#[test]
fn namespace_resolution_requires_qualification() {
    let catalogs = CatalogRegistry::new();
    let (catalog, namespace) =
        resolve_namespace(&catalogs, &object_name("ice.sales"), "CREATE SCHEMA")
            .expect("a two-part name resolves");
    assert_eq!(catalog, "ice");
    assert_eq!(namespace, vec!["sales"]);

    let err = resolve_namespace(&catalogs, &object_name("sales"), "CREATE SCHEMA")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("<catalog>.<schema>"),
        "must show the shape: {err}"
    );
}

/// A read-only catalog gets the direction note, not "unknown catalog".
#[test]
fn read_only_catalog_gets_the_direction_note() {
    let mut catalogs = CatalogRegistry::new();
    catalogs.set_read_only_catalogs(std::collections::HashSet::from(["pg".to_string()]));

    let read_only = catalog_handle(&catalogs, "pg").unwrap_err().to_string();
    assert!(
        read_only.contains("registered read-only"),
        "class: {read_only}"
    );

    let unknown = catalog_handle(&catalogs, "nope").unwrap_err().to_string();
    assert!(unknown.contains("unknown catalog"), "class: {unknown}");
    assert!(
        unknown.contains("not registered"),
        "must explain: {unknown}"
    );
}

/// Identifier hygiene rejects traversal, separators, and empties before a path is composed.
#[test]
fn escaping_identifiers_are_rejected() {
    assert!(reject_path_escape_ident("..", "table").is_err());
    assert!(reject_path_escape_ident("a/b", "table").is_err());
    assert!(reject_path_escape_ident("", "table").is_err());
    reject_path_escape_ident("orders", "table").expect("a plain identifier passes");
}
