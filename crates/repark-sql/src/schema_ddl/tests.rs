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

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::{Catalog, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use repark_iceberg::catalog::{memory_catalog, register_iceberg_catalog};
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;

struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    _warehouse_dir: TempDir,
}

impl Door {
    async fn sql(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        let read_only = HashSet::new();
        let frame = crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await?;
        frame.collect().await
    }

    async fn ok(&self, sql: &str) -> Vec<RecordBatch> {
        self.sql(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"))
    }

    async fn err(&self, sql: &str) -> String {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(err) => err.to_string(),
        }
    }

    async fn table_exists(&self, namespace: &str, table: &str) -> bool {
        self.catalog
            .table_exists(&TableIdent::new(
                NamespaceIdent::new(namespace.to_string()),
                table.to_string(),
            ))
            .await
            .expect("table_exists")
    }
}

async fn door() -> Door {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir
        .path()
        .to_str()
        .expect("utf8 warehouse")
        .to_string();
    let catalog: Arc<dyn Catalog> = memory_catalog(&warehouse).await.expect("memory catalog");
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    register_iceberg_catalog(&ctx, "ice", Arc::clone(&catalog))
        .await
        .expect("register catalog");
    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "ice".to_string(),
        Arc::clone(&catalog),
        LocationPolicy::TempFallbackAllowed {
            root: warehouse_dir.path().to_path_buf(),
        },
    );
    catalogs.note_local_warehouse_root(&warehouse);
    Door {
        ctx,
        catalogs,
        catalog,
        _warehouse_dir: warehouse_dir,
    }
}

#[tokio::test]
async fn drop_schema_nonempty_refuses_and_keeps_everything() {
    let door = door().await;
    door.ok("CREATE SCHEMA ice.bronze").await;
    door.ok("CREATE TABLE ice.bronze.orders AS SELECT 1 AS id")
        .await;
    let error = door.err("DROP SCHEMA ice.bronze").await;
    assert!(
        error.contains("Namespace bronze is not empty."),
        "the refusal must name the namespace like Spark: {error}"
    );
    assert!(
        error.contains("Contains 1 table(s)."),
        "the refusal must name the table count like Spark: {error}"
    );
    assert!(
        door.catalog
            .namespace_exists(&NamespaceIdent::new("bronze".to_string()))
            .await
            .expect("namespace_exists"),
        "a refused drop must leave the namespace behind"
    );
    assert!(
        door.table_exists("bronze", "orders").await,
        "a refused drop must leave the table readable"
    );
}

#[tokio::test]
async fn drop_schema_if_exists_and_cascade_still_refuse_a_nonempty_schema() {
    let door = door().await;
    door.ok("CREATE SCHEMA ice.bronze").await;
    door.ok("CREATE TABLE ice.bronze.orders AS SELECT 1 AS id")
        .await;
    for statement in [
        "DROP SCHEMA IF EXISTS ice.bronze",
        "DROP SCHEMA ice.bronze CASCADE",
        "DROP SCHEMA IF EXISTS ice.bronze CASCADE",
        "DROP DATABASE ice.bronze",
    ] {
        let error = door.err(statement).await;
        assert!(
            error.contains("Namespace bronze is not empty. Contains 1 table(s)."),
            "{statement} must refuse like Spark: {error}"
        );
    }
    assert!(
        door.table_exists("bronze", "orders").await,
        "a refused drop must leave the table readable"
    );
}

#[tokio::test]
async fn drop_schema_missing_and_empty_cascade_answer_like_spark() {
    let door = door().await;
    let missing = door.err("DROP SCHEMA ice.bronze").await;
    assert!(
        missing.contains("[SCHEMA_NOT_FOUND] The schema `ice`.`bronze` cannot be found.")
            && missing.contains("To tolerate the error on drop use DROP SCHEMA IF EXISTS."),
        "a missing schema must refuse like Spark: {missing}"
    );
    door.ok("CREATE SCHEMA ice.silver").await;
    door.ok("DROP SCHEMA ice.silver CASCADE").await;
    assert!(
        !door
            .catalog
            .namespace_exists(&NamespaceIdent::new("silver".to_string()))
            .await
            .expect("namespace_exists"),
        "CASCADE drops an empty namespace as Spark does"
    );
}

#[tokio::test]
async fn drop_schema_after_table_drop_drops() {
    let door = door().await;
    door.ok("CREATE SCHEMA ice.bronze").await;
    door.ok("CREATE TABLE ice.bronze.orders AS SELECT 1 AS id")
        .await;
    door.ok("DROP TABLE ice.bronze.orders").await;
    door.ok("DROP SCHEMA ice.bronze").await;
    assert!(
        !door
            .catalog
            .namespace_exists(&NamespaceIdent::new("bronze".to_string()))
            .await
            .expect("namespace_exists"),
        "a namespace emptied by an explicit table drop must drop"
    );
}
