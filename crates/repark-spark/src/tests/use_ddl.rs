use datafusion::arrow::array::BooleanArray;

use super::super::*;
use super::common::*;
use crate::use_ddl::{complete_name, default_namespace_for_catalog, session_defaults};

async fn two_catalog_setup(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, mut catalogs) = setup(wh).await;
    let warehouse = format!("{}/second", wh.path().to_str().unwrap());
    std::fs::create_dir_all(&warehouse).unwrap();
    let second: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await
            .unwrap(),
    );
    second
        .create_namespace(&NamespaceIdent::new("other".to_string()), HashMap::new())
        .await
        .unwrap();
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "duo", second.clone())
        .await
        .unwrap();
    catalogs.insert(
        "duo".to_string(),
        second,
        LocationPolicy::TempFallbackAllowed {
            root: std::env::temp_dir(),
        },
    );
    (ctx, catalogs)
}

fn current_of(catalogs: &CatalogRegistry) -> (String, String) {
    session_defaults(catalogs)
}

async fn run(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) {
    execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} must run: {error}"))
        .collect()
        .await
        .unwrap();
}

#[tokio::test]
async fn use_two_part_sets_catalog_and_namespace() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    assert_eq!(
        current_of(&catalogs),
        ("ice".to_string(), "sales".to_string())
    );
}

#[tokio::test]
async fn use_catalog_only_clears_v2_namespace_to_empty() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = two_catalog_setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "USE duo").await;
    assert_eq!(current_of(&catalogs), ("duo".to_string(), String::new()));
}

#[tokio::test]
async fn use_catalog_onto_session_catalog_restores_default() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    let warehouse = format!("{}/spark", wh.path().to_str().unwrap());
    std::fs::create_dir_all(&warehouse).unwrap();
    let session_like: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await
            .unwrap(),
    );
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "spark_catalog", session_like.clone())
        .await
        .unwrap();
    catalogs.insert(
        "spark_catalog".to_string(),
        session_like,
        LocationPolicy::TempFallbackAllowed {
            root: std::env::temp_dir(),
        },
    );
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "USE spark_catalog").await;
    assert_eq!(
        current_of(&catalogs),
        ("spark_catalog".to_string(), "default".to_string())
    );
    assert_eq!(default_namespace_for_catalog("spark_catalog"), "default");
    assert_eq!(default_namespace_for_catalog("ice"), "");
}

#[tokio::test]
async fn use_one_part_prefers_catalog_over_namespace() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = two_catalog_setup(&wh).await;
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.duo")
        .await
        .unwrap();
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "USE duo").await;
    assert_eq!(current_of(&catalogs), ("duo".to_string(), String::new()));
}

#[tokio::test]
async fn use_one_part_namespace_in_current_catalog() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = two_catalog_setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.second")
        .await
        .unwrap();
    run(&ctx, &catalogs, "USE second").await;
    assert_eq!(
        current_of(&catalogs),
        ("ice".to_string(), "second".to_string())
    );
}

#[tokio::test]
async fn use_self_catalog_keeps_namespace() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "USE ice").await;
    assert_eq!(
        current_of(&catalogs),
        ("ice".to_string(), "sales".to_string())
    );
}

#[tokio::test]
async fn use_missing_one_part_names_current_catalog_and_part() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    let error = execute(&ctx, &catalogs, "USE nosuch")
        .await
        .expect_err("missing name must refuse");
    let text = error.to_string();
    assert!(
        text.contains("[SCHEMA_NOT_FOUND]") && text.contains("`ice`.`nosuch`"),
        "got: {text}"
    );
    assert_eq!(
        current_of(&catalogs),
        ("ice".to_string(), "sales".to_string())
    );
}

#[tokio::test]
async fn use_two_part_missing_namespace_names_both_parts() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(&ctx, &catalogs, "USE ice.nosuchns")
        .await
        .expect_err("missing namespace must refuse");
    let text = error.to_string();
    assert!(
        text.contains("[SCHEMA_NOT_FOUND]") && text.contains("`ice`.`nosuchns`"),
        "got: {text}"
    );
}

#[tokio::test]
async fn use_two_part_unknown_first_names_current_and_both() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    let error = execute(&ctx, &catalogs, "USE missingcat.ns")
        .await
        .expect_err("unknown first part must refuse");
    let text = error.to_string();
    assert!(
        text.contains("[SCHEMA_NOT_FOUND]") && text.contains("`ice`.`missingcat`.`ns`"),
        "got: {text}"
    );
}

#[tokio::test]
async fn use_database_spelling_is_namespace_only() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = two_catalog_setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    let error = execute(&ctx, &catalogs, "USE DATABASE duo.other")
        .await
        .expect_err("DATABASE spelling must not resolve a catalog");
    let text = error.to_string();
    assert!(
        text.contains("[SCHEMA_NOT_FOUND]") && text.contains("`ice`.`duo`.`other`"),
        "got: {text}"
    );
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.second")
        .await
        .unwrap();
    run(&ctx, &catalogs, "USE SCHEMA second").await;
    assert_eq!(
        current_of(&catalogs),
        ("ice".to_string(), "second".to_string())
    );
}

#[tokio::test]
async fn use_catalog_spelling_is_a_parse_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(&ctx, &catalogs, "USE CATALOG ice")
        .await
        .expect_err("USE CATALOG must refuse like Spark");
    assert!(matches!(error, DataFusionError::SQL(_, _)), "got: {error}");
}

#[tokio::test]
async fn use_default_reaches_the_use_arm() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    assert!(crate::use_ddl::is_use_default("USE DEFAULT"));
    assert!(crate::use_ddl::is_use_default("  use   default  ;  "));
    assert!(!crate::use_ddl::is_use_default("USE DEFAULTX"));
    assert!(!crate::use_ddl::is_use_default("USEDEFAULT"));
    assert!(!crate::use_ddl::is_use_default("USE ice.sales"));
    let error = execute(&ctx, &catalogs, "USE DEFAULT")
        .await
        .expect_err("DEFAULT matches no test namespace");
    let text = error.to_string();
    assert!(
        text.contains("[SCHEMA_NOT_FOUND]") && text.contains("`DEFAULT`"),
        "got: {text}"
    );
}

#[tokio::test]
async fn use_returns_no_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let batches = execute(&ctx, &catalogs, "USE ice.sales")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(batches.iter().all(|batch| batch.num_rows() == 0));
}

async fn table_exists(
    catalogs: &CatalogRegistry,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> bool {
    let handle = catalogs.get(catalog).unwrap();
    handle
        .table_exists(&iceberg::TableIdent::new(
            NamespaceIdent::new(namespace.to_string()),
            table.to_string(),
        ))
        .await
        .unwrap()
}

#[tokio::test]
async fn alter_source_and_rename_dest_complete_short_names() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "CREATE TABLE ice.sales.t (id INT)").await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "ALTER TABLE t RENAME TO t2").await;
    assert!(!table_exists(&catalogs, "ice", "sales", "t").await);
    assert!(table_exists(&catalogs, "ice", "sales", "t2").await);
}

#[tokio::test]
async fn rename_two_part_dest_anchors_on_the_source_catalog() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "CREATE TABLE ice.sales.t (id INT)").await;
    run(&ctx, &catalogs, "CREATE NAMESPACE ice.ns2").await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.t RENAME TO ns2.t2").await;
    assert!(table_exists(&catalogs, "ice", "ns2", "t2").await);
}

#[tokio::test]
async fn rename_three_part_dest_reads_the_catalog_as_a_namespace() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = two_catalog_setup(&wh).await;
    run(&ctx, &catalogs, "CREATE TABLE ice.sales.t (id INT)").await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t RENAME TO duo.other.t2",
    )
    .await
    .expect_err("a three-part RENAME target is a namespace inside the source catalog");
    assert_eq!(
        error.to_string(),
        "Execution error: Cannot rename sales.t to duo.other.t2. Namespace does not exist: \
         duo.other"
    );
    assert!(table_exists(&catalogs, "ice", "sales", "t").await);
}

#[tokio::test]
async fn create_and_drop_complete_short_names() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "CREATE TABLE t (id BIGINT)").await;
    assert!(table_exists(&catalogs, "ice", "sales", "t").await);
    run(&ctx, &catalogs, "DROP TABLE t").await;
    assert!(!table_exists(&catalogs, "ice", "sales", "t").await);
}

#[tokio::test]
async fn ctas_completes_short_names() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "CREATE TABLE t AS SELECT 1 AS id").await;
    assert!(table_exists(&catalogs, "ice", "sales", "t").await);
}

#[tokio::test]
async fn call_two_part_resolves_current_catalog() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "CREATE TABLE ice.sales.t (id INT)").await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    let error = execute(&ctx, &catalogs, "CALL system.nosuchproc('sales.t')")
        .await
        .expect_err("unknown procedure must refuse");
    assert!(
        error
            .to_string()
            .contains("CALL system.nosuchproc is not supported"),
        "got: {error}"
    );
}

async fn string_column(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    column: usize,
) -> (Vec<String>, String, bool) {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut values = Vec::new();
    let mut name = String::new();
    let mut nullable = true;
    for batch in &batches {
        let field = batch.schema().field(column).clone();
        name = field.name().clone();
        nullable = field.is_nullable();
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.push(array.value(row).to_string());
        }
    }
    (values, name, nullable)
}

async fn show_tables_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<(String, String, bool)> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let namespaces = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tables = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let temporary = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                namespaces.value(row).to_string(),
                tables.value(row).to_string(),
                temporary.value(row),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn show_catalogs_lists_registered_sorted_with_session_catalog() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = two_catalog_setup(&wh).await;
    let (values, name, nullable) = string_column(&ctx, &catalogs, "SHOW CATALOGS", 0).await;
    assert_eq!(values, vec!["duo", "ice", "spark_catalog"]);
    assert_eq!(name, "catalog");
    assert!(!nullable);
}

#[tokio::test]
async fn show_catalogs_like_filters() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = two_catalog_setup(&wh).await;
    let (values, _, _) = string_column(&ctx, &catalogs, "SHOW CATALOGS LIKE 'i*'", 0).await;
    assert_eq!(values, vec!["ice"]);
}

#[tokio::test]
async fn show_tables_lists_current_namespace_after_use() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "CREATE TABLE ice.sales.t (id INT)").await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    let rows = show_tables_rows(&ctx, &catalogs, "SHOW TABLES").await;
    assert_eq!(rows, vec![("sales".to_string(), "t".to_string(), false)]);
    let batches = execute(&ctx, &catalogs, "SHOW TABLES")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "namespace");
    assert_eq!(schema.field(1).name(), "tableName");
    assert_eq!(schema.field(2).name(), "isTemporary");
    assert!(!schema.field(2).is_nullable());
}

#[tokio::test]
async fn show_tables_like_and_in_forms() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "CREATE TABLE ice.sales.t (id INT)").await;
    run(&ctx, &catalogs, "CREATE TABLE ice.sales.t2 (id INT)").await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    let rows = show_tables_rows(&ctx, &catalogs, "SHOW TABLES LIKE 't*'").await;
    assert_eq!(rows.len(), 2);
    let rows = show_tables_rows(&ctx, &catalogs, "SHOW TABLES IN ice.sales").await;
    assert_eq!(rows.len(), 2);
    let rows = show_tables_rows(&ctx, &catalogs, "SHOW TABLES FROM sales").await;
    assert_eq!(rows.len(), 2);
    let rows = show_tables_rows(&ctx, &catalogs, "SHOW TABLES LIKE 't2'").await;
    assert_eq!(rows, vec![("sales".to_string(), "t2".to_string(), false)]);
}

#[tokio::test]
async fn show_tables_missing_explicit_namespace_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    let error = execute(&ctx, &catalogs, "SHOW TABLES IN nosuch")
        .await
        .expect_err("explicit missing namespace must refuse");
    assert!(
        error.to_string().contains("[SCHEMA_NOT_FOUND]"),
        "got: {error}"
    );
}

#[tokio::test]
async fn show_tables_empty_ambient_scope_is_empty() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = two_catalog_setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "USE duo").await;
    let rows = show_tables_rows(&ctx, &catalogs, "SHOW TABLES").await;
    assert!(rows.is_empty());
    run(&ctx, &catalogs, "USE ice.sales").await;
    let rows = show_tables_rows(&ctx, &catalogs, "SHOW TABLES IN duo").await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn show_columns_answers_declaration_order() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (z INT, a STRING, m BIGINT)",
    )
    .await;
    let expected = vec!["z".to_string(), "a".to_string(), "m".to_string()];
    let (values, name, nullable) =
        string_column(&ctx, &catalogs, "SHOW COLUMNS IN ice.sales.t", 0).await;
    assert_eq!(values, expected);
    assert_eq!(name, "col_name");
    assert!(!nullable);
    let (values, _, _) = string_column(&ctx, &catalogs, "SHOW COLUMNS FROM ice.sales.t", 0).await;
    assert_eq!(values, expected);
    run(&ctx, &catalogs, "USE ice.sales").await;
    let (values, _, _) = string_column(&ctx, &catalogs, "SHOW COLUMNS IN t", 0).await;
    assert_eq!(values, expected);
    let (values, _, _) = string_column(&ctx, &catalogs, "SHOW COLUMNS IN sales.t", 0).await;
    assert_eq!(values, expected);
}

#[tokio::test]
async fn show_columns_missing_table_is_not_found() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(&ctx, &catalogs, "SHOW COLUMNS IN ice.sales.nothere")
        .await
        .expect_err("missing table must refuse");
    let text = error.to_string();
    assert!(
        text.contains("[TABLE_OR_VIEW_NOT_FOUND]")
            && text.contains("`ice`.`sales`.`nothere`")
            && text.contains("SQLSTATE: 42P01"),
        "got: {text}"
    );
}

#[tokio::test]
async fn show_columns_like_is_a_parse_refusal() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "CREATE TABLE ice.sales.t (id INT)").await;
    let error = execute(&ctx, &catalogs, "SHOW COLUMNS IN ice.sales.t LIKE 'i%'")
        .await
        .expect_err("SHOW COLUMNS LIKE must refuse like Spark");
    assert!(matches!(error, DataFusionError::SQL(_, _)), "got: {error}");
}

#[tokio::test]
async fn show_namespaces_bare_lists_current_catalog() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = two_catalog_setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    let (values, _, _) = string_column(&ctx, &catalogs, "SHOW NAMESPACES", 0).await;
    assert_eq!(values, vec!["sales".to_string()]);
    let (values, _, _) = string_column(&ctx, &catalogs, "SHOW SCHEMAS", 0).await;
    assert_eq!(values, vec!["sales".to_string()]);
    run(&ctx, &catalogs, "USE duo.other").await;
    let (values, _, _) = string_column(&ctx, &catalogs, "SHOW DATABASES", 0).await;
    assert_eq!(values, vec!["other".to_string()]);
}

#[test]
fn complete_name_expands_one_and_two_part_names() {
    let catalogs = CatalogRegistry::new();
    catalogs.set_defaults("ice", "sales");
    assert_eq!(
        complete_name(&catalogs, &["t".to_string()]).unwrap(),
        vec!["ice", "sales", "t"]
    );
    assert_eq!(
        complete_name(&catalogs, &["ns".to_string(), "t".to_string()]).unwrap(),
        vec!["ice", "ns", "t"]
    );
    assert_eq!(
        complete_name(
            &catalogs,
            &["c".to_string(), "n".to_string(), "t".to_string()]
        )
        .unwrap(),
        vec!["c", "n", "t"]
    );
}

#[test]
fn complete_name_bare_table_under_empty_namespace_is_not_found() {
    let catalogs = CatalogRegistry::new();
    catalogs.set_defaults("ice", "");
    let error = complete_name(&catalogs, &["t".to_string()]).unwrap_err();
    let text = error.to_string();
    assert!(
        text.contains("[TABLE_OR_VIEW_NOT_FOUND]") && text.contains("`ice`.``.`t`"),
        "got: {text}"
    );
    assert_eq!(
        complete_name(&catalogs, &["ns".to_string(), "t".to_string()]).unwrap(),
        vec!["ice", "ns", "t"]
    );
}

#[test]
fn show_and_cache_statement_variants_parse_as_expected() {
    let dialect = DatabricksDialect {};
    let parse = |sql: &str| {
        Parser::parse_sql(&dialect, sql)
            .unwrap_or_else(|error| panic!("{sql} must parse: {error}"))
            .remove(0)
    };
    assert!(matches!(
        parse("SHOW CATALOGS"),
        Statement::ShowCatalogs { .. }
    ));
    assert!(matches!(
        parse("SHOW COLUMNS IN ice.sales.t"),
        Statement::ShowColumns { .. }
    ));
    assert!(matches!(
        parse("SHOW TABLES LIKE 't*'"),
        Statement::ShowTables { .. }
    ));
    assert!(matches!(
        parse("CACHE TABLE ice.sales.t"),
        Statement::Cache { .. }
    ));
    assert!(matches!(
        parse("UNCACHE TABLE ice.sales.t"),
        Statement::UNCache { .. }
    ));
    assert!(matches!(
        parse("USE ice.sales"),
        Statement::Use(datafusion::sql::sqlparser::ast::Use::Object(_))
    ));
    assert!(matches!(
        parse("USE CATALOG ice"),
        Statement::Use(datafusion::sql::sqlparser::ast::Use::Catalog(_))
    ));
    assert!(Parser::parse_sql(&dialect, "REFRESH TABLE ice.sales.t").is_err());
    assert!(Parser::parse_sql(&dialect, "REFRESH ice.sales.t").is_err());
    let call = parse("CALL system.rollback_to_snapshot('ns.t', 1)");
    let Statement::Call(function) = call else {
        panic!("CALL must parse as Statement::Call");
    };
    assert_eq!(function.name.to_string(), "system.rollback_to_snapshot");
}

#[tokio::test]
async fn refresh_table_rebuilds_provider_and_answers_empty() {
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "CREATE TABLE ice.sales.t (id INT)").await;
    let schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();
    catalogs["ice"]
        .create_table(
            &NamespaceIdent::new("sales".to_string()),
            TableCreation::builder()
                .name("oob".to_string())
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();
    let names = ctx
        .catalog("ice")
        .unwrap()
        .schema("sales")
        .unwrap()
        .table_names();
    assert!(!names.iter().any(|name| name == "oob"));
    execute(&ctx, &catalogs, "SELECT * FROM ice.sales.oob")
        .await
        .expect_err("the stale provider must miss the out-of-band table");
    let batches = execute(&ctx, &catalogs, "REFRESH TABLE ice.sales.oob")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(batches.iter().all(|batch| batch.num_rows() == 0));
    let names = ctx
        .catalog("ice")
        .unwrap()
        .schema("sales")
        .unwrap()
        .table_names();
    assert!(names.iter().any(|name| name == "oob"));
    let rows = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.oob")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(rows.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn refresh_table_without_table_keyword_ok() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "CREATE TABLE ice.sales.t (id INT)").await;
    run(&ctx, &catalogs, "REFRESH ice.sales.t").await;
}

#[tokio::test]
async fn refresh_missing_table_is_not_found() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(&ctx, &catalogs, "REFRESH TABLE ice.sales.nothere")
        .await
        .unwrap_err();
    let text = error.to_string();
    assert!(
        text.contains("[TABLE_OR_VIEW_NOT_FOUND]")
            && text.contains("`ice`.`sales`.`nothere`")
            && text.contains("SQLSTATE: 42P01"),
        "got: {text}"
    );
}

#[tokio::test]
async fn refresh_temp_view_is_ok() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "REFRESH src").await;
}

#[tokio::test]
async fn refresh_path_is_ok() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "REFRESH '/tmp/nonewhere'").await;
}

#[test]
fn refresh_parse_accepts_table_and_path_forms_only() {
    assert!(crate::use_ddl::try_parse_refresh("REFRESH TABLE ice.sales.t").is_some());
    assert!(crate::use_ddl::try_parse_refresh("refresh t").is_some());
    assert!(crate::use_ddl::try_parse_refresh("REFRESH '/tmp/x'").is_some());
    assert!(crate::use_ddl::try_parse_refresh("SELECT 1").is_none());
    assert!(crate::use_ddl::try_parse_refresh("REFRESH TABLE").is_none());
    assert!(crate::use_ddl::try_parse_refresh("REFRESH TABLE t PARTITION (p = 1)").is_none());
    assert!(crate::use_ddl::try_parse_refresh("REFRESH RESOURCE x").is_none());
}
