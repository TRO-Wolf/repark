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

fn current_of(ctx: &SessionContext) -> (String, String) {
    session_defaults(ctx)
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
    assert_eq!(current_of(&ctx), ("ice".to_string(), "sales".to_string()));
}

#[tokio::test]
async fn use_catalog_only_clears_v2_namespace_to_empty() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = two_catalog_setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "USE duo").await;
    assert_eq!(current_of(&ctx), ("duo".to_string(), String::new()));
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
        current_of(&ctx),
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
    assert_eq!(current_of(&ctx), ("duo".to_string(), String::new()));
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
    assert_eq!(current_of(&ctx), ("ice".to_string(), "second".to_string()));
}

#[tokio::test]
async fn use_self_catalog_keeps_namespace() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "USE ice").await;
    assert_eq!(current_of(&ctx), ("ice".to_string(), "sales".to_string()));
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
    assert_eq!(current_of(&ctx), ("ice".to_string(), "sales".to_string()));
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
    assert_eq!(current_of(&ctx), ("ice".to_string(), "second".to_string()));
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

#[test]
fn complete_name_expands_one_and_two_part_names() {
    let ctx = SessionContext::new();
    crate::use_ddl::set_session_defaults(&ctx, "ice", "sales");
    assert_eq!(
        complete_name(&ctx, &["t".to_string()]).unwrap(),
        vec!["ice", "sales", "t"]
    );
    assert_eq!(
        complete_name(&ctx, &["ns".to_string(), "t".to_string()]).unwrap(),
        vec!["ice", "ns", "t"]
    );
    assert_eq!(
        complete_name(&ctx, &["c".to_string(), "n".to_string(), "t".to_string()]).unwrap(),
        vec!["c", "n", "t"]
    );
}

#[test]
fn complete_name_bare_table_under_empty_namespace_is_not_found() {
    let ctx = SessionContext::new();
    crate::use_ddl::set_session_defaults(&ctx, "ice", "");
    let error = complete_name(&ctx, &["t".to_string()]).unwrap_err();
    let text = error.to_string();
    assert!(
        text.contains("[TABLE_OR_VIEW_NOT_FOUND]") && text.contains("`t`"),
        "got: {text}"
    );
    assert_eq!(
        complete_name(&ctx, &["ns".to_string(), "t".to_string()]).unwrap(),
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
