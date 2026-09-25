use super::super::*;
use super::common::*;
use tempfile::TempDir;

fn sales(table: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("sales".into()), table.into())
}

async fn exists(catalogs: &CatalogRegistry, ident: &TableIdent) -> bool {
    catalog_handle(catalogs, "ice")
        .unwrap()
        .table_exists(ident)
        .await
        .unwrap()
}

async fn seeded(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} (id BIGINT, data STRING) USING iceberg"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (1, 'a')"),
    )
    .await;
}

#[tokio::test]
async fn a_catalog_qualified_target_is_a_missing_namespace_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for (table, target, namespace) in [
        ("r3", "ice.sales.r3b", "ice.sales"),
        ("rc", "other.sales.rcb", "other.sales"),
        ("rn", "nope.rnb", "nope"),
        ("rfour", "x.y.z.w", "x.y.z"),
    ] {
        seeded(&ctx, &catalogs, table).await;
        let error = execute(
            &ctx,
            &catalogs,
            &format!("ALTER TABLE ice.sales.{table} RENAME TO {target}"),
        )
        .await
        .unwrap_err();
        let DataFusionError::Execution(message) = &error else {
            panic!("{target}: expected an execution error, got {error:?}");
        };
        assert_eq!(
            message,
            &format!(
                "Cannot rename sales.{table} to {target}. Namespace does not exist: {namespace}"
            )
        );
        assert_eq!(
            rows(&ctx, &catalogs, &format!("SELECT * FROM ice.sales.{table}")).await,
            1
        );
    }
}

#[tokio::test]
async fn a_catalog_qualified_target_moves_into_an_existing_nested_namespace() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let handle = catalog_handle(&catalogs, "ice").unwrap();
    let nested = NamespaceIdent::from_strs(["ice", "nested"]).unwrap();
    handle
        .create_namespace(&NamespaceIdent::new("ice".into()), HashMap::new())
        .await
        .unwrap();
    handle
        .create_namespace(&nested, HashMap::new())
        .await
        .unwrap();
    seeded(&ctx, &catalogs, "rq").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rq RENAME TO ice.nested.rqb",
    )
    .await;
    let listed = handle.list_tables(&nested).await.unwrap();
    assert_eq!(listed, vec![TableIdent::new(nested.clone(), "rqb".into())]);
    assert!(!exists(&catalogs, &sales("rq")).await);
    assert!(!exists(&catalogs, &sales("rqb")).await);
}

#[tokio::test]
async fn short_and_other_namespace_targets_rename_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "CREATE NAMESPACE ice.ns2").await;
    seeded(&ctx, &catalogs, "r2").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.r2 RENAME TO sales.r2b",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.r2b").await,
        1
    );
    seeded(&ctx, &catalogs, "r1").await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.r1 RENAME TO r1b").await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.r1b").await,
        1
    );
    seeded(&ctx, &catalogs, "rx").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rx RENAME TO ns2.rxb",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.ns2.rxb").await, 1);
    for table in ["r2", "r1", "rx"] {
        assert!(!exists(&catalogs, &sales(table)).await, "{table}");
    }
}

#[tokio::test]
async fn an_existing_target_answers_spark_already_exists_text() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seeded(&ctx, &catalogs, "re").await;
    seeded(&ctx, &catalogs, "re2").await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.re RENAME TO sales.re2",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view \
         sales.re2 because it already exists.\nChoose a different name, drop or replace the \
         existing object, or add the IF NOT EXISTS clause to tolerate pre-existing objects. \
         SQLSTATE: 42P07"
    );
    assert!(exists(&catalogs, &sales("re")).await);
}

#[tokio::test]
async fn a_missing_source_keeps_the_catalog_answer() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(&ctx, &catalogs, "ALTER TABLE ice.ghost.t RENAME TO ghost.u")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("No such namespace"), "{error}");
    assert!(!error.contains("Cannot rename"), "{error}");
}
