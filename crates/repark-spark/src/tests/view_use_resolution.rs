use super::super::*;
use super::common::*;

async fn view_properties(catalogs: &CatalogRegistry, view: &str) -> HashMap<String, String> {
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), view.to_string());
    catalogs["ice"]
        .load_view(&ident)
        .await
        .expect("view must load")
        .metadata()
        .properties()
        .clone()
}

async fn view_exists(catalogs: &CatalogRegistry, view: &str) -> bool {
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), view.to_string());
    catalogs["ice"]
        .view_exists(&ident)
        .await
        .expect("view existence")
}

async fn shown_views(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: &str,
) -> Vec<(String, String, bool)> {
    let batches = execute(ctx, catalogs, statement)
        .await
        .expect("SHOW VIEWS must plan")
        .collect()
        .await
        .expect("SHOW VIEWS must collect");
    batches
        .iter()
        .flat_map(|batch| {
            let namespaces = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("namespace");
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("viewName");
            let temporary = batch
                .column(2)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                .expect("isTemporary");
            (0..batch.num_rows()).map(move |index| {
                (
                    namespaces.value(index).to_string(),
                    names.value(index).to_string(),
                    temporary.value(index),
                )
            })
        })
        .collect()
}

#[tokio::test]
async fn bare_alter_view_set_unset_and_rename_resolve_use_defaults() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v TBLPROPERTIES ('k'='v') AS SELECT * FROM src",
    )
    .await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(&ctx, &catalogs, "ALTER VIEW v SET TBLPROPERTIES ('j'='u')").await;
    run(&ctx, &catalogs, "ALTER VIEW v UNSET TBLPROPERTIES ('k')").await;
    let properties = view_properties(&catalogs, "v").await;
    assert_eq!(properties.get("j"), Some(&"u".to_string()));
    assert_eq!(properties.get("k"), None);
    run(&ctx, &catalogs, "ALTER VIEW v RENAME TO w").await;
    assert!(!view_exists(&catalogs, "v").await);
    assert!(view_exists(&catalogs, "w").await);
}

#[tokio::test]
async fn two_part_alter_view_set_unset_and_rename_resolve_the_use_catalog() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v TBLPROPERTIES ('k'='v') AS SELECT * FROM src",
    )
    .await;
    run(&ctx, &catalogs, "USE ice").await;
    run(
        &ctx,
        &catalogs,
        "ALTER VIEW sales.v SET TBLPROPERTIES ('j'='u')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER VIEW sales.v UNSET TBLPROPERTIES ('k')",
    )
    .await;
    let properties = view_properties(&catalogs, "v").await;
    assert_eq!(properties.get("j"), Some(&"u".to_string()));
    assert_eq!(properties.get("k"), None);
    run(&ctx, &catalogs, "ALTER VIEW sales.v RENAME TO sales.w").await;
    assert!(!view_exists(&catalogs, "v").await);
    assert!(view_exists(&catalogs, "w").await);
}

#[tokio::test]
async fn two_part_create_and_drop_view_resolve_the_use_catalog() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(&ctx, &catalogs, "USE ice").await;
    run(&ctx, &catalogs, "CREATE VIEW sales.v AS SELECT * FROM src").await;
    assert!(view_exists(&catalogs, "v").await);
    run(&ctx, &catalogs, "DROP VIEW sales.v").await;
    assert!(!view_exists(&catalogs, "v").await);
}

#[tokio::test]
async fn show_views_resolves_one_part_after_use_and_keeps_an_explicit_catalog() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v AS SELECT * FROM src",
    )
    .await;
    let expected = vec![("sales".to_string(), "v".to_string(), false)];
    run(&ctx, &catalogs, "USE ice.sales").await;
    assert_eq!(
        shown_views(&ctx, &catalogs, "SHOW VIEWS IN sales").await,
        expected
    );
    crate::use_ddl::set_session_defaults(&ctx, &catalogs, "other", "sales");
    assert_eq!(
        shown_views(&ctx, &catalogs, "SHOW VIEWS IN ice.sales").await,
        expected
    );
}
