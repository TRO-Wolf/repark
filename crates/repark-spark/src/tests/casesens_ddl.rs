use super::super::*;
use super::common::*;
use iceberg::spec::{NullOrder, SortDirection};
use std::collections::HashSet;

async fn create_order_target(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id INT NOT NULL, cat STRING, s STRUCT<a: INT>) \
             USING iceberg"
        ),
    )
    .await;
}

async fn load_target(catalogs: &CatalogRegistry, table: &str) -> iceberg::table::Table {
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    catalogs["ice"].load_table(&ident).await.unwrap()
}

async fn order_source_ids(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    clause: &str,
) -> Vec<(i32, SortDirection, NullOrder)> {
    run(
        ctx,
        catalogs,
        &format!("ALTER TABLE ice.sales.{table} WRITE {clause}"),
    )
    .await;
    load_target(catalogs, table)
        .await
        .metadata()
        .default_sort_order()
        .fields
        .iter()
        .map(|field| (field.source_id, field.direction, field.null_order))
        .collect::<Vec<_>>()
}

async fn order_refuses(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
    clause: &str,
    written: &str,
) {
    let error = execute(
        ctx,
        catalogs,
        &format!("ALTER TABLE ice.sales.{table} WRITE {clause}"),
    )
    .await
    .unwrap_err();
    let expected = format!("Cannot find field '{written}' in struct: struct<");
    assert!(error.to_string().contains(expected.as_str()), "{error}");
    assert!(
        load_target(catalogs, table)
            .await
            .metadata()
            .default_sort_order()
            .is_unsorted(),
        "the refused WRITE ORDER BY must commit nothing"
    );
}

#[tokio::test]
async fn write_order_follows_the_case_rule() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_order_target(&ctx, &catalogs, "o1").await;
    assert_eq!(
        order_source_ids(&ctx, &catalogs, "o1", "ORDERED BY CAT").await,
        vec![(2, SortDirection::Ascending, NullOrder::First)]
    );
    assert_eq!(
        order_source_ids(&ctx, &catalogs, "o1", "ORDERED BY s.A").await,
        vec![(4, SortDirection::Ascending, NullOrder::First)]
    );
    create_order_target(&ctx, &catalogs, "o1loc").await;
    assert_eq!(
        order_source_ids(&ctx, &catalogs, "o1loc", "LOCALLY ORDERED BY ID").await,
        vec![(1, SortDirection::Ascending, NullOrder::First)]
    );
    assert!(
        load_target(&catalogs, "o1loc")
            .await
            .metadata()
            .properties()
            .get("write.distribution-mode")
            .is_none()
    );
    assert_eq!(
        order_source_ids(
            &ctx,
            &catalogs,
            "o1",
            "DISTRIBUTED BY PARTITION LOCALLY ORDERED BY ID DESC"
        )
        .await,
        vec![(1, SortDirection::Descending, NullOrder::Last)]
    );
    let missing = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.o1 WRITE ORDERED BY nope",
    )
    .await
    .unwrap_err();
    assert!(
        missing
            .to_string()
            .contains("Cannot find field 'nope' in struct: struct<"),
        "{missing}"
    );

    let wh_strict = TempDir::new().unwrap();
    let (strict, strict_catalogs) = setup(&wh_strict).await;
    enable_case_sensitive(&strict);
    create_order_target(&strict, &strict_catalogs, "o2").await;
    order_refuses(&strict, &strict_catalogs, "o2", "ORDERED BY CAT", "CAT").await;
    order_refuses(
        &strict,
        &strict_catalogs,
        "o2",
        "LOCALLY ORDERED BY ID",
        "ID",
    )
    .await;
    order_refuses(&strict, &strict_catalogs, "o2", "ORDERED BY s.A", "s.A").await;
    order_refuses(
        &strict,
        &strict_catalogs,
        "o2",
        "ORDERED BY bucket(4, ID)",
        "ID",
    )
    .await;
    assert_eq!(
        order_source_ids(&strict, &strict_catalogs, "o2", "ORDERED BY cat").await,
        vec![(2, SortDirection::Ascending, NullOrder::First)]
    );
}

fn identifier_ids(table: &iceberg::table::Table) -> HashSet<i32> {
    table
        .metadata()
        .current_schema()
        .identifier_field_ids()
        .collect()
}

async fn create_identifier_target(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    run(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} (id INT NOT NULL, cat STRING) USING iceberg"),
    )
    .await;
}

#[tokio::test]
async fn identifier_fields_are_exact_under_both_settings() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_identifier_target(&ctx, &catalogs, "f1").await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.f1 SET IDENTIFIER FIELDS ID",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "External error: Cannot add field ID as an identifier field: not found in current \
         schema or added columns"
    );
    assert!(identifier_ids(&load_target(&catalogs, "f1").await).is_empty());
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.f1 SET IDENTIFIER FIELDS id",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.f1 DROP IDENTIFIER FIELDS ID",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "External error: Cannot complete drop identifier fields operation: field ID not found"
    );
    let table = load_target(&catalogs, "f1").await;
    let id = table
        .metadata()
        .current_schema()
        .field_by_name("id")
        .expect("field must exist")
        .id;
    assert_eq!(identifier_ids(&table), HashSet::from([id]));

    let wh_strict = TempDir::new().unwrap();
    let (strict, strict_catalogs) = setup(&wh_strict).await;
    enable_case_sensitive(&strict);
    create_identifier_target(&strict, &strict_catalogs, "f2").await;
    let error = execute(
        &strict,
        &strict_catalogs,
        "ALTER TABLE ice.sales.f2 SET IDENTIFIER FIELDS ID",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "External error: Cannot add field ID as an identifier field: not found in current \
         schema or added columns"
    );
    assert!(identifier_ids(&load_target(&strict_catalogs, "f2").await).is_empty());
    run(
        &strict,
        &strict_catalogs,
        "ALTER TABLE ice.sales.f2 SET IDENTIFIER FIELDS id",
    )
    .await;
    let table = load_target(&strict_catalogs, "f2").await;
    let id = table
        .metadata()
        .current_schema()
        .field_by_name("id")
        .expect("field must exist")
        .id;
    assert_eq!(identifier_ids(&table), HashSet::from([id]));
}
