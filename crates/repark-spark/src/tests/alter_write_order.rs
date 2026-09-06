use super::super::*;
use super::common::*;
use iceberg::spec::{NullOrder, SortDirection};

async fn create_write_target(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    execute(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} AS SELECT * FROM src"),
    )
    .await
    .unwrap();
}

async fn load_write_target(catalogs: &CatalogRegistry, table: &str) -> iceberg::table::Table {
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    catalogs["ice"].load_table(&ident).await.unwrap()
}

#[tokio::test]
async fn write_ordered_by_sets_order_and_range() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t WRITE ORDERED BY (id, name DESC NULLS LAST)",
    )
    .await
    .unwrap();
    let table = load_write_target(&catalogs, "t").await;
    let metadata = table.metadata();
    let dist = metadata
        .properties()
        .get("write.distribution-mode")
        .cloned();
    assert_eq!(dist.as_deref(), Some("range"));
    assert_eq!(metadata.default_sort_order_id(), 1);
    assert_eq!(metadata.sort_orders_iter().len(), 2);
    let fields = &metadata.default_sort_order().fields;
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].source_id, 1);
    assert_eq!(fields[0].direction, SortDirection::Ascending);
    assert_eq!(fields[0].null_order, NullOrder::First);
    assert_eq!(fields[1].source_id, 2);
    assert_eq!(fields[1].direction, SortDirection::Descending);
    assert_eq!(fields[1].null_order, NullOrder::Last);
}

#[tokio::test]
async fn write_locally_ordered_by_leaves_distribution_untouched() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t WRITE LOCALLY ORDERED BY (id DESC)",
    )
    .await
    .unwrap();
    let table = load_write_target(&catalogs, "t").await;
    let metadata = table.metadata();
    assert!(
        metadata
            .properties()
            .get("write.distribution-mode")
            .is_none()
    );
    assert_eq!(metadata.default_sort_order_id(), 1);
    let fields = &metadata.default_sort_order().fields;
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].source_id, 1);
    assert_eq!(fields[0].direction, SortDirection::Descending);
    assert_eq!(fields[0].null_order, NullOrder::Last);
}

#[tokio::test]
async fn write_distributed_by_partition_sets_hash_and_resets_order() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t WRITE ORDERED BY (id)",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t WRITE DISTRIBUTED BY PARTITION",
    )
    .await
    .unwrap();
    let table = load_write_target(&catalogs, "t").await;
    let metadata = table.metadata();
    let dist = metadata
        .properties()
        .get("write.distribution-mode")
        .cloned();
    assert_eq!(dist.as_deref(), Some("hash"));
    assert_eq!(metadata.default_sort_order_id(), 0);
    assert!(metadata.default_sort_order().is_unsorted());
    assert_eq!(metadata.sort_orders_iter().len(), 2);
}

#[tokio::test]
async fn write_distributed_by_partition_locally_ordered_sets_both() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    create_write_target(&ctx, &catalogs, "u").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY (id)",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.u WRITE DISTRIBUTED BY PARTITION ORDERED BY (id)",
    )
    .await
    .unwrap();
    for table_name in ["t", "u"] {
        let table = load_write_target(&catalogs, table_name).await;
        let metadata = table.metadata();
        let dist = metadata
            .properties()
            .get("write.distribution-mode")
            .cloned();
        assert_eq!(dist.as_deref(), Some("hash"), "{table_name}");
        assert_eq!(metadata.default_sort_order_id(), 1, "{table_name}");
        assert_eq!(
            metadata.default_sort_order().fields.len(),
            1,
            "{table_name}"
        );
    }
}

#[tokio::test]
async fn write_unordered_resets_order_and_sets_none() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t WRITE ORDERED BY (id)",
    )
    .await
    .unwrap();
    execute(&ctx, &catalogs, "ALTER TABLE ice.sales.t WRITE UNORDERED")
        .await
        .unwrap();
    let table = load_write_target(&catalogs, "t").await;
    let metadata = table.metadata();
    let dist = metadata
        .properties()
        .get("write.distribution-mode")
        .cloned();
    assert_eq!(dist.as_deref(), Some("none"));
    assert_eq!(metadata.default_sort_order_id(), 0);
    assert!(metadata.default_sort_order().is_unsorted());
    assert_eq!(metadata.sort_orders_iter().len(), 2);
}

#[tokio::test]
async fn write_order_bad_column_refuses_and_commits_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t WRITE ORDERED BY (nope)",
    )
    .await
    .expect_err("unknown sort column refuses");
    assert!(error.to_string().contains("nope"), "{error}");
    let table = load_write_target(&catalogs, "t").await;
    assert_eq!(table.metadata().default_sort_order_id(), 0);
    assert_eq!(table.metadata().sort_orders_iter().len(), 1);
}

#[tokio::test]
async fn write_order_malformed_shapes_refuse() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    for (sql, needle) in [
        (
            "ALTER TABLE ice.sales.t WRITE ORDERED BY ()",
            "at least one column",
        ),
        (
            "ALTER TABLE ice.sales.t WRITE DISTRIBUTED BY (id)",
            "expecting 'PARTITION'",
        ),
        (
            "ALTER TABLE ice.sales.t WRITE UNORDERED BY (id)",
            "trailing tokens",
        ),
        (
            "ALTER TABLE ice.sales.t WRITE ORDERED BY (id NULLS MIDDLE)",
            "NULLS FIRST or NULLS LAST",
        ),
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("malformed WRITE shape refuses");
        assert!(error.to_string().contains(needle), "{sql}: {error}");
    }
    let table = load_write_target(&catalogs, "t").await;
    assert_eq!(table.metadata().sort_orders_iter().len(), 1);
}

#[tokio::test]
async fn write_order_column_match_is_case_insensitive() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t write ordered by (ID)",
    )
    .await
    .unwrap();
    let table = load_write_target(&catalogs, "t").await;
    let fields = &table.metadata().default_sort_order().fields;
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].source_id, 1);
}

#[tokio::test]
async fn write_order_identical_order_reuses_its_id() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    for _ in 0..2 {
        execute(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.t WRITE ORDERED BY (id)",
        )
        .await
        .unwrap();
    }
    let table = load_write_target(&catalogs, "t").await;
    assert_eq!(table.metadata().default_sort_order_id(), 1);
    assert_eq!(table.metadata().sort_orders_iter().len(), 2);
}

#[tokio::test]
async fn write_order_bare_column_list_matches_paren_form() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t WRITE ORDERED BY id, name DESC",
    )
    .await
    .unwrap();
    let table = load_write_target(&catalogs, "t").await;
    let metadata = table.metadata();
    assert_eq!(metadata.default_sort_order_id(), 1);
    let fields = &metadata.default_sort_order().fields;
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].source_id, 1);
    assert_eq!(fields[1].source_id, 2);
    assert_eq!(fields[1].direction, SortDirection::Descending);
}

#[tokio::test]
async fn write_order_backtick_column_resolves() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t WRITE ORDERED BY (`id`)",
    )
    .await
    .unwrap();
    let table = load_write_target(&catalogs, "t").await;
    assert_eq!(table.metadata().default_sort_order().fields.len(), 1);
}

async fn create_struct_target(catalogs: &CatalogRegistry, table: &str) {
    use iceberg::TableCreation;
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, StructType, Type};
    let schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(
                2,
                "st",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(3, "a", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(4, "b", Type::Primitive(PrimitiveType::String)).into(),
                ])),
            )
            .into(),
        ])
        .build()
        .unwrap();
    catalogs["ice"]
        .create_table(
            &NamespaceIdent::new("sales".to_string()),
            TableCreation::builder()
                .name(table.to_string())
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn write_ordered_by_dotted_name_resolves_the_nested_field() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_struct_target(&catalogs, "t").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t WRITE ORDERED BY (st.a DESC)",
    )
    .await
    .unwrap();
    let table = load_write_target(&catalogs, "t").await;
    let metadata = table.metadata();
    assert_eq!(metadata.default_sort_order_id(), 1);
    let fields = &metadata.default_sort_order().fields;
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].source_id, 3);
    assert_eq!(fields[0].direction, SortDirection::Descending);
    assert_eq!(fields[0].null_order, NullOrder::Last);
    assert_eq!(
        metadata
            .properties()
            .get("write.distribution-mode")
            .map(String::as_str),
        Some("range")
    );
    create_struct_target(&catalogs, "u").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.u WRITE ORDERED BY (ST.A)",
    )
    .await
    .unwrap();
    let table = load_write_target(&catalogs, "u").await;
    assert_eq!(table.metadata().default_sort_order().fields[0].source_id, 3);
}

#[tokio::test]
async fn write_ordered_by_bad_dotted_name_refuses_and_commits_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_struct_target(&catalogs, "t").await;
    for form in [
        "ALTER TABLE ice.sales.t WRITE ORDERED BY (st.nope)",
        "ALTER TABLE ice.sales.t WRITE ORDERED BY (id.a)",
        "ALTER TABLE ice.sales.t WRITE ORDERED BY (st.a.b)",
    ] {
        let error = execute(&ctx, &catalogs, form).await.expect_err(form);
        assert!(error.to_string().contains("Cannot find field"), "{error}");
    }
    let table = load_write_target(&catalogs, "t").await;
    assert_eq!(table.metadata().sort_orders_iter().len(), 1);
}

#[tokio::test]
async fn write_order_transform_sort_refuses_as_fork_ceiling() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_write_target(&ctx, &catalogs, "t").await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t WRITE ORDERED BY (bucket(4, id))",
    )
    .await
    .expect_err("transform sort fields refuse");
    assert!(error.to_string().contains("bucket"), "{error}");
    let table = load_write_target(&catalogs, "t").await;
    assert_eq!(table.metadata().sort_orders_iter().len(), 1);
}

#[tokio::test]
async fn write_order_parser_ignores_other_statements() {
    assert!(
        crate::alter_write_order::try_parse_write_order_ddl(
            "ALTER TABLE ice.sales.t ADD COLUMN age INT"
        )
        .is_none()
    );
    assert!(crate::alter_write_order::try_parse_write_order_ddl("SELECT 1").is_none());
}
