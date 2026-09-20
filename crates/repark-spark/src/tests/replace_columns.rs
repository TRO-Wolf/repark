use super::super::*;
use super::common::*;

async fn seed_three_columns(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    execute(
        ctx,
        catalogs,
        &format!(
            "CREATE TABLE ice.sales.{table} (id BIGINT, data STRING, cat STRING) USING iceberg"
        ),
    )
    .await
    .unwrap();
    execute(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{table} VALUES (1, 'a', 'x'), (2, 'b', 'y')"),
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
}

async fn field_ids(catalogs: &CatalogRegistry, table: &str) -> Vec<(String, i32, bool)> {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.id, field.required))
        .collect()
}

async fn null_rows(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<Vec<bool>> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            out.push(
                (0..batch.num_columns())
                    .map(|column| batch.column(column).is_null(row))
                    .collect(),
            );
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn replace_columns_assigns_fresh_ids_and_nulls_existing_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three_columns(&ctx, &catalogs, "rc_basic").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rc_basic REPLACE COLUMNS (id BIGINT, data STRING)",
    )
    .await
    .unwrap();

    assert_eq!(
        field_ids(&catalogs, "rc_basic").await,
        vec![("id".to_string(), 4, false), ("data".to_string(), 5, false)]
    );
    let table = load_sales_table(&catalogs, "rc_basic").await;
    assert_eq!(table.metadata().last_column_id(), 5);
    assert_eq!(table.metadata().schemas_iter().count(), 2);
    assert_eq!(
        null_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rc_basic").await,
        vec![vec![true, true], vec![true, true]]
    );
}

#[tokio::test]
async fn replace_columns_with_the_same_list_still_rewrites_every_id() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three_columns(&ctx, &catalogs, "rc_same").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rc_same REPLACE COLUMNS (id BIGINT, data STRING, cat STRING)",
    )
    .await
    .unwrap();

    assert_eq!(
        field_ids(&catalogs, "rc_same").await,
        vec![
            ("id".to_string(), 4, false),
            ("data".to_string(), 5, false),
            ("cat".to_string(), 6, false)
        ]
    );
    assert_eq!(
        null_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rc_same").await,
        vec![vec![true, true, true], vec![true, true, true]]
    );
}

#[tokio::test]
async fn replace_columns_accepts_an_incompatible_type_on_a_kept_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three_columns(&ctx, &catalogs, "rc_type").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rc_type REPLACE COLUMNS (id INT, data BINARY)",
    )
    .await
    .unwrap();

    let table = load_sales_table(&catalogs, "rc_type").await;
    let fields = table.metadata().current_schema().as_struct().fields();
    assert_eq!(
        fields
            .iter()
            .map(|field| (field.name.clone(), field.id, field.field_type.to_string()))
            .collect::<Vec<_>>(),
        vec![
            ("id".to_string(), 4, "int".to_string()),
            ("data".to_string(), 5, "binary".to_string())
        ]
    );
}

#[tokio::test]
async fn replace_columns_adds_a_struct_with_level_order_fresh_ids() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three_columns(&ctx, &catalogs, "rc_struct").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rc_struct REPLACE COLUMNS (id BIGINT, s STRUCT<a: INT, b: STRING>)",
    )
    .await
    .unwrap();

    let table = load_sales_table(&catalogs, "rc_struct").await;
    assert_eq!(table.metadata().last_column_id(), 7);
    let schema = table.metadata().current_schema();
    assert_eq!(
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.name.clone(), field.id))
            .collect::<Vec<_>>(),
        vec![("id".to_string(), 4), ("s".to_string(), 5)]
    );
    assert_eq!(schema.field_by_name("s.a").map(|field| field.id), Some(6));
    assert_eq!(schema.field_by_name("s.b").map(|field| field.id), Some(7));
}

#[tokio::test]
async fn replace_columns_keeps_the_column_comment() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three_columns(&ctx, &catalogs, "rc_comment").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rc_comment REPLACE COLUMNS (id BIGINT COMMENT 'c1', data STRING)",
    )
    .await
    .unwrap();

    let table = load_sales_table(&catalogs, "rc_comment").await;
    let fields = table.metadata().current_schema().as_struct().fields();
    assert_eq!(fields[0].doc.as_deref(), Some("c1"));
    assert_eq!(fields[1].doc, None);
}

#[tokio::test]
async fn replace_columns_twice_nulls_the_row_written_in_between() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three_columns(&ctx, &catalogs, "rc_twice").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rc_twice REPLACE COLUMNS (id BIGINT, data STRING)",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.rc_twice VALUES (3, 'c')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(
        null_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rc_twice").await,
        vec![vec![false, false], vec![true, true], vec![true, true]]
    );

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rc_twice REPLACE COLUMNS (id BIGINT, data STRING)",
    )
    .await
    .unwrap();
    assert_eq!(
        field_ids(&catalogs, "rc_twice").await,
        vec![("id".to_string(), 6, false), ("data".to_string(), 7, false)]
    );
    assert_eq!(
        null_rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rc_twice").await,
        vec![vec![true, true], vec![true, true], vec![true, true]]
    );
}

#[tokio::test]
async fn replace_columns_refuses_not_null_as_a_parse_error() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three_columns(&ctx, &catalogs, "rc_not_null").await;

    let refusal = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rc_not_null REPLACE COLUMNS (id BIGINT NOT NULL, data STRING)",
    )
    .await
    .expect_err("NOT NULL must refuse");
    assert!(
        matches!(refusal, DataFusionError::SQL(_, _)),
        "got: {refusal:?}"
    );
    assert!(
        refusal
            .to_string()
            .contains("NOT NULL is not supported in Hive-style REPLACE COLUMNS."),
        "got: {refusal}"
    );
    assert_eq!(
        field_ids(&catalogs, "rc_not_null").await,
        vec![
            ("id".to_string(), 1, false),
            ("data".to_string(), 2, false),
            ("cat".to_string(), 3, false)
        ]
    );
}

#[tokio::test]
async fn replace_columns_refuses_a_duplicate_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three_columns(&ctx, &catalogs, "rc_dup").await;

    let refusal = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rc_dup REPLACE COLUMNS (id BIGINT, id STRING)",
    )
    .await
    .expect_err("duplicate column must refuse");
    assert!(
        refusal
            .to_string()
            .contains("[COLUMN_ALREADY_EXISTS] The column `id` already exists."),
        "got: {refusal}"
    );
    assert!(
        refusal.to_string().contains("SQLSTATE: 42711"),
        "got: {refusal}"
    );
}

#[tokio::test]
async fn replace_columns_refuses_when_a_partition_field_loses_its_source() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rc_part (id BIGINT, data STRING, cat STRING) USING iceberg \
         PARTITIONED BY (cat)",
    )
    .await
    .unwrap();

    for columns in [
        "(id BIGINT, data STRING)",
        "(id BIGINT, data STRING, cat STRING)",
    ] {
        let refusal = execute(
            &ctx,
            &catalogs,
            &format!("ALTER TABLE ice.sales.rc_part REPLACE COLUMNS {columns}"),
        )
        .await
        .expect_err("a live partition field must block the replace");
        assert!(
            refusal
                .to_string()
                .contains("Cannot find source column for partition field: 1000: cat: identity(3)"),
            "got: {refusal}"
        );
    }
    assert_eq!(
        field_ids(&catalogs, "rc_part").await,
        vec![
            ("id".to_string(), 1, false),
            ("data".to_string(), 2, false),
            ("cat".to_string(), 3, false)
        ]
    );
}

#[tokio::test]
async fn replace_columns_refuses_when_a_sort_field_loses_its_source() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three_columns(&ctx, &catalogs, "rc_sorted").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rc_sorted WRITE ORDERED BY id",
    )
    .await
    .unwrap();

    let refusal = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rc_sorted REPLACE COLUMNS (id BIGINT, data STRING)",
    )
    .await
    .expect_err("a live sort field must block the replace");
    assert!(
        refusal
            .to_string()
            .contains("Cannot find source column for sort field: identity(1) ASC NULLS FIRST"),
        "got: {refusal}"
    );
    assert_eq!(
        field_ids(&catalogs, "rc_sorted").await,
        vec![
            ("id".to_string(), 1, false),
            ("data".to_string(), 2, false),
            ("cat".to_string(), 3, false)
        ]
    );
}

#[tokio::test]
async fn replace_columns_refuses_a_position_and_a_nested_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    seed_three_columns(&ctx, &catalogs, "rc_shape").await;

    for (sql, needle) in [
        (
            "ALTER TABLE ice.sales.rc_shape REPLACE COLUMNS (id BIGINT, data STRING FIRST)",
            "Column position is not supported in Hive-style REPLACE COLUMNS.",
        ),
        (
            "ALTER TABLE ice.sales.rc_shape REPLACE COLUMNS (s.a BIGINT)",
            "Replacing with a nested column is not supported in Hive-style REPLACE COLUMNS.",
        ),
    ] {
        let refusal = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("the Hive-style form must refuse");
        assert!(refusal.to_string().contains(needle), "got: {refusal}");
    }
}
