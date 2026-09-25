use super::super::*;
use super::common::*;
use datafusion::sql::sqlparser::parser::ParserError;
use tempfile::TempDir;

fn schema_of(table: &iceberg::table::Table) -> Vec<(i32, String, String, bool, Option<String>)> {
    table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            (
                field.id,
                field.name.clone(),
                field.field_type.to_string(),
                field.required,
                field.doc.clone(),
            )
        })
        .collect()
}

fn spec_of(table: &iceberg::table::Table) -> Vec<(String, String, i32)> {
    table
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| {
            (
                field.name.clone(),
                field.transform.to_string(),
                field.source_id,
            )
        })
        .collect()
}

fn column(id: i32, name: &str, kind: &str) -> (i32, String, String, bool, Option<String>) {
    (id, name.into(), kind.into(), false, None)
}

fn identity(name: &str, source_id: i32) -> (String, String, i32) {
    (name.into(), "identity".into(), source_id)
}

async fn table_exists(catalogs: &CatalogRegistry, table: &str) -> bool {
    catalog_handle(catalogs, "ice")
        .unwrap()
        .table_exists(&TableIdent::new(
            NamespaceIdent::new("sales".into()),
            table.into(),
        ))
        .await
        .unwrap()
}

#[tokio::test]
async fn typed_partition_columns_append_identity_fields_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.base (id BIGINT, data STRING) USING iceberg \
         PARTITIONED BY (cat STRING)",
    )
    .await;
    let base = load_sales_table(&catalogs, "base").await;
    assert_eq!(
        schema_of(&base),
        vec![
            column(1, "id", "long"),
            column(2, "data", "string"),
            column(3, "cat", "string"),
        ]
    );
    assert_eq!(spec_of(&base), vec![identity("cat", 3)]);
    assert!(base.metadata().current_snapshot().is_none());

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.two (id BIGINT, data STRING) USING iceberg \
         PARTITIONED BY (cat STRING, k INT, d DATE)",
    )
    .await;
    let two = load_sales_table(&catalogs, "two").await;
    assert_eq!(
        schema_of(&two),
        vec![
            column(1, "id", "long"),
            column(2, "data", "string"),
            column(3, "cat", "string"),
            column(4, "k", "int"),
            column(5, "d", "date"),
        ]
    );
    assert_eq!(
        spec_of(&two),
        vec![identity("cat", 3), identity("k", 4), identity("d", 5)]
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opts (id BIGINT) USING iceberg \
         PARTITIONED BY (p STRING NOT NULL COMMENT 'hi', `p q` DECIMAL(10,2))",
    )
    .await;
    let opts = load_sales_table(&catalogs, "opts").await;
    assert_eq!(
        schema_of(&opts),
        vec![
            column(1, "id", "long"),
            (2, "p".into(), "string".into(), true, Some("hi".into())),
            column(3, "p q", "decimal(10,2)"),
        ]
    );
    assert_eq!(spec_of(&opts), vec![identity("p", 2), identity("p q", 3)]);

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bare USING iceberg PARTITIONED BY (p STRING)",
    )
    .await;
    let no_columns = load_sales_table(&catalogs, "bare").await;
    assert_eq!(schema_of(&no_columns), vec![column(1, "p", "string")]);
    assert_eq!(spec_of(&no_columns), vec![identity("p", 1)]);

    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.base VALUES (1, 'a', 'x')",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.base").await,
        1
    );
}

#[tokio::test]
async fn typed_partition_columns_beside_expressions_refuse_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for (table, partitioning, expected) in [
        (
            "mix_transform",
            "PARTITIONED BY (cat STRING, days(ts))",
            "Operation not allowed: PARTITION BY: Cannot mix partition expressions and partition \
             columns:\nExpressions: days(ts)\nColumns: cat string.",
        ),
        (
            "mix_many",
            "PARTITIONED BY (data, cat STRING, bucket(4, id), k INT)",
            "Operation not allowed: PARTITION BY: Cannot mix partition expressions and partition \
             columns:\nExpressions: data, bucket(4, id)\nColumns: cat string, k int.",
        ),
        (
            "mix_decimal",
            "PARTITIONED BY (data, p DECIMAL(10,2) NOT NULL, q ARRAY<INT>)",
            "Operation not allowed: PARTITION BY: Cannot mix partition expressions and partition \
             columns:\nExpressions: data\nColumns: p decimal(10,2), q array<int>.",
        ),
    ] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.{table} (id BIGINT, data STRING, ts TIMESTAMP) \
                 USING iceberg {partitioning}"
            ),
        )
        .await
        .unwrap_err();
        let DataFusionError::SQL(parser_error, None) = &error else {
            panic!("{table}: expected a parse error, got {error:?}");
        };
        assert_eq!(
            **parser_error,
            ParserError::ParserError(expected.into()),
            "{table}"
        );
        assert!(!table_exists(&catalogs, table).await, "{table}");
    }
}

#[tokio::test]
async fn typed_partition_columns_repeating_a_name_refuse_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for (table, partitioning, name) in [
        ("dup_declared", "PARTITIONED BY (data STRING)", "data"),
        ("dup_case", "PARTITIONED BY (DATA STRING)", "data"),
        ("dup_typed", "PARTITIONED BY (p STRING, P INT)", "p"),
    ] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE ice.sales.{table} (id BIGINT, data STRING) USING iceberg {partitioning}"),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Error during planning: [COLUMN_ALREADY_EXISTS] The column `{name}` already \
                 exists. Choose another name or rename the existing column. SQLSTATE: 42711"
            ),
            "{table}"
        );
        assert!(!table_exists(&catalogs, table).await, "{table}");
    }
}

#[tokio::test]
async fn untyped_and_transform_partitioning_keep_their_answers() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.untyped (id BIGINT, data STRING, cat STRING) USING iceberg \
         PARTITIONED BY (cat)",
    )
    .await;
    let untyped = load_sales_table(&catalogs, "untyped").await;
    assert_eq!(schema_of(&untyped).len(), 3);
    assert_eq!(spec_of(&untyped), vec![identity("cat", 3)]);

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.days (id BIGINT, ts TIMESTAMP) USING iceberg \
         PARTITIONED BY (days(ts))",
    )
    .await;
    let days = load_sales_table(&catalogs, "days").await;
    assert_eq!(schema_of(&days).len(), 2);
    assert_eq!(spec_of(&days), vec![("ts_day".into(), "day".into(), 2)]);

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nocols USING iceberg PARTITIONED BY (p)",
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("requires a column list"),
        "{error}"
    );
}
