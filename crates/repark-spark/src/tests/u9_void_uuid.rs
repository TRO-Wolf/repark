use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::arrow::util::pretty::pretty_format_batches;

use super::super::*;
use super::common::*;

async fn batches(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<RecordBatch> {
    execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

async fn rendered(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    pretty_format_batches(&batches(ctx, catalogs, sql).await)
        .unwrap()
        .to_string()
}

async fn field_types(catalogs: &CatalogRegistry, table: &str) -> Vec<(String, String)> {
    load_sales_table(catalogs, table)
        .await
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.field_type.to_string()))
        .collect()
}

fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(name, kind)| ((*name).to_string(), (*kind).to_string()))
        .collect()
}

#[tokio::test]
async fn void_is_unknown_on_v3_and_refuses_below_v3() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v3 (id INT, c VOID) USING iceberg TBLPROPERTIES \
         ('format-version'='3')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.v3 ADD COLUMN d VOID",
    )
    .await;
    assert_eq!(
        field_types(&catalogs, "v3").await,
        owned(&[("id", "int"), ("c", "unknown"), ("d", "unknown")])
    );
    for version in [1, 2] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.v{version} (id INT, c VOID) USING iceberg TBLPROPERTIES \
                 ('format-version'='{version}')"
            ),
        )
        .await
        .expect_err("void below v3 must refuse");
        assert!(
            error
                .to_string()
                .contains("unknown is not supported until v3"),
            "v{version} refusal must carry the fork Java text: {error}"
        );
    }
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v2b (id INT) USING iceberg TBLPROPERTIES \
         ('format-version'='2')",
    )
    .await;
    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.v2b ADD COLUMN c VOID",
    )
    .await
    .expect_err("void add below v3 must refuse");
    assert!(
        error
            .to_string()
            .contains("unknown is not supported until v3"),
        "v2 add refusal must carry the fork Java text: {error}"
    );
}

#[tokio::test]
async fn void_takes_null_writes_and_refuses_a_value() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v (id INT, c VOID) USING iceberg TBLPROPERTIES \
         ('format-version'='3')",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.v VALUES (0, NULL)").await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.v (id) VALUES (6)").await;
    assert_eq!(
        rendered(&ctx, &catalogs, "SELECT * FROM ice.sales.v ORDER BY id").await,
        "+----+---+\n| id | c |\n+----+---+\n| 0  |   |\n| 6  |   |\n+----+---+"
    );
    let error = execute(&ctx, &catalogs, "INSERT INTO ice.sales.v VALUES (2, 1)")
        .await
        .expect_err("a void value must refuse");
    let text = error.to_string();
    assert!(
        text.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]")
            && text.contains("Cannot safely cast `c` \"INT\" to \"VOID\"")
            && text.contains("SQLSTATE: KD000"),
        "void value refusal must carry Spark text: {text}"
    );
}

#[tokio::test]
async fn cast_null_to_void_is_a_typed_null() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let frame = execute(&ctx, &catalogs, "SELECT CAST(NULL AS VOID) AS x").await;
    let frame = frame.unwrap_or_else(|error| panic!("cast must plan: {error}"));
    assert_eq!(frame.schema().field(0).data_type(), &ArrowDataType::Null);
    let batches = frame.collect().await.expect("cast must collect");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}
