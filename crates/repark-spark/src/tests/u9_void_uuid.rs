use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::parser::ParserError;

use super::super::*;
use super::common::*;

const UUID_CANON: &str = "123e4567-e89b-12d3-a456-426614174000";

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

async fn provider_field_type(ctx: &SessionContext, table: &str, column: &str) -> ArrowDataType {
    ctx.table_provider(TableReference::full("ice", "sales", table))
        .await
        .unwrap_or_else(|error| panic!("{table}: {error}"))
        .schema()
        .field_with_name(column)
        .unwrap_or_else(|error| panic!("{table}.{column}: {error}"))
        .data_type()
        .clone()
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

#[tokio::test]
async fn uuid_is_a_string_column_end_to_end() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.u (id INT) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.u ADD COLUMN u UUID").await;
    assert_eq!(
        field_types(&catalogs, "u").await,
        owned(&[("id", "int"), ("u", "uuid")])
    );
    assert_eq!(
        provider_field_type(&ctx, "u", "u").await,
        ArrowDataType::Utf8
    );
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.u VALUES (1, '{UUID_CANON}'), (2, NULL)"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.u VALUES (4, '123E4567-E89B-12D3-A456-4266141740FF')",
    )
    .await;
    assert_eq!(
        rendered(&ctx, &catalogs, "SELECT * FROM ice.sales.u ORDER BY id").await,
        "+----+--------------------------------------+\n| id | u                                    |\n+----+--------------------------------------+\n| 1  | 123e4567-e89b-12d3-a456-426614174000 |\n| 2  |                                      |\n| 4  | 123e4567-e89b-12d3-a456-4266141740ff |\n+----+--------------------------------------+"
    );
    assert_eq!(
        rendered(
            &ctx,
            &catalogs,
            &format!("SELECT id FROM ice.sales.u WHERE u = '{UUID_CANON}'")
        )
        .await,
        "+----+\n| id |\n+----+\n| 1  |\n+----+"
    );
}

#[tokio::test]
async fn cast_to_uuid_refuses_with_sparks_text() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let error = execute(
        &ctx,
        &catalogs,
        &format!("SELECT CAST('{UUID_CANON}' AS UUID)"),
    )
    .await
    .expect_err("cast to uuid must refuse");
    let DataFusionError::SQL(parser, _) = error else {
        panic!("uuid cast refusal must be a SQL error: {error}");
    };
    let ParserError::ParserError(message) = parser.as_ref() else {
        panic!("uuid cast refusal must carry the message: {parser}");
    };
    let expected = format!(
        "[UNSUPPORTED_DATATYPE] Unsupported data type \"UUID\". SQLSTATE: 0A000\n== SQL (line 1, \
         position 55) ==\n...e89b-12d3-a456-426614174000' AS UUID)\n{}^^^^",
        " ".repeat(35)
    );
    assert_eq!(message, &expected);
}

#[tokio::test]
async fn alter_uuid_to_string_commits_without_changing_the_type() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.u (id INT) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.u ADD COLUMN u UUID").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.u ALTER COLUMN u TYPE STRING",
    )
    .await;
    assert_eq!(
        field_types(&catalogs, "u").await,
        owned(&[("id", "int"), ("u", "uuid")])
    );
}

#[tokio::test]
async fn delete_and_update_through_uuid_text() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.u (id INT) USING iceberg",
    )
    .await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.u ADD COLUMN u UUID").await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.u VALUES (1, '{UUID_CANON}'), (2, NULL)"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("DELETE FROM ice.sales.u WHERE u = '{UUID_CANON}'"),
    )
    .await;
    assert_eq!(
        rendered(&ctx, &catalogs, "SELECT * FROM ice.sales.u").await,
        "+----+---+\n| id | u |\n+----+---+\n| 2  |   |\n+----+---+"
    );
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.u SET id = 20 WHERE u IS NULL",
    )
    .await;
    assert_eq!(
        rendered(&ctx, &catalogs, "SELECT * FROM ice.sales.u").await,
        "+----+---+\n| id | u |\n+----+---+\n| 20 |   |\n+----+---+"
    );
}
