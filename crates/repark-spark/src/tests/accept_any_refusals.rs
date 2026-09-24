use super::super::*;
use super::common::*;

const ACCEPT_ANY: &str = "CREATE TABLE ice.sales.t (id BIGINT, data STRING, cat STRING) USING iceberg \
                          TBLPROPERTIES ('write.spark.accept-any-schema'='true')";
const PLAIN: &str = "CREATE TABLE ice.sales.t (id BIGINT, data STRING, cat STRING) USING iceberg";

async fn door(wh: &TempDir, create: &str, merge_schema: bool) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    if merge_schema {
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .extensions
            .insert(repark_functions::merge_schema::MergeSchemaConfig { enabled: true });
    }
    run(&ctx, &catalogs, create).await;
    (ctx, catalogs)
}

async fn refusal(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> repark_common::Error {
    let error = match execute(ctx, catalogs, sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .expect_err("the statement must refuse"),
        Err(error) => error,
    };
    repark_core::engine_err(error)
}

async fn assert_illegal_argument(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    expected: &str,
) {
    let mapped = refusal(ctx, catalogs, sql).await;
    assert!(
        matches!(mapped, repark_common::Error::IllegalArgument(ref message) if message.as_str() == expected),
        "{sql}: got {mapped}"
    );
}

async fn register_view(ctx: &SessionContext, name: &str, query: &str) {
    ctx.sql(&format!("CREATE VIEW {name} AS {query}"))
        .await
        .expect("view plan")
        .collect()
        .await
        .expect("view");
}

async fn column_types(catalogs: &CatalogRegistry) -> Vec<(String, String)> {
    let table = catalogs["ice"]
        .load_table(&TableIdent::from_strs(["sales", "t"]).expect("ident"))
        .await
        .expect("load");
    table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.field_type.to_string()))
        .collect()
}

async fn sorted_rows(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<String> {
    let batches = execute(ctx, catalogs, "SELECT * FROM ice.sales.t")
        .await
        .expect("select")
        .collect()
        .await
        .expect("collect");
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("format")
        .to_string();
    let mut lines: Vec<String> = text
        .lines()
        .filter(|line| line.starts_with("| ") && !line.contains(" id "))
        .map(str::to_string)
        .collect();
    lines.sort();
    lines
}

fn base_types() -> Vec<(String, String)> {
    vec![
        ("id".to_string(), "long".to_string()),
        ("data".to_string(), "string".to_string()),
        ("cat".to_string(), "string".to_string()),
    ]
}

#[tokio::test]
async fn positional_values_into_accept_any_refuses_with_the_values_column_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = door(&wh, ACCEPT_ANY, false).await;
    assert_illegal_argument(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')",
        "Field col1 not found in source schema",
    )
    .await;
    assert_illegal_argument(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (9, 'z')",
        "Field col1 not found in source schema",
    )
    .await;
    assert_illegal_argument(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t VALUES (9, 'z', 'q')",
        "Field col1 not found in source schema",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
    assert_eq!(column_types(&catalogs).await, base_types());
}

#[tokio::test]
async fn positional_select_into_accept_any_refuses_with_the_literal_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = door(&wh, ACCEPT_ANY, false).await;
    assert_illegal_argument(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 9, 'z', 'q'",
        "Field 9 not found in source schema",
    )
    .await;
    assert_illegal_argument(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 9 AS id, 'z', 'q' AS cat",
        "Field z not found in source schema",
    )
    .await;
    assert_illegal_argument(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 9 AS id, 'z' AS data, 'q' AS cat, 1 AS extra",
        "Field extra not found in source schema",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
}

#[tokio::test]
async fn a_view_with_other_names_refuses_on_its_first_unknown_column() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = door(&wh, ACCEPT_ANY, false).await;
    register_view(
        &ctx,
        "renamed",
        "SELECT CAST(7 AS BIGINT) AS a, 'g' AS b, 'x' AS c",
    )
    .await;
    assert_illegal_argument(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT * FROM renamed",
        "Field a not found in source schema",
    )
    .await;
}

#[tokio::test]
async fn matching_names_still_write_by_name_on_accept_any() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = door(&wh, ACCEPT_ANY, false).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 9 AS id, 'z' AS data, 'q' AS cat",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 8 AS ID, 'y' AS Data",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t (id, data, cat) VALUES (7, 'x', 'w')",
    )
    .await;
    assert_eq!(
        sorted_rows(&ctx, &catalogs).await,
        vec![
            "| 7  | x    | w   |".to_string(),
            "| 8  | y    |     |".to_string(),
            "| 9  | z    | q   |".to_string(),
        ]
    );
    assert_eq!(column_types(&catalogs).await, base_types());
}

#[tokio::test]
async fn positional_values_without_the_property_stay_positional() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = door(&wh, PLAIN, true).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 9, 'z', 'q'",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 4);
    assert_eq!(column_types(&catalogs).await, base_types());
}

#[tokio::test]
async fn merge_schema_conf_adds_the_values_columns_after_the_table_columns() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = door(&wh, ACCEPT_ANY, true).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (9, 'z', 'q')",
    )
    .await;
    let mut expected = base_types();
    expected.extend([
        ("col1".to_string(), "int".to_string()),
        ("col2".to_string(), "string".to_string()),
        ("col3".to_string(), "string".to_string()),
    ]);
    assert_eq!(column_types(&catalogs).await, expected);
    assert_eq!(
        sorted_rows(&ctx, &catalogs).await,
        vec![
            "|    |      |     | 1    | a    | x    |".to_string(),
            "|    |      |     | 2    | b    | y    |".to_string(),
            "|    |      |     | 3    | c    | x    |".to_string(),
            "|    |      |     | 9    | z    | q    |".to_string(),
        ]
    );
}

#[tokio::test]
async fn merge_schema_conf_widens_and_overwrites_like_spark() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = door(&wh, ACCEPT_ANY, true).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a', 'x')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT CAST(5 AS BIGINT) AS col1, 'b' AS col2, 'c' AS col3",
    )
    .await;
    assert_eq!(
        column_types(&catalogs).await[3],
        ("col1".to_string(), "long".to_string())
    );
    run(
        &ctx,
        &catalogs,
        "INSERT OVERWRITE ice.sales.t VALUES (9, 'z', 'q')",
    )
    .await;
    assert_eq!(
        sorted_rows(&ctx, &catalogs).await,
        vec!["|    |      |     | 9    | z    | q    |".to_string()]
    );
}

#[tokio::test]
async fn merge_schema_conf_refuses_an_incompatible_type_change() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = door(&wh, ACCEPT_ANY, true).await;
    assert_illegal_argument(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 'nine' AS id, 'z' AS data, 'q' AS cat",
        "Cannot change column type: id: long -> string",
    )
    .await;
    assert_eq!(column_types(&catalogs).await, base_types());
}

async fn merge_door(wh: &TempDir, source: &str) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = door(wh, PLAIN, false).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t SELECT 1 AS id, 'a' AS data, 'x' AS cat",
    )
    .await;
    register_view(&ctx, "s", source).await;
    (ctx, catalogs)
}

const INT_SOURCE: &str = "SELECT CAST(2 AS INT) AS id, 'B' AS data, 'y' AS cat, CAST(7 AS INT) AS extra \
                          UNION ALL SELECT CAST(4 AS INT), 'D', 'z', CAST(8 AS INT)";

const EVOLVING_MERGE: &str = "MERGE WITH SCHEMA EVOLUTION INTO ice.sales.t t USING s ON t.id = s.id \
                              WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *";

#[tokio::test]
async fn merge_schema_evolution_refuses_narrowing_the_target_type() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = merge_door(&wh, INT_SOURCE).await;
    assert_illegal_argument(
        &ctx,
        &catalogs,
        EVOLVING_MERGE,
        "Cannot change column type: id: long -> int",
    )
    .await;
    assert_eq!(column_types(&catalogs).await, base_types());
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 1);
}

#[tokio::test]
async fn merge_schema_evolution_refuses_a_non_promotable_type() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = merge_door(
        &wh,
        "SELECT CAST(2 AS BIGINT) AS id, CAST(5 AS INT) AS data, CAST(6 AS INT) AS cat",
    )
    .await;
    assert_illegal_argument(
        &ctx,
        &catalogs,
        EVOLVING_MERGE,
        "Cannot change column type: data: string -> int",
    )
    .await;
    assert_eq!(column_types(&catalogs).await, base_types());
}

#[tokio::test]
async fn merge_schema_evolution_with_a_matching_source_still_adds_the_column() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = merge_door(
        &wh,
        "SELECT CAST(2 AS BIGINT) AS id, 'B' AS data, 'y' AS cat, CAST(7 AS INT) AS extra \
         UNION ALL SELECT CAST(4 AS BIGINT), 'D', 'z', CAST(8 AS INT)",
    )
    .await;
    run(&ctx, &catalogs, EVOLVING_MERGE).await;
    let mut expected = base_types();
    expected.push(("extra".to_string(), "int".to_string()));
    assert_eq!(column_types(&catalogs).await, expected);
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);
}

#[tokio::test]
async fn merge_schema_evolution_with_only_a_delete_clause_does_not_evolve() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = merge_door(&wh, INT_SOURCE).await;
    run(
        &ctx,
        &catalogs,
        "MERGE WITH SCHEMA EVOLUTION INTO ice.sales.t t USING s ON t.id = s.id \
         WHEN MATCHED THEN DELETE",
    )
    .await;
    assert_eq!(column_types(&catalogs).await, base_types());
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 1);
}
