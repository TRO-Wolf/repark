use datafusion::arrow::util::pretty::pretty_format_batches;

use super::super::*;
use super::common::*;

const A: &str = "123e4567-e89b-12d3-a456-426614174000";
const B: &str = "223e4567-e89b-12d3-a456-426614174001";

async fn rendered(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    pretty_format_batches(&batches).unwrap().to_string()
}

async fn refusal(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    match execute(ctx, catalogs, sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => frame
            .collect()
            .await
            .map_or_else(|error| error.to_string(), |_| String::new()),
    }
}

async fn uuid_table(ctx: &SessionContext, catalogs: &CatalogRegistry, ddl: &str, table: &str) {
    run(ctx, catalogs, ddl).await;
    run(
        ctx,
        catalogs,
        &format!("ALTER TABLE ice.sales.{table} ADD COLUMN u UUID"),
    )
    .await;
}

#[tokio::test]
async fn merge_joins_inserts_and_updates_through_uuid_text() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    uuid_table(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.uw (id INT, name STRING) USING iceberg",
        "uw",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "INSERT INTO ice.sales.uw VALUES (1, 'a', '{A}'), (2, 'b', '{B}'), (3, 'c', NULL)"
        ),
    )
    .await;
    let statements = [
        format!(
            "MERGE INTO ice.sales.uw t USING (SELECT 2 AS id, '{B}' AS u) s ON t.u = s.u \
             WHEN MATCHED THEN UPDATE SET name = 'm'"
        ),
        format!(
            "MERGE INTO ice.sales.uw t USING (SELECT 7 AS id) s ON t.u = '{A}' \
             WHEN MATCHED THEN UPDATE SET name = 'q'"
        ),
        format!(
            "MERGE INTO ice.sales.uw t USING (SELECT 8 AS id, '{}' AS u) s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name, u) VALUES (s.id, 'x', s.u)",
            A.to_uppercase()
        ),
        format!(
            "MERGE INTO ice.sales.uw t USING (SELECT 3 AS id, '{}' AS u) s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET u = s.u",
            B.to_uppercase()
        ),
    ];
    for statement in &statements {
        run(&ctx, &catalogs, statement).await;
    }
    assert_eq!(
        rendered(
            &ctx,
            &catalogs,
            "SELECT id, name, u FROM ice.sales.uw ORDER BY id"
        )
        .await,
        format!(
            "+----+------+--------------------------------------+\n\
             | id | name | u                                    |\n\
             +----+------+--------------------------------------+\n\
             | 1  | q    | {A} |\n\
             | 2  | m    | {B} |\n\
             | 3  | c    | {B} |\n\
             | 8  | x    | {A} |\n\
             +----+------+--------------------------------------+"
        )
    );
    let invalid = refusal(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.uw t USING (SELECT 9 AS id, 'nope' AS u) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, name, u) VALUES (s.id, 'x', s.u)",
    )
    .await;
    assert!(
        invalid.contains("Invalid UUID string: nope"),
        "an invalid uuid text must refuse with the fork's parser text: {invalid}"
    );
}

#[tokio::test]
async fn insert_overwrite_both_forms_store_uuid_text_lower_case() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    uuid_table(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.uo (id INT, p INT) USING iceberg PARTITIONED BY (p)",
        "uo",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.uo VALUES (1, 1, '{A}'), (2, 2, '{A}')"),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "INSERT OVERWRITE ice.sales.uo SELECT 3, 1, '{}'",
            B.to_uppercase()
        ),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "INSERT OVERWRITE ice.sales.uo PARTITION (p = 2) SELECT 4, '{}'",
            A.to_uppercase()
        ),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "INSERT OVERWRITE ice.sales.uo (id, p, u) SELECT 5, 1, '{}'",
            A.to_uppercase()
        ),
    )
    .await;
    assert_eq!(
        rendered(
            &ctx,
            &catalogs,
            "SELECT id, p, u FROM ice.sales.uo ORDER BY id"
        )
        .await,
        format!(
            "+----+---+--------------------------------------+\n\
             | id | p | u                                    |\n\
             +----+---+--------------------------------------+\n\
             | 5  | 1 | {A} |\n\
             +----+---+--------------------------------------+"
        )
    );
}

#[tokio::test]
async fn metadata_column_projection_presents_uuid_text() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    uuid_table(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.um (id INT) USING iceberg",
        "um",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.um VALUES (1, '{A}')"),
    )
    .await;
    assert_eq!(
        rendered(
            &ctx,
            &catalogs,
            &format!("SELECT _file IS NOT NULL AS f, u FROM ice.sales.um WHERE u = '{A}'")
        )
        .await,
        format!(
            "+------+--------------------------------------+\n\
             | f    | u                                    |\n\
             +------+--------------------------------------+\n\
             | true | {A} |\n\
             +------+--------------------------------------+"
        )
    );
}

#[tokio::test]
async fn a_value_into_void_refuses_cannot_safely_cast_on_every_door() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.vw (id INT, c VOID) USING iceberg TBLPROPERTIES \
         ('format-version'='3')",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.vw VALUES (7, NULL)").await;
    let named = "`ice`.`sales`.`vw`";
    let cases = [
        ("INSERT INTO ice.sales.vw SELECT 7, 1", named, "INT"),
        ("INSERT INTO ice.sales.vw SELECT 8, 'x'", named, "STRING"),
        (
            "INSERT INTO ice.sales.vw (id, c) SELECT id, id FROM ice.sales.vw",
            named,
            "INT",
        ),
        (
            "INSERT INTO ice.sales.vw VALUES (3, CAST(NULL AS INT))",
            named,
            "INT",
        ),
        (
            "MERGE INTO ice.sales.vw t USING (SELECT 7 AS id) s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, c) VALUES (10, 5)",
            "``",
            "INT",
        ),
        (
            "MERGE INTO ice.sales.vw t USING (SELECT 7 AS id) s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET c = 5",
            "``",
            "INT",
        ),
        ("UPDATE ice.sales.vw SET c = 3 WHERE id = 7", "``", "INT"),
    ];
    for (sql, table, source) in cases {
        let text = refusal(&ctx, &catalogs, sql).await;
        let expected = format!(
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
             the table {table}: Cannot safely cast `c` \"{source}\" to \"VOID\". SQLSTATE: KD000"
        );
        assert!(text.ends_with(&expected), "{sql}: {text}");
    }
    run(&ctx, &catalogs, "INSERT INTO ice.sales.vw SELECT 11, NULL").await;
    assert_eq!(
        rendered(&ctx, &catalogs, "SELECT * FROM ice.sales.vw ORDER BY id").await,
        "+----+---+\n| id | c |\n+----+---+\n| 7  |   |\n| 11 |   |\n+----+---+"
    );
}

#[tokio::test]
async fn ctas_of_a_null_column_is_unknown_on_v3_and_refuses_on_v2() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.vc3 USING iceberg TBLPROPERTIES ('format-version'='3') \
         AS SELECT 1 AS id, NULL AS c",
    )
    .await;
    let fields: Vec<(String, String)> = load_sales_table(&catalogs, "vc3")
        .await
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.field_type.to_string()))
        .collect();
    assert_eq!(
        fields,
        vec![
            ("id".to_string(), "int".to_string()),
            ("c".to_string(), "unknown".to_string())
        ]
    );
    assert_eq!(
        rendered(&ctx, &catalogs, "SELECT * FROM ice.sales.vc3").await,
        "+----+---+\n| id | c |\n+----+---+\n| 1  |   |\n+----+---+"
    );
    let text = refusal(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.vc2 USING iceberg TBLPROPERTIES ('format-version'='2') \
         AS SELECT 1 AS id, NULL AS c",
    )
    .await;
    assert!(
        text.ends_with(
            "Invalid schema for v2:\n- Invalid type for c: unknown is not supported until v3"
        ),
        "v2 CTAS of a NULL column must refuse with the ADD COLUMN door's text: {text}"
    );
}
