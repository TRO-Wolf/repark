use std::fmt::Write as _;

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

#[tokio::test]
async fn snapshot_pinned_reads_present_uuid_text_and_filter_on_it() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    uuid_table(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.us (id INT, name STRING) USING iceberg",
        "us",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.us VALUES (1, 'a', '{A}'), (2, 'b', '{B}')"),
    )
    .await;
    let snapshot = load_sales_table(&catalogs, "us")
        .await
        .metadata()
        .current_snapshot_id()
        .unwrap();
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.us CREATE BRANCH b1").await;
    run(&ctx, &catalogs, "ALTER TABLE ice.sales.us CREATE TAG t1").await;
    let both = format!(
        "+----+--------------------------------------+\n\
         | id | u                                    |\n\
         +----+--------------------------------------+\n\
         | 1  | {A} |\n\
         | 2  | {B} |\n\
         +----+--------------------------------------+"
    );
    let first = "+----+\n| id |\n+----+\n| 1  |\n+----+";
    for pinned in [
        format!("ice.sales.us VERSION AS OF {snapshot}"),
        "ice.sales.us TIMESTAMP AS OF '2099-01-01 00:00:00'".to_string(),
        "ice.sales.us VERSION AS OF 't1'".to_string(),
        "ice.sales.us.branch_b1".to_string(),
    ] {
        assert_eq!(
            rendered(
                &ctx,
                &catalogs,
                &format!("SELECT id, u FROM {pinned} ORDER BY id")
            )
            .await,
            both,
            "{pinned}"
        );
        assert_eq!(
            rendered(
                &ctx,
                &catalogs,
                &format!("SELECT id FROM {pinned} WHERE u = '{A}'")
            )
            .await,
            first,
            "{pinned}"
        );
    }
}

fn id_u_rows(rows: &[(i32, &str)]) -> String {
    let rule = "+----+--------------------------------------+";
    let mut out = format!("{rule}\n| id | u                                    |\n{rule}\n");
    for (id, u) in rows {
        let _ = writeln!(out, "| {id:<2} | {u} |");
    }
    out.push_str(rule);
    out
}

#[tokio::test]
async fn a_binary_source_into_uuid_is_decoded_text_like_spark() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    uuid_table(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ub (id INT, name STRING) USING iceberg",
        "ub",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        &format!("INSERT INTO ice.sales.ub VALUES (1, 'a', '{A}')"),
    )
    .await;
    let raw = "X'123e4567e89b12d3a456426614174000'";
    for insert in ["INSERT (id, name, u) VALUES (s.id, 'bin', s.u)", "INSERT *"] {
        let text = refusal(
            &ctx,
            &catalogs,
            &format!(
                "MERGE INTO ice.sales.ub t USING (SELECT 40 AS id, 'bs' AS name, {raw} AS u) s \
                 ON t.id = s.id WHEN NOT MATCHED THEN {insert}"
            ),
        )
        .await;
        assert!(
            text.contains(
                "Invalid UUID string: \u{12}>Eg\u{fffd}\u{12}\u{4e4}VBf\u{14}\u{17}@\u{0}"
            ),
            "{insert}: sixteen raw bytes must refuse as their UTF-8 text like Spark: {text:?}"
        );
    }
    let hex = B.bytes().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    });
    let statements = [
        format!(
            "MERGE INTO ice.sales.ub t USING (SELECT 41 AS id, X'{hex}' AS u) s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name, u) VALUES (s.id, 'b8', s.u)"
        ),
        format!(
            "MERGE INTO ice.sales.ub t USING (SELECT 44 AS id, 'b9' AS name, X'{hex}' AS u) s \
             ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"
        ),
        format!(
            "MERGE INTO ice.sales.ub t USING (SELECT 1 AS id, X'{hex}' AS u) s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET u = s.u"
        ),
    ];
    for statement in &statements {
        run(&ctx, &catalogs, statement).await;
    }
    assert_eq!(
        rendered(
            &ctx,
            &catalogs,
            "SELECT id, u FROM ice.sales.ub ORDER BY id"
        )
        .await,
        id_u_rows(&[(1, B), (41, B), (44, B)])
    );
}

#[tokio::test]
async fn nested_uuid_assignments_type_as_text_like_spark() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let upper = B.to_uppercase();
    let cases = [
        (
            format!(
                "MERGE INTO {{t}} t USING (SELECT 1 AS id, '{B}' AS v) s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET t.s.u = s.v"
            ),
            [(1, B), (2, A)],
        ),
        (
            format!(
                "MERGE INTO {{t}} t USING (SELECT 1 AS id) s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET t.s.u = '{B}'"
            ),
            [(1, B), (2, A)],
        ),
        (
            format!(
                "MERGE INTO {{t}} t USING (SELECT 1 AS id, named_struct('u', '{upper}') AS ns) s \
                 ON t.id = s.id WHEN MATCHED THEN UPDATE SET s = s.ns"
            ),
            [(1, B), (2, A)],
        ),
        (
            format!("UPDATE {{t}} SET s.u = '{B}' WHERE id = 2"),
            [(1, A), (2, B)],
        ),
        (
            format!("UPDATE {{t}} SET s = named_struct('u', '{B}') WHERE id = 1"),
            [(1, B), (2, A)],
        ),
    ];
    for (index, (statement, expected)) in cases.iter().enumerate() {
        let table = format!("ice.sales.sn{index}");
        run(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE {table} (id INT) USING iceberg"),
        )
        .await;
        run(
            &ctx,
            &catalogs,
            &format!("ALTER TABLE {table} ADD COLUMN s STRUCT<u: UUID>"),
        )
        .await;
        run(
            &ctx,
            &catalogs,
            &format!(
                "INSERT INTO {table} VALUES (1, named_struct('u', '{A}')), \
                 (2, named_struct('u', '{A}'))"
            ),
        )
        .await;
        let statement = statement.replace("{t}", &table);
        run(&ctx, &catalogs, &statement).await;
        assert_eq!(
            rendered(
                &ctx,
                &catalogs,
                &format!("SELECT id, s.u AS u FROM {table} ORDER BY id")
            )
            .await,
            id_u_rows(expected),
            "{statement}"
        );
    }
}
