use datafusion::arrow::array::AsArray;

use super::super::*;
use super::common::*;

async fn setup_ntz(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    repark_functions::register_all(&ctx);
    (ctx, catalogs)
}

async fn setup_ntz_at(wh: &TempDir, zone: &str) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup_at_zone(wh, zone).await;
    repark_functions::register_all(&ctx);
    (ctx, catalogs)
}

async fn failure(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    match execute(ctx, catalogs, sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => match frame.collect().await {
            Err(error) => error.to_string(),
            Ok(done) => panic!("{sql} answered instead of refusing: {done:?}"),
        },
    }
}

async fn walls(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(i32, Option<String>)> {
    let out = execute(
        ctx,
        catalogs,
        &format!("SELECT id, CAST(c AS STRING) AS s FROM {table} ORDER BY id"),
    )
    .await
    .unwrap_or_else(|error| panic!("{table}: {error}"))
    .collect()
    .await
    .unwrap_or_else(|error| panic!("{table}: {error}"));
    let ids = out[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
    let texts = out[0].column(1).as_string::<i32>();
    (0..out[0].num_rows())
        .map(|row| {
            let wall = if texts.is_null(row) {
                None
            } else {
                Some(texts.value(row).to_string())
            };
            (ids.value(row), wall)
        })
        .collect()
}

#[test]
fn ntz_wall_udf_name_matches_the_registered_udf() {
    assert_eq!(
        repark_iceberg::write::ntz_store::NTZ_WALL_CAST_UDF_NAME,
        repark_functions::timestamp_ntz_cast::TIMESTAMP_NTZ_CAST_NAME
    );
}

#[tokio::test]
async fn ltz_values_store_their_session_zone_wall() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ntz (id INT, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ntz VALUES (1, TIMESTAMP'2024-01-01 12:00:00'), (3, NULL)",
    )
    .await;
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.ntz").await,
        vec![(1, Some("2024-01-01 12:00:00".to_string())), (3, None),]
    );
    let zone_wh = TempDir::new().unwrap();
    let (ny, ny_catalogs) = setup_ntz_at(&zone_wh, "America/New_York").await;
    run(
        &ny,
        &ny_catalogs,
        "CREATE TABLE ice.sales.ntz (id INT, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    run(
        &ny,
        &ny_catalogs,
        "INSERT INTO ice.sales.ntz VALUES (4, TIMESTAMP'2024-01-01 12:00:00'), (5, \
         TIMESTAMP'2024-01-01 12:00:00Z')",
    )
    .await;
    run(
        &ny,
        &ny_catalogs,
        "INSERT INTO ice.sales.ntz SELECT 40, TIMESTAMP'2024-01-04 12:00:00Z'",
    )
    .await;
    assert_eq!(
        walls(&ny, &ny_catalogs, "ice.sales.ntz").await,
        vec![
            (4, Some("2024-01-01 12:00:00".to_string())),
            (5, Some("2024-01-01 07:00:00".to_string())),
            (40, Some("2024-01-04 07:00:00".to_string())),
        ]
    );
}

#[tokio::test]
async fn update_and_merge_store_the_session_zone_wall() {
    for (zone, update_wall, merge_walls) in [
        (
            "UTC",
            "2024-05-05 12:00:00",
            ("2024-06-06 12:00:00", "2024-07-07 12:00:00"),
        ),
        (
            "America/New_York",
            "2024-05-05 08:00:00",
            ("2024-06-06 08:00:00", "2024-07-07 08:00:00"),
        ),
    ] {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_ntz_at(&warehouse, zone).await;
        run(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.ntz (id INT, c TIMESTAMP_NTZ) USING iceberg",
        )
        .await;
        run(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.ntz VALUES (0, TIMESTAMP_NTZ'2024-01-01 00:00:00'), (1, \
             TIMESTAMP_NTZ'2024-01-01 00:00:00')",
        )
        .await;
        run(
            &ctx,
            &catalogs,
            "UPDATE ice.sales.ntz SET c = TIMESTAMP'2024-05-05 12:00:00Z' WHERE id = 0",
        )
        .await;
        assert_eq!(
            walls(&ctx, &catalogs, "ice.sales.ntz").await,
            vec![
                (0, Some(update_wall.to_string())),
                (1, Some("2024-01-01 00:00:00".to_string())),
            ],
            "{zone} update"
        );
        run(
            &ctx,
            &catalogs,
            "MERGE INTO ice.sales.ntz t USING (SELECT 1 AS id, TIMESTAMP'2024-06-06 12:00:00Z' \
             AS c UNION ALL SELECT 2, TIMESTAMP'2024-07-07 12:00:00Z') s ON t.id = s.id WHEN \
             MATCHED THEN UPDATE SET c = s.c WHEN NOT MATCHED THEN INSERT *",
        )
        .await;
        assert_eq!(
            walls(&ctx, &catalogs, "ice.sales.ntz").await,
            vec![
                (0, Some(update_wall.to_string())),
                (1, Some(merge_walls.0.to_string())),
                (2, Some(merge_walls.1.to_string())),
            ],
            "{zone} merge"
        );
    }
}

#[tokio::test]
async fn date_values_store_midnight() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ntz (id INT, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ntz VALUES (3, DATE'2024-01-03')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ntz SELECT 4, DATE'2024-01-04'",
    )
    .await;
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.ntz").await,
        vec![
            (3, Some("2024-01-03 00:00:00".to_string())),
            (4, Some("2024-01-04 00:00:00".to_string())),
        ]
    );
}

#[tokio::test]
async fn ntz_values_store_into_a_timestamp_column_as_session_instants() {
    for (zone, unix_seconds) in [("UTC", 1_704_110_400), ("America/New_York", 1_704_128_400)] {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = setup_ntz_at(&warehouse, zone).await;
        run(
            &ctx,
            &catalogs,
            "CREATE TABLE ice.sales.ltz (id INT, c TIMESTAMP) USING iceberg",
        )
        .await;
        run(
            &ctx,
            &catalogs,
            "INSERT INTO ice.sales.ltz VALUES (0, TIMESTAMP_NTZ'2024-01-01 12:00:00')",
        )
        .await;
        let out = execute(
            &ctx,
            &catalogs,
            "SELECT unix_timestamp(c) AS u FROM ice.sales.ltz",
        )
        .await
        .unwrap_or_else(|error| panic!("{zone}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{zone}: {error}"));
        let ticks = out[0]
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        assert_eq!(ticks.value(0), unix_seconds, "{zone}");
    }
}

#[tokio::test]
async fn update_refusal_names_a_timestamp_literal_source_as_timestamp() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ltz (id INT, c TIMESTAMP) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ltz VALUES (1, TIMESTAMP'2024-01-01 00:00:00')",
    )
    .await;
    let text = failure(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.ltz SET id = TIMESTAMP'2024-01-01 12:00:00' WHERE id = 1",
    )
    .await;
    let expected = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible \
         data for the table ``: Cannot safely cast `id` \"TIMESTAMP\" to \"INT\". SQLSTATE: KD000";
    assert!(text.ends_with(expected), "{text}");
}

#[tokio::test]
async fn ntz_refusals_name_timestamp_ntz() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ntz (id INT, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ntz VALUES (0, TIMESTAMP_NTZ'2024-01-01 00:00:00')",
    )
    .await;
    for (sql, from) in [
        (
            "INSERT INTO ice.sales.ntz VALUES (2, '2024-03-10 02:30:00')",
            "STRING",
        ),
        ("INSERT INTO ice.sales.ntz VALUES (5, 1)", "INT"),
        ("INSERT INTO ice.sales.ntz VALUES (1, true)", "BOOLEAN"),
        ("INSERT INTO ice.sales.ntz SELECT 3, '2024-01-01'", "STRING"),
        ("INSERT INTO ice.sales.ntz SELECT 2, 1", "INT"),
    ] {
        let text = failure(&ctx, &catalogs, sql).await;
        let expected = format!(
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
             the table `ice`.`sales`.`ntz`: Cannot safely cast `c` \"{from}\" to \"TIMESTAMP_NTZ\". \
             SQLSTATE: KD000"
        );
        assert!(text.ends_with(&expected), "{sql}: {text}");
    }
    for sql in [
        "UPDATE ice.sales.ntz SET c = '2024-01-01 00:00:00' WHERE id = 0",
        "MERGE INTO ice.sales.ntz t USING (SELECT 0 AS id, '2024-01-01 00:00:00' AS c) s ON \
         t.id = s.id WHEN MATCHED THEN UPDATE SET c = s.c",
        "MERGE INTO ice.sales.ntz t USING (SELECT 0 AS id, '2024-01-01 00:00:00' AS c) s ON \
         t.id = s.id WHEN NOT MATCHED THEN INSERT *",
    ] {
        let text = failure(&ctx, &catalogs, sql).await;
        let expected = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible \
             data for the table ``: Cannot safely cast `c` \"STRING\" to \"TIMESTAMP_NTZ\". \
             SQLSTATE: KD000";
        assert!(text.ends_with(expected), "{sql}: {text}");
    }
}

#[tokio::test]
async fn stacked_sign_numeric_values_into_ntz_refuse_like_single_signed() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ntz (id INT, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ntz VALUES (0, TIMESTAMP_NTZ'2024-01-01 00:00:00')",
    )
    .await;
    for (cell, from) in [
        ("- -1", "INT"),
        ("- - -1", "INT"),
        ("- -(1)", "INT"),
        ("- -1.5", "DECIMAL(2,1)"),
        ("+-1", "INT"),
        ("-+1", "INT"),
        ("- -1BD", "DECIMAL(1,0)"),
        ("-1.5", "DECIMAL(2,1)"),
        ("-1BD", "DECIMAL(1,0)"),
        ("(- -1)", "INT"),
        ("+- -1", "INT"),
        ("+(- -1)", "INT"),
        ("-(- -1)", "INT"),
        ("- -1 + 0", "BIGINT"),
        ("abs(- -1)", "INT"),
        ("CAST(- -1 AS INT)", "INT"),
    ] {
        let sql = format!("INSERT INTO ice.sales.ntz VALUES (1, {cell})");
        let text = failure(&ctx, &catalogs, &sql).await;
        let expected = format!(
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
             the table `ice`.`sales`.`ntz`: Cannot safely cast `c` \"{from}\" to \"TIMESTAMP_NTZ\". \
             SQLSTATE: KD000"
        );
        assert!(text.ends_with(&expected), "{sql}: {text}");
    }
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.ntz").await,
        vec![(0, Some("2024-01-01 00:00:00".to_string()))]
    );
}

#[tokio::test]
async fn stacked_sign_multi_row_with_null_and_bad_row_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ntz (id INT, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.ntz VALUES (0, TIMESTAMP_NTZ'2024-01-01 00:00:00')",
    )
    .await;
    let sql = "INSERT INTO ice.sales.ntz VALUES (900, NULL), (901, +- -1)";
    let text = failure(&ctx, &catalogs, sql).await;
    let expected = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data \
         for the table `ice`.`sales`.`ntz`: Cannot safely cast `c` \"INT\" to \"TIMESTAMP_NTZ\". \
         SQLSTATE: KD000";
    assert!(text.ends_with(expected), "{sql}: {text}");
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.ntz").await,
        vec![(0, Some("2024-01-01 00:00:00".to_string()))]
    );
}

#[tokio::test]
async fn equal_cells_in_one_row_store_and_bad_rows_still_refuse() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    for sql in [
        "CREATE TABLE ice.sales.eq (id INT, a INT, b INT, c TIMESTAMP_NTZ) USING iceberg",
        "CREATE TABLE ice.sales.two (id INT, c TIMESTAMP_NTZ, d TIMESTAMP_NTZ) USING iceberg",
        "CREATE TABLE ice.sales.idn (id INT, c TIMESTAMP_NTZ, qty INT) USING iceberg",
        "INSERT INTO ice.sales.eq VALUES (1, 1, 1, TIMESTAMP_NTZ'2024-01-02 03:04:05')",
        "INSERT INTO ice.sales.eq VALUES (3, NULL, NULL, TIMESTAMP_NTZ'2024-01-02 03:04:05')",
        "INSERT INTO ice.sales.eq VALUES (6, 7, CAST(7 AS INT), TIMESTAMP_NTZ'2024-01-02 03:04:05')",
        "INSERT INTO ice.sales.eq VALUES (7, 1, 2, TIMESTAMP_NTZ'2024-01-02 03:04:05'), (8, 3, 3, \
         TIMESTAMP_NTZ'2024-01-02 03:04:05')",
        "INSERT INTO ice.sales.eq (c, a, b, id) VALUES (TIMESTAMP_NTZ'2024-01-02 03:04:05', 21, \
         21, 21)",
        "INSERT INTO ice.sales.two VALUES (1, TIMESTAMP_NTZ'2024-01-02 03:04:05', \
         TIMESTAMP_NTZ'2024-01-02 03:04:05')",
        "INSERT INTO ice.sales.two VALUES (3, NULL, NULL)",
        "INSERT INTO ice.sales.idn VALUES (1, TIMESTAMP_NTZ'2024-01-02 03:04:05', 1)",
    ] {
        run(&ctx, &catalogs, sql).await;
    }
    for (sql, table, from) in [
        (
            "INSERT INTO ice.sales.eq VALUES (10, 1, 1, '2024-01-02 03:04:05')",
            "eq",
            "STRING",
        ),
        (
            "INSERT INTO ice.sales.eq VALUES (11, 1, 1, - -1)",
            "eq",
            "INT",
        ),
        (
            "INSERT INTO ice.sales.two VALUES (5, '2024-01-02 03:04:05', '2024-01-02 03:04:05')",
            "two",
            "STRING",
        ),
    ] {
        let text = failure(&ctx, &catalogs, sql).await;
        let expected = format!(
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
             the table `ice`.`sales`.`{table}`: Cannot safely cast `c` \"{from}\" to \
             \"TIMESTAMP_NTZ\". SQLSTATE: KD000"
        );
        assert!(text.ends_with(&expected), "{sql}: {text}");
    }
    let wall = Some("2024-01-02 03:04:05".to_string());
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.eq").await,
        [1, 3, 6, 7, 8, 21].map(|id| (id, wall.clone())).to_vec()
    );
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.two").await,
        vec![(1, wall.clone()), (3, None)]
    );
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.idn").await,
        vec![(1, wall)]
    );
}

#[tokio::test]
async fn default_cells_in_ntz_tables_store_null() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    for sql in [
        "CREATE TABLE ice.sales.idn (id INT, c TIMESTAMP_NTZ, qty INT) USING iceberg",
        "CREATE TABLE ice.sales.mix (id INT, l TIMESTAMP, c TIMESTAMP_NTZ) USING iceberg",
        "INSERT INTO ice.sales.idn VALUES (6, DEFAULT, 1)",
        "INSERT INTO ice.sales.idn VALUES (7, TIMESTAMP_NTZ'2024-01-02 03:04:05', DEFAULT)",
        "INSERT INTO ice.sales.idn (id, c) VALUES (20, DEFAULT)",
        "INSERT INTO ice.sales.mix VALUES (4, DEFAULT, TIMESTAMP_NTZ'2024-01-02 03:04:05')",
        "INSERT INTO ice.sales.mix (id, l, c) VALUES (20, DEFAULT, TIMESTAMP_NTZ'2024-01-02 \
         03:04:05')",
    ] {
        run(&ctx, &catalogs, sql).await;
    }
    let wall = Some("2024-01-02 03:04:05".to_string());
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.idn").await,
        vec![(6, None), (7, wall.clone()), (20, None)]
    );
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.mix").await,
        vec![(4, wall.clone()), (20, wall)]
    );
}

#[tokio::test]
async fn values_from_utc_stores_the_session_wall() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz_at(&warehouse, "America/New_York").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.vfun (id INT, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.vfun VALUES (1, \
         from_utc_timestamp('2024-01-01 12:00:00','Asia/Tokyo'))",
    )
    .await;
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.vfun").await,
        vec![(1, Some("2024-01-01 21:00:00".to_string()))]
    );
}

#[tokio::test]
async fn values_dst_from_utc_store_the_session_walls() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz_at(&warehouse, "America/New_York").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.vdst (id INT, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.vdst VALUES \
         (1, from_utc_timestamp('2024-03-10 06:30:00','America/New_York')), \
         (2, from_utc_timestamp('2024-03-10 07:30:00','America/New_York')), \
         (3, from_utc_timestamp('2024-11-03 05:30:00','America/New_York')), \
         (4, from_utc_timestamp('2024-11-03 06:30:00','America/New_York'))",
    )
    .await;
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.vdst").await,
        vec![
            (1, Some("2024-03-10 01:30:00".to_string())),
            (2, Some("2024-03-10 03:30:00".to_string())),
            (3, Some("2024-11-03 01:30:00".to_string())),
            (4, Some("2024-11-03 01:30:00".to_string())),
        ]
    );
}

#[tokio::test]
async fn select_star_from_values_from_utc_stores_the_session_wall() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz_at(&warehouse, "America/New_York").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.vsub (id INT, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.vsub SELECT * FROM \
         (VALUES (1, from_utc_timestamp('2024-01-01 12:00:00','Asia/Tokyo'))) AS v(id, c)",
    )
    .await;
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.vsub").await,
        vec![(1, Some("2024-01-01 21:00:00".to_string()))]
    );
}

#[tokio::test]
async fn nested_values_from_utc_into_ltz_stores_the_instant() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz_at(&warehouse, "America/New_York").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.vln (id INT, c TIMESTAMP) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.vln SELECT * FROM \
         (VALUES (1, from_utc_timestamp('2024-01-01 12:00:00','Asia/Tokyo'))) AS v(id, c)",
    )
    .await;
    let out = execute(
        &ctx,
        &catalogs,
        "SELECT id, unix_timestamp(c) AS u FROM ice.sales.vln ORDER BY id",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let ticks = out[0]
        .column(1)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
    assert_eq!(ticks.value(0), 1_704_160_800);
}

#[tokio::test]
async fn select_from_utc_into_ntz_keeps_the_bare_value_wrap() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz_at(&warehouse, "America/New_York").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.vsel (id INT, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.vsel SELECT 1, \
         from_utc_timestamp('2024-01-01 12:00:00','Asia/Tokyo')",
    )
    .await;
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.vsel").await,
        vec![(1, Some("2024-01-01 21:00:00".to_string()))]
    );
}

async fn id_strings(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<(i32, String)> {
    let out = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let ids = out[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
    let texts = out[0].column(1).as_string::<i32>();
    (0..out[0].num_rows())
        .map(|row| (ids.value(row), texts.value(row).to_string()))
        .collect()
}

#[tokio::test]
async fn backslash_quote_strings_store_their_exact_values() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bs (id INT, s STRING, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    let wall = "TIMESTAMP_NTZ'2024-01-02 03:04:05'";
    for (id, cell) in [
        (1, r#"'{"msg": "it\\''s"}'"#),
        (2, r"'C:\\dir\\''s file'"),
        (3, r"'end\\'''"),
        (4, r"'no quote'"),
        (101, r"'end\\'"),
        (102, r"'a\\\\''b'"),
        (103, r"'a\nb\tc'"),
        (104, r"'\u00e9'"),
        (106, r"'-- x\\''y'"),
        (109, r"'a\nb\\''c'"),
        (1051, r"'a\\''b'"),
        (1054, r"'\\'''"),
    ] {
        let sql = format!("INSERT INTO ice.sales.bs VALUES ({id}, {cell}, {wall})");
        run(&ctx, &catalogs, &sql).await;
    }
    run(
        &ctx,
        &catalogs,
        &format!(
            "INSERT INTO ice.sales.bs (c, id, s) VALUES ({wall}, 5, {})",
            r"'x\\''y'"
        ),
    )
    .await;
    for (id, cell) in [
        (
            10,
            r"CAST(replace('2024-01-02 03:04:05\\''', '\\''', '') AS TIMESTAMP_NTZ)",
        ),
        (
            107,
            r"CAST(replace('-- x\\''y2024-01-02 03:04:05', '-- x\\''y', '') AS TIMESTAMP_NTZ)",
        ),
    ] {
        let sql = format!("INSERT INTO ice.sales.bs VALUES ({id}, 'x', {cell})");
        run(&ctx, &catalogs, &sql).await;
    }
    assert_eq!(
        id_strings(
            &ctx,
            &catalogs,
            "SELECT id, s FROM ice.sales.bs ORDER BY id"
        )
        .await,
        vec![
            (1, "{\"msg\": \"it\\'s\"}".to_string()),
            (2, "C:\\dir\\'s file".to_string()),
            (3, "end\\'".to_string()),
            (4, "no quote".to_string()),
            (5, "x\\'y".to_string()),
            (10, "x".to_string()),
            (101, "end\\".to_string()),
            (102, "a\\\\'b".to_string()),
            (103, "a\nb\tc".to_string()),
            (104, "é".to_string()),
            (106, "-- x\\'y".to_string()),
            (107, "x".to_string()),
            (109, "a\nb\\'c".to_string()),
            (1051, "a\\'b".to_string()),
            (1054, "\\'".to_string()),
        ]
    );
    assert_eq!(
        walls(&ctx, &catalogs, "ice.sales.bs").await,
        [
            1, 2, 3, 4, 5, 10, 101, 102, 103, 104, 106, 107, 109, 1051, 1054
        ]
        .map(|id| (id, Some("2024-01-02 03:04:05".to_string())))
        .to_vec()
    );
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bsm (id INT, m MAP<STRING, STRING>, c TIMESTAMP_NTZ) USING iceberg",
    )
    .await;
    for (id, cell) in [(6, r"map('k', 'v\\''w')"), (401, r"map('k', '-- x\\''y')")] {
        let sql = format!("INSERT INTO ice.sales.bsm VALUES ({id}, {cell}, {wall})");
        run(&ctx, &catalogs, &sql).await;
    }
    assert_eq!(
        id_strings(
            &ctx,
            &catalogs,
            "SELECT id, m['k'] AS v FROM ice.sales.bsm ORDER BY id"
        )
        .await,
        vec![(6, "v\\'w".to_string()), (401, "-- x\\'y".to_string()),]
    );
}
