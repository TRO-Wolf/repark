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

const LTZ_SOURCES: &str = "SELECT 1 AS id, TIMESTAMP'2024-07-01 12:00:00' AS c UNION ALL SELECT 2, \
                           TIMESTAMP'2024-01-01 12:00:00Z' UNION ALL SELECT 3, \
                           TIMESTAMP'2024-03-10 02:30:00' UNION ALL SELECT 4, \
                           TIMESTAMP'2024-11-03 01:30:00'";

const NTZ_SOURCES: &str = "SELECT 1 AS id, TIMESTAMP_NTZ'2024-07-01 12:00:00' AS c UNION ALL SELECT \
                           2, TIMESTAMP_NTZ'2024-03-10 02:30:00' UNION ALL SELECT 3, \
                           TIMESTAMP_NTZ'2024-11-03 01:30:00'";

const LTZ_INTO_NTZ_WALLS: [(&str, [&str; 4]); 3] = [
    (
        "UTC",
        [
            "2024-07-01 12:00:00",
            "2024-01-01 12:00:00",
            "2024-03-10 02:30:00",
            "2024-11-03 01:30:00",
        ],
    ),
    (
        "America/New_York",
        [
            "2024-07-01 12:00:00",
            "2024-01-01 07:00:00",
            "2024-03-10 03:30:00",
            "2024-11-03 01:30:00",
        ],
    ),
    (
        "Asia/Kolkata",
        [
            "2024-07-01 12:00:00",
            "2024-01-01 17:30:00",
            "2024-03-10 02:30:00",
            "2024-11-03 01:30:00",
        ],
    ),
];

const NTZ_INTO_LTZ_MICROS: [(&str, [i64; 3]); 3] = [
    (
        "UTC",
        [
            1_719_835_200_000_000,
            1_710_037_800_000_000,
            1_730_597_400_000_000,
        ],
    ),
    (
        "America/New_York",
        [
            1_719_849_600_000_000,
            1_710_055_800_000_000,
            1_730_611_800_000_000,
        ],
    ),
    (
        "Asia/Kolkata",
        [
            1_719_815_400_000_000,
            1_710_018_000_000_000,
            1_730_577_600_000_000,
        ],
    ),
];

const DATE_INTO_LTZ_MICROS: [(&str, i64); 3] = [
    ("UTC", 1_710_028_800_000_000),
    ("America/New_York", 1_710_046_800_000_000),
    ("Asia/Kolkata", 1_710_009_000_000_000),
];

fn door_sql(door: &str, source: &str) -> (bool, bool, String) {
    let (seeded, dynamic, template) = match door {
        "by_name" => (
            false,
            false,
            "INSERT INTO {t} BY NAME SELECT c, id, 1 AS p FROM ({s}) s",
        ),
        "overwrite" => (
            false,
            false,
            "INSERT OVERWRITE {t} SELECT id, c, 1 FROM ({s}) s",
        ),
        "overwrite_by_name" => (
            false,
            false,
            "INSERT OVERWRITE {t} BY NAME SELECT c, id, 1 AS p FROM ({s}) s",
        ),
        "overwrite_partition" => (
            false,
            false,
            "INSERT OVERWRITE {t} PARTITION (p = 1) SELECT id, c FROM ({s}) s",
        ),
        "overwrite_dynamic" => (
            false,
            true,
            "INSERT OVERWRITE {t} SELECT id, c, id FROM ({s}) s",
        ),
        "overwrite_columns" => (
            false,
            true,
            "INSERT OVERWRITE {t} (id, c, p) SELECT id, c, id FROM ({s}) s",
        ),
        "merge_update" => (
            true,
            false,
            "MERGE INTO {t} t USING ({s}) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET c = s.c",
        ),
        "merge_update_star" => (
            true,
            false,
            "MERGE INTO {t} t USING (SELECT id, c, 1 AS p FROM ({s}) u) s ON t.id = s.id WHEN \
             MATCHED THEN UPDATE SET *",
        ),
        "merge_insert" => (
            false,
            false,
            "MERGE INTO {t} t USING ({s}) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, c, \
             p) VALUES (s.id, s.c, 1)",
        ),
        "merge_insert_star" => (
            false,
            false,
            "MERGE INTO {t} t USING (SELECT id, c, 1 AS p FROM ({s}) u) s ON t.id = s.id WHEN \
             NOT MATCHED THEN INSERT *",
        ),
        other => panic!("unknown door {other}"),
    };
    let sql = template
        .replace("{t}", "ice.sales.door")
        .replace("{s}", source);
    (seeded, dynamic, sql)
}

fn set_dynamic_overwrite(ctx: &SessionContext) {
    let state = ctx.state_ref();
    let mut state = state.write();
    state
        .config_mut()
        .options_mut()
        .extensions
        .insert(repark_core::PartitionOverwriteModeConfig {
            mode: repark_core::PartitionOverwriteMode::Dynamic,
        });
}

async fn store_through_door(
    zone: &str,
    door: &str,
    column_type: &str,
    source: &str,
    rows: usize,
) -> (SessionContext, CatalogRegistry, TempDir) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_ntz_at(&warehouse, zone).await;
    let (seeded, dynamic, sql) = door_sql(door, source);
    if dynamic {
        set_dynamic_overwrite(&ctx);
    }
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.door (id INT, c {column_type}, p INT) USING iceberg \
             PARTITIONED BY (p)"
        ),
    )
    .await;
    if seeded {
        let seed = (1..=rows)
            .map(|id| format!("({id}, NULL, 1)"))
            .collect::<Vec<_>>()
            .join(", ");
        run(
            &ctx,
            &catalogs,
            &format!("INSERT INTO ice.sales.door VALUES {seed}"),
        )
        .await;
    }
    run(&ctx, &catalogs, &sql).await;
    (ctx, catalogs, warehouse)
}

async fn door_instants(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<(i32, i64)> {
    let out = execute(
        ctx,
        catalogs,
        "SELECT id, c FROM ice.sales.door ORDER BY id",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let mut rows = Vec::new();
    for batch in &out {
        let ids = batch
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
        let ticks = batch
            .column(1)
            .as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), ticks.value(row)));
        }
    }
    rows
}

async fn assert_ltz_into_ntz(door: &str) {
    for (zone, expected) in LTZ_INTO_NTZ_WALLS {
        let (ctx, catalogs, _warehouse) =
            store_through_door(zone, door, "TIMESTAMP_NTZ", LTZ_SOURCES, 4).await;
        let want: Vec<(i32, Option<String>)> = (1..=4)
            .zip(expected)
            .map(|(id, wall)| (id, Some(wall.to_string())))
            .collect();
        assert_eq!(
            walls(&ctx, &catalogs, "ice.sales.door").await,
            want,
            "{door} {zone}"
        );
    }
}

async fn assert_ntz_into_ltz(door: &str) {
    for (zone, expected) in NTZ_INTO_LTZ_MICROS {
        let (ctx, catalogs, _warehouse) =
            store_through_door(zone, door, "TIMESTAMP", NTZ_SOURCES, 3).await;
        let want: Vec<(i32, i64)> = (1..=3).zip(expected).collect();
        assert_eq!(door_instants(&ctx, &catalogs).await, want, "{door} {zone}");
    }
}

#[tokio::test]
async fn by_name_stores_the_ltz_session_wall() {
    assert_ltz_into_ntz("by_name").await;
}

#[tokio::test]
async fn by_name_stores_the_ntz_session_instant() {
    assert_ntz_into_ltz("by_name").await;
}

#[tokio::test]
async fn overwrite_stores_the_ltz_session_wall() {
    assert_ltz_into_ntz("overwrite").await;
}

#[tokio::test]
async fn overwrite_stores_the_ntz_session_instant() {
    assert_ntz_into_ltz("overwrite").await;
}

#[tokio::test]
async fn overwrite_by_name_stores_the_ltz_session_wall() {
    assert_ltz_into_ntz("overwrite_by_name").await;
}

#[tokio::test]
async fn overwrite_by_name_stores_the_ntz_session_instant() {
    assert_ntz_into_ltz("overwrite_by_name").await;
}

#[tokio::test]
async fn overwrite_partition_stores_the_ltz_session_wall() {
    assert_ltz_into_ntz("overwrite_partition").await;
}

#[tokio::test]
async fn overwrite_partition_stores_the_ntz_session_instant() {
    assert_ntz_into_ltz("overwrite_partition").await;
}

#[tokio::test]
async fn dynamic_overwrite_stores_the_ltz_session_wall() {
    assert_ltz_into_ntz("overwrite_dynamic").await;
}

#[tokio::test]
async fn dynamic_overwrite_stores_the_ntz_session_instant() {
    assert_ntz_into_ltz("overwrite_dynamic").await;
}

#[tokio::test]
async fn column_list_overwrite_stores_the_ltz_session_wall() {
    assert_ltz_into_ntz("overwrite_columns").await;
}

#[tokio::test]
async fn column_list_overwrite_stores_the_ntz_session_instant() {
    assert_ntz_into_ltz("overwrite_columns").await;
}

#[tokio::test]
async fn merge_update_stores_the_ltz_session_wall() {
    assert_ltz_into_ntz("merge_update").await;
}

#[tokio::test]
async fn merge_update_stores_the_ntz_session_instant() {
    assert_ntz_into_ltz("merge_update").await;
}

#[tokio::test]
async fn merge_update_star_stores_the_ltz_session_wall() {
    assert_ltz_into_ntz("merge_update_star").await;
}

#[tokio::test]
async fn merge_update_star_stores_the_ntz_session_instant() {
    assert_ntz_into_ltz("merge_update_star").await;
}

#[tokio::test]
async fn merge_insert_stores_the_ltz_session_wall() {
    assert_ltz_into_ntz("merge_insert").await;
}

#[tokio::test]
async fn merge_insert_stores_the_ntz_session_instant() {
    assert_ntz_into_ltz("merge_insert").await;
}

#[tokio::test]
async fn merge_insert_star_stores_the_ltz_session_wall() {
    assert_ltz_into_ntz("merge_insert_star").await;
}

#[tokio::test]
async fn merge_insert_star_stores_the_ntz_session_instant() {
    assert_ntz_into_ltz("merge_insert_star").await;
}

#[tokio::test]
async fn date_stores_the_session_midnight_through_every_door() {
    for door in [
        "by_name",
        "overwrite",
        "overwrite_partition",
        "overwrite_dynamic",
        "merge_update",
        "merge_insert_star",
    ] {
        for (zone, micros) in DATE_INTO_LTZ_MICROS {
            let (ctx, catalogs, _warehouse) = store_through_door(
                zone,
                door,
                "TIMESTAMP",
                "SELECT 1 AS id, DATE'2024-03-10' AS c",
                1,
            )
            .await;
            assert_eq!(
                door_instants(&ctx, &catalogs).await,
                vec![(1, micros)],
                "{door} {zone}"
            );
        }
    }
}
