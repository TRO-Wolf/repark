use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, TimestampNanosecondArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type, TimeUnit};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const INSTANTS: [&str; 6] = [
    "2026-01-02 03:04:05.123456789",
    "1969-12-31 23:59:59.999999999",
    "2026-03-08 06:59:59.999999999",
    "2026-03-08 07:00:00.000000001",
    "2026-11-01 05:30:00.000000001",
    "2026-11-01 06:30:00.000000001",
];
const INSTANT_TICKS: [i64; 6] = [
    1_767_323_045_123_456_789,
    -1,
    1_772_953_199_999_999_999,
    1_772_953_200_000_000_001,
    1_793_511_000_000_000_001,
    1_793_514_600_000_000_001,
];
const NEW_YORK_WALLS: [i64; 6] = [
    1_767_305_045_123_456_789,
    -18_000_000_000_001,
    1_772_935_199_999_999_999,
    1_772_938_800_000_000_001,
    1_793_496_600_000_000_001,
    1_793_496_600_000_000_001,
];
const KOLKATA_WALLS: [i64; 6] = [
    1_767_342_845_123_456_789,
    19_799_999_999_999,
    1_772_972_999_999_999_999,
    1_772_973_000_000_000_001,
    1_793_530_800_000_000_001,
    1_793_534_400_000_000_001,
];
const WRITES: [(&str, &str); 8] = [
    (
        "insert select",
        "INSERT INTO ice.ns.t SELECT id, tz, tz FROM ice.ns.src",
    ),
    (
        "insert overwrite",
        "INSERT OVERWRITE ice.ns.t SELECT id, tz, tz FROM ice.ns.src",
    ),
    (
        "replace where",
        "INSERT INTO ice.ns.t REPLACE WHERE id >= 0 SELECT id, tz, tz FROM ice.ns.src",
    ),
    (
        "merge insert",
        "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, v, c) VALUES (s.id, s.tz, s.tz)",
    ),
    (
        "merge insert star",
        "MERGE INTO ice.ns.t t USING (SELECT id, tz AS v, tz AS c FROM ice.ns.src) s \
         ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
    ),
    (
        "merge update",
        "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET v = s.tz",
    ),
    ("update where", "UPDATE ice.ns.t SET v = c WHERE id >= 0"),
    ("update", "UPDATE ice.ns.t SET v = c"),
];

fn session() -> ReparkSession {
    session_at("America/New_York")
}

fn session_at(zone: &str) -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
        .build()
        .expect("session")
}

async fn run(session: &ReparkSession, sql: &str) {
    session
        .sql(sql)
        .await
        .expect("SQL plan")
        .collect()
        .await
        .expect("SQL execute");
}

#[tokio::test]
async fn insert_and_merge_store_the_same_nanosecond_wall_time() {
    let warehouse = TempDir::new().expect("warehouse");
    let session = session();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id INT, ts timestamp_ns) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.t VALUES (1, TIMESTAMP '2026-01-02 03:04:05.123456789')",
    )
    .await;
    run(
        &session,
        "MERGE INTO ice.ns.t t USING (SELECT 2 AS id, \
         TIMESTAMP '2026-01-02 03:04:05.123456789' AS ts) s ON t.id=s.id \
         WHEN NOT MATCHED THEN INSERT *",
    )
    .await;
    let batches = session
        .sql("SELECT ts FROM ice.ns.t ORDER BY id")
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ns values")
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(values.len(), 2);
    println!("INSERT={} MERGE={}", values[0], values[1]);
    assert_eq!(
        values[0], values[1],
        "same input must store the same wall time across write doors"
    );
}

async fn ticks(session: &ReparkSession, sql: &str) -> (DataType, Vec<Option<i64>>) {
    let batches = session
        .sql(sql)
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let data_type = batches[0].schema().field(0).data_type().clone();
    let values = batches
        .iter()
        .flat_map(|batch| {
            let ints = cast(batch.column(0), &DataType::Int64).expect("ticks as int64");
            ints.as_primitive::<Int64Type>().iter().collect::<Vec<_>>()
        })
        .collect();
    (data_type, values)
}

async fn stored_through_every_door(zone: &str, target: &str) -> Vec<(DataType, Vec<Option<i64>>)> {
    let mut stored = Vec::new();
    for (door, write) in WRITES {
        let warehouse = TempDir::new().expect("warehouse");
        let session = session_at(zone);
        session
            .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
            .await
            .expect("catalog");
        run(&session, "CREATE NAMESPACE ice.ns").await;
        run(
            &session,
            "CREATE TABLE ice.ns.src (id INT, tz timestamptz_ns) USING iceberg \
             TBLPROPERTIES ('format-version'='3')",
        )
        .await;
        let rows = INSTANTS
            .iter()
            .enumerate()
            .map(|(index, text)| format!("({index}, CAST('{text}+00:00' AS timestamptz_ns))"))
            .collect::<Vec<_>>()
            .join(", ");
        run(&session, &format!("INSERT INTO ice.ns.src VALUES {rows}")).await;
        run(
            &session,
            &format!(
                "CREATE TABLE ice.ns.t (id INT, v {target}, c timestamptz_ns) USING iceberg \
                 TBLPROPERTIES ('format-version'='3')"
            ),
        )
        .await;
        if door.contains("update") || door == "replace where" {
            run(
                &session,
                "INSERT INTO ice.ns.t SELECT id, NULL, tz FROM ice.ns.src",
            )
            .await;
        }
        run(&session, write).await;
        let (source_type, source) = ticks(&session, "SELECT tz FROM ice.ns.src ORDER BY id").await;
        assert_eq!(
            source_type,
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
        );
        assert_eq!(source, INSTANT_TICKS.map(Some).to_vec(), "{door}");
        stored.push(ticks(&session, "SELECT v FROM ice.ns.t ORDER BY id").await);
    }
    stored
}

#[tokio::test]
async fn every_door_stores_a_nanosecond_instant_as_its_session_wall() {
    for (zone, walls) in [
        ("UTC", INSTANT_TICKS),
        ("America/New_York", NEW_YORK_WALLS),
        ("Asia/Kolkata", KOLKATA_WALLS),
    ] {
        let stored = stored_through_every_door(zone, "timestamp_ns").await;
        for ((door, _), (data_type, values)) in WRITES.iter().zip(stored) {
            assert_eq!(
                data_type,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                "{zone} {door}"
            );
            assert_eq!(values, walls.map(Some).to_vec(), "{zone} {door}");
        }
    }
}

#[tokio::test]
async fn every_door_keeps_a_nanosecond_instant_in_a_zoned_column() {
    for zone in ["UTC", "America/New_York", "Asia/Kolkata"] {
        let stored = stored_through_every_door(zone, "timestamptz_ns").await;
        for ((door, _), (data_type, values)) in WRITES.iter().zip(stored) {
            assert_eq!(
                data_type,
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                "{zone} {door}"
            );
            assert_eq!(values, INSTANT_TICKS.map(Some).to_vec(), "{zone} {door}");
        }
    }
}

fn session_with(zone: &str, ansi: bool) -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
        .config(
            "spark.sql.ansi.enabled",
            if ansi { "true" } else { "false" },
        )
        .build()
        .expect("session")
}

async fn attempt(session: &ReparkSession, sql: &str) -> Result<(), String> {
    match session.sql(sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn catalog(session: &ReparkSession) -> TempDir {
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(session, "CREATE NAMESPACE ice.ns").await;
    warehouse
}

const NESTED_INSTANT: &str = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'";
const NESTED_WALLS: [(&str, i64); 3] = [
    ("UTC", 1_767_323_045_123_456_000),
    ("America/New_York", 1_767_305_045_123_456_000),
    ("Asia/Kolkata", 1_767_342_845_123_456_000),
];

const OVERFLOW_LITERALS: [&str; 4] = [
    "TIMESTAMP '3000-01-01 00:00:00.000001+00:00'",
    "TIMESTAMP '1600-01-01 00:00:00.000001+00:00'",
    "DATE '3000-01-01'",
    "DATE '1677-09-21'",
];
const EDGE_LITERALS: [(&str, &str); 2] = [
    (
        "Asia/Kolkata",
        "TIMESTAMP '2262-04-11 23:47:16.854775+00:00'",
    ),
    (
        "America/New_York",
        "TIMESTAMP '1677-09-21 00:12:43.145225+00:00'",
    ),
];
const MERGE_ON_READ: &str = ", 'write.delete.mode'='merge-on-read', \
     'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read'";

#[tokio::test]
async fn an_overflowing_literal_stores_null_without_ansi_as_insert_does() {
    for zone in ["UTC", "America/New_York", "Asia/Kolkata"] {
        let session = session_with(zone, false);
        let _warehouse = catalog(&session).await;
        let edges = EDGE_LITERALS
            .iter()
            .filter(|(edge_zone, _)| *edge_zone == zone)
            .map(|(_, literal)| *literal);
        for (index, literal) in OVERFLOW_LITERALS.into_iter().chain(edges).enumerate() {
            let doors = [
                (
                    "insert values",
                    "",
                    format!("INSERT INTO {{t}} VALUES (1, {literal})"),
                ),
                (
                    "update where",
                    "",
                    format!("UPDATE {{t}} SET v = {literal} WHERE id = 1"),
                ),
                (
                    "update where mor",
                    MERGE_ON_READ,
                    format!("UPDATE {{t}} SET v = {literal} WHERE id = 1"),
                ),
                (
                    "merge insert values",
                    "",
                    format!(
                        "MERGE INTO {{t}} t USING (SELECT 1 AS id) s ON t.id = s.id \
                         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, {literal})"
                    ),
                ),
            ];
            for (door_index, (door, properties, write)) in doors.iter().enumerate() {
                let table = format!("ice.ns.o{index}_{door_index}");
                run(
                    &session,
                    &format!(
                        "CREATE TABLE {table} (id INT, v timestamp_ns) USING iceberg \
                         TBLPROPERTIES ('format-version'='3'{properties})"
                    ),
                )
                .await;
                run(&session, &format!("INSERT INTO {table} VALUES (100, NULL)")).await;
                if door.starts_with("update") {
                    run(&session, &format!("INSERT INTO {table} VALUES (1, NULL)")).await;
                    run(
                        &session,
                        &format!("UPDATE {table} SET v = {NESTED_INSTANT} WHERE id = 100"),
                    )
                    .await;
                }
                let outcome = attempt(&session, &write.replace("{t}", &table)).await;
                assert_eq!(outcome, Ok(()), "{zone} {door} {literal}");
                let (_, stored) =
                    ticks(&session, &format!("SELECT v FROM {table} WHERE id = 1")).await;
                assert_eq!(stored, vec![None], "{zone} {door} {literal}");
            }
        }
    }
}

const WALL_SOURCE_DOORS: [(&str, &str); 4] = [
    (
        "insert select",
        "INSERT INTO ice.ns.t SELECT id, {c}, 0 FROM ice.ns.src WHERE id IN (100, {i})",
    ),
    (
        "insert overwrite",
        "INSERT OVERWRITE ice.ns.t SELECT id, {c}, 0 FROM ice.ns.src WHERE id IN (100, {i})",
    ),
    (
        "insert by name",
        "INSERT INTO ice.ns.t BY NAME SELECT 0 AS k, {c} AS v, id FROM ice.ns.src \
         WHERE id IN (100, {i})",
    ),
    (
        "insert overwrite by name",
        "INSERT OVERWRITE ice.ns.t BY NAME SELECT 0 AS k, {c} AS v, id FROM ice.ns.src \
         WHERE id IN (100, {i})",
    ),
];

#[tokio::test]
async fn an_overflowing_wall_source_answers_as_insert_select_on_the_overwrite_doors() {
    for zone in ["UTC", "America/New_York", "Asia/Kolkata"] {
        for ansi in [true, false] {
            for (column, normal) in [
                ("n", 1_767_323_045_123_456_000_i64),
                ("d", 1_767_312_000_000_000_000_i64),
            ] {
                for row in [1, 2] {
                    for (door, write) in WALL_SOURCE_DOORS {
                        let session = session_with(zone, ansi);
                        let _warehouse = catalog(&session).await;
                        run(
                            &session,
                            "CREATE TABLE ice.ns.src (id INT, n TIMESTAMP_NTZ, d DATE) USING \
                             iceberg TBLPROPERTIES ('format-version'='3')",
                        )
                        .await;
                        run(
                            &session,
                            "INSERT INTO ice.ns.src \
                             SELECT 100, TIMESTAMP_NTZ '2026-01-02 03:04:05.123456', \
                             DATE '2026-01-02' UNION ALL \
                             SELECT 1, TIMESTAMP_NTZ '2026-01-01 00:00:00.000001' + \
                             INTERVAL '974' YEAR, DATE '3000-01-01' UNION ALL \
                             SELECT 2, TIMESTAMP_NTZ '2026-01-01 00:00:00.000001' - \
                             INTERVAL '426' YEAR, DATE '1677-09-21'",
                        )
                        .await;
                        run(
                            &session,
                            "CREATE TABLE ice.ns.t (id INT, v timestamp_ns, k INT) USING iceberg \
                             TBLPROPERTIES ('format-version'='3')",
                        )
                        .await;
                        let write = write
                            .replace("{c}", column)
                            .replace("{i}", &row.to_string());
                        let outcome = attempt(&session, &write).await;
                        let label = format!("{zone} ansi={ansi} {door} {column} row {row}");
                        if ansi {
                            let error = outcome.expect_err(&label);
                            assert!(error.contains("[CAST_OVERFLOW]"), "{label}: {error}");
                            assert!(error.contains("\"TIMESTAMP_NS\""), "{label}: {error}");
                        } else {
                            assert_eq!(outcome, Ok(()), "{label}");
                            let (_, stored) =
                                ticks(&session, "SELECT v FROM ice.ns.t ORDER BY id").await;
                            assert_eq!(stored, vec![None, Some(normal)], "{label}");
                        }
                    }
                }
            }
        }
    }
}

fn leaf_ticks(array: &ArrayRef) -> Vec<Option<i64>> {
    match array.data_type() {
        DataType::Struct(_) => leaf_ticks(array.as_struct().column(0)),
        DataType::List(_) => leaf_ticks(array.as_list::<i32>().values()),
        DataType::Map(_, _) => leaf_ticks(array.as_map().entries().column(1)),
        _ => {
            let ints = cast(array, &DataType::Int64).expect("ticks as int64");
            ints.as_primitive::<Int64Type>().iter().collect()
        }
    }
}

async fn nested_ticks(session: &ReparkSession, table: &str) -> Vec<Option<i64>> {
    let batches = session
        .sql(&format!("SELECT st FROM {table} ORDER BY id"))
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    batches
        .iter()
        .flat_map(|batch| leaf_ticks(batch.column(0)))
        .collect()
}

const CARRY_SEED: [&str; 4] = [
    "2026-01-02 03:04:05.123456789",
    "1969-12-31 23:59:59.999999999",
    "2026-03-08 02:30:00.000000001",
    "2026-11-01 01:30:00.000000001",
];
const CARRY_TICKS: [i64; 4] = [
    1_767_323_045_123_456_789,
    -1,
    1_772_937_000_000_000_001,
    1_793_496_600_000_000_001,
];
const CARRIES: [(&str, &[&str]); 6] = [
    ("delete", &["DELETE FROM ice.ns.t WHERE id = 0"]),
    (
        "update sibling",
        &[
            "UPDATE ice.ns.t SET k = k + 1 WHERE id > 0",
            "DELETE FROM ice.ns.t WHERE id = 0",
        ],
    ),
    (
        "merge sibling",
        &[
            "MERGE INTO ice.ns.t t USING (SELECT id FROM ice.ns.t) s ON t.id = s.id \
             WHEN MATCHED AND t.id = 0 THEN DELETE WHEN MATCHED THEN UPDATE SET k = 7",
        ],
    ),
    (
        "rewrite data files",
        &[
            "DELETE FROM ice.ns.t WHERE id = 0",
            "CALL ice.system.rewrite_data_files(table => 'ns.t', \
             options => map('min-input-files', '2', 'rewrite-all', 'true'))",
        ],
    ),
    (
        "rewrite manifests",
        &[
            "DELETE FROM ice.ns.t WHERE id = 0",
            "CALL ice.system.rewrite_manifests(table => 'ns.t')",
        ],
    ),
    (
        "rewrite position delete files",
        &[
            "DELETE FROM ice.ns.t WHERE id = 0",
            "CALL ice.system.rewrite_position_delete_files(table => 'ns.t')",
        ],
    ),
];

#[tokio::test]
async fn untouched_rows_carry_their_nanosecond_ticks() {
    for (column_type, suffix) in [("timestamp_ns", ""), ("timestamptz_ns", "+00:00")] {
        for properties in ["", MERGE_ON_READ] {
            for (carry, statements) in CARRIES {
                let session = session_at("America/New_York");
                let _warehouse = catalog(&session).await;
                run(
                    &session,
                    &format!(
                        "CREATE TABLE ice.ns.t (id INT, v {column_type}, k INT) USING iceberg \
                         TBLPROPERTIES ('format-version'='3'{properties})"
                    ),
                )
                .await;
                for half in [0..2, 2..4] {
                    let rows = half
                        .map(|index| {
                            let text = CARRY_SEED[index];
                            format!("({index}, CAST('{text}{suffix}' AS {column_type}), 0)")
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    run(&session, &format!("INSERT INTO ice.ns.t VALUES {rows}")).await;
                }
                let label = format!("{column_type} {carry} mor={}", !properties.is_empty());
                let (_, seeded) = ticks(&session, "SELECT v FROM ice.ns.t ORDER BY id").await;
                assert_eq!(seeded, CARRY_TICKS.map(Some).to_vec(), "{label} seed");
                for statement in statements {
                    run(&session, statement).await;
                }
                let (_, carried) = ticks(&session, "SELECT v FROM ice.ns.t ORDER BY id").await;
                assert_eq!(
                    carried,
                    CARRY_TICKS[1..]
                        .iter()
                        .map(|v| Some(*v))
                        .collect::<Vec<_>>(),
                    "{label}"
                );
            }
        }
    }
}

const NTZ_NESTED_DOORS: [&str; 6] = [
    "insert values",
    "insert select",
    "merge set field",
    "merge set struct",
    "update set field",
    "merge insert",
];

/// Rewritten 2026-10-20 (ICE-NTZ-NESTED-REFUSE-1, ruling T-3): this pin held the split main
/// had, field assignment on the session wall and the other doors on the UTC wall. Outside UTC
/// the six doors now refuse an instant with one text and write nothing. In UTC they store the
/// walls they stored before. The text was re-pinned the same day, in fix round 1: the hint no
/// longer names the source's type.
///
/// pins: ice-ntz-nested-refuse-1/C-002, C-003
#[tokio::test]
async fn a_nested_microsecond_ntz_field_refuses_an_instant_outside_utc() {
    let lit = NESTED_INSTANT;
    for (zone, _) in NESTED_WALLS {
        let session = session_at(zone);
        let _warehouse = catalog(&session).await;
        run(
            &session,
            "CREATE TABLE ice.ns.t (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>) \
             USING iceberg TBLPROPERTIES ('format-version'='3')",
        )
        .await;
        run(
            &session,
            "INSERT INTO ice.ns.t VALUES (3, named_struct('v', NULL, 'n', 1)), \
             (4, named_struct('v', NULL, 'n', 1)), (5, named_struct('v', NULL, 'n', 1))",
        )
        .await;
        let writes = [
            format!("INSERT INTO ice.ns.t VALUES (1, named_struct('v', {lit}, 'n', 1))"),
            format!("INSERT INTO ice.ns.t SELECT 2, named_struct('v', {lit}, 'n', 1)"),
            format!(
                "MERGE INTO ice.ns.t t USING (SELECT 3 AS id, {lit} AS x) s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET t.st.v = s.x"
            ),
            format!(
                "MERGE INTO ice.ns.t t USING (SELECT 4 AS id, {lit} AS x) s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET t.st = named_struct('v', s.x, 'n', 1)"
            ),
            format!("UPDATE ice.ns.t SET st.v = {lit} WHERE id = 5"),
            format!(
                "MERGE INTO ice.ns.t t USING (SELECT 6 AS id, {lit} AS x) s ON t.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, named_struct('v', s.x, 'n', 1))"
            ),
        ];
        let mut outcomes = Vec::new();
        for (door, write) in NTZ_NESTED_DOORS.iter().zip(&writes) {
            let outcome = attempt(&session, write).await;
            let refused = outcome.as_ref().is_err_and(|error| {
                error.contains(
                    "Cannot safely cast `st`.`v` to \"TIMESTAMP_NTZ\". A value that is not \
                     provably a TIMESTAMP_NTZ wall time is not written into a nested \
                     TIMESTAMP_NTZ field while the session time zone is not UTC",
                )
            });
            outcomes.push((*door, outcome.is_ok(), refused));
        }
        let stored = nested_ticks(&session, "ice.ns.t").await;
        if zone == "UTC" {
            let wall = Some(NESTED_WALLS[0].1 / 1_000);
            let expected: Vec<_> = NTZ_NESTED_DOORS
                .iter()
                .map(|door| (*door, true, false))
                .collect();
            assert_eq!(outcomes, expected, "{zone}");
            assert_eq!(stored, vec![wall; 6], "{zone}");
        } else {
            let expected: Vec<_> = NTZ_NESTED_DOORS
                .iter()
                .map(|door| (*door, false, true))
                .collect();
            assert_eq!(outcomes, expected, "{zone}");
            assert_eq!(stored, vec![None; 3], "{zone}");
        }
    }
}

const ZONE_FLOOR_SEED: [&str; 4] = [
    "1969-12-31 23:59:59.999999999",
    "1969-12-31 23:59:59.999998999",
    "1969-12-31 23:59:59.999999000",
    "2026-03-08 02:30:00.000000001",
];
const ZONE_FLOOR_TARGETS: [(&str, &str); 2] = [("TIMESTAMP", "n"), ("TIMESTAMP_NTZ", "z")];
const ZONE_FLOOR_INSERT: &str = "INSERT INTO {t} SELECT id, {s}, 0 FROM ice.ns.src";
const ZONE_FLOOR_DOORS: [(&str, &str); 4] = [
    (
        "merge insert",
        "MERGE INTO {t} t USING ice.ns.src s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, s.{s}, 0)",
    ),
    (
        "merge insert star",
        "MERGE INTO {t} t USING (SELECT id, {s} AS v, 0 AS k FROM ice.ns.src) s \
         ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
    ),
    (
        "insert overwrite",
        "INSERT OVERWRITE {t} SELECT id, {s}, 0 FROM ice.ns.src",
    ),
    (
        "insert by name",
        "INSERT INTO {t} BY NAME SELECT 0 AS k, {s} AS v, id FROM ice.ns.src",
    ),
];

async fn zone_floor_session(zone: &str) -> (ReparkSession, TempDir) {
    let session = session_at(zone);
    let warehouse = catalog(&session).await;
    run(
        &session,
        "CREATE TABLE ice.ns.src (id INT, n timestamp_ns, z timestamptz_ns) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    let rows = ZONE_FLOOR_SEED
        .iter()
        .enumerate()
        .map(|(index, text)| {
            format!(
                "({index}, CAST('{text}' AS timestamp_ns), CAST('{text}+00:00' AS timestamptz_ns))"
            )
        })
        .chain(std::iter::once(format!(
            "({}, NULL, NULL)",
            ZONE_FLOOR_SEED.len()
        )))
        .collect::<Vec<_>>()
        .join(", ");
    run(&session, &format!("INSERT INTO ice.ns.src VALUES {rows}")).await;
    (session, warehouse)
}

async fn zone_floor_store(
    session: &ReparkSession,
    table: &str,
    target: &str,
    version: u8,
    write: &str,
    source: &str,
) -> Vec<Option<i64>> {
    run(
        session,
        &format!(
            "CREATE TABLE {table} (id INT, v {target}, k INT) USING iceberg PARTITIONED BY (k) \
             TBLPROPERTIES ('format-version'='{version}')"
        ),
    )
    .await;
    run(session, &write.replace("{t}", table).replace("{s}", source)).await;
    let (_, stored) = ticks(session, &format!("SELECT v FROM {table} ORDER BY id")).await;
    stored
}

#[tokio::test]
async fn every_door_stores_a_nanosecond_source_as_insert_does_in_new_york() {
    let (utc, _utc_warehouse) = zone_floor_session("UTC").await;
    let (new_york, _new_york_warehouse) = zone_floor_session("America/New_York").await;
    for (target_index, (target, source)) in ZONE_FLOOR_TARGETS.into_iter().enumerate() {
        for version in [2, 3] {
            let label = format!("{source} into {target} v{version}");
            let table = |door: &str| format!("ice.ns.t{target_index}_v{version}_{door}");
            let utc_reading = zone_floor_store(
                &utc,
                &table("insert"),
                target,
                version,
                ZONE_FLOOR_INSERT,
                source,
            )
            .await;
            let insert = zone_floor_store(
                &new_york,
                &table("insert"),
                target,
                version,
                ZONE_FLOOR_INSERT,
                source,
            )
            .await;
            assert_eq!(insert.len(), ZONE_FLOOR_SEED.len() + 1, "{label}");
            assert_eq!(insert.last(), Some(&None), "{label}");
            assert_ne!(insert, utc_reading, "{label}: INSERT reads the UTC wall");
            for (door_index, (door, write)) in ZONE_FLOOR_DOORS.into_iter().enumerate() {
                let stored = zone_floor_store(
                    &new_york,
                    &table(&door_index.to_string()),
                    target,
                    version,
                    write,
                    source,
                )
                .await;
                assert_eq!(stored, insert, "{label} {door}");
            }
        }
    }
}
