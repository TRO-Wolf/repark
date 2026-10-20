use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const INSTANT: &str = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'";
const WALL: &str = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'";
const UTC_WALL: i64 = 1_767_323_045_123_456;
const ZONES: [(&str, i64); 2] = [
    ("America/New_York", 1_767_305_045_123_456),
    ("Asia/Kolkata", 1_767_342_845_123_456),
];
const HEAD: &str = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible \
                    data for the table `ice`.`ns`.";
const HINT: &str = "A value that is not provably a TIMESTAMP_NTZ wall time is not written into a \
                    nested TIMESTAMP_NTZ field while the session time zone is not UTC";

fn session(zone: &str) -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
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

async fn run(session: &ReparkSession, sql: &str) {
    assert_eq!(attempt(session, sql).await, Ok(()), "{sql}");
}

async fn catalog(session: &ReparkSession) -> TempDir {
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(session, "CREATE NAMESPACE ice.ns").await;
    run(
        session,
        "CREATE TABLE ice.ns.src (id INT, x TIMESTAMP, w TIMESTAMP_NTZ) USING iceberg \
         TBLPROPERTIES ('format-version'='2')",
    )
    .await;
    run(
        session,
        &format!("INSERT INTO ice.ns.src VALUES (1, {INSTANT}, {WALL})"),
    )
    .await;
    run(
        session,
        "CREATE TEMPORARY VIEW zoned AS SELECT id, x, w FROM ice.ns.src",
    )
    .await;
    warehouse
}

fn leaf(array: &ArrayRef) -> Vec<Option<i64>> {
    match array.data_type() {
        DataType::Struct(_) => leaf(array.as_struct().column(0)),
        DataType::List(_) => leaf(array.as_list::<i32>().values()),
        DataType::Map(_, _) => leaf(array.as_map().entries().column(1)),
        _ => {
            let ints = cast(array, &DataType::Int64).expect("ticks as int64");
            ints.as_primitive::<Int64Type>().iter().collect()
        }
    }
}

async fn stored(session: &ReparkSession, sql: &str) -> Vec<Option<i64>> {
    let batches = session
        .sql(sql)
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    batches
        .iter()
        .flat_map(|batch| leaf(batch.column(0)))
        .collect()
}

fn files_under(root: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() { files_under(&path) } else { 1 }
        })
        .sum()
}

const SHAPES: [(&str, &str, &str, &str); 5] = [
    (
        "struct",
        "STRUCT<v: TIMESTAMP_NTZ, n: INT>",
        "named_struct('v', {x}, 'n', 1)",
        "`st`.`v`",
    ),
    (
        "struct in struct",
        "STRUCT<i: STRUCT<v: TIMESTAMP_NTZ>, n: INT>",
        "named_struct('i', named_struct('v', {x}), 'n', 1)",
        "`st`.`i`.`v`",
    ),
    (
        "array",
        "ARRAY<TIMESTAMP_NTZ>",
        "array({x})",
        "`st`.`element`",
    ),
    (
        "array of struct",
        "ARRAY<STRUCT<v: TIMESTAMP_NTZ>>",
        "array(named_struct('v', {x}))",
        "`st`.`element`.`v`",
    ),
    (
        "map value",
        "MAP<STRING, TIMESTAMP_NTZ>",
        "map('a', {x})",
        "`st`.`value`",
    ),
];

const DOORS: [(&str, &str, &str); 18] = [
    (
        "insert values",
        "INSERT INTO {t} VALUES (2, {st}, 0)",
        "lit",
    ),
    (
        "insert select",
        "INSERT INTO {t} SELECT id + 1, {st}, 0 FROM ice.ns.src",
        "col",
    ),
    (
        "insert with a column list",
        "INSERT INTO {t} (id, st) SELECT id + 1, {st} FROM ice.ns.src",
        "col",
    ),
    (
        "insert by name",
        "INSERT INTO {t} BY NAME SELECT 0 AS k, {st} AS st, id + 1 AS id FROM ice.ns.src",
        "col",
    ),
    (
        "insert by name, star",
        "INSERT INTO {t} BY NAME SELECT * FROM \
         (SELECT 0 AS k, {st} AS st, id + 1 AS id FROM ice.ns.src) AS s",
        "col",
    ),
    (
        "insert overwrite",
        "INSERT OVERWRITE {t} SELECT id, {st}, 0 FROM ice.ns.src",
        "col",
    ),
    (
        "insert overwrite values",
        "INSERT OVERWRITE {t} VALUES (1, {st}, 0)",
        "lit",
    ),
    (
        "replace where",
        "INSERT INTO {t} REPLACE WHERE id >= 0 SELECT id, {st}, 0 FROM ice.ns.src",
        "col",
    ),
    (
        "merge insert",
        "MERGE INTO {t} t USING (SELECT id + 5 AS id, {st} AS v FROM ice.ns.src) s \
         ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, st, k) VALUES (s.id, s.v, 0)",
        "col",
    ),
    (
        "merge insert built in the clause",
        "MERGE INTO {t} t USING ice.ns.src s ON t.id = s.id + 5 \
         WHEN NOT MATCHED THEN INSERT (id, st, k) VALUES (s.id + 5, {st}, 0)",
        "s.x",
    ),
    (
        "merge insert star",
        "MERGE INTO {t} t USING (SELECT id + 5 AS id, {st} AS st, 0 AS k FROM ice.ns.src) s \
         ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
        "col",
    ),
    (
        "merge update",
        "MERGE INTO {t} t USING (SELECT id, {st} AS v FROM ice.ns.src) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.st = s.v",
        "col",
    ),
    (
        "merge update star",
        "MERGE INTO {t} t USING (SELECT id, {st} AS st, 0 AS k FROM ice.ns.src) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET *",
        "col",
    ),
    (
        "update where",
        "UPDATE {t} SET st = {st} WHERE id >= 0",
        "lit",
    ),
    ("update with no where", "UPDATE {t} SET st = {st}", "lit"),
    (
        "insert select from a temporary view",
        "INSERT INTO {t} SELECT id + 1, {st}, 0 FROM zoned",
        "col",
    ),
    (
        "insert select from a subquery",
        "INSERT INTO {t} SELECT id, v, 0 FROM (SELECT id + 1 AS id, {st} AS v FROM ice.ns.src) s",
        "col",
    ),
    (
        "insert values with a scalar subquery",
        "INSERT INTO {t} VALUES (2, {st}, 0)",
        "(SELECT x FROM ice.ns.src)",
    ),
];

fn value(spelling: &str, instant: bool) -> String {
    match (spelling, instant) {
        ("lit", true) => INSTANT.to_string(),
        ("lit", false) => WALL.to_string(),
        ("col", true) => "x".to_string(),
        ("col", false) => "w".to_string(),
        ("s.x", true) => "s.x".to_string(),
        ("s.x", false) => "s.w".to_string(),
        (other, true) => other.to_string(),
        (other, false) => other.replace("x FROM", "w FROM"),
    }
}

async fn seeded(session: &ReparkSession, table: &str, column_type: &str) {
    run(
        session,
        &format!(
            "CREATE TABLE {table} (id INT, st {column_type}, k INT) USING iceberg \
             TBLPROPERTIES ('format-version'='2')"
        ),
    )
    .await;
    run(
        session,
        &format!("INSERT INTO {table} (id, k) VALUES (1, 0)"),
    )
    .await;
}

/// Every door refuses an instant for a nested microsecond `TIMESTAMP_NTZ` leaf outside UTC.
///
/// Five shapes go through eighteen doors in two zones. Each must take the named refusal
/// with the target's leaf path. Per shape no file may appear and no snapshot be added.
///
/// pins: ice-ntz-nested-refuse-1/C-002
#[tokio::test]
async fn every_door_refuses_an_instant_outside_utc_and_writes_nothing() {
    let mut wrong = Vec::new();
    for (zone, _) in ZONES {
        let session = session(zone);
        let warehouse = catalog(&session).await;
        for (shape_index, (shape, column_type, build, path)) in SHAPES.iter().enumerate() {
            let table = format!("ice.ns.n{shape_index}");
            seeded(&session, &table, column_type).await;
            let before = files_under(warehouse.path());
            for (door, write, spelling) in DOORS {
                let write = write
                    .replace("{t}", &table)
                    .replace("{st}", &build.replace("{x}", &value(spelling, true)));
                let label = format!("{zone} / {shape} / {door}");
                match attempt(&session, &write).await {
                    Ok(()) => wrong.push(format!("{label}: stored")),
                    Err(error) => {
                        let named = error.contains(HEAD)
                            && error.contains(HINT)
                            && error.contains(&format!("Cannot safely cast {path} to"));
                        if !named {
                            wrong.push(format!("{label}: {error}"));
                        }
                    }
                }
            }
            if files_under(warehouse.path()) != before {
                wrong.push(format!("{zone} / {shape}: a file was written"));
            }
            let count = format!("SELECT count(*) FROM ice.ns.`n{shape_index}$snapshots`");
            if stored(&session, &count).await != vec![Some(1)] {
                wrong.push(format!("{zone} / {shape}: a snapshot was added"));
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

const FIELD_DOORS: [&str; 5] = [
    "MERGE INTO {t} t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.st.v = s.x",
    "MERGE INTO {t} t USING (SELECT 1 AS id) s ON t.id = s.id \
     WHEN MATCHED THEN UPDATE SET st.v = {lit}",
    "UPDATE {t} SET st.v = {lit} WHERE id >= 0",
    "UPDATE {t} SET st.v = {lit}",
    "UPDATE {t} SET st.v = (SELECT x FROM ice.ns.src) WHERE id >= 0",
];

/// A field assignment of an instant refuses too, though it stored the session wall on main.
///
/// pins: ice-ntz-nested-refuse-1/C-002
#[tokio::test]
async fn a_field_assignment_of_an_instant_refuses_too() {
    for (zone, _) in ZONES {
        let session = session(zone);
        let _warehouse = catalog(&session).await;
        seeded(&session, "ice.ns.t", "STRUCT<v: TIMESTAMP_NTZ, n: INT>").await;
        for write in FIELD_DOORS {
            let write = write.replace("{t}", "ice.ns.t").replace("{lit}", INSTANT);
            let refused = attempt(&session, &write).await.expect_err(&write);
            assert!(refused.contains(HINT), "{zone} {write}: {refused}");
            assert!(
                refused.contains("Cannot safely cast `st`.`v` to \"TIMESTAMP_NTZ\""),
                "{zone} {write}: {refused}"
            );
        }
        let snapshots = stored(&session, "SELECT count(*) FROM ice.ns.`t$snapshots`").await;
        assert_eq!(snapshots, vec![Some(1)], "{zone}");
    }
}

/// The whole text: the table, the column and the leaf, and a hint that deletes nothing.
///
/// pins: ice-ntz-nested-refuse-1/C-006
#[tokio::test]
async fn the_refusal_names_the_table_the_column_and_the_leaf() {
    let session = session("America/New_York");
    let _warehouse = catalog(&session).await;
    seeded(&session, "ice.ns.t", "STRUCT<a: INT, b: TIMESTAMP_NTZ>").await;
    let refused = attempt(
        &session,
        "INSERT INTO ice.ns.t SELECT id, named_struct('a', 1, 'b', x), 0 FROM ice.ns.src",
    )
    .await
    .expect_err("refused");
    let expected = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible \
                    data for the table `ice`.`ns`.`t`: Cannot safely cast `st`.`b` to \
                    \"TIMESTAMP_NTZ\". A value that is not provably a TIMESTAMP_NTZ wall time \
                    is not written into a nested TIMESTAMP_NTZ field while the session time zone \
                    is not UTC: cast the leaf itself, as in named_struct('<field>', \
                    CAST(<value> AS TIMESTAMP_NTZ), ...) or array(CAST(<value> AS \
                    TIMESTAMP_NTZ)), to store the session wall time (a CAST of the whole struct, \
                    array or map is refused, and in an UPDATE assign the field itself), or set \
                    spark.sql.session.timeZone to UTC for the statement. SQLSTATE: KD000";
    assert!(refused.ends_with(expected), "{refused}");
    let lowered = expected.to_lowercase();
    for word in [
        "drop",
        "truncate",
        "overwrite",
        "recreate",
        "delete",
        "replace",
    ] {
        assert!(!lowered.contains(word), "{word}");
    }
}

async fn stored_by(
    session: &ReparkSession,
    table: &str,
    door: &str,
    write: &str,
) -> Result<Vec<i64>, String> {
    seeded(session, table, "STRUCT<v: TIMESTAMP_NTZ, n: INT>").await;
    attempt(session, write).await?;
    let read = format!("SELECT st FROM {table} WHERE st.v IS NOT NULL");
    let values: Vec<i64> = stored(session, &read).await.into_iter().flatten().collect();
    if values.is_empty() {
        return Err(format!("{door}: nothing stored"));
    }
    Ok(values)
}

/// A naive value still stores outside UTC: a wall stores itself, and the hint's
/// `CAST(<instant> AS TIMESTAMP_NTZ)` stores the session wall Spark stores. The plain
/// `UPDATE` that assigns the whole struct adds the session offset to a `TIMESTAMP_NTZ` cast,
/// so there the cast refuses.
///
/// pins: ice-ntz-nested-refuse-1/C-004, C-005
#[tokio::test]
async fn a_wall_or_the_hints_cast_stores_outside_utc_and_the_plain_update_refuses_the_cast() {
    let mut wrong = Vec::new();
    for (zone, session_wall) in ZONES {
        let session = session(zone);
        let _warehouse = catalog(&session).await;
        let mut index = 0;
        for (door, write, spelling) in DOORS {
            if matches!(
                door,
                "update with no where" | "insert values with a scalar subquery"
            ) {
                continue;
            }
            for (kind, inner, expected) in [
                ("wall", value(spelling, false), UTC_WALL),
                (
                    "cast",
                    format!("CAST({} AS TIMESTAMP_NTZ)", value(spelling, true)),
                    session_wall,
                ),
            ] {
                index += 1;
                let table = format!("ice.ns.w{index}");
                let build = format!("named_struct('v', {inner}, 'n', 1)");
                let write = write.replace("{t}", &table).replace("{st}", &build);
                let label = format!("{zone} / {door} / {kind}");
                let outcome = stored_by(&session, &table, door, &write).await;
                match (door, kind, outcome) {
                    ("update where", "cast", Err(error)) if error.contains(HINT) => {}
                    ("update where", "cast", outcome) => {
                        wrong.push(format!("{label}: {outcome:?}"));
                    }
                    (_, _, Ok(values)) if values.iter().all(|value| *value == expected) => {}
                    (_, _, outcome) => wrong.push(format!("{label}: {outcome:?}")),
                }
            }
        }
        let table = "ice.ns.field";
        seeded(&session, table, "STRUCT<v: TIMESTAMP_NTZ, n: INT>").await;
        run(
            &session,
            &format!("UPDATE {table} SET st.v = CAST({INSTANT} AS TIMESTAMP_NTZ) WHERE id >= 0"),
        )
        .await;
        let read = format!("SELECT st FROM {table}");
        assert_eq!(stored(&session, &read).await, vec![Some(session_wall)]);
    }
    assert_eq!(wrong, Vec::<String>::new());
}

const UNSUPPLIED: [&str; 10] = [
    "INSERT INTO ice.ns.t (id, k) VALUES (10, 0)",
    "INSERT INTO ice.ns.t (id, k) SELECT id + 10, 0 FROM ice.ns.src",
    "INSERT INTO ice.ns.t VALUES (12, NULL, 0)",
    "INSERT INTO ice.ns.t BY NAME SELECT 0 AS k, id + 13 AS id FROM ice.ns.src",
    "UPDATE ice.ns.t SET k = k + 1 WHERE id >= 10",
    "UPDATE ice.ns.t SET st.n = 7 WHERE id = 1",
    "UPDATE ice.ns.t SET st = NULL WHERE id = 10",
    "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id \
     WHEN MATCHED THEN UPDATE SET k = 7 \
     WHEN NOT MATCHED THEN INSERT (id, k) VALUES (s.id + 20, 0)",
    "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id + 30 \
     WHEN NOT MATCHED THEN INSERT (id, st, k) VALUES (s.id + 30, NULL, 0)",
    "DELETE FROM ice.ns.t WHERE id = 11",
];

/// A statement that does not supply the column runs outside UTC and keeps the stored wall.
///
/// pins: ice-ntz-nested-refuse-1/C-007
#[tokio::test]
async fn a_statement_that_does_not_supply_the_column_runs_and_carries_its_rows() {
    let session = session("America/New_York");
    let _warehouse = catalog(&session).await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT) \
         USING iceberg TBLPROPERTIES ('format-version'='2')",
    )
    .await;
    run(
        &session,
        &format!("INSERT INTO ice.ns.t VALUES (1, named_struct('v', {WALL}, 'n', 1), 0)"),
    )
    .await;
    let carried = "SELECT st FROM ice.ns.t WHERE id = 1";
    for write in UNSUPPLIED {
        run(&session, write).await;
        assert_eq!(
            stored(&session, carried).await,
            vec![Some(UTC_WALL)],
            "{write}"
        );
    }
    for call in [
        "CALL ice.system.rewrite_data_files(table => 'ns.t', \
         options => map('min-input-files', '2', 'rewrite-all', 'true'))",
        "CALL ice.system.rewrite_manifests(table => 'ns.t')",
    ] {
        run(&session, call).await;
        assert_eq!(
            stored(&session, carried).await,
            vec![Some(UTC_WALL)],
            "{call}"
        );
    }
}

/// Controls: a zoned or nanosecond leaf and a top-level `TIMESTAMP_NTZ` answer as on main.
///
/// pins: ice-ntz-nested-refuse-1/C-008
#[tokio::test]
async fn a_zoned_leaf_and_a_top_level_ntz_column_are_not_guarded() {
    let session = session("America/New_York");
    let _warehouse = catalog(&session).await;
    for (index, (column_type, build, expected)) in [
        (
            "STRUCT<v: TIMESTAMP, n: INT>",
            "named_struct('v', x, 'n', 1)",
            UTC_WALL,
        ),
        (
            "STRUCT<v: timestamptz_ns, n: INT>",
            "named_struct('v', x, 'n', 1)",
            UTC_WALL * 1_000,
        ),
        ("TIMESTAMP_NTZ", "x", ZONES[0].1),
    ]
    .into_iter()
    .enumerate()
    {
        let table = format!("ice.ns.c{index}");
        run(
            &session,
            &format!(
                "CREATE TABLE {table} (id INT, st {column_type}) USING iceberg \
                 TBLPROPERTIES ('format-version'='3')"
            ),
        )
        .await;
        run(
            &session,
            &format!("INSERT INTO {table} SELECT id, {build} FROM ice.ns.src"),
        )
        .await;
        let read = format!("SELECT st FROM {table}");
        assert_eq!(
            stored(&session, &read).await,
            vec![Some(expected)],
            "{column_type}"
        );
    }
    run(
        &session,
        "CREATE TABLE ice.ns.both (id INT, st STRUCT<a: timestamp_ns, b: TIMESTAMP_NTZ>) \
         USING iceberg TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    let refused = attempt(
        &session,
        "INSERT INTO ice.ns.both SELECT id, named_struct('a', CAST(NULL AS timestamp_ns), 'b', x) \
         FROM ice.ns.src",
    )
    .await
    .expect_err("refused");
    assert!(
        refused.contains("A nested timestamp_ns leaf is not writable yet"),
        "{refused}"
    );
}

/// The type read: a `WITH` clause before the `INSERT` types its source (a wall passes the gate
/// and gets main's own answer, an instant refuses), an instant into a zoned sibling field is
/// not a supply of the naive leaf, a static `PARTITION` clause reads every source column, and
/// a value the router cannot plan refuses (fail closed) where main raised the planner's own
/// error.
///
/// pins: ice-ntz-nested-refuse-1/C-001, C-004
#[tokio::test]
async fn the_type_read_follows_a_cte_skips_a_zoned_sibling_and_fails_closed() {
    let session = session("America/New_York");
    let _warehouse = catalog(&session).await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id INT, st STRUCT<v: TIMESTAMP_NTZ, w: TIMESTAMP>, k INT) \
         USING iceberg TBLPROPERTIES ('format-version'='2')",
    )
    .await;
    run(&session, "INSERT INTO ice.ns.t (id, k) VALUES (1, 0)").await;
    let walled = attempt(
        &session,
        &format!(
            "WITH c AS (SELECT 2 AS id, {WALL} AS w) \
             INSERT INTO ice.ns.t SELECT id, named_struct('v', w, 'w', NULL), 0 FROM c"
        ),
    )
    .await
    .expect_err("main's own answer");
    assert!(walled.contains("not implemented yet"), "{walled}");
    run(
        &session,
        &format!("UPDATE ice.ns.t SET st.w = {INSTANT} WHERE id = 1"),
    )
    .await;
    let zoned = stored(&session, "SELECT st.w FROM ice.ns.t WHERE id = 1").await;
    assert_eq!(zoned, vec![Some(UTC_WALL)]);
    let refused = attempt(
        &session,
        &format!(
            "WITH c AS (SELECT 3 AS id, {INSTANT} AS w) \
             INSERT INTO ice.ns.t SELECT id, named_struct('v', w, 'w', NULL), 0 FROM c"
        ),
    )
    .await
    .expect_err("an instant through a CTE");
    assert!(refused.contains(HINT), "{refused}");
    let untyped = attempt(
        &session,
        "INSERT INTO ice.ns.t SELECT 4, named_struct('v', missing, 'w', NULL), 0",
    )
    .await
    .expect_err("an unplannable value");
    assert!(untyped.contains(HINT), "{untyped}");
    let untyped = attempt(
        &session,
        "UPDATE ice.ns.t SET st = named_struct('v', missing, 'w', NULL) WHERE id = 1",
    )
    .await
    .expect_err("an unplannable assignment");
    assert!(untyped.contains(HINT), "{untyped}");
    run(
        &session,
        "CREATE TABLE ice.ns.p (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT) \
         USING iceberg PARTITIONED BY (k) TBLPROPERTIES ('format-version'='2')",
    )
    .await;
    let partitioned = attempt(
        &session,
        "INSERT INTO ice.ns.p PARTITION (k = 0) SELECT id, named_struct('v', x, 'n', 1) \
         FROM ice.ns.src",
    )
    .await
    .expect_err("a static partition clause reads every source column");
    assert!(partitioned.contains(HINT), "{partitioned}");
    run(
        &session,
        "INSERT INTO ice.ns.p PARTITION (k = 0) SELECT id, named_struct('v', w, 'n', 1) \
         FROM ice.ns.src",
    )
    .await;
}
