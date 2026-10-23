use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const WALL: &str = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'";
const INSTANT: &str = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'";
const LTZ: &str = "STRUCT<v: TIMESTAMP, n: INT>";
const NTZ: &str = "STRUCT<v: TIMESTAMP_NTZ, n: INT>";
const NEW_YORK_READ: i64 = 1_767_341_045_123_456;
const UTC_READ: i64 = 1_767_323_045_123_456;
const REFUSAL: &str = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible \
                       data for the table `ice`.`ns`.`t`: Cannot safely cast `st`.`v` to \
                       \"TIMESTAMP\". The session time zone is not UTC, and a nested \
                       TIMESTAMP_NTZ value cannot yet be stored into a nested TIMESTAMP field \
                       outside UTC: set spark.sql.session.timeZone to UTC for the statement, or \
                       cast the leaf to TIMESTAMP before nesting it, as in \
                       named_struct('<field>', CAST(<value> AS TIMESTAMP), ...). SQLSTATE: KD000";

fn session(zone: &str) -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
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
    for setup in [
        "CREATE NAMESPACE ice.ns".to_string(),
        "CREATE TABLE ice.ns.src (id INT, x TIMESTAMP, w TIMESTAMP_NTZ) USING iceberg \
         TBLPROPERTIES ('format-version'='2')"
            .to_string(),
        format!("INSERT INTO ice.ns.src VALUES (1, {INSTANT}, {WALL})"),
        format!(
            "CREATE TABLE ice.ns.t (id INT, st {LTZ}, k INT) USING iceberg \
             TBLPROPERTIES ('format-version'='2')"
        ),
        "INSERT INTO ice.ns.t (id, k) VALUES (1, 0)".to_string(),
    ] {
        run(session, &setup).await;
    }
    warehouse
}

fn leaf(array: &ArrayRef) -> Vec<Option<i64>> {
    match array.data_type() {
        DataType::Struct(_) => {
            let structs = array.as_struct();
            let ticks = cast(structs.column(0), &DataType::Int64).expect("ticks");
            let ticks = ticks.as_primitive::<Int64Type>();
            (0..structs.len())
                .map(|row| (!structs.is_null(row) && !ticks.is_null(row)).then(|| ticks.value(row)))
                .collect()
        }
        _ => Vec::new(),
    }
}

async fn stored(session: &ReparkSession) -> Vec<i64> {
    let batches = session
        .sql("SELECT st FROM ice.ns.t ORDER BY id")
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    batches
        .iter()
        .flat_map(|batch| leaf(batch.column(0)))
        .flatten()
        .collect()
}

const STORE_DOORS: [(&str, &str); 7] = [
    (
        "SQL INSERT BY NAME",
        "INSERT INTO ice.ns.t BY NAME SELECT 2 AS id, {V} AS st, 0 AS k FROM ice.ns.src",
    ),
    (
        "SQL INSERT OVERWRITE",
        "INSERT OVERWRITE ice.ns.t SELECT id, {V}, 0 FROM ice.ns.src",
    ),
    (
        "SQL INSERT OVERWRITE VALUES",
        "INSERT OVERWRITE ice.ns.t VALUES (1, {V}, 0)",
    ),
    (
        "MERGE INSERT",
        "MERGE INTO ice.ns.t t USING (SELECT 5 AS id, x, w FROM ice.ns.src) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, st, k) VALUES (s.id, {V}, 0)",
    ),
    (
        "MERGE UPDATE",
        "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN \
         UPDATE SET t.st = {V}",
    ),
    (
        "MERGE UPDATE SET *",
        "MERGE INTO ice.ns.t t USING (SELECT id, {V} AS st, 0 AS k FROM ice.ns.src) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET *",
    ),
    ("UPDATE", "UPDATE ice.ns.t SET st = {V} WHERE id >= 0"),
];

const NAIVE_VALUES: [(&str, &str); 2] = [
    (
        "a container cast of an instant",
        "CAST(named_struct('v', {INSTANT}, 'n', 1) AS {NTZ})",
    ),
    (
        "a TIMESTAMP_NTZ literal",
        "named_struct('v', {WALL}, 'n', 1)",
    ),
];

fn spelled(door: &str, value: &str) -> String {
    let value = value
        .replace("{NTZ}", NTZ)
        .replace("{WALL}", WALL)
        .replace("{INSTANT}", INSTANT);
    door.replace("{V}", &value).replace("{WALL}", WALL)
}

#[tokio::test]
async fn a_nested_wall_into_a_nested_timestamp_refuses_outside_utc_on_every_store_door() {
    let mut wrong = Vec::new();
    for (door, write) in STORE_DOORS {
        for (source, value) in NAIVE_VALUES {
            let session = session("America/New_York");
            let _warehouse = catalog(&session).await;
            let outcome = attempt(&session, &spelled(write, value)).await;
            if !outcome.as_ref().is_err_and(|error| error.contains(REFUSAL)) {
                wrong.push(format!("{door} / {source}: {outcome:?}"));
            }
            if stored(&session).await != Vec::<i64>::new() {
                wrong.push(format!("{door} / {source}: a value was written"));
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

#[tokio::test]
async fn a_utc_session_stores_the_wall_read_in_utc_on_every_store_door() {
    let mut wrong = Vec::new();
    for (door, write) in STORE_DOORS {
        for (source, value) in NAIVE_VALUES {
            let session = session("UTC");
            let _warehouse = catalog(&session).await;
            if let Err(error) = attempt(&session, &spelled(write, value)).await {
                wrong.push(format!("{door} / {source}: {error}"));
                continue;
            }
            let values = stored(&session).await;
            if values.is_empty() || values.iter().any(|value| *value != UTC_READ) {
                wrong.push(format!("{door} / {source}: {values:?}"));
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

const SEAM_DOORS: [(&str, &str); 4] = [
    (
        "SQL INSERT SELECT",
        "INSERT INTO ice.ns.t SELECT 2, {V}, 0 FROM ice.ns.src",
    ),
    (
        "SQL INSERT VALUES",
        "INSERT INTO ice.ns.t VALUES (2, {V}, 0)",
    ),
    (
        "MERGE UPDATE of the field",
        "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN \
         UPDATE SET t.st.v = {WALL}",
    ),
    (
        "SQL INSERT BY NAME of a leaf cast to TIMESTAMP",
        "INSERT INTO ice.ns.t BY NAME SELECT 2 AS id, named_struct('v', CAST(w AS TIMESTAMP), \
         'n', 1) AS st, 0 AS k FROM ice.ns.src",
    ),
];

#[tokio::test]
async fn doors_that_read_the_wall_in_the_session_zone_store_spark_instant_outside_utc() {
    let mut wrong = Vec::new();
    for (door, write) in SEAM_DOORS {
        let session = session("America/New_York");
        let _warehouse = catalog(&session).await;
        let write = spelled(write, "named_struct('v', {WALL}, 'n', 1)");
        if let Err(error) = attempt(&session, &write).await {
            wrong.push(format!("{door}: {error}"));
            continue;
        }
        let values = stored(&session).await;
        if values.is_empty() || values.iter().any(|value| *value != NEW_YORK_READ) {
            wrong.push(format!("{door}: {values:?}"));
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

const PARTITIONED_DOORS: [(&str, &str); 6] = [
    (
        "PARTITION (k = 0) SELECT of a literal",
        "INSERT OVERWRITE ice.ns.p PARTITION (k = 0) SELECT 1, {L}",
    ),
    (
        "PARTITION (k = 0) SELECT of a column",
        "INSERT OVERWRITE ice.ns.p PARTITION (k = 0) SELECT id, {C} FROM ice.ns.src",
    ),
    (
        "PARTITION (k = 0) VALUES",
        "INSERT OVERWRITE ice.ns.p PARTITION (k = 0) VALUES (1, {L})",
    ),
    (
        "PARTITION (k = 0) with a column list",
        "INSERT OVERWRITE ice.ns.p PARTITION (k = 0) (id, st) SELECT 1, {L}",
    ),
    (
        "TABLE keyword with PARTITION (k = 0)",
        "INSERT OVERWRITE TABLE ice.ns.p PARTITION (k = 0) SELECT 1, {L}",
    ),
    (
        "dynamic PARTITION (k)",
        "INSERT OVERWRITE ice.ns.p PARTITION (k) SELECT 1, {L}, 0",
    ),
];

const PARTITIONED_SOURCES: [(&str, &str, &str); 2] = [
    (
        "an instant struct cast to TIMESTAMP_NTZ",
        "CAST(named_struct('v', {INSTANT}, 'n', 1) AS {NTZ})",
        "CAST(named_struct('v', x, 'n', 1) AS {NTZ})",
    ),
    (
        "a TIMESTAMP_NTZ value",
        "named_struct('v', {WALL}, 'n', 1)",
        "named_struct('v', w, 'n', 1)",
    ),
];

fn data_files(root: &std::path::Path) -> usize {
    let mut pending = vec![root.to_path_buf()];
    let mut found = 0;
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                found += 1;
            }
        }
    }
    found
}

async fn partitioned(session: &ReparkSession) -> TempDir {
    let warehouse = catalog(session).await;
    for setup in [
        format!("CREATE TABLE ice.ns.p (id INT, st {LTZ}, k INT) USING iceberg PARTITIONED BY (k)"),
        format!("INSERT INTO ice.ns.p VALUES (1, CAST(NULL AS {LTZ}), 0)"),
    ] {
        run(session, &setup).await;
    }
    warehouse
}

async fn stored_in_p(session: &ReparkSession) -> Vec<i64> {
    let batches = session
        .sql("SELECT st FROM ice.ns.p ORDER BY id")
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    batches
        .iter()
        .flat_map(|batch| leaf(batch.column(0)))
        .flatten()
        .collect()
}

fn partitioned_write(door: &str, literal: &str, column: &str) -> String {
    let resolve = |value: &str| {
        value
            .replace("{NTZ}", NTZ)
            .replace("{WALL}", WALL)
            .replace("{INSTANT}", INSTANT)
    };
    door.replace("{L}", &resolve(literal))
        .replace("{C}", &resolve(column))
}

#[tokio::test]
async fn a_partitioned_overwrite_refuses_a_nested_wall_outside_utc_and_writes_no_file() {
    let mut wrong = Vec::new();
    for zone in ["America/New_York", "Australia/Sydney", "Asia/Kathmandu"] {
        for (door, write) in PARTITIONED_DOORS {
            for (source, literal, column) in PARTITIONED_SOURCES {
                let session = session(zone);
                let warehouse = partitioned(&session).await;
                let before = data_files(warehouse.path());
                let outcome = attempt(&session, &partitioned_write(write, literal, column)).await;
                let refusal = REFUSAL.replace("`ice`.`ns`.`t`", "`ice`.`ns`.`p`");
                if !outcome
                    .as_ref()
                    .is_err_and(|error| error.contains(&refusal))
                {
                    wrong.push(format!("{zone} {door} / {source}: {outcome:?}"));
                }
                if data_files(warehouse.path()) != before {
                    wrong.push(format!("{zone} {door} / {source}: a data file was written"));
                }
                if stored_in_p(&session).await != Vec::<i64>::new() {
                    wrong.push(format!("{zone} {door} / {source}: a value was written"));
                }
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

#[tokio::test]
async fn a_partitioned_overwrite_in_utc_stores_the_utc_read() {
    let mut wrong = Vec::new();
    for (door, write) in PARTITIONED_DOORS {
        for (source, literal, column) in PARTITIONED_SOURCES {
            let session = session("UTC");
            let _warehouse = partitioned(&session).await;
            let write = partitioned_write(write, literal, column);
            if let Err(error) = attempt(&session, &write).await {
                wrong.push(format!("{door} / {source}: {error}"));
                continue;
            }
            let values = stored_in_p(&session).await;
            if values != vec![UTC_READ] {
                wrong.push(format!("{door} / {source}: {values:?}"));
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

const POSITION_SHAPES: [(&str, &str, &str, &str, &str); 2] = [
    (
        "partition column first",
        "(k INT, id INT, st STRUCT<v: TIMESTAMP, n: INT>)",
        "(0, 1, CAST(NULL AS STRUCT<v: TIMESTAMP, n: INT>))",
        "SELECT 1, {L}",
        "SELECT 0, 1, {L}",
    ),
    (
        "partition column in the middle",
        "(id INT, k INT, st STRUCT<v: TIMESTAMP, n: INT>)",
        "(1, 0, CAST(NULL AS STRUCT<v: TIMESTAMP, n: INT>))",
        "SELECT 1, {L}",
        "SELECT 1, 0, {L}",
    ),
];

const POSITION_CLAUSES: [(&str, &str); 2] = [
    ("PARTITION (k = 0)", "static"),
    ("PARTITION (k)", "dynamic"),
];

async fn positioned(session: &ReparkSession, columns: &str, seed: &str) -> TempDir {
    let warehouse = catalog(session).await;
    for setup in [
        format!("CREATE TABLE ice.ns.p {columns} USING iceberg PARTITIONED BY (k)"),
        format!("INSERT INTO ice.ns.p VALUES {seed}"),
    ] {
        run(session, &setup).await;
    }
    warehouse
}

fn positioned_write(clause: &str, select: &str, literal: &str) -> String {
    let literal = literal
        .replace("{NTZ}", NTZ)
        .replace("{WALL}", WALL)
        .replace("{INSTANT}", INSTANT);
    format!(
        "INSERT OVERWRITE ice.ns.p {clause} {}",
        select.replace("{L}", &literal)
    )
}

#[tokio::test]
async fn a_partition_column_before_the_struct_still_refuses_outside_utc_and_writes_no_file() {
    let mut wrong = Vec::new();
    let refusal = REFUSAL.replace("`ice`.`ns`.`t`", "`ice`.`ns`.`p`");
    for zone in ["America/New_York", "Australia/Sydney"] {
        for (shape, columns, seed, static_select, dynamic_select) in POSITION_SHAPES {
            for (clause, kind) in POSITION_CLAUSES {
                let select = if kind == "static" {
                    static_select
                } else {
                    dynamic_select
                };
                for (source, literal, _) in PARTITIONED_SOURCES {
                    let session = session(zone);
                    let warehouse = positioned(&session, columns, seed).await;
                    let before = data_files(warehouse.path());
                    let outcome =
                        attempt(&session, &positioned_write(clause, select, literal)).await;
                    let label = format!("{zone} {shape} {kind} / {source}");
                    if !outcome
                        .as_ref()
                        .is_err_and(|error| error.contains(&refusal))
                    {
                        wrong.push(format!("{label}: {outcome:?}"));
                    }
                    if data_files(warehouse.path()) != before {
                        wrong.push(format!("{label}: a data file was written"));
                    }
                    if stored_in_p(&session).await != Vec::<i64>::new() {
                        wrong.push(format!("{label}: a value was written"));
                    }
                }
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

#[tokio::test]
async fn a_partition_column_before_the_struct_stores_the_utc_read_in_utc() {
    let mut wrong = Vec::new();
    for (shape, columns, seed, static_select, dynamic_select) in POSITION_SHAPES {
        for (clause, kind) in POSITION_CLAUSES {
            let select = if kind == "static" {
                static_select
            } else {
                dynamic_select
            };
            for (source, literal, _) in PARTITIONED_SOURCES {
                let session = session("UTC");
                let _warehouse = positioned(&session, columns, seed).await;
                let write = positioned_write(clause, select, literal);
                if let Err(error) = attempt(&session, &write).await {
                    wrong.push(format!("{shape} {kind} / {source}: {error}"));
                    continue;
                }
                let values = stored_in_p(&session).await;
                if values != vec![UTC_READ] {
                    wrong.push(format!("{shape} {kind} / {source}: {values:?}"));
                }
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

#[tokio::test]
async fn an_update_of_an_inner_struct_with_a_wall_refuses_outside_utc() {
    let session = session("America/New_York");
    let _warehouse = catalog(&session).await;
    let inner = "STRUCT<i: STRUCT<v: TIMESTAMP>>";
    run(
        &session,
        &format!(
            "CREATE TABLE ice.ns.q (id INT, st {inner}) USING iceberg \
             TBLPROPERTIES ('format-version'='2')"
        ),
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.q VALUES (1, named_struct('i', named_struct('v', \
         TIMESTAMP '2000-01-01 00:00:00+00:00')))",
    )
    .await;
    let outcome = attempt(
        &session,
        &format!("UPDATE ice.ns.q SET st.i = named_struct('v', {WALL}) WHERE id = 1"),
    )
    .await;
    let refused = "Cannot write incompatible data for the table `ice`.`ns`.`q`: Cannot safely \
                   cast `st`.`i`.`v` to \"TIMESTAMP\"";
    assert!(
        outcome.as_ref().is_err_and(|error| error.contains(refused)),
        "{outcome:?}"
    );
    let batches = session
        .sql("SELECT CAST(st.i.v AS BIGINT) FROM ice.ns.q")
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let kept = batches[0].column(0).as_primitive::<Int64Type>().value(0);
    assert_eq!(kept, 946_684_800);
}

#[tokio::test]
async fn a_plain_update_of_a_struct_with_a_cast_wall_text_stores_spark_instant() {
    let session = session("America/New_York");
    let _warehouse = catalog(&session).await;
    run(
        &session,
        "UPDATE ice.ns.t SET st = named_struct('v', \
         CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP_NTZ), 'n', 1) WHERE id = 1",
    )
    .await;
    assert_eq!(stored(&session).await, vec![NEW_YORK_READ]);
}

const SYDNEY_READ: i64 = 1_767_283_445_123_456;
const MIXED: &str = "STRUCT<v: TIMESTAMP, u: TIMESTAMP, n: INT>";

const UNSHIFTED_UPDATES: [(&str, &str, &str, &str); 3] = [
    (
        "an extract of a container cast beside the cast leaf",
        MIXED,
        "named_struct('v', CAST({INSTANT} AS TIMESTAMP_NTZ), 'u', \
         CAST(array({INSTANT}) AS ARRAY<TIMESTAMP_NTZ>)[0], 'n', 1)",
        "SELECT unix_micros(st.v), unix_micros(st.u) FROM ice.ns.m",
    ),
    (
        "an extract leaf with the cast only inside an INT field",
        LTZ,
        "named_struct('v', CAST(array({INSTANT}) AS ARRAY<TIMESTAMP_NTZ>)[0], 'n', \
         CASE WHEN CAST({INSTANT} AS TIMESTAMP_NTZ) IS NOT NULL THEN 1 ELSE 0 END)",
        "SELECT unix_micros(st.v) FROM ice.ns.m",
    ),
    (
        "a TIMESTAMP_NTZ literal beside the cast leaf",
        MIXED,
        "named_struct('v', CAST({INSTANT} AS TIMESTAMP_NTZ), 'u', {WALL}, 'n', 1)",
        "SELECT unix_micros(st.v), unix_micros(st.u) FROM ice.ns.m",
    ),
];

async fn mixed(session: &ReparkSession, struct_type: &str) -> TempDir {
    let warehouse = catalog(session).await;
    for setup in [
        format!(
            "CREATE TABLE ice.ns.m (id INT, st {struct_type}, k INT) USING iceberg \
             TBLPROPERTIES ('format-version'='2')"
        ),
        "INSERT INTO ice.ns.m (id, k) VALUES (1, 0)".to_string(),
    ] {
        run(session, &setup).await;
    }
    warehouse
}

async fn micros(session: &ReparkSession, sql: &str) -> Vec<Option<i64>> {
    let batches = session
        .sql(sql)
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    batches
        .iter()
        .flat_map(|batch| {
            batch.columns().iter().flat_map(|column| {
                let ticks = cast(column, &DataType::Int64).expect("ticks");
                ticks.as_primitive::<Int64Type>().iter().collect::<Vec<_>>()
            })
        })
        .collect()
}

fn resolved(value: &str) -> String {
    value.replace("{INSTANT}", INSTANT).replace("{WALL}", WALL)
}

#[tokio::test]
async fn a_plain_update_with_an_unshifted_wall_beside_the_shifted_cast_refuses_outside_utc() {
    let refusal = REFUSAL.replace("`ice`.`ns`.`t`", "`ice`.`ns`.`m`");
    let mut wrong = Vec::new();
    for zone in ["America/New_York", "Australia/Sydney"] {
        for (case, struct_type, value, read) in UNSHIFTED_UPDATES {
            let value = resolved(value);
            for (door, write) in [
                (
                    "UPDATE",
                    format!("UPDATE ice.ns.m SET st = {value} WHERE id >= 0"),
                ),
                (
                    "MERGE UPDATE",
                    format!(
                        "MERGE INTO ice.ns.m t USING ice.ns.src s ON t.id = s.id WHEN MATCHED \
                         THEN UPDATE SET t.st = {value}"
                    ),
                ),
            ] {
                let session = session(zone);
                let warehouse = mixed(&session, struct_type).await;
                let before = data_files(warehouse.path());
                let outcome = attempt(&session, &write).await;
                if !outcome
                    .as_ref()
                    .is_err_and(|error| error.contains(&refusal))
                {
                    wrong.push(format!("{zone} {door} {case}: {outcome:?}"));
                }
                if data_files(warehouse.path()) != before {
                    wrong.push(format!("{zone} {door} {case}: a data file was written"));
                }
                if micros(&session, read).await.iter().any(Option::is_some) {
                    wrong.push(format!("{zone} {door} {case}: a value was written"));
                }
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

#[tokio::test]
async fn a_plain_update_with_two_walls_in_utc_stores_the_utc_read() {
    let mut wrong = Vec::new();
    for (case, struct_type, value, read) in UNSHIFTED_UPDATES {
        let session = session("UTC");
        let _warehouse = mixed(&session, struct_type).await;
        let write = format!("UPDATE ice.ns.m SET st = {} WHERE id >= 0", resolved(value));
        if let Err(error) = attempt(&session, &write).await {
            wrong.push(format!("{case}: {error}"));
            continue;
        }
        let values = micros(&session, read).await;
        if values.is_empty() || values.iter().any(|value| *value != Some(UTC_READ)) {
            wrong.push(format!("{case}: {values:?}"));
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

#[tokio::test]
async fn a_plain_update_whose_only_wall_is_the_shifted_cast_stores_spark_instant() {
    let mut wrong = Vec::new();
    for (zone, text_read) in [
        ("America/New_York", NEW_YORK_READ),
        ("Australia/Sydney", SYDNEY_READ),
    ] {
        for (value, expected) in [
            (
                "named_struct('v', CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP_NTZ), 'n', 1)"
                    .to_string(),
                text_read,
            ),
            (
                format!("named_struct('v', CAST({INSTANT} AS TIMESTAMP_NTZ), 'n', 1)"),
                UTC_READ,
            ),
        ] {
            let session = session(zone);
            let _warehouse = mixed(&session, LTZ).await;
            let write = format!("UPDATE ice.ns.m SET st = {value} WHERE id >= 0");
            if let Err(error) = attempt(&session, &write).await {
                wrong.push(format!("{zone} {value}: {error}"));
                continue;
            }
            let values = micros(&session, "SELECT unix_micros(st.v) FROM ice.ns.m").await;
            if values != vec![Some(expected)] {
                wrong.push(format!("{zone} {value}: {values:?}"));
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

const TRY_CAST_UPDATES: [(&str, &str, &str, &str, &str); 4] = [
    (
        "TRY_CAST of wall text",
        LTZ,
        "named_struct('v', TRY_CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP_NTZ), 'n', 1)",
        "id = 1",
        "SELECT unix_micros(st.v) FROM ice.ns.m",
    ),
    (
        "TRY_CAST of bad text",
        LTZ,
        "named_struct('v', TRY_CAST('not a time' AS TIMESTAMP_NTZ), 'n', 1)",
        "id >= 0",
        "SELECT unix_micros(st.v) FROM ice.ns.m",
    ),
    (
        "TRY_CAST of an instant",
        LTZ,
        "named_struct('v', TRY_CAST({INSTANT} AS TIMESTAMP_NTZ), 'n', 1)",
        "id >= 0",
        "SELECT unix_micros(st.v) FROM ice.ns.m",
    ),
    (
        "CAST of an instant beside TRY_CAST of wall text",
        MIXED,
        "named_struct('v', CAST({INSTANT} AS TIMESTAMP_NTZ), 'u', TRY_CAST('2026-01-02 \
         03:04:05.123456' AS TIMESTAMP_NTZ), 'n', 1)",
        "id >= 0",
        "SELECT unix_micros(st.v), unix_micros(st.u) FROM ice.ns.m",
    ),
];

#[tokio::test]
async fn a_plain_update_whose_only_naive_leaf_is_a_try_cast_stores_spark_instant() {
    let mut wrong = Vec::new();
    for (case, struct_type, value, predicate, read) in TRY_CAST_UPDATES {
        let session = session("America/New_York");
        let _warehouse = mixed(&session, struct_type).await;
        let write = format!(
            "UPDATE ice.ns.m SET st = {} WHERE {predicate}",
            resolved(value)
        );
        if let Err(error) = attempt(&session, &write).await {
            wrong.push(format!("{case}: {error}"));
            continue;
        }
        let values = micros(&session, read).await;
        let expected: Vec<Option<i64>> = match case {
            "TRY_CAST of wall text" => vec![Some(NEW_YORK_READ)],
            "TRY_CAST of bad text" => vec![None],
            "TRY_CAST of an instant" => vec![Some(UTC_READ)],
            "CAST of an instant beside TRY_CAST of wall text" => {
                vec![Some(UTC_READ), Some(NEW_YORK_READ)]
            }
            _ => vec![],
        };
        if values != expected {
            wrong.push(format!("{case}: {values:?}"));
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}
