use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const INSTANT: &str = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'";
const WALL: &str = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'";
const TEXT: &str = "'2026-01-02 03:04:05.123456'";
const UTC_WALL: i64 = 1_767_323_045_123_456;
const MIDNIGHT: i64 = 1_767_312_000_000_000;
const ZONES: [(&str, i64); 2] = [
    ("America/New_York", 1_767_305_045_123_456),
    ("Asia/Kolkata", 1_767_342_845_123_456),
];
const S: &str = "STRUCT<v: TIMESTAMP_NTZ, n: INT>";
const HINT: &str = "A value that is not provably a TIMESTAMP_NTZ wall time is not written into a \
                    nested TIMESTAMP_NTZ field while the session time zone is not UTC";

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
    run(session, "CREATE NAMESPACE ice.ns").await;
    for setup in [
        "CREATE TABLE ice.ns.src (id INT, x TIMESTAMP, w TIMESTAMP_NTZ) USING iceberg \
         TBLPROPERTIES ('format-version'='2')"
            .to_string(),
        format!("INSERT INTO ice.ns.src VALUES (1, {INSTANT}, {WALL})"),
        "CREATE TABLE ice.ns.z (id INT, st STRUCT<v: TIMESTAMP, n: INT>) USING iceberg \
         TBLPROPERTIES ('format-version'='2')"
            .to_string(),
        "INSERT INTO ice.ns.z SELECT id, named_struct('v', x, 'n', 1) FROM ice.ns.src".to_string(),
    ] {
        run(session, &setup).await;
    }
    warehouse
}

async fn table(session: &ReparkSession, name: &str, column_type: &str, extra: &str) {
    run(
        session,
        &format!(
            "CREATE TABLE ice.ns.{name} (id INT, st {column_type}, k INT) USING iceberg \
             {extra}TBLPROPERTIES ('format-version'='2')"
        ),
    )
    .await;
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

async fn snapshots(session: &ReparkSession, name: &str) -> Vec<Option<i64>> {
    stored(
        session,
        &format!("SELECT count(*) FROM ice.ns.`{name}$snapshots`"),
    )
    .await
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

async fn container_tables(session: &ReparkSession) {
    table(session, "t", S, "").await;
    run(session, "INSERT INTO ice.ns.t (id, k) VALUES (1, 0)").await;
    run(session, "ALTER TABLE ice.ns.t CREATE BRANCH bx").await;
    table(session, "p", S, "PARTITIONED BY (k) ").await;
    table(session, "a", "ARRAY<TIMESTAMP_NTZ>", "").await;
    run(
        session,
        &format!(
            "CREATE TEMPORARY VIEW cast_view AS \
             SELECT id, CAST(named_struct('v', x, 'n', 1) AS {S}) AS st, 0 AS k FROM ice.ns.src"
        ),
    )
    .await;
    run(
        session,
        &format!(
            "CREATE VIEW ice.ns.cast_kept AS \
             SELECT id, CAST(named_struct('v', x, 'n', 1) AS {S}) AS st, 0 AS k FROM ice.ns.src"
        ),
    )
    .await;
    let frame = session
        .sql(&format!(
            "SELECT id, CAST(named_struct('v', x, 'n', 1) AS {S}) AS st, 0 AS k FROM ice.ns.src"
        ))
        .await
        .expect("frame");
    session
        .create_or_replace_temp_view_from("cast_frame", &frame)
        .expect("frame view");
}

const CONTAINER_CASTS: [(&str, &str, &str); 14] = [
    (
        "insert select",
        "t",
        "INSERT INTO ice.ns.t SELECT id + 1, CAST(named_struct('v', x, 'n', 1) AS {S}), 0 \
         FROM ice.ns.src",
    ),
    (
        "insert values",
        "t",
        "INSERT INTO ice.ns.t VALUES (2, CAST(named_struct('v', {I}, 'n', 1) AS {S}), 0)",
    ),
    (
        "insert select, try_cast",
        "t",
        "INSERT INTO ice.ns.t SELECT id + 1, TRY_CAST(named_struct('v', x, 'n', 1) AS {S}), 0 \
         FROM ice.ns.src",
    ),
    (
        "insert select of a zoned struct column",
        "t",
        "INSERT INTO ice.ns.t SELECT id + 1, CAST(st AS {S}), 0 FROM ice.ns.z",
    ),
    (
        "merge insert values",
        "t",
        "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id + 5 WHEN NOT MATCHED THEN \
         INSERT (id, st, k) VALUES (s.id + 5, CAST(named_struct('v', s.x, 'n', 1) AS {S}), 0)",
    ),
    (
        "merge update set",
        "t",
        "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN \
         UPDATE SET t.st = CAST(named_struct('v', s.x, 'n', 1) AS {S})",
    ),
    (
        "merge update star",
        "t",
        "MERGE INTO ice.ns.t t USING (SELECT id, CAST(named_struct('v', x, 'n', 1) AS {S}) AS st, \
         0 AS k FROM ice.ns.src) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *",
    ),
    (
        "plain update",
        "t",
        "UPDATE ice.ns.t SET st = CAST(named_struct('v', {I}, 'n', 1) AS {S}) WHERE id >= 0",
    ),
    (
        "insert with a static partition",
        "p",
        "INSERT INTO ice.ns.p PARTITION (k = 0) SELECT id, CAST(named_struct('v', x, 'n', 1) \
         AS {S}) FROM ice.ns.src",
    ),
    (
        "insert into a branch",
        "t",
        "INSERT INTO ice.ns.t.branch_bx SELECT id + 1, CAST(named_struct('v', x, 'n', 1) AS {S}), \
         0 FROM ice.ns.src",
    ),
    (
        "insert from a temporary view",
        "t",
        "INSERT INTO ice.ns.t SELECT id + 1, st, k FROM cast_view",
    ),
    (
        "insert from a catalog view",
        "t",
        "INSERT INTO ice.ns.t SELECT id + 1, st, k FROM ice.ns.cast_kept",
    ),
    (
        "replace where from a frame",
        "t",
        "INSERT INTO ice.ns.t REPLACE WHERE id >= 0 SELECT id, st, k FROM cast_frame",
    ),
    (
        "array",
        "a",
        "INSERT INTO ice.ns.a SELECT id, CAST(array(x) AS ARRAY<TIMESTAMP_NTZ>), 0 \
         FROM ice.ns.src",
    ),
];

fn spelled(write: &str) -> String {
    write.replace("{S}", S).replace("{I}", INSTANT)
}

/// A `CAST` or `TRY_CAST` of a whole struct or array that turns a zoned leaf into a
/// naive one refuses outside UTC on every door, writes no file and adds no snapshot. The
/// planner's container cast keeps the UTC wall, so the planned type alone read as naive.
///
/// pins: ice-ntz-nested-refuse-1/C-015
#[tokio::test]
async fn a_container_cast_of_an_instant_refuses_on_every_door_outside_utc() {
    let mut wrong = Vec::new();
    for (zone, _) in ZONES {
        let session = session(zone);
        let warehouse = catalog(&session).await;
        container_tables(&session).await;
        let before = files_under(warehouse.path());
        for (door, _, write) in CONTAINER_CASTS {
            let write = spelled(write);
            match attempt(&session, &write).await {
                Err(error) if error.contains(HINT) => {}
                outcome => wrong.push(format!("{zone} / {door}: {outcome:?}")),
            }
        }
        if files_under(warehouse.path()) != before {
            wrong.push(format!("{zone}: a file was written"));
        }
        for (name, count) in [("t", 1), ("p", 0), ("a", 0)] {
            if snapshots(&session, name).await != vec![Some(count)] {
                wrong.push(format!("{zone} / {name}: a snapshot was added"));
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

/// In a UTC session the same container casts store the UTC wall, which is the session wall.
///
/// pins: ice-ntz-nested-refuse-1/C-015
#[tokio::test]
async fn a_container_cast_of_an_instant_stores_the_wall_in_a_utc_session() {
    let session = session("UTC");
    let _warehouse = catalog(&session).await;
    container_tables(&session).await;
    let mut wrong = Vec::new();
    for (door, name, write) in CONTAINER_CASTS {
        let write = spelled(write);
        if let Err(error) = attempt(&session, &write).await {
            wrong.push(format!("{door}: {error}"));
            continue;
        }
        let values: Vec<i64> = stored(&session, &format!("SELECT st FROM ice.ns.{name}"))
            .await
            .into_iter()
            .flatten()
            .collect();
        if values.is_empty() || values.iter().any(|value| *value != UTC_WALL) {
            wrong.push(format!("{door}: {values:?}"));
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

/// The leaf cast the hint names stores the session wall outside UTC: inside a struct, in an
/// array, through `transform`, in a `MERGE` value. A container cast whose input holds no zoned
/// leaf (a wall, a string) stores its wall.
///
/// pins: ice-ntz-nested-refuse-1/C-015
#[tokio::test]
async fn the_leaf_cast_and_a_container_cast_of_a_wall_still_store_outside_utc() {
    let mut wrong = Vec::new();
    for (zone, session_wall) in ZONES {
        let session = session(zone);
        let _warehouse = catalog(&session).await;
        let cases = [
            (
                S,
                "INSERT INTO {t} SELECT id, named_struct('v', CAST(x AS TIMESTAMP_NTZ), 'n', 1), 0 \
                 FROM ice.ns.src"
                    .to_string(),
                session_wall,
            ),
            (
                "ARRAY<TIMESTAMP_NTZ>",
                "INSERT INTO {t} SELECT id, array(CAST(x AS TIMESTAMP_NTZ)), 0 FROM ice.ns.src"
                    .to_string(),
                session_wall,
            ),
            (
                "ARRAY<TIMESTAMP_NTZ>",
                "INSERT INTO {t} SELECT id, transform(array(x), e -> CAST(e AS TIMESTAMP_NTZ)), 0 \
                 FROM ice.ns.src"
                    .to_string(),
                session_wall,
            ),
            (
                S,
                "MERGE INTO {t} t USING ice.ns.src s ON t.id = s.id WHEN NOT MATCHED THEN \
                 INSERT (id, st, k) VALUES (s.id, named_struct('v', CAST(s.x AS TIMESTAMP_NTZ), \
                 'n', 1), 0)"
                    .to_string(),
                session_wall,
            ),
            (
                S,
                format!(
                    "INSERT INTO {{t}} SELECT id, CAST(named_struct('v', w, 'n', 1) AS {S}), 0 \
                     FROM ice.ns.src"
                ),
                UTC_WALL,
            ),
            (
                S,
                format!(
                    "INSERT INTO {{t}} SELECT 1, CAST(named_struct('v', {TEXT}, 'n', 1) AS {S}), 0"
                ),
                UTC_WALL,
            ),
            (
                "ARRAY<TIMESTAMP_NTZ>",
                "INSERT INTO {t} SELECT id, CAST(array(w) AS ARRAY<TIMESTAMP_NTZ>), 0 \
                 FROM ice.ns.src"
                    .to_string(),
                UTC_WALL,
            ),
        ];
        for (index, (column_type, write, expected)) in cases.iter().enumerate() {
            let name = format!("l{index}");
            table(&session, &name, column_type, "").await;
            let write = write.replace("{t}", &format!("ice.ns.{name}"));
            if let Err(error) = attempt(&session, &write).await {
                wrong.push(format!("{zone} / {write}: {error}"));
                continue;
            }
            let values = stored(&session, &format!("SELECT st FROM ice.ns.{name}")).await;
            if values != vec![Some(*expected)] {
                wrong.push(format!("{zone} / {write}: {values:?}"));
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

const PREPARED: [(&str, &str); 6] = [
    (
        "typed TIMESTAMP, select",
        "PREPARE p{n}(TIMESTAMP) AS INSERT INTO ice.ns.t SELECT 5, named_struct('v', $1, 'n', 1), 0",
    ),
    (
        "untyped, select",
        "PREPARE p{n} AS INSERT INTO ice.ns.t SELECT 5, named_struct('v', $1, 'n', 1), 0",
    ),
    (
        "untyped, values",
        "PREPARE p{n} AS INSERT INTO ice.ns.t VALUES (5, named_struct('v', $1, 'n', 1), 0)",
    ),
    (
        "typed TIMESTAMP_NTZ, select",
        "PREPARE p{n}(TIMESTAMP_NTZ) AS INSERT INTO ice.ns.t SELECT 5, \
         named_struct('v', $1, 'n', 1), 0",
    ),
    (
        "typed STRING through the leaf cast",
        "PREPARE p{n}(STRING) AS INSERT INTO ice.ns.t SELECT 5, \
         named_struct('v', CAST($1 AS TIMESTAMP_NTZ), 'n', 1), 0",
    ),
    (
        "typed INT outside the column",
        "PREPARE p{n}(INT) AS INSERT INTO ice.ns.t SELECT id + $1, \
         named_struct('v', w, 'n', 1), 0 FROM ice.ns.src",
    ),
];

/// A `PREPARE` that holds a parameter and supplies the column refuses in every zone, typed or
/// untyped, in a `SELECT` or a `VALUES` source, wherever the parameter sits: `EXECUTE` binds
/// the argument after the decision and casts an instant to the declared type through its UTC
/// wall (a `STRING`, a `TIMESTAMP_NTZ`). A `PREPARE` with no parameter is decided as the write
/// it runs, and one that does not supply the column prepares; both execute.
///
/// pins: ice-ntz-nested-refuse-1/C-016
#[tokio::test]
async fn a_prepared_write_with_a_parameter_that_supplies_the_column_refuses_in_every_zone() {
    let mut wrong = Vec::new();
    for zone in ["UTC", "America/New_York"] {
        let session = session(zone);
        let _warehouse = catalog(&session).await;
        table(&session, "t", S, "").await;
        for (index, (form, prepare)) in PREPARED.iter().enumerate() {
            let prepare = prepare.replace("{n}", &index.to_string());
            match attempt(&session, &prepare).await {
                Err(error) if error.contains(HINT) => {}
                outcome => wrong.push(format!("{zone} / {form}: {outcome:?}")),
            }
        }
        if snapshots(&session, "t").await != vec![Some(0)] {
            wrong.push(format!("{zone}: a snapshot was added"));
        }
        for prepare in [
            "PREPARE keyed(INT) AS INSERT INTO ice.ns.t (id, k) VALUES ($1, 0)",
            "PREPARE fixed AS INSERT INTO ice.ns.t \
             SELECT 7, named_struct('v', CAST(x AS TIMESTAMP_NTZ), 'n', 1), 0 FROM ice.ns.src",
            "PREPARE walled AS INSERT INTO ice.ns.t \
             SELECT 8, named_struct('v', w, 'n', 1), 0 FROM ice.ns.src",
            "EXECUTE keyed(10)",
            "EXECUTE fixed",
            "EXECUTE walled",
        ] {
            if let Err(error) = attempt(&session, prepare).await {
                wrong.push(format!("{zone} / {prepare}: {error}"));
            }
        }
        let walls = stored(
            &session,
            "SELECT st FROM ice.ns.t WHERE st IS NOT NULL ORDER BY id",
        )
        .await;
        let session_wall = if zone == "UTC" { UTC_WALL } else { ZONES[0].1 };
        if walls != vec![Some(session_wall), Some(UTC_WALL)] {
            wrong.push(format!("{zone}: {walls:?}"));
        }
        if snapshots(&session, "t").await != vec![Some(3)] {
            wrong.push(format!("{zone}: the three executions did not commit"));
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

/// The plain `UPDATE` that assigns a whole struct or array adds the session offset to every
/// `TIMESTAMP_NTZ` cast in the value, whatever its input, so outside UTC such a value refuses
/// there (fail closed). A literal, `to_timestamp_ntz`, the field assignment and the `MERGE`
/// door store the wall; a UTC session stores it on every door.
///
/// pins: ice-ntz-nested-refuse-1/C-017
#[tokio::test]
async fn the_plain_update_refuses_a_timestamp_ntz_cast_in_a_whole_struct_outside_utc() {
    let shifted = [
        (
            S,
            format!("named_struct('v', CAST({TEXT} AS TIMESTAMP_NTZ), 'n', 1)"),
        ),
        (
            S,
            format!("named_struct('v', TRY_CAST({TEXT} AS TIMESTAMP_NTZ), 'n', 1)"),
        ),
        (
            S,
            format!("named_struct('v', CAST({WALL} AS TIMESTAMP_NTZ), 'n', 1)"),
        ),
        (
            "ARRAY<TIMESTAMP_NTZ>",
            format!("array(CAST({TEXT} AS TIMESTAMP_NTZ))"),
        ),
    ];
    let mut wrong = Vec::new();
    for (zone, _) in ZONES {
        let session = session(zone);
        let _warehouse = catalog(&session).await;
        for (index, (column_type, value)) in shifted.iter().enumerate() {
            let name = format!("u{index}");
            table(&session, &name, column_type, "").await;
            run(
                &session,
                &format!("INSERT INTO ice.ns.{name} (id, k) VALUES (1, 0)"),
            )
            .await;
            let write = format!("UPDATE ice.ns.{name} SET st = {value} WHERE id >= 0");
            match attempt(&session, &write).await {
                Err(error) if error.contains(HINT) => {}
                outcome => wrong.push(format!("{zone} / {write}: {outcome:?}")),
            }
            if snapshots(&session, &name).await != vec![Some(1)] {
                wrong.push(format!("{zone} / {name}: a snapshot was added"));
            }
        }
        let kept = [
            format!("UPDATE {{t}} SET st = named_struct('v', {WALL}, 'n', 1) WHERE id >= 0"),
            format!(
                "UPDATE {{t}} SET st = named_struct('v', to_timestamp_ntz({TEXT}), 'n', 1) \
                 WHERE id >= 0"
            ),
            format!("UPDATE {{t}} SET st.v = CAST({TEXT} AS TIMESTAMP_NTZ) WHERE id >= 0"),
            format!(
                "MERGE INTO {{t}} t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN \
                 UPDATE SET t.st = named_struct('v', CAST({TEXT} AS TIMESTAMP_NTZ), 'n', 1)"
            ),
        ];
        for (index, write) in kept.iter().enumerate() {
            let name = format!("k{index}");
            table(&session, &name, S, "").await;
            run(
                &session,
                &format!("INSERT INTO ice.ns.{name} (id, k) VALUES (1, 0)"),
            )
            .await;
            run(&session, &write.replace("{t}", &format!("ice.ns.{name}"))).await;
            let values = stored(&session, &format!("SELECT st FROM ice.ns.{name}")).await;
            if values != vec![Some(UTC_WALL)] {
                wrong.push(format!("{zone} / {write}: {values:?}"));
            }
        }
    }
    let session = session("UTC");
    let _warehouse = catalog(&session).await;
    for (index, (column_type, value)) in shifted.iter().enumerate() {
        let name = format!("u{index}");
        table(&session, &name, column_type, "").await;
        run(
            &session,
            &format!("INSERT INTO ice.ns.{name} (id, k) VALUES (1, 0)"),
        )
        .await;
        run(
            &session,
            &format!("UPDATE ice.ns.{name} SET st = {value} WHERE id >= 0"),
        )
        .await;
        let values = stored(&session, &format!("SELECT st FROM ice.ns.{name}")).await;
        if values != vec![Some(UTC_WALL)] {
            wrong.push(format!("UTC / {value}: {values:?}"));
        }
    }
    let dated = "UPDATE ice.ns.u0 SET st = named_struct('v', CAST(DATE '2026-01-02' AS \
                 TIMESTAMP_NTZ), 'n', 1) WHERE id >= 0";
    run(&session, dated).await;
    let values = stored(&session, "SELECT st FROM ice.ns.u0").await;
    if values != vec![Some(MIDNIGHT)] {
        wrong.push(format!("UTC / date: {values:?}"));
    }
    assert_eq!(wrong, Vec::<String>::new());
}

/// The plain `UPDATE` that assigns a whole struct stores a value whose zoned sub-expression is
/// not a `TIMESTAMP_NTZ` cast: `to_timestamp_ntz` of an instant stores the session wall, and a
/// zoned column that feeds only an `INT` field or a condition leaves the wall as written. Spark
/// 4.1.2 and main store the same values; fix round 1 refused all four.
///
/// pins: ice-ntz-nested-refuse-1/C-022
#[tokio::test]
async fn the_plain_update_stores_a_wall_beside_a_zoned_sub_expression() {
    let cases = [
        (
            format!("named_struct('v', to_timestamp_ntz({INSTANT}), 'n', 1)"),
            None,
        ),
        (
            format!("named_struct('v', to_timestamp_ntz(coalesce(z, {INSTANT})), 'n', 1)"),
            None,
        ),
        (
            format!("named_struct('v', {WALL}, 'n', IF(z IS NULL, 1, 2))"),
            Some(UTC_WALL),
        ),
        (
            format!("named_struct('v', st.v, 'n', CAST(year(coalesce(z, {INSTANT})) AS INT))"),
            Some(UTC_WALL),
        ),
    ];
    let mut wrong = Vec::new();
    for (zone, session_wall) in ZONES {
        let session = session(zone);
        let _warehouse = catalog(&session).await;
        for (index, (value, expected)) in cases.iter().enumerate() {
            let name = format!("w{index}");
            run(
                &session,
                &format!(
                    "CREATE TABLE ice.ns.{name} (id INT, st {S}, k INT, z TIMESTAMP) USING \
                     iceberg TBLPROPERTIES ('format-version'='2')"
                ),
            )
            .await;
            run(
                &session,
                &format!(
                    "INSERT INTO ice.ns.{name} (id, st, k) VALUES (1, named_struct('v', {WALL}, \
                     'n', 0), 0)"
                ),
            )
            .await;
            let write = format!("UPDATE ice.ns.{name} SET st = {value} WHERE id >= 0");
            if let Err(error) = attempt(&session, &write).await {
                wrong.push(format!("{zone} / {write}: {error}"));
                continue;
            }
            let values = stored(&session, &format!("SELECT st FROM ice.ns.{name}")).await;
            if values != vec![Some(expected.unwrap_or(session_wall))] {
                wrong.push(format!("{zone} / {write}: {values:?}"));
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

fn counted(calls: &Arc<AtomicUsize>) -> datafusion::logical_expr::ScalarUDF {
    let calls = Arc::clone(calls);
    create_udf(
        "counted",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |args: &[ColumnarValue]| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(args[0].clone())
        }),
    )
}

async fn calls_of(zone: &str, writes: &[String]) -> (usize, Vec<bool>, usize, usize) {
    let session = session(zone);
    let warehouse = catalog(&session).await;
    let calls = Arc::new(AtomicUsize::new(0));
    session.context().register_udf(counted(&calls));
    table(&session, "t", S, "").await;
    run(&session, "INSERT INTO ice.ns.t (id, k) VALUES (1, 0)").await;
    let state = session.context().state();
    let defaults = &state.config().options().catalog;
    let tables = |session: &ReparkSession| {
        session
            .context()
            .catalog(&defaults.default_catalog)
            .and_then(|catalog| catalog.schema(&defaults.default_schema))
            .map(|schema| schema.table_names())
            .unwrap_or_default()
    };
    let views = tables(&session);
    let files = files_under(warehouse.path());
    let mut outcomes = Vec::new();
    for write in writes {
        outcomes.push(attempt(&session, write).await.is_ok());
    }
    let added = files_under(warehouse.path()).saturating_sub(files);
    assert_eq!(tables(&session), views, "{zone}: a view was left");
    let count = stored(&session, "SELECT count(*) FROM ice.ns.`t$snapshots`").await;
    let count = usize::try_from(count[0].unwrap_or(0)).unwrap_or(0);
    (calls.load(Ordering::SeqCst), outcomes, count, added)
}

/// The type read plans its `SELECT` and never runs it: a volatile function in the source is
/// called as often in New York as in UTC, where the gate does not run; a refused write calls
/// it never, adds no snapshot, writes no file, and leaves no view.
///
/// pins: ice-ntz-nested-refuse-1/C-018
#[tokio::test]
async fn the_type_read_never_runs_the_source() {
    let allowed = [
        "INSERT INTO ice.ns.t SELECT counted(id) + 1, named_struct('v', w, 'n', 1), 0 \
         FROM ice.ns.src"
            .to_string(),
        format!(
            "UPDATE ice.ns.t SET st = named_struct('v', {WALL}, 'n', counted(id)) WHERE id >= 0"
        ),
        "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN \
         UPDATE SET t.st = named_struct('v', s.w, 'n', counted(s.id))"
            .to_string(),
    ];
    let (utc, outcomes, utc_snapshots, _) = calls_of("UTC", &allowed).await;
    assert_eq!(outcomes, vec![true; 3]);
    let (zoned, outcomes, zoned_snapshots, _) = calls_of("America/New_York", &allowed).await;
    assert_eq!(outcomes, vec![true; 3]);
    assert!(utc > 0);
    assert_eq!(zoned, utc);
    assert_eq!(zoned_snapshots, utc_snapshots);
    let refused = [
        "INSERT INTO ice.ns.t SELECT counted(id) + 1, named_struct('v', x, 'n', 1), 0 \
         FROM ice.ns.src"
            .to_string(),
        format!(
            "UPDATE ice.ns.t SET st = named_struct('v', {INSTANT}, 'n', counted(id)) WHERE id >= 0"
        ),
        "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN \
         UPDATE SET t.st = named_struct('v', s.x, 'n', counted(s.id))"
            .to_string(),
    ];
    let (calls, outcomes, snapshots, files) = calls_of("America/New_York", &refused).await;
    assert_eq!(outcomes, vec![false; 3]);
    assert_eq!((calls, snapshots, files), (0, 1, 0));
}
