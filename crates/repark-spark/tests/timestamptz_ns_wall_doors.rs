use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, AsArray, DictionaryArray, Int32Array, TimestampNanosecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const WALLS: [&str; 4] = [
    "2026-01-02 03:04:05.123456789",
    "1969-12-31 23:59:59.999999999",
    "2026-03-08 02:30:00.000000001",
    "2026-11-01 01:30:00.000000001",
];
const NEW_YORK_INSTANTS: [i64; 4] = [
    1_767_341_045_123_456_789,
    17_999_999_999_999,
    1_772_955_000_000_000_001,
    1_793_511_000_000_000_001,
];
const KOLKATA_INSTANTS: [i64; 4] = [
    1_767_303_245_123_456_789,
    -19_800_000_000_001,
    1_772_917_200_000_000_001,
    1_793_476_800_000_000_001,
];
const ZONES: [&str; 5] = [
    "UTC",
    "America/New_York",
    "Asia/Kolkata",
    "Asia/Kathmandu",
    "Australia/Lord_Howe",
];
const MERGE_ON_READ: &str = ", 'write.delete.mode'='merge-on-read', \
                             'write.update.mode'='merge-on-read', \
                             'write.merge.mode'='merge-on-read'";
const SOURCES: [(&str, &str); 4] = [
    ("w", "timestamp_ns"),
    ("n", "TIMESTAMP_NTZ"),
    ("d", "DATE"),
    ("l", "TIMESTAMP"),
];
const REFERENCE: &str = "INSERT INTO ice.ns.t SELECT id, {c}, {c} FROM ice.ns.src";
const DOORS: [(&str, bool, &str); 10] = [
    (
        "insert by name",
        false,
        "INSERT INTO ice.ns.t BY NAME SELECT {c} AS c, {c} AS v, id FROM ice.ns.src",
    ),
    (
        "insert overwrite",
        false,
        "INSERT OVERWRITE ice.ns.t SELECT id, {c}, {c} FROM ice.ns.src",
    ),
    (
        "replace where",
        true,
        "INSERT INTO ice.ns.t REPLACE WHERE id >= 0 SELECT id, {c}, {c} FROM ice.ns.src",
    ),
    (
        "merge insert",
        false,
        "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, v, c) VALUES (s.id, s.{c}, s.{c})",
    ),
    (
        "merge insert star",
        false,
        "MERGE INTO ice.ns.t t USING (SELECT id, {c} AS v, {c} AS c FROM ice.ns.src) s \
         ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
    ),
    (
        "merge update",
        true,
        "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET v = s.{c}",
    ),
    (
        "merge update star",
        true,
        "MERGE INTO ice.ns.t t USING (SELECT id, {c} AS v, {c} AS c FROM ice.ns.src) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET *",
    ),
    (
        "update where",
        true,
        "UPDATE ice.ns.t SET v = c WHERE id >= 0",
    ),
    ("update with no where", true, "UPDATE ice.ns.t SET v = c"),
    (
        "update of a case",
        true,
        "UPDATE ice.ns.t SET v = CASE WHEN id >= 0 THEN c ELSE c END WHERE id >= 0",
    ),
];

fn session(zone: &str, ansi: bool) -> ReparkSession {
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
    warehouse
}

async fn source(session: &ReparkSession) {
    run(
        session,
        "CREATE TABLE ice.ns.src (id INT, w timestamp_ns, n TIMESTAMP_NTZ, d DATE, l TIMESTAMP) \
         USING iceberg TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    let rows = WALLS
        .iter()
        .enumerate()
        .map(|(index, wall)| {
            let micros = &wall[..26];
            let day = &wall[..10];
            format!(
                "({index}, CAST('{wall}' AS timestamp_ns), to_timestamp_ntz('{micros}'), \
                 DATE '{day}', TIMESTAMP '{micros}+00:00')"
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    run(session, &format!("INSERT INTO ice.ns.src VALUES {rows}")).await;
}

async fn target(session: &ReparkSession, column: &str, properties: &str, seeded: bool) {
    run(session, "DROP TABLE IF EXISTS ice.ns.t").await;
    run(
        session,
        &format!(
            "CREATE TABLE ice.ns.t (id INT, v timestamptz_ns, c {column}) USING iceberg \
             TBLPROPERTIES ('format-version'='3'{properties})"
        ),
    )
    .await;
    if seeded {
        run(
            session,
            &REFERENCE
                .replace("SELECT id, {c}, {c}", "SELECT id, NULL, {c}")
                .replace("{c}", column_of(column)),
        )
        .await;
    }
}

fn column_of(column_type: &str) -> &'static str {
    SOURCES
        .iter()
        .find(|(_, sql_type)| *sql_type == column_type)
        .map_or("w", |(name, _)| name)
}

async fn stored(session: &ReparkSession) -> Vec<Option<i64>> {
    let batches = session
        .sql("SELECT v FROM ice.ns.t ORDER BY id")
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    batches
        .iter()
        .flat_map(|batch| {
            assert_eq!(
                batch.schema().field(0).data_type(),
                &DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
            );
            let ints = cast(batch.column(0), &DataType::Int64).expect("ticks as int64");
            ints.as_primitive::<Int64Type>().iter().collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn every_door_stores_the_instant_insert_stores_in_a_zoned_column() {
    let mut wrong = Vec::new();
    for zone in ZONES {
        let session = session(zone, true);
        let _warehouse = catalog(&session).await;
        source(&session).await;
        for (name, column) in SOURCES {
            target(&session, column, "", false).await;
            run(&session, &REFERENCE.replace("{c}", name)).await;
            let reference = stored(&session).await;
            let fixed = match (zone, name) {
                ("UTC", "w") => Some([
                    1_767_323_045_123_456_789,
                    -1,
                    1_772_937_000_000_000_001,
                    1_793_496_600_000_000_001,
                ]),
                ("America/New_York", "w") => Some(NEW_YORK_INSTANTS),
                ("Asia/Kolkata", "w") => Some(KOLKATA_INSTANTS),
                _ => None,
            };
            if let Some(fixed) = fixed {
                assert_eq!(reference, fixed.map(Some).to_vec(), "{zone} INSERT");
            }
            for properties in ["", MERGE_ON_READ] {
                for (door, seeded, write) in DOORS {
                    target(&session, column, properties, seeded).await;
                    let label =
                        format!("{zone} / {name} / {door} / mor={}", !properties.is_empty());
                    match attempt(&session, &write.replace("{c}", name)).await {
                        Ok(()) => {
                            let found = stored(&session).await;
                            if found != reference {
                                wrong.push(format!("{label}: {found:?} for {reference:?}"));
                            }
                        }
                        Err(error) => wrong.push(format!("{label}: {error}")),
                    }
                }
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

const PAST_THE_RANGE: [(&str, &str); 3] = [
    ("DATE", "DATE '3000-01-01'"),
    (
        "TIMESTAMP_NTZ",
        "TIMESTAMP_NTZ '2026-01-01 00:00:00.000001' + INTERVAL '974' YEAR",
    ),
    ("TIMESTAMP", "TIMESTAMP '3000-01-01 00:00:00.000001+00:00'"),
];

#[tokio::test]
async fn a_value_past_the_range_answers_as_insert_does_on_every_door() {
    let mut wrong = Vec::new();
    for ansi in [true, false] {
        let session = session("America/New_York", ansi);
        let _warehouse = catalog(&session).await;
        for (column, far) in PAST_THE_RANGE {
            run(&session, "DROP TABLE IF EXISTS ice.ns.src").await;
            run(
                &session,
                &format!(
                    "CREATE TABLE ice.ns.src (id INT, x {column}) USING iceberg \
                     TBLPROPERTIES ('format-version'='3')"
                ),
            )
            .await;
            run(
                &session,
                &format!("INSERT INTO ice.ns.src VALUES (0, {far})"),
            )
            .await;
            for required in ["", " NOT NULL"] {
                for (door, seeded, write) in DOORS {
                    run(&session, "DROP TABLE IF EXISTS ice.ns.t").await;
                    run(
                        &session,
                        &format!(
                            "CREATE TABLE ice.ns.t (id INT, v timestamptz_ns{required}, c {column}) \
                             USING iceberg TBLPROPERTIES ('format-version'='3')"
                        ),
                    )
                    .await;
                    if seeded {
                        run(
                            &session,
                            "INSERT INTO ice.ns.t SELECT id, \
                             CAST('2026-01-02 03:04:05+00:00' AS timestamptz_ns), x FROM ice.ns.src",
                        )
                        .await;
                    }
                    let outcome = attempt(&session, &write.replace("{c}", "x")).await;
                    let label = format!("ansi={ansi} {column}{required} {door}");
                    match (ansi, required.is_empty(), outcome) {
                        (true, _, Err(error)) => {
                            if !(error.contains("[CAST_OVERFLOW]")
                                && error.contains("\"TIMESTAMPTZ_NS\""))
                            {
                                wrong.push(format!("{label}: {error}"));
                            }
                        }
                        (false, true, Ok(())) => {
                            let found = stored(&session).await;
                            if found != vec![None] {
                                wrong.push(format!("{label}: {found:?}"));
                            }
                        }
                        (false, false, Err(error)) => {
                            if !(error.contains("non-nullable")
                                || error.contains("cannot assign NULL to required column"))
                            {
                                wrong.push(format!("{label}: {error}"));
                            }
                        }
                        (_, _, outcome) => wrong.push(format!("{label}: {outcome:?}")),
                    }
                }
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

const LEAF_DOORS: [(&str, &[&str]); 4] = [
    (
        "insert select",
        &["INSERT INTO ice.ns.t SELECT id, named_struct('v', w, 'k', 1) FROM ice.ns.src"],
    ),
    (
        "insert overwrite",
        &["INSERT OVERWRITE ice.ns.t SELECT id, named_struct('v', w, 'k', 1) FROM ice.ns.src"],
    ),
    (
        "merge set struct",
        &[
            "INSERT INTO ice.ns.t (id) SELECT id FROM ice.ns.src",
            "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.st = named_struct('v', s.w, 'k', 1)",
        ],
    ),
    (
        "merge set field",
        &[
            "INSERT INTO ice.ns.t SELECT id, named_struct('v', l, 'k', 1) FROM ice.ns.src",
            "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.st.v = s.w",
        ],
    ),
];

#[tokio::test]
async fn a_nested_zoned_leaf_keeps_the_one_reading_main_has_on_every_door() {
    let session = session("America/New_York", true);
    let _warehouse = catalog(&session).await;
    source(&session).await;
    for (door, statements) in LEAF_DOORS {
        run(&session, "DROP TABLE IF EXISTS ice.ns.t").await;
        run(
            &session,
            "CREATE TABLE ice.ns.t (id INT, st STRUCT<v: timestamptz_ns, k: INT>) USING iceberg \
             TBLPROPERTIES ('format-version'='3')",
        )
        .await;
        for statement in statements {
            run(&session, statement).await;
        }
        let batches = session
            .sql("SELECT st.v FROM ice.ns.t ORDER BY id")
            .await
            .expect("read")
            .collect()
            .await
            .expect("collect");
        let leaves: Vec<Option<i64>> = batches
            .iter()
            .flat_map(|batch| {
                let ints = cast(batch.column(0), &DataType::Int64).expect("ticks as int64");
                ints.as_primitive::<Int64Type>().iter().collect::<Vec<_>>()
            })
            .collect();
        let read_as_utc = vec![
            Some(1_767_323_045_123_456_789),
            Some(-1),
            Some(1_772_937_000_000_000_001),
            Some(1_793_496_600_000_000_001),
        ];
        assert_eq!(leaves, read_as_utc, "{door}");
    }
}

const WALL_TICKS: [i64; 4] = [
    1_767_323_045_123_456_789,
    -1,
    1_772_937_000_000_000_001,
    1_793_496_600_000_000_001,
];

const ENCODED_DOORS: [(&str, bool, &str); 4] = [
    (
        "insert overwrite",
        false,
        "INSERT OVERWRITE ice.ns.t SELECT id, v FROM runs",
    ),
    (
        "insert by name",
        false,
        "INSERT INTO ice.ns.t BY NAME SELECT v, id FROM runs",
    ),
    (
        "insert overwrite by name",
        false,
        "INSERT OVERWRITE ice.ns.t BY NAME SELECT v, id FROM runs",
    ),
    (
        "replace where",
        true,
        "INSERT INTO ice.ns.t REPLACE WHERE id >= 0 SELECT id, v FROM runs",
    ),
];

fn dictionary_walls() -> RecordBatch {
    let values = TimestampNanosecondArray::from(WALL_TICKS.to_vec());
    let keys = Int32Array::from(vec![0, 1, 2, 3]);
    let encoded: ArrayRef = Arc::new(DictionaryArray::new(keys, Arc::new(values)));
    let ids: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", encoded.data_type().clone(), false),
    ]);
    RecordBatch::try_new(Arc::new(schema), vec![ids, encoded]).expect("batch")
}

#[tokio::test]
async fn a_dictionary_encoded_wall_stores_inserts_instant_on_the_overwrite_doors() {
    let session = session("America/New_York", true);
    let _warehouse = catalog(&session).await;
    session
        .create_or_replace_temp_view("runs", vec![dictionary_walls()])
        .expect("view");
    run(
        &session,
        "CREATE TABLE ice.ns.t (id INT, v timestamptz_ns) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(&session, "INSERT INTO ice.ns.t SELECT id, v FROM runs").await;
    let reference = stored(&session).await;
    assert_eq!(reference, NEW_YORK_INSTANTS.map(Some).to_vec(), "INSERT");
    let mut wrong = Vec::new();
    for (door, seeded, write) in ENCODED_DOORS {
        run(&session, "DROP TABLE ice.ns.t").await;
        run(
            &session,
            "CREATE TABLE ice.ns.t (id INT, v timestamptz_ns) USING iceberg \
             TBLPROPERTIES ('format-version'='3')",
        )
        .await;
        if seeded {
            run(&session, "INSERT INTO ice.ns.t SELECT id, v FROM runs").await;
        }
        match attempt(&session, write).await {
            Ok(()) => {
                let found = stored(&session).await;
                if found != reference {
                    wrong.push(format!("{door}: {found:?} for {reference:?}"));
                }
            }
            Err(error) => wrong.push(format!("{door}: {error}")),
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}
