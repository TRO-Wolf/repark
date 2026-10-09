use std::sync::Arc;

use datafusion::arrow::array::{AsArray, TimestampNanosecondArray};
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
