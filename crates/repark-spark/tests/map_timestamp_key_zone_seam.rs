use std::sync::Arc;

use datafusion::arrow::array::temporal_conversions::timestamp_us_to_datetime;
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{Int64Type, TimestampMicrosecondType};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const NEW_YORK: &str = "America/New_York";
const KOLKATA: &str = "Asia/Kolkata";
const DATE_KEYS: [(&str, i64); 2] = [
    (NEW_YORK, 1_767_330_000_000_000),
    (KOLKATA, 1_767_292_200_000_000),
];
const WALL_KEYS: [(&str, i64); 2] = [
    (NEW_YORK, 1_767_341_045_123_456),
    (KOLKATA, 1_767_303_245_123_456),
];
const NTZ_KEYS: [(&str, &str, i64); 2] = [
    (
        NEW_YORK,
        "2026-01-01 22:04:05.123456",
        1_767_305_045_123_456,
    ),
    (KOLKATA, "2026-01-02 08:34:05.123456", 1_767_342_845_123_456),
];
const TWO_DATE_KEYS: [(&str, i64, i64); 2] = [
    (NEW_YORK, 1_767_330_000_000_000, 1_767_416_400_000_000),
    (KOLKATA, 1_767_292_200_000_000, 1_767_378_600_000_000),
];

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
        "CREATE TABLE ice.ns.src (id INT, c_date DATE) USING iceberg \
         TBLPROPERTIES ('format-version'='2')"
            .to_string(),
        "INSERT INTO ice.ns.src VALUES (1, DATE '2026-01-02')".to_string(),
        "CREATE TABLE ice.ns.t (id INT, v MAP<TIMESTAMP, STRING>, k INT) USING iceberg \
         TBLPROPERTIES ('format-version'='2')"
            .to_string(),
    ] {
        run(session, &setup).await;
    }
    warehouse
}

async fn tick_lists(session: &ReparkSession, sql: &str) -> Vec<Vec<Option<i64>>> {
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
            let lists = batch.column(0).as_list::<i32>();
            (0..lists.len())
                .map(|row| {
                    if lists.is_null(row) {
                        return Vec::<Option<i64>>::new();
                    }
                    let values = lists.value(row);
                    let ticks = values.as_primitive::<Int64Type>();
                    (0..ticks.len())
                        .map(|index| ticks.is_valid(index).then(|| ticks.value(index)))
                        .collect()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn wall_lists(session: &ReparkSession, sql: &str) -> Vec<Vec<(i64, String)>> {
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
            let lists = batch.column(0).as_list::<i32>();
            (0..lists.len())
                .map(|row| {
                    if lists.is_null(row) {
                        return Vec::new();
                    }
                    let values = lists.value(row);
                    let walls = values.as_primitive::<TimestampMicrosecondType>();
                    (0..walls.len())
                        .filter_map(|index| {
                            if walls.is_null(index) {
                                return None;
                            }
                            let micros = walls.value(index);
                            let text = timestamp_us_to_datetime(micros)
                                .expect("a session wall is in range")
                                .format("%Y-%m-%d %H:%M:%S%.6f")
                                .to_string();
                            Some((micros, text))
                        })
                        .collect()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

const KEY_WRITES: [(&str, &str, &str); 2] = [
    ("a date literal", "map(DATE '2026-01-02', 'x')", ""),
    ("a date column", "map(c_date, 'x')", " FROM ice.ns.src"),
];

#[tokio::test]
async fn map_date_keys_store_the_session_midnight_through_insert_by_name() {
    let mut wrong = Vec::new();
    for (zone, expected) in DATE_KEYS {
        for (source, value, from) in KEY_WRITES {
            let session = session(zone);
            let _warehouse = catalog(&session).await;
            let write =
                format!("INSERT INTO ice.ns.t BY NAME SELECT 0 AS k, {value} AS v, 1 AS id{from}");
            if let Err(error) = attempt(&session, &write).await {
                wrong.push(format!("{zone} / {source}: {error}"));
                continue;
            }
            let keys = tick_lists(
                &session,
                "SELECT transform(map_keys(v), x -> unix_micros(x)) AS k FROM ice.ns.t ORDER BY \
                 id",
            )
            .await;
            if keys != vec![vec![Some(expected)]] {
                wrong.push(format!("{zone} / {source}: {keys:?}"));
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

#[tokio::test]
async fn map_key_casts_on_the_read_path_answer_spark_values() {
    let mut wrong = Vec::new();
    for (zone, expected) in DATE_KEYS {
        let session = session(zone);
        let _warehouse = catalog(&session).await;
        let keys = tick_lists(
            &session,
            "SELECT transform(map_keys(CAST(map(DATE '2026-01-02', 'x') AS MAP<TIMESTAMP, \
             STRING>)), x -> unix_micros(x))",
        )
        .await;
        if keys != vec![vec![Some(expected)]] {
            wrong.push(format!("{zone} date key: {keys:?}"));
        }
    }
    for (zone, expected) in WALL_KEYS {
        let session = session(zone);
        let _warehouse = catalog(&session).await;
        let keys = tick_lists(
            &session,
            "SELECT transform(map_keys(CAST(map(TIMESTAMP_NTZ '2026-01-02 03:04:05.123456', 'x') \
             AS MAP<TIMESTAMP, STRING>)), x -> unix_micros(x))",
        )
        .await;
        if keys != vec![vec![Some(expected)]] {
            wrong.push(format!("{zone} wall key: {keys:?}"));
        }
    }
    for (zone, text, micros) in NTZ_KEYS {
        let session = session(zone);
        let _warehouse = catalog(&session).await;
        let keys = wall_lists(
            &session,
            "SELECT map_keys(CAST(map(TIMESTAMP '2026-01-02 03:04:05.123456+00:00', 'x') AS \
             MAP<TIMESTAMP_NTZ, STRING>))",
        )
        .await;
        if keys != vec![vec![(micros, text.to_string())]] {
            wrong.push(format!("{zone} instant key: {keys:?}"));
        }
    }
    for (zone, first, second) in TWO_DATE_KEYS {
        let session = session(zone);
        let _warehouse = catalog(&session).await;
        let keys = tick_lists(
            &session,
            "SELECT transform(map_keys(CAST(map(DATE '2026-01-02', 'x', DATE '2026-01-03', 'y') \
             AS MAP<TIMESTAMP, STRING>)), x -> unix_micros(x))",
        )
        .await;
        let mut ticks: Vec<i64> = keys.into_iter().flatten().flatten().collect();
        ticks.sort_unstable();
        if ticks != vec![first, second] {
            wrong.push(format!("{zone} two date keys: {ticks:?}"));
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}
