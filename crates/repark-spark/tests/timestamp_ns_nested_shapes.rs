use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const NANOS: &str = "2026-01-02 03:04:05.123456789";
const TICKS: i64 = 1_767_323_045_123_456_789;
const INSTANT: &str = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'";
const REFUSAL: &str = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible \
                       data for the table `ice`.`ns`.";
const NOT_YET: &str = "A nested timestamp_ns leaf is not writable yet";

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
        "CREATE TABLE ice.ns.src (id INT, x TIMESTAMP, ns timestamp_ns) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        session,
        &format!("INSERT INTO ice.ns.src VALUES (1, {INSTANT}, CAST('{NANOS}' AS timestamp_ns))"),
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

const SHAPES: [(&str, &str, &str, &str); 9] = [
    (
        "struct",
        "STRUCT<v: timestamp_ns, n: INT>",
        "named_struct('v', {x}, 'n', 1)",
        "`st`.`v`",
    ),
    (
        "struct in struct",
        "STRUCT<i: STRUCT<v: timestamp_ns, n: INT>, m: INT>",
        "named_struct('i', named_struct('v', {x}, 'n', 1), 'm', 2)",
        "`st`.`i`.`v`",
    ),
    (
        "array of struct",
        "ARRAY<STRUCT<v: timestamp_ns, n: INT>>",
        "array(named_struct('v', {x}, 'n', 1))",
        "`st`.`element`.`v`",
    ),
    (
        "array",
        "ARRAY<timestamp_ns>",
        "array({x})",
        "`st`.`element`",
    ),
    (
        "struct of array",
        "STRUCT<a: ARRAY<timestamp_ns>, n: INT>",
        "named_struct('a', array({x}), 'n', 1)",
        "`st`.`a`.`element`",
    ),
    (
        "map value",
        "MAP<STRING, timestamp_ns>",
        "map('k', {x})",
        "`st`.`value`",
    ),
    (
        "map key",
        "MAP<timestamp_ns, INT>",
        "map({x}, 1)",
        "`st`.`key`",
    ),
    (
        "map of struct",
        "MAP<STRING, STRUCT<v: timestamp_ns, n: INT>>",
        "map('k', named_struct('v', {x}, 'n', 1))",
        "`st`.`value`.`v`",
    ),
    (
        "struct, one field renamed",
        "STRUCT<a: timestamp_ns, b: TIMESTAMP>",
        "named_struct('q', {x}, 'b', {x})",
        "`st`.`a`",
    ),
];
const DOORS: [(&str, &str, &str); 17] = [
    (
        "insert values",
        "INSERT INTO {t} VALUES (2, {st}, 0)",
        "lit",
    ),
    (
        "insert select",
        "INSERT INTO {t} SELECT id, {st}, 0 FROM ice.ns.src",
        "ns",
    ),
    (
        "insert with a column list",
        "INSERT INTO {t} (id, st) SELECT id, {st} FROM ice.ns.src",
        "ns",
    ),
    (
        "insert by name",
        "INSERT INTO {t} BY NAME SELECT 0 AS k, {st} AS st, id FROM ice.ns.src",
        "ns",
    ),
    (
        "insert by name, star",
        "INSERT INTO {t} BY NAME SELECT * FROM (SELECT 0 AS k, {st} AS st, id FROM ice.ns.src) AS s",
        "ns",
    ),
    (
        "insert overwrite",
        "INSERT OVERWRITE {t} SELECT id, {st}, 0 FROM ice.ns.src",
        "ns",
    ),
    (
        "insert overwrite values",
        "INSERT OVERWRITE {t} VALUES (2, {st}, 0)",
        "lit",
    ),
    (
        "replace where",
        "INSERT INTO {t} REPLACE WHERE id >= 0 SELECT id, {st}, 0 FROM ice.ns.src",
        "ns",
    ),
    (
        "merge insert",
        "MERGE INTO {t} t USING (SELECT id + 5 AS id, {st} AS v FROM ice.ns.src) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, st, k) VALUES (s.id, s.v, 0)",
        "ns",
    ),
    (
        "merge insert star",
        "MERGE INTO {t} t USING (SELECT id + 5 AS id, {st} AS st, 0 AS k FROM ice.ns.src) s \
         ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
        "ns",
    ),
    (
        "merge update",
        "MERGE INTO {t} t USING (SELECT id, {st} AS v FROM ice.ns.src) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.st = s.v",
        "ns",
    ),
    (
        "merge update star",
        "MERGE INTO {t} t USING (SELECT id, {st} AS st, 0 AS k FROM ice.ns.src) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET *",
        "ns",
    ),
    (
        "update where",
        "UPDATE {t} SET st = {st} WHERE id >= 0",
        "lit",
    ),
    ("update with no where", "UPDATE {t} SET st = {st}", "lit"),
    (
        "insert select, an array element",
        "INSERT INTO {t} SELECT id, {st}, 0 FROM ice.ns.src",
        "array(ns, NULL)[0]",
    ),
    (
        "insert select from a temporary view",
        "INSERT INTO {t} SELECT id, {st}, 0 FROM narrowed",
        "x",
    ),
    (
        "insert select from a subquery",
        "INSERT INTO {t} SELECT id, v, 0 FROM (SELECT id, {st} AS v FROM ice.ns.src) AS s",
        "coalesce(ns, NULL)",
    ),
];

#[tokio::test]
async fn every_door_refuses_a_nested_nanosecond_leaf_and_writes_nothing() {
    let literal = format!("CAST('{NANOS}' AS timestamp_ns)");
    let mut wrong = Vec::new();
    for zone in ["America/New_York", "Asia/Kolkata"] {
        let session = session(zone);
        let warehouse = catalog(&session).await;
        run(
            &session,
            "CREATE TEMPORARY VIEW narrowed AS SELECT id, coalesce(ns, NULL) AS x FROM ice.ns.src",
        )
        .await;
        for (shape_index, (shape, column_type, build, path)) in SHAPES.iter().enumerate() {
            let table = format!("ice.ns.n{shape_index}");
            run(
                &session,
                &format!(
                    "CREATE TABLE {table} (id INT, st {column_type}, k INT) USING iceberg \
                     TBLPROPERTIES ('format-version'='3')"
                ),
            )
            .await;
            run(
                &session,
                &format!("INSERT INTO {table} (id, k) VALUES (1, 0)"),
            )
            .await;
            let before = files_under(warehouse.path());
            for (door, write, value) in DOORS {
                let value = match value {
                    "lit" => literal.as_str(),
                    other => other,
                };
                let write = write
                    .replace("{t}", &table)
                    .replace("{st}", &build.replace("{x}", value));
                let label = format!("{zone} / {shape} / {door}");
                match attempt(&session, &write).await {
                    Ok(()) => wrong.push(format!("{label}: stored")),
                    Err(error) => {
                        let named = error.contains(REFUSAL)
                            && error.contains(NOT_YET)
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

const FIELD_DOORS: [&str; 4] = [
    "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id \
     WHEN MATCHED THEN UPDATE SET t.st.v = s.ns",
    "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id \
     WHEN MATCHED THEN UPDATE SET st.v = s.x",
    "UPDATE ice.ns.t SET st.v = CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns) WHERE id = 1",
    "UPDATE ice.ns.t SET st.v = TIMESTAMP '2026-01-02 03:04:05'",
];

#[tokio::test]
async fn a_field_assignment_into_the_leaf_is_refused_too() {
    let session = session("America/New_York");
    let _warehouse = catalog(&session).await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id INT, st STRUCT<v: timestamp_ns, n: INT>) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(&session, "INSERT INTO ice.ns.t (id) VALUES (1)").await;
    for write in FIELD_DOORS {
        let refused = attempt(&session, write).await.expect_err(write);
        assert!(refused.contains(REFUSAL), "{write}: {refused}");
        assert!(
            refused.contains("Cannot safely cast `st`.`v` to"),
            "{write}: {refused}"
        );
    }
    let snapshots = stored(&session, "SELECT count(*) FROM ice.ns.`t$snapshots`").await;
    assert_eq!(snapshots, vec![Some(1)]);
}

#[tokio::test]
async fn the_refusal_names_the_table_the_column_and_the_leaf() {
    let session = session("America/New_York");
    let _warehouse = catalog(&session).await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id INT, st STRUCT<a: timestamp_ns, b: TIMESTAMP>) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    let refused = attempt(
        &session,
        "INSERT OVERWRITE ice.ns.t SELECT id, named_struct('q', ns, 'b', x) FROM ice.ns.src",
    )
    .await
    .expect_err("refused");
    let expected = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible \
                    data for the table `ice`.`ns`.`t`: Cannot safely cast `st`.`a` to \
                    \"TIMESTAMP_NS\". A nested timestamp_ns leaf is not writable yet: omit the \
                    column `st` or supply NULL for it. SQLSTATE: KD000";
    assert!(refused.ends_with(expected), "{refused}");
}

const UNSUPPLIED: [&str; 12] = [
    "INSERT INTO ice.ns.t (id, k) VALUES (10, 0)",
    "INSERT INTO ice.ns.t (id, k) SELECT id + 10, 0 FROM ice.ns.src",
    "INSERT INTO ice.ns.t VALUES (12, NULL, 0)",
    "INSERT INTO ice.ns.t SELECT id + 12, NULL, 0 FROM ice.ns.src",
    "INSERT INTO ice.ns.t BY NAME SELECT 0 AS k, id + 13 AS id FROM ice.ns.src",
    "INSERT INTO ice.ns.t BY NAME SELECT 0 AS k, NULL AS st, id + 14 AS id FROM ice.ns.src",
    "UPDATE ice.ns.t SET k = k + 1 WHERE id >= 10",
    "UPDATE ice.ns.t SET k = k + 1",
    "UPDATE ice.ns.t SET st = NULL WHERE id = 10",
    "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id + 9 \
     WHEN MATCHED THEN UPDATE SET k = 7 \
     WHEN NOT MATCHED THEN INSERT (id, k) VALUES (s.id + 20, 0)",
    "MERGE INTO ice.ns.t t USING ice.ns.src s ON t.id = s.id + 30 \
     WHEN NOT MATCHED THEN INSERT (id, st, k) VALUES (s.id + 30, NULL, 0)",
    "DELETE FROM ice.ns.t WHERE id = 11",
];

#[tokio::test]
async fn a_statement_that_does_not_supply_the_column_runs_and_carries_its_rows() {
    for properties in [
        "",
        ", 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', \
         'write.merge.mode'='merge-on-read'",
    ] {
        let session = session("America/New_York");
        let _warehouse = catalog(&session).await;
        run(
            &session,
            &format!(
                "CREATE TABLE ice.ns.t USING iceberg TBLPROPERTIES ('format-version'='3'\
                 {properties}) AS SELECT id, named_struct('v', ns, 'n', 1) AS st, 0 AS k \
                 FROM ice.ns.src"
            ),
        )
        .await;
        let carried = "SELECT st FROM ice.ns.t WHERE id = 1";
        assert_eq!(stored(&session, carried).await, vec![Some(TICKS)]);
        let first = stored(
            &session,
            "SELECT snapshot_id FROM ice.ns.`t$snapshots` ORDER BY committed_at LIMIT 1",
        )
        .await;
        for write in UNSUPPLIED {
            run(&session, write).await;
            assert_eq!(
                stored(&session, carried).await,
                vec![Some(TICKS)],
                "{write}"
            );
        }
        for call in [
            "CALL ice.system.rewrite_data_files(table => 'ns.t', \
             options => map('min-input-files', '2', 'rewrite-all', 'true'))",
            "CALL ice.system.rewrite_manifests(table => 'ns.t')",
        ] {
            run(&session, call).await;
            assert_eq!(stored(&session, carried).await, vec![Some(TICKS)], "{call}");
        }
        let nulls = stored(&session, "SELECT count(*) FROM ice.ns.t WHERE st IS NULL").await;
        assert_eq!(nulls, vec![Some(6)], "{properties}");
        let Some(Some(first)) = first.first() else {
            panic!("no first snapshot");
        };
        let travelled = format!("SELECT st FROM ice.ns.t VERSION AS OF {first}");
        assert_eq!(stored(&session, &travelled).await, vec![Some(TICKS)]);
        let refused = attempt(
            &session,
            "INSERT INTO ice.ns.t SELECT id + 40, named_struct('v', ns, 'n', 1), 0 FROM ice.ns.src",
        )
        .await
        .expect_err("refused");
        assert!(refused.contains(NOT_YET), "{refused}");
    }
}

#[tokio::test]
async fn a_nested_zoned_or_microsecond_leaf_is_not_guarded() {
    let session = session("America/New_York");
    let _warehouse = catalog(&session).await;
    for (index, (leaf_type, ticks)) in [
        ("timestamptz_ns", 1_767_323_045_123_456_000_i64),
        ("TIMESTAMP_NTZ", 1_767_323_045_123_456),
        ("TIMESTAMP", 1_767_323_045_123_456),
    ]
    .into_iter()
    .enumerate()
    {
        let table = format!("ice.ns.c{index}");
        run(
            &session,
            &format!(
                "CREATE TABLE {table} (id INT, st STRUCT<v: {leaf_type}, n: INT>) USING iceberg \
                 TBLPROPERTIES ('format-version'='3')"
            ),
        )
        .await;
        run(
            &session,
            &format!("INSERT INTO {table} SELECT id, named_struct('v', x, 'n', 1) FROM ice.ns.src"),
        )
        .await;
        run(
            &session,
            &format!("UPDATE {table} SET st = named_struct('v', {INSTANT}, 'n', 2) WHERE id = 1"),
        )
        .await;
        let read = format!("SELECT st FROM {table}");
        assert_eq!(
            stored(&session, &read).await,
            vec![Some(ticks)],
            "{leaf_type}"
        );
    }
}

const NARROWED_UPDATES: [&str; 4] = [
    "CASE WHEN id > 0 THEN {c} ELSE NULL END",
    "CASE WHEN id < 0 THEN NULL ELSE {c} END",
    "if(id > 0, {c}, NULL)",
    "array({c}, NULL)[0]",
];

#[tokio::test]
async fn an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds() {
    let session = session("America/New_York");
    let _warehouse = catalog(&session).await;
    run(
        &session,
        "CREATE TABLE ice.ns.u (id INT, v timestamp_ns, c timestamp_ns, z timestamptz_ns) USING \
         iceberg TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.u VALUES \
         (1, NULL, CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns), \
          CAST('2026-01-02 03:04:05.123456789+00:00' AS timestamptz_ns)), \
         (2, NULL, CAST('2026-03-08 02:30:00.000000001' AS timestamp_ns), \
          CAST('2026-03-08 07:30:00.000000001+00:00' AS timestamptz_ns))",
    )
    .await;
    for value in NARROWED_UPDATES {
        for source in ["c", "z"] {
            let write = format!("UPDATE ice.ns.u SET v = {}", value.replace("{c}", source));
            let refused = attempt(&session, &write).await.expect_err(&write);
            let named = refused.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]")
                && refused.contains("Cannot safely cast `v` \"TIMESTAMP\" to \"TIMESTAMP_NS\"")
                && refused.contains("narrowed from nanoseconds to microseconds");
            assert!(named, "{write}: {refused}");
        }
    }
    let read = "SELECT v FROM ice.ns.u ORDER BY id";
    assert_eq!(stored(&session, read).await, vec![None, None]);
    for kept in ["CAST(c AS TIMESTAMP)", "date_trunc('second', c)"] {
        run(
            &session,
            &format!("UPDATE ice.ns.u SET v = {kept} WHERE id > 0"),
        )
        .await;
    }
    assert_eq!(
        stored(&session, read).await,
        vec![
            Some(1_767_323_045_000_000_000),
            Some(1_772_937_000_000_000_000)
        ]
    );
    run(&session, "UPDATE ice.ns.u SET v = c").await;
    let walls = vec![Some(TICKS), Some(1_772_937_000_000_000_001)];
    assert_eq!(stored(&session, read).await, walls);
    run(
        &session,
        "UPDATE ice.ns.u SET v = CASE WHEN id > 0 THEN c ELSE CAST(NULL AS timestamp_ns) END",
    )
    .await;
    assert_eq!(stored(&session, read).await, walls);
    run(
        &session,
        "UPDATE ice.ns.u SET v = CASE WHEN if(id > 0, c, NULL) IS NOT NULL THEN c \
         ELSE CAST(NULL AS timestamp_ns) END",
    )
    .await;
    assert_eq!(stored(&session, read).await, walls);
    run(
        &session,
        &format!("UPDATE ice.ns.u SET v = if(id > 1, {INSTANT}, NULL)"),
    )
    .await;
    assert_eq!(
        stored(&session, read).await,
        vec![None, Some(1_767_305_045_123_456_000)]
    );
    run(&session, "UPDATE ice.ns.u SET v = z").await;
    assert_eq!(
        stored(&session, read).await,
        vec![
            Some(1_767_305_045_123_456_789),
            Some(1_772_940_600_000_000_001),
        ]
    );
    run(&session, &format!("UPDATE ice.ns.u SET v = {INSTANT}")).await;
    assert_eq!(
        stored(&session, read).await,
        vec![Some(1_767_305_045_123_456_000); 2]
    );
}
