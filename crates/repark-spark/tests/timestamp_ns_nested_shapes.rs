use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const INSTANT: &str = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'";
const WALLS: [(&str, i64); 2] = [
    ("America/New_York", 1_767_305_045_123_456_000),
    ("Asia/Kolkata", 1_767_342_845_123_456_000),
];
const REFUSAL: &str = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]";
const LEAF: &str = "cannot be stored into this timestamp_ns leaf";

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
    run(
        session,
        "CREATE TABLE ice.ns.src (id INT, x TIMESTAMP, ns timestamp_ns) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        session,
        &format!(
            "INSERT INTO ice.ns.src VALUES \
             (1, {INSTANT}, CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns))"
        ),
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

async fn stored(session: &ReparkSession, table: &str) -> Vec<Option<i64>> {
    let batches = session
        .sql(&format!("SELECT st FROM {table} ORDER BY id"))
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

#[derive(Clone, Copy, Debug)]
enum Answer {
    Wall,
    Refused(&'static str),
}

const PAIR: &str = "STRUCT<a: timestamp_ns, b: TIMESTAMP>";
const DOORS: [(&str, &str); 6] = [
    (
        "insert select",
        "INSERT INTO {t} SELECT id, {st} FROM ice.ns.src",
    ),
    (
        "insert overwrite",
        "INSERT OVERWRITE {t} SELECT id, {st} FROM ice.ns.src",
    ),
    (
        "insert by name",
        "INSERT INTO {t} BY NAME SELECT {st} AS st, id FROM ice.ns.src",
    ),
    (
        "merge insert",
        "MERGE INTO {t} t USING (SELECT id, {st} AS v FROM ice.ns.src) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.v)",
    ),
    (
        "merge update",
        "MERGE INTO {t} t USING (SELECT id, {st} AS v FROM ice.ns.src) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET st = s.v",
    ),
    ("update where", "UPDATE {t} SET st = {st} WHERE id >= 0"),
];
const BY_NAME_UNPAIRED: Answer = Answer::Refused("no source field of that name");
const NOT_FOUND: Answer = Answer::Refused("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA]");
const ARRAY_MERGE: Answer = Answer::Refused("cannot store-assign column `st`");
const SPELLINGS: [(&str, &str, &str, [Answer; 6]); 6] = [
    (
        "names in order",
        PAIR,
        "named_struct('a', x, 'b', x)",
        [Answer::Wall; 6],
    ),
    (
        "names swapped",
        PAIR,
        "named_struct('b', x, 'a', x)",
        [Answer::Wall; 6],
    ),
    (
        "one field renamed",
        PAIR,
        "named_struct('q', x, 'b', x)",
        [
            BY_NAME_UNPAIRED,
            Answer::Wall,
            Answer::Wall,
            NOT_FOUND,
            NOT_FOUND,
            NOT_FOUND,
        ],
    ),
    (
        "one field differently cased",
        PAIR,
        "named_struct('A', x, 'b', x)",
        [
            BY_NAME_UNPAIRED,
            Answer::Wall,
            Answer::Wall,
            Answer::Wall,
            Answer::Wall,
            Answer::Wall,
        ],
    ),
    (
        "no name shared",
        PAIR,
        "named_struct('q', x, 'r', x)",
        [
            BY_NAME_UNPAIRED,
            Answer::Wall,
            Answer::Wall,
            NOT_FOUND,
            NOT_FOUND,
            NOT_FOUND,
        ],
    ),
    (
        "one field renamed, in an array",
        "ARRAY<STRUCT<a: timestamp_ns, b: TIMESTAMP>>",
        "array(named_struct('q', x, 'b', x))",
        [
            BY_NAME_UNPAIRED,
            Answer::Wall,
            Answer::Wall,
            ARRAY_MERGE,
            ARRAY_MERGE,
            Answer::Refused("the source field names are not the target's, in order"),
        ],
    ),
];

#[tokio::test]
async fn a_struct_source_pairs_as_the_door_that_stores_it_pairs() {
    let mut wrong = Vec::new();
    for (zone, wall) in WALLS {
        let session = session(zone, true);
        let _warehouse = catalog(&session).await;
        for (shape_index, (spelling, column_type, value, answers)) in SPELLINGS.iter().enumerate() {
            for (door_index, ((door, write), answer)) in DOORS.iter().zip(answers).enumerate() {
                let table = format!("ice.ns.p{shape_index}_{door_index}");
                run(
                    &session,
                    &format!(
                        "CREATE TABLE {table} (id INT, st {column_type}) USING iceberg \
                         TBLPROPERTIES ('format-version'='3')"
                    ),
                )
                .await;
                let value = if door.starts_with("update") {
                    run(&session, &format!("INSERT INTO {table} (id) VALUES (1)")).await;
                    value.replace('x', INSTANT)
                } else {
                    if door.starts_with("merge update") {
                        run(&session, &format!("INSERT INTO {table} (id) VALUES (1)")).await;
                    }
                    (*value).to_string()
                };
                let write = write.replace("{t}", &table).replace("{st}", &value);
                let outcome = attempt(&session, &write).await;
                let label = format!("{zone} / {spelling} / {door}");
                match (answer, outcome) {
                    (Answer::Wall, Ok(())) => {
                        let got = stored(&session, &table).await;
                        if got != vec![Some(wall)] {
                            wrong.push(format!("{label}: stored {got:?}"));
                        }
                    }
                    (Answer::Refused(text), Err(error)) if error.contains(text) => {}
                    (answer, outcome) => wrong.push(format!("{label}: {answer:?} but {outcome:?}")),
                }
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

#[tokio::test]
async fn the_refusal_names_the_leaf_and_the_reason() {
    let session = session("America/New_York", true);
    let _warehouse = catalog(&session).await;
    run(
        &session,
        &format!(
            "CREATE TABLE ice.ns.t (id INT, st {PAIR}) USING iceberg \
             TBLPROPERTIES ('format-version'='3')"
        ),
    )
    .await;
    let refused = attempt(
        &session,
        "INSERT INTO ice.ns.t SELECT id, named_struct('q', x, 'b', x) FROM ice.ns.src",
    )
    .await
    .expect_err("refused");
    let expected = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible \
                    data for the table ``: Cannot safely cast `st`.`a` \"Struct(\"q\": \
                    Timestamp(µs, \"UTC\"), \"b\": Timestamp(µs, \"UTC\"))\" to \
                    \"TIMESTAMP_NS\". The nested source shape cannot be stored into this \
                    timestamp_ns leaf: no source field of that name and nullability pairs with \
                    it. SQLSTATE: KD000";
    assert!(refused.contains(expected), "{refused}");
    assert_eq!(stored(&session, "ice.ns.t").await, Vec::new());
}

const NARROWED: [&str; 4] = [
    "array(ns, NULL)",
    "array(coalesce(ns, NULL))",
    "array(CASE WHEN id = 1 THEN ns ELSE NULL END)",
    "array(CAST(ns AS TIMESTAMP))",
];
const NARROWING_DOORS: [(&str, &str); 5] = [
    (
        "insert select",
        "INSERT INTO {t} SELECT id, {st} FROM ice.ns.src",
    ),
    (
        "insert select through a subquery",
        "INSERT INTO {t} SELECT id, v FROM (SELECT id, {st} AS v FROM ice.ns.src) AS s",
    ),
    (
        "insert overwrite",
        "INSERT OVERWRITE {t} SELECT id, {st} FROM ice.ns.src",
    ),
    (
        "insert by name",
        "INSERT INTO {t} BY NAME SELECT {st} AS st, id FROM ice.ns.src",
    ),
    ("update where", "UPDATE {t} SET st = {st} WHERE id >= 0"),
];

#[tokio::test]
async fn a_narrowed_nanosecond_value_is_refused_not_truncated() {
    let session = session("America/New_York", true);
    let _warehouse = catalog(&session).await;
    for (value_index, value) in NARROWED.iter().enumerate() {
        for (door_index, (door, write)) in NARROWING_DOORS.iter().enumerate() {
            let table = format!("ice.ns.c{value_index}_{door_index}");
            let carried = if door.starts_with("update") {
                ", ns timestamp_ns"
            } else {
                ""
            };
            run(
                &session,
                &format!(
                    "CREATE TABLE {table} (id INT, st ARRAY<timestamp_ns>{carried}) \
                     USING iceberg TBLPROPERTIES ('format-version'='3')"
                ),
            )
            .await;
            if door.starts_with("update") {
                run(
                    &session,
                    &format!("INSERT INTO {table} (id, ns) SELECT id, ns FROM ice.ns.src"),
                )
                .await;
            }
            let write = write.replace("{t}", &table).replace("{st}", value);
            let refused = attempt(&session, &write)
                .await
                .expect_err(&format!("{door}: {value}"));
            assert!(refused.contains(REFUSAL), "{door} {value}: {refused}");
            assert!(refused.contains(LEAF), "{door} {value}: {refused}");
            assert!(
                refused.contains("narrows a nanosecond value to microseconds"),
                "{door} {value}: {refused}"
            );
        }
    }
    run(
        &session,
        "CREATE TABLE ice.ns.whole (id INT, st ARRAY<timestamp_ns>) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.whole SELECT id, array(ns, CAST(NULL AS timestamp_ns)) FROM ice.ns.src",
    )
    .await;
    assert_eq!(
        stored(&session, "ice.ns.whole").await,
        vec![Some(1_767_323_045_123_456_789), None]
    );
}

#[tokio::test]
async fn a_field_beside_the_leaf_may_be_narrowed_and_merge_is_guarded_too() {
    let session = session("America/New_York", true);
    let _warehouse = catalog(&session).await;
    run(
        &session,
        &format!(
            "CREATE TABLE ice.ns.beside (id INT, st {PAIR}, ns timestamp_ns) USING iceberg \
             TBLPROPERTIES ('format-version'='3')"
        ),
    )
    .await;
    let beside = "named_struct('a', ns, 'b', CAST(ns AS TIMESTAMP))";
    run(
        &session,
        &format!("INSERT INTO ice.ns.beside SELECT id, {beside}, ns FROM ice.ns.src"),
    )
    .await;
    assert_eq!(
        stored(&session, "ice.ns.beside").await,
        vec![Some(1_767_323_045_123_456_789)]
    );
    run(
        &session,
        &format!("UPDATE ice.ns.beside SET st = {beside} WHERE id >= 0"),
    )
    .await;
    run(
        &session,
        "MERGE INTO ice.ns.beside t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN \
         UPDATE SET st = named_struct('a', s.ns, 'b', CAST(s.ns AS TIMESTAMP))",
    )
    .await;
    assert_eq!(
        stored(&session, "ice.ns.beside").await,
        vec![Some(1_767_323_045_123_456_789)]
    );
    let cut = "named_struct('a', coalesce(ns, NULL), 'b', x)";
    for merge in [
        format!(
            "MERGE INTO ice.ns.beside t USING (SELECT id + 1 AS id, {cut} AS v FROM ice.ns.src) s \
             ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.v)"
        ),
        format!(
            "MERGE INTO ice.ns.beside t USING (SELECT id, {cut} AS v FROM ice.ns.src) s \
             ON t.id = s.id WHEN MATCHED THEN UPDATE SET st = s.v"
        ),
        "MERGE INTO ice.ns.beside t USING (SELECT id, coalesce(ns, NULL) AS v FROM ice.ns.src) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.st.a = s.v"
            .to_string(),
    ] {
        let refused = attempt(&session, &merge).await.expect_err(&merge);
        assert!(refused.contains(REFUSAL), "{merge}: {refused}");
        assert!(
            refused.contains("narrows a nanosecond value to microseconds"),
            "{merge}: {refused}"
        );
    }
    assert_eq!(
        stored(&session, "ice.ns.beside").await,
        vec![Some(1_767_323_045_123_456_789)]
    );
}

#[tokio::test]
async fn a_required_zoned_column_refuses_an_overflow_as_main_does() {
    let session = session("America/New_York", false);
    let _warehouse = catalog(&session).await;
    run(
        &session,
        "CREATE TABLE ice.ns.t (id INT, v timestamptz_ns NOT NULL, k INT) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    let refused = attempt(
        &session,
        "INSERT INTO ice.ns.t SELECT 1, TIMESTAMP '3000-01-01 00:00:00.000001+00:00', 0",
    )
    .await
    .expect_err("refused");
    assert!(
        refused.ends_with("Column 'v' is declared as non-nullable but contains null values"),
        "{refused}"
    );
}

const UNSTORABLE: [(&str, &str, &str); 4] = [
    (
        "UPDATE with no WHERE",
        "UPDATE ice.ns.t SET st = named_struct('a', TIMESTAMP '2026-01-02 03:04:05', 'b', NULL)",
        "this statement cannot store a nested value",
    ),
    (
        "an integer leaf",
        "INSERT INTO ice.ns.t SELECT id, named_struct('a', 5, 'b', x) FROM ice.ns.src",
        "the source layout cannot carry a timestamp",
    ),
    (
        "a struct where the leaf is",
        "INSERT INTO ice.ns.t SELECT id, named_struct('a', named_struct('z', x), 'b', x) \
         FROM ice.ns.src",
        "the source nests differently from the target",
    ),
    (
        "fewer fields than the target, by position",
        "INSERT OVERWRITE ice.ns.t SELECT id, named_struct('q', x) FROM ice.ns.src",
        "no source field pairs with it by position or by a complete set of names",
    ),
];

#[tokio::test]
async fn what_cannot_feed_the_leaf_is_refused_by_name() {
    let session = session("America/New_York", true);
    let _warehouse = catalog(&session).await;
    run(
        &session,
        &format!(
            "CREATE TABLE ice.ns.t (id INT, st {PAIR}) USING iceberg \
             TBLPROPERTIES ('format-version'='3')"
        ),
    )
    .await;
    run(&session, "INSERT INTO ice.ns.t (id) VALUES (7)").await;
    for (case, write, reason) in UNSTORABLE {
        let refused = attempt(&session, write).await.expect_err(case);
        assert!(refused.contains(REFUSAL), "{case}: {refused}");
        assert!(refused.contains(LEAF), "{case}: {refused}");
        assert!(refused.contains(reason), "{case}: {refused}");
    }
    assert_eq!(stored(&session, "ice.ns.t").await, vec![None]);
    run(&session, "UPDATE ice.ns.t SET id = id + 1").await;
}

#[tokio::test]
async fn a_null_struct_and_a_null_typed_field_store_as_they_are() {
    let session = session("America/New_York", true);
    let _warehouse = catalog(&session).await;
    let value = "CASE WHEN id = 2 THEN NULL ELSE named_struct('v', x, 'w', NULL) END";
    for (index, write) in [
        "INSERT INTO {t} SELECT id, {st} FROM ice.ns.two",
        "INSERT OVERWRITE {t} SELECT id, {st} FROM ice.ns.two",
        "INSERT INTO {t} BY NAME SELECT {st} AS st, id FROM ice.ns.two",
        "MERGE INTO {t} t USING (SELECT id, {st} AS v FROM ice.ns.two) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.v)",
    ]
    .iter()
    .enumerate()
    {
        if index == 0 {
            run(
                &session,
                "CREATE TABLE ice.ns.two (id INT, x TIMESTAMP) USING iceberg \
                 TBLPROPERTIES ('format-version'='3')",
            )
            .await;
            run(
                &session,
                &format!("INSERT INTO ice.ns.two VALUES (1, {INSTANT}), (2, {INSTANT})"),
            )
            .await;
        }
        let table = format!("ice.ns.z{index}");
        run(
            &session,
            &format!(
                "CREATE TABLE {table} (id INT, st STRUCT<v: timestamp_ns, w: timestamp_ns>) \
                 USING iceberg TBLPROPERTIES ('format-version'='3')"
            ),
        )
        .await;
        run(
            &session,
            &write.replace("{t}", &table).replace("{st}", value),
        )
        .await;
        assert_eq!(
            stored(&session, &table).await,
            vec![Some(WALLS[0].1), None],
            "{write}"
        );
    }
}
