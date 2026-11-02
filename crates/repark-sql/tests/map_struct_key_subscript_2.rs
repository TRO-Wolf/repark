use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const UTC: &str = "UTC";
const NEW_YORK: &str = "America/New_York";

const FLOAT_TABLES: [&str; 4] = [
    "CREATE TABLE ice.ns.r3sd (id INT, m MAP<STRUCT<k: DOUBLE, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3sd VALUES (1, map(named_struct('k', CAST(0 AS DOUBLE), 'j', 2), 7, named_struct('k', CAST('NaN' AS DOUBLE), 'j', 2), 8)), (2, map(named_struct('k', CAST(1 AS DOUBLE), 'j', 2), 9))",
    "CREATE TABLE ice.ns.r3ad (id INT, m MAP<ARRAY<DOUBLE>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3ad VALUES (1, map(array(CAST(0 AS DOUBLE)), 7, array(CAST('NaN' AS DOUBLE)), 8)), (2, map(array(CAST(1 AS DOUBLE)), 9))",
];

const NTZ_TABLES: [&str; 4] = [
    "CREATE TABLE ice.ns.r3tn (id INT, m MAP<STRUCT<t: TIMESTAMP_NTZ, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3tn VALUES (1, map(named_struct('t', TIMESTAMP_NTZ '2024-01-01 00:00:00', 'j', 2), 7, named_struct('t', TIMESTAMP_NTZ '2024-01-01 05:00:00', 'j', 2), 8)), (2, map(named_struct('t', TIMESTAMP_NTZ '2024-01-02 00:00:00', 'j', 2), 9, named_struct('t', TIMESTAMP_NTZ '2024-01-02 05:00:00', 'j', 2), 10))",
    "CREATE TABLE ice.ns.r3atn (id INT, m MAP<ARRAY<TIMESTAMP_NTZ>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3atn VALUES (1, map(array(TIMESTAMP_NTZ '2024-01-01 00:00:00'), 7, array(TIMESTAMP_NTZ '2024-01-01 05:00:00'), 8)), (2, map(array(TIMESTAMP_NTZ '2024-01-03 00:00:00'), 9))",
];

const ELEMENT_TABLES: [&str; 10] = [
    "CREATE TABLE ice.ns.r3aall (id INT, m MAP<ARRAY<STRING>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3aall VALUES (1, map(array('1.5'), 7, array('1.0E20'), 8, array('NaN'), 9, array('1'), 10, array('2024-01-01'), 11, array('1.50'), 12, array('true'), 13, array('-0.0'), 14, array('2024-01-01 00:00:00'), 15, array('1.1'), 16)), (2, map(array('zz'), 99))",
    "CREATE TABLE ice.ns.r3at (id INT, m MAP<ARRAY<TIMESTAMP>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3at VALUES (1, map(array(TIMESTAMP '2024-01-01 00:00:00'), 7)), (2, map(array(TIMESTAMP '2024-01-02 00:00:00'), 9))",
    "CREATE TABLE ice.ns.r3nas (id INT, m MAP<STRUCT<a: INT, arr: ARRAY<STRUCT<x: STRING>>>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3nas VALUES (1, map(named_struct('a', 1, 'arr', array(named_struct('x', '1.5'))), 7, named_struct('a', 1, 'arr', array(named_struct('x', '1'))), 8)), (2, map(named_struct('a', 2, 'arr', array(named_struct('x', 'q'))), 9))",
    "CREATE TABLE ice.ns.r3nai (id INT, m MAP<STRUCT<a: INT, arr: ARRAY<STRUCT<x: INT, y: INT>>>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3nai VALUES (1, map(named_struct('a', 1, 'arr', array(named_struct('x', 1, 'y', 2))), 7)), (2, map(named_struct('a', 2, 'arr', array(named_struct('x', 3, 'y', 4))), 9))",
    "CREATE TABLE ice.ns.r3ai (id INT, m MAP<ARRAY<INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3ai VALUES (1, map(array(1), 7, array(2), 8, array(-1), 9, array(0), 10)), (2, map(array(5), 99))",
];

const M_D: &str = "map(named_struct('k', CAST(0 AS DOUBLE), 'j', 2), 1, named_struct('k', CAST('NaN' AS DOUBLE), 'j', 2), 2, named_struct('k', CAST('Infinity' AS DOUBLE), 'j', 2), 3, named_struct('k', CAST('1.5' AS DOUBLE), 'j', 2), 4, named_struct('k', CAST('1.1' AS DOUBLE), 'j', 2), 5)";
const M_DN: &str = "map(named_struct('k', CAST('-0.0' AS DOUBLE), 'j', 2), 1, named_struct('k', sqrt(CAST(-1 AS DOUBLE)), 'j', 2), 2)";
const M_F: &str = "map(named_struct('k', CAST(1.1 AS FLOAT), 'j', 2), 1, named_struct('k', CAST('NaN' AS FLOAT), 'j', 2), 2, named_struct('k', CAST(0 AS FLOAT), 'j', 2), 3)";
const M_AS: &str = "map(array('1.5'), 7, array('1.0E20'), 8, array('NaN'), 9, array('1'), 10, array('2024-01-01'), 11, array('1.50'), 12, array('true'), 13, array('-0.0'), 14, array('2024-01-01 00:00:00'), 15)";

const ANSI_TABLES: [&str; 12] = [
    "CREATE TABLE ice.ns.c (id INT, m MAP<STRUCT<k: INT, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.c VALUES (1, map(named_struct('k', 1, 'j', 2), 7, named_struct('k', 3, 'j', 2), 8)), (2, map(named_struct('k', 1, 'j', 5), 9))",
    "CREATE TABLE ice.ns.r4fd (id INT, m MAP<STRUCT<k: DOUBLE, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r4fd VALUES (1, map(named_struct('k', CAST(0 AS DOUBLE), 'j', 2), 7, named_struct('k', CAST('NaN' AS DOUBLE), 'j', 2), 8, named_struct('k', CAST('Infinity' AS DOUBLE), 'j', 2), 9, named_struct('k', CAST('-Infinity' AS DOUBLE), 'j', 2), 10, named_struct('k', 1.1D, 'j', 2), 11, named_struct('k', CAST(CAST(1.1 AS FLOAT) AS DOUBLE), 'j', 2), 12)), (2, map(named_struct('k', CAST(1 AS DOUBLE), 'j', 2), 13))",
    "CREATE TABLE ice.ns.r3ast (id INT, m MAP<ARRAY<STRUCT<k: STRING, j: INT>>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3ast VALUES (1, map(array(named_struct('k', 'a', 'j', 2)), 7)), (2, map(array(named_struct('k', 'b', 'j', 2)), 9))",
    "CREATE TABLE ice.ns.r3sa (id INT, m MAP<STRUCT<a: INT, arr: ARRAY<STRING>>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3sa VALUES (1, map(named_struct('a', 1, 'arr', array('x')), 7)), (2, map(named_struct('a', 2, 'arr', array('y')), 9))",
    "CREATE TABLE ice.ns.r3as (id INT, m MAP<ARRAY<STRING>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r3as VALUES (1, map(array('a', 'b'), 7, array('1.5'), 8)), (2, map(array('c'), 9))",
    "CREATE TABLE ice.ns.r4adn (id INT, m MAP<ARRAY<DOUBLE>, INT>) USING iceberg",
    "INSERT INTO ice.ns.r4adn VALUES (1, map(array(CAST(0 AS DOUBLE), CAST('NaN' AS DOUBLE)), 7, array(CAST(1 AS DOUBLE), CAST(0 AS DOUBLE)), 8)), (2, map(array(CAST('NaN' AS DOUBLE)), 9))",
];

const ANSI_OFF_REFUSALS: [(&str, &str); 10] = [
    (
        "SELECT id, m[named_struct('j', 1, 'k', 2)] FROM ice.ns.c ORDER BY id",
        "\"STRUCT<j: INT",
    ),
    (
        "SELECT id, m[named_struct('k', CAST(1 AS BIGINT), 'j', 2)] FROM ice.ns.c ORDER BY id",
        "\"STRUCT<k: BIGINT",
    ),
    (
        "SELECT id, m[named_struct('x', id, 'y', 2)] FROM ice.ns.c ORDER BY id",
        "\"STRUCT<x: INT",
    ),
    (
        "SELECT map(named_struct('k', 1, 'j', 2), 7)[named_struct('j', 1, 'k', 2)]",
        "\"STRUCT<j: INT",
    ),
    (
        "SELECT id, m[named_struct('k', 1, 'j', 2)] FROM ice.ns.r4fd ORDER BY id",
        "\"STRUCT<k: INT",
    ),
    (
        "SELECT id, m[named_struct('k', CAST('NaN' AS FLOAT), 'j', 2)] FROM ice.ns.r4fd ORDER BY id",
        "\"STRUCT<k: FLOAT",
    ),
    (
        "SELECT id, m[array(named_struct('x', 'a', 'y', 2))] FROM ice.ns.r3ast ORDER BY id",
        "\"ARRAY<STRUCT<x: STRING",
    ),
    (
        "SELECT id, m[array(named_struct('k', 'a', 'j', CAST(2 AS BIGINT)))] FROM ice.ns.r3ast ORDER BY id",
        "\"ARRAY<STRUCT<k: STRING",
    ),
    (
        "SELECT id, m[named_struct('b', CAST(2 AS BIGINT), 'c', array('y'))] FROM ice.ns.r3sa ORDER BY id",
        "\"STRUCT<b: BIGINT",
    ),
    (
        "SELECT map(named_struct('k', 0.0D), 7)[named_struct('k', CAST(-0.0D AS FLOAT))]",
        "\"STRUCT<k: FLOAT",
    ),
];

const ANSI_OFF_ANSWERS: [(&str, &[&str]); 14] = [
    (
        "SELECT id, m[named_struct('k', 1, 'j', 2)] FROM ice.ns.c ORDER BY id",
        &["1|7", "2|NULL"],
    ),
    (
        "SELECT id, m[named_struct('k', CAST('Infinity' AS DOUBLE), 'j', 2)] FROM ice.ns.r4fd ORDER BY id",
        &["1|9", "2|NULL"],
    ),
    (
        "SELECT id, m[named_struct('k', -exp(1000D), 'j', 2)] FROM ice.ns.r4fd ORDER BY id",
        &["1|10", "2|NULL"],
    ),
    (
        "SELECT id, m[named_struct('a', 1, 'arr', array('x'))] FROM ice.ns.r3sa ORDER BY id",
        &["1|7", "2|NULL"],
    ),
    (
        "SELECT map(named_struct('a', 1, 'arr', array(named_struct('x', '1.5'))), 7)[named_struct('a', 1, 'arr', array(named_struct('x', '1.5')))]",
        &["7"],
    ),
    (
        "SELECT map(named_struct('a', array(named_struct('b', array(0.0D)))), 7)[named_struct('a', array(named_struct('b', array(-0.0D))))]",
        &["7"],
    ),
    (
        "SELECT map(named_struct('k', CAST(0 AS FLOAT)), 7)[named_struct('k', CAST(-0.0D AS FLOAT))]",
        &["7"],
    ),
    ("SELECT map(array(1), 7)[array(CAST(1 AS BIGINT))]", &["7"]),
    (
        "SELECT map(array(CAST(0 AS DOUBLE)), 1, array(CAST('NaN' AS DOUBLE)), 2)[array(0)]",
        &["1"],
    ),
    (
        "SELECT map(array('1.5'), 7, array('1'), 10)[array(1)]",
        &["10"],
    ),
    (
        "SELECT map(array(TIMESTAMP '2024-01-01 00:00:00'), 7)[array(DATE '2024-01-01')]",
        &["7"],
    ),
    (
        "SELECT id, m[array(CAST('1.5' AS DOUBLE))] FROM ice.ns.r3as ORDER BY id",
        &["1|8", "2|NULL"],
    ),
    (
        "SELECT id, m[array(CAST(-0.0D AS FLOAT), CAST('NaN' AS FLOAT))] FROM ice.ns.r4adn ORDER BY id",
        &["1|7", "2|NULL"],
    ),
    (
        "SELECT id, m[array(1, 0)] FROM ice.ns.r4adn ORDER BY id",
        &["1|8", "2|NULL"],
    ),
];

const ANSI_ON_ANSWERS: [(&str, &[&str]); 4] = [
    (
        "SELECT id, m[named_struct('j', 1, 'k', 2)] FROM ice.ns.c ORDER BY id",
        &["1|7", "2|NULL"],
    ),
    (
        "SELECT id, m[named_struct('k', CAST(1 AS BIGINT), 'j', 2)] FROM ice.ns.c ORDER BY id",
        &["1|7", "2|NULL"],
    ),
    (
        "SELECT id, m[named_struct('x', id, 'y', 2)] FROM ice.ns.c ORDER BY id",
        &["1|7", "2|NULL"],
    ),
    (
        "SELECT map(named_struct('k', 1, 'j', 2), 7)[named_struct('j', 1, 'k', 2)]",
        &["7"],
    ),
];

struct Door {
    session: ReparkSession,
    _dir: TempDir,
}

async fn spark_door(zone: &str, tables: &[&[&str]]) -> Door {
    spark_door_with(&[("spark.sql.session.timeZone", zone)], tables).await
}

async fn spark_door_with(settings: &[(&str, &str)], tables: &[&[&str]]) -> Door {
    let dir = TempDir::new().expect("warehouse");
    let warehouse = dir.path().to_str().expect("utf8").to_string();
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let mut builder = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect);
    for (key, value) in settings {
        builder = builder.config(*key, *value);
    }
    let session = builder.build().expect("spark session");
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .expect("catalog");
    let door = Door { session, _dir: dir };
    run(
        &door.session,
        &format!("CREATE NAMESPACE ice.ns LOCATION '{warehouse}/ns'"),
    )
    .await;
    for statement in tables.iter().flat_map(|table| table.iter()) {
        run(&door.session, statement).await;
    }
    door
}

async fn run(session: &ReparkSession, sql: &str) {
    answer(session, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` must succeed: {error}"));
}

async fn batches(session: &ReparkSession, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let frame = session.sql(sql).await.map_err(|error| error.to_string())?;
    let joined = tokio::spawn(async move { frame.collect().await }).await;
    joined
        .map_err(|error| format!("panicked: {error}"))?
        .map_err(|error| error.to_string())
}

fn cell(column: &dyn Array, row: usize) -> String {
    if column.is_null(row) {
        return "NULL".to_string();
    }
    let text = cast(&column.slice(row, 1), &DataType::Utf8).expect("render as text");
    text.as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8")
        .value(0)
        .to_string()
}

async fn answer(session: &ReparkSession, sql: &str) -> Result<Vec<String>, String> {
    let batches = batches(session, sql).await?;
    Ok(batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| cell(column.as_ref(), row))
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect())
}

async fn expect(door: &Door, sql: &str, expected: &[&str]) {
    let answered = answer(&door.session, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` must answer Spark's values: {error}"));
    assert_eq!(answered, expected, "{sql}");
}

#[tokio::test]
async fn a_signed_zero_or_nan_leaf_matches_the_entry_spark_matches_in_a_table() {
    let door = spark_door(UTC, &[&FLOAT_TABLES]).await;
    for (key, answer) in [
        (
            "named_struct('k', CAST('-0.0' AS DOUBLE), 'j', 2)",
            ["1|7", "2|NULL"],
        ),
        (
            "named_struct('x', sqrt(CAST(-1 AS DOUBLE)), 'y', 2)",
            ["1|8", "2|NULL"],
        ),
        (
            "named_struct('k', CAST('NaN' AS DOUBLE), 'j', 2)",
            ["1|8", "2|NULL"],
        ),
        ("named_struct('k', 1, 'j', 2)", ["1|NULL", "2|9"]),
    ] {
        expect(
            &door,
            &format!("SELECT id, m[{key}] FROM ice.ns.r3sd ORDER BY id"),
            &answer,
        )
        .await;
    }
    for (key, answer) in [
        ("array(CAST('-0.0' AS DOUBLE))", ["1|7", "2|NULL"]),
        ("array(sqrt(CAST(-1 AS DOUBLE)))", ["1|8", "2|NULL"]),
        ("array(CAST('NaN' AS DOUBLE))", ["1|8", "2|NULL"]),
        ("array(1)", ["1|NULL", "2|9"]),
    ] {
        expect(
            &door,
            &format!("SELECT id, m[{key}] FROM ice.ns.r3ad ORDER BY id"),
            &answer,
        )
        .await;
    }
}

#[tokio::test]
async fn a_signed_zero_or_nan_on_either_side_of_a_literal_lookup_matches_like_spark() {
    let door = spark_door(UTC, &[]).await;
    for (map, key, answer) in [
        (
            M_D,
            "named_struct('k', CAST('-0.0' AS DOUBLE), 'j', 2)",
            "1",
        ),
        (
            M_D,
            "named_struct('k', sqrt(CAST(-1 AS DOUBLE)), 'j', 2)",
            "2",
        ),
        (M_DN, "named_struct('k', CAST(0 AS DOUBLE), 'j', 2)", "1"),
        (
            M_DN,
            "named_struct('k', CAST('-0.0' AS DOUBLE), 'j', 2)",
            "1",
        ),
        (
            M_DN,
            "named_struct('k', CAST('NaN' AS DOUBLE), 'j', 2)",
            "2",
        ),
        (
            M_DN,
            "named_struct('k', sqrt(CAST(-1 AS DOUBLE)), 'j', 2)",
            "2",
        ),
        (
            M_F,
            "named_struct('k', CAST('-0.0' AS DOUBLE), 'j', 2)",
            "3",
        ),
        (
            M_F,
            "named_struct('k', sqrt(CAST(-1 AS DOUBLE)), 'j', 2)",
            "2",
        ),
        (
            M_F,
            "named_struct('k', CAST(CAST('-0.0' AS DOUBLE) AS FLOAT), 'j', 2)",
            "3",
        ),
        (
            "map(array(CAST(0 AS DOUBLE)), 1, array(CAST('NaN' AS DOUBLE)), 2)",
            "array(sqrt(CAST(-1 AS DOUBLE)))",
            "2",
        ),
        (
            "map(array(CAST(0 AS DOUBLE)), 1, array(CAST('NaN' AS DOUBLE)), 2)",
            "array(CAST('-0.0' AS DOUBLE))",
            "1",
        ),
        (
            "map(array(named_struct('k', CAST(0 AS DOUBLE))), 1, array(named_struct('k', CAST('NaN' AS DOUBLE))), 2)",
            "array(named_struct('x', CAST('-0.0' AS DOUBLE)))",
            "1",
        ),
        (
            "map(named_struct('a', 1, 'arr', array(named_struct('x', CAST(0 AS DOUBLE)))), 7)",
            "named_struct('a', 1, 'arr', array(named_struct('x', CAST('-0.0' AS DOUBLE))))",
            "7",
        ),
    ] {
        expect(&door, &format!("SELECT {map}[{key}]"), &[answer]).await;
    }
}

#[tokio::test]
async fn a_timestamp_leaf_against_a_timestamp_ntz_field_takes_the_session_zone() {
    let door = spark_door(NEW_YORK, &[&NTZ_TABLES]).await;
    for (key, answer) in [
        (
            "named_struct('t', TIMESTAMP '2024-01-01 00:00:00', 'j', 2)",
            ["1|7", "2|NULL"],
        ),
        (
            "named_struct('t', CAST(date_add(DATE '2023-12-31', id) AS TIMESTAMP), 'j', 2)",
            ["1|7", "2|9"],
        ),
        (
            "named_struct('t', TIMESTAMP_NTZ '2024-01-01 00:00:00', 'j', 2)",
            ["1|7", "2|NULL"],
        ),
        (
            "named_struct('t', DATE '2024-01-01', 'j', 2)",
            ["1|7", "2|NULL"],
        ),
    ] {
        expect(
            &door,
            &format!("SELECT id, m[{key}] FROM ice.ns.r3tn ORDER BY id"),
            &answer,
        )
        .await;
    }
    expect(
        &door,
        "SELECT id, m[array(TIMESTAMP '2024-01-01 00:00:00')] FROM ice.ns.r3atn ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    for sql in [
        "SELECT map(named_struct('t', TIMESTAMP_NTZ '2024-01-01 00:00:00', 'j', 2), 7, named_struct('t', TIMESTAMP_NTZ '2024-01-01 05:00:00', 'j', 2), 8)[named_struct('t', TIMESTAMP '2024-01-01 00:00:00', 'j', 2)]",
        "SELECT map(named_struct('t', TIMESTAMP '2024-01-01 00:00:00', 'j', 2), 7, named_struct('t', TIMESTAMP '2023-12-31 19:00:00', 'j', 2), 8)[named_struct('t', TIMESTAMP_NTZ '2024-01-01 00:00:00', 'j', 2)]",
        "SELECT map(named_struct('t', TIMESTAMP '2024-01-01 00:00:00', 'j', 2), 7, named_struct('t', TIMESTAMP '2023-12-31 19:00:00', 'j', 2), 8)[named_struct('t', DATE '2024-01-01', 'j', 2)]",
        "SELECT map(named_struct('d', DATE '2024-01-01', 'j', 2), 7, named_struct('d', DATE '2024-01-02', 'j', 2), 8)[named_struct('d', TIMESTAMP '2024-01-01 22:00:00', 'j', 2)]",
        "SELECT map(named_struct('k', '2024-01-01 00:00:00', 'j', 2), 7, named_struct('k', '2024-01-01 05:00:00', 'j', 2), 8)[named_struct('k', TIMESTAMP '2024-01-01 00:00:00', 'j', 2)]",
        "SELECT map(array(TIMESTAMP '2024-01-01 00:00:00'), 7)[array(DATE '2024-01-01')]",
    ] {
        expect(&door, sql, &["7"]).await;
    }
}

#[tokio::test]
async fn a_timestamp_leaf_against_a_timestamp_ntz_field_in_utc_stays_sparks_value() {
    let door = spark_door(UTC, &[&NTZ_TABLES]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('t', TIMESTAMP '2024-01-01 00:00:00', 'j', 2)] FROM ice.ns.r3tn ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m[named_struct('t', CAST(date_add(DATE '2023-12-31', id) AS TIMESTAMP), 'j', 2)] FROM ice.ns.r3tn ORDER BY id",
        &["1|7", "2|9"],
    )
    .await;
    expect(
        &door,
        "SELECT map(named_struct('t', TIMESTAMP_NTZ '2024-01-01 00:00:00', 'j', 2), 7, named_struct('t', TIMESTAMP_NTZ '2024-01-01 05:00:00', 'j', 2), 8)[named_struct('t', TIMESTAMP '2024-01-01 00:00:00', 'j', 2)]",
        &["7"],
    )
    .await;
}

#[tokio::test]
async fn an_array_element_leaf_is_cast_like_sparks_cast() {
    let door = spark_door(UTC, &[&ELEMENT_TABLES]).await;
    for (element, answer) in [
        ("CAST('1.5' AS DOUBLE)", "1|7"),
        ("1.5D", "1|7"),
        ("CAST('1e20' AS DOUBLE)", "1|8"),
        ("CAST(1.5 AS FLOAT)", "1|7"),
        ("CAST('NaN' AS DOUBLE)", "1|9"),
        ("1", "1|10"),
        ("DATE '2024-01-01'", "1|11"),
        ("CAST(1.50 AS DECIMAL(5,2))", "1|12"),
        ("true", "1|13"),
        ("CAST('-0.0' AS DOUBLE)", "1|14"),
        ("TIMESTAMP '2024-01-01 00:00:00'", "1|15"),
        ("CAST(1.1 AS FLOAT)", "1|16"),
    ] {
        expect(
            &door,
            &format!("SELECT id, m[array({element})] FROM ice.ns.r3aall ORDER BY id"),
            &[answer, "2|NULL"],
        )
        .await;
    }
    for (element, answer) in [
        ("1", "10"),
        ("CAST(1.5 AS FLOAT)", "7"),
        ("DATE '2024-01-01'", "11"),
        ("CAST(1.50 AS DECIMAL(5,2))", "12"),
        ("true", "13"),
        ("TIMESTAMP '2024-01-01 00:00:00'", "15"),
        ("'1'", "10"),
    ] {
        expect(
            &door,
            &format!("SELECT {M_AS}[array({element})]"),
            &[answer],
        )
        .await;
    }
    for (element, answer) in [
        ("CAST('1.7' AS DOUBLE)", ["1|7", "2|NULL"]),
        ("CAST('-1.7' AS DOUBLE)", ["1|9", "2|NULL"]),
        ("CAST(1.5 AS DECIMAL(2,1))", ["1|7", "2|NULL"]),
        ("1.7D", ["1|7", "2|NULL"]),
    ] {
        expect(
            &door,
            &format!("SELECT id, m[array({element})] FROM ice.ns.r3ai ORDER BY id"),
            &answer,
        )
        .await;
    }
}

#[tokio::test]
async fn a_datetime_element_and_a_nested_array_of_structs_are_cast_like_spark() {
    let door = spark_door(UTC, &[&ELEMENT_TABLES]).await;
    expect(
        &door,
        "SELECT id, m[array(DATE '2024-01-01')] FROM ice.ns.r3at ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    for sql in [
        "SELECT map(array(TIMESTAMP '2024-01-01 00:00:00'), 7)[array(DATE '2024-01-01')]",
        "SELECT map(array(TIMESTAMP '2024-01-01 00:00:00'), 7)[array(TIMESTAMP_NTZ '2024-01-01 00:00:00')]",
        "SELECT map(array(TIMESTAMP '2024-01-01 00:00:00'), 7)[array(TIMESTAMP '2024-01-01 00:00:00')]",
        "SELECT map(named_struct('a', 1, 'arr', array(named_struct('x', '1'))), 7)[named_struct('a', 1, 'arr', array(named_struct('x', 1)))]",
    ] {
        expect(&door, sql, &["7"]).await;
    }
    for (key, answer) in [
        (
            "named_struct('a', 1, 'arr', array(named_struct('x', CAST('1.5' AS DOUBLE))))",
            ["1|7", "2|NULL"],
        ),
        (
            "named_struct('a', 1, 'arr', array(named_struct('x', 1)))",
            ["1|8", "2|NULL"],
        ),
        (
            "named_struct('b', 1, 'c', array(named_struct('z', '1')))",
            ["1|8", "2|NULL"],
        ),
    ] {
        expect(
            &door,
            &format!("SELECT id, m[{key}] FROM ice.ns.r3nas ORDER BY id"),
            &answer,
        )
        .await;
    }
    for (key, answer) in [
        (
            "named_struct('b', CAST(1 AS BIGINT), 'c', array(named_struct('y', CAST('1.0' AS DOUBLE), 'x', CAST(2 AS BIGINT))))",
            ["1|7", "2|NULL"],
        ),
        (
            "named_struct('b', CAST(1 AS BIGINT), 'c', array(named_struct('y', CAST('2.0' AS DOUBLE), 'x', CAST(1 AS BIGINT))))",
            ["1|NULL", "2|NULL"],
        ),
    ] {
        expect(
            &door,
            &format!("SELECT id, m[{key}] FROM ice.ns.r3nai ORDER BY id"),
            &answer,
        )
        .await;
    }
}

async fn expect_ansi_off(door: &Door) {
    for (sql, given) in ANSI_OFF_REFUSALS {
        let error = answer(&door.session, sql)
            .await
            .expect_err("with ANSI off a key of another type refuses like Spark");
        for fragment in [
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]",
            "The second parameter requires the",
            given,
            "SQLSTATE: 42K09",
        ] {
            assert!(error.contains(fragment), "{sql}: {fragment} not in {error}");
        }
    }
    for (sql, answered) in ANSI_OFF_ANSWERS {
        expect(door, sql, answered).await;
    }
}

async fn expect_ansi_on(door: &Door) {
    for (sql, answered) in ANSI_ON_ANSWERS {
        expect(door, sql, answered).await;
    }
}

#[tokio::test]
async fn with_ansi_off_in_the_builder_a_struct_key_of_another_type_refuses_like_spark() {
    let door = spark_door_with(
        &[
            ("spark.sql.session.timeZone", UTC),
            ("spark.sql.ansi.enabled", "false"),
        ],
        &[&ANSI_TABLES],
    )
    .await;
    expect_ansi_off(&door).await;
}

#[tokio::test]
async fn ansi_set_inside_a_session_gates_the_struct_key_cast_both_ways() {
    let door = spark_door(UTC, &[&ANSI_TABLES]).await;
    expect_ansi_on(&door).await;
    run(&door.session, "SET spark.sql.ansi.enabled = false").await;
    expect_ansi_off(&door).await;
    run(&door.session, "SET spark.sql.ansi.enabled = true").await;
    expect_ansi_on(&door).await;
}

#[tokio::test]
async fn with_ansi_on_in_the_builder_a_struct_key_is_cast_by_position() {
    let door = spark_door_with(
        &[
            ("spark.sql.session.timeZone", UTC),
            ("spark.sql.ansi.enabled", "true"),
        ],
        &[&ANSI_TABLES],
    )
    .await;
    expect_ansi_on(&door).await;
}
