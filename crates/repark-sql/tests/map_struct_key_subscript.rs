use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

const PAIR_KEY_TABLE: [&str; 3] = [
    "CREATE TABLE ice.ns.c (id INT, m MAP<STRUCT<k: INT, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.c VALUES (1, map(named_struct('k', 1, 'j', 2), 7, named_struct('k', 3, 'j', 2), 8))",
    "INSERT INTO ice.ns.c VALUES (2, map(named_struct('k', 1, 'j', 5), 9))",
];

const ONE_FIELD_KEY_TABLE: [&str; 3] = [
    "CREATE TABLE ice.ns.d (id INT, m MAP<STRUCT<j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.d VALUES (1, map(named_struct('j', 2), 7, named_struct('j', 3), 8))",
    "INSERT INTO ice.ns.d VALUES (2, map(named_struct('j', 5), 9))",
];

const NULL_FIELD_KEY_TABLE: [&str; 3] = [
    "CREATE TABLE ice.ns.n (id INT, m MAP<STRUCT<k: INT, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.n VALUES (1, map(named_struct('k', 1, 'j', CAST(NULL AS INT)), 5, named_struct('k', 1, 'j', 2), 6))",
    "INSERT INTO ice.ns.n VALUES (2, CAST(NULL AS MAP<STRUCT<k: INT, j: INT>, INT>))",
];

const SCALAR_KEY_TABLE: [&str; 3] = [
    "CREATE TABLE ice.ns.e (id INT, m MAP<STRING, INT>) USING iceberg",
    "INSERT INTO ice.ns.e VALUES (1, map('a', 7, 'b', 8))",
    "INSERT INTO ice.ns.e VALUES (2, map('c', 9))",
];

const STRING_FIELD_KEY_TABLE: [&str; 3] = [
    "CREATE TABLE ice.ns.s (id INT, m MAP<STRUCT<k: STRING, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.s VALUES (1, map(named_struct('k', 'a', 'j', 2), 7, named_struct('k', 'b', 'j', 2), 8))",
    "INSERT INTO ice.ns.s VALUES (2, map(named_struct('k', 'a', 'j', 5), 9))",
];

const LEAF_TABLES: [&str; 18] = [
    "CREATE TABLE ice.ns.ls (id INT, m MAP<STRUCT<k: STRING, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.ls VALUES (1, map(named_struct('k', 'a', 'j', 2), 7, named_struct('k', '2024-01-01', 'j', 2), 8, named_struct('k', 'true', 'j', 2), 9, named_struct('k', '1.0', 'j', 2), 10, named_struct('k', '1', 'j', 2), 11, named_struct('k', '1.0E20', 'j', 2), 13, named_struct('k', '2024-01-01 00:00:00', 'j', 2), 14)), (2, map(named_struct('k', 'a', 'j', 5), 12))",
    "CREATE TABLE ice.ns.nn (id INT, m MAP<STRUCT<k: INT NOT NULL, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.nn VALUES (1, map(named_struct('k', 1, 'j', 2), 7, named_struct('k', 3, 'j', 2), 8)), (2, map(named_struct('k', 1, 'j', 5), 9))",
    "CREATE TABLE ice.ns.ts (id INT, m MAP<STRUCT<t: TIMESTAMP, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.ts VALUES (1, map(named_struct('t', TIMESTAMP '2024-01-01 00:00:00', 'j', 2), 7)), (2, map(named_struct('t', TIMESTAMP '2024-01-02 00:00:00', 'j', 2), 8))",
    "CREATE TABLE ice.ns.tsn (id INT, m MAP<STRUCT<t: TIMESTAMP_NTZ, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.tsn VALUES (1, map(named_struct('t', TIMESTAMP_NTZ '2024-01-01 00:00:00', 'j', 2), 7)), (2, map(named_struct('t', TIMESTAMP_NTZ '2024-01-02 00:00:00', 'j', 2), 8))",
    "CREATE TABLE ice.ns.dt (id INT, m MAP<STRUCT<d: DATE, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.dt VALUES (1, map(named_struct('d', DATE '2024-01-01', 'j', 2), 7)), (2, map(named_struct('d', DATE '2024-01-02', 'j', 2), 8))",
    "CREATE TABLE ice.ns.bo (id INT, m MAP<STRUCT<b: BOOLEAN, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.bo VALUES (1, map(named_struct('b', true, 'j', 2), 7)), (2, map(named_struct('b', false, 'j', 2), 8))",
    "CREATE TABLE ice.ns.bi (id INT, m MAP<STRUCT<b: BINARY, j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.bi VALUES (1, map(named_struct('b', X'01', 'j', 2), 7)), (2, map(named_struct('b', X'61', 'j', 2), 8))",
    "CREATE TABLE ice.ns.dec (id INT, m MAP<STRUCT<k: DECIMAL(5,2), j: INT>, INT>) USING iceberg",
    "INSERT INTO ice.ns.dec VALUES (1, map(named_struct('k', CAST(1.50 AS DECIMAL(5,2)), 'j', 2), 7, named_struct('k', CAST(1.51 AS DECIMAL(5,2)), 'j', 2), 8)), (2, map(named_struct('k', CAST(123.45 AS DECIMAL(5,2)), 'j', 2), 9))",
    "CREATE TABLE ice.ns.deep (id INT, m MAP<STRUCT<a: INT, s: STRUCT<x: INT, t: STRUCT<p: INT, q: INT>>>, INT>) USING iceberg",
    "INSERT INTO ice.ns.deep VALUES (1, map(named_struct('a', 1, 's', named_struct('x', 2, 't', named_struct('p', 3, 'q', 4))), 7, named_struct('a', 1, 's', named_struct('x', 2, 't', CAST(NULL AS STRUCT<p: INT, q: INT>))), 8, named_struct('a', 1, 's', CAST(NULL AS STRUCT<x: INT, t: STRUCT<p: INT, q: INT>>)), 9)), (2, map(named_struct('a', 2, 's', named_struct('x', 3, 't', named_struct('p', 4, 'q', 4))), 12))",
];

const ARRAY_STRUCT_TABLE: [&str; 2] = [
    "CREATE TABLE ice.ns.as_ (id INT, m MAP<ARRAY<STRUCT<k: INT, j: INT>>, INT>) USING iceberg",
    "INSERT INTO ice.ns.as_ VALUES (1, map(array(named_struct('k', 1, 'j', 2)), 7, array(named_struct('k', 1, 'j', 2), named_struct('k', 3, 'j', 4)), 8, array(named_struct('k', 1, 'j', 2), CAST(NULL AS STRUCT<k: INT, j: INT>)), 10)), (2, map(array(named_struct('k', 5, 'j', 6)), 9))",
];

const ARRAY_OF_STRUCT_LITERAL: &str = "SELECT map(array(named_struct('k', 1, 'j', 2)), 7)";

const STRUCT_OF_ARRAY_LITERAL: &str =
    "SELECT map(named_struct('a', array(named_struct('x', 1, 'y', 2))), 7)";

const TWO_ENTRY_LITERAL: &str =
    "SELECT map(named_struct('k', 1, 'j', 2), 7, named_struct('k', 3, 'j', 2), 8) AS m";

struct Door {
    session: ReparkSession,
    _dir: TempDir,
}

async fn spark_door(tables: &[&[&str]]) -> Door {
    let dir = TempDir::new().expect("warehouse");
    let warehouse = dir.path().to_str().expect("utf8").to_string();
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config("spark.sql.session.timeZone", "UTC")
        .build()
        .expect("spark session");
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

fn ansi_door() -> Door {
    let dir = TempDir::new().expect("warehouse");
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("ansi session");
    Door { session, _dir: dir }
}

async fn run(session: &ReparkSession, sql: &str) {
    answer(session, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` must succeed: {error}"));
}

fn panic_text(error: tokio::task::JoinError) -> String {
    let Ok(payload) = error.try_into_panic() else {
        return "the query task was cancelled".to_string();
    };
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
        .map_or_else(
            || "panicked".to_string(),
            |text| format!("panicked: {text}"),
        )
}

async fn batches(session: &ReparkSession, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let frame = session.sql(sql).await.map_err(|error| error.to_string())?;
    let joined = tokio::spawn(async move { frame.collect().await }).await;
    joined
        .map_err(panic_text)?
        .map_err(|error| error.to_string())
}

fn cell(column: &dyn Array, row: usize) -> String {
    if column.is_null(row) {
        return "NULL".to_string();
    }
    let text = cast(&column.slice(row, 1), &DataType::Utf8).expect("render as text");
    text.as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("utf8")
        .value(0)
        .to_string()
}

fn render(batches: &[RecordBatch]) -> Vec<String> {
    batches
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
        .collect()
}

async fn answer(session: &ReparkSession, sql: &str) -> Result<Vec<String>, String> {
    batches(session, sql).await.map(|batches| render(&batches))
}

async fn expect(door: &Door, sql: &str, expected: &[&str]) {
    let answered = answer(&door.session, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` must answer Spark's values: {error}"));
    assert_eq!(answered, expected, "{sql}");
}

async fn expect_refusal(door: &Door, sql: &str, fragments: &[&str]) {
    let error = answer(&door.session, sql)
        .await
        .expect_err("the key refuses like Spark");
    for fragment in [
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]",
        "SQLSTATE: 42K09",
    ]
    .iter()
    .chain(fragments)
    {
        assert!(error.contains(fragment), "{sql}: {fragment} not in {error}");
    }
}

async fn expect_sorted(door: &Door, sql: &str, expected: &[&str]) {
    let mut answered = answer(&door.session, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` must answer Spark's values: {error}"));
    answered.sort();
    assert_eq!(answered, expected, "{sql}");
}

#[tokio::test]
async fn a_struct_key_subscript_over_a_table_answers_sparks_values() {
    let door = spark_door(&[&PAIR_KEY_TABLE]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', 1, 'j', 2)] FROM ice.ns.c ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT m[named_struct('k', 1, 'j', 2)] FROM ice.ns.c ORDER BY id",
        &["7", "NULL"],
    )
    .await;
}

#[tokio::test]
async fn a_struct_key_subscript_answers_the_type_of_the_map_values() {
    let door = spark_door(&[&PAIR_KEY_TABLE]).await;
    let answered = batches(
        &door.session,
        "SELECT m[named_struct('k', 1, 'j', 2)] AS v FROM ice.ns.c ORDER BY id",
    )
    .await
    .unwrap_or_else(|error| panic!("the subscript must answer: {error}"));
    assert_eq!(answered[0].schema().field(0).data_type(), &DataType::Int32);
}

#[tokio::test]
async fn a_struct_key_subscript_under_a_filter_answers_sparks_values() {
    let door = spark_door(&[&PAIR_KEY_TABLE]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', 3, 'j', 2)] FROM ice.ns.c WHERE id = 1",
        &["1|8"],
    )
    .await;
}

#[tokio::test]
async fn a_struct_key_subscript_over_a_literal_map_answers_sparks_values() {
    let door = spark_door(&[]).await;
    expect(
        &door,
        &format!("SELECT m[named_struct('k', 1, 'j', 2)] FROM ({TWO_ENTRY_LITERAL})"),
        &["7"],
    )
    .await;
    expect(
        &door,
        &format!("SELECT m[named_struct('k', 3, 'j', 2)] FROM ({TWO_ENTRY_LITERAL})"),
        &["8"],
    )
    .await;
    expect(
        &door,
        "SELECT m[named_struct('k', 1, 'j', 2)] FROM (SELECT map(named_struct('k', 1, 'j', 2), 7) AS m)",
        &["7"],
    )
    .await;
    expect(
        &door,
        "SELECT map(named_struct('k', 1, 'j', 2), 7, named_struct('k', 3, 'j', 2), 8)[named_struct('k', 3, 'j', 2)]",
        &["8"],
    )
    .await;
    expect_sorted(
        &door,
        &format!(
            "SELECT m[named_struct('k', 1, 'j', 2)] FROM ({TWO_ENTRY_LITERAL} UNION ALL SELECT map(named_struct('k', 1, 'j', 5), 9))"
        ),
        &["7", "NULL"],
    )
    .await;
}

#[tokio::test]
async fn a_one_field_struct_key_subscript_answers_sparks_values() {
    let door = spark_door(&[&ONE_FIELD_KEY_TABLE]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('j', 2)] FROM ice.ns.d ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT map(named_struct('a', 1), 'x')[named_struct('a', 1)]",
        &["x"],
    )
    .await;
}

#[tokio::test]
async fn a_struct_key_holding_a_null_field_matches_the_entry_with_that_null_field() {
    let door = spark_door(&[&NULL_FIELD_KEY_TABLE]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', 1, 'j', CAST(NULL AS INT))] FROM ice.ns.n ORDER BY id",
        &["1|5", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT map(named_struct('k', 1, 'j', CAST(NULL AS INT)), 5, named_struct('k', 1, 'j', 2), 6)[named_struct('k', 1, 'j', CAST(NULL AS INT))]",
        &["5"],
    )
    .await;
}

#[tokio::test]
async fn a_null_map_and_a_missing_struct_key_answer_null() {
    let door = spark_door(&[&PAIR_KEY_TABLE, &NULL_FIELD_KEY_TABLE]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', 1, 'j', 2)] FROM ice.ns.n ORDER BY id",
        &["1|6", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', 9, 'j', 9)] FROM ice.ns.c ORDER BY id",
        &["1|NULL", "2|NULL"],
    )
    .await;
}

#[tokio::test]
async fn a_struct_key_is_cast_to_the_map_key_type_by_position_like_spark() {
    let door = spark_door(&[&PAIR_KEY_TABLE]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('j', 2, 'k', 1)] FROM ice.ns.c ORDER BY id",
        &["1|NULL", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m[named_struct('j', 1, 'k', 2)] FROM ice.ns.c ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m[named_struct('a', 1, 'b', 2)] FROM ice.ns.c ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', 1L, 'j', 2L)] FROM ice.ns.c ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
}

#[tokio::test]
async fn a_nested_struct_key_is_cast_by_position_at_every_level() {
    let door = spark_door(&[]).await;
    expect(
        &door,
        "SELECT map(named_struct('a', named_struct('x', 1, 'y', 2)), 7)[named_struct('b', named_struct('y', 1, 'x', 2))]",
        &["7"],
    )
    .await;
    expect(
        &door,
        "SELECT map(named_struct('a', named_struct('x', 1, 'y', 2)), 7)[named_struct('b', named_struct('y', 2, 'x', 1))]",
        &["NULL"],
    )
    .await;
}

#[tokio::test]
async fn a_null_struct_key_answers_null() {
    let door = spark_door(&[&PAIR_KEY_TABLE]).await;
    expect(
        &door,
        "SELECT id, m[CAST(NULL AS STRUCT<k: INT, j: INT>)] FROM ice.ns.c ORDER BY id",
        &["1|NULL", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m[NULL] FROM ice.ns.c ORDER BY id",
        &["1|NULL", "2|NULL"],
    )
    .await;
}

#[tokio::test]
async fn a_struct_key_with_another_field_count_refuses_like_spark() {
    let door = spark_door(&[&PAIR_KEY_TABLE]).await;
    let sql = "SELECT id, m[named_struct('k', 1)] FROM ice.ns.c ORDER BY id";
    let error = answer(&door.session, sql)
        .await
        .expect_err("a key of another shape refuses like Spark");
    for fragment in [
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]",
        "The second parameter requires the \"STRUCT<k: INT, j: INT>\" type",
        "has the type \"STRUCT<k: INT",
    ] {
        assert!(error.contains(fragment), "{sql}: {error}");
    }
}

#[tokio::test]
async fn a_struct_key_subscript_nests_inside_coalesce_and_a_where_clause() {
    let door = spark_door(&[&PAIR_KEY_TABLE]).await;
    expect(
        &door,
        "SELECT id, coalesce(m[named_struct('k', 1, 'j', 2)], -1) FROM ice.ns.c ORDER BY id",
        &["1|7", "2|-1"],
    )
    .await;
    expect(
        &door,
        "SELECT id FROM ice.ns.c WHERE m[named_struct('k', 1, 'j', 2)] = 7",
        &["1"],
    )
    .await;
    expect(
        &door,
        "SELECT id FROM ice.ns.c WHERE m[named_struct('k', 1, 'j', 5)] IS NOT NULL",
        &["2"],
    )
    .await;
}

#[tokio::test]
async fn a_per_row_struct_key_answers_sparks_values() {
    let door = spark_door(&[&PAIR_KEY_TABLE]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', id, 'j', 2)] FROM ice.ns.c ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
}

#[tokio::test]
async fn an_array_key_subscript_answers_sparks_values() {
    let door = spark_door(&[]).await;
    expect(
        &door,
        "SELECT map(array(1, 2), 7, array(3), 8)[array(3)]",
        &["8"],
    )
    .await;
    expect(
        &door,
        "SELECT map(array(1, 2), 7, array(3), 8)[array(1, 2)]",
        &["7"],
    )
    .await;
}

#[tokio::test]
async fn element_at_and_a_scalar_key_subscript_stay_sparks_values() {
    let door = spark_door(&[&PAIR_KEY_TABLE, &SCALAR_KEY_TABLE]).await;
    expect(
        &door,
        "SELECT id, element_at(m, named_struct('k', 1, 'j', 2)) FROM ice.ns.c ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m['a'] FROM ice.ns.e ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
}

#[tokio::test]
async fn the_ansi_door_answers_a_struct_key_subscript_over_a_literal_map() {
    let door = ansi_door();
    expect(
        &door,
        &format!("SELECT m[named_struct('k', 1, 'j', 2)] FROM ({TWO_ENTRY_LITERAL})"),
        &["7"],
    )
    .await;
    expect(
        &door,
        &format!("SELECT m[named_struct('k', 3, 'j', 2)] FROM ({TWO_ENTRY_LITERAL})"),
        &["8"],
    )
    .await;
    expect(
        &door,
        "SELECT m[named_struct('k', 1, 'j', 2)] FROM (SELECT map(named_struct('k', 1, 'j', 2), 7) AS m)",
        &["7"],
    )
    .await;
}

#[tokio::test]
async fn a_struct_inside_an_array_key_is_cast_by_position() {
    let door = spark_door(&[]).await;
    expect(
        &door,
        &format!("{ARRAY_OF_STRUCT_LITERAL}[array(named_struct('j', 1, 'k', 2))]"),
        &["7"],
    )
    .await;
    expect(
        &door,
        &format!("{ARRAY_OF_STRUCT_LITERAL}[array(named_struct('j', 2, 'k', 1))]"),
        &["NULL"],
    )
    .await;
    expect(
        &door,
        &format!("{ARRAY_OF_STRUCT_LITERAL}[array(named_struct('k', 1L, 'j', 2L))]"),
        &["7"],
    )
    .await;
    expect(
        &door,
        &format!(
            "{ARRAY_OF_STRUCT_LITERAL}[array(named_struct('k', 1, 'j', 2), named_struct('k', 3, 'j', 4))]"
        ),
        &["NULL"],
    )
    .await;
}

#[tokio::test]
async fn a_struct_inside_an_array_inside_a_struct_key_is_cast_by_position() {
    let door = spark_door(&[]).await;
    expect(
        &door,
        &format!(
            "{STRUCT_OF_ARRAY_LITERAL}[named_struct('a', array(named_struct('y', 1, 'x', 2)))]"
        ),
        &["7"],
    )
    .await;
    expect(
        &door,
        &format!(
            "{STRUCT_OF_ARRAY_LITERAL}[named_struct('a', array(named_struct('y', 2, 'x', 1)))]"
        ),
        &["NULL"],
    )
    .await;
}

#[tokio::test]
async fn a_struct_key_leaf_of_another_kind_refuses_like_spark() {
    let door = spark_door(&[&PAIR_KEY_TABLE]).await;
    for (key, given) in [
        ("named_struct('k', '1', 'j', 2)", "\"STRUCT<k: STRING"),
        (
            "named_struct('k', DATE'2024-01-01', 'j', 2)",
            "\"STRUCT<k: DATE",
        ),
        ("named_struct('k', true, 'j', 2)", "\"STRUCT<k: BOOLEAN"),
    ] {
        expect_refusal(
            &door,
            &format!("SELECT id, m[{key}] FROM ice.ns.c ORDER BY id"),
            &[
                "The second parameter requires the \"STRUCT<k: INT, j: INT>\" type",
                given,
            ],
        )
        .await;
    }
}

#[tokio::test]
async fn a_numeric_or_null_struct_key_leaf_is_cast_like_spark() {
    let door = spark_door(&[&PAIR_KEY_TABLE]).await;
    for key in [
        "named_struct('k', CAST(1 AS DECIMAL(5,2)), 'j', 2)",
        "named_struct('k', 1.0, 'j', 2)",
        "named_struct('k', CAST(1 AS DOUBLE), 'j', 2)",
        "named_struct('k', CAST(1 AS TINYINT), 'j', 2)",
    ] {
        expect(
            &door,
            &format!("SELECT id, m[{key}] FROM ice.ns.c ORDER BY id"),
            &["1|7", "2|NULL"],
        )
        .await;
    }
    expect(
        &door,
        "SELECT id, m[named_struct('k', NULL, 'j', 2)] FROM ice.ns.c ORDER BY id",
        &["1|NULL", "2|NULL"],
    )
    .await;
}

#[tokio::test]
async fn a_string_struct_key_leaf_answers_like_spark() {
    let door = spark_door(&[&STRING_FIELD_KEY_TABLE]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', 'a', 'j', 2)] FROM ice.ns.s ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m[named_struct('x', 'a', 'y', 5)] FROM ice.ns.s ORDER BY id",
        &["1|NULL", "2|9"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', 1, 'j', 2)] FROM ice.ns.s ORDER BY id",
        &["1|NULL", "2|NULL"],
    )
    .await;
}

#[tokio::test]
async fn a_nested_key_refusal_names_the_types_in_spark_notation() {
    let door = spark_door(&[]).await;
    expect_refusal(
        &door,
        &format!("{ARRAY_OF_STRUCT_LITERAL}[array(named_struct('k', 1))]"),
        &[
            "The second parameter requires the \"ARRAY<STRUCT<k: INT",
            "\"ARRAY<STRUCT<k: INT",
        ],
    )
    .await;
    expect_refusal(
        &door,
        &format!("{ARRAY_OF_STRUCT_LITERAL}[array(named_struct('k', '1', 'j', 2))]"),
        &[
            "The second parameter requires the \"ARRAY<STRUCT<k: INT",
            "\"ARRAY<STRUCT<k: STRING",
        ],
    )
    .await;
    expect_refusal(
        &door,
        &format!(
            "{STRUCT_OF_ARRAY_LITERAL}[named_struct('a', array(named_struct('x', '1', 'y', 2)))]"
        ),
        &[
            "The second parameter requires the \"STRUCT<a: ARRAY<STRUCT<x: INT",
            "\"STRUCT<a: ARRAY<STRUCT<x: STRING",
        ],
    )
    .await;
}

#[tokio::test]
async fn the_ansi_door_casts_a_struct_inside_an_array_key_by_position() {
    let door = ansi_door();
    let literal = "SELECT map(make_array(named_struct('k', 1, 'j', 2)), 7)";
    expect(
        &door,
        &format!("{literal}[make_array(named_struct('j', 1, 'k', 2))]"),
        &["7"],
    )
    .await;
    expect(
        &door,
        &format!("{literal}[make_array(named_struct('j', 2, 'k', 1))]"),
        &["NULL"],
    )
    .await;
    expect_refusal(
        &door,
        &format!("SELECT m[named_struct('k', '1', 'j', 2)] FROM ({TWO_ENTRY_LITERAL})"),
        &["\"STRUCT<k: STRING"],
    )
    .await;
}

#[tokio::test]
async fn a_floating_leaf_against_a_string_field_is_cast_with_sparks_text() {
    let door = spark_door(&[&LEAF_TABLES]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', 1e20D, 'j', 2)] FROM ice.ns.ls ORDER BY id",
        &["1|13", "2|NULL"],
    )
    .await;
    for (map_key, lookup) in [
        ("'1.2345678E7'", "12345678.0D"),
        ("'1.0E-5'", "0.00001D"),
        ("'1.0E10'", "CAST(1e10 AS FLOAT)"),
    ] {
        expect(
            &door,
            &format!(
                "SELECT map(named_struct('k', {map_key}, 'j', 2), 7)[named_struct('k', {lookup}, 'j', 2)]"
            ),
            &["7"],
        )
        .await;
    }
}

#[tokio::test]
async fn any_leaf_against_a_string_field_is_cast_like_spark() {
    let door = spark_door(&[&LEAF_TABLES]).await;
    for (leaf, answer) in [
        ("DATE'2024-01-01'", "1|8"),
        ("true", "1|9"),
        ("TIMESTAMP '2024-01-01 00:00:00'", "1|14"),
        ("X'61'", "1|7"),
        ("1.0D", "1|10"),
        ("1.0", "1|10"),
        ("1", "1|11"),
        ("CAST(1.0 AS FLOAT)", "1|10"),
    ] {
        expect(
            &door,
            &format!("SELECT id, m[named_struct('k', {leaf}, 'j', 2)] FROM ice.ns.ls ORDER BY id"),
            &[answer, "2|NULL"],
        )
        .await;
    }
    expect(
        &door,
        "SELECT map(named_struct('k', 'true', 'j', 2), 9)[named_struct('k', true, 'j', 2)]",
        &["9"],
    )
    .await;
    expect(
        &door,
        "SELECT map(named_struct('k', '2024-01-01', 'j', 2), 8)[named_struct('k', DATE '2024-01-01', 'j', 2)]",
        &["8"],
    )
    .await;
}

#[tokio::test]
async fn date_timestamp_and_timestamp_ntz_leaves_are_cast_among_themselves_like_spark() {
    let door = spark_door(&[&LEAF_TABLES]).await;
    for (table, field, leaf) in [
        ("dt", "d", "DATE'2024-01-01'"),
        ("dt", "d", "TIMESTAMP '2024-01-01 00:00:00'"),
        ("ts", "t", "TIMESTAMP '2024-01-01 00:00:00'"),
        ("ts", "t", "DATE '2024-01-01'"),
        ("ts", "t", "TIMESTAMP_NTZ '2024-01-01 00:00:00'"),
        ("tsn", "t", "TIMESTAMP_NTZ '2024-01-01 00:00:00'"),
        ("tsn", "t", "TIMESTAMP '2024-01-01 00:00:00'"),
    ] {
        expect(
            &door,
            &format!(
                "SELECT id, m[named_struct('{field}', {leaf}, 'j', 2)] FROM ice.ns.{table} ORDER BY id"
            ),
            &["1|7", "2|NULL"],
        )
        .await;
    }
    expect(
        &door,
        "SELECT id, m[named_struct('d', date_add(DATE'2023-12-31', id), 'j', 2)] FROM ice.ns.dt ORDER BY id",
        &["1|7", "2|8"],
    )
    .await;
    expect(
        &door,
        "SELECT map(named_struct('d', DATE '2024-01-01', 'j', 2), 7)[named_struct('d', TIMESTAMP '2024-01-01 10:00:00', 'j', 2)]",
        &["7"],
    )
    .await;
}

#[tokio::test]
async fn equal_boolean_and_binary_leaves_answer_and_other_kinds_refuse_like_spark() {
    let door = spark_door(&[&LEAF_TABLES]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('b', true, 'j', 2)] FROM ice.ns.bo ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m[named_struct('b', X'01', 'j', 2)] FROM ice.ns.bi ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    for (table, key, given) in [
        ("bo", "named_struct('b', 1, 'j', 2)", "\"STRUCT<b: INT"),
        (
            "bo",
            "named_struct('b', 'true', 'j', 2)",
            "\"STRUCT<b: STRING",
        ),
        ("bi", "named_struct('b', 'a', 'j', 2)", "\"STRUCT<b: STRING"),
        (
            "dt",
            "named_struct('d', '2024-01-01', 'j', 2)",
            "\"STRUCT<d: STRING",
        ),
        (
            "ts",
            "named_struct('t', '2024-01-01 00:00:00', 'j', 2)",
            "\"STRUCT<t: STRING",
        ),
    ] {
        expect_refusal(
            &door,
            &format!("SELECT id, m[{key}] FROM ice.ns.{table} ORDER BY id"),
            &[given],
        )
        .await;
    }
}

#[tokio::test]
async fn a_null_leaf_for_a_required_key_field_matches_nothing() {
    let door = spark_door(&[&LEAF_TABLES]).await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', CAST(NULL AS INT), 'j', 2)] FROM ice.ns.nn ORDER BY id",
        &["1|NULL", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m[named_struct('k', CASE WHEN id = 1 THEN 1 END, 'j', 2)] FROM ice.ns.nn ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
    expect(
        &door,
        "SELECT id, m[named_struct('j', 1L, 'k', 2L)] FROM ice.ns.nn ORDER BY id",
        &["1|7", "2|NULL"],
    )
    .await;
}

#[tokio::test]
async fn decimal_and_deep_struct_keys_stay_sparks_values() {
    let door = spark_door(&[&LEAF_TABLES, &ARRAY_STRUCT_TABLE]).await;
    for (leaf, answer) in [
        ("CAST(1.505 AS DECIMAL(6,3))", ["1|8", "2|NULL"]),
        ("CAST(1.504 AS DECIMAL(6,3))", ["1|7", "2|NULL"]),
        ("CAST(123.45 AS DECIMAL(10,2))", ["1|NULL", "2|9"]),
        ("1.51D", ["1|8", "2|NULL"]),
    ] {
        expect(
            &door,
            &format!("SELECT id, m[named_struct('k', {leaf}, 'j', 2)] FROM ice.ns.dec ORDER BY id"),
            &answer,
        )
        .await;
    }
    for (key, answer) in [
        (
            "named_struct('A', 1, 'S', named_struct('X', 2, 'T', named_struct('Q', 3, 'P', 4)))",
            ["1|7", "2|NULL"],
        ),
        (
            "named_struct('a', 1, 's', named_struct('x', 2, 't', CAST(NULL AS STRUCT<q: INT, p: INT>)))",
            ["1|8", "2|NULL"],
        ),
        (
            "named_struct('a', 1L, 's', CAST(NULL AS STRUCT<x: BIGINT, t: STRUCT<p: BIGINT, q: BIGINT>>))",
            ["1|9", "2|NULL"],
        ),
        (
            "named_struct('a', id, 's', named_struct('x', id + 1, 't', named_struct('p', id + 2, 'q', 4)))",
            ["1|7", "2|12"],
        ),
    ] {
        expect(
            &door,
            &format!("SELECT id, m[{key}] FROM ice.ns.deep ORDER BY id"),
            &answer,
        )
        .await;
    }
    for key in [
        "array(named_struct('k', 1, 'j', 2), CAST(NULL AS STRUCT<k: INT, j: INT>))",
        "array(named_struct('k', 1L, 'j', 2L), CAST(NULL AS STRUCT<k: BIGINT, j: BIGINT>))",
        "array(named_struct('x', 1, 'y', 2), CAST(NULL AS STRUCT<x: INT, y: INT>))",
    ] {
        expect(
            &door,
            &format!("SELECT id, m[{key}] FROM ice.ns.as_ ORDER BY id"),
            &["1|10", "2|NULL"],
        )
        .await;
    }
}
