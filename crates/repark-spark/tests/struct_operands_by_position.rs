use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const AMBIGUOUS: &str = "[AMBIGUOUS_REFERENCE_TO_FIELDS] Ambiguous reference to the field";
const V2: &str = " USING iceberg TBLPROPERTIES ('format-version'='2')";
const MX: [&str; 2] = [
    "CREATE TABLE {t} (id INT, v INT, l ARRAY<INT>, m MAP<STRING, INT>, s STRUCT<x: INT, y: INT>)",
    "INSERT INTO {t} VALUES (1, 1, array(10, 20), map('a', 7), named_struct('x', 8, 'y', 9))",
];
const UV: [&str; 2] = [
    "CREATE TABLE {t} (id INT, v INT)",
    "INSERT INTO {t} VALUES (1, 1), (2, 2)",
];
const SA: [&str; 2] = [
    "CREATE TABLE {t} (id INT, s STRUCT<a: INT>)",
    "INSERT INTO {t} VALUES (0, named_struct('a', 0))",
];

const SUBSCRIPT_NAMES: &[(&str, Result<&[&str], &str>)] = &[
    ("SELECT struct(m['a']) FROM {t}", Ok(&["{a: 7}"])),
    ("SELECT struct(s['x']) FROM {t}", Ok(&["{x: 8}"])),
    (
        "SELECT struct(m['a'], s['x'], l[0]) FROM {t}",
        Ok(&["{a: 7, x: 8, col3: 10}"]),
    ),
    ("SELECT struct(map('a', 1)['a']) FROM {t}", Ok(&["{a: 1}"])),
    (
        "SELECT struct(named_struct('x', 1).x) FROM {t}",
        Ok(&["{x: 1}"]),
    ),
    ("SELECT struct(l[1 - 1]) FROM {t}", Ok(&["{col1: 10}"])),
    ("SELECT struct(m['a']).a FROM {t}", Ok(&["7"])),
    ("SELECT struct(s['x']).x FROM {t}", Ok(&["8"])),
    ("SELECT struct(map('a', 7)['a']).a FROM {t}", Ok(&["7"])),
    ("SELECT struct(l[0], 1).col1 FROM {t}", Ok(&["10"])),
    ("SELECT struct(m['a'], 2 AS a).a FROM {t}", Err(AMBIGUOUS)),
    ("SELECT struct(s['x'], 5 AS x).x FROM {t}", Err(AMBIGUOUS)),
    (
        "SELECT struct(map('a', 7)['a'], 2 AS a).a FROM {t}",
        Err(AMBIGUOUS),
    ),
    (
        "SELECT struct(m['a'], map('a', 3)['a']).a FROM {t}",
        Err(AMBIGUOUS),
    ),
];

const SUBSCRIPT_WRITES: &[(&str, Option<&str>, &[&str])] = &[
    (
        "INSERT INTO {t} SELECT 50, struct(m['a'], 2 AS a).a, l, m, s FROM {t}",
        Some(AMBIGUOUS),
        &["1, 1"],
    ),
    (
        "INSERT INTO {t} SELECT 51, struct(s['x'], 5 AS x).x, l, m, s FROM {t}",
        Some(AMBIGUOUS),
        &["1, 1"],
    ),
    (
        "UPDATE {t} SET v = struct(map('a', 7)['a'], 2 AS a).a + 100",
        Some(AMBIGUOUS),
        &["1, 1"],
    ),
    (
        "INSERT INTO {t} SELECT 52, struct(m['a']).a, l, m, s FROM {t}",
        None,
        &["1, 1", "52, 7"],
    ),
    (
        "UPDATE {t} SET v = struct(m['a']).a + 100",
        None,
        &["1, 107"],
    ),
];

const POSITIONAL: &[(&str, Option<&[&str]>)] = &[
    (
        "SELECT id, struct(id, v) = struct(1, 1) FROM {t}",
        Some(&["1, true", "2, false"]),
    ),
    (
        "SELECT id, struct(id, v) != struct(1, 1) FROM {t}",
        Some(&["1, false", "2, true"]),
    ),
    (
        "SELECT id, struct(id, v) <=> struct(1, 1) FROM {t}",
        Some(&["1, true", "2, false"]),
    ),
    (
        "SELECT id, struct(id, v) < struct(2, 0) FROM {t}",
        Some(&["1, true", "2, false"]),
    ),
    (
        "SELECT id, struct(id, v) IN (struct(1, 1), struct(5, 5)) FROM {t}",
        Some(&["1, true", "2, false"]),
    ),
    (
        "SELECT id, struct(id, v) NOT IN (struct(1, 1)) FROM {t}",
        Some(&["1, false", "2, true"]),
    ),
    (
        "SELECT id, struct(id, v) = CAST(NULL AS STRUCT<a: INT, b: INT>) FROM {t}",
        Some(&["1, null", "2, null"]),
    ),
    (
        "SELECT named_struct('a', 1) = named_struct('b', 1)",
        Some(&["true"]),
    ),
    (
        "SELECT named_struct('a', 1, 'b', 2) = named_struct('b', 1, 'a', 2)",
        Some(&["true"]),
    ),
    (
        "SELECT named_struct('a', named_struct('x', 1)) = named_struct('b', named_struct('y', 1))",
        Some(&["true"]),
    ),
    (
        "SELECT struct(id) AS r FROM {t} UNION ALL SELECT struct(7)",
        Some(&["{id: 1}", "{id: 2}", "{id: 7}"]),
    ),
    (
        "SELECT struct(id) AS r FROM {t} UNION SELECT struct(1)",
        Some(&["{id: 1}", "{id: 2}"]),
    ),
    (
        "SELECT named_struct('a', 1) AS r UNION ALL SELECT named_struct('b', 2)",
        Some(&["{a: 1}", "{a: 2}"]),
    ),
    (
        "SELECT struct(id) AS r FROM {t} UNION ALL SELECT struct(7) UNION ALL SELECT named_struct('z', 8)",
        Some(&["{id: 1}", "{id: 2}", "{id: 7}", "{id: 8}"]),
    ),
    ("SELECT struct(1, 2) = struct(1)", None),
    ("SELECT struct(1) = struct(array(1))", None),
    ("SELECT struct(1, 2) IN (struct(1))", None),
    ("SELECT struct(1) AS r UNION ALL SELECT struct(1, 2)", None),
];

const POSITIONAL_WRITES: &[(&[&str; 2], &str, bool, &[&str])] = &[
    (
        &UV,
        "DELETE FROM {t} WHERE struct(id, v) = struct(1, 1)",
        true,
        &["2, 2"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE struct(id, v) IN (struct(1, 1))",
        true,
        &["2, 2"],
    ),
    (
        &UV,
        "UPDATE {t} SET v = 50 WHERE struct(id, v) = struct(2, 2)",
        true,
        &["1, 1", "2, 50"],
    ),
    (
        &UV,
        "UPDATE {t} SET v = 60 WHERE struct(id, v) <=> struct(1, 1)",
        true,
        &["1, 60", "2, 2"],
    ),
    (
        &SA,
        "INSERT INTO {t} SELECT 1, struct(id) FROM (SELECT 5 AS id) UNION ALL SELECT 2, struct(7)",
        true,
        &["0, {a: 0}", "1, {a: 5}", "2, {a: 7}"],
    ),
    (
        &SA,
        "INSERT INTO {t} SELECT 1, named_struct('q', 5) UNION ALL SELECT 2, named_struct('r', 7)",
        true,
        &["0, {a: 0}", "1, {a: 5}", "2, {a: 7}"],
    ),
    (
        &SA,
        "INSERT INTO {t} SELECT 6, s FROM {t} WHERE s = struct(0)",
        true,
        &["0, {a: 0}", "6, {a: 0}"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE struct(id, v) = struct(1)",
        false,
        &["1, 1", "2, 2"],
    ),
];

async fn catalog_session() -> (ReparkSession, TempDir) {
    let session = ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .build()
        .expect("session");
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    answer(&session, "CREATE NAMESPACE ice.ns")
        .await
        .expect("namespace");
    (session, warehouse)
}

fn lines(batches: &[RecordBatch]) -> Vec<String> {
    let options = FormatOptions::default().with_null("null");
    let mut rows = Vec::new();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options).expect("formatter"))
            .collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            let cells = formatters
                .iter()
                .map(|formatter| formatter.value(row).to_string())
                .collect::<Vec<_>>();
            rows.push(cells.join(", "));
        }
    }
    rows.sort();
    rows
}

async fn answer(session: &ReparkSession, sql: &str) -> Result<Vec<String>, String> {
    let frame = session.sql(sql).await.map_err(|error| error.to_string())?;
    let batches = frame.collect().await.map_err(|error| error.to_string())?;
    Ok(lines(&batches))
}

async fn create(session: &ReparkSession, table: &str, fixture: &[&str; 2]) {
    answer(
        session,
        &format!("{}{V2}", fixture[0].replace("{t}", table)),
    )
    .await
    .expect("create");
    answer(session, &fixture[1].replace("{t}", table))
        .await
        .expect("seed");
}

fn sorted(rows: &[&str]) -> Vec<String> {
    let mut rows = rows.iter().map(ToString::to_string).collect::<Vec<_>>();
    rows.sort();
    rows
}

#[tokio::test]
async fn a_string_subscript_names_its_struct_field_and_a_colliding_name_refuses() {
    let (session, _warehouse) = catalog_session().await;
    let table = "ice.ns.subscript_names";
    create(&session, table, &MX).await;
    for (sql, want) in SUBSCRIPT_NAMES {
        let sql = sql.replace("{t}", table);
        let got = answer(&session, &sql).await;
        match want {
            Ok(rows) => assert_eq!(got, Ok(sorted(rows)), "{sql}"),
            Err(message) => {
                let error = got.expect_err(&sql);
                assert!(error.contains(message), "{sql}: {error}");
            }
        }
    }
    for (index, (sql, refusal, after)) in SUBSCRIPT_WRITES.iter().enumerate() {
        let target = format!("ice.ns.subscript_write_{index}");
        create(&session, &target, &MX).await;
        let sql = sql.replace("{t}", &target);
        let got = answer(&session, &sql).await;
        match refusal {
            None => assert!(got.is_ok(), "{sql}: {got:?}"),
            Some(message) => {
                let error = got.expect_err(&sql);
                assert!(error.contains(message), "{sql}: {error}");
            }
        }
        let readback = answer(&session, &format!("SELECT id, v FROM {target}")).await;
        assert_eq!(readback, Ok(sorted(after)), "{sql}");
    }
}

#[tokio::test]
async fn struct_operands_compare_and_union_by_position() {
    let (session, _warehouse) = catalog_session().await;
    let table = "ice.ns.positional";
    create(&session, table, &UV).await;
    for (sql, want) in POSITIONAL {
        let sql = sql.replace("{t}", table);
        let got = answer(&session, &sql).await;
        match want {
            Some(rows) => assert_eq!(got, Ok(sorted(rows)), "{sql}"),
            None => assert!(got.is_err(), "{sql}: {got:?}"),
        }
    }
    for (index, (fixture, sql, writes, after)) in POSITIONAL_WRITES.iter().enumerate() {
        let target = format!("ice.ns.positional_write_{index}");
        create(&session, &target, fixture).await;
        let sql = sql.replace("{t}", &target);
        let got = answer(&session, &sql).await;
        assert_eq!(got.is_ok(), *writes, "{sql}: {got:?}");
        let readback = answer(&session, &format!("SELECT * FROM {target}")).await;
        assert_eq!(readback, Ok(sorted(after)), "{sql}");
    }
}

const BINARY_OP: &str = "[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES]";
const DATA_DIFF: &str = "[DATATYPE_MISMATCH.DATA_DIFF_TYPES]";
const UNION_TYPES: &str = "[INCOMPATIBLE_COLUMN_TYPE]";

const FIELD_TYPES: &[(&str, Result<&[&str], &str>)] = &[
    (
        "SELECT struct(1) = struct(CAST(1 AS BIGINT))",
        Ok(&["true"]),
    ),
    ("SELECT struct(1) = struct(1.0D)", Ok(&["true"])),
    (
        "SELECT struct(1) < struct(CAST(1 AS BIGINT))",
        Ok(&["false"]),
    ),
    (
        "SELECT struct(1) <=> struct(CAST(1 AS BIGINT))",
        Ok(&["true"]),
    ),
    (
        "SELECT named_struct('a', 1) = named_struct('a', CAST(1 AS BIGINT))",
        Ok(&["true"]),
    ),
    (
        "SELECT named_struct('A', 1) = named_struct('a', CAST(1 AS BIGINT))",
        Ok(&["true"]),
    ),
    (
        "SELECT named_struct('a', struct(1)) = named_struct('a', struct(CAST(1 AS BIGINT)))",
        Ok(&["true"]),
    ),
    (
        "SELECT struct(CAST(1 AS DECIMAL(5,2))) = struct(CAST(1 AS DECIMAL(5,2)))",
        Ok(&["true"]),
    ),
    (
        "SELECT struct(1) = struct(CAST(1 AS DECIMAL(12,2)))",
        Ok(&["true"]),
    ),
    (
        "SELECT struct(CAST(1 AS FLOAT)) = struct(1.0D)",
        Ok(&["true"]),
    ),
    (
        "SELECT struct(array(1)) = struct(array(CAST(1 AS BIGINT)))",
        Ok(&["true"]),
    ),
    (
        "SELECT id, named_struct('id', id) = named_struct('id', CAST(1 AS BIGINT)) FROM {t}",
        Ok(&["1, true", "2, false"]),
    ),
    ("SELECT struct(1) IN (struct('1'))", Ok(&["true"])),
    (
        "SELECT struct(1) IN (struct(CAST(1 AS DECIMAL(5,2))))",
        Ok(&["true"]),
    ),
    (
        "SELECT struct(1) AS r UNION ALL SELECT struct(CAST(1 AS BIGINT))",
        Ok(&["{col1: 1}", "{col1: 1}"]),
    ),
    ("SELECT struct(1) = struct('1')", Err(BINARY_OP)),
    (
        "SELECT struct(CAST(1 AS DECIMAL(5,2))) = struct(CAST(1 AS DECIMAL(7,2)))",
        Err(BINARY_OP),
    ),
    (
        "SELECT struct(1) = struct(CAST(1 AS DECIMAL(5,2)))",
        Err(BINARY_OP),
    ),
    (
        "SELECT struct(1.0D) = struct(CAST(1 AS DECIMAL(5,2)))",
        Err(BINARY_OP),
    ),
    (
        "SELECT id, struct(id) = struct(CAST(1 AS BIGINT)) FROM {t}",
        Err(BINARY_OP),
    ),
    (
        "SELECT id, struct(id, v) = struct(1, CAST(1 AS BIGINT)) FROM {t}",
        Err(BINARY_OP),
    ),
    (
        "SELECT id, struct(id, v) = CAST(NULL AS STRUCT<a: BIGINT, b: INT>) FROM {t}",
        Err(BINARY_OP),
    ),
    (
        "SELECT id, named_struct('id', id) = named_struct('id', '1') FROM {t}",
        Err(BINARY_OP),
    ),
    (
        "SELECT id, struct(id) IN (struct(CAST(1 AS BIGINT))) FROM {t}",
        Err(DATA_DIFF),
    ),
    ("SELECT struct(1) IN (struct(array(1)))", Err(DATA_DIFF)),
    (
        "SELECT struct(id) AS r FROM {t} UNION ALL SELECT struct(CAST(1 AS BIGINT))",
        Err(UNION_TYPES),
    ),
    (
        "SELECT struct(id) AS r FROM {t} UNION ALL SELECT struct('1')",
        Err(UNION_TYPES),
    ),
    (
        "SELECT named_struct('a', 1) AS r UNION ALL SELECT named_struct('b', 1.0D)",
        Err(UNION_TYPES),
    ),
];

const FIELD_TYPE_WRITES: &[(&[&str; 2], &str, Option<&str>, &[&str])] = &[
    (
        &UV,
        "DELETE FROM {t} WHERE struct(id) = struct('1')",
        Some(BINARY_OP),
        &["1, 1", "2, 2"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE struct(id) = struct(CAST(1 AS BIGINT))",
        Some(BINARY_OP),
        &["1, 1", "2, 2"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE struct(id) = struct(1.0D)",
        Some(BINARY_OP),
        &["1, 1", "2, 2"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE named_struct('a', struct(id)) = named_struct('a', struct(CAST(1 AS BIGINT)))",
        Some(BINARY_OP),
        &["1, 1", "2, 2"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE (struct(id, v) <=> CAST(NULL AS STRUCT<a: BIGINT, b: INT>)) = false",
        Some(BINARY_OP),
        &["1, 1", "2, 2"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE named_struct('id', id) = named_struct('id', '1')",
        Some(BINARY_OP),
        &["1, 1", "2, 2"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE named_struct('id', id) = named_struct('id', CAST(1 AS DECIMAL(5,2)))",
        Some(BINARY_OP),
        &["1, 1", "2, 2"],
    ),
    (
        &UV,
        "UPDATE {t} SET v = 9 WHERE struct(id) = struct('1')",
        Some(BINARY_OP),
        &["1, 1", "2, 2"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE struct(id) IN (struct('1'))",
        Some(DATA_DIFF),
        &["1, 1", "2, 2"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE struct(id) IN (struct(CAST(1 AS BIGINT)))",
        Some(DATA_DIFF),
        &["1, 1", "2, 2"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE named_struct('id', id) = named_struct('id', CAST(1 AS BIGINT))",
        None,
        &["2, 2"],
    ),
    (
        &UV,
        "DELETE FROM {t} WHERE struct(id) = struct(1)",
        None,
        &["2, 2"],
    ),
    (
        &SA,
        "INSERT INTO {t} SELECT 1, struct(5) UNION ALL SELECT 2, struct(CAST(1 AS BIGINT))",
        None,
        &["0, {a: 0}", "1, {a: 5}", "2, {a: 1}"],
    ),
    (
        &SA,
        "INSERT INTO {t} SELECT 1, named_struct('q', 5) UNION ALL SELECT 2, named_struct('r', CAST(1 AS BIGINT))",
        Some(UNION_TYPES),
        &["0, {a: 0}"],
    ),
];

#[tokio::test]
async fn struct_operands_whose_field_types_spark_cannot_pair_refuse_and_write_nothing() {
    let (session, _warehouse) = catalog_session().await;
    let table = "ice.ns.field_types";
    create(&session, table, &UV).await;
    for (sql, want) in FIELD_TYPES {
        let sql = sql.replace("{t}", table);
        let got = answer(&session, &sql).await;
        match want {
            Ok(rows) => assert_eq!(got, Ok(sorted(rows)), "{sql}"),
            Err(class) => {
                let error = got.expect_err(&sql);
                assert!(error.contains(class), "{sql}: {error}");
            }
        }
    }
    for (index, (fixture, sql, refusal, after)) in FIELD_TYPE_WRITES.iter().enumerate() {
        let target = format!("ice.ns.field_type_write_{index}");
        create(&session, &target, fixture).await;
        let sql = sql.replace("{t}", &target);
        let got = answer(&session, &sql).await;
        match refusal {
            None => assert!(got.is_ok(), "{sql}: {got:?}"),
            Some(class) => {
                let error = got.expect_err(&sql);
                assert!(error.contains(class), "{sql}: {error}");
            }
        }
        let readback = answer(&session, &format!("SELECT * FROM {target}")).await;
        assert_eq!(readback, Ok(sorted(after)), "{sql}");
    }
}
