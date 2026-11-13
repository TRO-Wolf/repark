use std::collections::HashMap;
use std::path::{Path, PathBuf};

use pyo3::prelude::*;

use super::{copy_destination, session_sql_with_write_options};
use crate::exceptions::UnsupportedOperationException;
use crate::session::PyReparkSession;

const HEX: &str = "0123456789abcdef0123456789abcdef";

fn table_refusal(target: &str, workaround: &str) -> String {
    format!(
        "DataFrame.write and writeTo cannot write a repark.sql() frame into the table {target}: \
         the write would return without storing any rows, so nothing was written; {workaround} \
         (NATIVE-DATAFRAME-WRITE-SILENT-1)"
    )
}

fn append_refusal(target: &str) -> String {
    table_refusal(
        target,
        &format!(
            "run INSERT INTO {target} SELECT ... through repark.sql(...) and collect the result"
        ),
    )
}

fn overwrite_refusal(target: &str) -> String {
    table_refusal(
        target,
        &format!(
            "run INSERT OVERWRITE {target} SELECT ... through repark.sql(...) and collect the \
             result"
        ),
    )
}

fn dynamic_refusal(target: &str) -> String {
    table_refusal(
        target,
        &format!(
            "on an unpartitioned table, which is every table repark.sql(...) can create, \
             overwritePartitions replaces every row: run INSERT OVERWRITE {target} SELECT ... \
             through repark.sql(...) and collect the result"
        ),
    )
}

fn memory_overwrite_refusal(target: &str) -> String {
    table_refusal(
        target,
        &format!(
            "{target} is an in-memory table, where INSERT OVERWRITE is not implemented: run \
             CREATE OR REPLACE TABLE {target} AS SELECT ... through repark.sql(...) and collect \
             the result, which replaces the table with the query's columns and rows"
        ),
    )
}

fn path_refusal(path: &str) -> String {
    format!(
        "DataFrame.write cannot write a repark.sql() frame to the path {path}: the write would \
         return without storing any rows, so nothing was written; write the frame from a \
         ReparkSession (NATIVE-DATAFRAME-WRITE-SILENT-1)"
    )
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "repark-pending-write-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn run(session: &PyReparkSession, sql: &str) -> Vec<String> {
    let batches = session.runtime.block_on(async {
        let frame = session.session.sql(sql).await.expect("statement");
        frame.collect().await.expect("collect")
    });
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|column| {
                    arrow::util::display::array_value_to_string(column, row).expect("render cell")
                })
                .collect();
            rows.push(cells.join(","));
        }
    }
    rows.sort();
    rows
}

fn files_under(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
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

fn native_session(py: Python<'_>, warehouse: &Path) -> Py<PyReparkSession> {
    let session = Py::new(
        py,
        PyReparkSession::native(py, None, None, None, None).expect("native session"),
    )
    .expect("session object");
    let session_ref = session.borrow(py);
    let root = warehouse.join("ice");
    session_ref
        .register_memory_catalog(py, "ice", &root.to_string_lossy())
        .expect("memory catalog");
    session_ref
        .create_namespace(py, "ice", "ns", Some(&root.join("ns").to_string_lossy()))
        .expect("namespace");
    run(&session_ref, "CREATE TABLE ice.ns.t (id INT)");
    run(&session_ref, "INSERT INTO ice.ns.t VALUES (7), (8)");
    run(&session_ref, "CREATE TABLE m (id INT)");
    run(&session_ref, "INSERT INTO m VALUES (7), (8)");
    drop(session_ref);
    session
}

fn refusal(
    py: Python<'_>,
    session: &Py<PyReparkSession>,
    sql: &str,
    force_dynamic_overwrite: bool,
) -> String {
    let error = session_sql_with_write_options(
        py,
        session.borrow(py),
        sql,
        HashMap::new(),
        false,
        force_dynamic_overwrite,
        false,
    )
    .err()
    .expect("the pending write refuses");
    assert!(error.is_instance_of::<UnsupportedOperationException>(py));
    error.value(py).to_string()
}

#[test]
fn copy_destination_strips_only_an_exact_staging_segment() {
    let staged = |name: &str| format!("repark-staging-{HEX}-{name}");
    let unnamed = format!("repark-staging-{HEX}");
    for (raw, named) in [
        (format!("/w/{}", staged("dest")), "/w/dest"),
        (format!("/w/{}/", staged("dest")), "/w/dest"),
        (format!("/w/{}", staged("my-dir")), "/w/my-dir"),
        (staged("relx"), "relx"),
        (format!("a/b/{}", staged("relx")), "a/b/relx"),
        (staged("out"), "out"),
        (format!("{}/", staged("out")), "out"),
        (format!("data/{}", staged("out")), "data/out"),
        (format!("/w/{}", staged("out")), "/w/out"),
        (staged(".."), ".."),
        (staged(&unnamed), unnamed.as_str()),
        (unnamed.clone(), "."),
        (format!("{unnamed}/"), "."),
        (format!("/{unnamed}"), "/"),
    ] {
        assert_eq!(copy_destination(&raw), named, "{raw}");
    }
    for raw in [
        format!("/w/repark-staging-{}-dest", HEX.to_uppercase()),
        format!("/w/repark-staging-{}-dest", &HEX[1..]),
        format!("/w/repark-staging-{HEX}dest"),
        format!("/w/repark-staging-{HEX}-"),
        "/w/plain".to_string(),
        String::new(),
    ] {
        assert_eq!(copy_destination(&raw), raw);
    }
}

#[test]
fn native_pending_inserts_refuse_before_any_effect() {
    let warehouse = scratch("insert");
    Python::attach(|py| {
        let session = native_session(py, &warehouse);
        let table_dir = warehouse.join("ice").join("ns").join("t");
        let files_before = files_under(&table_dir);
        let cases = [
            (
                "INSERT INTO ice.ns.t SELECT 9",
                false,
                append_refusal("ice.ns.t"),
            ),
            (
                "INSERT OVERWRITE ice.ns.t SELECT 9",
                false,
                overwrite_refusal("ice.ns.t"),
            ),
            (
                "INSERT OVERWRITE ice.ns.t SELECT 9",
                true,
                dynamic_refusal("ice.ns.t"),
            ),
            (
                "INSERT INTO datafusion.public.m SELECT 9",
                false,
                append_refusal("datafusion.public.m"),
            ),
            (
                "INSERT OVERWRITE datafusion.public.m SELECT 9",
                false,
                memory_overwrite_refusal("datafusion.public.m"),
            ),
            (
                "INSERT OVERWRITE m SELECT 9",
                true,
                memory_overwrite_refusal("m"),
            ),
        ];
        for (sql, dynamic, expected) in cases {
            assert_eq!(refusal(py, &session, sql, dynamic), expected, "{sql}");
        }
        let session_ref = session.borrow(py);
        assert_eq!(run(&session_ref, "SELECT id FROM ice.ns.t"), ["7", "8"]);
        assert_eq!(run(&session_ref, "SELECT id FROM m"), ["7", "8"]);
        assert_eq!(files_under(&table_dir), files_before);
    });
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[test]
fn native_pending_copy_refuses_naming_the_destination_and_writes_nothing() {
    let warehouse = scratch("copy");
    Python::attach(|py| {
        let session = native_session(py, &warehouse);
        let staging = warehouse.join(format!("repark-staging-{HEX}-dest"));
        let staging_text = staging.to_string_lossy().to_string();
        session.borrow(py).note_local_write_root(&staging_text);
        let sql = format!("COPY (SELECT 1 AS id) TO '{staging_text}' STORED AS PARQUET");
        let expected = path_refusal(&warehouse.join("dest").to_string_lossy());
        assert_eq!(refusal(py, &session, &sql, false), expected);
        assert!(!staging.exists());
        assert!(!warehouse.join("dest").exists());
    });
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[test]
fn native_reads_and_spark_door_writes_pass_the_guard() {
    let warehouse = scratch("controls");
    Python::attach(|py| {
        let native = native_session(py, &warehouse);
        session_sql_with_write_options(
            py,
            native.borrow(py),
            "SELECT id FROM ice.ns.t",
            HashMap::new(),
            false,
            false,
            false,
        )
        .expect("a read is not a pending write");
        let spark = Py::new(
            py,
            PyReparkSession::new(py, None, None, None, None, None).expect("spark session"),
        )
        .expect("session object");
        let spark_ref = spark.borrow(py);
        spark_ref
            .register_memory_catalog(py, "sc", &warehouse.join("sc").to_string_lossy())
            .expect("memory catalog");
        for statement in [
            "CREATE NAMESPACE sc.ns",
            "CREATE TABLE sc.ns.t (id INT) USING iceberg",
        ] {
            spark_ref.sql(py, statement).expect("seed statement");
        }
        drop(spark_ref);
        for sql in [
            "INSERT INTO sc.ns.t SELECT 9",
            "INSERT OVERWRITE sc.ns.t SELECT 10",
        ] {
            session_sql_with_write_options(
                py,
                spark.borrow(py),
                sql,
                HashMap::new(),
                false,
                false,
                false,
            )
            .expect("the Spark door writes eagerly");
        }
        assert_eq!(run(&spark.borrow(py), "SELECT id FROM sc.ns.t"), ["10"]);
    });
    let _ = std::fs::remove_dir_all(&warehouse);
}
