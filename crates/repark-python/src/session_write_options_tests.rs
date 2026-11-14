use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{DmlStatement, LogicalPlan, LogicalPlanBuilder, WriteOp};
use pyo3::prelude::*;

use super::{copy_destination, pending_write_refusal, session_sql_with_write_options};
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

fn route(statement: &str) -> String {
    format!(
        "register the frame as a temporary view <view> and run {statement} through \
         repark.sql(...) and collect the result"
    )
}

fn plain_refusal(target: &str, statement: &str) -> String {
    table_refusal(target, &route(statement))
}

fn dynamic_refusal(target: &str, statement: &str) -> String {
    table_refusal(
        target,
        &format!(
            "on an unpartitioned table, which is every table repark.sql(...) can create, \
             overwritePartitions replaces every row: {}",
            route(statement)
        ),
    )
}

fn memory_overwrite_refusal(target: &str, statement: &str) -> String {
    table_refusal(
        target,
        &format!(
            "{target} is an in-memory table, where INSERT OVERWRITE is not implemented: {}, \
             which replaces the table with the query's columns and rows",
            route(statement)
        ),
    )
}

fn unwritable_refusal(target: &str, reason: &str) -> String {
    format!(
        "DataFrame.write and writeTo cannot write a repark.sql() frame into {target}: {reason}, \
         so nothing was written (NATIVE-DATAFRAME-WRITE-SILENT-1)"
    )
}

fn path_refusal(path: &str) -> String {
    format!(
        "DataFrame.write cannot write a repark.sql() frame to the path {path}: the write would \
         return without storing any rows, so nothing was written; convert the frame on a \
         ReparkSession and write the result: spark.createDataFrame(frame.collect(), \
         frame.schema), which holds every row in memory (NATIVE-DATAFRAME-WRITE-SILENT-1)"
    )
}

fn followed(message: &str, view: &str) -> String {
    let statement = message
        .split_once("and run ")
        .and_then(|(_, rest)| rest.split_once(" through repark.sql(...)"))
        .map(|(statement, _)| statement)
        .expect("the message names a statement");
    statement.replace("<view>", view)
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
    for statement in [
        "CREATE TABLE ice.ns.t (id INT, p INT)",
        "INSERT INTO ice.ns.t VALUES (7, 1), (8, 2)",
        "CREATE TABLE m (id INT, p INT)",
        "INSERT INTO m VALUES (7, 1), (8, 2)",
        "CREATE VIEW frame_rows AS SELECT 1 AS p, 9 AS id",
    ] {
        run(&session_ref, statement);
    }
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
        let by_name = |verb: &str, target: &str| {
            format!("{verb} {target} (\"id\", \"p\") SELECT \"id\", \"p\" FROM frame_rows")
        };
        let cases = [
            (
                "INSERT INTO ice.ns.t SELECT * FROM frame_rows".to_string(),
                false,
                plain_refusal("ice.ns.t", "INSERT INTO ice.ns.t SELECT * FROM <view>"),
            ),
            (
                by_name("INSERT INTO", "ice.ns.t"),
                false,
                plain_refusal(
                    "ice.ns.t",
                    "INSERT INTO ice.ns.t (p, id) SELECT p, id FROM <view>",
                ),
            ),
            (
                "INSERT OVERWRITE ice.ns.t SELECT * FROM frame_rows".to_string(),
                false,
                plain_refusal("ice.ns.t", "INSERT OVERWRITE ice.ns.t SELECT * FROM <view>"),
            ),
            (
                by_name("INSERT OVERWRITE", "ice.ns.t"),
                false,
                plain_refusal(
                    "ice.ns.t",
                    "INSERT OVERWRITE ice.ns.t (p, id) SELECT p, id FROM <view>",
                ),
            ),
            (
                by_name("INSERT OVERWRITE", "ice.ns.t"),
                true,
                dynamic_refusal(
                    "ice.ns.t",
                    "INSERT OVERWRITE ice.ns.t (p, id) SELECT p, id FROM <view>",
                ),
            ),
            (
                "INSERT INTO datafusion.public.m SELECT * FROM frame_rows".to_string(),
                false,
                plain_refusal(
                    "datafusion.public.m",
                    "INSERT INTO datafusion.public.m SELECT * FROM <view>",
                ),
            ),
            (
                "INSERT OVERWRITE datafusion.public.m SELECT * FROM frame_rows".to_string(),
                false,
                memory_overwrite_refusal(
                    "datafusion.public.m",
                    "CREATE OR REPLACE TABLE datafusion.public.m AS SELECT p AS id, id AS p FROM \
                     <view>",
                ),
            ),
            (
                by_name("INSERT OVERWRITE", "m"),
                true,
                memory_overwrite_refusal(
                    "m",
                    "CREATE OR REPLACE TABLE m AS SELECT id, p FROM <view>",
                ),
            ),
        ];
        for (sql, dynamic, expected) in &cases {
            assert_eq!(&refusal(py, &session, sql, *dynamic), expected, "{sql}");
        }
        let session_ref = session.borrow(py);
        assert_eq!(
            run(&session_ref, "SELECT id, p FROM ice.ns.t"),
            ["7,1", "8,2"]
        );
        assert_eq!(run(&session_ref, "SELECT id, p FROM m"), ["7,1", "8,2"]);
        assert_eq!(files_under(&table_dir), files_before);
    });
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[test]
fn each_named_table_statement_stores_the_writers_rows_when_followed() {
    let warehouse = scratch("followed");
    Python::attach(|py| {
        let session = native_session(py, &warehouse);
        let cases = [
            (
                "INSERT INTO ice.ns.t (\"id\", \"p\") SELECT \"id\", \"p\" FROM frame_rows",
                false,
                "ice.ns.t",
                vec!["7,1", "8,2", "9,1"],
            ),
            (
                "INSERT INTO ice.ns.t SELECT * FROM frame_rows",
                false,
                "ice.ns.t",
                vec!["1,9", "7,1", "8,2", "9,1"],
            ),
            (
                "INSERT OVERWRITE ice.ns.t (\"id\", \"p\") SELECT \"id\", \"p\" FROM frame_rows",
                true,
                "ice.ns.t",
                vec!["9,1"],
            ),
            (
                "INSERT OVERWRITE m SELECT * FROM frame_rows",
                false,
                "m",
                vec!["1,9"],
            ),
        ];
        for (sql, dynamic, target, expected) in cases {
            let message = refusal(py, &session, sql, dynamic);
            let session_ref = session.borrow(py);
            run(&session_ref, &followed(&message, "frame_rows"));
            assert_eq!(
                run(&session_ref, &format!("SELECT id, p FROM {target}")),
                expected,
                "{message}"
            );
        }
    });
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[test]
fn names_that_do_not_read_as_a_bare_column_are_quoted() {
    let warehouse = scratch("quoted");
    Python::attach(|py| {
        let session = native_session(py, &warehouse);
        let session_ref = session.borrow(py);
        for statement in [
            "CREATE TABLE q (\"Mixed Case\" INT, \"current_date\" INT, \"a\"\"b\" INT, \
             plain INT)",
            "CREATE VIEW odd_rows AS SELECT 4 AS plain, 3 AS \"a\"\"b\", 1 AS \"Mixed Case\", \
             2 AS \"current_date\"",
        ] {
            run(&session_ref, statement);
        }
        drop(session_ref);
        let sql = "INSERT INTO datafusion.public.q (\"Mixed Case\", \"current_date\", \
                   \"a\"\"b\", \"plain\") SELECT \"Mixed Case\", \"current_date\", \
                   \"a\"\"b\", \"plain\" FROM odd_rows";
        let message = refusal(py, &session, sql, false);
        let columns = "plain, \"a\"\"b\", \"Mixed Case\", \"current_date\"";
        assert_eq!(
            message,
            plain_refusal(
                "datafusion.public.q",
                &format!(
                    "INSERT INTO datafusion.public.q ({columns}) SELECT {columns} FROM <view>"
                ),
            )
        );
        let session_ref = session.borrow(py);
        run(&session_ref, &followed(&message, "odd_rows"));
        assert_eq!(
            run(
                &session_ref,
                "SELECT \"Mixed Case\", \"current_date\", \"a\"\"b\", plain FROM q"
            ),
            ["1,2,3,4"]
        );
    });
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[test]
fn a_native_view_refuses_without_naming_a_statement() {
    let warehouse = scratch("view");
    Python::attach(|py| {
        let session = native_session(py, &warehouse);
        run(
            &session.borrow(py),
            "CREATE VIEW v AS SELECT 7 AS id, 1 AS p",
        );
        let reason = "it is a view, and the native door cannot write through a view";
        for sql in [
            "INSERT INTO datafusion.public.v SELECT * FROM frame_rows",
            "INSERT OVERWRITE datafusion.public.v SELECT * FROM frame_rows",
        ] {
            assert_eq!(
                refusal(py, &session, sql, false),
                unwritable_refusal("datafusion.public.v", reason),
                "{sql}"
            );
        }
        assert_eq!(run(&session.borrow(py), "SELECT id, p FROM v"), ["7,1"]);
    });
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[test]
fn a_database_source_table_refuses_without_naming_a_statement() {
    let directory = scratch("source");
    let config = directory.join("repark.toml");
    std::fs::write(
        &config,
        "[default.database.postgres.company_db]\nhost = \"203.0.113.1\"\n",
    )
    .expect("config fixture");
    let session = repark_core::ReparkSessionBuilder::default()
        .from_config_file(Some(config))
        .build()
        .expect("file-built session");
    session
        .register_configured_sources()
        .expect("registration opens no connection");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let input = LogicalPlanBuilder::empty(false)
        .build()
        .expect("an empty input plans");
    let plan = LogicalPlan::Dml(DmlStatement::new(
        TableReference::full("company_db", "public", "t"),
        provider_as_source(Arc::new(EmptyTable::new(schema))),
        WriteOp::Insert(InsertOp::Append),
        Arc::new(input),
    ));
    assert_eq!(
        pending_write_refusal(
            &session,
            "INSERT INTO company_db.public.t SELECT 1",
            &plan,
            false
        ),
        Some(unwritable_refusal(
            "company_db.public.t",
            "it is a table of the database source company_db, and the native door cannot \
             write into a database source"
        ))
    );
    let _ = std::fs::remove_dir_all(&directory);
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
