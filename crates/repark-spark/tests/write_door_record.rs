use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int32Type;
use repark_core::{OverwriteIntent, ReparkSession};
use repark_spark::write_door::{WriteDoor, last_write_doors};
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const STRUCT_TABLE: &str = "(id INT, st STRUCT<v: INT, w: INT>, k INT) USING iceberg";
const SOURCE_ROW: &str = "SELECT 1 AS id, named_struct('v', 7, 'w', 8) AS st, 0 AS k";
const MERGE_HEAD: &str = "MERGE INTO ice.ns.t t USING ice.ns.s s ON t.id = s.id";

fn session() -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .build()
        .expect("session")
}

async fn run(session: &ReparkSession, sql: &str) {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

async fn run_with_option(session: &ReparkSession, sql: &str) {
    let options = HashMap::from([(
        "snapshot-property.write-spine-1".to_string(),
        "record".to_string(),
    )]);
    let frame = session
        .sql_with_write_options(sql, &options, OverwriteIntent::Session, false)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

fn recorded(session: &ReparkSession) -> Vec<&'static str> {
    last_write_doors(session.context())
        .into_iter()
        .map(WriteDoor::id)
        .collect()
}

async fn warehouse(session: &ReparkSession) -> TempDir {
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    for setup in [
        "CREATE NAMESPACE ice.ns".to_string(),
        format!("CREATE TABLE ice.ns.t {STRUCT_TABLE}"),
        format!("CREATE TABLE ice.ns.s {STRUCT_TABLE}"),
        format!("INSERT INTO ice.ns.s {SOURCE_ROW}"),
        format!(
            "CREATE TABLE ice.ns.e {STRUCT_TABLE} \
             TBLPROPERTIES ('write.spark.accept-any-schema'='true')"
        ),
    ] {
        run(session, &setup).await;
    }
    warehouse
}

#[tokio::test]
async fn every_spark_door_records_the_door_the_statement_took() {
    let session = session();
    let _warehouse = warehouse(&session).await;
    let cases: [(String, &[&str]); 16] = [
        (format!("INSERT INTO ice.ns.t {SOURCE_ROW}"), &["D1"]),
        (
            format!("INSERT INTO ice.ns.t BY NAME {SOURCE_ROW}"),
            &["D3"],
        ),
        (format!("INSERT INTO ice.ns.e {SOURCE_ROW}"), &["D3"]),
        (format!("INSERT OVERWRITE ice.ns.t {SOURCE_ROW}"), &["D4"]),
        (
            format!("INSERT INTO ice.ns.t REPLACE WHERE k = 0 {SOURCE_ROW}"),
            &["D5"],
        ),
        (
            format!("CREATE TABLE ice.ns.c USING iceberg AS {SOURCE_ROW}"),
            &["D6"],
        ),
        (
            format!(
                "{MERGE_HEAD} WHEN NOT MATCHED THEN INSERT (id, st, k) VALUES (s.id, s.st, s.k)"
            ),
            &["D7"],
        ),
        (
            format!("{MERGE_HEAD} WHEN NOT MATCHED THEN INSERT *"),
            &["D7"],
        ),
        (
            format!("{MERGE_HEAD} WHEN MATCHED THEN UPDATE SET t.st = s.st"),
            &["D8"],
        ),
        (
            format!("{MERGE_HEAD} WHEN MATCHED THEN UPDATE SET *"),
            &["D8"],
        ),
        (
            format!("{MERGE_HEAD} WHEN MATCHED THEN UPDATE SET t.st.v = s.k"),
            &["D9"],
        ),
        (
            format!("{MERGE_HEAD} WHEN MATCHED THEN UPDATE SET t.k = s.k"),
            &["D9"],
        ),
        (
            format!(
                "{MERGE_HEAD} WHEN MATCHED THEN UPDATE SET t.st = s.st \
                 WHEN NOT MATCHED THEN INSERT *"
            ),
            &["D8", "D7"],
        ),
        (
            "UPDATE ice.ns.t SET k = 2 WHERE id = 1".to_string(),
            &["D10"],
        ),
        ("DELETE FROM ice.ns.t WHERE id = 1".to_string(), &[]),
        ("SELECT * FROM ice.ns.t".to_string(), &[]),
    ];
    for (sql, expected) in &cases {
        run(&session, sql).await;
        assert_eq!(recorded(&session), *expected, "{sql}");
    }
    run_with_option(&session, &format!("INSERT INTO ice.ns.t {SOURCE_ROW}")).await;
    assert_eq!(recorded(&session), ["D2"], "an INSERT with a write option");
}

#[tokio::test]
async fn a_session_write_conf_append_records_the_options_door() {
    let session = session();
    let _warehouse = warehouse(&session).await;
    assert!(session.set_iceberg_session_write_conf("spark.sql.iceberg.compression-codec", "zstd"));
    run(&session, &format!("INSERT INTO ice.ns.t {SOURCE_ROW}")).await;
    assert_eq!(recorded(&session), ["D2"]);
    run(&session, "SELECT 1").await;
    assert_eq!(recorded(&session), Vec::<&str>::new());
}

#[tokio::test]
async fn an_update_the_fork_provider_runs_records_the_update_door() {
    let session = session();
    let _warehouse = warehouse(&session).await;
    run(&session, &format!("INSERT INTO ice.ns.t {SOURCE_ROW}")).await;
    run(&session, "UPDATE ice.ns.t SET k = 5").await;
    assert_eq!(recorded(&session), ["D10"]);
    let batches = session
        .sql("SELECT k FROM ice.ns.t")
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let stored: Vec<i32> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(stored, [5]);
}

#[tokio::test]
async fn a_merge_records_each_door_once_however_many_arms_take_it() {
    let session = session();
    let _warehouse = warehouse(&session).await;
    run(
        &session,
        &format!(
            "{MERGE_HEAD} WHEN MATCHED THEN UPDATE SET t.st.v = s.k, t.st.w = s.k \
             WHEN NOT MATCHED AND s.k = 0 THEN INSERT * WHEN NOT MATCHED THEN INSERT *"
        ),
    )
    .await;
    assert_eq!(recorded(&session), ["D9", "D7"]);
}

#[tokio::test]
async fn a_merge_whole_column_update_records_the_cast_path_it_takes() {
    let session = session();
    let _warehouse = warehouse(&session).await;
    let columns = "(id INT, arr ARRAY<INT>, m MAP<STRING, INT>, sa ARRAY<STRUCT<v: INT, w: INT>>, \
                   ts TIMESTAMP, ntz TIMESTAMP_NTZ) USING iceberg";
    run(&session, &format!("CREATE TABLE ice.ns.ct {columns}")).await;
    run(&session, &format!("CREATE TABLE ice.ns.cs {columns}")).await;
    run(
        &session,
        "INSERT INTO ice.ns.cs SELECT 1, array(1), map('a', 1), array(named_struct('v', 1, 'w', 2)), \
         TIMESTAMP '2026-01-02 03:04:05', TIMESTAMP_NTZ '2026-01-02 03:04:05'",
    )
    .await;
    for (column, expected) in [
        ("arr", "D8"),
        ("m", "D8"),
        ("sa", "D8"),
        ("ts", "D9"),
        ("ntz", "D9"),
    ] {
        let sql = format!(
            "MERGE INTO ice.ns.ct t USING ice.ns.cs s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.{column} = s.{column}"
        );
        run(&session, &sql).await;
        assert_eq!(recorded(&session), [expected], "{sql}");
    }
}
