use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const FORMS: [&str; 3] = [
    "UPDATE ice.ns.t SET v = c",
    "UPDATE ice.ns.t SET v = c WHERE id >= 0",
    "UPDATE ice.ns.t SET v = CAST(c AS TIMESTAMP) WHERE id >= 0",
];
const SOURCES: [(&str, &str); 2] = [
    ("timestamp_ns", "'1969-12-31 23:59:59.999998500'"),
    ("timestamptz_ns", "'1969-12-31 23:59:59.999998500+00:00'"),
];

fn session() -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "America/New_York")
        .build()
        .expect("session")
}

async fn run(session: &ReparkSession, sql: &str) {
    session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("run `{sql}`: {error}"));
}

async fn stored(source: &str, value: &str, mode: &str, form: &str) -> Vec<Option<i64>> {
    let warehouse = TempDir::new().expect("warehouse");
    let session = session();
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    run(
        &session,
        &format!(
            "CREATE TABLE ice.ns.t (id INT, v TIMESTAMP, c {source}) USING iceberg \
             TBLPROPERTIES ('format-version'='3', 'write.update.mode'='{mode}', \
             'write.delete.mode'='{mode}')"
        ),
    )
    .await;
    run(
        &session,
        &format!(
            "INSERT INTO ice.ns.t (id, c) VALUES (1, CAST('2026-01-02 03:04:05.123456789' AS \
             {source})), (2, CAST({value} AS {source}))"
        ),
    )
    .await;
    run(&session, form).await;
    let batches = session
        .sql("SELECT v FROM ice.ns.t ORDER BY id")
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    batches
        .iter()
        .flat_map(|batch| {
            assert_eq!(
                batch.column(0).data_type(),
                &DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Microsecond,
                    Some("UTC".into())
                )
            );
            let ints = cast(batch.column(0), &DataType::Int64).expect("ticks");
            ints.as_primitive::<Int64Type>().iter().collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_identity_update_stores_what_its_other_forms_store() {
    for (source, value) in SOURCES {
        for mode in ["copy-on-write", "merge-on-read"] {
            let unfiltered = stored(source, value, mode, FORMS[0]).await;
            assert!(unfiltered.iter().all(Option::is_some), "{source} {mode}");
            for form in &FORMS[1..] {
                assert_eq!(
                    stored(source, value, mode, form).await,
                    unfiltered,
                    "{source} {mode}: `{form}`"
                );
            }
        }
    }
}
