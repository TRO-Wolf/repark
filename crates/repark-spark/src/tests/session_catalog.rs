use repark_core::{ErrorClass, ReparkSession};

use super::common::*;
use crate::{SparkDialect, SparkExtension};

async fn harness_session(wh: &TempDir, extra: &[(&str, &str)]) -> ReparkSession {
    let root = wh.path().to_str().unwrap();
    let mut builder = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .config("spark.sql.catalog.hc.type", "hadoop")
        .config("spark.sql.catalog.hc.warehouse", format!("{root}/hc"));
    for (key, value) in extra {
        builder = builder.config(*key, *value);
    }
    let session = builder.build().unwrap();
    session.register_configured_catalogs().await.unwrap();
    session
        .register_memory_catalog("sc", &format!("{root}/sc"))
        .await
        .unwrap();
    session
        .register_memory_catalog("spark_catalog", &format!("{root}/session"))
        .await
        .unwrap();
    for (catalog, namespace) in [("sc", "ns"), ("hc", "ns"), ("spark_catalog", "default")] {
        session
            .create_namespace(catalog, namespace, HashMap::new())
            .await
            .unwrap();
    }
    session
}

async fn rows(session: &ReparkSession, sql: &str) -> Vec<Vec<String>> {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            out.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        datafusion::arrow::util::display::array_value_to_string(column, row)
                            .unwrap()
                    })
                    .collect(),
            );
        }
    }
    out
}

async fn refusal(session: &ReparkSession, sql: &str) -> repark_core::Error {
    match session.sql(sql).await {
        Ok(frame) => match frame.collect().await {
            Ok(_) => panic!("{sql} must refuse"),
            Err(error) => repark_core::engine_err(error),
        },
        Err(error) => error,
    }
}

fn pair(catalog: &str, namespace: &str) -> Vec<Vec<String>> {
    vec![vec![catalog.to_string(), namespace.to_string()]]
}

#[tokio::test]
async fn a_fresh_session_is_in_spark_catalog_beside_configured_and_registered_catalogs() {
    let wh = TempDir::new().unwrap();
    let session = harness_session(&wh, &[]).await;
    assert_eq!(
        rows(&session, "SELECT current_catalog(), current_database()").await,
        pair("spark_catalog", "default")
    );
    assert_eq!(
        rows(&session, "SHOW CATALOGS").await,
        [["hc"], ["sc"], ["spark_catalog"]].map(|row| vec![row[0].to_string()])
    );
}

#[tokio::test]
async fn spark_catalog_names_the_session_catalog_only() {
    let wh = TempDir::new().unwrap();
    let session = harness_session(&wh, &[]).await;
    rows(&session, "CREATE TABLE sc.ns.t0 (id INT) USING iceberg").await;
    let error = refusal(&session, "SELECT * FROM spark_catalog.ns.t0").await;
    assert_eq!(error.exception_class(), ErrorClass::Analysis, "{error}");
}
