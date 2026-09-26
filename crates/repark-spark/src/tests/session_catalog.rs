use repark_core::{ErrorClass, ReparkSession};

use super::common::*;
use crate::{SparkDialect, SparkExtension, session_catalog};

const CATALOG_NOT_FOUND_NOPE: &str = "[CATALOG_NOT_FOUND] The catalog `nope` not found. Consider \
     to set the SQL config \"spark.sql.catalog.nope\" to a catalog plugin. SQLSTATE: 42P08";

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

fn current(session: &ReparkSession) -> (String, String) {
    session.catalogs_snapshot().current_defaults()
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
async fn the_default_catalog_conf_at_build_is_the_first_current_catalog() {
    let wh = TempDir::new().unwrap();
    let session = harness_session(&wh, &[("spark.sql.defaultCatalog", "hc")]).await;
    assert_eq!(
        rows(&session, "SELECT current_catalog(), current_database()").await,
        pair("hc", "")
    );
    rows(&session, "CREATE TABLE ns.bt (id INT) USING iceberg").await;
    assert_eq!(
        rows(&session, "SELECT count(*) FROM hc.ns.bt").await,
        [["0"]].map(|row| vec![row[0].to_string()])
    );
}

#[tokio::test]
async fn the_runtime_default_catalog_moves_current_until_use_pins_it() {
    let wh = TempDir::new().unwrap();
    let session = harness_session(&wh, &[]).await;
    session_catalog::apply_default_catalog(&session, Some("sc"));
    assert_eq!(
        rows(&session, "SELECT current_catalog(), current_database()").await,
        pair("sc", "")
    );
    rows(&session, "CREATE TABLE ns.dc_t (id INT) USING iceberg").await;
    assert_eq!(
        rows(&session, "SELECT count(*) FROM sc.ns.dc_t").await,
        [["0"]].map(|row| vec![row[0].to_string()])
    );
    session_catalog::apply_default_catalog(&session, None);
    assert_eq!(
        rows(&session, "SELECT current_catalog(), current_database()").await,
        pair("spark_catalog", "default")
    );
    rows(&session, "USE hc.ns").await;
    session_catalog::apply_default_catalog(&session, Some("sc"));
    assert_eq!(
        rows(&session, "SELECT current_catalog(), current_database()").await,
        pair("hc", "ns")
    );
    session_catalog::apply_default_catalog(&session, None);
    assert_eq!(current(&session), ("hc".to_string(), "ns".to_string()));
    rows(&session, "USE sc").await;
    session_catalog::apply_default_catalog(&session, None);
    assert_eq!(current(&session), ("sc".to_string(), String::new()));
}

#[tokio::test]
async fn use_forms_answer_as_spark() {
    let wh = TempDir::new().unwrap();
    let session = harness_session(&wh, &[]).await;
    session
        .create_namespace("spark_catalog", "otherns", HashMap::new())
        .await
        .unwrap();
    for (sql, catalog, namespace) in [
        ("USE sc", "sc", ""),
        ("USE sc.ns", "sc", "ns"),
        ("USE ns", "sc", "ns"),
        ("USE sc", "sc", "ns"),
        ("USE hc", "hc", ""),
        ("USE ns", "hc", "ns"),
        ("USE spark_catalog", "spark_catalog", "default"),
        ("USE spark_catalog.otherns", "spark_catalog", "otherns"),
        ("USE spark_catalog", "spark_catalog", "otherns"),
        ("USE sc.ns", "sc", "ns"),
        ("USE spark_catalog.default", "spark_catalog", "default"),
    ] {
        rows(&session, sql).await;
        assert_eq!(
            rows(&session, "SELECT current_catalog(), current_database()").await,
            pair(catalog, namespace),
            "{sql}"
        );
    }
    for (sql, rendered) in [
        ("USE zz.yy", "`spark_catalog`.`zz`.`yy`"),
        ("USE zz", "`spark_catalog`.`zz`"),
        ("USE zz.yy.xx", "`spark_catalog`.`zz`.`yy`.`xx`"),
        ("USE sc.nope", "`sc`.`nope`"),
        ("USE hc.ns.x", "`hc`.`ns`.`x`"),
    ] {
        let error = refusal(&session, sql).await;
        assert_eq!(
            error.exception_class(),
            ErrorClass::Analysis,
            "{sql}: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains(&format!(
                "[SCHEMA_NOT_FOUND] The schema {rendered} cannot be found."
            )) && message.contains("SQLSTATE: 42704"),
            "{sql}: {message}"
        );
    }
    assert_eq!(
        current(&session),
        ("spark_catalog".to_string(), "default".to_string())
    );
}

#[tokio::test]
async fn a_bare_memory_type_refuses_every_first_use_with_sparks_text() {
    let wh = TempDir::new().unwrap();
    let mem_wh = format!("{}/c_mem", wh.path().to_str().unwrap());
    let session = harness_session(
        &wh,
        &[
            (
                "spark.sql.catalog.c_mem",
                "org.apache.iceberg.spark.SparkCatalog",
            ),
            ("spark.sql.catalog.c_mem.type", "memory"),
            ("spark.sql.catalog.c_mem.warehouse", mem_wh.as_str()),
        ],
    )
    .await;
    assert_eq!(
        rows(&session, "SHOW CATALOGS").await,
        [["hc"], ["sc"], ["spark_catalog"]].map(|row| vec![row[0].to_string()])
    );
    for sql in [
        "USE c_mem",
        "USE c_mem.n1",
        "SHOW NAMESPACES IN c_mem",
        "SHOW NAMESPACES IN c_mem",
        "SHOW TABLES IN c_mem",
        "CREATE NAMESPACE IF NOT EXISTS c_mem.n1",
        "SELECT * FROM c_mem.n1.t",
    ] {
        let error = refusal(&session, sql).await;
        assert_eq!(
            error.exception_class(),
            ErrorClass::Unsupported,
            "{sql}: {error}"
        );
        assert_eq!(error.to_string(), "Unknown catalog type: memory", "{sql}");
    }
    assert_eq!(
        current(&session),
        ("spark_catalog".to_string(), "default".to_string())
    );
}

#[tokio::test]
async fn both_kind_keys_refuse_every_first_use_as_illegal_argument() {
    let wh = TempDir::new().unwrap();
    let both_wh = format!("{}/c_both", wh.path().to_str().unwrap());
    let session = harness_session(
        &wh,
        &[
            ("spark.sql.catalog.c_both.type", "memory"),
            (
                "spark.sql.catalog.c_both.catalog-impl",
                "org.apache.iceberg.inmemory.InMemoryCatalog",
            ),
            ("spark.sql.catalog.c_both.warehouse", both_wh.as_str()),
        ],
    )
    .await;
    for sql in [
        "USE c_both",
        "SHOW NAMESPACES IN c_both",
        "CREATE NAMESPACE IF NOT EXISTS c_both.n1",
        "SELECT * FROM c_both.n1.t",
    ] {
        let error = refusal(&session, sql).await;
        assert_eq!(
            error.exception_class(),
            ErrorClass::IllegalArgument,
            "{sql}: {error}"
        );
        assert_eq!(
            error.to_string(),
            "Cannot create catalog c_both, both type and catalog-impl are set: type=memory, \
             catalog-impl=org.apache.iceberg.inmemory.InMemoryCatalog",
            "{sql}"
        );
    }
    assert_eq!(
        current(&session),
        ("spark_catalog".to_string(), "default".to_string())
    );
}

#[tokio::test]
async fn the_catalog_impl_long_form_and_the_opt_in_stay_catalogs() {
    let wh = TempDir::new().unwrap();
    let root = wh.path().to_str().unwrap();
    let impl_wh = format!("{root}/c_impl");
    let session = harness_session(
        &wh,
        &[
            (
                "spark.sql.catalog.c_impl.catalog-impl",
                "org.apache.iceberg.inmemory.InMemoryCatalog",
            ),
            ("spark.sql.catalog.c_impl.warehouse", impl_wh.as_str()),
        ],
    )
    .await;
    rows(&session, "CREATE NAMESPACE c_impl.n1").await;
    rows(&session, "CREATE TABLE c_impl.n1.t (id INT) USING iceberg").await;
    assert_eq!(
        rows(&session, "SELECT count(*) FROM c_impl.n1.t").await,
        [["0"]].map(|row| vec![row[0].to_string()])
    );
    let rt_wh = format!("{root}/c_rt");
    let refused = session
        .register_late_catalog_block(&HashMap::from([
            (
                "spark.sql.catalog.c_rt.type".to_string(),
                "memory".to_string(),
            ),
            (
                "spark.sql.catalog.c_rt.warehouse".to_string(),
                rt_wh.clone(),
            ),
        ]))
        .await
        .unwrap();
    assert!(!refused);
    let error = refusal(&session, "SHOW NAMESPACES IN c_rt").await;
    assert_eq!(error.to_string(), "Unknown catalog type: memory");
    let registered = session
        .register_late_catalog_block(&HashMap::from([
            (
                "repark.sql.catalogExtensions".to_string(),
                "true".to_string(),
            ),
            (
                "spark.sql.catalog.c_rt.type".to_string(),
                "memory".to_string(),
            ),
            ("spark.sql.catalog.c_rt.warehouse".to_string(), rt_wh),
        ]))
        .await
        .unwrap();
    assert!(registered);
    rows(&session, "CREATE NAMESPACE c_rt.n1").await;
}

#[tokio::test]
async fn spark_catalog_names_the_session_catalog_only() {
    let wh = TempDir::new().unwrap();
    let session = harness_session(&wh, &[]).await;
    rows(&session, "CREATE TABLE sc.ns.t0 (id INT) USING iceberg").await;
    let error = refusal(&session, "SELECT * FROM spark_catalog.ns.t0").await;
    assert_eq!(error.exception_class(), ErrorClass::Analysis, "{error}");
}

#[tokio::test]
async fn a_default_catalog_that_names_no_catalog_refuses_where_spark_resolves_it() {
    let wh = TempDir::new().unwrap();
    let session = harness_session(&wh, &[("spark.sql.defaultCatalog", "nope")]).await;
    assert_eq!(rows(&session, "SELECT 1").await.len(), 1);
    rows(
        &session,
        "CREATE TABLE sc.ns.three_part (id INT) USING iceberg",
    )
    .await;
    assert_eq!(
        rows(&session, "SELECT count(*) FROM sc.ns.three_part").await,
        [["0"]].map(|row| vec![row[0].to_string()])
    );
    for sql in [
        "SELECT current_catalog()",
        "SELECT current_database()",
        "SHOW NAMESPACES",
        "SHOW TABLES",
        "CREATE TABLE ns.t (id INT) USING iceberg",
        "USE sc",
        "SELECT * FROM nope.ns.t",
    ] {
        let error = refusal(&session, sql).await;
        assert_eq!(
            error.exception_class(),
            ErrorClass::Analysis,
            "{sql}: {error}"
        );
        assert!(
            error.to_string().contains(CATALOG_NOT_FOUND_NOPE),
            "{sql}: {error}"
        );
    }
}
