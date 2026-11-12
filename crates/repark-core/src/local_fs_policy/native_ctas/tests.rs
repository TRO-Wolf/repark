use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder};
use tempfile::TempDir;

use super::*;
use crate::ReparkSession;

const SHAPES: [&str; 6] = [
    "SELECT 1 AS id",
    "SELECT id FROM {c}.ns.src UNION SELECT id FROM {c}.ns.src",
    "SELECT DISTINCT id FROM {c}.ns.src",
    "SELECT id FROM {c}.ns.src INTERSECT SELECT id FROM {c}.ns.src",
    "SELECT id, count(*) AS n FROM {c}.ns.src GROUP BY id",
    "VALUES (1), (2)",
];

const FORMS: [&str; 8] = [
    "CREATE TABLE {t} AS {q}",
    "create table {t} as {q}",
    "CREATE TABLE IF NOT EXISTS {t} AS {q}",
    "CREATE OR REPLACE TABLE {t} AS {q}",
    "CREATE TABLE {t} (id BIGINT) AS SELECT 1 AS id",
    "EXPLAIN CREATE TABLE {t} AS {q}",
    "EXPLAIN ANALYZE CREATE TABLE {t} AS {q}",
    "PREPARE p_{n} AS CREATE TABLE {t} AS {q}",
];

async fn native_session(root: &TempDir) -> ReparkSession {
    let session = ReparkSession::builder()
        .with_local_fs_guard()
        .build()
        .expect("native session");
    let ice = root.path().join("ice");
    session
        .register_memory_catalog("ice", ice.to_str().expect("utf8 warehouse"))
        .await
        .expect("memory catalog");
    let hadoop = HashMap::from([
        (
            "spark.sql.catalog.hd".to_string(),
            "org.apache.iceberg.spark.SparkCatalog".to_string(),
        ),
        (
            "spark.sql.catalog.hd.type".to_string(),
            "hadoop".to_string(),
        ),
        (
            "spark.sql.catalog.hd.warehouse".to_string(),
            root.path().join("hd").to_string_lossy().into_owned(),
        ),
    ]);
    session
        .register_late_configured_catalogs(&hadoop)
        .await
        .expect("hadoop catalog");
    for catalog in ["ice", "hd"] {
        session
            .create_namespace(catalog, "ns", HashMap::new())
            .await
            .expect("namespace");
        run(&session, &format!("CREATE TABLE {catalog}.ns.src (id INT)")).await;
        run(
            &session,
            &format!("INSERT INTO {catalog}.ns.src VALUES (1), (1), (2), (3)"),
        )
        .await;
    }
    session
}

async fn run(session: &ReparkSession, sql: &str) -> Vec<RecordBatch> {
    session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

async fn refusal(session: &ReparkSession, sql: &str) -> repark_common::Error {
    match session.sql(sql).await {
        Ok(frame) => match frame.collect().await {
            Ok(_) => panic!("{sql}: answered"),
            Err(error) => panic!("{sql}: refused late, at execution: {error}"),
        },
        Err(error) => error,
    }
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
            ids.extend(values.iter().flatten().map(i64::from));
        } else if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            ids.extend(values.iter().flatten());
        } else {
            panic!("unexpected column type {:?}", column.data_type());
        }
    }
    ids.sort_unstable();
    ids
}

fn exists(session: &ReparkSession, table: &str) -> bool {
    session.context().table_exist(table).expect("table lookup")
}

#[tokio::test]
async fn every_ctas_into_an_iceberg_catalog_refuses_before_any_create() {
    let root = TempDir::new().expect("warehouse");
    let session = native_session(&root).await;
    let mut slot = 0;
    for catalog in ["ice", "hd"] {
        for form in FORMS {
            for shape in SHAPES {
                slot += 1;
                let target = format!("{catalog}.ns.t{slot}");
                let query = shape.replace("{c}", catalog);
                let sql = form
                    .replace("{t}", &target)
                    .replace("{q}", &query)
                    .replace("{n}", &slot.to_string());
                let error = refusal(&session, &sql).await;
                assert!(
                    matches!(&error, repark_common::Error::NotImplemented(text) if *text == refusal_message(&target)),
                    "{sql}: {error:?}"
                );
                assert!(!exists(&session, &target), "{sql} created {target}");
            }
        }
        let mixed = format!("{}.NS.t_mixed", catalog.to_uppercase());
        let error = refusal(&session, &format!("CREATE TABLE {mixed} AS SELECT 1 AS id")).await;
        let resolved = format!("{catalog}.ns.t_mixed");
        assert_eq!(error.to_string(), refusal_message(&resolved));
        assert!(!exists(&session, &resolved));
    }
}

#[tokio::test]
async fn an_unqualified_ctas_under_an_iceberg_default_catalog_refuses() {
    let root = TempDir::new().expect("warehouse");
    let session = native_session(&root).await;
    run(&session, "SET datafusion.catalog.default_catalog = 'hd'").await;
    run(&session, "SET datafusion.catalog.default_schema = 'ns'").await;
    let error = refusal(&session, "CREATE TABLE bare AS SELECT DISTINCT id FROM src").await;
    assert_eq!(error.to_string(), refusal_message("hd.ns.bare"));
    assert!(!exists(&session, "hd.ns.bare"));
}

#[tokio::test]
async fn a_refused_replace_leaves_the_existing_table_and_its_rows() {
    let root = TempDir::new().expect("warehouse");
    let session = native_session(&root).await;
    for catalog in ["ice", "hd"] {
        let target = format!("{catalog}.ns.kept");
        run(&session, &format!("CREATE TABLE {target} (id INT)")).await;
        run(&session, &format!("INSERT INTO {target} VALUES (7), (8)")).await;
        for sql in [
            format!("CREATE OR REPLACE TABLE {target} AS SELECT id FROM {catalog}.ns.src"),
            format!(
                "CREATE OR REPLACE TABLE {target} AS SELECT 10 / (id - id) AS id FROM {catalog}.ns.src"
            ),
            format!("CREATE TABLE IF NOT EXISTS {target} AS SELECT id FROM {catalog}.ns.src"),
            format!("CREATE TABLE {target} AS SELECT id FROM {catalog}.ns.src"),
        ] {
            let error = refusal(&session, &sql).await;
            assert_eq!(error.to_string(), refusal_message(&target), "{sql}");
            let rows = run(&session, &format!("SELECT id FROM {target}")).await;
            assert_eq!(ids(&rows), vec![7, 8], "{sql}");
        }
    }
}

#[tokio::test]
async fn the_controls_keep_answering() {
    let root = TempDir::new().expect("warehouse");
    let session = native_session(&root).await;
    for target in ["m_bare", "datafusion.public.m_qualified"] {
        for (index, form) in [
            "CREATE TABLE {t} AS SELECT id FROM ice.ns.src UNION SELECT id FROM hd.ns.src",
            "CREATE OR REPLACE TABLE {t} AS SELECT DISTINCT id FROM ice.ns.src",
        ]
        .into_iter()
        .enumerate()
        {
            let table = format!("{target}{index}");
            run(&session, &form.replace("{t}", &table)).await;
            let rows = run(&session, &format!("SELECT id FROM {table}")).await;
            assert_eq!(ids(&rows), vec![1, 2, 3], "{form}");
        }
    }
    for catalog in ["ice", "hd"] {
        let target = format!("{catalog}.ns.w");
        run(&session, &format!("CREATE TABLE {target} (id INT)")).await;
        assert!(exists(&session, &target));
        run(
            &session,
            &format!("INSERT INTO {target} SELECT DISTINCT id FROM {catalog}.ns.src"),
        )
        .await;
        let rows = run(&session, &format!("SELECT id FROM {target}")).await;
        assert_eq!(ids(&rows), vec![1, 2, 3]);
        let explained = run(
            &session,
            &format!("EXPLAIN INSERT INTO {target} SELECT id FROM {catalog}.ns.src"),
        )
        .await;
        assert!(!explained.is_empty());
    }
    let view = run(
        &session,
        "CREATE VIEW v_mem AS SELECT DISTINCT id FROM ice.ns.src",
    )
    .await;
    assert!(view.iter().all(|batch| batch.num_rows() == 0));
    assert_eq!(
        ids(&run(&session, "SELECT id FROM v_mem").await),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn the_walk_names_the_resolved_target_and_only_iceberg_catalogs() {
    let warehouse = TempDir::new().expect("warehouse");
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "ice",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.path().to_string_lossy().into_owned(),
                )]),
            )
            .await
            .expect("memory catalog"),
    );
    let catalogs = CatalogRegistry::from([("ice".to_string(), catalog)]);
    assert!(refused_catalog(&catalogs, "ice"));
    assert!(!refused_catalog(&catalogs, "datafusion"));
    assert!(!refused_catalog(&catalogs, "ICE"));
    let ctx = SessionContext::new();
    let state = ctx.state();
    let memory = state
        .create_logical_plan("CREATE TABLE m AS SELECT 1 AS id")
        .await
        .expect("plan");
    refuse_iceberg_ctas(&ctx, &catalogs, &memory).expect("an in-memory target is not refused");
    let iceberg = state
        .create_logical_plan("EXPLAIN CREATE TABLE ICE.Ns.T AS SELECT 1 AS id")
        .await
        .expect("plan");
    let error = refuse_iceberg_ctas(&ctx, &catalogs, &iceberg).expect_err("an Iceberg target");
    assert!(
        error.to_string().contains(&refusal_message("ice.ns.t")),
        "{error}"
    );
    let empty = state
        .create_logical_plan("CREATE TABLE ice.ns.m2 (id INT)")
        .await
        .expect("plan");
    let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(create)) = &empty else {
        panic!("a plain CREATE TABLE plans a CreateMemoryTable");
    };
    assert!(!carries_query(create));
    refuse_iceberg_ctas(&ctx, &catalogs, &empty).expect("a CREATE TABLE without a query");
}
