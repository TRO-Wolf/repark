use std::path::{Path, PathBuf};

use repark_core::ReparkSession;

use super::super::*;
use super::common::*;
use crate::{SparkDialect, SparkExtension};

async fn spark_session(catalog: &str, warehouse: &str) -> ReparkSession {
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .build()
        .unwrap();
    session
        .register_memory_catalog(catalog, warehouse)
        .await
        .unwrap();
    session
}

async fn run(session: &ReparkSession, sql: &str) {
    session.sql(sql).await.unwrap().collect().await.unwrap();
}

async fn table_location(session: &ReparkSession, catalog: &str, ns: &str, table: &str) -> String {
    let handle = session.catalogs_snapshot().get(catalog).unwrap().clone();
    let ident = TableIdent::new(NamespaceIdent::new(ns.to_string()), table.to_string());
    let loaded = handle.load_table(&ident).await.unwrap();
    loaded.metadata().location().to_string()
}

fn has_files(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|mut entries| entries.next().is_some())
}

async fn layout_registry(
    warehouse: &Path,
    namespaces: &[&[&str]],
    layout_root: Option<PathBuf>,
) -> CatalogRegistry {
    named_layout_registry("mem", warehouse, namespaces, layout_root).await
}

async fn named_layout_registry(
    name: &str,
    warehouse: &Path,
    namespaces: &[&[&str]],
    layout_root: Option<PathBuf>,
) -> CatalogRegistry {
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.to_str().unwrap().to_string(),
                )]),
            )
            .await
            .unwrap(),
    );
    for parts in namespaces {
        let ident = NamespaceIdent::from_strs(parts.iter().copied()).unwrap();
        catalog
            .create_namespace(&ident, HashMap::new())
            .await
            .unwrap();
    }
    let mut catalogs = CatalogRegistry::from([(name.to_string(), catalog)]);
    if let Some(root) = layout_root {
        catalogs.set_warehouse_layout_root(name, root);
    }
    catalogs
}

async fn default_location(
    catalogs: &CatalogRegistry,
    namespace: &[&str],
    table: &str,
) -> datafusion::error::Result<String> {
    location_in(catalogs, "mem", namespace, table).await
}

async fn location_in(
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    namespace: &[&str],
    table: &str,
) -> datafusion::error::Result<String> {
    let ident = NamespaceIdent::from_strs(namespace.iter().copied()).unwrap();
    let full_name = format!("{catalog_name}.{}.{table}", namespace.join("."));
    let catalog = catalogs.get(catalog_name).unwrap().clone();
    resolve_create_plan_for(
        catalog.as_ref(),
        catalogs,
        catalog_name,
        &ident,
        table,
        &full_name,
        None,
    )
    .await
    .map(|plan| plan.location)
}

#[tokio::test]
async fn mem_layout_ctas_lands_at_warehouse_namespace_table() {
    let wh = TempDir::new().unwrap();
    let session = spark_session("mem", wh.path().to_str().unwrap()).await;
    session
        .create_namespace("mem", "ns", HashMap::new())
        .await
        .unwrap();
    run(
        &session,
        "CREATE TABLE mem.ns.t USING iceberg AS SELECT 1 AS id",
    )
    .await;
    let expected = wh.path().join("ns").join("t");
    assert_eq!(
        table_location(&session, "mem", "ns", "t").await,
        expected.to_str().unwrap()
    );
    assert!(has_files(&expected.join("metadata")), "{expected:?}");
    assert!(has_files(&expected.join("data")), "{expected:?}");
    assert!(!wh.path().join("repark_ctas").exists());
}

#[tokio::test]
async fn mem_layout_column_def_create_lands_at_warehouse_namespace_table() {
    let wh = TempDir::new().unwrap();
    let session = spark_session("mem", wh.path().to_str().unwrap()).await;
    session
        .create_namespace("mem", "ns", HashMap::new())
        .await
        .unwrap();
    run(&session, "CREATE TABLE mem.ns.c (id BIGINT) USING iceberg").await;
    let expected = wh.path().join("ns").join("c");
    assert_eq!(
        table_location(&session, "mem", "ns", "c").await,
        expected.to_str().unwrap()
    );
    assert!(has_files(&expected.join("metadata")), "{expected:?}");
    assert!(!wh.path().join("repark_ctas").exists());
}

#[tokio::test]
async fn mem_layout_explicit_location_wins() {
    let wh = TempDir::new().unwrap();
    let elsewhere = TempDir::new().unwrap();
    let explicit = elsewhere.path().join("mine");
    let session = spark_session("mem", wh.path().to_str().unwrap()).await;
    session
        .create_namespace("mem", "ns", HashMap::new())
        .await
        .unwrap();
    run(
        &session,
        &format!(
            "CREATE TABLE mem.ns.t USING iceberg LOCATION '{}' AS SELECT 1 AS id",
            explicit.display()
        ),
    )
    .await;
    assert_eq!(
        table_location(&session, "mem", "ns", "t").await,
        explicit.to_str().unwrap()
    );
    assert!(!wh.path().join("ns").join("t").exists());
}

#[tokio::test]
async fn mem_layout_namespace_location_wins() {
    let wh = TempDir::new().unwrap();
    let owned = wh.path().join("owned_ns");
    let session = spark_session("mem", wh.path().to_str().unwrap()).await;
    session
        .create_namespace(
            "mem",
            "ns",
            HashMap::from([("location".to_string(), owned.to_str().unwrap().to_string())]),
        )
        .await
        .unwrap();
    run(
        &session,
        "CREATE TABLE mem.ns.t USING iceberg AS SELECT 1 AS id",
    )
    .await;
    assert_eq!(
        table_location(&session, "mem", "ns", "t").await,
        owned.join("t").to_str().unwrap()
    );
    assert!(!wh.path().join("ns").exists());
}

#[tokio::test]
async fn mem_layout_multi_level_namespace_nests_each_level() {
    let wh = TempDir::new().unwrap();
    let catalogs = layout_registry(
        wh.path(),
        &[&["a"], &["a", "b"]],
        Some(wh.path().to_path_buf()),
    )
    .await;
    let location = default_location(&catalogs, &["a", "b"], "t").await.unwrap();
    assert_eq!(
        location,
        wh.path().join("a").join("b").join("t").to_str().unwrap()
    );
}

#[tokio::test]
async fn mem_layout_unrecorded_temp_fallback_keeps_repark_ctas_path() {
    let wh = TempDir::new().unwrap();
    let catalogs = layout_registry(wh.path(), &[&["ns"]], None).await;
    let location = default_location(&catalogs, &["ns"], "t").await.unwrap();
    let expected = std::env::temp_dir()
        .join("repark_ctas")
        .join("mem")
        .join("ns")
        .join("t");
    assert_eq!(location, expected.to_str().unwrap());
}

fn escape_refusal(kind: &str, segment: &str) -> String {
    match segment {
        ".." => format!("{kind} identifier \"..\" must not contain path traversal ('..')"),
        other => format!("{kind} identifier {other:?} must not contain path separators"),
    }
}

async fn assert_escape_refused(
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    namespace: &[&str],
    table: &str,
    expected: &str,
) {
    match location_in(catalogs, catalog_name, namespace, table).await {
        Err(datafusion::error::DataFusionError::Plan(message)) => {
            assert_eq!(message, expected, "{catalog_name}.{namespace:?}.{table}");
        }
        other => {
            panic!("{catalog_name}.{namespace:?}.{table}: expected a Plan refusal, got {other:?}")
        }
    }
}

#[tokio::test]
async fn mem_layout_refuses_path_escape_identifiers() {
    let wh = TempDir::new().unwrap();
    let root = Some(wh.path().to_path_buf());
    let catalogs = layout_registry(
        wh.path(),
        &[&["ns"], &[".."], &["x/y"], &["ns", ".."], &["ns", "x/y"]],
        root.clone(),
    )
    .await;
    for table in ["..", "x/y"] {
        assert_escape_refused(
            &catalogs,
            "mem",
            &["ns"],
            table,
            &escape_refusal("table", table),
        )
        .await;
    }
    for (namespace, segment) in [
        (&[".."][..], ".."),
        (&["x/y"][..], "x/y"),
        (&["ns", ".."][..], ".."),
        (&["ns", "x/y"][..], "x/y"),
    ] {
        assert_escape_refused(
            &catalogs,
            "mem",
            namespace,
            "t",
            &escape_refusal("namespace", segment),
        )
        .await;
    }
    for name in ["..", "x/y"] {
        let catalogs = named_layout_registry(name, wh.path(), &[&["ns"]], root.clone()).await;
        assert_escape_refused(
            &catalogs,
            name,
            &["ns"],
            "t",
            &escape_refusal("catalog", name),
        )
        .await;
    }
}

#[tokio::test]
async fn mem_layout_file_uri_warehouse_gives_the_plain_location() {
    let wh = TempDir::new().unwrap();
    let warehouse = format!("file://{}", wh.path().display());
    let session = spark_session("mem", &warehouse).await;
    session
        .create_namespace("mem", "ns", HashMap::new())
        .await
        .unwrap();
    run(
        &session,
        "CREATE TABLE mem.ns.t USING iceberg AS SELECT 1 AS id",
    )
    .await;
    let expected = wh.path().join("ns").join("t");
    assert_eq!(
        table_location(&session, "mem", "ns", "t").await,
        expected.to_str().unwrap()
    );
    assert!(has_files(&expected.join("data")), "{expected:?}");
}

#[tokio::test]
async fn mem_layout_single_slash_file_warehouse_root_gives_the_plain_location() {
    let wh = TempDir::new().unwrap();
    let root =
        repark_core::memory_warehouse_fallback_root(&format!("file:{}", wh.path().display()));
    let catalogs = layout_registry(wh.path(), &[&["ns"]], Some(root)).await;
    let location = default_location(&catalogs, &["ns"], "t").await.unwrap();
    assert_eq!(location, wh.path().join("ns").join("t").to_str().unwrap());
}
