use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{
    MEMORY_CATALOG_METADATA_NAMING, MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder,
};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableIdent};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};

const Q_OPTIMIZER: &str = "SELECT CAST('q' AS INT) AS id, 'r' AS v";
const Q_FIRST: &str = "SELECT id, 10 / (id - 1) AS q FROM range(1, 6)";
const Q_LAST: &str = "SELECT id, 10 / (5 - id) AS q FROM range(1, 6)";
const Q_LATE: &str = "SELECT id, 10 / (199999 - id) AS q FROM range(0, 200000)";
const Q_OK: &str = "SELECT id, id * 2 AS q FROM range(1, 6)";
const Q_PLAN: &str = "SELECT id, no_such_column FROM range(1, 3)";
const Q_SEED: &str = "SELECT id, id AS q FROM range(1, 4)";
const ROWS_FIVE: &str = "rows id,q;1,2;2,4;3,6;4,8;5,10";
const ROWS_THREE: &str = "rows id,q;1,1;2,2;3,3";
const REFUSAL_TEXT: &str = "no Iceberg table found at";
const TEXT_CARD: &str = "CTAS-PATH-READ-TEXT-1";
const CODEC_KEY: &str = "spark.sql.iceberg.compression-codec";

struct FailRow {
    row: &'static str,
    query: &'static str,
    partition: bool,
    target_size: bool,
    options_path: bool,
    assert_no_data: bool,
    failure_mark: &'static str,
}

const FAIL_ROWS: [FailRow; 6] = [
    FailRow {
        row: "F1",
        query: Q_OPTIMIZER,
        partition: false,
        target_size: false,
        options_path: false,
        assert_no_data: true,
        failure_mark: "simplify_expressions",
    },
    FailRow {
        row: "F2",
        query: Q_FIRST,
        partition: false,
        target_size: false,
        options_path: false,
        assert_no_data: true,
        failure_mark: "DIVIDE_BY_ZERO",
    },
    FailRow {
        row: "F3",
        query: Q_LAST,
        partition: false,
        target_size: false,
        options_path: false,
        assert_no_data: true,
        failure_mark: "DIVIDE_BY_ZERO",
    },
    FailRow {
        row: "F4",
        query: Q_LAST,
        partition: true,
        target_size: false,
        options_path: false,
        assert_no_data: true,
        failure_mark: "DIVIDE_BY_ZERO",
    },
    FailRow {
        row: "F5",
        query: Q_LATE,
        partition: false,
        target_size: true,
        options_path: false,
        assert_no_data: true,
        failure_mark: "DIVIDE_BY_ZERO",
    },
    FailRow {
        row: "F6",
        query: Q_LAST,
        partition: false,
        target_size: false,
        options_path: true,
        assert_no_data: false,
        failure_mark: "DIVIDE_BY_ZERO",
    },
];

fn session() -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("spark.sql.ansi.enabled", "true")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .build()
        .expect("session")
}

async fn open_catalog(kind: &str, root: &Path) -> Arc<dyn Catalog> {
    let mut props = HashMap::from([(
        MEMORY_CATALOG_WAREHOUSE.to_string(),
        root.display().to_string(),
    )]);
    if kind == "hadoop" {
        props.insert(
            MEMORY_CATALOG_METADATA_NAMING.to_string(),
            "hadoop".to_string(),
        );
    }
    Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load("memory", props)
            .await
            .expect("catalog"),
    )
}

async fn catalog_at(kind: &str, root: &Path) -> Arc<dyn Catalog> {
    let catalog = open_catalog(kind, root).await;
    let ns_props = HashMap::from([("location".to_string(), format!("{}/ns", root.display()))]);
    catalog
        .create_namespace(&NamespaceIdent::new("ns".to_string()), ns_props)
        .await
        .expect("namespace");
    catalog
}

async fn run(session: &ReparkSession, sql: &str) -> Result<String, String> {
    let frame = session.sql(sql).await.map_err(|error| error.to_string())?;
    let batches = frame.collect().await.map_err(|error| error.to_string())?;
    let shown = pretty_format_batches(&batches)
        .map_err(|error| error.to_string())?
        .to_string();
    let lines: Vec<String> = shown
        .lines()
        .filter(|line| line.starts_with('|'))
        .map(|line| {
            line.trim_matches('|')
                .split('|')
                .map(str::trim)
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    Ok(format!("rows {}", lines.join(";")))
}

fn ident(name: &str) -> TableIdent {
    TableIdent::from_strs(["ns", name]).expect("identifier")
}

fn files_under(dir: &Path) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).into_iter().flatten().flatten() {
            if entry.path().is_dir() {
                stack.push(entry.path());
            } else {
                found.insert(entry.path().display().to_string());
            }
        }
    }
    found
}

fn metadata_files(table_dir: &Path) -> Vec<String> {
    files_under(&table_dir.join("metadata"))
        .into_iter()
        .filter(|path| path.ends_with(".metadata.json"))
        .collect()
}

fn data_files(table_dir: &Path) -> Vec<String> {
    files_under(&table_dir.join("data")).into_iter().collect()
}

fn label(row: &str, spelling: &str, kind: &str, version: &str) -> String {
    format!("{row}-{spelling}-{kind}-v{version}-{TEXT_CARD}")
}

fn plain_label(row: &str, spelling: &str, kind: &str, version: &str) -> String {
    format!("{row}-{spelling}-{kind}-v{version}")
}

fn ctas_statement(verb: &str, row: &FailRow, target: &str, version: &str) -> String {
    let partition = if row.partition {
        " PARTITIONED BY (q)"
    } else {
        ""
    };
    let mut properties = vec![format!("'format-version'='{version}'")];
    if row.target_size {
        properties.push("'write.target-file-size-bytes'='1024'".to_string());
    }
    format!(
        "{verb} fault.ns.{target} USING iceberg{partition} TBLPROPERTIES ({}) AS {}",
        properties.join(", "),
        row.query
    )
}

async fn fail_cell(row: &FailRow, spelling: &str, kind: &str, version: &str) {
    let cell = label(row.row, spelling, kind, version);
    let tmp = tempfile::TempDir::new().expect("root");
    let root = tmp.path();
    let catalog = catalog_at(kind, root).await;
    let session = session();
    session
        .register_iceberg_catalog("fault", Arc::clone(&catalog))
        .await
        .expect("register");
    if row.options_path {
        assert!(
            session.set_iceberg_session_write_conf(CODEC_KEY, "zstd"),
            "{cell}: codec key must route to the options path"
        );
    }
    let verb = if spelling == "cor" {
        "CREATE OR REPLACE TABLE"
    } else {
        "CREATE TABLE"
    };
    let failed = run(&session, &ctas_statement(verb, row, "c", version))
        .await
        .err()
        .unwrap_or_else(|| panic!("{cell}: the statement must fail"));
    assert!(
        failed.contains(row.failure_mark),
        "{cell}: failure after begin, got {failed}"
    );
    let table_dir: PathBuf = root.join("ns").join("c");
    let meta = metadata_files(&table_dir);
    assert!(meta.is_empty(), "{cell}: {meta:?}");
    if row.assert_no_data {
        let data = data_files(&table_dir);
        assert!(data.is_empty(), "{cell}: {data:?}");
    }
    let same = run(&session, "SELECT * FROM fault.ns.c ORDER BY 1")
        .await
        .err()
        .unwrap_or_else(|| panic!("{cell}: the same-session catalog must refuse"));
    assert!(same.contains("not found"), "{cell}: {same}");
    let cold = catalog_at(kind, root).await;
    let cold_error = cold
        .load_table(&ident("c"))
        .await
        .err()
        .unwrap_or_else(|| panic!("{cell}: a fresh catalog must refuse"));
    assert!(
        matches!(cold_error.kind(), ErrorKind::TableNotFound),
        "{cell}: {cold_error:?}"
    );
    match session
        .read_iceberg_path(&table_dir.display().to_string())
        .await
    {
        Err(error) => assert!(error.to_string().contains(REFUSAL_TEXT), "{cell}: {error}"),
        Ok(_) => panic!("{cell}: the path read must refuse"),
    }
}

async fn k1_cell(kind: &str, version: &str) {
    let cell = plain_label("K1", "sql", kind, version);
    let tmp = tempfile::TempDir::new().expect("root");
    let root = tmp.path();
    let catalog = catalog_at(kind, root).await;
    let session = session();
    session
        .register_iceberg_catalog("fault", Arc::clone(&catalog))
        .await
        .expect("register");
    let statement = format!(
        "CREATE TABLE fault.ns.c USING iceberg TBLPROPERTIES ('format-version'='{version}') AS {Q_PLAN}"
    );
    run(&session, &statement)
        .await
        .err()
        .unwrap_or_else(|| panic!("{cell}: analysis must fail"));
    let left = files_under(&root.join("ns").join("c"));
    assert!(left.is_empty(), "{cell}: {left:?}");
}

async fn k2_cell(spelling: &str, kind: &str, version: &str) {
    let cell = plain_label("K2", spelling, kind, version);
    let tmp = tempfile::TempDir::new().expect("root");
    let root = tmp.path();
    let catalog = catalog_at(kind, root).await;
    let session = session();
    session
        .register_iceberg_catalog("fault", Arc::clone(&catalog))
        .await
        .expect("register");
    let verb = if spelling == "cor" {
        "CREATE OR REPLACE TABLE"
    } else {
        "CREATE TABLE"
    };
    let statement = format!(
        "{verb} fault.ns.c USING iceberg TBLPROPERTIES ('format-version'='{version}') AS {Q_OK}"
    );
    run(&session, &statement)
        .await
        .unwrap_or_else(|error| panic!("{cell}: success must stay green: {error}"));
    let table_dir: PathBuf = root.join("ns").join("c");
    let meta = metadata_files(&table_dir);
    if kind == "hadoop" {
        assert!(
            meta.iter().any(|path| path.ends_with("/v1.metadata.json")),
            "{cell}: {meta:?}"
        );
        assert!(
            table_dir.join("metadata/version-hint.text").exists(),
            "{cell}: version hint missing"
        );
        assert!(
            !meta.iter().any(|path| path.contains("/00000-")),
            "{cell}: {meta:?}"
        );
    } else {
        assert_eq!(meta.len(), 1, "{cell}: {meta:?}");
        assert!(meta[0].contains("/00000-"), "{cell}: {meta:?}");
    }
    let read = run(&session, "SELECT * FROM fault.ns.c ORDER BY 1")
        .await
        .unwrap_or_else(|error| panic!("{cell}: rows must read: {error}"));
    assert_eq!(read, ROWS_FIVE, "{cell}");
}

async fn k4_cell(spelling: &str, kind: &str, version: &str) {
    let cell = plain_label("K4", spelling, kind, version);
    let tmp = tempfile::TempDir::new().expect("root");
    let root = tmp.path();
    let catalog = catalog_at(kind, root).await;
    let session = session();
    session
        .register_iceberg_catalog("fault", Arc::clone(&catalog))
        .await
        .expect("register");
    let seed = format!(
        "CREATE TABLE fault.ns.t USING iceberg TBLPROPERTIES ('format-version'='{version}') AS {Q_SEED}"
    );
    run(&session, &seed)
        .await
        .unwrap_or_else(|error| panic!("{cell}: seed must land: {error}"));
    let table_dir: PathBuf = root.join("ns").join("t");
    let before = files_under(&table_dir);
    let verb = if spelling == "replace" {
        "REPLACE TABLE"
    } else {
        "CREATE OR REPLACE TABLE"
    };
    let statement = format!(
        "{verb} fault.ns.t USING iceberg TBLPROPERTIES ('format-version'='{version}') AS {Q_LAST}"
    );
    run(&session, &statement)
        .await
        .err()
        .unwrap_or_else(|| panic!("{cell}: the replace query must fail"));
    let read = run(&session, "SELECT * FROM fault.ns.t ORDER BY 1")
        .await
        .unwrap_or_else(|error| panic!("{cell}: live rows must read: {error}"));
    assert_eq!(read, ROWS_THREE, "{cell}");
    let after = files_under(&table_dir);
    assert_eq!(before, after, "{cell}");
}

async fn options_success_cell(kind: &str, version: &str) {
    let cell = plain_label("K3", "sql", kind, version);
    let tmp = tempfile::TempDir::new().expect("root");
    let root = tmp.path();
    let catalog = catalog_at(kind, root).await;
    let session = session();
    session
        .register_iceberg_catalog("fault", Arc::clone(&catalog))
        .await
        .expect("register");
    assert!(
        session.set_iceberg_session_write_conf(CODEC_KEY, "zstd"),
        "{cell}: codec key must route to the options path"
    );
    let statement = format!(
        "CREATE TABLE fault.ns.c USING iceberg TBLPROPERTIES ('format-version'='{version}') AS {Q_OK}"
    );
    run(&session, &statement)
        .await
        .unwrap_or_else(|error| panic!("{cell}: options success must stay green: {error}"));
    let table_dir: PathBuf = root.join("ns").join("c");
    let meta = metadata_files(&table_dir);
    if kind == "hadoop" {
        assert!(
            meta.iter().any(|path| path.contains("/00000-")),
            "{cell}: the options stray stays: {meta:?}"
        );
        assert!(
            meta.iter().any(|path| path.ends_with("/v1.metadata.json")),
            "{cell}: {meta:?}"
        );
    } else {
        assert_eq!(meta.len(), 2, "{cell}: {meta:?}");
    }
    let read = run(&session, "SELECT * FROM fault.ns.c ORDER BY 1")
        .await
        .unwrap_or_else(|error| panic!("{cell}: rows must read: {error}"));
    assert_eq!(read, ROWS_FIVE, "{cell}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_ctas_create_leaves_no_metadata_on_either_catalog() {
    for row in FAIL_ROWS {
        for spelling in ["sql", "cor"] {
            for kind in ["memory", "hadoop"] {
                for version in ["2", "3"] {
                    fail_cell(&row, spelling, kind, version).await;
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_analysis_failure_writes_nothing_on_either_catalog() {
    for kind in ["memory", "hadoop"] {
        for version in ["2", "3"] {
            k1_cell(kind, version).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fast_path_success_keeps_its_metadata_on_either_catalog() {
    for spelling in ["sql", "cor"] {
        for kind in ["memory", "hadoop"] {
            for version in ["2", "3"] {
                k2_cell(spelling, kind, version).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_replace_keeps_the_live_table_on_either_catalog() {
    for spelling in ["cor", "replace"] {
        for kind in ["memory", "hadoop"] {
            for version in ["2", "3"] {
                k4_cell(spelling, kind, version).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_options_path_success_keeps_both_metadata_files_on_either_catalog() {
    for kind in ["memory", "hadoop"] {
        for version in ["2", "3"] {
            options_success_cell(kind, version).await;
        }
    }
}
