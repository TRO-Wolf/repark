use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use datafusion::arrow::util::pretty::pretty_format_batches;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{
    MEMORY_CATALOG_METADATA_NAMING, MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder,
};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation,
    TableIdent,
};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use serde_json::Value;

const CELLS: &str = include_str!("../../../docs/corpus/commit-cells-2026-11-04.jsonl");
const DOOR: &str = "spark";
const SEEDS: [&str; 2] = [
    "CREATE TABLE fault.ns.t (id INT, v INT) USING iceberg TBLPROPERTIES ('format-version'='2')",
    "INSERT INTO fault.ns.t VALUES (1, 10), (2, 20), (3, 30)",
];
const PRE_T: &str = "rows id,v;1,10;2,20;3,30";
const STAGED_HEADS: [&str; 3] = ["CTAS", "RTAS", "CREATE TABLE"];

const NONE: u8 = 0;
const BEFORE: u8 = 1;
const LIST: u8 = 2;
const LOAD: u8 = 3;
const REFRESH: u8 = 4;
const UNKNOWN: u8 = 5;
const LOST_ACK: u8 = 6;

#[derive(Debug)]
struct FaultCatalog {
    inner: Arc<dyn Catalog>,
    point: AtomicU8,
    spent: AtomicBool,
    tripped: AtomicBool,
}

impl FaultCatalog {
    fn fault(&self) -> iceberg::Result<()> {
        let point = self.point.load(Ordering::SeqCst);
        if !matches!(point, BEFORE | UNKNOWN) || self.spent.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let kind = if point == UNKNOWN {
            ErrorKind::CommitStateUnknown
        } else {
            ErrorKind::Unexpected
        };
        Err(iceberg::Error::new(kind, "catalog update unavailable"))
    }

    fn published<T>(&self, result: iceberg::Result<T>) -> iceberg::Result<T> {
        if result.is_ok() {
            self.tripped.store(true, Ordering::SeqCst);
        }
        result
    }

    fn lost_ack(&self, result: iceberg::Result<Table>) -> iceberg::Result<Table> {
        let table = result?;
        if self.point.load(Ordering::SeqCst) == LOST_ACK {
            return Err(iceberg::Error::new(
                ErrorKind::Unexpected,
                "publish acknowledgement lost",
            ));
        }
        Ok(table)
    }

    fn after(&self, point: u8, what: &str) -> iceberg::Result<()> {
        if self.tripped.load(Ordering::SeqCst) && self.point.load(Ordering::SeqCst) == point {
            return Err(iceberg::Error::new(
                ErrorKind::Unexpected,
                format!("{what} unavailable"),
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Catalog for FaultCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.after(REFRESH, "namespace refresh")?;
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.after(LIST, "table listing")?;
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.fault()?;
        self.published(self.inner.create_table(namespace, creation).await)
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        self.after(LOAD, "table load")?;
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.fault()?;
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(&self, table: &TableIdent, location: String) -> iceberg::Result<Table> {
        self.fault()?;
        self.published(self.inner.register_table(table, location).await)
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.fault()?;
        self.published(self.inner.update_table(commit).await)
    }

    async fn publish_create_table(&self, table: Table) -> iceberg::Result<Table> {
        self.fault()?;
        let published = self.inner.publish_create_table(table).await;
        self.lost_ack(self.published(published))
    }

    async fn publish_replace_table(
        &self,
        table: Table,
        base: Option<String>,
    ) -> iceberg::Result<Table> {
        self.fault()?;
        let published = self.inner.publish_replace_table(table, base).await;
        self.lost_ack(self.published(published))
    }
}

fn session() -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .build()
        .expect("session")
}

async fn catalog_at(kind: &str, root: &Path) -> Arc<dyn Catalog> {
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
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load("memory", props)
            .await
            .expect("catalog"),
    );
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

async fn read_through(catalog: Arc<dyn Catalog>, target: &str) -> String {
    let reader = session();
    reader
        .register_iceberg_catalog("fault", catalog)
        .await
        .expect("register reader");
    run(
        &reader,
        &format!("SELECT * FROM fault.ns.{target} ORDER BY 1"),
    )
    .await
    .unwrap_or_else(|_| "absent".to_string())
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

fn version(path: &str) -> u64 {
    let name = Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    name.trim_start_matches('v')
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap_or(0)
}

fn local(path: &str) -> String {
    let path = path.strip_prefix("file:").unwrap_or(path);
    format!("/{}", path.trim_start_matches('/'))
}

async fn orphans(table: Option<&Table>, table_dir: &Path) -> usize {
    let on_disk = files_under(&table_dir.join("data"));
    let mut referenced = BTreeSet::new();
    let Some(table) = table else {
        return on_disk.len();
    };
    for snapshot in table.metadata().snapshots() {
        let list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .expect("list");
        for manifest in list.entries() {
            for entry in manifest
                .load_manifest(table.file_io())
                .await
                .expect("manifest")
                .entries()
            {
                referenced.insert(local(entry.data_file().file_path()));
            }
        }
    }
    on_disk.difference(&referenced).count()
}

async fn reopen(root: &Path, kind: &str, target: &str, newest: Option<String>) -> String {
    let Some(newest) = newest else {
        return "absent".to_string();
    };
    let cold = catalog_at(kind, &root.join("cold")).await;
    cold.register_table(&ident(target), newest)
        .await
        .expect("reopen register");
    read_through(cold, target).await
}

fn hadoop_walk(table_dir: &Path) -> Option<String> {
    let files: Vec<String> = metadata_files(table_dir)
        .into_iter()
        .filter(|path| {
            Path::new(path)
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with('v'))
        })
        .collect();
    let find = |wanted: u64| files.iter().find(|path| version(path) == wanted).cloned();
    let hinted = std::fs::read_to_string(table_dir.join("metadata/version-hint.text")).ok();
    let mut at = hinted
        .and_then(|text| text.trim().parse().ok())
        .filter(|v| find(*v).is_some())
        .or_else(|| files.iter().map(|path| version(path)).max())?;
    while find(at + 1).is_some() {
        at += 1;
    }
    find(at)
}

struct Outcome {
    failed: Option<String>,
    fresh: String,
    reopened: String,
    walked: String,
    orphans: usize,
    rejected_files: Vec<String>,
}

async fn cell(stmt: &str, kind: &str, target: &str, point: u8) -> Outcome {
    let tmp = tempfile::TempDir::new().expect("root");
    let root = tmp.path();
    let inner = catalog_at(kind, root).await;
    let fault = Arc::new(FaultCatalog {
        inner: Arc::clone(&inner),
        point: AtomicU8::new(NONE),
        spent: AtomicBool::new(false),
        tripped: AtomicBool::new(false),
    });
    let writer = session();
    writer
        .register_iceberg_catalog("fault", Arc::clone(&fault) as Arc<dyn Catalog>)
        .await
        .expect("register");
    for sql in SEEDS {
        run(&writer, sql).await.expect("seed");
    }
    let table_dir: PathBuf = root.join("ns").join(target);
    let before: BTreeSet<String> = metadata_files(&table_dir).into_iter().collect();
    fault.tripped.store(false, Ordering::SeqCst);
    fault.point.store(point, Ordering::SeqCst);
    let failed = run(&writer, stmt).await.err();
    fault.point.store(NONE, Ordering::SeqCst);
    let after = metadata_files(&table_dir);
    let newest = after.iter().max_by_key(|path| version(path)).cloned();
    let loaded = inner.load_table(&ident(target)).await.ok();
    Outcome {
        failed,
        fresh: read_through(Arc::clone(&inner), target).await,
        reopened: reopen(root, kind, target, newest).await,
        walked: if kind == "hadoop" {
            reopen(root, kind, target, hadoop_walk(&table_dir)).await
        } else {
            "-".to_string()
        },
        orphans: orphans(loaded.as_ref(), &table_dir).await,
        rejected_files: after
            .into_iter()
            .filter(|path| !before.contains(path))
            .collect(),
    }
}

fn changed_rows(head: &str) -> &'static str {
    match head {
        "CTAS" => "rows id,v;7,70",
        "RTAS" => "rows id,v;8,80",
        _ => "rows ",
    }
}

fn unchanged_rows(head: &str) -> &'static str {
    if head == "RTAS" { PRE_T } else { "absent" }
}

fn point_of(name: &str) -> Option<u8> {
    match name {
        "none" => Some(NONE),
        "catalog_update_before_publish" => Some(BEFORE),
        "listing_after_publish" => Some(LIST),
        "load_after_publish" => Some(LOAD),
        "refresh_after_publish" => Some(REFRESH),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_commit_table_d2_rows_and_their_controls_hold_on_the_spark_door() {
    let mut pinned = Vec::new();
    let mut skipped = Vec::new();
    for line in CELLS.lines().filter(|line| !line.trim().is_empty()) {
        let row: Value = serde_json::from_str(line).expect("cell row");
        let text = |key: &str| row[key].as_str().unwrap_or_default().to_string();
        let (head, point_name, group) = (text("head"), text("point"), text("group"));
        if text("door") != DOOR || !STAGED_HEADS.contains(&head.as_str()) {
            continue;
        }
        let Some(point) = point_of(&point_name) else {
            skipped.push(text("view"));
            continue;
        };
        let target = if head == "RTAS" { "t" } else { "c" };
        let kind = text("catalog");
        let seen = cell(&text("stmt"), &kind, target, point).await;
        let view = text("view");
        if point == BEFORE {
            assert_eq!(group, "D2", "{view}");
            assert!(
                seen.failed
                    .as_deref()
                    .is_some_and(|e| e.contains("catalog update unavailable")),
                "{view}"
            );
            assert!(
                seen.rejected_files.is_empty(),
                "{view}: {:?}",
                seen.rejected_files
            );
            assert_eq!(seen.fresh, unchanged_rows(&head), "{view}");
            assert_eq!(seen.reopened, unchanged_rows(&head), "{view}");
            assert_eq!(seen.orphans, 0, "{view}");
            if kind == "hadoop" {
                assert_eq!(seen.walked, unchanged_rows(&head), "{view}");
            }
        } else {
            assert_eq!(text("class"), "EQUAL", "{view}");
            assert!(seen.failed.is_none(), "{view}: {:?}", seen.failed);
            assert_eq!(seen.fresh, changed_rows(&head), "{view}");
            assert_eq!(seen.reopened, changed_rows(&head), "{view}");
        }
        pinned.push(view);
    }
    assert_eq!(pinned.len(), 30, "{pinned:?}");
    assert_eq!(skipped.len(), 9, "{skipped:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_format_version_3_create_leaves_no_table_on_either_catalog() {
    for kind in ["memory", "hadoop"] {
        let stmt = "CREATE TABLE fault.ns.c USING iceberg TBLPROPERTIES ('format-version'='3') AS SELECT CAST(7 AS INT) AS id, CAST(70 AS INT) AS v";
        let landed = cell(stmt, kind, "c", NONE).await;
        assert_eq!(
            landed.reopened, "rows id,v;7,70",
            "{kind}: {:?}",
            landed.failed
        );
        let seen = cell(stmt, kind, "c", BEFORE).await;
        assert!(
            seen.failed
                .as_deref()
                .is_some_and(|e| e.contains("catalog update unavailable")),
            "{kind}: {:?}",
            seen.failed
        );
        assert!(
            seen.rejected_files.is_empty(),
            "{kind}: {:?}",
            seen.rejected_files
        );
        assert_eq!(
            (seen.fresh.as_str(), seen.reopened.as_str(), seen.orphans),
            ("absent", "absent", 0),
            "{kind}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_publish_outcome_keeps_the_staged_files_and_reports_unknown() {
    let stmt = "CREATE OR REPLACE TABLE fault.ns.t USING iceberg TBLPROPERTIES ('format-version'='2') AS SELECT CAST(8 AS INT) AS id, CAST(80 AS INT) AS v";
    for kind in ["memory", "hadoop"] {
        let seen = cell(stmt, kind, "t", UNKNOWN).await;
        assert!(
            seen.failed
                .as_deref()
                .is_some_and(|e| e.contains("CommitStateUnknown")),
            "{kind}: {:?}",
            seen.failed
        );
        assert_eq!(seen.rejected_files.len(), 1, "{kind}");
        assert_eq!(
            (seen.fresh.as_str(), seen.reopened.as_str()),
            (PRE_T, "rows id,v;8,80"),
            "{kind}"
        );
        assert_eq!(seen.orphans, 1, "{kind}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_next_write_after_a_rejected_hadoop_replace_takes_the_freed_version() {
    let tmp = tempfile::TempDir::new().expect("root");
    let inner = catalog_at("hadoop", tmp.path()).await;
    let fault = Arc::new(FaultCatalog {
        inner: Arc::clone(&inner),
        point: AtomicU8::new(NONE),
        spent: AtomicBool::new(false),
        tripped: AtomicBool::new(false),
    });
    let writer = session();
    writer
        .register_iceberg_catalog("fault", Arc::clone(&fault) as Arc<dyn Catalog>)
        .await
        .expect("register");
    for sql in SEEDS {
        run(&writer, sql).await.expect("seed");
    }
    fault.point.store(BEFORE, Ordering::SeqCst);
    let rtas = "CREATE OR REPLACE TABLE fault.ns.t USING iceberg AS SELECT CAST(8 AS INT) AS id, CAST(80 AS INT) AS v";
    assert!(run(&writer, rtas).await.is_err());
    fault.point.store(NONE, Ordering::SeqCst);
    run(&writer, "INSERT INTO fault.ns.t VALUES (4, 40)")
        .await
        .expect("next write");
    let table_dir = tmp.path().join("ns").join("t");
    let walked = hadoop_walk(&table_dir).expect("walk");
    assert!(walked.ends_with("/v3.metadata.json"), "{walked}");
    assert_eq!(
        reopen(tmp.path(), "hadoop", "t", Some(walked)).await,
        "rows id,v;1,10;2,20;3,30;4,40"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publish_that_landed_before_its_error_keeps_every_committed_file() {
    let heads = [
        (
            "CTAS",
            "c",
            "CREATE TABLE fault.ns.c USING iceberg AS SELECT CAST(7 AS INT) AS id, CAST(70 AS INT) AS v",
        ),
        (
            "RTAS",
            "t",
            "CREATE OR REPLACE TABLE fault.ns.t USING iceberg AS SELECT CAST(8 AS INT) AS id, CAST(80 AS INT) AS v",
        ),
        (
            "CREATE TABLE",
            "c",
            "CREATE TABLE fault.ns.c (id INT, v INT) USING iceberg",
        ),
    ];
    for kind in ["memory", "hadoop"] {
        for (head, target, stmt) in heads {
            let seen = cell(stmt, kind, target, LOST_ACK).await;
            let view = format!("{head} {kind}");
            assert!(
                seen.failed
                    .as_deref()
                    .is_some_and(|e| e.contains("acknowledgement lost")),
                "{view}: {:?}",
                seen.failed
            );
            assert_eq!(
                (seen.fresh.as_str(), seen.reopened.as_str(), seen.orphans),
                (changed_rows(head), changed_rows(head), 0),
                "{view}"
            );
        }
    }
}
