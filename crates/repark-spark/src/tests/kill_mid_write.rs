use super::super::*;
use super::common::*;
use super::kill_mid_write_hold::{
    HOLD_LIMIT, HoldCatalog, HoldMode, local_path, reachable, snapshot_ids,
};
use super::session_write_conf::set_session_conf;
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const CHILD_CELL: &str = "REPARK_KILL_MID_WRITE_CHILD";
const CHILD_DIR: &str = "REPARK_KILL_MID_WRITE_DIR";
const CHILD_TEST: &str = "tests::kill_mid_write::kill_mid_write_child";
const ORPHANING_PARENT: &str = "REPARK_KILL_MID_WRITE_ORPHANING_PARENT";
const ORPHANING_TEST: &str = "tests::kill_mid_write::kill_mid_write_orphaning_parent";
const SESSION_KEY: &str = "spark.sql.iceberg.snapshot-property.k";
const MERGE: &str = "MERGE INTO fault.sales.t AS t USING src AS s ON t.id = s.id \
                     WHEN MATCHED AND s.id = 2 THEN UPDATE SET name = 'm' \
                     WHEN NOT MATCHED THEN INSERT *";
const INSERT: &str = "INSERT INTO fault.sales.t SELECT id + 10, name FROM src";
const MERGE_ON_READ: &str = ", 'write.merge.mode' = 'merge-on-read', \
                             'write.delete.mode' = 'merge-on-read', \
                             'write.update.mode' = 'merge-on-read'";
const KINDS: [&str; 5] = ["data", "deletes", "manifests", "lists", "metadata"];

struct Cell {
    id: &'static str,
    props: &'static str,
    statement: &'static str,
    session_key: bool,
    mode: HoldMode,
    creates: bool,
    orphans: [usize; 5],
}

const CELLS: [Cell; 9] = [
    Cell {
        id: "K-1",
        props: "",
        statement: INSERT,
        session_key: false,
        mode: HoldMode::Hold,
        creates: false,
        orphans: [1, 0, 1, 1, 0],
    },
    Cell {
        id: "K-2",
        props: "",
        statement: INSERT,
        session_key: true,
        mode: HoldMode::Hold,
        creates: false,
        orphans: [1, 0, 1, 1, 0],
    },
    Cell {
        id: "K-3",
        props: "",
        statement: "INSERT OVERWRITE fault.sales.t SELECT id + 20, name FROM src",
        session_key: false,
        mode: HoldMode::Hold,
        creates: false,
        orphans: [1, 0, 2, 1, 0],
    },
    Cell {
        id: "K-4",
        props: "",
        statement: MERGE,
        session_key: false,
        mode: HoldMode::Hold,
        creates: false,
        orphans: [1, 0, 2, 1, 0],
    },
    Cell {
        id: "K-5",
        props: MERGE_ON_READ,
        statement: MERGE,
        session_key: false,
        mode: HoldMode::Hold,
        creates: false,
        orphans: [1, 1, 2, 1, 0],
    },
    Cell {
        id: "K-6",
        props: "",
        statement: "DELETE FROM fault.sales.t WHERE id = 2",
        session_key: false,
        mode: HoldMode::Hold,
        creates: false,
        orphans: [1, 0, 2, 1, 0],
    },
    Cell {
        id: "K-7",
        props: "",
        statement: "CREATE TABLE fault.sales.c USING iceberg \
                    TBLPROPERTIES ('format-version' = '{format}') AS SELECT id, name FROM src",
        session_key: false,
        mode: HoldMode::Hold,
        creates: true,
        orphans: [0, 0, 0, 0, 0],
    },
    Cell {
        id: "K-C0",
        props: "",
        statement: INSERT,
        session_key: false,
        mode: HoldMode::Forward,
        creates: false,
        orphans: [0, 0, 0, 0, 0],
    },
    Cell {
        id: "K-C1",
        props: "",
        statement: INSERT,
        session_key: false,
        mode: HoldMode::ForwardThenHold,
        creates: false,
        orphans: [0, 0, 0, 0, 0],
    },
];

#[derive(Debug, PartialEq)]
struct Rows {
    rendered: String,
    types: Vec<DataType>,
}

#[derive(Debug)]
struct State {
    metadata: String,
    current: Option<i64>,
    snapshots: BTreeSet<i64>,
    rows: Rows,
}

struct Marker {
    location: String,
    snapshots: BTreeSet<i64>,
    summary_keys: BTreeSet<String>,
    files: BTreeMap<String, String>,
    formats: BTreeMap<String, String>,
}

fn cell(id: &str) -> &'static Cell {
    CELLS
        .iter()
        .find(|cell| cell.id == id)
        .unwrap_or_else(|| panic!("no cell {id}"))
}

fn table_name(cell: &Cell) -> &'static str {
    if cell.creates { "c" } else { "t" }
}

async fn rows_of(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Rows {
    let frame = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let types = frame
        .schema()
        .as_arrow()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect();
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect {sql}: {error}"));
    let rendered = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("render rows")
        .to_string();
    Rows { rendered, types }
}

async fn state_of(ctx: &SessionContext, catalogs: &CatalogRegistry, name: &str) -> State {
    let ident = TableIdent::from_strs(["sales", name]).expect("table ident");
    let table = catalogs["ice"]
        .load_table(&ident)
        .await
        .unwrap_or_else(|error| panic!("load sales.{name}: {error}"));
    State {
        metadata: local_path(table.metadata_location().expect("a metadata file")),
        current: table.metadata().current_snapshot_id(),
        snapshots: snapshot_ids(table.metadata()),
        rows: rows_of(
            ctx,
            catalogs,
            &format!("SELECT * FROM ice.sales.{name} ORDER BY id"),
        )
        .await,
    }
}

fn files_under(dir: &Path) -> BTreeSet<String> {
    let mut files = BTreeSet::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).expect("list a table directory") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.insert(path.display().to_string());
            }
        }
    }
    files
}

fn newest_metadata(location: &str) -> String {
    let metadata_dir = Path::new(location).join("metadata");
    files_under(&metadata_dir)
        .into_iter()
        .filter(|path| path.ends_with(".metadata.json"))
        .max_by_key(|path| {
            path.rsplit('/')
                .next()
                .and_then(|name| name.split_once('-'))
                .and_then(|(version, _)| version.parse::<u64>().ok())
                .expect("a versioned metadata file name")
        })
        .expect("at least one metadata file")
}

fn age_every_file(location: &str) {
    let stamp = std::time::SystemTime::now() - Duration::from_hours(48);
    for path in files_under(Path::new(location)) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open a file to age it")
            .set_times(
                std::fs::FileTimes::new()
                    .set_modified(stamp)
                    .set_accessed(stamp),
            )
            .expect("age a file");
    }
}

fn read_marker(path: &Path) -> Marker {
    let text = std::fs::read_to_string(path).expect("read the marker");
    let mut marker = Marker {
        location: String::new(),
        snapshots: BTreeSet::new(),
        summary_keys: BTreeSet::new(),
        files: BTreeMap::new(),
        formats: BTreeMap::new(),
    };
    for line in text.lines() {
        let (kind, value) = line.split_once(' ').expect("a marker line");
        match kind {
            "location" => marker.location = value.to_string(),
            "snapshot" => {
                marker
                    .snapshots
                    .insert(value.parse().expect("a snapshot id"));
            }
            "summary" => {
                marker.summary_keys.insert(value.to_string());
            }
            "format" => {
                let (format, path) = value.split_once(' ').expect("a format line");
                marker.formats.insert(path.to_string(), format.to_string());
            }
            _ => {
                marker.files.insert(value.to_string(), kind.to_string());
            }
        }
    }
    marker
}

fn child_output(root: &Path) -> String {
    let read = |name: &str| std::fs::read_to_string(root.join(name)).unwrap_or_default();
    format!(
        "stdout:\n{}\nstderr:\n{}",
        read("child.out"),
        read("child.err")
    )
}

async fn await_marker_or_exit(child: &mut Child, marker: &Path, root: &Path) -> Option<ExitStatus> {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        if marker.exists() {
            return None;
        }
        if let Some(status) = child.try_wait().expect("poll the child") {
            return Some(status);
        }
        if Instant::now() > deadline {
            child.kill().expect("kill the late child");
            child.wait().expect("reap the late child");
            panic!(
                "the child never reached the hold in 60 s\n{}",
                child_output(root)
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn await_exit(child: &mut Child, root: &Path) -> ExitStatus {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        if let Some(status) = child.try_wait().expect("poll the child") {
            return status;
        }
        if Instant::now() > deadline {
            child.kill().expect("kill the late child");
            child.wait().expect("reap the late child");
            panic!(
                "the forwarding child never exited in 60 s\n{}",
                child_output(root)
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn spawn_child(cell: &Cell, format: u8, root: &Path) -> Child {
    Command::new(std::env::current_exe().expect("the test binary"))
        .args([CHILD_TEST, "--exact", "--test-threads=1", "--nocapture"])
        .env(CHILD_CELL, format!("{}:{format}", cell.id))
        .env(CHILD_DIR, root)
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(root.join("child.out")).expect("child stdout file"))
        .stderr(std::fs::File::create(root.join("child.err")).expect("child stderr file"))
        .spawn()
        .expect("spawn the child")
}

async fn seed(warehouse: &Path, cell: &Cell, format: u8) -> State {
    let (ctx, catalogs) = setup_v3_at(warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg \
             TBLPROPERTIES ('format-version' = '{format}'{})",
            cell.props
        ),
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    state_of(&ctx, &catalogs, "t").await
}

async fn register_hold(
    ctx: &SessionContext,
    catalogs: &mut CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    warehouse: &Path,
) {
    repark_iceberg::catalog::register_iceberg_catalog(ctx, "fault", catalog.clone())
        .await
        .unwrap_or_else(|error| panic!("register the hold catalog: {error}"));
    catalogs.insert(
        "fault".to_string(),
        catalog,
        LocationPolicy::TempFallbackAllowed {
            root: warehouse.to_path_buf(),
        },
    );
}

fn orphan_counts(orphans: &BTreeSet<String>, marker: &Marker) -> [usize; 5] {
    let mut counts = [0; 5];
    for orphan in orphans {
        let kind = marker.files.get(orphan).unwrap_or_else(|| {
            panic!("orphan {orphan} is not a file the crashed commit wrote (ruling 82's ceiling)")
        });
        let index = KINDS
            .iter()
            .position(|known| known == kind)
            .expect("a known kind");
        counts[index] += 1;
    }
    counts
}

async fn dry_run_orphans(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    name: &str,
) -> BTreeSet<String> {
    let older_than = chrono::Utc::now().timestamp_millis() - 25 * 60 * 60 * 1000;
    let sql = format!(
        "CALL ice.system.remove_orphan_files(table => 'sales.{name}', dry_run => true, \
         older_than => {older_than})"
    );
    let batches = execute(ctx, catalogs, &sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect {sql}: {error}"));
    let mut listed = BTreeSet::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a string column");
        for index in 0..column.len() {
            listed.insert(local_path(column.value(index)));
        }
    }
    listed
}

async fn run_child_to_the_hold(cell: &Cell, format: u8, root: &Path) -> Marker {
    let id = cell.id;
    let marker_path = root.join("marker");
    let mut child = spawn_child(cell, format, root);
    let early_exit = await_marker_or_exit(&mut child, &marker_path, root).await;
    let status = match (cell.mode, early_exit) {
        (HoldMode::Forward, None) => await_exit(&mut child, root).await,
        (HoldMode::Forward, Some(status)) => status,
        (_, Some(status)) => panic!(
            "{id} v{format}: the child exited ({status}) before the hold\n{}",
            child_output(root)
        ),
        (_, None) => {
            assert!(
                child.try_wait().expect("poll the held child").is_none(),
                "{id} v{format}: the child must still be running at the kill\n{}",
                child_output(root)
            );
            child.kill().expect("kill the held child");
            child.wait().expect("reap the killed child")
        }
    };
    if cell.mode == HoldMode::Forward {
        assert!(
            status.success(),
            "{id} v{format}: the forwarding child must commit and exit 0, got {status}\n{}",
            child_output(root)
        );
    } else {
        assert_eq!(
            status.signal(),
            Some(9),
            "{id} v{format}: the child must die of SIGKILL at the hold, got {status}"
        );
    }
    let marker = read_marker(&marker_path);
    assert_eq!(
        marker.snapshots.len(),
        1,
        "{id} v{format}: the marker names one attempted snapshot"
    );
    assert_eq!(
        marker.summary_keys.contains("k"),
        cell.session_key,
        "{id} v{format}: the session snapshot property marks the routed append"
    );
    marker
}

async fn assert_reopened(
    cell: &Cell,
    format: u8,
    pre: Option<&State>,
    reopened: &State,
    marker: &Marker,
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) {
    let attempted = *marker.snapshots.iter().next().expect("one attempted id");
    let newest = newest_metadata(&marker.location);
    let id = cell.id;
    match (pre, cell.mode) {
        (Some(pre), HoldMode::Hold) => {
            assert_eq!(
                reopened.metadata, pre.metadata,
                "{id} v{format}: the newest metadata file is the pre-state's"
            );
            assert_eq!(
                reopened.current, pre.current,
                "{id} v{format}: current snapshot"
            );
            assert_eq!(
                reopened.snapshots, pre.snapshots,
                "{id} v{format}: snapshot set"
            );
            assert!(
                !reopened.snapshots.contains(&attempted),
                "{id} v{format}: the attempted snapshot is absent"
            );
            assert_eq!(reopened.rows, pre.rows, "{id} v{format}: rows and types");
        }
        (Some(pre), HoldMode::Forward | HoldMode::ForwardThenHold) => {
            assert_ne!(
                reopened.metadata, pre.metadata,
                "{id} v{format}: the commit wrote a newer metadata file"
            );
            assert_eq!(
                reopened.current,
                Some(attempted),
                "{id} v{format}: committed"
            );
            let mut expected = pre.snapshots.clone();
            expected.insert(attempted);
            assert_eq!(reopened.snapshots, expected, "{id} v{format}: snapshot set");
            let committed = rows_of(
                ctx,
                catalogs,
                "SELECT * FROM ice.sales.t WHERE id < 10 ORDER BY id",
            )
            .await;
            assert_eq!(
                committed, pre.rows,
                "{id} v{format}: the pre-state rows stay"
            );
            let added = rows_of(
                ctx,
                catalogs,
                "SELECT id - 10 AS id, name FROM ice.sales.t WHERE id > 10 ORDER BY id",
            )
            .await;
            assert_eq!(
                added.rendered, pre.rows.rendered,
                "{id} v{format}: the statement's rows, once"
            );
        }
        (None, _) => {
            assert_eq!(
                reopened.metadata,
                local_path(&newest),
                "{id} v{format}: the staged metadata file is the only commit"
            );
            assert_eq!(reopened.current, Some(attempted), "{id} v{format}: created");
            assert_eq!(
                reopened.snapshots,
                BTreeSet::from([attempted]),
                "{id} v{format}: snapshot set"
            );
            let source = rows_of(ctx, catalogs, "SELECT id, name FROM src ORDER BY id").await;
            assert_eq!(
                reopened.rows.rendered, source.rendered,
                "{id} v{format}: every CTAS row"
            );
        }
    }
}

async fn assert_orphans(
    cell: &Cell,
    format: u8,
    marker: &Marker,
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) {
    let id = cell.id;
    let name = table_name(cell);
    let reopened_table = catalogs["ice"]
        .load_table(&TableIdent::from_strs(["sales", name]).expect("table ident"))
        .await
        .expect("load the reopened table");
    let reached = reachable(&reopened_table).await;
    let orphans: BTreeSet<String> = files_under(Path::new(&marker.location))
        .into_iter()
        .filter(|path| !reached.contains_key(path))
        .collect();
    let counts = orphan_counts(&orphans, marker);
    println!(
        "{id} v{format} orphans data={} deletes={} manifests={} lists={} metadata={}",
        counts[0], counts[1], counts[2], counts[3], counts[4]
    );
    assert_eq!(counts, cell.orphans, "{id} v{format}: orphans per kind");
    let delete_format = if format == 2 { "Parquet" } else { "Puffin" };
    for orphan in &orphans {
        if marker.files.get(orphan).map(String::as_str) == Some("deletes") {
            assert_eq!(
                marker.formats.get(orphan).map(String::as_str),
                Some(delete_format),
                "{id} v{format}: the delete file {orphan} is a {delete_format} file"
            );
        }
    }
    if cell.mode == HoldMode::Hold && !cell.creates {
        let written: BTreeSet<String> = marker.files.keys().cloned().collect();
        assert_eq!(
            orphans, written,
            "{id} v{format}: the orphans are exactly the files the crashed commit wrote"
        );
    }

    age_every_file(&marker.location);
    assert_eq!(
        dry_run_orphans(ctx, catalogs, name).await,
        orphans,
        "{id} v{format}: remove_orphan_files lists the counted set"
    );
}

async fn assert_writable(
    id: &str,
    format: u8,
    name: &str,
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
) {
    let before_write = rows_of(
        ctx,
        catalogs,
        &format!("SELECT * FROM ice.sales.{name} ORDER BY id"),
    )
    .await;
    run(
        ctx,
        catalogs,
        &format!("INSERT INTO ice.sales.{name} VALUES (100, 'w')"),
    )
    .await;
    assert_eq!(
        rows_of(
            ctx,
            catalogs,
            &format!("SELECT * FROM ice.sales.{name} WHERE id <> 100 ORDER BY id"),
        )
        .await,
        before_write,
        "{id} v{format}: the reopened rows stay after a write"
    );
    assert_eq!(
        rows(
            ctx,
            catalogs,
            &format!("SELECT * FROM ice.sales.{name} WHERE id = 100 AND name = 'w'"),
        )
        .await,
        1,
        "{id} v{format}: the reopened table is writable"
    );
}

async fn kill_cell(id: &str, format: u8) {
    let started = Instant::now();
    let cell = cell(id);
    let name = table_name(cell);
    let root = TempDir::new().expect("the cell's directory");
    let warehouse = root.path().join("warehouse");
    std::fs::create_dir(&warehouse).expect("the warehouse directory");
    let pre = if cell.creates {
        None
    } else {
        let pre = seed(&warehouse, cell, format).await;
        std::fs::write(root.path().join("pre"), &pre.metadata).expect("record the pre-state");
        Some(pre)
    };
    let marker = run_child_to_the_hold(cell, format, root.path()).await;
    let newest = newest_metadata(&marker.location);
    let (ctx, catalogs) = setup_v3_at(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        &format!(
            "CALL ice.system.register_table(table => 'sales.{name}', metadata_file => '{newest}')"
        ),
    )
    .await;
    let reopened = state_of(&ctx, &catalogs, name).await;
    assert_reopened(
        cell,
        format,
        pre.as_ref(),
        &reopened,
        &marker,
        &ctx,
        &catalogs,
    )
    .await;
    assert_orphans(cell, format, &marker, &ctx, &catalogs).await;
    assert_writable(id, format, name, &ctx, &catalogs).await;
    println!("{id} v{format} took {} ms", started.elapsed().as_millis());
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_mid_write_child() {
    let Ok(selector) = std::env::var(CHILD_CELL) else {
        return;
    };
    let root = PathBuf::from(std::env::var(CHILD_DIR).expect("the parent names its directory"));
    let (id, format) = selector.split_once(':').expect("a cell:format selector");
    let cell = cell(id);
    let warehouse = root.join("warehouse");
    let (ctx, mut catalogs) = setup_v3_at(&warehouse).await;
    if !cell.creates {
        let pre = std::fs::read_to_string(root.join("pre")).expect("the pre-state file");
        run(
            &ctx,
            &catalogs,
            &format!(
                "CALL ice.system.register_table(table => 'sales.t', metadata_file => '{pre}')"
            ),
        )
        .await;
    }
    let hold = Arc::new(HoldCatalog {
        inner: catalogs["ice"].clone(),
        mode: cell.mode,
        marker: root.join("marker"),
        parent: std::os::unix::process::parent_id(),
    });
    register_hold(&ctx, &mut catalogs, hold, &warehouse).await;
    if cell.session_key {
        set_session_conf(&ctx, SESSION_KEY, "v");
    }
    run(&ctx, &catalogs, &cell.statement.replace("{format}", format)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k1_insert_select_killed_at_the_commit_v2() {
    kill_cell("K-1", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k1_insert_select_killed_at_the_commit_v3() {
    kill_cell("K-1", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k2_insert_with_a_session_write_key_killed_at_the_commit_v2() {
    kill_cell("K-2", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k2_insert_with_a_session_write_key_killed_at_the_commit_v3() {
    kill_cell("K-2", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k3_insert_overwrite_killed_at_the_commit_v2() {
    kill_cell("K-3", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k3_insert_overwrite_killed_at_the_commit_v3() {
    kill_cell("K-3", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k4_merge_copy_on_write_killed_at_the_commit_v2() {
    kill_cell("K-4", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k4_merge_copy_on_write_killed_at_the_commit_v3() {
    kill_cell("K-4", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k5_merge_on_read_killed_at_the_commit_v2() {
    kill_cell("K-5", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k5_merge_on_read_killed_at_the_commit_v3() {
    kill_cell("K-5", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k6_delete_copy_on_write_killed_at_the_commit_v2() {
    kill_cell("K-6", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k6_delete_copy_on_write_killed_at_the_commit_v3() {
    kill_cell("K-6", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k7_ctas_killed_at_publish_reads_created_on_the_local_catalog_v2() {
    kill_cell("K-7", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn k7_ctas_killed_at_publish_reads_created_on_the_local_catalog_v3() {
    kill_cell("K-7", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kc0_forwarding_hook_commits_with_no_orphans_v2() {
    kill_cell("K-C0", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kc0_forwarding_hook_commits_with_no_orphans_v3() {
    kill_cell("K-C0", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kc1_killed_after_the_metadata_file_reads_committed_v2() {
    kill_cell("K-C1", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kc1_killed_after_the_metadata_file_reads_committed_v3() {
    kill_cell("K-C1", 3).await;
}

fn still_running(pid: &str) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|stat| {
            stat.rsplit_once(')')
                .map(|(_, rest)| rest.trim_start().to_string())
        })
        .is_some_and(|rest| !rest.starts_with('Z'))
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_mid_write_orphaning_parent() {
    let Ok(root) = std::env::var(ORPHANING_PARENT) else {
        return;
    };
    let root = PathBuf::from(root);
    let mut child = spawn_child(cell("K-1"), 2, &root);
    let early_exit = await_marker_or_exit(&mut child, &root.join("marker"), &root).await;
    assert!(
        early_exit.is_none(),
        "the child must reach the hold\n{}",
        child_output(&root)
    );
    std::fs::write(root.join("child.pid"), child.id().to_string()).expect("record the child");
    std::process::exit(0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_held_child_whose_parent_is_gone_exits_without_committing() {
    let root = TempDir::new().expect("the cell's directory");
    let warehouse = root.path().join("warehouse");
    std::fs::create_dir(&warehouse).expect("the warehouse directory");
    let pre = seed(&warehouse, cell("K-1"), 2).await;
    std::fs::write(root.path().join("pre"), &pre.metadata).expect("record the pre-state");
    let status = Command::new(std::env::current_exe().expect("the test binary"))
        .args([ORPHANING_TEST, "--exact", "--test-threads=1", "--nocapture"])
        .env(ORPHANING_PARENT, root.path())
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(root.path().join("parent.out")).expect("parent stdout"))
        .stderr(std::fs::File::create(root.path().join("parent.err")).expect("parent stderr"))
        .status()
        .expect("run the parent that leaves its child");
    assert!(
        status.success(),
        "the orphaning parent must exit 0 with its child still held, got {status}\n{}",
        child_output(root.path())
    );
    let pid = std::fs::read_to_string(root.path().join("child.pid")).expect("the child's pid");
    let gone_by = Instant::now() + Duration::from_secs(10);
    while still_running(&pid) {
        if Instant::now() > gone_by {
            Command::new("kill")
                .args(["-9", &pid])
                .status()
                .expect("kill the stranded child");
            panic!("a held child outlived its parent by 10 s (the hold limit is {HOLD_LIMIT:?})");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let marker = read_marker(&root.path().join("marker"));
    assert_eq!(
        newest_metadata(&marker.location),
        pre.metadata,
        "the released child must not commit: no metadata file past the pre-state"
    );
}
