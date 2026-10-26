use super::super::*;
use super::common::*;
use super::kill_mid_write::{await_marker_or_exit, child_output, newest_metadata};
use super::kill_mid_write_hold::{HOLD_LIMIT, HoldCatalog, HoldMode, local_path};
use super::resource_bound::{
    D2, Lane, MERGE_KEY, MERGE_KIND, MOR_MERGE, ORDERED_KEY, ORDERED_KIND, SESSION_KEY,
    assert_refused, attempt, bounded_at, create_target, observe, order_target, register_matched,
    register_rows, seed,
};
use super::session_write_conf::set_session_conf;
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const CHILD_CELL: &str = "REPARK_RESOURCE_BOUND_KILL_CHILD";
const CHILD_DIR: &str = "REPARK_RESOURCE_BOUND_KILL_DIR";
const CHILD_PARENT: &str = "REPARK_RESOURCE_BOUND_KILL_PARENT";
const CHILD_TEST: &str = "tests::resource_bound_kill::resource_bound_kill_child";
const KINDS: [&str; 5] = ["data", "deletes", "manifests", "lists", "metadata"];

struct Pair {
    id: &'static str,
    rows: usize,
    pool_mib: usize,
    statement: &'static str,
    kind: &'static str,
    key: &'static str,
    refusing: &'static str,
    merge_on_read: bool,
}

const PAIRS: [Pair; 2] = [
    Pair {
        id: "KR-1",
        rows: 100_000,
        pool_mib: 32,
        statement: D2,
        kind: ORDERED_KIND,
        key: ORDERED_KEY,
        refusing: "0.1",
        merge_on_read: false,
    },
    Pair {
        id: "KR-2",
        rows: 200_000,
        pool_mib: 512,
        statement: MOR_MERGE,
        kind: MERGE_KIND,
        key: MERGE_KEY,
        refusing: "0.01",
        merge_on_read: true,
    },
];

fn pair(id: &str) -> &'static Pair {
    PAIRS
        .iter()
        .find(|pair| pair.id == id)
        .unwrap_or_else(|| panic!("no pair {id}"))
}

async fn prepare(ctx: &SessionContext, pair: &Pair) {
    register_rows(ctx, pair.rows);
    if pair.merge_on_read {
        register_matched(ctx, 100).await;
    } else {
        set_session_conf(ctx, SESSION_KEY, "v");
    }
}

async fn seeded(pair: &Pair, format: u8, keys: &[(&str, &str)]) -> Lane {
    let root = TempDir::new().unwrap();
    let warehouse = root.path().join("warehouse");
    std::fs::create_dir(&warehouse).unwrap();
    let (ctx, catalogs) = bounded_at(&warehouse, pair.pool_mib, keys, 1).await;
    let lane = Lane {
        root,
        ctx,
        catalogs,
    };
    create_target(&lane, format, false, pair.merge_on_read).await;
    prepare(&lane.ctx, pair).await;
    if pair.merge_on_read {
        seed(&lane, pair.rows, 32).await;
    } else {
        order_target(&lane).await;
    }
    lane
}

struct Marker {
    location: String,
    snapshots: usize,
    files: BTreeMap<String, String>,
}

fn parse_marker(path: &Path) -> Marker {
    let text = std::fs::read_to_string(path).expect("read the marker");
    let mut marker = Marker {
        location: String::new(),
        snapshots: 0,
        files: BTreeMap::new(),
    };
    for line in text.lines() {
        let (kind, value) = line.split_once(' ').expect("a marker line");
        match kind {
            "location" => marker.location = value.to_string(),
            "snapshot" => marker.snapshots += 1,
            "summary" | "format" => {}
            _ => {
                marker.files.insert(value.to_string(), kind.to_string());
            }
        }
    }
    marker
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

async fn refusal_side(pair: &Pair, format: u8) {
    let label = format!("{} v{format} refusal", pair.id);
    let lane = seeded(pair, format, &[(pair.key, pair.refusing)]).await;
    assert_refused(&lane, pair.statement, pair.kind, pair.key, &label).await;
}

async fn kill_side(pair: &Pair, format: u8) {
    let label = format!("{} v{format} kill", pair.id);
    let lane = seeded(pair, format, &[(pair.key, "0")]).await;
    let pre = observe(&lane).await;
    let table = lane.catalogs["ice"]
        .load_table(&TableIdent::from_strs(["sales", "t"]).unwrap())
        .await
        .unwrap();
    let pre_metadata = local_path(table.metadata_location().expect("a metadata file"));
    let root = lane.root.path().to_path_buf();
    std::fs::write(root.join("pre"), &pre_metadata).unwrap();
    let marker_path = root.join("marker");
    let mut child = Command::new(std::env::current_exe().expect("the test binary"))
        .args([CHILD_TEST, "--exact", "--test-threads=1", "--nocapture"])
        .env(CHILD_CELL, format!("{}:{format}", pair.id))
        .env(CHILD_DIR, &root)
        .env(CHILD_PARENT, std::process::id().to_string())
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(root.join("child.out")).unwrap())
        .stderr(std::fs::File::create(root.join("child.err")).unwrap())
        .spawn()
        .expect("spawn the child");
    let early = await_marker_or_exit(&mut child, &marker_path, &root).await;
    assert!(
        early.is_none(),
        "{label}: the child reaches the commit hold\n{}",
        child_output(&root)
    );
    child.kill().expect("kill the held child");
    let status = child.wait().expect("reap the killed child");
    assert_eq!(
        status.signal(),
        Some(9),
        "{label}: the child dies of SIGKILL"
    );
    let output = child_output(&root);
    assert!(
        !output.contains("REPARK: the "),
        "{label}: the child never refused\n{output}"
    );
    let marker = parse_marker(&marker_path);
    assert_eq!(marker.snapshots, 1, "{label}: one attempted snapshot");
    let newest = newest_metadata(&marker.location);
    assert_eq!(
        local_path(&newest),
        pre_metadata,
        "{label}: the newest metadata file is the pre-state's"
    );
    let warehouse = root.join("warehouse");
    let (ctx, catalogs) = bounded_at(&warehouse, 2048, &[], 1).await;
    run(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.register_table(table => 'sales.t', metadata_file => '{newest}')"),
    )
    .await;
    let Lane { root, .. } = lane;
    let reopened = Lane {
        root,
        ctx,
        catalogs,
    };
    let after = observe(&reopened).await;
    assert_eq!(after.current, pre.current, "{label}: current snapshot");
    assert_eq!(after.snapshots, pre.snapshots, "{label}: snapshot set");
    assert_eq!(after.batch, pre.batch, "{label}: rows and types");
    let written: BTreeSet<String> = marker.files.keys().cloned().collect();
    assert_eq!(
        after.orphans, written,
        "{label}: the orphans are exactly the files the killed commit wrote"
    );
    let mut counts = [0; 5];
    for orphan in &after.orphans {
        let kind = &marker.files[orphan];
        counts[KINDS.iter().position(|known| known == kind).unwrap()] += 1;
    }
    println!(
        "{label} orphans data={} deletes={} manifests={} lists={} metadata={}",
        counts[0], counts[1], counts[2], counts[3], counts[4]
    );
    assert!(
        counts[0] > 0,
        "{label}: the killed commit staged data files"
    );
    if pair.merge_on_read {
        assert!(
            counts[1] > 0,
            "{label}: the killed commit staged delete files"
        );
    }
    assert!(
        files_under(Path::new(&marker.location)).is_superset(&written),
        "{label}: every attempted file is on disk"
    );
    attempt(&reopened, "SELECT count(*) FROM ice.sales.t")
        .await
        .expect("the reopened table reads");
}

#[tokio::test(flavor = "multi_thread")]
async fn resource_bound_kill_child() {
    let Ok(selector) = std::env::var(CHILD_CELL) else {
        return;
    };
    let root = PathBuf::from(std::env::var(CHILD_DIR).expect("the parent names its directory"));
    let (id, _format) = selector.split_once(':').expect("a pair:format selector");
    let pair = pair(id);
    let warehouse = root.join("warehouse");
    let (ctx, mut catalogs) = bounded_at(&warehouse, pair.pool_mib, &[(pair.key, "0")], 1).await;
    let pre = std::fs::read_to_string(root.join("pre")).expect("the pre-state file");
    run(
        &ctx,
        &catalogs,
        &format!("CALL ice.system.register_table(table => 'sales.t', metadata_file => '{pre}')"),
    )
    .await;
    prepare(&ctx, pair).await;
    let hold: Arc<dyn Catalog> = Arc::new(HoldCatalog {
        inner: catalogs["ice"].clone(),
        mode: HoldMode::Hold,
        marker: root.join("marker"),
        parent: std::env::var(CHILD_PARENT)
            .expect("the parent names itself")
            .parse()
            .expect("a parent pid"),
        limit: HOLD_LIMIT,
    });
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "fault", hold.clone())
        .await
        .unwrap();
    catalogs.insert(
        "fault".to_string(),
        hold,
        LocationPolicy::TempFallbackAllowed {
            root: warehouse.clone(),
        },
    );
    let statement = pair.statement.replace("ice.sales.t", "fault.sales.t");
    run(&ctx, &catalogs, &statement).await;
}

async fn kill_against_refusal(id: &str, format: u8) {
    let pair = pair(id);
    refusal_side(pair, format).await;
    kill_side(pair, format).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kr1_ordered_insert_refuses_above_the_bound_and_dies_at_the_kill_v2() {
    kill_against_refusal("KR-1", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kr1_ordered_insert_refuses_above_the_bound_and_dies_at_the_kill_v3() {
    kill_against_refusal("KR-1", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kr2_merge_on_read_refuses_above_the_bound_and_dies_at_the_kill_v2() {
    kill_against_refusal("KR-2", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kr2_merge_on_read_refuses_above_the_bound_and_dies_at_the_kill_v3() {
    kill_against_refusal("KR-2", 3).await;
}
