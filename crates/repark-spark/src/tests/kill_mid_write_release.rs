use super::common::*;
use super::kill_mid_write::{
    await_marker_or_exit, cell, child_output, newest_metadata, read_marker, seed, spawn_child,
    spawn_child_with,
};
use super::kill_mid_write_hold::HOLD_LIMIT;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const ORPHANING_PARENT: &str = "REPARK_KILL_MID_WRITE_ORPHANING_PARENT";
const ORPHANING_TEST: &str = "tests::kill_mid_write_release::kill_mid_write_orphaning_parent";
const EARLY_ORPHANING_PARENT: &str = "REPARK_KILL_MID_WRITE_EARLY_ORPHANING_PARENT";
const EARLY_ORPHANING_TEST: &str =
    "tests::kill_mid_write_release::kill_mid_write_early_orphaning_parent";
const RELEASE_BOUND: Duration = Duration::from_secs(10);

fn still_running(pid: &str) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|stat| {
            stat.rsplit_once(')')
                .map(|(_, rest)| rest.trim_start().to_string())
        })
        .is_some_and(|rest| !rest.starts_with('Z'))
}

fn release_of(root: &Path) -> String {
    std::fs::read_to_string(root.join("released")).unwrap_or_else(|_| "no release file".to_string())
}

async fn seeded_root() -> (TempDir, String) {
    let root = TempDir::new().expect("the cell's directory");
    let warehouse = root.path().join("warehouse");
    std::fs::create_dir(&warehouse).expect("the warehouse directory");
    let pre = seed(&warehouse, cell("K-1"), 2).await;
    std::fs::write(root.path().join("pre"), &pre.metadata).expect("record the pre-state");
    (root, pre.metadata)
}

fn run_parent_that_leaves_its_child(root: &Path, key: &str, test: &str) -> String {
    let status = Command::new(std::env::current_exe().expect("the test binary"))
        .args([test, "--exact", "--test-threads=1", "--nocapture"])
        .env(key, root)
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(root.join("parent.out")).expect("parent stdout"))
        .stderr(std::fs::File::create(root.join("parent.err")).expect("parent stderr"))
        .status()
        .expect("run the parent that leaves its child");
    assert!(
        status.success(),
        "the parent must exit 0 and leave its child, got {status}\n{}",
        child_output(root)
    );
    std::fs::read_to_string(root.join("child.pid")).expect("the child's pid")
}

async fn await_gone(pid: &str) {
    let gone_by = Instant::now() + RELEASE_BOUND;
    while still_running(pid) {
        if Instant::now() > gone_by {
            Command::new("kill")
                .args(["-9", pid])
                .status()
                .expect("kill the stranded child");
            panic!("a held child outlived its parent by 10 s (the hold limit is {HOLD_LIMIT:?})");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn assert_released_without_commit(root: &Path, pre: &str, reason: &str) {
    assert_eq!(
        release_of(root),
        format!("released {reason} 75"),
        "the child must end its own hold, for {reason}, with exit code 75"
    );
    let marker = read_marker(&root.join("marker"));
    assert_eq!(
        newest_metadata(&marker.location),
        pre,
        "the released child must not commit: no metadata file past the pre-state"
    );
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
async fn kill_mid_write_early_orphaning_parent() {
    let Ok(root) = std::env::var(EARLY_ORPHANING_PARENT) else {
        return;
    };
    let root = PathBuf::from(root);
    let child = spawn_child(cell("K-1"), 2, &root);
    std::fs::write(root.join("child.pid"), child.id().to_string()).expect("record the child");
    std::process::exit(0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_held_child_whose_parent_is_gone_exits_without_committing() {
    let (root, pre) = seeded_root().await;
    let pid = run_parent_that_leaves_its_child(root.path(), ORPHANING_PARENT, ORPHANING_TEST);
    await_gone(&pid).await;
    assert_released_without_commit(root.path(), &pre, "parent");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_child_whose_parent_is_gone_before_it_starts_releases_at_the_hold_without_committing() {
    let (root, pre) = seeded_root().await;
    let pid =
        run_parent_that_leaves_its_child(root.path(), EARLY_ORPHANING_PARENT, EARLY_ORPHANING_TEST);
    await_gone(&pid).await;
    assert_released_without_commit(root.path(), &pre, "parent");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_held_child_whose_live_parent_never_kills_it_is_released_at_the_hold_limit() {
    let (root, pre) = seeded_root().await;
    let limit = Duration::from_secs(1);
    let mut child = spawn_child_with(cell("K-1"), 2, root.path(), Some(limit));
    let early_exit =
        await_marker_or_exit(&mut child, &root.path().join("marker"), root.path()).await;
    assert!(
        early_exit.is_none(),
        "the child must reach the hold\n{}",
        child_output(root.path())
    );
    let held_at = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll the held child") {
            break status;
        }
        if held_at.elapsed() > RELEASE_BOUND {
            child.kill().expect("kill the stranded child");
            child.wait().expect("reap the stranded child");
            panic!("a held child with a {limit:?} hold limit was still holding after 10 s");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let held = held_at.elapsed();
    assert_eq!(
        status.code(),
        Some(75),
        "the hold limit releases with exit code 75, got {status}"
    );
    assert!(
        held + Duration::from_millis(100) >= limit,
        "the child left its hold after {held:?}, before the {limit:?} limit"
    );
    assert_released_without_commit(root.path(), &pre, "deadline");
}
