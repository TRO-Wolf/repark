use repark_repo::{repository::Repository, state};
use std::{fs, process::Command};

#[test]
fn state_reports_conflicting_bins_and_invalid_closed_metadata() {
    let dir = tempfile::tempdir().unwrap();
    Command::new("git")
        .args(["init", "-q"])
        .current_dir(dir.path())
        .status()
        .unwrap();
    for path in [
        "task/ledgers/staging/u1-ledger.md",
        "task/ledgers/completed/u1-ledger.md",
    ] {
        let file = dir.path().join(path);
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, "ledger").unwrap();
    }
    fs::write(dir.path().join("STATUS.md"), "<!-- ws id=w1 state=closed closed=bad by=no history=docs/history/w1 -->\n- work\n<!-- /ws -->\n").unwrap();
    let slate = dir.path().join("briefs/next-sequence.md");
    fs::create_dir_all(slate.parent().unwrap()).unwrap();
    fs::write(slate, "| 1 | unit <!-- unit id=u1 --> |\n").unwrap();
    Command::new("git")
        .args(["add", "."])
        .current_dir(dir.path())
        .status()
        .unwrap();
    let repo = Repository::load(dir.path(), "worktree").unwrap();
    let answer = state::run(&repo, &[]).unwrap();
    assert_eq!(answer["ok"], false);
    let diagnostics = answer["diagnostics"].to_string();
    assert!(diagnostics.contains("multiple ledger bins"));
    assert!(diagnostics.contains("closed ws needs valid closed date"));
    assert_eq!(answer, state::run(&repo, &[]).unwrap());
}

#[test]
fn state_rejects_unclosed_markers_and_keeps_archived_unit() {
    let dir = tempfile::tempdir().unwrap();
    Command::new("git")
        .args(["init", "-q"])
        .current_dir(dir.path())
        .status()
        .unwrap();
    let archive = dir
        .path()
        .join("task/ledgers/archive/2026-09/2026-09-27-u-ledger.md");
    fs::create_dir_all(archive.parent().unwrap()).unwrap();
    fs::write(archive, "ledger").unwrap();
    let status = dir.path().join("STATUS.md");
    fs::write(&status, "<!-- ws id=w state=open\n").unwrap();
    let slate = dir.path().join("briefs/next-sequence.md");
    fs::create_dir_all(slate.parent().unwrap()).unwrap();
    fs::write(&slate, "| unit <!-- unit id=u\n").unwrap();
    Command::new("git")
        .args(["add", "."])
        .current_dir(dir.path())
        .status()
        .unwrap();
    let repository = Repository::load(dir.path(), "worktree").unwrap();
    let invalid = state::run(&repository, &[]).unwrap();
    assert_eq!(invalid["ok"], false);
    assert_eq!(invalid["units"][0]["bins"], serde_json::json!(["archive"]));
    assert_eq!(invalid["diagnostics"].as_array().unwrap().len(), 2);
    fs::write(&status, "<!-- ws id=w state=open -->\nwork\n<!-- /ws -->\n").unwrap();
    fs::write(&slate, "| unit <!-- unit id=u --> |\n").unwrap();
    let repository = Repository::load(dir.path(), "worktree").unwrap();
    assert_eq!(state::run(&repository, &[]).unwrap()["ok"], true);
}
