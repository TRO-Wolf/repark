use serde_json::Value;
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn git(root: &Path, arguments: &[&str]) {
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(root)
            .args(arguments)
            .output()
            .unwrap()
            .status
            .success()
    );
}

fn cli(root: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_repark-repo"))
        .arg("--repo")
        .arg(root)
        .args(arguments)
        .output()
        .unwrap()
}

#[test]
fn combined_checks_match_separate_reports_and_keep_failures() {
    let root = tempfile::tempdir().unwrap();
    git(root.path(), &["init", "-q"]);
    fs::create_dir_all(root.path().join("task/ledgers/staging")).unwrap();
    fs::write(
        root.path().join("task/ledgers/staging/demo-ledger.md"),
        "| C-001 | text | OPEN | evidence |\n",
    )
    .unwrap();
    fs::write(root.path().join("README.md"), "[missing](missing.md)\n").unwrap();
    git(root.path(), &["add", "."]);
    let combined = cli(root.path(), &["checks"]);
    assert_eq!(combined.status.code(), Some(1));
    let value: Value = serde_json::from_slice(&combined.stdout).unwrap();
    for name in ["docs-links", "ledger-grammar"] {
        let separate = cli(root.path(), &["checks", name]);
        assert_eq!(separate.status.code(), Some(1));
        let report: Value = serde_json::from_slice(&separate.stdout).unwrap();
        assert_eq!(value["checks"][name], report["checks"][name]);
        assert!(
            !report["checks"][name]["diagnostics"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn checks_reject_snapshot_cache_and_unknown_options() {
    let root = tempfile::tempdir().unwrap();
    git(root.path(), &["init", "-q"]);
    for arguments in [
        vec!["--snapshot", "index", "checks"],
        vec!["--snapshot", "HEAD", "checks"],
        vec!["--cache-dir", "cache", "checks"],
        vec!["checks", "other"],
        vec!["checks", "--write"],
        vec!["checks", "ledger-grammar"],
    ] {
        let result = cli(root.path(), &arguments);
        assert_eq!(result.status.code(), Some(2), "{arguments:?}");
        assert!(!result.stderr.is_empty());
    }
    assert!(!root.path().join("cache").exists());
}

#[test]
fn worktree_changes_and_untracked_targets_are_rechecked() {
    let root = tempfile::tempdir().unwrap();
    git(root.path(), &["init", "-q"]);
    fs::write(root.path().join("README.md"), "[target](target.txt)\n").unwrap();
    git(root.path(), &["add", "."]);
    let missing = cli(root.path(), &["checks", "docs-links"]);
    assert!(String::from_utf8_lossy(&missing.stdout).contains("does not exist"));
    fs::write(root.path().join("target.txt"), "present").unwrap();
    let untracked = cli(root.path(), &["checks", "docs-links"]);
    assert!(String::from_utf8_lossy(&untracked.stdout).contains("not tracked"));
    git(root.path(), &["add", "target.txt"]);
    assert!(cli(root.path(), &["checks", "docs-links"]).status.success());
    fs::remove_file(root.path().join("README.md")).unwrap();
    assert_eq!(
        cli(root.path(), &["checks", "docs-links"]).status.code(),
        Some(2)
    );
}

#[test]
fn make_and_ci_run_combined_document_checks_once() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let makefile = fs::read_to_string(root.join("Makefile")).unwrap();
    let prerequisites = makefile
        .lines()
        .find(|line| line.starts_with("ci:"))
        .unwrap();
    assert_eq!(
        prerequisites
            .split_whitespace()
            .filter(|word| *word == "check-repo-docs")
            .count(),
        1
    );
    assert!(
        !prerequisites
            .split_whitespace()
            .any(|word| matches!(word, "check-ledger-grammar" | "check-docs-links"))
    );
    assert!(makefile.contains("\tscripts/repo-tool.sh checks\n"));
    assert!(makefile.contains("\tscripts/repo-tool.sh checks ledger-grammar\n"));
    assert!(makefile.contains("\tscripts/repo-tool.sh checks docs-links\n"));
    let workflow = fs::read_to_string(root.join(".github/workflows/ci.yml")).unwrap();
    assert_eq!(workflow.matches("run: make check-repo-docs").count(), 1);
    assert!(!workflow.contains("run: python3 scripts/check_docs_links.py"));
    assert!(!workflow.contains("run: python3 scripts/check_ledger_grammar.py"));
}
