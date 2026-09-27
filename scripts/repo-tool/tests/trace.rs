use repark_repo::{repository::Repository, trace};
use std::{fs, process::Command};

#[test]
fn trace_reports_only_explicit_clause_pin_markers() {
    let dir = tempfile::tempdir().unwrap();
    Command::new("git")
        .args(["init", "-q"])
        .current_dir(dir.path())
        .status()
        .unwrap();
    let path = dir.path().join("task/ledgers/staging/u1-ledger.md");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, "| C-001 | a | PROVEN | recorded evidence |\n| C-002 | b | proof | PROVEN | no pin |\n| C-001 | duplicate | x | OPEN | x |\n").unwrap();
    let citation = dir.path().join("scripts/test/map.md");
    fs::create_dir_all(citation.parent().unwrap()).unwrap();
    fs::write(citation, "pins\u{003a} u1/C-001, C-999\n").unwrap();
    Command::new("git")
        .args(["add", "."])
        .current_dir(dir.path())
        .status()
        .unwrap();
    let repo = Repository::load(dir.path(), "worktree").unwrap();
    let answer = trace::run(&repo, &["--unit".to_owned(), "u1".to_owned()]).unwrap();
    assert_eq!(answer["ok"], false);
    let diagnostics = answer["diagnostics"].to_string();
    assert!(diagnostics.contains("duplicate clause"));
    assert!(diagnostics.contains("unknown pin"));
    assert!(diagnostics.contains("unpinned PROVEN"));
    assert_eq!(answer["clauses"].as_array().unwrap().len(), 2);
    assert!(
        answer["clauses"][0]["pin_markers"][0]
            .as_str()
            .unwrap()
            .contains("scripts/test/map.md")
    );
    assert_eq!(
        answer,
        trace::run(&repo, &["--unit".to_owned(), "u1".to_owned()]).unwrap()
    );
    let missing = trace::run(&repo, &["--unit".to_owned(), "absent".to_owned()]).unwrap();
    assert_eq!(missing["ok"], false);
    assert!(missing["diagnostics"].to_string().contains("no ledger"));
}

#[test]
fn trace_normalizes_archived_unit_and_detects_duplicate_unit() {
    let dir = tempfile::tempdir().unwrap();
    Command::new("git")
        .args(["init", "-q"])
        .current_dir(dir.path())
        .status()
        .unwrap();
    for path in [
        "task/ledgers/staging/u2-ledger.md",
        "task/ledgers/archive/2026-09/2026-09-27-u2-ledger.md",
    ] {
        let file = dir.path().join(path);
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, "| C-001 | claim | proof | OPEN |\n").unwrap();
    }
    Command::new("git")
        .args(["add", "."])
        .current_dir(dir.path())
        .status()
        .unwrap();
    let repo = Repository::load(dir.path(), "worktree").unwrap();
    let answer = trace::run(&repo, &["--unit".to_owned(), "u2".to_owned()]).unwrap();
    assert_eq!(answer["ok"], false);
    assert!(
        answer["diagnostics"]
            .to_string()
            .contains("duplicate unit u2")
    );
}

#[test]
fn trace_requires_clause_rows_and_unfenced_pin_citations() {
    let dir = tempfile::tempdir().unwrap();
    Command::new("git")
        .args(["init", "-q"])
        .current_dir(dir.path())
        .status()
        .unwrap();
    let ledger = dir.path().join("task/ledgers/staging/u-ledger.md");
    let citation = dir.path().join("scripts/map.md");
    fs::create_dir_all(ledger.parent().unwrap()).unwrap();
    fs::create_dir_all(citation.parent().unwrap()).unwrap();
    fs::write(&ledger, "| Clause | Requirement | Verdict | Evidence |\n|---|---|---|---|\n| C-001 | claim | PROVEN | source |\n").unwrap();
    fs::write(&citation, "```text\npins\u{003a} u/C-001\n```\n").unwrap();
    Command::new("git")
        .args(["add", "."])
        .current_dir(dir.path())
        .status()
        .unwrap();
    let arguments = ["--unit".to_owned(), "u".to_owned()];
    let repository = Repository::load(dir.path(), "worktree").unwrap();
    let fenced = trace::run(&repository, &arguments).unwrap();
    assert_eq!(fenced["ok"], false);
    assert!(
        fenced["diagnostics"]
            .to_string()
            .contains("unpinned PROVEN")
    );
    fs::write(&citation, "pins\u{003a} u/C-001\n").unwrap();
    let repository = Repository::load(dir.path(), "worktree").unwrap();
    assert_eq!(trace::run(&repository, &arguments).unwrap()["ok"], true);
    fs::write(
        &ledger,
        "| Clause | Requirement | Verdict | Evidence |\n|---|---|---|---|\n",
    )
    .unwrap();
    fs::write(&citation, "").unwrap();
    let repository = Repository::load(dir.path(), "worktree").unwrap();
    let empty = trace::run(&repository, &arguments).unwrap();
    assert_eq!(empty["ok"], false);
    assert!(empty["diagnostics"].to_string().contains("no clause rows"));
}
