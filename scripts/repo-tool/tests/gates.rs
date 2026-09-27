use repark_repo::{gates, repository::Repository};
use serde_json::json;
use std::{fs, process::Command};

#[test]
fn gates_checks_required_targets_and_literal_pins_without_execution() {
    let dir = tempfile::tempdir().unwrap();
    Command::new("git")
        .args(["init", "-q"])
        .current_dir(dir.path())
        .status()
        .unwrap();
    fs::write(
        dir.path().join("Makefile"),
        "ci:\nverify:\npreflight:\nRUFF := uvx ruff@0.15.22\n",
    )
    .unwrap();
    fs::write(dir.path().join("Cargo.toml"), "datafusion = \"50\"\n").unwrap();
    let config = json!({"version":1,"targets":["ci","verify","preflight"],"pins":[{"name":"datafusion","kind":"toml","path":"Cargo.toml","key":"datafusion","expected":"50"},{"name":"ruff","kind":"make","path":"Makefile","key":"RUFF","expected":"uvx ruff@0.15.22"}]}).to_string();
    fs::write(dir.path().join("gates.json"), config).unwrap();
    Command::new("git")
        .args(["add", "."])
        .current_dir(dir.path())
        .status()
        .unwrap();
    let repo = Repository::load(dir.path(), "worktree").unwrap();
    let args = ["--config".to_owned(), "gates.json".to_owned()];
    let answer = gates::run(&repo, &args).unwrap();
    assert_eq!(answer["ok"], true);
    assert_eq!(answer, gates::run(&repo, &args).unwrap());
    fs::write(dir.path().join("Cargo.toml"), "datafusion = \"51\"\n").unwrap();
    let repo = Repository::load(dir.path(), "worktree").unwrap();
    assert_eq!(gates::run(&repo, &args).unwrap()["ok"], false);
    fs::write(
        dir.path().join("Makefile"),
        "ci:\nverify:\npreflight:\n# RUFF := uvx ruff@0.15.22\n",
    )
    .unwrap();
    let repo = Repository::load(dir.path(), "worktree").unwrap();
    assert!(
        gates::run(&repo, &args).unwrap()["diagnostics"]
            .to_string()
            .contains("ruff key RUFF absent")
    );
    fs::write(dir.path().join("gates.json"), json!({"version":1,"targets":["ci"],"pins":[{"name":"x","kind":"toml","path":"Cargo.toml","key":"datafusion","expected":"51"}]}).to_string()).unwrap();
    let repo = Repository::load(dir.path(), "worktree").unwrap();
    assert!(gates::run(&repo, &args).is_err());
    fs::write(dir.path().join("gates.json"), json!({"version":1,"targets":["ci","verify","preflight","absent"],"pins":[{"name":"ruff","kind":"make","path":"Makefile","key":"RUFF","expected":"uvx ruff@0.15.22"}]}).to_string()).unwrap();
    let repo = Repository::load(dir.path(), "worktree").unwrap();
    assert!(
        gates::run(&repo, &args).unwrap()["diagnostics"]
            .to_string()
            .contains("Makefile target absent: absent")
    );
    fs::write(
        dir.path().join("Makefile"),
        "ci:\nverify:\npreflight:\nRUFF := uvx ruff@0.15.22\nRUFF := uvx ruff@0.15.22\n",
    )
    .unwrap();
    let repo = Repository::load(dir.path(), "worktree").unwrap();
    assert!(gates::run(&repo, &args).is_err());
}
