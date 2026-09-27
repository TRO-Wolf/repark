use serde_json::Value;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn git(root: &Path, arguments: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()
        .expect("git");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn cli(root: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_repark-repo"))
        .arg("--repo")
        .arg(root)
        .args(arguments)
        .output()
        .expect("CLI")
}

fn fixture() -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("fixture");
    git(root.path(), &["init", "-q"]);
    fs::write(root.path().join("map.md"), "# Map\n").expect("map");
    fs::write(root.path().join("lib.rs"), "pub fn value() {}\n").expect("source");
    git(root.path(), &["add", "."]);
    root
}

#[test]
fn public_cli_reports_findings_and_uses_selected_snapshot() {
    let root = fixture();
    fs::write(root.path().join("map.md"), "# Map\n- [dead](missing.rs)\n").expect("map");
    let working = cli(root.path(), &["maps", "--check"]);
    assert_eq!(working.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<Value>(&working.stdout).unwrap()["input_scope"],
        "maps"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&working.stdout).expect("JSON")["ok"],
        false
    );
    assert!(
        cli(root.path(), &["--snapshot", "index", "maps", "--check"])
            .status
            .success()
    );
    assert_eq!(
        cli(root.path(), &["maps", "--bogus"]).status.code(),
        Some(2)
    );
    assert_eq!(
        cli(root.path(), &["--snapshot", "index", "maps", "--write"])
            .status
            .code(),
        Some(2)
    );
}

#[cfg(unix)]
fn hook(root: &Path) -> Output {
    Command::new("bash")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../check_map_md.sh"))
        .current_dir(root)
        .env("REPO_TOOL_BINARY", env!("CARGO_BIN_EXE_repark-repo"))
        .output()
        .expect("hook")
}

#[cfg(unix)]
#[test]
fn staged_hook_accepts_current_inventory_and_ignores_unstaged_map() {
    use std::os::unix::fs::PermissionsExt;
    let root = fixture();
    fs::create_dir(root.path().join("scripts")).expect("scripts");
    let wrapper = root.path().join("scripts/repo-tool.sh");
    fs::write(
        &wrapper,
        "#!/bin/sh\nexec \"$REPO_TOOL_BINARY\" --repo \"$PWD\" \"$@\"\n",
    )
    .expect("wrapper");
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).expect("mode");
    git(
        root.path(),
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
    );
    fs::write(root.path().join("lib.rs"), "pub fn updated() {}\n").expect("source");
    git(root.path(), &["add", "lib.rs"]);
    assert!(
        cli(root.path(), &["maps", "--write", "--init", "--path", "."])
            .status
            .success()
    );
    let legacy = hook(root.path());
    assert!(legacy.status.success());
    assert!(String::from_utf8_lossy(&legacy.stderr).contains("lockstep"));
    git(root.path(), &["add", "map.md"]);
    git(
        root.path(),
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "managed inventory",
        ],
    );
    fs::write(root.path().join("lib.rs"), "pub fn next() {}\n").expect("source");
    git(root.path(), &["add", "lib.rs"]);
    fs::write(root.path().join("map.md"), "- [unstaged](missing.md)\n").expect("unstaged");
    let managed = hook(root.path());
    assert!(
        managed.status.success(),
        "{}",
        String::from_utf8_lossy(&managed.stderr)
    );
    assert!(!String::from_utf8_lossy(&managed.stderr).contains("WARNING"));
}
