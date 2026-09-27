use repark_repo::{docs_links, validation::Inputs};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

fn git(root: &Path, arguments: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn write(root: &Path, relative: &str, text: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().expect("parent")).expect("create parent");
    fs::write(path, text).expect("write fixture");
}

fn fixture() -> TempDir {
    let root = tempfile::tempdir().expect("tempdir");
    git(root.path(), &["init", "-q"]);
    root
}

fn python_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("scripts directory")
        .join("check_docs_links.py")
}

fn python(root: &Path) -> (i32, Vec<String>, String) {
    let output = Command::new("python3")
        .arg(python_script())
        .arg("--repo")
        .arg(root)
        .output()
        .expect("run Python reference");
    let status = output.status.code().expect("exit code");
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 diagnostics");
    let diagnostics = stderr
        .lines()
        .filter(|line| !line.starts_with("docs-links: "))
        .map(str::to_owned)
        .collect();
    (
        status,
        diagnostics,
        String::from_utf8(output.stdout).expect("UTF-8 summary"),
    )
}

fn rust(root: &Path) -> Value {
    let mut inputs = Inputs::load(root).expect("load Git inputs");
    docs_links::run(&mut inputs).expect("run Rust checker")
}

fn assert_matches_python(root: &Path) -> Value {
    let result = rust(root);
    let (status, diagnostics, stdout) = python(root);
    assert_eq!(result["ok"], status == 0);
    assert_eq!(result["diagnostics"], serde_json::json!(diagnostics));
    if status == 0 {
        assert_eq!(
            stdout.trim(),
            format!(
                "docs-links: {} files, {} links checked — clean",
                result["files"], result["links"]
            )
        );
    }
    result
}

#[test]
fn preserves_regex_links_fences_spans_and_heading_collision_order() {
    let root = fixture();
    write(
        root.path(),
        "README.md",
        "# Repeat\n# Repeat\n# Repeat-1\n# [Visible](page.md)\n",
    );
    write(
        root.path(),
        "docs/page.md",
        "# Page\n[one](../README.md#repeat) [two](../README.md#repeat-1) [three](../README.md#repeat-1-1)\n[bad](../README.md#repeat-2)\n`[ignored](missing.md)` [missing](missing.md)\n```md\n[fenced](missing.md)\n~~~\n[visible](../README.md#visible)\n[external](HTTPS://example.com) [absolute](/not/here)\n[spaced](<space name.md>) [titled](../README.md \"title\")\n",
    );
    write(root.path(), "docs/space name.md", "# Space\n");
    write(
        root.path(),
        "docs/unclosed.md",
        "# Open\n  ```text\n[hidden](lost.md)\n",
    );
    git(root.path(), &["add", "."]);
    let result = assert_matches_python(root.path());
    assert_eq!(result["files"], 4);
    assert!(result["diagnostics"].to_string().contains("repeat-2"));
    assert!(
        result["diagnostics"]
            .to_string()
            .contains("unclosed fenced code block")
    );
    assert!(
        !result["diagnostics"]
            .to_string()
            .contains("docs/page.md:6:")
    );
}

#[test]
fn distinguishes_missing_untracked_escape_symlinks_and_ledger_docs_cells() {
    let root = fixture();
    let outside = tempfile::tempdir().expect("outside");
    write(outside.path(), "outside.md", "# Outside\n");
    write(root.path(), "README.md", "# Root\n");
    write(root.path(), "docs/untracked.md", "# Untracked\n");
    write(
        root.path(),
        "docs/links.md",
        "[missing](missing.md)\n[untracked](untracked.md)\n[escaped](../../outside.md)\n[symlink](escape.md)\n[dangling](dangling.md)\n[tracked directory](tracked-dir)\n[untracked directory](untracked-dir)\n[local](#absent)\n",
    );
    std::os::unix::fs::symlink(
        outside.path().join("outside.md"),
        root.path().join("docs/escape.md"),
    )
    .expect("symlink");
    std::os::unix::fs::symlink(
        outside.path().join("missing.md"),
        root.path().join("docs/dangling.md"),
    )
    .expect("dangling symlink");
    write(root.path(), "docs/tracked-dir/file.txt", "tracked\n");
    write(root.path(), "docs/untracked-dir/file.txt", "untracked\n");
    write(
        root.path(),
        "task/ledgers/staging/unit-ledger.md",
        "| Evidence | docs:README.md#root docs:docs/untracked.md docs:docs/missing.md docs:../outside.md |\n| Literal | `docs:ignored.md` docs:<placeholder> |\n| Boundary | \u{301}docs:README.md#root adocs:missing.md /docs:missing.md |\n",
    );
    git(
        root.path(),
        &[
            "add",
            "README.md",
            "docs/links.md",
            "docs/tracked-dir/file.txt",
            "task/ledgers/staging/unit-ledger.md",
        ],
    );
    let result = assert_matches_python(root.path());
    let diagnostics = result["diagnostics"].to_string();
    assert!(diagnostics.contains("does not exist"));
    assert!(diagnostics.contains("exists but is not tracked"));
    assert!(diagnostics.contains("resolves outside the repository"));
    assert!(diagnostics.contains("anchor #absent"));
    assert_eq!(result["links"], 13);
}

#[test]
fn unicode_slugs_and_python_line_separators_match_reference() {
    let root = fixture();
    write(
        root.path(),
        "README.md",
        "# École Σ Straße 你好\r\n# École Σ Straße 你好\r[valid](#école-σ-straße-你好-1)\u{85}[missing](lost.md)\u{2028}[valid](#école-σ-straße-你好)\n",
    );
    git(root.path(), &["add", "."]);
    let result = assert_matches_python(root.path());
    assert_eq!(result["links"], 3);
    assert_eq!(
        result["diagnostics"],
        serde_json::json!(["README.md:4: lost.md -> does not exist"])
    );
}

#[test]
fn allowlist_suppresses_matching_findings_and_reports_stale_entries() {
    let root = fixture();
    write(
        root.path(),
        "README.md",
        "# Root\n[missing](lost.md)\n[repeat](lost.md)\n[good](README.md#root)\n",
    );
    write(
        root.path(),
        "scripts/docs_links_allowlist.txt",
        "# Existing debt\nREADME.md:lost.md\nREADME.md:old.md\n",
    );
    git(root.path(), &["add", "."]);
    let result = assert_matches_python(root.path());
    assert_eq!(
        result["diagnostics"].as_array().expect("diagnostics").len(),
        1
    );
    assert!(
        result["diagnostics"]
            .to_string()
            .contains("stale entry README.md:old.md")
    );
    assert_eq!(result["links"], 3);
}

#[test]
fn malformed_allowlist_is_an_environment_error() {
    let root = fixture();
    write(root.path(), "README.md", "# Root\n");
    write(
        root.path(),
        "scripts/docs_links_allowlist.txt",
        "bad entry\n",
    );
    git(root.path(), &["add", "."]);
    let mut inputs = Inputs::load(root.path()).expect("Git inputs");
    let error = docs_links::run(&mut inputs).expect_err("malformed allowlist");
    let (status, diagnostics, _) = python(root.path());
    assert_eq!(status, 2);
    assert!(diagnostics[0].contains("malformed allowlist entry"));
    assert!(error.to_string().contains("malformed allowlist entry"));
}

#[test]
fn python_control_whitespace_and_combining_marks_match() {
    let root = fixture();
    write(
        root.path(),
        "README.md",
        "# A\u{1f}B\n[one](#a-b)\n[two](#ab)\n# a\u{5b0}b\n[mark](#ab)\n",
    );
    write(
        root.path(),
        "task/ledgers/staging/demo-ledger.md",
        "| evidence | \u{5b0}docs:missing.md |\n",
    );
    git(root.path(), &["add", "."]);
    assert_matches_python(root.path());
}

#[test]
fn long_valid_symlink_chain_matches_python() {
    let root = fixture();
    write(root.path(), "README.md", "[long](link-0)\n");
    write(root.path(), "target.txt", "target\n");
    for number in 0..48 {
        let target = if number == 47 {
            "target.txt".to_owned()
        } else {
            format!("link-{}", number + 1)
        };
        std::os::unix::fs::symlink(target, root.path().join(format!("link-{number}"))).unwrap();
    }
    git(root.path(), &["add", "."]);
    assert_eq!(assert_matches_python(root.path())["ok"], true);
}

#[test]
fn symlink_cycle_fails_and_revisiting_a_resolved_link_is_valid() {
    let root = fixture();
    write(
        root.path(),
        "README.md",
        "[twice](alias/../alias/file.txt)\n",
    );
    write(root.path(), "directory/file.txt", "target\n");
    std::os::unix::fs::symlink("directory", root.path().join("alias")).unwrap();
    git(root.path(), &["add", "."]);
    assert_eq!(assert_matches_python(root.path())["ok"], true);
    std::os::unix::fs::symlink("cycle", root.path().join("cycle")).unwrap();
    write(root.path(), "README.md", "[cycle](cycle)\n");
    let mut inputs = Inputs::load(root.path()).unwrap();
    assert!(
        docs_links::run(&mut inputs)
            .unwrap_err()
            .to_string()
            .contains("symlink loop")
    );
}
