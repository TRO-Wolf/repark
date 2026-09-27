use repark_repo::maps;
use repark_repo::repository::Repository;
use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

fn git(root: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn fixture() -> TempDir {
    let root = tempfile::tempdir().expect("tempdir");
    git(root.path(), &["init", "-q"]);
    fs::write(
        root.path().join("map.md"),
        "# Root\n\n- [child](child/map.md)\n",
    )
    .expect("map");
    fs::create_dir(root.path().join("child")).expect("child");
    fs::write(root.path().join("child/map.md"), "# Child\n").expect("child map");
    fs::write(root.path().join("child/lib.rs"), "pub fn value() {}\n").expect("source");
    git(root.path(), &["add", "."]);
    root
}

fn run(root: &Path, source: &str, args: &[&str]) -> serde_json::Value {
    let repository = Repository::load(root, source).expect("snapshot");
    maps::run(
        &repository,
        &args
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>(),
    )
    .expect("maps")
}

#[test]
fn validates_links_and_strict_coverage_by_snapshot() {
    let root = fixture();
    assert_eq!(run(root.path(), "index", &["--check"])["ok"], true);
    assert_eq!(
        run(root.path(), "index", &["--check", "--strict"])["ok"],
        false
    );
    fs::write(
        root.path().join("child/map.md"),
        "# Child\n- [missing](lost.md)\n",
    )
    .expect("change");
    assert_eq!(run(root.path(), "worktree", &["--check"])["ok"], false);
    assert_eq!(run(root.path(), "index", &["--check"])["ok"], true);
}

#[test]
fn init_write_is_idempotent_and_preserves_authored_text() {
    let root = fixture();
    let first = run(
        root.path(),
        "worktree",
        &["--write", "--init", "--path", "child"],
    );
    assert_eq!(first["written"], serde_json::json!(["child/map.md"]));
    let text = fs::read_to_string(root.path().join("child/map.md")).expect("read");
    assert!(text.starts_with("# Child\n"));
    assert!(text.contains("- [lib.rs](lib.rs)"));
    let second = run(root.path(), "worktree", &["--write", "--path", "child"]);
    assert_eq!(second["written"], serde_json::json!([]));
    assert_eq!(
        fs::read_to_string(root.path().join("child/map.md")).expect("read"),
        text
    );
}

#[test]
fn rejects_bad_markers_and_escape_links() {
    let root = fixture();
    fs::write(
        root.path().join("child/map.md"),
        "# Child\n<!-- repo-tool:contents:start -->\n- [bad](../../outside.md)\n",
    )
    .expect("change");
    let bad = run(root.path(), "worktree", &["--check"]);
    assert_eq!(bad["ok"], false);
    assert!(bad["diagnostics"].to_string().contains("malformed"));
    fs::write(
        root.path().join("child/map.md"),
        "# Child\n- [bad](../../outside.md)\n",
    )
    .expect("change");
    let escape = run(root.path(), "worktree", &["--check"]);
    assert!(
        escape["diagnostics"]
            .to_string()
            .contains("escapes repository")
    );
}

#[test]
fn ignores_code_and_reports_duplicate_rows() {
    let root = fixture();
    fs::write(root.path().join("child/map.md"), "# Child\n`[fake](lost.md)`\n```markdown\n- [fake](lost.md)\n```\n- [lib](lib.rs)\n- [again](lib.rs)\n").expect("change");
    let result = run(root.path(), "worktree", &["--check"]);
    assert_eq!(result["ok"], false);
    assert_eq!(result["diagnostics"].as_array().expect("array").len(), 1);
    assert!(result["diagnostics"].to_string().contains("duplicate row"));
}

#[test]
fn renamed_file_uses_selected_snapshot() {
    let root = fixture();
    fs::write(
        root.path().join("child/map.md"),
        "# Child\n- [lib](lib.rs)\n",
    )
    .expect("change");
    git(root.path(), &["add", "."]);
    git(root.path(), &["mv", "child/lib.rs", "child/new.rs"]);
    let result = run(root.path(), "index", &["--check", "--strict"]);
    assert!(result["diagnostics"].to_string().contains("dead link"));
    assert!(result["diagnostics"].to_string().contains("new.rs"));
}

#[test]
fn missing_map_requires_explicit_init() {
    let root = fixture();
    fs::create_dir(root.path().join("new")).expect("directory");
    fs::write(root.path().join("new/file.py"), "pass\n").expect("file");
    git(root.path(), &["add", "new/file.py"]);
    let missing = run(root.path(), "worktree", &["--check", "--path", "new"]);
    assert_eq!(missing["ok"], false);
    let created = run(
        root.path(),
        "worktree",
        &["--write", "--init", "--path", "new"],
    );
    assert_eq!(created["written"], serde_json::json!(["new/map.md"]));
    let map = fs::read_to_string(root.path().join("new/map.md")).expect("map");
    assert!(map.contains("- [file.py](file.py)"));
}

#[test]
fn refuses_stale_write_and_archive_mutation() {
    let root = fixture();
    let repository = Repository::load(root.path(), "worktree").expect("snapshot");
    fs::write(root.path().join("child/map.md"), "changed\n").expect("change");
    let result = maps::run(
        &repository,
        &["--write", "--init", "--path", "child"].map(str::to_owned),
    );
    assert!(result.is_err());
    fs::create_dir_all(root.path().join("docs/history/old")).expect("archive");
    fs::write(root.path().join("docs/history/old/map.md"), "# Old\n").expect("map");
    git(root.path(), &["add", "docs/history/old/map.md"]);
    let repository = Repository::load(root.path(), "worktree").expect("snapshot");
    let result = maps::run(
        &repository,
        &["--write", "--init", "--path", "docs/history/old"].map(str::to_owned),
    );
    assert!(result.is_err());
}

#[test]
fn refuses_stale_inventory_input() {
    let root = fixture();
    let repository = Repository::load(root.path(), "worktree").expect("snapshot");
    fs::write(root.path().join("child/lib.rs"), "pub fn changed() {}\n").expect("change");
    let result = maps::run(
        &repository,
        &["--write", "--init", "--path", "child"].map(str::to_owned),
    );
    assert!(result.is_err());
}

#[test]
fn angle_link_with_spaces_resolves() {
    let root = fixture();
    fs::write(root.path().join("child/my file.md"), "# file\n").expect("file");
    fs::write(
        root.path().join("child/map.md"),
        "# Child\n- [file](<my file.md>)\n",
    )
    .expect("map");
    git(root.path(), &["add", "child/my file.md"]);
    assert_eq!(run(root.path(), "index", &["--check"])["ok"], true);
}

#[test]
fn managed_block_drift_is_a_finding() {
    let root = fixture();
    fs::write(
        root.path().join("child/map.md"),
        "# Child\n<!-- repo-tool:contents:start -->\n<!-- repo-tool:contents:end -->\n",
    )
    .expect("map");
    git(root.path(), &["add", "child/map.md"]);
    let result = run(root.path(), "index", &["--check"]);
    assert_eq!(result["ok"], false);
    assert!(
        result["diagnostics"]
            .to_string()
            .contains("managed contents differ")
    );
    let written = run(root.path(), "worktree", &["--write", "--path", "child"]);
    assert_eq!(written["written"], serde_json::json!(["child/map.md"]));
    assert_eq!(run(root.path(), "worktree", &["--check"])["ok"], true);
}

#[test]
fn fenced_markers_preserve_authored_example() {
    let root = fixture();
    let example = "# Child\n```markdown\n<!-- repo-tool:contents:start -->\nExample remains authored.\n<!-- repo-tool:contents:end -->\n```\n";
    fs::write(root.path().join("child/map.md"), example).expect("map");
    let init = run(
        root.path(),
        "worktree",
        &["--write", "--init", "--path", "child"],
    );
    assert_eq!(init["ok"], true);
    let text = fs::read_to_string(root.path().join("child/map.md")).expect("map");
    assert!(text.starts_with(example));
    assert_eq!(text.matches("<!-- repo-tool:contents:start -->").count(), 2);
    let write = run(root.path(), "worktree", &["--write", "--path", "child"]);
    assert_eq!(write["written"], serde_json::json!([]));
    assert_eq!(
        fs::read_to_string(root.path().join("child/map.md")).expect("map"),
        text
    );
}

#[test]
fn reference_links_validate_targets_and_missing_definitions() {
    let root = fixture();
    fs::write(
        root.path().join("child/map.md"),
        "# Child\n- [missing][ref]\n- [collapsed][]\n- [shortcut]\n- [undefined][where]\n\n[ref]: missing.md\n[collapsed]: missing.md\n[shortcut]: missing.md\n",
    )
    .expect("map");
    let result = run(root.path(), "worktree", &["--check", "--path", "child"]);
    assert_eq!(result["ok"], false);
    let diagnostics = result["diagnostics"].as_array().expect("diagnostics");
    assert_eq!(diagnostics.len(), 5);
    assert!(
        result["diagnostics"]
            .to_string()
            .contains("dead link: missing.md")
    );
    assert!(
        !result["diagnostics"]
            .to_string()
            .contains("unresolved reference")
    );
}

#[test]
fn archive_path_can_be_checked_but_not_written() {
    let root = fixture();
    fs::create_dir_all(root.path().join("docs/history/old")).expect("archive");
    fs::write(
        root.path().join("docs/history/old/map.md"),
        "# Old\n- [bad](missing.md)\n",
    )
    .expect("map");
    git(root.path(), &["add", "docs/history/old/map.md"]);
    let result = run(
        root.path(),
        "index",
        &["--check", "--path", "docs/history/old"],
    );
    assert_eq!(result["ok"], false);
    assert!(result["diagnostics"].to_string().contains("dead link"));
}

#[test]
fn non_standalone_active_marker_is_rejected() {
    let root = fixture();
    fs::write(
        root.path().join("child/map.md"),
        "# Child\ntext <!-- repo-tool:contents:start -->\n",
    )
    .expect("map");
    let result = run(root.path(), "worktree", &["--check", "--path", "child"]);
    assert_eq!(result["ok"], false);
    assert!(
        result["diagnostics"]
            .to_string()
            .contains("must occupy its own line")
    );
}

#[test]
fn require_managed_ignores_fenced_markers() {
    let root = fixture();
    fs::write(
        root.path().join("child/map.md"),
        "# Child\n```markdown\n~~~\n<!-- repo-tool:contents:start -->\n<!-- repo-tool:contents:end -->\n```\n",
    )
    .expect("map");
    git(root.path(), &["add", "child/map.md"]);
    let result = run(
        root.path(),
        "index",
        &["--check", "--require-managed", "--path", "child"],
    );
    assert_eq!(result["ok"], false);
    assert!(
        result["diagnostics"]
            .to_string()
            .contains("managed contents block required")
    );
    let init = run(
        root.path(),
        "worktree",
        &["--write", "--init", "--path", "child"],
    );
    assert_eq!(init["ok"], true);
    let result = run(
        root.path(),
        "worktree",
        &["--check", "--require-managed", "--path", "child"],
    );
    assert_eq!(result["ok"], true);
}

#[test]
fn require_managed_rejects_write() {
    let root = fixture();
    let repository = Repository::load(root.path(), "worktree").expect("snapshot");
    let result = maps::run(
        &repository,
        &["--write", "--require-managed", "--path", "child"].map(str::to_owned),
    );
    assert!(result.is_err());
}

#[test]
fn multiline_code_spans_do_not_form_reference_links() {
    let root = fixture();
    fs::write(
        root.path().join("child/map.md"),
        "# Child\n- SQL projection: `[id,\n _deleted]`, `[id]`, `[count(*)]` are examples.\n- [lib](lib.rs)\n",
    )
    .expect("map");
    let result = run(root.path(), "worktree", &["--check", "--path", "child"]);
    assert_eq!(result["ok"], true);
    fs::write(
        root.path().join("child/map.md"),
        "# Child\n- SQL projection: `[id,\n _deleted]`, `[id]`, `[count(*)]` are examples.\n- [missing][ref]\n\n[ref]: missing.md\n",
    )
    .expect("map");
    let result = run(root.path(), "worktree", &["--check", "--path", "child"]);
    assert_eq!(result["ok"], false);
    assert!(
        result["diagnostics"]
            .to_string()
            .contains("dead link: missing.md")
    );
}

#[test]
fn generated_links_escape_names_and_encoded_traversal_is_refused() {
    let root = fixture();
    fs::write(root.path().join("child/odd # [file].md"), "# Notes\n").unwrap();
    git(root.path(), &["add", "."]);
    assert_eq!(
        run(
            root.path(),
            "worktree",
            &["--write", "--init", "--path", "child"]
        )["ok"],
        true
    );
    let checked = run(root.path(), "worktree", &["--check", "--path", "child"]);
    assert_eq!(checked["ok"], true, "{checked}");
    let text = fs::read_to_string(root.path().join("child/map.md")).unwrap();
    assert!(text.contains("odd%20%23%20%5Bfile%5D.md"));
    fs::write(root.path().join("map.md"), "[escape](%2E%2E/private.md)\n").unwrap();
    assert_eq!(
        run(root.path(), "worktree", &["--check", "--path", "."])["ok"],
        false
    );
}

#[test]
fn opt_in_preserves_authored_inventory_without_false_duplicates() {
    let root = fixture();
    let prose = "# Child\n\n- [lib.rs](lib.rs) — authored reason.\n";
    fs::write(root.path().join("child/map.md"), prose).unwrap();
    assert_eq!(
        run(
            root.path(),
            "worktree",
            &["--write", "--init", "--path", "child"]
        )["ok"],
        true
    );
    let text = fs::read_to_string(root.path().join("child/map.md")).unwrap();
    assert!(text.starts_with(prose));
    assert_eq!(
        run(root.path(), "worktree", &["--check", "--path", "child"])["ok"],
        true
    );
}

#[test]
fn scoped_writes_allow_body_edits_but_refuse_changed_inventory_or_map() {
    use repark_repo::repository::InputScope;
    let root = fixture();
    let arguments = ["--write", "--init", "--path", "child"].map(str::to_owned);
    let snapshot = Repository::load_scoped(root.path(), "worktree", InputScope::Maps).unwrap();
    fs::write(root.path().join("child/lib.rs"), "changed body").unwrap();
    assert_eq!(maps::run(&snapshot, &arguments).unwrap()["ok"], true);
    let snapshot = Repository::load_scoped(root.path(), "worktree", InputScope::Maps).unwrap();
    fs::remove_file(root.path().join("child/lib.rs")).unwrap();
    assert!(maps::run(&snapshot, &arguments).is_err());
    fs::write(root.path().join("child/lib.rs"), "restored").unwrap();
    let snapshot = Repository::load_scoped(root.path(), "worktree", InputScope::Maps).unwrap();
    fs::write(root.path().join("child/map.md"), "concurrent edit").unwrap();
    assert!(maps::run(&snapshot, &arguments).is_err());
    assert_eq!(
        fs::read_to_string(root.path().join("child/map.md")).unwrap(),
        "concurrent edit"
    );
}

#[test]
fn link_line_numbers_include_unicode_multiline_labels_and_late_rows() {
    let root = fixture();
    let mut text = "é\n".repeat(1000);
    text.push_str("[split\nlabel](gone.rs)\n\n- [one](lib.rs)\n- [two](lib.rs)\n");
    fs::write(root.path().join("child/map.md"), text).unwrap();
    let answer = run(root.path(), "worktree", &["--check"]);
    assert_eq!(
        answer["diagnostics"],
        serde_json::json!([
            "child/map.md:1001: dead link: gone.rs",
            "child/map.md:1005: duplicate row: lib.rs first appears at line 1004"
        ])
    );
}
