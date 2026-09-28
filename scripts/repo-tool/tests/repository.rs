use repark_repo::repository::{Repository, checked_path};
use std::path::Path;
use std::process::Command;

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

#[test]
fn index_reads_staged_bytes_and_worktree_reads_current_bytes() {
    let root = tempfile::tempdir().expect("fixture");
    git(root.path(), &["init", "-q"]);
    std::fs::write(root.path().join("a.md"), "staged\n").expect("write");
    git(root.path(), &["add", "a.md"]);
    std::fs::write(root.path().join("a.md"), "working\n").expect("write");
    let staged = Repository::load(root.path(), "index").expect("index");
    let working = Repository::load(root.path(), "worktree").expect("worktree");
    assert_eq!(staged.read("a.md").expect("text"), "staged\n");
    assert_eq!(working.read("a.md").expect("text"), "working\n");
    assert_ne!(staged.snapshot, working.snapshot);
    assert!(staged.write_text("a.md", "replacement").is_err());
    assert_eq!(
        working.snapshot,
        Repository::load(root.path(), "worktree")
            .expect("repeat")
            .snapshot
    );
}

#[test]
fn deleted_worktree_paths_do_not_survive_indexing() {
    let root = tempfile::tempdir().expect("fixture");
    git(root.path(), &["init", "-q"]);
    std::fs::write(root.path().join("a.md"), "old").expect("write");
    git(root.path(), &["add", "a.md"]);
    std::fs::remove_file(root.path().join("a.md")).expect("delete");
    assert!(
        !Repository::load(root.path(), "worktree")
            .expect("load")
            .contains("a.md")
    );
    assert!(
        Repository::load(root.path(), "index")
            .expect("load")
            .contains("a.md")
    );
}

#[test]
fn writes_reject_paths_outside_root_and_changed_files() {
    let root = tempfile::tempdir().expect("fixture");
    git(root.path(), &["init", "-q"]);
    std::fs::write(root.path().join("map.md"), "old").expect("write");
    git(root.path(), &["add", "map.md"]);
    let snapshot = Repository::load(root.path(), "worktree").expect("snapshot");
    std::fs::write(root.path().join("map.md"), "new").expect("write");
    assert!(snapshot.write_text("map.md", "clobber").is_err());
    for path in ["../escape", "a//b", "a/./b", "a/", "a/../b"] {
        assert!(checked_path(root.path(), path).is_err(), "{path}");
    }
    assert_eq!(
        std::fs::read_to_string(root.path().join("map.md")).expect("read"),
        "new"
    );
}

#[cfg(unix)]
#[test]
fn symlink_reads_never_open_external_content() {
    let root = tempfile::tempdir().expect("fixture");
    let outside = tempfile::tempdir().expect("outside");
    git(root.path(), &["init", "-q"]);
    std::fs::write(outside.path().join("private"), "secret").expect("write");
    std::os::unix::fs::symlink(outside.path().join("private"), root.path().join("link"))
        .expect("link");
    git(root.path(), &["add", "link"]);
    let snapshot = Repository::load(root.path(), "worktree").expect("snapshot");
    assert!(snapshot.paths.contains("link"));
    assert!(!snapshot.files.contains_key("link"));
    assert!(snapshot.write_text("link", "replacement").is_err());
}

#[test]
fn gitlink_object_changes_invalidate_snapshot() {
    let root = tempfile::tempdir().expect("fixture");
    git(root.path(), &["init", "-q"]);
    git(
        root.path(),
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            "160000,1111111111111111111111111111111111111111,vendor",
        ],
    );
    let first = Repository::load(root.path(), "index").expect("first");
    git(
        root.path(),
        &[
            "update-index",
            "--cacheinfo",
            "160000,2222222222222222222222222222222222222222,vendor",
        ],
    );
    let second = Repository::load(root.path(), "index").expect("second");
    assert_ne!(first.snapshot, second.snapshot);
    assert!(second.contains("vendor"));
    assert!(second.read("vendor").is_err());
}

#[test]
fn map_scope_ignores_bodies_but_tracks_content_names_and_presence() {
    use repark_repo::repository::InputScope;
    let root = tempfile::tempdir().expect("fixture");
    git(root.path(), &["init", "-q"]);
    std::fs::write(root.path().join("map.md"), "# One\n").expect("map");
    std::fs::write(root.path().join("lib.rs"), "before").expect("source");
    git(root.path(), &["add", "."]);
    let first = Repository::load_scoped(root.path(), "worktree", InputScope::Maps).unwrap();
    let full = Repository::load(root.path(), "worktree").unwrap();
    assert_eq!(first.files.len(), 1);
    assert!(first.contains("lib.rs"));
    assert!(first.read("lib.rs").is_err());
    assert_ne!(first.snapshot, full.snapshot);
    std::fs::write(root.path().join("lib.rs"), "after!").unwrap();
    let changed = Repository::load_scoped(root.path(), "worktree", InputScope::Maps).unwrap();
    assert_eq!(first.snapshot, changed.snapshot);
    assert_ne!(
        full.snapshot,
        Repository::load(root.path(), "worktree").unwrap().snapshot
    );
    std::fs::write(root.path().join("map.md"), "# Two\n").unwrap();
    let text = Repository::load_scoped(root.path(), "worktree", InputScope::Maps).unwrap();
    assert_ne!(first.snapshot, text.snapshot);
    let staged = Repository::load_scoped(root.path(), "index", InputScope::Maps).unwrap();
    assert_eq!(first.snapshot, staged.snapshot);
    std::fs::remove_file(root.path().join("lib.rs")).unwrap();
    let deleted = Repository::load_scoped(root.path(), "worktree", InputScope::Maps).unwrap();
    assert_ne!(text.snapshot, deleted.snapshot);
    assert!(!deleted.contains("lib.rs"));
    assert!(staged.contains("lib.rs"));
    std::fs::write(root.path().join("renamed.rs"), "after!").unwrap();
    git(root.path(), &["add", "-A"]);
    let renamed = Repository::load_scoped(root.path(), "worktree", InputScope::Maps).unwrap();
    assert_ne!(deleted.snapshot, renamed.snapshot);
    assert!(renamed.contains("renamed.rs"));
    let tree = Command::new("git")
        .args(["write-tree"])
        .current_dir(root.path())
        .output()
        .unwrap();
    assert!(tree.status.success());
    let reference = String::from_utf8(tree.stdout).unwrap();
    let committed =
        Repository::load_scoped(root.path(), reference.trim(), InputScope::Maps).unwrap();
    assert_eq!(renamed.snapshot, committed.snapshot);
    assert_eq!(renamed.files, committed.files);
}

#[test]
fn scoped_snapshots_cannot_authorize_runtime_evidence() {
    use repark_repo::{evidence, repository::InputScope, workflow};
    let root = tempfile::tempdir().unwrap();
    git(root.path(), &["init", "-q"]);
    for scope in [
        InputScope::Maps,
        InputScope::Files(std::collections::BTreeSet::new()),
    ] {
        let snapshot = Repository::load_scoped(root.path(), "worktree", scope).unwrap();
        for result in [evidence::run(&snapshot, &[]), workflow::run(&snapshot, &[])] {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("full worktree snapshot")
            );
        }
    }
}

#[cfg(unix)]
#[test]
fn scoped_inventory_refuses_symlinked_parent_and_leaf_replacements() {
    use repark_repo::repository::InputScope;
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    git(root.path(), &["init", "-q"]);
    std::fs::create_dir(root.path().join("child")).unwrap();
    std::fs::write(root.path().join("child/lib.rs"), "tracked").unwrap();
    git(root.path(), &["add", "."]);
    std::fs::remove_file(root.path().join("child/lib.rs")).unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("child/lib.rs")).unwrap();
    assert!(Repository::load_scoped(root.path(), "worktree", InputScope::Maps).is_err());
    std::fs::remove_file(root.path().join("child/lib.rs")).unwrap();
    std::fs::remove_dir(root.path().join("child")).unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("child")).unwrap();
    assert!(Repository::load_scoped(root.path(), "worktree", InputScope::Maps).is_err());
}
