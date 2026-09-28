use repark_repo::{cache, repository::Repository};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};

fn repository(root: &std::path::Path, snapshot: &str) -> Repository {
    Repository {
        root: root.to_owned(),
        source: "worktree".to_owned(),
        snapshot: snapshot.to_owned(),
        input_scope: repark_repo::repository::InputScope::Full,
        paths: BTreeSet::new(),
        files: BTreeMap::new(),
    }
}

#[test]
fn failures_are_cached_and_changed_inputs_recompute() {
    let root = tempfile::tempdir().expect("fixture");
    let repository = repository(root.path(), "one");
    let arguments = vec!["maps".to_owned(), "--check".to_owned()];
    let count = AtomicUsize::new(0);
    let first = cache::execute(&repository, "cache", &arguments, || {
        count.fetch_add(1, Ordering::Relaxed);
        Ok(json!({"ok":false,"diagnostics":["missing map"]}))
    })
    .expect("first");
    let second = cache::execute(&repository, "cache", &arguments, || {
        count.fetch_add(1, Ordering::Relaxed);
        Ok(json!({"ok":true}))
    })
    .expect("cached");
    assert_eq!(first, second);
    assert_eq!(count.load(Ordering::Relaxed), 1);
    let mut changed = repository.clone();
    changed.snapshot = "two".to_owned();
    let third =
        cache::execute(&changed, "cache", &arguments, || Ok(json!({"ok":true}))).expect("changed");
    assert_eq!(third["ok"], true);
}

#[test]
fn mutations_and_runtime_evidence_cannot_use_structural_cache() {
    let root = tempfile::tempdir().expect("fixture");
    let repository = repository(root.path(), "one");
    for arguments in [vec!["maps", "--write"], vec!["workflow"], vec!["evidence"]] {
        let arguments: Vec<String> = arguments.into_iter().map(str::to_owned).collect();
        assert!(
            cache::execute(&repository, "cache", &arguments, || Ok(json!({"ok":true}))).is_err()
        );
    }
    assert!(!root.path().join("cache").exists());
}

#[test]
fn damaged_cache_is_recomputed() {
    let root = tempfile::tempdir().expect("fixture");
    let repository = repository(root.path(), "one");
    let arguments = vec!["trace".to_owned()];
    cache::execute(&repository, "cache", &arguments, || Ok(json!({"ok":true}))).expect("populate");
    let path = std::fs::read_dir(root.path().join("cache"))
        .expect("directory")
        .next()
        .expect("entry")
        .expect("path")
        .path();
    std::fs::write(path, "invalid JSON").expect("damage");
    let result = cache::execute(&repository, "cache", &arguments, || Ok(json!({"ok":false})))
        .expect("recompute");
    assert_eq!(result["ok"], false);
}

#[test]
fn identical_bytes_from_different_sources_keep_provenance() {
    let root = tempfile::tempdir().expect("fixture");
    let mut repository = repository(root.path(), "same");
    let arguments = vec!["maps".to_owned()];
    cache::execute(&repository, "cache", &arguments, || {
        Ok(json!({"ok":true,"source":"worktree"}))
    })
    .expect("worktree");
    repository.source = "index".to_owned();
    let result = cache::execute(&repository, "cache", &arguments, || {
        Ok(json!({"ok":true,"source":"index"}))
    })
    .expect("index");
    assert_eq!(result["source"], "index");
}

#[test]
fn scoped_map_cache_reuses_body_edits_and_invalidates_changed_links() {
    use repark_repo::{maps, repository::InputScope};
    let root = tempfile::tempdir().unwrap();
    assert!(
        std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(root.path())
            .status()
            .unwrap()
            .success()
    );
    std::fs::write(root.path().join("map.md"), "[missing](gone.rs)\n").unwrap();
    std::fs::write(root.path().join("lib.rs"), "before").unwrap();
    assert!(
        std::process::Command::new("git")
            .args(["add", "."])
            .current_dir(root.path())
            .status()
            .unwrap()
            .success()
    );
    let arguments = ["maps", "--check"].map(str::to_owned);
    let count = AtomicUsize::new(0);
    for body in ["before", "after"] {
        std::fs::write(root.path().join("lib.rs"), body).unwrap();
        let repository =
            Repository::load_scoped(root.path(), "worktree", InputScope::Maps).unwrap();
        let result = cache::execute(&repository, "cache", &arguments, || {
            count.fetch_add(1, Ordering::Relaxed);
            maps::run(&repository, &arguments[1..])
        })
        .unwrap();
        assert_eq!(result["ok"], false);
    }
    assert_eq!(count.load(Ordering::Relaxed), 1);
    std::fs::write(root.path().join("map.md"), "[present](lib.rs)\n").unwrap();
    let repository = Repository::load_scoped(root.path(), "worktree", InputScope::Maps).unwrap();
    let result = cache::execute(&repository, "cache", &arguments, || {
        count.fetch_add(1, Ordering::Relaxed);
        maps::run(&repository, &arguments[1..])
    })
    .unwrap();
    assert_eq!(result["ok"], true);
    assert_eq!(count.load(Ordering::Relaxed), 2);
}
