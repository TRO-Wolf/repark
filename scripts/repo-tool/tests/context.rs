use repark_repo::{context, repository::Repository};
use serde_json::json;
use std::{fs, path::Path, process::Command};

fn snapshot(files: &[(&str, &str)]) -> (tempfile::TempDir, Repository) {
    let dir = tempfile::tempdir().unwrap();
    assert!(
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(dir.path())
            .status()
            .unwrap()
            .success()
    );
    for (path, contents) in files {
        let destination = dir.path().join(path);
        fs::create_dir_all(destination.parent().unwrap_or(Path::new("."))).unwrap();
        fs::write(destination, contents).unwrap();
    }
    assert!(
        Command::new("git")
            .args(["add", "."])
            .current_dir(dir.path())
            .status()
            .unwrap()
            .success()
    );
    let repo = Repository::load(dir.path(), "worktree").unwrap();
    (dir, repo)
}

#[test]
fn context_keeps_full_contract_and_refuses_ambiguous_or_oversized_sources() {
    let config = json!({"version":1,"global":[{"path":"README.md","section":"Intro"}],"roles":{"critic":[{"path":"docs/critic.md"}]},"units":{"u1":[{"path":"task/ledgers/staging/u1-ledger.md"}]}}).to_string();
    let (_dir, repo) = snapshot(&[
        ("AGENTS.md", "contract"),
        (
            "README.md",
            "```md\n# Intro\n```\n# Intro\nA\n```md\n# Other\n```\n# Other\nB\n",
        ),
        ("docs/critic.md", "critic-only"),
        ("task/ledgers/staging/u1-ledger.md", "unit"),
        ("config.json", &config),
    ]);
    let args = [
        "--config",
        "config.json",
        "--role",
        "critic",
        "--unit",
        "u1",
    ]
    .map(str::to_owned);
    let answer = context::run(&repo, &args).unwrap();
    assert_eq!(answer["ok"], true);
    assert_eq!(answer["sources"][0]["text"], "contract");
    assert_eq!(
        answer["sources"][1]["text"],
        "# Intro\nA\n```md\n# Other\n```\n"
    );
    assert_eq!(answer, context::run(&repo, &args).unwrap());
    let mut limited = args.to_vec();
    limited.extend(["--max-bytes".to_owned(), "3".to_owned()]);
    assert_eq!(context::run(&repo, &limited).unwrap()["ok"], false);
    let (_dir, repo) = snapshot(&[
        ("AGENTS.md", "contract"),
        ("README.md", "# Intro\nA\n# Intro\nB\n"),
        ("docs/critic.md", "critic-only"),
        ("task/ledgers/staging/u1-ledger.md", "unit"),
        ("config.json", &config),
    ]);
    assert_eq!(context::run(&repo, &args).unwrap()["ok"], false);
    let (_dir, repo) = snapshot(&[
        ("AGENTS.md", "contract"),
        ("README.md", "# Intro\nA\n"),
        ("docs/critic.md", "critic-only"),
        ("config.json", &config),
    ]);
    assert_eq!(context::run(&repo, &args).unwrap()["ok"], false);
    let (_dir, repo) = snapshot(&[
        ("README.md", "# Intro\nA\n"),
        ("docs/critic.md", "critic-only"),
        ("task/ledgers/staging/u1-ledger.md", "unit"),
        ("config.json", &config),
    ]);
    assert_eq!(context::run(&repo, &args).unwrap()["ok"], false);
    let bad_version = config.replace("\"version\":1", "\"version\":2");
    let (_dir, repo) = snapshot(&[
        ("AGENTS.md", "contract"),
        ("README.md", "# Intro\nA\n"),
        ("docs/critic.md", "critic-only"),
        ("task/ledgers/staging/u1-ledger.md", "unit"),
        ("config.json", &bad_version),
    ]);
    assert!(context::run(&repo, &args).is_err());
}

#[test]
fn context_never_selects_heading_inside_mixed_or_long_fence() {
    let config = json!({"version":1,"global":[{"path":"README.md","section":"Approved"}],"roles":{"critic":[{"path":"docs/critic.md"}]},"units":{"u1":[{"path":"unit.md"}]}}).to_string();
    let args = [
        "--config",
        "config.json",
        "--role",
        "critic",
        "--unit",
        "u1",
    ]
    .map(str::to_owned);
    let (_dir, repo) = snapshot(&[
        ("AGENTS.md", "contract"),
        (
            "README.md",
            "````md\n~~~\n# Approved\nexample only\n```\n# Still fenced\n````\n# Real\nactual\n",
        ),
        ("docs/critic.md", "critic"),
        ("unit.md", "unit"),
        ("config.json", &config),
    ]);
    assert_eq!(context::run(&repo, &args).unwrap()["ok"], false);
    let (_dir, repo) = snapshot(&[
        ("AGENTS.md", "contract"),
        (
            "README.md",
            "````md\n~~~\n# Approved\nexample only\n```\n````\n# Approved\nreal source\n",
        ),
        ("docs/critic.md", "critic"),
        ("unit.md", "unit"),
        ("config.json", &config),
    ]);
    let answer = context::run(&repo, &args).unwrap();
    assert_eq!(answer["ok"], true);
    assert_eq!(answer["sources"][1]["text"], "# Approved\nreal source\n");
}

#[test]
fn scoped_context_reads_only_approved_inputs_and_binds_config() {
    let config = json!({"version":1,"global":[{"path":"README.md"}],"roles":{"critic":[{"path":"critic.md"}],"executor":[{"path":"actor.md"}]},"units":{"u1":[{"path":"unit.md"}]}}).to_string();
    let (root, _) = snapshot(&[
        ("AGENTS.md", "full contract"),
        ("config.json", &config),
        ("README.md", "overview"),
        ("critic.md", "review"),
        ("actor.md", "execution"),
        ("unit.md", "unit"),
    ]);
    let args = [
        "--config",
        "config.json",
        "--role",
        "critic",
        "--unit",
        "u1",
    ]
    .map(str::to_owned);
    let initial = context::load_repository(root.path(), "worktree", &args).unwrap();
    assert_eq!(initial.files.len(), 5);
    assert!(!initial.contains("actor.md"));
    let answer = context::run(&initial, &args).unwrap();
    assert_eq!(answer["ok"], true);
    assert_eq!(answer["sources"][0]["text"], "full contract");
    assert_eq!(answer["input_scope"]["files"].as_array().unwrap().len(), 5);
    fs::write(root.path().join("actor.md"), "unrelated changes").unwrap();
    assert_eq!(
        initial.snapshot,
        context::load_repository(root.path(), "worktree", &args)
            .unwrap()
            .snapshot
    );
    fs::write(root.path().join("critic.md"), "changed").unwrap();
    assert_ne!(
        initial.snapshot,
        context::load_repository(root.path(), "worktree", &args)
            .unwrap()
            .snapshot
    );
    assert_eq!(
        initial.snapshot,
        context::load_repository(root.path(), "index", &args)
            .unwrap()
            .snapshot
    );
    fs::write(
        root.path().join("config.json"),
        config.replace("critic.md", "actor.md"),
    )
    .unwrap();
    let redirected = context::load_repository(root.path(), "worktree", &args).unwrap();
    assert!(redirected.contains("actor.md"));
    assert!(!redirected.contains("critic.md"));
    assert_ne!(initial.snapshot, redirected.snapshot);
    fs::remove_file(root.path().join("AGENTS.md")).unwrap();
    let missing = context::load_repository(root.path(), "worktree", &args).unwrap();
    assert_eq!(context::run(&missing, &args).unwrap()["ok"], false);
}
