use repark_repo::evidence::run;
use repark_repo::repository::Repository;
use serde_json::{Value, json};
use std::path::Path;

fn git(root: &Path, arguments: &[&str]) {
    let status = std::process::Command::new("git")
        .current_dir(root)
        .args(arguments)
        .status()
        .unwrap();
    assert!(status.success());
}

fn setup() -> (tempfile::TempDir, Repository, String) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    git(root, &["init", "-q"]);
    git(root, &["config", "user.email", "test@example.com"]);
    git(root, &["config", "user.name", "Test"]);
    std::fs::write(root.join("tracked.txt"), "initial").unwrap();
    git(root, &["add", "tracked.txt"]);
    git(root, &["commit", "-qm", "fixture"]);
    std::fs::create_dir(root.join("runtime")).unwrap();
    let repository = Repository::load(root, "worktree").unwrap();
    let head = String::from_utf8(
        std::process::Command::new("git")
            .current_dir(root)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    (directory, repository, head)
}

fn write(root: &Path, name: &str, value: &Value) -> String {
    let path = format!("runtime/{name}");
    std::fs::write(root.join(&path), serde_json::to_vec(value).unwrap()).unwrap();
    path
}

fn requirements(head: &str, snapshot: &str) -> Value {
    json!({"schema_version":1,"head":head,"snapshot":snapshot,"unit":"u1","actor_id":"actor-a","required_gates":[{"gate":"verify","command":"make verify"}],"required_coverage":["code","security"],"severity_floor":"s1"})
}

fn gate(head: &str, snapshot: &str, evidence: &str) -> Value {
    json!({"schema_version":1,"phase":"gate","head":head,"snapshot":snapshot,"unit":"u1","complete":true,"receipts":[{"gate":"verify","command":"make verify","run_id":"run1","exit_code":0,"head":head,"snapshot":snapshot,"evidence":evidence}],"review":null,"findings":[]})
}

fn critic(head: &str, snapshot: &str, evidence: &str) -> Value {
    json!({"schema_version":1,"phase":"critic","head":head,"snapshot":snapshot,"unit":"u1","complete":true,"receipts":[],"review":{"reviewer_id":"reviewer-b","coverage":[{"id":"code","disposition":"attacked","evidence":evidence,"reason":null},{"id":"security","disposition":"not_applicable","evidence":null,"reason":"No security-sensitive changes"}]},"findings":[]})
}

fn assess(repository: &Repository, input: String, requirements: String, head: &str) -> Value {
    run(
        repository,
        &[
            "--input".into(),
            input,
            "--requirements".into(),
            requirements,
            "--head".into(),
            head.into(),
            "--unit".into(),
            "u1".into(),
        ],
    )
    .unwrap()
}

#[test]
fn matching_gate_and_independent_critic_pass_separately() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    std::fs::write(root.join("runtime/output.txt"), "actual output").unwrap();
    let req = write(
        root,
        "requirements.json",
        &requirements(&head, &repository.snapshot),
    );
    let gate_path = write(
        root,
        "gate.json",
        &gate(&head, &repository.snapshot, "runtime/output.txt"),
    );
    let critic_path = write(
        root,
        "critic.json",
        &critic(&head, &repository.snapshot, "runtime/output.txt"),
    );
    assert_eq!(
        assess(&repository, gate_path, req.clone(), &head)["ok"],
        true
    );
    assert_eq!(assess(&repository, critic_path, req, &head)["ok"], true);
}

#[test]
fn gate_handback_cannot_satisfy_critic_coverage() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    std::fs::write(root.join("runtime/output.txt"), "output").unwrap();
    let req = write(
        root,
        "requirements.json",
        &requirements(&head, &repository.snapshot),
    );
    let mut payload = critic(&head, &repository.snapshot, "runtime/output.txt");
    payload["review"]["coverage"] = json!([{"id":"code","disposition":"attacked","evidence":"runtime/output.txt","reason":null}]);
    let critic_path = write(root, "critic.json", &payload);
    let result = assess(&repository, critic_path, req, &head);
    assert_eq!(result["ok"], false);
    assert!(
        result["assessment"]["errors"]
            .to_string()
            .contains("missing coverage")
    );
}

#[test]
fn dirty_tracked_file_invalidates_old_snapshot() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    std::fs::write(root.join("runtime/output.txt"), "output").unwrap();
    let req = write(
        root,
        "requirements.json",
        &requirements(&head, &repository.snapshot),
    );
    let input = write(
        root,
        "gate.json",
        &gate(&head, &repository.snapshot, "runtime/output.txt"),
    );
    std::fs::write(root.join("tracked.txt"), "dirty").unwrap();
    let dirty_repository = Repository::load(root, "worktree").unwrap();
    assert_ne!(repository.snapshot, dirty_repository.snapshot);
    let stale_handle = assess(&repository, input.clone(), req.clone(), &head);
    assert_eq!(stale_handle["ok"], false);
    let result = assess(&dirty_repository, input, req, &head);
    assert_eq!(result["ok"], false);
    assert!(
        result["assessment"]["errors"]
            .to_string()
            .contains("stale head or content snapshot")
    );
}

#[test]
fn index_source_fails_closed_for_current_evidence() {
    let (directory, _, head) = setup();
    let root = directory.path();
    let index = Repository::load(root, "index").unwrap();
    let req = write(
        root,
        "requirements.json",
        &requirements(&head, &index.snapshot),
    );
    let input = write(
        root,
        "gate.json",
        &gate(&head, &index.snapshot, "runtime/output.txt"),
    );
    assert!(
        run(
            &index,
            &[
                "--input".into(),
                input,
                "--requirements".into(),
                req,
                "--head".into(),
                head,
                "--unit".into(),
                "u1".into()
            ]
        )
        .is_err()
    );
}

#[test]
fn wrong_command_empty_artifact_and_same_reviewer_fail() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    std::fs::write(root.join("runtime/output.txt"), "").unwrap();
    let req = write(
        root,
        "requirements.json",
        &requirements(&head, &repository.snapshot),
    );
    let mut payload = gate(&head, &repository.snapshot, "runtime/output.txt");
    payload["receipts"][0]["command"] = json!("echo pass");
    let input = write(root, "gate.json", &payload);
    assert_eq!(assess(&repository, input, req.clone(), &head)["ok"], false);
    std::fs::write(root.join("runtime/output.txt"), "output").unwrap();
    let mut payload = critic(&head, &repository.snapshot, "runtime/output.txt");
    payload["review"]["reviewer_id"] = json!("actor-a");
    let input = write(root, "critic.json", &payload);
    assert_eq!(assess(&repository, input, req, &head)["ok"], false);
}

#[test]
fn s1_open_blocks_and_s3_open_is_advisory() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    std::fs::write(root.join("runtime/output.txt"), "output").unwrap();
    let req = write(
        root,
        "requirements.json",
        &requirements(&head, &repository.snapshot),
    );
    let mut payload = critic(&head, &repository.snapshot, "runtime/output.txt");
    payload["findings"] =
        json!([{"id":"F-1","severity":"s1","evidence":"runtime/output.txt","disposition":"open"}]);
    let s1 = write(root, "s1.json", &payload);
    payload["findings"][0]["severity"] = json!("s3");
    let s3 = write(root, "s3.json", &payload);
    assert_eq!(assess(&repository, s1, req.clone(), &head)["ok"], false);
    assert_eq!(assess(&repository, s3, req, &head)["ok"], true);
}

#[test]
fn empty_gate_artifact_fails_even_with_matching_command() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    std::fs::write(root.join("runtime/output.txt"), "").unwrap();
    let req = write(
        root,
        "requirements.json",
        &requirements(&head, &repository.snapshot),
    );
    let input = write(
        root,
        "gate.json",
        &gate(&head, &repository.snapshot, "runtime/output.txt"),
    );
    let result = assess(&repository, input, req, &head);
    assert_eq!(result["structurally_valid"], false);
    assert!(
        result["assessment"]["errors"]
            .to_string()
            .contains("invalid receipt")
    );
}
