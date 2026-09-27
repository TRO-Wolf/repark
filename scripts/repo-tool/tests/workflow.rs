use repark_repo::repository::Repository;
use repark_repo::workflow::run;
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

fn event(
    head: &str,
    snapshot: &str,
    id: &str,
    revision: u64,
    kind: &str,
    requirements: Option<&str>,
    handback: Option<&str>,
) -> Value {
    json!({"schema_version":1,"id":id,"expected_revision":revision,"head":head,"snapshot":snapshot,"unit":"u1","kind":kind,"requirements":requirements,"handback":handback})
}

fn requirements(head: &str, snapshot: &str) -> Value {
    json!({"schema_version":1,"head":head,"snapshot":snapshot,"unit":"u1","actor_id":"actor-a","required_gates":[{"gate":"verify","command":"make verify"}],"required_coverage":["code"],"severity_floor":"s1"})
}

fn gate(head: &str, snapshot: &str) -> Value {
    json!({"schema_version":1,"phase":"gate","head":head,"snapshot":snapshot,"unit":"u1","complete":true,"receipts":[{"gate":"verify","command":"make verify","run_id":"run1","exit_code":0,"head":head,"snapshot":snapshot,"evidence":"runtime/output.txt"}],"review":null,"findings":[]})
}

fn critic(head: &str, snapshot: &str, blocked: bool) -> Value {
    let findings = if blocked {
        json!([{"id":"F-1","severity":"s1","evidence":"runtime/output.txt","disposition":"open"}])
    } else {
        json!([])
    };
    json!({"schema_version":1,"phase":"critic","head":head,"snapshot":snapshot,"unit":"u1","complete":true,"receipts":[],"review":{"reviewer_id":"reviewer-b","coverage":[{"id":"code","disposition":"attacked","evidence":"runtime/output.txt","reason":null}]},"findings":findings})
}

fn apply(repository: &Repository, root: &Path, name: &str, payload: &Value) -> Value {
    let event_path = write(root, name, payload);
    run(
        repository,
        &[
            "--state".into(),
            "runtime/state.json".into(),
            "--event".into(),
            event_path,
            "--apply".into(),
        ],
    )
    .unwrap()
}

#[test]
fn plan_is_read_only_and_duplicate_is_idempotent() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    let payload = event(
        &head,
        &repository.snapshot,
        "e1",
        0,
        "worker_completed",
        None,
        None,
    );
    let path = write(root, "event.json", &payload);
    let args = vec![
        "--state".into(),
        "runtime/state.json".into(),
        "--event".into(),
        path.clone(),
    ];
    assert_eq!(run(&repository, &args).unwrap()["stage"], "gate");
    assert!(!root.join("runtime/state.json").exists());
    let mut apply_args = args;
    apply_args.push("--apply".into());
    assert_eq!(run(&repository, &apply_args).unwrap()["revision"], 1);
    assert_eq!(run(&repository, &apply_args).unwrap()["duplicate"], true);
    let stale = write(
        root,
        "stale.json",
        &event(&head, &repository.snapshot, "e2", 0, "hold", None, None),
    );
    assert!(
        run(
            &repository,
            &[
                "--state".into(),
                "runtime/state.json".into(),
                "--event".into(),
                stale
            ]
        )
        .is_err()
    );
    let changed = write(
        root,
        "changed.json",
        &event(&head, &repository.snapshot, "e1", 0, "hold", None, None),
    );
    assert!(
        run(
            &repository,
            &[
                "--state".into(),
                "runtime/state.json".into(),
                "--event".into(),
                changed
            ]
        )
        .is_err()
    );
}

#[test]
fn gate_evidence_cannot_advance_critic_stage() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    std::fs::write(root.join("runtime/output.txt"), "output").unwrap();
    let req = write(
        root,
        "requirements.json",
        &requirements(&head, &repository.snapshot),
    );
    let gate_path = write(root, "gate.json", &gate(&head, &repository.snapshot));
    apply(
        &repository,
        root,
        "worker.json",
        &event(
            &head,
            &repository.snapshot,
            "e1",
            0,
            "worker_completed",
            None,
            None,
        ),
    );
    apply(
        &repository,
        root,
        "gate-event.json",
        &event(
            &head,
            &repository.snapshot,
            "e2",
            1,
            "gate_completed",
            Some(&req),
            Some(&gate_path),
        ),
    );
    let wrong = write(
        root,
        "wrong.json",
        &event(
            &head,
            &repository.snapshot,
            "e3",
            2,
            "critic_completed",
            Some(&req),
            Some(&gate_path),
        ),
    );
    assert!(
        run(
            &repository,
            &[
                "--state".into(),
                "runtime/state.json".into(),
                "--event".into(),
                wrong,
                "--apply".into()
            ]
        )
        .is_err()
    );
    let critic_path = write(
        root,
        "critic.json",
        &critic(&head, &repository.snapshot, false),
    );
    assert_eq!(
        apply(
            &repository,
            root,
            "critic-event.json",
            &event(
                &head,
                &repository.snapshot,
                "e3",
                2,
                "critic_completed",
                Some(&req),
                Some(&critic_path)
            )
        )["stage"],
        "ready"
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "A real remediation commit must preserve the policy across all workflow phases"
)]
fn remediation_commit_can_change_head_and_snapshot() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    std::fs::write(root.join("runtime/output.txt"), "output").unwrap();
    let req = write(
        root,
        "requirements-old.json",
        &requirements(&head, &repository.snapshot),
    );
    let gate_path = write(root, "gate-old.json", &gate(&head, &repository.snapshot));
    let critic_path = write(
        root,
        "critic-old.json",
        &critic(&head, &repository.snapshot, true),
    );
    apply(
        &repository,
        root,
        "worker.json",
        &event(
            &head,
            &repository.snapshot,
            "e1",
            0,
            "worker_completed",
            None,
            None,
        ),
    );
    apply(
        &repository,
        root,
        "gate-event.json",
        &event(
            &head,
            &repository.snapshot,
            "e2",
            1,
            "gate_completed",
            Some(&req),
            Some(&gate_path),
        ),
    );
    assert_eq!(
        apply(
            &repository,
            root,
            "critic-event.json",
            &event(
                &head,
                &repository.snapshot,
                "e3",
                2,
                "critic_completed",
                Some(&req),
                Some(&critic_path)
            )
        )["stage"],
        "remediation"
    );
    std::fs::write(root.join("tracked.txt"), "remediated").unwrap();
    git(root, &["add", "tracked.txt"]);
    git(root, &["commit", "-qm", "remediation fixture"]);
    let next = Repository::load(root, "worktree").unwrap();
    let next_head = String::from_utf8(
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
    assert_ne!(head, next_head);
    let result = apply(
        &next,
        root,
        "remediation.json",
        &event(
            &next_head,
            &next.snapshot,
            "e4",
            3,
            "remediation_completed",
            None,
            None,
        ),
    );
    assert_eq!(result["stage"], "gate");
    let stale = write(
        root,
        "stale.json",
        &event(
            &head,
            &repository.snapshot,
            "e5",
            4,
            "gate_completed",
            Some(&req),
            Some(&gate_path),
        ),
    );
    assert!(
        run(
            &next,
            &[
                "--state".into(),
                "runtime/state.json".into(),
                "--event".into(),
                stale
            ]
        )
        .is_err()
    );
    let next_requirements = write(
        root,
        "requirements-new.json",
        &requirements(&next_head, &next.snapshot),
    );
    let next_gate = write(root, "gate-new.json", &gate(&next_head, &next.snapshot));
    let next_critic = write(
        root,
        "critic-new.json",
        &critic(&next_head, &next.snapshot, false),
    );
    assert_eq!(
        apply(
            &next,
            root,
            "gate-new-event.json",
            &event(
                &next_head,
                &next.snapshot,
                "e5",
                4,
                "gate_completed",
                Some(&next_requirements),
                Some(&next_gate)
            )
        )["stage"],
        "critic"
    );
    assert_eq!(
        apply(
            &next,
            root,
            "critic-new-event.json",
            &event(
                &next_head,
                &next.snapshot,
                "e6",
                5,
                "critic_completed",
                Some(&next_requirements),
                Some(&next_critic)
            )
        )["stage"],
        "ready"
    );
}

#[test]
fn critic_cannot_lower_coverage_roster_after_gate() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    std::fs::write(root.join("runtime/output.txt"), "output").unwrap();
    let mut gate_policy = requirements(&head, &repository.snapshot);
    gate_policy["required_coverage"] = json!(["code", "security"]);
    let gate_requirements = write(root, "gate-requirements.json", &gate_policy);
    let gate_path = write(root, "gate.json", &gate(&head, &repository.snapshot));
    apply(
        &repository,
        root,
        "worker.json",
        &event(
            &head,
            &repository.snapshot,
            "e1",
            0,
            "worker_completed",
            None,
            None,
        ),
    );
    apply(
        &repository,
        root,
        "gate-event.json",
        &event(
            &head,
            &repository.snapshot,
            "e2",
            1,
            "gate_completed",
            Some(&gate_requirements),
            Some(&gate_path),
        ),
    );
    let lowered = write(
        root,
        "lowered.json",
        &requirements(&head, &repository.snapshot),
    );
    let critic_path = write(
        root,
        "critic.json",
        &critic(&head, &repository.snapshot, false),
    );
    let critic_event = write(
        root,
        "critic-event.json",
        &event(
            &head,
            &repository.snapshot,
            "e3",
            2,
            "critic_completed",
            Some(&lowered),
            Some(&critic_path),
        ),
    );
    assert!(
        run(
            &repository,
            &[
                "--state".into(),
                "runtime/state.json".into(),
                "--event".into(),
                critic_event,
                "--apply".into()
            ]
        )
        .is_err()
    );
    let state: Value =
        serde_json::from_slice(&std::fs::read(root.join("runtime/state.json")).unwrap()).unwrap();
    assert_eq!(state["stage"], "critic");
    assert_eq!(state["revision"], 2);
}

#[test]
fn critic_cap_holds_and_evidence_mutation_breaks_replay() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    std::fs::write(root.join("runtime/output.txt"), "output").unwrap();
    let req = write(
        root,
        "requirements.json",
        &requirements(&head, &repository.snapshot),
    );
    let gate_path = write(root, "gate.json", &gate(&head, &repository.snapshot));
    let critic_path = write(
        root,
        "critic.json",
        &critic(&head, &repository.snapshot, true),
    );
    let steps = [
        ("worker_completed", None, None),
        (
            "gate_completed",
            Some(req.as_str()),
            Some(gate_path.as_str()),
        ),
        (
            "critic_completed",
            Some(req.as_str()),
            Some(critic_path.as_str()),
        ),
        ("remediation_completed", None, None),
        (
            "gate_completed",
            Some(req.as_str()),
            Some(gate_path.as_str()),
        ),
        (
            "critic_completed",
            Some(req.as_str()),
            Some(critic_path.as_str()),
        ),
        ("remediation_completed", None, None),
        (
            "gate_completed",
            Some(req.as_str()),
            Some(gate_path.as_str()),
        ),
        (
            "critic_completed",
            Some(req.as_str()),
            Some(critic_path.as_str()),
        ),
    ];
    for (revision, (kind, requirements, handback)) in steps.iter().enumerate() {
        let result = apply(
            &repository,
            root,
            &format!("event-{revision}.json"),
            &event(
                &head,
                &repository.snapshot,
                &format!("e{revision}"),
                revision as u64,
                kind,
                *requirements,
                *handback,
            ),
        );
        if revision == 8 {
            assert_eq!(result["stage"], "held");
            assert!(result["disposition"].as_str().unwrap().contains("cap"));
        }
    }
    std::fs::write(root.join("runtime/output.txt"), "changed evidence").unwrap();
    let input = write(
        root,
        "next.json",
        &event(&head, &repository.snapshot, "e9", 9, "hold", None, None),
    );
    assert!(
        run(
            &repository,
            &[
                "--state".into(),
                "runtime/state.json".into(),
                "--event".into(),
                input
            ]
        )
        .is_err()
    );
}

#[test]
fn forged_ready_state_and_unsafe_paths_are_rejected() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    let input = write(
        root,
        "event.json",
        &event(
            &head,
            &repository.snapshot,
            "e1",
            0,
            "worker_completed",
            None,
            None,
        ),
    );
    assert!(
        run(
            &repository,
            &[
                "--state".into(),
                "../escape.json".into(),
                "--event".into(),
                input.clone()
            ]
        )
        .is_err()
    );
    std::fs::write(root.join("runtime/state.json"),serde_json::to_vec(&json!({"schema_version":1,"head":head,"snapshot":repository.snapshot,"unit":"u1","revision":0,"stage":"ready","critic_cycles":0,"disposition":null,"events":[]})).unwrap()).unwrap();
    assert!(
        run(
            &repository,
            &[
                "--state".into(),
                "runtime/state.json".into(),
                "--event".into(),
                input.clone()
            ]
        )
        .is_err()
    );
    std::fs::remove_file(root.join("runtime/state.json")).unwrap();
    std::os::unix::fs::symlink(
        root.join("runtime/event.json"),
        root.join("runtime/link.json"),
    )
    .unwrap();
    assert!(
        run(
            &repository,
            &[
                "--state".into(),
                "runtime/state.json".into(),
                "--event".into(),
                "runtime/link.json".into()
            ]
        )
        .is_err()
    );
}

#[test]
fn held_lock_prevents_state_write() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    let input = write(
        root,
        "event.json",
        &event(
            &head,
            &repository.snapshot,
            "e1",
            0,
            "worker_completed",
            None,
            None,
        ),
    );
    std::fs::write(root.join("runtime/state.json.lock"), "held").unwrap();
    let result = run(
        &repository,
        &[
            "--state".into(),
            "runtime/state.json".into(),
            "--event".into(),
            input,
            "--apply".into(),
        ],
    );
    assert!(result.is_err());
    assert!(!root.join("runtime/state.json").exists());
}

#[test]
fn duplicate_event_cannot_report_ready_for_changed_inputs() {
    let (directory, repository, head) = setup();
    let root = directory.path();
    let payload = event(
        &head,
        &repository.snapshot,
        "e1",
        0,
        "worker_completed",
        None,
        None,
    );
    apply(&repository, root, "first.json", &payload);
    std::fs::write(root.join("tracked.txt"), "changed").unwrap();
    let args = [
        "--state".into(),
        "runtime/state.json".into(),
        "--event".into(),
        "runtime/first.json".into(),
    ];
    assert!(run(&repository, &args).is_err());
    let refreshed = Repository::load(root, "worktree").unwrap();
    assert!(run(&refreshed, &args).is_err());
}
