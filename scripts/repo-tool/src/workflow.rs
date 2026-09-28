use crate::evidence::{Handback, Phase, Requirements, assess, evidence_digest, file};
use crate::repository::{Repository, checked_path, git};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::Write;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Worker,
    Gate,
    Critic,
    Remediation,
    Held,
    Ready,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    WorkerCompleted,
    GateCompleted,
    CriticCompleted,
    RemediationCompleted,
    Hold,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub schema_version: u8,
    pub id: String,
    pub expected_revision: u64,
    pub head: String,
    pub snapshot: String,
    pub unit: String,
    pub kind: EventKind,
    pub requirements: Option<String>,
    pub handback: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventRecord {
    pub event: Event,
    pub evidence_digest: Option<String>,
    pub ready: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub schema_version: u8,
    pub head: String,
    pub snapshot: String,
    pub unit: String,
    pub revision: u64,
    pub stage: Stage,
    pub critic_cycles: u8,
    pub disposition: Option<String>,
    pub policy_digest: Option<String>,
    pub events: Vec<EventRecord>,
}

#[derive(Serialize)]
struct WorkflowOutput {
    ok: bool,
    applied: bool,
    duplicate: bool,
    revision: u64,
    stage: Stage,
    disposition: Option<String>,
    action_intents: Vec<String>,
}

struct Lock {
    path: std::path::PathBuf,
    _file: File,
}

impl Drop for Lock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn option(arguments: &[String], name: &str) -> Result<String> {
    let mut values = arguments.windows(2).filter(|pair| pair[0] == name);
    let value = values.next().with_context(|| format!("missing {name}"))?[1].clone();
    if values.next().is_some() || value.starts_with("--") {
        bail!("invalid {name}");
    }
    Ok(value)
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "-_.".contains(character))
}

fn initial(event: &Event) -> State {
    State {
        schema_version: 1,
        head: event.head.clone(),
        snapshot: event.snapshot.clone(),
        unit: event.unit.clone(),
        revision: 0,
        stage: Stage::Worker,
        critic_cycles: 0,
        disposition: None,
        policy_digest: None,
        events: Vec::new(),
    }
}

fn current_binding(repository: &Repository, event: &Event) -> Result<()> {
    let current_head = String::from_utf8(git(&repository.root, &["rev-parse", "HEAD"])?)?;
    let refreshed = Repository::load(&repository.root, "worktree")?;
    if event.head.len() != 40
        || event.head != current_head.trim()
        || event.snapshot != repository.snapshot
        || event.snapshot != refreshed.snapshot
        || event.snapshot.len() != 64
    {
        bail!("stale or invalid event head or content snapshot");
    }
    Ok(())
}

fn policy_digest(requirements: &Requirements) -> Result<String> {
    let bytes = serde_json::to_vec(&(
        requirements.schema_version,
        &requirements.unit,
        &requirements.actor_id,
        &requirements.required_gates,
        &requirements.required_coverage,
        &requirements.severity_floor,
    ))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn evaluate(
    repository: &Repository,
    event: &Event,
    current: bool,
) -> Result<(bool, String, String)> {
    let requirements_path = event
        .requirements
        .as_deref()
        .context("event needs requirements")?;
    let handback_path = event.handback.as_deref().context("event needs handback")?;
    let requirements: Requirements = file(repository, requirements_path)?;
    let handback: Handback = file(repository, handback_path)?;
    if event.head != requirements.head
        || event.snapshot != requirements.snapshot
        || event.unit != requirements.unit
    {
        bail!("event does not match evidence requirements");
    }
    let phase = match event.kind {
        EventKind::GateCompleted => Phase::Gate,
        EventKind::CriticCompleted => Phase::Critic,
        _ => bail!("event cannot carry evidence"),
    };
    if handback.phase != phase {
        bail!("evidence phase does not match event phase");
    }
    let assessment = assess(repository, &requirements, &handback, current)?;
    if !assessment.valid {
        bail!("invalid evidence: {}", assessment.errors.join("; "));
    }
    Ok((
        assessment.ready,
        evidence_digest(repository, requirements_path, handback_path, &handback)?,
        policy_digest(&requirements)?,
    ))
}

fn advance(
    state: &mut State,
    event: &Event,
    ready: Option<bool>,
    policy: Option<&str>,
) -> Result<Vec<String>> {
    if event.schema_version != 1
        || !valid_id(&event.id)
        || !valid_id(&event.unit)
        || event.unit != state.unit
    {
        bail!("invalid event identity");
    }
    if state
        .events
        .iter()
        .any(|previous| previous.event.id == event.id)
    {
        bail!("duplicate event in transition history");
    }
    if event.expected_revision != state.revision {
        bail!("stale or out-of-order event revision");
    }
    if event.kind != EventKind::RemediationCompleted
        && (event.head != state.head || event.snapshot != state.snapshot)
    {
        bail!("head or snapshot change requires remediation completion");
    }
    if matches!(
        event.kind,
        EventKind::GateCompleted | EventKind::CriticCompleted
    ) {
        let policy = policy.context("evidence event has no requirements policy")?;
        match &state.policy_digest {
            Some(previous) if previous != policy => {
                bail!("requirements policy changed during workflow")
            }
            None if event.kind != EventKind::GateCompleted => bail!("critic has no gate policy"),
            None => state.policy_digest = Some(policy.to_owned()),
            _ => {}
        }
    } else if policy.is_some() {
        bail!("unexpected requirements policy on non-evidence event");
    }
    let mut intents = Vec::new();
    match (&state.stage, &event.kind, ready) {
        (Stage::Worker, EventKind::WorkerCompleted, None) => {
            state.stage = Stage::Gate;
            intents.push("request_gate_evidence".to_owned());
        }
        (Stage::Gate, EventKind::GateCompleted, Some(passed)) => {
            state.stage = if passed {
                Stage::Critic
            } else {
                Stage::Remediation
            };
            intents.push(
                if passed {
                    "request_critic_evidence"
                } else {
                    "request_remediation"
                }
                .to_owned(),
            );
        }
        (Stage::Critic, EventKind::CriticCompleted, Some(passed)) => {
            state.critic_cycles += 1;
            if passed {
                state.stage = Stage::Ready;
                state.disposition = Some("review_ready".to_owned());
            } else if state.critic_cycles >= 3 {
                state.stage = Stage::Held;
                state.disposition =
                    Some("critic cycle cap reached with blocking evidence".to_owned());
            } else {
                state.stage = Stage::Remediation;
                intents.push("request_remediation".to_owned());
            }
        }
        (Stage::Remediation, EventKind::RemediationCompleted, None) => {
            state.head.clone_from(&event.head);
            state.snapshot.clone_from(&event.snapshot);
            state.stage = Stage::Gate;
            intents.push("request_gate_evidence".to_owned());
        }
        (
            Stage::Worker | Stage::Gate | Stage::Critic | Stage::Remediation,
            EventKind::Hold,
            None,
        ) => {
            state.stage = Stage::Held;
            state.disposition = Some("explicit hold".to_owned());
        }
        _ => bail!("event is invalid for current stage"),
    }
    if ready.is_none() && (event.requirements.is_some() || event.handback.is_some()) {
        bail!("event has unexpected evidence fields");
    }
    state.revision += 1;
    Ok(intents)
}

fn load_state(repository: &Repository, path: &str, event: &Event) -> Result<State> {
    let path_on_disk = checked_path(&repository.root, path)?;
    if !path_on_disk.exists() {
        return Ok(initial(event));
    }
    let state: State = serde_json::from_slice(&std::fs::read(path_on_disk)?)?;
    if state.schema_version != 1
        || state.unit != event.unit
        || state.revision != state.events.len() as u64
        || state.critic_cycles > 3
    {
        bail!("invalid or mismatched workflow state");
    }
    let first = state
        .events
        .first()
        .context("persisted state has no events")?;
    let mut replay = initial(&first.event);
    for record in &state.events {
        let (ready, digest, policy) = if matches!(
            record.event.kind,
            EventKind::GateCompleted | EventKind::CriticCompleted
        ) {
            let (ready, digest, policy) = evaluate(repository, &record.event, false)?;
            (Some(ready), Some(digest), Some(policy))
        } else {
            (None, None, None)
        };
        if ready != record.ready || digest != record.evidence_digest {
            bail!("persisted evidence decision or digest mismatch");
        }
        advance(&mut replay, &record.event, ready, policy.as_deref())?;
        replay.events.push(EventRecord {
            event: record.event.clone(),
            ready,
            evidence_digest: digest,
        });
    }
    if replay.head != state.head
        || replay.snapshot != state.snapshot
        || replay.stage != state.stage
        || replay.critic_cycles != state.critic_cycles
        || replay.disposition != state.disposition
        || replay.policy_digest != state.policy_digest
    {
        bail!("workflow state does not match validated history");
    }
    Ok(state)
}

fn acquire_lock(repository: &Repository, state_path: &str) -> Result<Lock> {
    let path = checked_path(&repository.root, &format!("{state_path}.lock"))?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| format!("workflow state locked: {}", path.display()))?;
    Ok(Lock { path, _file: file })
}

fn save_state(repository: &Repository, path: &str, state: &State) -> Result<()> {
    let destination = checked_path(&repository.root, path)?;
    let temporary = checked_path(
        &repository.root,
        &format!("{path}.tmp.{}", std::process::id()),
    )?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let result = (|| -> Result<()> {
        file.write_all(&serde_json::to_vec_pretty(state)?)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &destination)?;
        if let Some(parent) = destination.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[expect(
    clippy::missing_errors_doc,
    reason = "CLI error contract is documented in WORKFLOW.md"
)]
pub fn run(repository: &Repository, arguments: &[String]) -> Result<Value> {
    if repository.source != "worktree"
        || repository.input_scope != crate::repository::InputScope::Full
    {
        bail!("workflow requires a full worktree snapshot");
    }
    let state_path = option(arguments, "--state")?;
    let event_path = option(arguments, "--event")?;
    if state_path == event_path || repository.paths.contains(&state_path) {
        bail!("state path conflicts with input or tracked source");
    }
    let apply = arguments.iter().any(|argument| argument == "--apply");
    if apply && arguments.iter().any(|argument| argument == "--plan") {
        bail!("choose --plan or --apply");
    }
    let event: Event = file(repository, &event_path)?;
    let _lock = if apply {
        Some(acquire_lock(repository, &state_path)?)
    } else {
        None
    };
    let mut state = load_state(repository, &state_path, &event)?;
    current_binding(repository, &event)?;
    if let Some(previous) = state
        .events
        .iter()
        .find(|record| record.event.id == event.id)
    {
        if previous.event != event {
            bail!("contradictory reuse of event ID");
        }
        return Ok(serde_json::to_value(WorkflowOutput {
            ok: true,
            applied: apply,
            duplicate: true,
            revision: state.revision,
            stage: state.stage,
            disposition: state.disposition,
            action_intents: Vec::new(),
        })?);
    }
    let (ready, digest, policy) = if matches!(
        event.kind,
        EventKind::GateCompleted | EventKind::CriticCompleted
    ) {
        let (ready, digest, policy) = evaluate(repository, &event, true)?;
        (Some(ready), Some(digest), Some(policy))
    } else {
        (None, None, None)
    };
    let intents = advance(&mut state, &event, ready, policy.as_deref())?;
    state.events.push(EventRecord {
        event,
        ready,
        evidence_digest: digest,
    });
    if apply {
        save_state(repository, &state_path, &state)?;
    }
    Ok(serde_json::to_value(WorkflowOutput {
        ok: true,
        applied: apply,
        duplicate: false,
        revision: state.revision,
        stage: state.stage,
        disposition: state.disposition,
        action_intents: intents,
    })?)
}
