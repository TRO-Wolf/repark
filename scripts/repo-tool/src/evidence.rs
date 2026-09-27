use crate::repository::{Repository, checked_path, git, validate_relative};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GateRequirement {
    pub gate: String,
    pub command: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Requirements {
    pub schema_version: u8,
    pub head: String,
    pub snapshot: String,
    pub unit: String,
    pub actor_id: String,
    pub required_gates: Vec<GateRequirement>,
    pub required_coverage: Vec<String>,
    pub severity_floor: Severity,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    S0,
    S1,
    S2,
    S3,
}

impl Severity {
    fn rank(&self) -> u8 {
        match self {
            Self::S0 => 0,
            Self::S1 => 1,
            Self::S2 => 2,
            Self::S3 => 3,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Open,
    Remediated,
    AcceptedFlagged,
    Withdrawn,
    Disputed,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Gate,
    Critic,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub gate: String,
    pub command: String,
    pub run_id: String,
    pub exit_code: i32,
    pub head: String,
    pub snapshot: String,
    pub evidence: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Finding {
    pub id: String,
    pub severity: Severity,
    pub evidence: String,
    pub disposition: Disposition,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageDisposition {
    Attacked,
    NotApplicable,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Coverage {
    pub id: String,
    pub disposition: CoverageDisposition,
    pub evidence: Option<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Review {
    pub reviewer_id: String,
    pub coverage: Vec<Coverage>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Handback {
    pub schema_version: u8,
    pub phase: Phase,
    pub head: String,
    pub snapshot: String,
    pub unit: String,
    pub complete: bool,
    pub receipts: Vec<Receipt>,
    pub review: Option<Review>,
    pub findings: Vec<Finding>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Assessment {
    pub valid: bool,
    pub ready: bool,
    pub errors: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Serialize)]
struct EvidenceOutput {
    ok: bool,
    structurally_valid: bool,
    assessment: Assessment,
    unit: String,
    head: String,
    snapshot: String,
}

pub(crate) fn argument(arguments: &[String], name: &str) -> Result<String> {
    let mut values = arguments.windows(2).filter(|pair| pair[0] == name);
    let value = values.next().with_context(|| format!("missing {name}"))?[1].clone();
    if values.next().is_some() || value.starts_with("--") {
        bail!("invalid {name}");
    }
    Ok(value)
}

pub(crate) fn file<T: for<'de> Deserialize<'de>>(repository: &Repository, path: &str) -> Result<T> {
    let bytes = artifact(repository, path)?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {path}"))
}

pub(crate) fn artifact(repository: &Repository, path: &str) -> Result<Vec<u8>> {
    validate_relative(path)?;
    if repository.paths.contains(path) {
        bail!("runtime artifact is tracked: {path}");
    }
    let path_on_disk = checked_path(&repository.root, path)?;
    if !path_on_disk.is_file() {
        bail!("artifact is not a regular file: {path}");
    }
    std::fs::read(&path_on_disk).with_context(|| format!("read {}", path_on_disk.display()))
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "-_.".contains(character))
}

fn full_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn full_snapshot(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn nonempty_artifact(repository: &Repository, path: &str) -> bool {
    artifact(repository, path).is_ok_and(|bytes| !bytes.is_empty())
}

pub(crate) fn evidence_digest(
    repository: &Repository,
    requirements_path: &str,
    handback_path: &str,
    handback: &Handback,
) -> Result<String> {
    let mut paths = BTreeSet::from([requirements_path.to_owned(), handback_path.to_owned()]);
    for receipt in &handback.receipts {
        paths.insert(receipt.evidence.clone());
    }
    for finding in &handback.findings {
        paths.insert(finding.evidence.clone());
    }
    if let Some(review) = &handback.review {
        for coverage in &review.coverage {
            if let Some(path) = &coverage.evidence {
                paths.insert(path.clone());
            }
        }
    }
    let mut digest = Sha256::new();
    for path in paths {
        let bytes = artifact(repository, &path)?;
        digest.update(path.as_bytes());
        digest.update([0]);
        digest.update(bytes.len().to_le_bytes());
        digest.update(bytes);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn validate_binding(
    repository: &Repository,
    requirements: &Requirements,
    handback: &Handback,
    current: bool,
    errors: &mut Vec<String>,
) -> Result<()> {
    if requirements.schema_version != 1 || handback.schema_version != 1 {
        errors.push("unsupported schema version".to_owned());
    }
    if requirements.severity_floor.rank() != 1 {
        errors.push("severity floor must be S1".to_owned());
    }
    if !full_sha(&requirements.head) || !full_snapshot(&requirements.snapshot) {
        errors.push("invalid head or snapshot".to_owned());
    }
    if current {
        let current_head = String::from_utf8(git(&repository.root, &["rev-parse", "HEAD"])?)?;
        let refreshed = Repository::load(&repository.root, "worktree")?;
        if requirements.head != current_head.trim()
            || requirements.snapshot != repository.snapshot
            || requirements.snapshot != refreshed.snapshot
        {
            errors.push("stale head or content snapshot".to_owned());
        }
    }
    if !valid_id(&requirements.unit)
        || !valid_id(&requirements.actor_id)
        || requirements.unit != handback.unit
    {
        errors.push("unit or actor identity invalid".to_owned());
    }
    if handback.head != requirements.head || handback.snapshot != requirements.snapshot {
        errors.push("handback head or snapshot mismatch".to_owned());
    }
    if !handback.complete {
        errors.push("completeness attestation is false".to_owned());
    }
    Ok(())
}

fn validate_rosters<'a>(
    requirements: &'a Requirements,
    errors: &mut Vec<String>,
) -> (BTreeMap<&'a str, &'a str>, BTreeSet<&'a str>) {
    let mut required = BTreeMap::new();
    for gate in &requirements.required_gates {
        if !valid_id(&gate.gate)
            || gate.command.trim().is_empty()
            || required
                .insert(gate.gate.as_str(), gate.command.as_str())
                .is_some()
        {
            errors.push(format!("invalid or duplicate required gate: {}", gate.gate));
        }
    }
    if required.is_empty() {
        errors.push("required gate roster is empty".to_owned());
    }
    let mut coverage_roster = BTreeSet::new();
    for id in &requirements.required_coverage {
        if !valid_id(id) || !coverage_roster.insert(id.as_str()) {
            errors.push(format!("invalid or duplicate coverage ID: {id}"));
        }
    }
    if coverage_roster.is_empty() {
        errors.push("required coverage roster is empty".to_owned());
    }
    (required, coverage_roster)
}

fn validate_gate(
    repository: &Repository,
    requirements: &Requirements,
    handback: &Handback,
    required: &BTreeMap<&str, &str>,
    errors: &mut Vec<String>,
    blockers: &mut Vec<String>,
) {
    if handback.review.is_some() || !handback.findings.is_empty() {
        errors.push("gate handback has critic fields".to_owned());
    }
    let mut seen = BTreeSet::new();
    for receipt in &handback.receipts {
        if !seen.insert(receipt.gate.as_str())
            || required.get(receipt.gate.as_str()) != Some(&receipt.command.as_str())
        {
            errors.push(format!(
                "unexpected, duplicate, or wrong command receipt: {}",
                receipt.gate
            ));
        }
        if receipt.head != requirements.head
            || receipt.snapshot != requirements.snapshot
            || !valid_id(&receipt.run_id)
            || !nonempty_artifact(repository, &receipt.evidence)
        {
            errors.push(format!("invalid receipt: {}", receipt.gate));
        }
        if receipt.exit_code != 0 {
            blockers.push(format!(
                "gate {} exited {}",
                receipt.gate, receipt.exit_code
            ));
        }
    }
    for gate in required.keys() {
        if !seen.contains(gate) {
            errors.push(format!("missing required gate: {gate}"));
        }
    }
}

fn validate_critic(
    repository: &Repository,
    requirements: &Requirements,
    handback: &Handback,
    coverage_roster: &BTreeSet<&str>,
    errors: &mut Vec<String>,
) {
    if !handback.receipts.is_empty() {
        errors.push("critic handback has gate receipts".to_owned());
    }
    let Some(review) = &handback.review else {
        errors.push("critic review attestation missing".to_owned());
        return;
    };
    if !valid_id(&review.reviewer_id) || review.reviewer_id == requirements.actor_id {
        errors.push("reviewer identity absent or same as actor".to_owned());
    }
    let mut seen = BTreeSet::new();
    for coverage in &review.coverage {
        if !coverage_roster.contains(coverage.id.as_str()) || !seen.insert(coverage.id.as_str()) {
            errors.push(format!("unexpected or duplicate coverage: {}", coverage.id));
        }
        match coverage.disposition {
            CoverageDisposition::Attacked
                if !coverage
                    .evidence
                    .as_deref()
                    .is_some_and(|path| nonempty_artifact(repository, path))
                    || coverage.reason.is_some() =>
            {
                errors.push(format!("invalid attacked coverage: {}", coverage.id));
            }
            CoverageDisposition::NotApplicable
                if coverage
                    .reason
                    .as_deref()
                    .is_none_or(|reason| reason.trim().is_empty())
                    || coverage.evidence.is_some() =>
            {
                errors.push(format!("invalid not-applicable coverage: {}", coverage.id));
            }
            _ => {}
        }
    }
    for id in coverage_roster.difference(&seen) {
        errors.push(format!("missing coverage: {id}"));
    }
}

fn validate_findings(
    repository: &Repository,
    requirements: &Requirements,
    handback: &Handback,
    errors: &mut Vec<String>,
    blockers: &mut Vec<String>,
) {
    let mut finding_ids = BTreeSet::new();
    for finding in &handback.findings {
        if !valid_id(&finding.id)
            || !finding_ids.insert(finding.id.as_str())
            || !nonempty_artifact(repository, &finding.evidence)
        {
            errors.push(format!("invalid finding: {}", finding.id));
        }
        let at_floor = finding.severity.rank() <= requirements.severity_floor.rank();
        match finding.disposition {
            Disposition::Open | Disposition::Disputed if at_floor => {
                blockers.push(format!("finding {} at severity floor", finding.id));
            }
            Disposition::AcceptedFlagged if at_floor => {
                errors.push(format!(
                    "finding {} accepted at or above severity floor",
                    finding.id
                ));
            }
            _ => {}
        }
    }
}

pub(crate) fn assess(
    repository: &Repository,
    requirements: &Requirements,
    handback: &Handback,
    current: bool,
) -> Result<Assessment> {
    let mut errors = Vec::new();
    let mut blockers = Vec::new();
    validate_binding(repository, requirements, handback, current, &mut errors)?;
    let (required, coverage_roster) = validate_rosters(requirements, &mut errors);
    match handback.phase {
        Phase::Gate => validate_gate(
            repository,
            requirements,
            handback,
            &required,
            &mut errors,
            &mut blockers,
        ),
        Phase::Critic => validate_critic(
            repository,
            requirements,
            handback,
            &coverage_roster,
            &mut errors,
        ),
    }
    validate_findings(
        repository,
        requirements,
        handback,
        &mut errors,
        &mut blockers,
    );
    let valid = errors.is_empty();
    Ok(Assessment {
        valid,
        ready: valid && blockers.is_empty(),
        errors,
        blockers,
    })
}

#[expect(
    clippy::missing_errors_doc,
    reason = "CLI error contract is documented in WORKFLOW.md"
)]
pub fn run(repository: &Repository, arguments: &[String]) -> Result<Value> {
    if repository.source != "worktree"
        || repository.input_scope != crate::repository::InputScope::Full
    {
        bail!("evidence requires a full worktree snapshot");
    }
    let input = argument(arguments, "--input")?;
    let requirements_path = argument(arguments, "--requirements")?;
    let head = argument(arguments, "--head")?;
    let unit = argument(arguments, "--unit")?;
    let requirements: Requirements = file(repository, &requirements_path)?;
    let handback: Handback = file(repository, &input)?;
    let mut assessment = assess(repository, &requirements, &handback, true)?;
    if head != requirements.head || unit != requirements.unit {
        assessment.valid = false;
        assessment.ready = false;
        assessment
            .errors
            .push("CLI head or unit does not match requirements".to_owned());
    }
    Ok(serde_json::to_value(EvidenceOutput {
        ok: assessment.ready,
        structurally_valid: assessment.valid,
        assessment,
        unit: requirements.unit,
        head: requirements.head,
        snapshot: requirements.snapshot,
    })?)
}
