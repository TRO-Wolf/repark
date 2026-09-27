# Evidence and local workflow contract

The evidence command validates typed gate or critic handbacks against an independent requirements roster. The workflow command records local review state and emits bounded action intents. It executes no gate, launches no worker or critic, and performs no merge. A separate authorized driver must establish the provenance of every submitted artifact and approve any action it takes from an intent.

All JSON objects reject unknown fields. Runtime JSON, state and evidence paths are repository-relative, untracked, regular files. Paths cannot traverse upward or cross a symlink. Evidence artifacts must contain at least one byte. The 40-character `head` binds the Git commit; the 64-character `snapshot` binds the content loaded by `Repository::load` from tracked worktree files. A tracked dirty edit changes that snapshot and invalidates old current evidence. Untracked runtime artifacts do not change it.

## Requirements and gate handback

`evidence --requirements runtime/requirements.json --input runtime/gate.json --head <full-sha> --unit unit-1` checks the current HEAD and snapshot. Requirements are supplied separately from the handback. The example SHA and digest below are placeholders for actual full values.

```json
{
  "schema_version": 1,
  "head": "0123456789abcdef0123456789abcdef01234567",
  "snapshot": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "unit": "unit-1",
  "actor_id": "actor-a",
  "required_gates": [{"gate": "verify", "command": "make verify"}],
  "required_coverage": ["code", "security"],
  "severity_floor": "s1"
}
```

Both rosters require unique identifiers. A gate receipt must name the exact required command, a run ID, exit code, matching head and snapshot, and a nonempty output artifact. Gate handbacks cannot carry critic review or findings.

```json
{
  "schema_version": 1,
  "phase": "gate",
  "head": "0123456789abcdef0123456789abcdef01234567",
  "snapshot": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "unit": "unit-1",
  "complete": true,
  "receipts": [{"gate": "verify", "command": "make verify", "run_id": "run-41", "exit_code": 0, "head": "0123456789abcdef0123456789abcdef01234567", "snapshot": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "evidence": "runtime/verify.log"}],
  "review": null,
  "findings": []
}
```

## Critic handback

The critic phase requires a reviewer ID distinct from the actor ID and exactly one coverage record for every required coverage ID. `attacked` needs a nonempty evidence artifact and null reason. `not_applicable` needs a nonempty reason and null evidence. The `findings` array is required even when no findings were reported. Critic handbacks cannot carry gate receipts.

```json
{
  "schema_version": 1,
  "phase": "critic",
  "head": "0123456789abcdef0123456789abcdef01234567",
  "snapshot": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "unit": "unit-1",
  "complete": true,
  "receipts": [],
  "review": {"reviewer_id": "reviewer-b", "coverage": [
    {"id": "code", "disposition": "attacked", "evidence": "runtime/critic-code.txt", "reason": null},
    {"id": "security", "disposition": "not_applicable", "evidence": null, "reason": "No security-sensitive changes"}
  ]},
  "findings": [{"id": "F-1", "severity": "s3", "evidence": "runtime/critic-code.txt", "disposition": "open"}]
}
```

The severity floor is fixed at S1. An open or disputed S0/S1 finding blocks readiness. `accepted_flagged` is invalid at S0/S1; an open S2/S3 finding remains advisory. A nonzero gate exit blocks readiness. Missing roster entries, stale bindings, false completeness, absent artifacts and invalid references make the structure invalid. Output separates `structurally_valid` from `ok`: a structurally valid blocked handback has `structurally_valid: true` and `ok: false`. Claimed commands, run IDs, reviewer identities and coverage actions are metadata. Schema validation cannot prove that a command ran or that an independent person or process reviewed the work.

## Local workflow

`workflow --state runtime/state.json --event runtime/event.json` plans without writing. Add `--apply` to persist one event. Each event has a stable ID and expected revision. Exact duplicate IDs are idempotent while their input binding remains current; contradictory reuse, stale inputs and stale revisions fail.

```json
{
  "schema_version": 1,
  "id": "event-1",
  "expected_revision": 0,
  "head": "0123456789abcdef0123456789abcdef01234567",
  "snapshot": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "unit": "unit-1",
  "kind": "worker_completed",
  "requirements": null,
  "handback": null
}
```

A `gate_completed` or `critic_completed` event supplies the relevant requirements and handback paths. The phase field must match its event kind. Other events use null evidence paths. The sequence is `worker` → `gate` → `critic` → `ready`, with blocked assessments moving through `remediation` to `gate`. Only `remediation_completed` can change the recorded head and snapshot. The first gate fixes the requirements policy: schema version, unit, actor identity, exact gate names and commands, coverage roster, and severity floor. Every later gate and critic must use that same policy, including after remediation. An explicit hold or a third blocked critic cycle moves to `held` with a disposition. `ready` means evidence is ready for review; it is never an automatic merge.

State records each event, its validated ready decision, a SHA-256 digest of the requirements, handback and referenced artifacts, and the first gate's policy digest. On load, the controller revalidates historical artifacts against their recorded head and snapshot and reconstructs the state and policy digest. Historical events do not have to match the *current* checkout after a remediation commit. Artifact mutation, policy drift, or an inconsistent stage, revision, cycle count or disposition fails closed. This is an accidental-corruption and consistency guard, not a signature: someone who can rewrite state and all evidence can forge a matching history. The authorized driver must protect those files or verify provenance elsewhere.

Apply acquires a create-new lock, writes a temporary state file, syncs it, atomically replaces state and syncs the parent directory. Concurrent apply fails while the lock exists. After a process crash leaves a lock, an operator must inspect state before removing it. Action intents are limited to `request_gate_evidence`, `request_critic_evidence`, and `request_remediation`; they contain no command, destination, credential or authorization to launch work.

Malformed JSON or unsafe paths return an error. A well-formed handback with a failed gate or blocking finding returns `ok: false`. Drivers must preserve this distinction. Runtime evidence files must remain available and unchanged for later replay.
