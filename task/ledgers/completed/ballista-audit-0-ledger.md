# Unit ledger — BALLISTA-AUDIT-0 steps 1–2 · Half A facts, Half B judgement

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when BALLISTA-AUDIT-0 merges.

**Unit:** BALLISTA-AUDIT-0 steps 1–2 · **Date:** 2026-09-08 · **Model:** muse-spark-1.3-contributor · **Branch:** `docs/ballista-audit-0` · **Path:** READING
**Document:** [../../roadmap/epic-term/ballista-audit-2026-09-08.md](../../roadmap/epic-term/ballista-audit-2026-09-08.md)
**Upstream:** Apache DataFusion Ballista tag `54.1.0`, commit `f4e66525`, at `upstream-ballista/` (git-excluded, never committed).

"Red first" does not apply to this docs-only reading unit: there is no pin
against base-tree behaviour, only measured numbers and read judgements from
the pinned upstream checkout. Each clause below is PROVEN on document
evidence under R-10 (2026-09-09): its evidence cell names the discharging
section of the audit document. Step-2 work below; **Model:** muse-spark-1.3-contributor.

## Clause table

| Clause | Inventory item | Verdict | Evidence (command + head of output) |
|---|---|---|---|
| C-001 | Line counts by crate and directory, tests split into `tests/` vs `#[cfg(test)]` | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#appendix-a1--line-counts-by-crate-and-directory |
| C-002 | Direct dependencies per crate (D-8: `Cargo.toml` source, no `cargo tree`) | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#appendix-a2--direct-dependencies-per-crate |
| C-003 | Protobuf file inventory with line counts | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#appendix-a3--protobuf-file-inventory |
| C-004 | Extension-point inventory by grep, file and line per hit | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#appendix-a4--extension-point-inventory-by-grep |
| C-005 | File list per lifecycle step (submit → result) | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#appendix-a5--file-list-per-lifecycle-step |

## Notes

Step 1 writes Half A only: the document's §26 A–H + §27 headings exist, the
five appendix tables are filled, §26 prose and the §28 decision gate stay for
step 2. D-1 confirmed measured: `git log --oneline -1` → `f4e66525`,
`git describe --tags` → `54.1.0`, root `Cargo.toml` line 37
`datafusion = "54"` against this workspace's `datafusion = "54.1.0"`.

## Step-2 clause table (Half B judgement, D-2/D-3/D-4)

| Clause | Judgement item | Verdict | Evidence (document section + measured anchor) |
|---|---|---|---|
| C-006 | §26.A–C: version scope, inventory and dependency judgements | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#26a--version-pair-and-audit-scope |
| C-007 | §26.D: serialization chapter + ADR-0004 answer | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#26d--serialization-protobuf-files-and-extension-codecs-facts-in-appendixes-a3a4 |
| C-008 | §26.E: lifecycle judgement | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#26e--lifecycle-file-map-submit-to-result-facts-in-appendix-a5 |
| C-009 | §26.F + appendix A6: classification, crate structure, python gap | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#26f--module-classification-and-crate-structure-proposal |
| C-010 | §26.G: risks R-1–R-7 | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#26g--risks |
| C-011 | §26.H + §28: depend recommendation + decision gate | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#28--decision-gate |

Step-2 notes: appendix numbers untouched (D-7 not triggered except the new
A6 row, which carries its own command). Headings unchanged per N-3.
`upstream-ballista/` never staged (excluded; moved aside for the commit only
if hooks scan it, then restored at `f4e66525`).

## Step-3 clause (orchestrator, tier O)

| Clause | Item | Verdict | Evidence |
|---|---|---|---|
| C-012 | The ADR-0004 disposition is recorded, and the unification brief points at the audit as its Milestone 0 | PROVEN | docs: task/roadmap/epic-term/ballista-audit-2026-09-08.md#26d--serialization-protobuf-files-and-extension-codecs-facts-in-appendixes-a3a4 |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ballista-audit-0
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each card clause was walked against the document itself, not a paraphrase. The orchestrator reproduced two Half A numbers exactly (187 files / 76209 lines; 4 .proto files) and five Half B citations exactly (ballista.proto 883 lines; serde/mod.rs lines 185, 352, 364; scheduler 30182; execution_graph.rs 3020) against the pinned upstream checkout at f4e66525.
      artifacts: [task/roadmap/epic-term/ballista-audit-2026-09-08.md, docs/adr/0004-server-prep-disciplines.md]
    - id: AT-2
      status: ATTACKED
      evidence: The measurement boundary was attacked and found short — the seven enumerated directories omitted five upstream .rs files (python/ binding, dev/msrvcheck). Appendix A6 closes that gap with its own command, and the python/ binding is now covered in §26 because it bears on the Python-API requirement.
      artifacts: [task/roadmap/epic-term/ballista-audit-2026-09-08.md]
    - id: AT-3
      status: N/A
      justification: A reading unit. No code, no execution path, no failure mode is introduced; the deliverable is one document plus a disposition paragraph in an existing ADR.
    - id: AT-4
      status: N/A
      justification: No state, no ordering, no concurrency is introduced. The only shared artifact is the git-excluded upstream scratch checkout, verified byte-identical at f4e66525 after each worker moved it aside for a commit, and now moved out of the clone entirely.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no deserialization, no path handling. Nothing from the audited upstream is vendored; the scratch checkout never entered a commit.
    - id: AT-6
      status: ATTACKED
      evidence: The compatibility question this unit exists to answer — whether Ballista's protobuf plan serialization can carry RePark's Iceberg write/commit nodes — was attacked at the source and the ADR's stated premise was found imprecise. The codec CAN carry custom nodes through a single-slot override; the write ban stands on the commit-coordinator ground instead. Recorded as a disposition in the ADR rather than a silent correction.
      artifacts: [docs/adr/0004-server-prep-disciplines.md, task/roadmap/epic-term/ballista-audit-2026-09-08.md]
    - id: AT-7
      status: N/A
      justification: No execution path is added, so there is no resource or performance behaviour to break. The audit's own cost numbers (about 59k source plus 20k test lines to import) are inputs to the depend-versus-import recommendation, not a performance claim about this repository.
    - id: AT-8
      status: ATTACKED
      evidence: The upstream contract is the whole subject. D-1's version pair was verified by the orchestrator, not the worker: tag 54.1.0, commit f4e66525, upstream root Cargo.toml `datafusion = "54"` against this workspace's `datafusion = "54.1.0"`. Every extension-point claim cites file and line in the pinned tree.
      artifacts: [task/roadmap/epic-term/ballista-audit-2026-09-08.md]
    - id: AT-9
      status: N/A
      justification: No failure path exists to diagnose. The document's own reproducibility is the operability property, and it is held by D-7: every number carries the command that produced it.
    - id: AT-10
      status: N/A
      justification: A documentation unit whose clauses prove on document evidence under the grammar gate's reading-unit rule (R-10, 2026-09-09), so no test pins them. The gates that do apply were re-run by the orchestrator, not taken on the workers' word: check-docs-compaction and check-ledgers green on the step-1, step-2 and departure trees, and make preflight before the PR.
  complete: true
```

## Why every clause reads PROVEN (R-10, 2026-09-09)

Each clause's evidence cell names the section of
[the audit document](../../roadmap/epic-term/ballista-audit-2026-09-08.md) that
discharges it; the commands and measured outputs behind every number live in
that section. `scripts/check_ledger_grammar.py`'s reading-unit rule exempts a
staging ledger whose first 40 lines carry the READING path marker from the
pins-citation rule; rules A and C still hold, so the tables keep their shape
and the attestation above names document sections instead of tests. The
2026-09-08 "parked for the owner" paragraph — the gate had no vocabulary for a
unit whose propositions are measurements — is answered by R-10 and removed.
