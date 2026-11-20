# Production audit — 2026-10-11

**Path:** READING

Class: ledger. Retires when the requested audit and subsequent performance report are delivered; move to completed at the eventual departure commit. No commit or push is authorized.

## Scope and authorization

The owner requested a docs bugs branch, a deep review of the current repository for bugs, safety, security, inaccurate claims and production blockers, followed by a Sol 6.1 High performance review on the same branch. This is a findings-only unit. Runtime remediation, dependency changes, cloud mutations and publication are outside scope. The existing dirty checkout is preserved.

Base: `07be1dc930711bb927204f3df99d08526c9aaef6`, fetched from main on 2026-10-11. Branch: `docs/production-bugs-audit-2026-10-11`. Fork pin: `076d5f982d1d2ef352c3bb5d76d02cff27bc164a`.

## PROPOSITION LEDGER

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The review targets a pinned main revision in an isolated docs branch. | Branch and base identity. | PROVEN | Git HEAD is the base above; original checkout retains its prior changes. |
| C-002 | The audit reports findings rather than changing runtime behavior. | Owner request and explicit file scope. | PROVEN | Owner request; edits limited to this ledger, docs/cutover, docs/perf and their maps. |
| C-003 | Coverage is partitioned into engine semantics, writes and recovery, catalog and filesystem safety, credentials and connectors, lifecycle and resource bounds, packaging and gates, and public claims. | Finite review partition with evidence and limitations per partition. | PROVEN | These seven partitions define the report coverage; unexecuted cells are reported as gaps, never certified. |
| C-004 | The performance agent starts after the primary audit report is written. | Owner sequence and agent configuration. | PROVEN | Owner explicitly requests Sol 6.1 High after the primary review; report will record actual dispatch and completion. |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 4/4. These are scope obligations, not a claim that the product passes review. Owner authorization is the request above.

## Work plan

- [x] Isolate the branch and pin current main.
- [x] Execute baseline gates and establish available local runtime evidence.
- [x] Review the seven partitions with separate quality, security, logic and claims lenses.
- [x] Write ranked findings with triggers, evidence strength, impact and acceptance tests.
- [x] Dispatch Sol 6.1 High for speed and memory review; reconcile its findings.
- [x] Validate document links, maps and final diff; record remaining validation gaps.

## Execution record

2026-10-11: 733 GiB available before worktree/build. Shared Cargo target used to avoid a duplicate build tree. Baseline make verify running; output retained temporarily under /tmp/repark-production-audit-2026-10-11. No other task artifacts removed.

## COVERAGE_ATTESTATION

The primary review uses sequential quality, safety/security, logic and claims lenses in this session. It is a procedural separation, not an independent amnesiac review. The requested Sol 6.1 High performance review is a separate scratch-clone pass after the primary report was written. Category completion means the category was examined within the stated finite scope; it does not mean all repository behavior is certified.

```yaml
COVERAGE_ATTESTATION:
  - id: AT-1
    status: ATTACKED
    artifacts: [docs/cutover/production-audit-2026-10-11.md, scope clauses C-001..C-004]
  - id: AT-2
    status: ATTACKED
    artifacts: [docs/cutover/production-audit-evidence-2026-10-11.md, Unicode and NULL controls]
  - id: AT-3
    status: ATTACKED
    artifacts: [crates/repark-iceberg/src/write/merge/abort.rs, fork transaction unknown-outcome retry and reconciliation, microbatch recovery tests]
  - id: AT-4
    status: ATTACKED
    artifacts: [crates/repark-connect/src/pool.rs, repeatable-read scan lane initialization, microbatch lifecycle and sink fencing]
  - id: AT-5
    status: ATTACKED
    artifacts: [PA-01..PA-04 executed local boundary probes, TLS and redaction source checks, PC-01 scope limitation]
  - id: AT-6
    status: ATTACKED
    artifacts: [PA-05..PA-09, stored Parquet ticks and existing registry evidence]
  - id: AT-7
    status: ATTACKED
    artifacts: [PA-10, PA-11, PC-02 conditional admission gap, separate Sol performance review]
  - id: AT-8
    status: ATTACKED
    artifacts: [make audit outcome, current Cargo/fork pins, fresh module identity and live Spark banner]
  - id: AT-9
    status: ATTACKED
    artifacts: [dbt mismatch exception, caught panic stderr, memory accounting exclusions, failed and cancelled workflow evidence]
  - id: AT-10
    status: ATTACKED
    artifacts: [fresh local direct/refused and permitted controls, stale indexed-transform classification, oracle fixture failures, baseline-preservation pins]
  complete: true
```

No CCC-CONVERGED or production-certification claim is made. Runtime findings remain open. Tests and source admission not exercised are listed in the assessment's scope limits.

2026-10-11: Primary report and evidence appendix written before dispatch. Sol performance agent requested with model `gpt-6.1-sol`, reasoning `high`, isolated same-named branch in a shared-object scratch clone. Root owns assembly and final validation. No commit or push.

2026-10-11: Targeted Python current-build suite: 526 passed, 33 warnings, 370.89 seconds. Document/map/ledger gates passed. The first verify process ended without a final status during the session model change; restarted full verify with explicit exit-code recording. Baseline validation remains pending until that run completes.

2026-10-11: Sol 6.1 High completed. Twelve ranked performance candidates; nine isolated diagnostic samples and four EXPLAIN controls, no new builds or external operations. Report/map transferred to the attached branch. Root spot-checked the strongest retained-state findings. The second verify stopped at a temporary missing link to the not-yet-transferred performance document; exit 2, an audit-assembly error. That document is now present, and full verify was restarted against the assembled branch with persisted exit recording. No product defect inferred from that link failure.

2026-10-11: Full Rust execution observed a two-session microbatch completion failure. The exact same test binary also failed alone after 14.28s with the ten-second termination bound. Recorded as PA-15, cause unclassified under concurrent host work; no runtime fix or test weakening. The full workspace command remains running until its final summary.

2026-10-11: Assembled make verify completed exit 2. Static ci checks passed; Rust command exited101 at core, with 1419 passed/1 failed/1 ignored. Remaining unexecuted crates are being checked separately without fail-fast. PA-15 remains unresolved; report-only delivery will disclose the red default gate.


2026-10-11: Remaining eight crate groups completed exit 0: 5710 passed, zero failed and six ignored across 66 test/doctest result groups. Optional cluster feature was off. Final isolated PA-15 retry passed in 2.19s after broad execution ended; two prior completion-bound failures remain disclosed and causally unclassified. The full default verify result remains exit 2.

2026-10-11: Final docs/map/ledger/spelling and staged whitespace validation completed. Seven Markdown files staged, no runtime source changes, no commit/push/PR. Retained attached worktree, fresh environment, shared caches and uncommitted performance scratch clone for review/reproduction. Removed task-owned temporary fixtures, compiled UTF-8 helper and generated audit requirements after preserving evidence. Free disk at handoff: 623 GiB; overlapping unrelated builds prevent attributing disk growth to this task alone. Full resource record is in the evidence appendix. Findings-only delivery is complete; remediation and production qualification remain open.


## PR publication — 2026-10-12

The owner explicitly requested a pull request for this branch. This authorizes the documentation commit, branch push and PR creation. It supersedes the earlier publication restriction; runtime remediation and cloud mutations remain outside scope. The reports remain pinned to the audited revision, without claiming that later main commits were evaluated.

Publication plan: run the required preflight; record its real outcome; move this finished reading-unit ledger to completed with repaired links; verify installed hooks and the documentation diff; commit, push and open the PR. Open product findings are the requested report's content, not fixes delivered by this unit.

Disk before preflight: 791 GiB available. The previous temporary audit directory is no longer present at pickup; reproduction scripts and selected outputs remain preserved in the evidence appendix. New publication logs use `/tmp/repark-production-audit-pr-2026-10-12`. The original checkout remains outside this unit.


2026-10-12: Full default Rust workspace validation passed: 7433 passed, zero failed and 68 ignored across 76 test/doctest groups; the previously intermittent microbatch test passed. Static checks and an explicit installed pre-commit hook invocation passed. Full preflight rebuilt the editable native module and advanced to the 17088-test facade suite. The initial PR is a draft while its long-plan subprocess tests run; the PR will carry the final preflight outcome. No full preflight success or merge readiness is claimed. Disk after the Rust phase: 790 GiB available. Reports and ledger are complete as a findings-only deliverable; product remediation remains open.
