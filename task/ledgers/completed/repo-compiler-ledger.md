# Repository compiler implementation ledger

Date: 2026-09-27. Model: GPT-6 Sol implementation agents; root integration.
This ledger closes when the repository compiler implementation is delivered and accepted.
Base: f7422565. Branch: codex/repo-compiler. Profile: HIGH (validation and workflow boundaries).

## Owner authorization

The owner approved the eight proposed tooling capabilities and requested GPT-6 Sol agents
on an isolated feature branch. This includes the deliberate map-generator policy amendment.
Commit, push, merge, live campaign dispatch and AWS operations are outside this execution.

## Charter

| Clause | Requirement | Verdict | Evidence |
|---|---|---|---|
| C-001 | Snapshot indexing is deterministic for worktree, index and explicit Git refs; paths cannot escape the repository. | PROVEN | Temporary Git fixture contract. |
| C-002 | Maps validate links and generate opted-in structural sections without changing authored text; check is read-only and writes reject stale snapshots. | PROVEN | Map fixture matrix: add/delete/rename, malformed markers, unsafe paths, repeatability. |
| C-003 | Role context compiles explicit approved source sections with provenance and fails on missing inputs or size overflow. | PROVEN | Role/section/budget fixtures; critical instructions are never silently truncated. |
| C-004 | Typed review evidence binds full revision identities, required checks, canonical severities and dispositions; invalid records cannot pass. | PROVEN | Valid, stale, missing, malformed and blocking-finding fixtures. |
| C-005 | Traceability indexes clauses and recorded pins, reporting missing links without claiming behavioral proof. | PROVEN | Clause/pin fixtures. |
| C-006 | Lifecycle views derive explicit records; existing archive operations remain the canonical mutation path. | PROVEN | Open/closed/invalid record fixtures; no inferred closure. |
| C-007 | Gate definitions validate named Make targets and supplied canonical tool pins; stable diagnostics and input-keyed caching retain failures. | PROVEN | Target/pin/cache invalidation fixtures. |
| C-008 | A persisted event controller applies authorized local transitions idempotently, enforces severity/cycle rules, and emits actions without live dispatch. | PROVEN | Duplicate, stale, out-of-order, cap and invalid-evidence fixtures. |

## Plan

- [x] Implement shared snapshots and standalone Rust build surface.
- [x] Implement maps, role context, traceability, lifecycle views and gate validation.
- [x] Implement typed evidence, diagnostic caching and persisted event decisions.
- [x] Integrate Make targets and the approved map-policy amendment.
- [x] Run negative fixtures, standalone gates, repository verification and independent review.

## Pre-execution review

PER-REPO-COMPILER-1: PROCEED. Scope maps to C-001 through C-008. Work is isolated
in separate clones. Agents own disjoint modules; root owns architecture and assembly.
Malformed input fails closed. No operational campaign files are mutated. No evidence reuse
is authorized for runtime tests. The existing gates remain required.

## Coverage attestation

```text
COVERAGE_ATTESTATION:
  - id: AT-1
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/maps.rs, scripts/repo-tool/tests/trace.rs]
  - id: AT-2
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/evidence.rs, scripts/repo-tool/tests/workflow.rs]
  - id: AT-3
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/repository.rs, scripts/repo-tool/tests/workflow.rs]
  - id: AT-4
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/workflow.rs, scripts/repo-tool/tests/state.rs]
  - id: AT-5
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/repository.rs, scripts/repo-tool/tests/maps.rs]
  - id: AT-6
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/context.rs, scripts/repo-tool/tests/state.rs]
  - id: AT-7
    status: N/A
    justification: The reviewed local compiler has no engine or live-service execution; no system-breaking resource issue was measured.
  - id: AT-8
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/evidence.rs, scripts/repo-tool/tests/gates.rs]
  - id: AT-9
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/cache.rs, scripts/repo-tool/tests/trace.rs]
  - id: AT-10
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/maps.rs, scripts/repo-tool/tests/context.rs, scripts/repo-tool/tests/state.rs]
complete: true
```

Independent Sol review reproduced map prose overwrite, missed reference links, false critic
readiness, empty trace acceptance, malformed lifecycle markers and fenced heading errors.
Each has a regression fixture. Coverage records attacks performed, not a claim that validation
is complete; the verification results below decide delivery.

## Verification record — 2026-09-27

- Standalone `make repo-tool-check`: 53 passing regression tests; formatting, all-targets
  Clippy and the production panic/method gate pass.
- Worktree and index map checks: 338 maps, zero diagnostics. The legacy Python check also
  passes on this base. CLI fixtures verify staged isolation and unchanged managed maps.
- Repository context, selected-unit trace, lifecycle view and configured gate checks pass.
  The critic preset includes the complete contract and returns about 66 KB of source text.
- Owner-ruling bytes, document budgets, ledger grammar/lifecycle, manifest and document links
  pass. Workflow YAML parsing and the pinned workflow security linter pass.
- Independent review findings are fixed with regression tests: phase confusion, weakened
  review rosters, stale evidence, map example overwrite, reference links, fenced headings,
  empty traces and malformed lifecycle markers. The root also pinned duplicate-event stale
  input rejection and filename escaping.
- Full `make -k verify` exited 0: 5,873 workspace tests passed, 8 ignored, none failed.
  Final standalone checks cover the compiler fixes made while the unchanged engine compiled.
  The added map gate was also checked independently against worktree, index and committed HEAD.

## Local timing — 2026-09-27

Five warm invocations on this checkout, with the engine verification build running separately:
Python map check median 0.2475 s; Rust direct check 0.5360 s; Rust cached check 0.4497 s;
Rust build wrapper 0.6288 s. These are wall-clock samples, not a CPU speedup claim. Rust binds
results to tracked content identity and also validates managed inventories and reference links.
The deterministic commands eliminate model work only when the harness adopts their outputs;
no reduction in account token usage has been measured yet.

## Resources and isolation

Free space was checked before builds and at verification boundaries: approximately 1.2 TB
remained initially and 1.1 TB after verification. Separate Sol clones protected the main checkout. Agent build caches were cleaned;
source clones and this branch remain for review. The integration engine cache (40 GB) and compiler cache (2.3 GB) are retained to
avoid rebuilding during review. No source commit, push or live dispatch occurred.

Implementation is verified and staged. The ledger remains in staging until owner acceptance
and an authorized source commit; no delivery or merge is inferred from test results.

## Scoped-input performance follow-up — 2026-09-27

The owner requested correction of the measured Rust slowdown. Map checks now hash tracked
names, modes and presence plus map contents; they do not open unrelated source bodies.
Context capture reads its config and approved sources, keeping the entire contract mandatory.
A changed config between dependency selection and capture fails. Both outputs declare their
input scope. Evidence and workflow still require full repository content identity.
Link diagnostics build one newline index per map instead of rescanning every prefix.

Seven warm samples per command, with command order rotated on this checkout and release
binaries compared directly. The first invocation was excluded. Median wall times:

| Command | Previous Rust | Scoped Rust |
|---|---:|---:|
| Map check | 0.5330 s | 0.1282 s |
| Cached map check | 0.4381 s | 0.0821 s |
| Critic context packet | 0.4305 s | 0.0255 s |

The Python map check measured 0.2488 s. The Rust wrapper, including Cargo's freshness check,
measured 0.2195 s. Map and context output matched the previous binary after excluding the
new scope field and scoped digest. Full index output, including its digest, matched exactly.
Raw samples remain in `/tmp/repo-compiler-scoped-benchmark.json` for local review.
These measurements establish command speed only; account token savings remain unmeasured.

The standalone gate passes all 60 tests, formatting, Clippy and the production panic/method
ban. New tests pin unrelated-body cache reuse, dependency invalidation, staged context,
missing contracts, symlink refusal, stale map writes and diagnostic line numbers. Partial
snapshots cannot authorize runtime evidence. Full repository verification is running.

Disk checks before builds and broad validation found approximately 1.1 TB free. The existing
isolated checkout and build caches remain available for review; no additional checkout was made.

Verification completed: `make -k verify` exited 0. The final run passed all 60 compiler
tests and 5,873 workspace tests, with 8 ignored and no failures. Worktree and index map
checks found 338 maps clean; the committed base found 335 maps clean. Documentation,
ledger grammar and map lockstep checks pass. Temporary benchmark binaries, editing scripts
and the two benchmark cache directories were removed. Raw timing samples and validation
logs remain under `/tmp/` for review. No source commit, push or live dispatch occurred.

## PR preparation authorization — 2026-09-27

The owner requested the PR after accepting the implementation and asking for deferred roadmap
capture. This supersedes the earlier commit/push exclusion: commit, push and PR creation are
now authorized. Merge and live harness dispatch remain outside scope. The roadmap extension
is documentation only and does not authorize the deferred implementation.

SLR-PR-PREP-1: PROCEED. Trace C-001..C-008 and check-port C-001..C-004. Preserve the reviewed
branch in a hook-checked commit, incorporate the current main release commit, run preflight,
obtain independent readiness review, then publish. Main advanced by one release-only commit
(9392dbc3); no engine implementation changed. The integration checkout is isolated and has
approximately 1.1 TB free. Conflict resolution must preserve both release metadata and compiler
navigation. No destructive reset or hook bypass is part of recovery; retain the branch on failure.

## Findings index reconstructed for readiness — 2026-09-27

The prior review recorded reproduced defects in prose. This index assigns stable identifiers
and severities during PR preparation; it does not invent a new review cycle or claim retained
red logs where none were preserved. Each regression below is in the enforced compiler suite.

```yaml
FINDING:
  id: F-compiler-1
  severity: S1
  category: AT-2
  clause: C-002
  disposition: REMEDIATED
  summary: Fenced map examples could be treated as managed blocks and overwrite authored prose.
  regression: scripts/repo-tool/tests/maps.rs::fenced_markers_preserve_authored_example
```

```yaml
FINDING:
  id: F-compiler-2
  severity: S1
  category: AT-6
  clause: C-002
  disposition: REMEDIATED
  summary: Reference links and missing definitions were not fully validated.
  regression: scripts/repo-tool/tests/maps.rs::reference_links_validate_targets_and_missing_definitions
```

```yaml
FINDING:
  id: F-compiler-3
  severity: S1
  category: AT-4
  clause: C-004, C-008
  disposition: REMEDIATED
  summary: Gate evidence could be presented as critic evidence and falsely advance readiness.
  regression: scripts/repo-tool/tests/workflow.rs::gate_evidence_cannot_advance_critic_stage
```

```yaml
FINDING:
  id: F-compiler-4
  severity: S1
  category: AT-4
  clause: C-004, C-008
  disposition: REMEDIATED
  summary: A critic could lower the requirements roster accepted by the preceding gate.
  regression: scripts/repo-tool/tests/workflow.rs::critic_cannot_lower_coverage_roster_after_gate
```

```yaml
FINDING:
  id: F-compiler-5
  severity: S1
  category: AT-1
  clause: C-005
  disposition: REMEDIATED
  summary: An empty trace or fenced citation could be accepted as a valid explicit trace.
  regression: scripts/repo-tool/tests/trace.rs::trace_requires_clause_rows_and_unfenced_pin_citations
```

```yaml
FINDING:
  id: F-compiler-6
  severity: S1
  category: AT-6
  clause: C-006
  disposition: REMEDIATED
  summary: Unclosed lifecycle markers were not rejected.
  regression: scripts/repo-tool/tests/state.rs::state_rejects_unclosed_markers_and_keeps_archived_unit
```

```yaml
FINDING:
  id: F-compiler-7
  severity: S1
  category: AT-6
  clause: C-003
  disposition: REMEDIATED
  summary: Fenced headings could be selected as real context sections.
  regression: scripts/repo-tool/tests/context.rs::context_never_selects_heading_inside_mixed_or_long_fence
```

```yaml
FINDING:
  id: F-compiler-8
  severity: S1
  category: AT-4
  clause: C-008
  disposition: REMEDIATED
  summary: A duplicate event could return ready after tracked inputs changed.
  regression: scripts/repo-tool/tests/workflow.rs::duplicate_event_cannot_report_ready_for_changed_inputs
```

The independent readiness auditor rechecks this index against the final test source and results.
No compiler finding remains open or accepted-flagged. The check-port error normalization flag
is recorded separately in that unit ledger.

## Publication verification — 2026-09-27

The owner authorized commit, push and PR creation. The branch rebased cleanly onto release
commit 9392dbc3; source implementation is cc2f891d. Installed commit hooks were explicitly
invoked and passed before the first commit, then fired on the commit itself.

Full `make -k preflight` exited 0: 5,954 Rust/compiler tests passed, 8 ignored; facade
13,875 passed, 481 skipped, 147 expected failures; parity-cap 23 passed; dbt 64 passed,
1 skipped. Rust/Python dependency audits and workflow parsing/security checks passed.
The full log is `/tmp/repo-compiler-preflight.log`. Prior 36 Python differential-reference
regression tests also passed. No engine source was changed during publication preparation.

Independent readiness auditor `sol_pr_readiness` confirmed scope, complete coverage, finding
dispositions and clause trace. Remote PR-head CI remains a separate publication requirement;
this record does not claim CI success, merge or delivery acceptance. Final departure changes
are ledger relocation, link repairs and the matching context-preset path. They receive scoped
compiler, context, documentation and lifecycle validation after relocation.

Implementation disposition: CONVERGED. The user accepted the work and requested publication;
the implementation ledger now moves to completed in the departure commit. PR review and
merge remain pending. Earlier staging/authorization statements describe their dated phase
and are superseded by this publication record. No live harness dispatch is authorized.

Disk checks before and during preflight found approximately 1.1 TB free. The isolated checkout,
its build caches and validation logs remain for PR review; only task-owned disposable
collection cache files are removed. No other task's checkout or artifacts were touched.

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: repo-compiler
  flags: []
  count: 0
```
