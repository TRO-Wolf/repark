## Errata — 2026-09-28: PR #874 external review remediation

ACTOR_REMEDIATE, STANDARD path. Owner requests C874-1 through C874-4 repaired and
C874-5 documented for an owner ruling. No merge or policy amendment is authorized.
The original completed record below is preserved. This erratum closes when the repair
is reviewed and the updated PR is handed back.

SLR-C874-1: PROCEED. C-001/C-002 cover tracked path handling and map link rejection;
C-007 covers hook and dependency gate wiring. Preserve selected-index semantics, retain
legacy rejection coverage alongside GFM parsing, and never use a reported Rust defect as
a fallback trigger. Tests must reproduce the reported gaps before remediation. Supply-chain
scope explicitly includes the standalone manifest, both audit workflows and Dependabot.
Policy scope is an inventory of conflicting text only; no policy choice is inferred.

C874-1 through C874-4 are REMEDIATED with the evidence below. C874-5 remains an
owner decision flag. No policy resolution or merge readiness is claimed.
Disk before testing: 905 GB free. Existing compiler and engine caches remain shared in this clone.

### C874-5 policy decision inventory (2026-09-28)

The owner requested flags only. No contract, contributor guide or skill policy is changed.
The decision is whether an unchanged managed inventory excuses a same-change map edit.
If approved, reconcile all these live statements together; if declined, remove the exception
in the map lockstep guard and its tests instead. PR readiness does not settle this decision.

| Home | Conflicting text or dependent behavior |
|---|---|
| AGENTS.md, hard rules, lines 127–130 | The new managed-map exception says stable inventories need no edits. |
| AGENTS.md, document lifecycle, line 252 | Navigation maps require lockstep in the same commit. |
| PROJECT.md, line 85 | Maps require lockstep with code changes. |
| CONTRIBUTING.md, lines 32–34 | Directory maps must change in the same pull request as code. |
| DEVELOPMENT.md, lines 47 and 110 | Branch guard and troubleshooting require the map in the same branch diff. |
| compact-context-docs SKILL.md, lines 33–34 and 64–65 | Touched directories must update their maps in the same commit. |
| SEPMO binding-manifest.md, line 34 | Navigation binds mandatory lockstep; line 39 also names the old Python drift-gate wiring. |
| scripts/check_map_md.sh and compiler hook tests | Managed-inventory bypass implements the new exception and must follow the ruling. |

Related stale workflow documentation is a wiring fact, not a policy ruling: the ci.yml row
in `.github/workflows/map.md` now names the Rust combined docs/ledger target.

### Remediation evidence and independent review

| Finding | Disposition | Regression evidence |
|---|---|---|
| C874-1 (S1), C-002 | REMEDIATED | `maps_compat.rs`: Python/Rust real-tree baseline, copied real corpus with the reported table target changed, each legacy rejection class, positive controls and both Git snapshots. GFM tables are enabled; the legacy scan is retained, including Python line separators. |
| C874-2 (S3), C-001 | REMEDIATED | `tracked_backslash_does_not_break_any_checker` compares all three checkers. POSIX backslashes are filename bytes; traversal validation remains. |
| C874-3 (S3), C-002/C-007 | REMEDIATED | `test_repo_tool_fallback.py`: no Cargo, failed build, Rust finding without fallback, staged/unstaged and alternate/temporary indices, actual installed hook, deleted Cargo manifest, unsupported syntax and path boundaries. |
| C874-4 (S3), C-007 | REMEDIATED | Both lockfiles pass the Make audit/deny targets. CI mirrors them; Dependabot names the standalone directory; the crate declares Apache-2.0. Configuration inspection and the actual audit/deny executions prove this wiring change. |
| C874-5 (S3) | ACCEPTED_FLAGGED for review, owner ruling required before merge | The policy inventory above names each conflicting home. The owner requested flags, not a policy amendment. |

The initial map differential run failed all three new tests on the original implementation
(`/tmp/c874-maps-red.log`); a separate line-separator probe failed before its correction
(`/tmp/c874-lines-red.log`). The final map suite includes four differential tests alongside
21 existing map tests. `make repo-tool-check` passes all 85 Rust tests and 18 fallback tests.
`/tmp/c874-final-scoped.log` also records green Python lint/format and docs/map/lifecycle gates.
The actual staged corpus passes all 338 maps through the Python fallback
(`/tmp/c874-fallback-real-index.log`). The complete Python harness passes 700 tests with
5 skipped (`/tmp/c874-py-test.log`). Both Cargo workspaces pass audit/deny, and all 13 workflows
parse with no unsuppressed security findings (`/tmp/c874-supply-chain.log`).

Fresh independent Sol critics re-attested the affected paths. The map reviewer executed
novel public CLI cases for HTML, tables, wrapped duplicate rows, staged isolation, valid
parent traversal and all Python line separators. The fallback reviewer attacked AT-1 through
AT-10 and found four additional S1 false-pass classes during remediation: map symlinks, fenced
managed markers, reference definitions, and escaping links including parser-only syntax.
Each was reproduced, fixed and pinned; the reviewer rechecked the fixes and converged with
no open S1. Valid within-repository parent links still pass. The fallback deliberately refuses
reference definitions and ambiguous link syntax that needs Rust; it never retries a Rust
checker finding through Python. This is a conservative hook fallback, not a replacement for
mandatory Rust CI checks. Existing F-checks-5 remains the previously recorded S2 flag.

The 2026-09-27 full preflight remains evidence for unchanged engine/facade/dbt behavior.
The current `make -k verify` exited 0: 5,958 Rust/compiler tests passed, 8 ignored
(`/tmp/c874-verify.log`). The final scoped run rechecked the compiler, fallback and Python/static
gates after the last fallback edit. Independent readiness audit `RA-C874-repair` passed the
local scope, coverage, findings and trace checks: `READY_FOR_DRAFT_UPDATE`. Required remote CI
on the new head and the C874-5 owner ruling remain merge gates. No merge is authorized.

Final disk check: 892 GB free. Critic and test fixtures were cleaned by their owners. This
isolated checkout, its existing build caches and validation logs remain available for PR review.


## Errata — 2026-09-27: PR Python integration remediation

PR #874's first Python CI run found two map-guard integration regressions that the local
preflight roster did not exercise. The complete Python harness was missing from preflight;
its source-cap subset was insufficient. This corrects the earlier implication that a local
preflight pass alone covered that CI job. The original result remains an accurate run record.

SLR-PR-CI-1: PROCEED. Trace C-002/C-007 and check-port C-004. Scope is the map hook,
its existing regression fixture, preflight membership and their maps. Preserve legacy
warning/error behavior when no executable compiler wrapper exists; the separate compiler
and map gates remain mandatory in this repository. Retain the safer workflow environment
variable and update the fixture to verify it. Add the full existing `py-test` target to
preflight while retaining its named source-cap target. Explicit `uv run --isolated` prevents
the local native virtual environment from changing that harness run. No engine or Rust compiler source changes.

```yaml
FINDING:
  id: F-compiler-9
  severity: S1
  category: AT-9
  clause: C-002
  disposition: REMEDIATED
  summary: Standalone legacy hook invocation emitted command-not-found noise when the optional compiler wrapper was absent; the executable check now selects the existing legacy path.
  regression: python/repark-parity/tests/test_map_pr_gate_1.py::test_staged_mode_warns_and_exits_zero
```

```yaml
FINDING:
  id: F-compiler-10
  severity: S1
  category: AT-8
  clause: C-007
  disposition: REMEDIATED
  summary: The workflow assertion retained the old inline expression, and preflight omitted the CI harness that caught it. The assertion now pins the quoted environment variable and preflight includes the full harness in an explicitly isolated environment.
  regression: python/repark-parity/tests/test_map_pr_gate_1.py::test_ci_guard_is_pr_only_and_gitattributes_carries_union; test_preflight_runs_full_parity_harness
```

Both original CI failures reproduce locally in `/tmp/repo-compiler-map-ci-red.log`;
all 16 map-gate tests pass after remediation in `/tmp/repo-compiler-map-ci-green.log`.
The first full local harness attempt was interrupted after 1 pass in 271.84 seconds because
`--no-project` selected the native virtual environment; this is not a passing result. The
explicitly isolated rerun and scoped verification are recorded below. Independent Sol review
found no new AT-8/AT-9 defect in the remediation. Remote CI must rerun on the new head before
final readiness.

Scoped verification exited 0: `/tmp/repo-compiler-ci-remediation-isolated.log`. The isolated
Python harness passed 700 tests with 5 skipped; compiler tests passed all 81. Rust compiler
lint/format, Python lint/format, docs/maps/lifecycle and workflow gates passed. The original
full engine/facade/dbt preflight evidence remains unchanged; those suites were not rerun for
this hook, fixture and Make wiring correction. Disk check: 1.1 TB available.

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
