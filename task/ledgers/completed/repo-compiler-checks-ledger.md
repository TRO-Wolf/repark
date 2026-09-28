# Repository compiler check ports

Date: 2026-09-27. This ledger closes when the docs-link and ledger-grammar ports are accepted
and committed. The owner approved this slice with “Proceed”. Existing isolated branch:
`codex/repo-compiler`; its earlier implementation remains staged and uncommitted.

## Scope audit

| Clause | Proposition | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Docs-link validation preserves the existing Python gate's findings and pass/fail behavior. | Differential fixtures for links, headings, fences, paths and allowlists. | PROVEN | Reference is scripts/check_docs_links.py; scope is worktree validation. |
| C-002 | Ledger grammar preserves row, citation, exemption, attestation and finding rules. | Differential fixtures against scripts/check_ledger_grammar.py. | PROVEN | Reference grammar is fully available; baseline exceptions move to one shared JSON file. |
| C-003 | Both checks can share one tracked-file inventory and text cache without accepting stale evidence. | Combined CLI agrees with separate checks; snapshot/cache options refused. | PROVEN | Checks read the worktree and return findings; they do not produce runtime attestations. |
| C-004 | Make and CI invoke the Rust gates only after parity and performance validation. | Gate wiring inspection, benchmarks, standalone tests and make verify. | PROVEN | Existing named Make targets and workflow steps are the integration points. |

Scope verdict: PASS; zero OPEN or REJECTED scope clauses. This is feasibility evidence,
not a claim that implementation validation has already passed. STANDARD path applies because
this changes multiple modules and CLI commands. Prior implementation has not landed; pickup
compaction has no merged delta and this slice does not archive another unit's ledgers.

## Plan

- [x] Add shared worktree inputs and a checks command; reject cached or staged invocations.
- [x] Port docs links and ledger grammar, keeping Python references and one exception table.
- [x] Differential-test valid and invalid cases, then compare real-tree outputs and timings.
- [x] Switch Make/CI, update maps and contracts, run adversarial review and verification.

Files: scripts/repo-tool/src/{validation,docs_links,ledger_grammar,ledger_records,lib,main}.rs;
scripts/repo-tool/tests/{docs_links,ledger_grammar,validation}.rs; compiler README, INTERFACE,
CHECKS.md and directory maps; scripts/check_ledger_grammar.py and shared exception JSON;
scripts/map.md; Makefile; .github/workflows/ci.yml and map.md; AGENTS.md and root map.md;
this ledger and staging/map.md. No new library dependency, commit, push or live dispatch.

## Pre-execution review

SLR-CHECKS-1: PROCEED. Trace C-001..C-004. References are readable, Rust dependencies exist,
and disk has 1.1 TB free. Risks: grammar drift handled by differential negative fixtures;
filesystem existence outside the tracked inventory handled by worktree-only inputs and no
persistent cache; weaker gates handled by preserving legacy rules and a separate critic pass.
No open scope uncertainty. Changes remain reversible working-tree edits in the isolated branch.

## Scope adjustment and remediation — 2026-09-27

SLR-CHECKS-2: PROCEED. Trace C-002/C-004. The existing Python ratchet test copies and edits
embedded constants. Moving the baseline to JSON requires changing that fixture to copy and
mutate JSON instead. Add python/repark-parity/tests/test_dl_2_ledger_grammar.py and its map to
the plan; retain the same raised-ceiling and stale-row assertions. No policy change.

Fresh critic inputs found two compatibility defects: Python's U+001F whitespace semantics
and Rust's broader Other_Alphabetic classification. A new differential docs fixture failed
before remediation. Shared compatibility whitespace helpers and letter/number categories
replace host-language defaults. Regression fixtures cover headings, docs cells, reading
headers and multiline citations. Review will recheck both findings on the final code.

SLR-CHECKS-3: PROCEED. Trace C-001/C-004. A final path-domain probe found that the first
resolver rejected a valid 48-link chain accepted by Python. The red differential fixture is
recorded in `/tmp/repo-checks-chain-red.log`. Iterative component resolution replaces the
fixed recursion cap; active-link tracking rejects cycles while allowing repeated resolved
links. New tests cover both boundaries. The same critic rechecks the affected path lenses.

## Independent review — 2026-09-27

Sol reviewed the final implementation against both Python references and re-attacked the
remediations with fresh CLI fixtures. No blocking findings remain. The root also found and
pinned the long-chain defect; Sol independently confirmed the correction with 64 links.

```yaml
FINDING:
  id: F-checks-1
  severity: S1
  category: AT-6
  clause: C-001, C-002
  disposition: REMEDIATED
  summary: Python control whitespace differed from Rust defaults; shared compatibility helpers and differential fixtures now match headings, reading exemptions and citations.
```

```yaml
FINDING:
  id: F-checks-2
  severity: S1
  category: AT-2
  clause: C-001
  disposition: REMEDIATED
  summary: Other_Alphabetic marks changed heading slugs and docs-cell boundaries; Unicode letter and number classes now match the reference fixtures.
```

```yaml
FINDING:
  id: F-checks-3
  severity: S2
  category: AT-9
  clause: C-002
  disposition: REMEDIATED
  summary: Nonprintable invalid statuses now use Python-compatible escaping; fresh U+200D and U+001F diagnostics match.
```

```yaml
FINDING:
  id: F-checks-4
  severity: S1
  category: AT-7
  clause: C-001
  disposition: REMEDIATED
  summary: The fixed symlink depth rejected a valid 48-link chain; iterative resolution accepts long valid chains and detects active cycles.
```

```yaml
FINDING:
  id: F-checks-5
  severity: S2
  category: AT-9
  clause: C-001
  disposition: ACCEPTED_FLAGGED
  summary: A symlink cycle raises an uncaught Python RuntimeError with exit 1 but Rust returns an environment error with exit 2. Both refuse the cycle. The documented CLI error contract normalizes legacy exceptions; no link guard is weakened.
```

```yaml
COVERAGE_ATTESTATION:
  - id: AT-1
    status: ATTACKED
    artifacts: [scripts/check_docs_links.py, scripts/check_ledger_grammar.py, Makefile]
    evidence: Scope walked against reference semantics and gate wiring.
  - id: AT-2
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/docs_links.rs, scripts/repo-tool/tests/ledger_grammar.rs]
    evidence: Control whitespace, combining marks, malformed statuses, long chains and repeated aliases.
  - id: AT-3
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/docs_links.rs, scripts/repo-tool/tests/validation.rs]
    evidence: Missing tracked inputs, malformed allowlist and cycle refusal.
  - id: AT-4
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/validation.rs]
    evidence: Worktree mutation invalidates success; cache and snapshot inputs refused.
  - id: AT-5
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/docs_links.rs]
    evidence: Escaping and dangling symlinks, parent symlinks and final repository bounds.
  - id: AT-6
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/docs_links.rs, scripts/repo-tool/tests/ledger_grammar.rs]
    evidence: Fresh Python differential inputs for Unicode, rows, exceptions and resolved paths.
  - id: AT-7
    status: ATTACKED
    artifacts: [scripts/repo-tool/src/docs_links.rs, scripts/repo-tool/tests/docs_links.rs]
    evidence: Iterative resolver reviewed; fresh 64-link chain succeeds and two-link cycle terminates. Citation scan probed on about 14 MB.
  - id: AT-8
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/validation.rs, Makefile, .github/workflows/ci.yml]
    evidence: Combined invocation runs once; individual Make targets remain available.
  - id: AT-9
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/ledger_grammar.rs, scripts/repo-tool/CHECKS.md]
    evidence: Invalid-status escaping matches; environment exit normalization is recorded.
  - id: AT-10
    status: ATTACKED
    artifacts: [scripts/repo-tool/tests/docs_links.rs, scripts/repo-tool/tests/validation.rs]
    evidence: Red-before-fix character and long-chain fixtures; final affected review suite passes 12 tests.
complete: true
```

## Validation and timing — 2026-09-27

The standalone compiler gate passes 81 tests, formatting, all-target Clippy and the
production panic/method ban. The retained Python regression suite passes 36 tests. Python
lint/format and workflow parsing/security lint pass. Both references and both Rust checks
pass on the current tree with zero findings: 1,190 Markdown files, 6,638 links and 283 live
ledgers. Full repository verification follows this record.

Six rotated rounds, first round excluded, release binary, five retained samples per command:

| Command | Median seconds |
|---|---:|
| Python docs links | 1.3569 |
| Rust docs links | 0.3127 |
| Python ledger grammar | 0.3482 |
| Rust ledger grammar | 0.1611 |
| Rust combined checks | 0.4419 |
| Rust combined build wrapper | 0.5429 |

The combined binary is about 3.9 times faster than the sum of the two Python medians;
including Cargo freshness checking, about 3.1 times faster. Samples are local wall time,
not measured account token savings. Raw samples: `/tmp/repo-checks-benchmark-final.json`.
No persistent check cache or skipped validation contributes to these timings.


## Final verification and handoff — 2026-09-27

`make -k verify` exited 0 on the final source: 5,954 tests passed, 8 ignored and none failed
(5,873 workspace tests plus 81 compiler tests). All lint, format and static gates passed.
The separate retained-reference regression suite passed 36 tests. Independent review has no
open blocking finding; F-checks-5 remains the documented error-normalization exception.
Logs remain at `/tmp/repo-checks-verify.log`, `/tmp/repo-checks-reference-tests.log` and
`/tmp/repo-checks-workflows.log` for local review.

Free space was checked before builds, tests and broad validation, and during verification;
approximately 1.1 TB remains. Reviewers removed their disposable fixture directories.
The isolated checkout, its 42 GB engine target and 2.6 GB compiler target remain for review
and incremental validation. This slice created no additional worktree or dependency cache.
Timing samples and validation logs are retained under `/tmp/`.

Implementation is verified and staged on `codex/repo-compiler`. The original working tree was
not modified. No commit, push or live harness dispatch occurred. This ledger stays in staging
until owner acceptance and an authorized commit; verification does not imply delivery or merge.

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
  pr_unit: repo-compiler-checks
  flags:
    - finding_id: F-checks-5
      severity: S2
      rationale: Both implementations refuse symlink cycles; Rust uses the documented environment-error exit instead of a legacy traceback.
      user_surfaced: CONFIRMED (disclosed in the implementation handoff before the owner requested PR creation)
  count: 1
```
