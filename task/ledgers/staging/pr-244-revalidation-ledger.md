# PR-244 revalidation ledger

**Date:** 2026-08-26 · **Branch:** `docs/proc-1-tiered-review` · **Base:** `fd8bfc2`
· **Path:** STANDARD · **Scope:** process documentation, maps, ledger, and focused tree pins only.

**Retires:** this ledger moves to `../completed/` after PR #244 passes its readiness audit.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Current-main source-size gates and all touched-directory maps remain green. | Focused source-size and map gates. | **PROVEN** | Current-main gates are present; final command evidence is recorded below. |
| C-002 | Every SEPMO execution unit uses one Actor followed sequentially by one Critic stage. LIGHT uses the spine's in-line Critic and never selects external CCC. STANDARD/HIGH can select the one bound CCC engine, which can apply multiple attack lenses or passes. Finder and Verifier are not roles in that loop. Delivery stays post-convergence readiness verification. Explicit hardening lanes outside the loop remain available. | Tree pins over the manifest, runbook, and navigation prose. | **PROVEN** | `test_every_execution_unit_has_one_actor_then_one_critic_stage`; `test_adjacent_process_roles_stay_outside_the_execution_loop`. |
| C-003 | LIGHT, STANDARD, and HIGH scale effort only. Every tier keeps clause pins, full coverage attestation, the severity floor, required gates, and readiness. | One pin enumerates every invariant for every tier. | **PROVEN** | `test_review_tiers_scale_effort_without_relaxing_the_bar`. |
| C-004 | The MW-6 evidence, disk-headroom correction, and Iceberg handoff correction remain present at their truthful homes. | Existing focused pins over all three artifact groups. | **PROVEN** | `test_mw6_evidence_is_home_and_excluded_from_lint`; `test_disk_runbook_carries_the_2026_08_25_block`; `test_handoff_f7_records_the_unit_3_ruling`. |
| C-005 | Process policy is single-homed and other process documents use pointers. Duplicate or conflicting role and tier prose is removed. | Tree pin over the manifest and routing documents. | **PROVEN** | `test_process_policy_is_single_homed_and_routes_by_pointer`. |
| C-006 | This unit changes no engine or runtime behavior, and focused tree tests pin C-001 through C-007. | Diff-scope check plus one `pins:` citation for each clause. | **PROVEN** | `test_revalidation_scope_has_a_pin_for_every_clause`; final diff scope recorded below. |
| C-007 | The PR diff remains compatible with current main and keeps maps and ledgers reviewable. | Focused tests, map gates, ledger grammar, and final diff review. | **PROVEN** | Final command and diff evidence is recorded below. |

## STANDARD rubric

| Criterion | Result | Evidence |
|---|---|---|
| Small, bounded surface | Yes | Process documents, maps, one live ledger, and focused tree pins only. |
| Product behavior risk | None | Product code and runtime behavior are out of scope. |
| Cross-document drift risk | Material | The current PR repeats role and tier rules across several process documents. |
| Required review effort | STANDARD | The bound external Critic engine attacks the complete diff with all applicable lenses. |
| Bar retained | Yes | Clause pins, full coverage attestation, S1 floor, required gates, and readiness remain mandatory. |

## Actor execution record

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PR-244-actor-start
  agent: Actor
  action: revalidate and compact PR 244 process artifacts against C-001 through C-007
  charter_trace: C-001, C-002, C-003, C-004, C-005, C-006, C-007
  preconditions:
    - assigned worktree is clean at fd8bfc2: SATISFIED
    - free disk space is sufficient for focused tests and repository gates: SATISFIED (651 G)
    - product code, .github, completed PROC-1 ledger, commits, pushes, and PR metadata are excluded: SATISFIED
  success_condition: focused pins and required gates pass on a compact pointer-oriented diff
  step_risks:
    - tier prose weakens a mandatory invariant: CONTROLLED by per-tier enumeration pins
    - role prose creates extra execution-loop roles: CONTROLLED by explicit role-boundary pins
    - evidence corrections move or lose their truthful homes: CONTROLLED by preservation pins
  contingencies:
    - a source-size or map gate fails: stop and repair only the touched process artifact
    - a required broad gate cannot run: record the exact command, exit, and residual risk
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: null
```

## Pin citations

- `pins: pr-244-revalidation/C-001` — current-main source-size and map gates stay green.
- `pins: pr-244-revalidation/C-002` — the execution loop has one Actor and one Critic stage.
- `pins: pr-244-revalidation/C-003` — all three effort tiers retain the complete bar.
- `pins: pr-244-revalidation/C-004` — the three correction and evidence homes remain truthful.
- `pins: pr-244-revalidation/C-005` — process policy stays single-homed and pointer-oriented.
- `pins: pr-244-revalidation/C-006` — focused pins cover every frozen clause without product code.
- `pins: pr-244-revalidation/C-007` — maps, ledger grammar, focused tests, and diff review stay green.

## Verification

- `UV_CACHE_DIR=/tmp/pr244-uv-cache PYTHONPATH=python/repark-parity/src uv run --no-project
  --with pytest --with pyarrow --with 'pydantic>=2.10,<3' pytest
  python/repark-parity/tests/test_proc_1_tiered_review.py -q` — exit 0, 13 passed.
- `make check-lib-py check-rust-file-size check-map-sync check-docs-compaction` with the task-local
  uv cache — exit 0. The guards report 392 Python files, 273 Rust files, and 152 maps clean.
- `make py-lint py-format-check spell-check` with task-local uv cache and tool directories — exit
  0 after the pinned tools were provisioned. Ruff reports all checks passed and 393 files formatted.
- `make ci` with the same task-local uv directories — reached and passed Rust format, clippy,
  panic ban, crate DAG, crate roots, both source-size guards, Python conventions, docstring
  presence, and manifest checks. It then exited 2 at `check-ledgers`: the Git-backed guard cannot
  resolve the new untracked ledger. The shared worktree index is read-only, so this session cannot
  add an intent-to-add entry. No content finding preceded that Git-state limitation.
- `make test` with the same task-local uv directories — exit 0, full Rust workspace tests and
  doc-tests passed.
- `git diff --check` — exit 0. `git diff --name-only` contains no `crates/`, runtime Python, or
  `.github/` path. The completed PROC-1 ledger is unchanged.

The first focused and lint attempts did not start their test tools because the default uv cache and
tool directories are read-only. A task-local rerun reached the tools; the lint rerun needed network
access to provision the pinned binaries. `make verify` was not repeated because its `make ci` prefix
has the recorded Git-state stop above; its `make test` half passed separately.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PR-244-actor-conclude
  agent: Actor
  action: conclude the PR 244 revalidation build
  charter_trace: C-001, C-002, C-003, C-004, C-005, C-006, C-007
  preconditions:
    - focused clause pins pass: SATISFIED (13 passed)
    - current-main source-size and map guards pass: SATISFIED
    - full Rust workspace tests pass: SATISFIED
    - product code, .github, and the completed PROC-1 ledger remain untouched: SATISFIED
  success_condition: the process has one Actor and one bound Critic engine, tiers retain the full bar, and evidence corrections remain truthful
  step_risks:
    - duplicated process prose drifts: HANDLED by manifest-only policy and pointer pins
    - a tier omits a mandatory invariant: HANDLED by per-tier invariant enumeration
    - an evidence correction moves or changes: HANDLED by the three preservation pins
  contingencies:
    - Git-backed ledger gates require the new file in Git's tracked set: RECORDED (shared index is read-only)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: null
```

## Actor remediation — cycle 1

The C-002 proposition is corrected in place under the allowed uncommitted-proposition rule. Its
earlier wording selected the external CCC engine for LIGHT and contradicted portable canon. The
corrected proposition keeps one Actor followed by one Critic stage on every unit. LIGHT uses the
spine's in-line Critic. STANDARD/HIGH can select the one bound external engine at their configured
intensity.

The focused pins now retain both clause namespaces. `proc-1-tiered-review/C-001..C-011` still pins
the frozen completed ledger. `pr-244-revalidation/C-001..C-007` pins this remediation ledger.

- Focused suite after repair: exit 0, 16 passed.
- `make check-ledgers`: exit 0; 146 ledgers and 558 links clean.
- `make check-ledger-grammar`: exit 2 from `make`, with exactly one finding: this live ledger has no
  `COVERAGE_ATTESTATION` block. It reports no missing completed-ledger or remediation-ledger pin.
- LIGHT mutation probe: changing LIGHT to use the external engine made
  `test_review_tiers_scale_effort_without_relaxing_the_bar` fail; restoring the canonical boundary
  returned the full suite to green.
- Taxonomy mutation probe: removing AT-10 from Critic-1 made
  `test_manifest_owns_proof_isolation_and_taxonomy_mapping` fail; restoring the mapping returned the
  full suite to green.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PR-244-actor-remediation-1
  agent: Actor
  action: repair F-PR244-001, F-PR244-002, and F-PR244-003
  charter_trace: C-001, C-002, C-003, C-004, C-005, C-006, C-007
  preconditions:
    - the three findings cite current tree evidence: SATISFIED
    - portable CCC and Critic-reference constraints were checked: SATISFIED
    - product code, .github, and completed ledgers remain excluded: SATISFIED
  success_condition: both pin namespaces resolve, the two-path Critic rule matches canon, and proof plus taxonomy bindings are mutation-pinned
  step_risks:
    - LIGHT selects the external engine: HANDLED by the LIGHT mutation probe
    - a completed-ledger clause loses its executable pin: HANDLED by ledger grammar and substantive assertions
    - the taxonomy row becomes nominal: HANDLED by the AT-10 mutation probe
  contingencies:
    - the only remaining ledger-grammar item is the absent coverage attestation: VERIFIED
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: null
```

This Actor record claims no finding disposition, coverage attestation, or convergence.

### Final remediation verification

- Focused suite: exit 0, 16 passed after all remediation and ledger edits.
- Source-size, map-sync, and docs-compaction targets: exit 0.
- Ruff lint, Ruff format check, typos, and `check-ledgers`: exit 0.
- `make ci`: exit 2 only at `check-ledger-grammar` after every preceding target passed. The one
  grammar finding is the absent `COVERAGE_ATTESTATION` block.
- `make verify` was not repeated because it starts with the same `make ci` boundary. The full Rust
  `make test` half passed in the Actor build before this documentation-only remediation.

## Actor remediation — cycle 2

F-PR244-004 identified four semantic restatements outside the manifest. The staging map, parity
test map, lessons record, and SEPMO map now point to the manifest's `review_profile` and
`critic_engine` rows without paraphrasing their selection rule. The C-005 pin reads all four
carriers and rejects the known semantic paraphrases. Both pin namespaces remain present.

- Focused suite after repair: exit 0, 16 passed.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PR-244-actor-remediation-2
  agent: Actor
  action: remove four semantic restatements and strengthen the C-005 single-home pin
  charter_trace: C-005
  preconditions:
    - the manifest remains the semantic policy home: SATISFIED
    - the four cited carriers contain the conflicting paraphrases: SATISFIED
    - both completed-ledger and remediation-ledger pin families remain required: SATISFIED
  success_condition: all four carriers point to both manifest rows and contain no forbidden paraphrase
  step_risks:
    - a router keeps an equivalent role-selection restatement: HANDLED by the four-carrier token scan
    - pointer compaction drops a historical measurement: HANDLED by the lessons evidence pin
  contingencies:
    - the only permitted ledger-grammar item remains the absent coverage attestation: TO VERIFY
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: null
```

This Actor record claims no finding disposition, coverage attestation, or convergence.

Cycle-2 final evidence: focused suite exit 0 with 16 passed; `check-map-sync`, `check-ledgers`, and
`git diff --check` exit 0. `make check-ledger-grammar` exits 2 with exactly the absent
`COVERAGE_ATTESTATION` finding. The cycle-2 contingency is therefore verified.

## Critic record

Context break executed; attacking artifacts, not memory. The fresh Critic ran the CCC quality,
safety, logic, and claims lenses sequentially. Three cycles produced four S1 findings.

```yaml
FINDING:
  id: F-PR244-001
  severity: S1
  category: AT-10
  clause: C-006, C-007
  disposition: REMEDIATED
  claim: the rewritten tests removed all eleven completed PROC-1 clause pins
  evidence: both citation families now resolve and ledger grammar reports no missing pins
```

```yaml
FINDING:
  id: F-PR244-002
  severity: S1
  category: AT-1
  clause: C-002, C-003, C-005, C-007
  disposition: REMEDIATED
  claim: LIGHT selected external CCC contrary to SEPMO and CCC canon
  evidence: LIGHT now uses the in-line Critic; STANDARD and HIGH can select bound CCC
```

```yaml
FINDING:
  id: F-PR244-003
  severity: S1
  category: AT-8
  clause: C-003, C-005, C-006, C-007
  disposition: REMEDIATED
  claim: compaction removed proof, mutation, isolation, and taxonomy duties
  evidence: the manifest and focused pins restore every named duty
```

```yaml
FINDING:
  id: F-PR244-004
  severity: S1
  category: AT-1
  clause: C-002, C-005, C-007
  disposition: REMEDIATED
  claim: four routing and record passages restated a conflicting role-selection rule
  evidence: all four passages now point to the two authoritative manifest rows
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: pr-244-revalidation
  cycle: 4
  risk_tier: standard
  critic_engine: ccc
  categories:
    - id: AT-1
      status: ATTACKED
      artifacts: [tier bindings, four pointer-only carriers, F-PR244-004 red-green probe]
    - id: AT-2
      status: ATTACKED
      artifacts: [LIGHT, STANDARD, and HIGH boundary pins; 17-test focused suite]
    - id: AT-3
      status: N/A
      justification: no runtime operation, retry, transaction, or cleanup path changed
    - id: AT-4
      status: N/A
      justification: no mutable state, lock, concurrency, or ordering surface changed
    - id: AT-5
      status: ATTACKED
      artifacts: [MW-6 evidence pin and whitespace-insensitive frozen-artifact comparison]
    - id: AT-6
      status: ATTACKED
      artifacts: [SEPMO canon, CCC canon, both clause families, completed ledger semantics]
    - id: AT-7
      status: N/A
      justification: documentation and tree pins add no system-breaking performance path
    - id: AT-8
      status: ATTACKED
      artifacts: [lifecycle pin, completed-map truth pin, F-PR244-001 and F-PR244-004 probes]
    - id: AT-9
      status: ATTACKED
      artifacts: [focused suite, ledger gates, map sync, overlay-inclusive whitespace checks]
    - id: AT-10
      status: ATTACKED
      artifacts: [truthful completed map, exact-one lifecycle pin, scope diff]
  reattested: [AT-1, AT-5, AT-6, AT-8, AT-9, AT-10]
  complete: true
  convergence: CONVERGED; no open or sustained finding remains at the S1 floor
```

## Actor readiness remediation

The owner authorized two byte-only corrections in frozen evidence: remove the completed PROC-1
ledger's extra EOF blank line and remove the trailing space from `oracle_k2.log` line 21. Neither
change alters the completed ledger's semantics or the oracle text.

- F-PR244-001 RED: removing only `pins: proc-1-tiered-review/C-011` made
  `make check-ledger-grammar` exit 2 with exactly one missing-pin finding for C-011.
- F-PR244-001 GREEN: restoring the citation returned the focused suite and ledger grammar to green.
- F-PR244-004 RED: adding `Every unit uses one bound engine` to the staging map made
  `test_process_policy_is_single_homed_and_routes_by_pointer` fail on forbidden `one bound`.
- F-PR244-004 GREEN: restoring the pointer-only row returned the focused suite to green.
- The PR-244 lifecycle pin now accepts exactly one ledger in `staging/` or `completed/` and requires
  exactly the matching bin map to link it.
- The completed map now points to the filed coverage attestation instead of calling it pending.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PR-244-actor-readiness-remediation
  agent: Actor
  action: clear the four R7 readiness blockers authorized by the owner
  charter_trace: C-001, C-005, C-006, C-007
  preconditions:
    - only two frozen-artifact whitespace bytes are authorized: SATISFIED
    - completed-ledger semantics and Critic records remain immutable: SATISFIED
    - both temporary mutations were restored before verification: SATISFIED
  success_condition: lifecycle turnover stays pinned, the completed map is truthful, the complete PR diff is whitespace-clean, and both regression probes redden
  step_risks:
    - the lifecycle pin assumes staging forever: HANDLED by exact-one-bin and matching-map assertions
    - completed review state is reported as pending: HANDLED by the completed-map pin
    - a frozen artifact changes semantically: HANDLED by byte-scoped diff review
  contingencies:
    - the uncommitted overlay cannot change main...HEAD output: RECORD both the requested range and the overlay-inclusive check
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: null
```

This Actor section does not modify or reinterpret any Critic finding, disposition, or attestation.

Final readiness evidence: focused suite exit 0 with 17 passed; `check-ledgers`,
`check-ledger-grammar`, and `check-map-sync` exit 0; `make ci` exit 0. `git diff --check` and the
overlay-inclusive `git diff --check main` exit 0. The requested committed-only
`git diff --check main...HEAD` exits 2 on the two historical whitespace defects because this
no-commit remediation cannot change that range; the working overlay contains both corrections.

## Pointers

- Up: [map.md](map.md)
- Original PROC-1 record: [../completed/proc-1-tiered-review-ledger.md](../completed/proc-1-tiered-review-ledger.md)
