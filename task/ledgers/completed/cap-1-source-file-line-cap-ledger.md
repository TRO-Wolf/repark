# Charter ledger — CAP-1 · 1,000-line source ceiling

**Date:** 2026-08-26 · **Branch:** `governance/context-economy` · **Base:** `ea92bd9`
(`main` `32c6102` plus the required pickup archive) · **SEPMO path:** STANDARD · **Size:** L

**Retires:** this ledger moves to `../completed/` in CAP-1's final commit.

**Owner approval:** the owner approved the twelve-clause CAP-1 proposition ledger and authorized
commits, push, and PR creation on 2026-08-26 with: “push that thang and open the PR”.

**Scope boundary:** CAP-1 implements only the source-file ceiling. It does not retire `map.md`, build
the JSON ledger CLI, change comment policy, amend SEPMO roles, or split an oversized source file.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The default ceiling is 1,000 lines for `crates/**/*.rs`; blank lines count | Boundary provocation and tree scan | **PROVEN** | Owner-approved CE-3 rule; `test_cap_1_rust_boundary_counts_blank_lines` |
| C-002 | The default ceiling is 1,000 lines for `python/**/*.py` and `scripts/**/*.py`; blank lines count | Boundary provocation and root scan | **PROVEN** | Owner-approved CE-3 rule; `test_cap_1_python_boundary_counts_blank_lines` |
| C-003 | The migration domain at base `32c6102` is the deterministic set of source paths under C-001/C-002 whose `splitlines()` count exceeds 1,000: 80 paths, partitioned as 45 Rust and 35 Python | Exact set-equality pin against the exception tables | **PROVEN** | Measured 2026-08-26 over 663 files; `test_cap_1_exception_tables_equal_the_measured_debt` |
| C-004 | Every migration exception uses the file's exact measured count as its baseline and grants no slack | Exact baseline pin and growth provocation | **PROVEN** | Owner-approved CE-3 rule; `test_cap_1_exception_tables_equal_the_measured_debt`, `test_cap_1_growth_above_exact_baseline_fails` |
| C-005 | An exception that shrinks remains red until its baseline ratchets down, and an exception at or below 1,000 remains red until its row is removed | Shrink and retirement provocations | **PROVEN** | Owner-approved exact-ratchet rule; `test_cap_1_shrink_requires_baseline_update`, `test_cap_1_default_compliant_exception_requires_removal` |
| C-006 | Missing exception paths, unreadable source files, and empty scan sets fail closed | Failure-path provocations | **PROVEN** | Existing guard contract extended without weakening; `test_cap_1_fail_closed_conditions` |
| C-007 | Each exception records why the debt exists and a cohesive split seam; the universal retirement condition is reaching 1,000 lines; no unscheduled task receives a fabricated ID | Table-shape and non-empty-field pin | **PROVEN** | Owner-approved CE-3 rule; `test_cap_1_exception_records_are_actionable` |
| C-008 | A line-neutral fix to an oversized file is legal; baseline growth is red unless the owner explicitly approves a reviewed baseline amendment | Exact-baseline behavior pin and error-text pin | **PROVEN** | Narrow-fix safeguard; `test_cap_1_exact_baseline_is_green_and_growth_is_red` |
| C-009 | The existing Rust gate is lowered and the existing Python gate is generalized; no pytest test becomes a parallel enforcement gate and no new gate target is introduced | Makefile/workflow wiring pin | **PROVEN** | Existing `check-rust-file-size` and `check-lib-py` surfaces; `test_cap_1_existing_gate_surfaces_remain_dual_wired` |
| C-010 | The facade re-export-only module rule remains scoped to `python/repark/src/repark` and keeps its behavior while the line scan widens | Positive and negative no-stub pins | **PROVEN** | Existing `check_lib_py.py` contract; `test_cap_1_facade_no_stub_scope_is_unchanged` |
| C-011 | Checked-in source below `tests/goldens/` or `tests/fixtures/` is exempt; other `.rs` and `.py` files under the declared roots count | Path-partition provocation | **PROVEN** | Owner-approved generated-fixture/golden exemption; `test_cap_1_only_named_fixture_paths_are_exempt` |
| C-012 | The authoritative contract, developer command guide, CI navigation, script navigation, test navigation, and ledger navigation describe the changed gate without restating its numeric SSOT outside the scripts | Tree pins and map gates | **PROVEN** | `test_cap_1_prose_and_navigation_name_the_generalized_gate`; `make check-map-sync` |

VERDICT: PASS iff OPEN=0 and REJECTED=0. LOGIC_SCORE = 12/12.

## Exact audit-time domain

The exception domain is finite and reproducible at `32c6102`: every regular file returned from
`crates/`, `python/`, and `scripts/` with suffix `.rs` or `.py`, excluding only path components
`tests/goldens` and `tests/fixtures`, partitioned by language and tested for `splitlines() > 1000`.
The exception-table set-equality pin is the addressable enumeration: a missing, extra, or renamed path
fails by path, and every row's integer must equal the measured count.

## Planned artifacts

- `scripts/check_rust_file_size.py` — 1,000-line Rust default and exact ratchet.
- `scripts/check_lib_py.py` — all-Python source scan plus the unchanged facade-only no-stub rule.
- `python/repark-parity/tests/test_cap_1_source_file_line_cap.py` — clause pins and provocations.
- `AGENTS.md`, `DEVELOPMENT.md`, `Makefile`, `.github/workflows/ci.yml` — truthful carrier wording;
  existing command names and job names stay stable.
- The matching `map.md` files and this ledger.

## Proportionality rubric

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-cap-1
  pr_unit: cap-1-source-file-line-cap
  criteria:
    blast_radius: FAIL (the default applies to 663 files and both source languages)
    reversibility: PASS (one gate-only commit can revert the policy without product-data effects)
    size: FAIL (80 exact exception rows plus tests and contract/navigation updates exceed 150 lines and five files)
    novelty: PASS (extends the two existing ratchet guards)
    sensitivity: PASS (no engine, catalog, write path, credentials, or runtime behavior)
    clarity: PASS (twelve approved clauses; zero OPEN or REJECTED)
  path: STANDARD
  recorded_by: Orchestrator
```

## Pre-execution review

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PER-cap-1
  agent: Orchestrator
  action: execute the approved CAP-1 source-file-ceiling charter as one PR unit
  charter_trace: C-001..C-012
  preconditions:
    - charter frozen: SATISFIED (owner approved the displayed twelve-clause ledger on 2026-08-26)
    - clause carving complete: SATISFIED (C-001..C-012 map only to CAP-1; CAP-1 maps back to C-001..C-012)
    - proportionality recorded: SATISFIED (RUBRIC-cap-1 selects STANDARD)
    - bindings resolved: SATISFIED (single-session Actor; procedural context break; CCC; make verify and make preflight)
    - contingencies executable: SATISFIED (all recovery is additive source/table correction or a revert commit; no destructive reset)
  success_condition: focused clause pins, make verify, CCC convergence, and make preflight all pass on the final diff
  step_risks:
    - eighty baselines drift while being transcribed: HANDLED(exact set-and-count test derives the live scan independently)
    - the widened Python scan applies facade syntax rules outside the facade: HANDLED(no-stub invocation remains guarded by the facade root)
    - an exception silently permits regrowth after shrinkage: HANDLED(the gate rejects line_count below baseline until the row ratchets)
  contingencies:
    - a baseline pin fails: EXECUTABLE(remeasure and correct the table before commit)
    - an existing source file changes during the unit: EXECUTABLE(rebase or update the exact baseline with the concurrent change's disposition)
    - a gate regression appears: EXECUTABLE(revert or amend the CAP-1 implementation before push)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: "—"
```

```yaml
PRE_EXECUTION_REVIEW:
  id: PER-cap-1
  slr: SLR-PER-cap-1
  plan_checklist:
    charter_frozen: SATISFIED (owner approval, 2026-08-26)
    carving_clause_complete:
      forward: SATISFIED (C-001..C-012 -> CAP-1)
      backward: SATISFIED (CAP-1 -> C-001..C-012)
    rubric_recorded: SATISFIED (1/1 unit carries RUBRIC-cap-1)
    bindings_resolved: SATISFIED (binding-manifest v2.3 resolves roles, context break, CCC, and gates)
    contingencies_executable: SATISFIED (only additive amendments or revert commits are named)
  verdict: PROCEED
  gap_route: "—"
  gap_detail: "—"
```

## Actor build record

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-cap-1-actor-build
  agent: Actor
  action: implement the exact-baseline 1,000-line policy in the two existing source-size guards
  charter_trace: C-001..C-011
  preconditions:
    - approved ceiling and roots: SATISFIED (C-001, C-002)
    - migration domain measured: SATISFIED (663 files; 45 Rust and 35 Python offenders at 32c6102)
    - existing carrier identified: SATISFIED (check_rust_file_size.py and check_lib_py.py are dual-wired)
    - facade-only syntax rule identified: SATISFIED (_is_reexport_only is invoked only for the facade root)
  success_condition: both guards pass the live tree and focused provocations fail every forbidden boundary
  step_risks:
    - a hand-entered path or count is wrong: HANDLED(the independent exact-set pin names the mismatch)
    - widened scanning executes AST checks on benchmark or test modules: HANDLED(AST/no-stub checking remains facade-only)
    - a shrunken exception retains regrowth room: HANDLED(line_count below baseline is a gate error)
  contingencies:
    - a table mismatch appears: EXECUTABLE(correct the exact row from the measured path before commit)
    - a guard rejects an intended fixture: EXECUTABLE(classify it against the two approved fixture paths and add only a matching test)
    - an unrelated source changes concurrently: EXECUTABLE(rebase and re-run the exact census before push)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: "—"
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-cap-1-actor-pins
  agent: Actor
  action: add CAP-1 clause pins and update the contract and navigation carriers
  charter_trace: C-001..C-012
  preconditions:
    - both live guards pass: SATISFIED (273 Rust files; 390 Python files; 45 and 35 exact exceptions)
    - test home identified: SATISFIED (parity-harness tests already pin repository guard scripts)
    - prose homes identified: SATISFIED (AGENTS, DEVELOPMENT, Makefile, CI workflow, and maps)
  success_condition: every C-001..C-012 citation resolves and the focused test module passes
  step_risks:
    - tests duplicate the production gate instead of attacking it: HANDLED(boundary tests call the real check_file; independent census is only a configuration identity pin)
    - prose creates a second numeric SSOT: HANDLED(the number stays in gate scripts and the charter ledger only)
    - workflow behavior changes accidentally: HANDLED(only its descriptive comment and stable direct command are touched)
  contingencies:
    - a provocation passes unexpectedly: EXECUTABLE(fix the production guard before accepting the pin)
    - a prose pin conflicts with the contract: EXECUTABLE(change the lower-authority carrier to point at AGENTS and the scripts)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: "—"
```

## CCC cycle 1 — filed findings

```yaml
CONTEXT_BREAK:
  id: CB-cap-1-cycle-1
  mechanism: PROCEDURAL_IN_SESSION
  manifest_binding: context_break_mechanics procedural default; CCC review-only on scratch clone
  handed_to_critic: [C-001..C-012, staged diff and artifacts, focused and broad test results, CCC attack taxonomy]
  withheld_until_initial_findings_filed: [actor build summary, actor self-logic reviews]
  declaration_logged: "Context break executed; attacking artifacts, not memory."
  honesty_note: procedural, not amnesia; cycle-1 tree copied to /tmp/repark-cap1-critic.QTDh1u/tree
```

Critic-1 attacked correctness, maintainability, and mutation resistance. Critic-2 attacked security
and safety and returned CLEAN. Critic-3 attacked boundary logic and fail-closed behavior. Critic-4
re-measured every migration and wiring claim. The four findings at the S1 floor are filed before
Actor remediation:

```yaml
FINDING:
  id: Q-CAP1-001
  severity: S1
  category: AT-2
  clause: C-005, C-007
  disposition: REMEDIATED (default-compliant and malformed-row provocations cover both guards)
  claim: an exception at the default baseline, or with an empty reason or split seam, can pass the production guards
  evidence: direct check_file provocation passed with baseline 1000 and blank metadata

FINDING:
  id: L-CAP1-001
  severity: S1
  category: AT-3
  clause: C-006
  disposition: REMEDIATED (existing outside-scan path provocation covers the Rust main entry point)
  claim: the Rust stale-row check accepts an existing source path outside crates/**/*.rs
  evidence: main returned zero with outside.rs as the sole exception row and crates/small.rs as the scan set

FINDING:
  id: CL-CAP1-001
  severity: S1
  category: AT-6
  clause: C-003, C-004
  disposition: REMEDIATED (literal base path/count tuples must equal both gates and the measured tree)
  claim: the census pin derives its expected debt from the current tree and does not freeze the approved base path set
  evidence: a one-for-one legacy-path swap can preserve the 45/35 counts and satisfy gate-to-tree equality

FINDING:
  id: Q-CAP1-002
  severity: S1
  category: AT-10
  clause: C-011
  disposition: REMEDIATED (main-entry fixture and near-miss provocations cover both guards)
  claim: the exemption test checks the helper but does not prove main excludes approved paths and scans near misses
  evidence: removing the _is_exempt call from scan construction leaves the helper-only test green on the live tree
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-cap-1-actor-remediate-1
  agent: Actor
  action: close the four cycle-1 findings with production validation and mutation-resistant pins
  charter_trace: C-003, C-004, C-005, C-006, C-007, C-011
  preconditions:
    - findings are reproducible: SATISFIED (each failing or false-green provocation was run on the scratch clone)
    - remediation stays inside approved artifacts: SATISFIED (the two gates, CAP-1 pins, and this ledger)
    - no policy expansion is required: SATISFIED (all fixes enforce already-approved clauses)
  success_condition: malformed rows and outside-scan paths fail; the base debt set is literal; exemption wiring is provoked through main
  step_risks:
    - row validation emits duplicate errors: HANDLED(validate a scanned row once in check_file)
    - the literal census becomes an accidental second runtime gate: HANDLED(keep it in the clause-pin test as an audit-time oracle)
    - integration fixture tests accidentally enter facade AST scope: HANDLED(place synthetic Python files outside FACADE_ROOT)
  contingencies:
    - live census differs from the frozen base: EXECUTABLE(stop and disposition the changed source path explicitly)
    - a fixture near-miss passes: EXECUTABLE(correct scan construction before accepting the pin)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: "—"
```

## CCC cycle 2 — claims finding

```yaml
FINDING:
  id: CL-CAP1-002
  severity: S1
  category: AT-8
  clause: C-012
  disposition: REMEDIATED (standalone-number pin covers every changed non-ledger carrier)
  claim: the staging navigation restates the numeric source ceiling outside its declared SSOT
  evidence: task/ledgers/staging/map.md says 1,000-line default while C-012 permits the number only in gate scripts and this charter ledger
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-cap-1-actor-remediate-2
  agent: Actor
  action: remove the duplicate numeric ceiling and pin every changed non-ledger carrier against recurrence
  charter_trace: C-012
  preconditions:
    - duplicate located: SATISFIED (one changed navigation carrier contains the standalone number)
    - canonical homes remain available: SATISFIED (both scripts and this charter ledger retain the number)
  success_condition: changed contract, guide, workflow, Makefile, test-map, root-map, and staging-map carriers contain no standalone 1000 value
  step_risks:
    - an unrelated larger number creates a false positive: HANDLED(use standalone-number boundaries)
    - removing the number makes navigation unclear: HANDLED(name the source-line default and exact no-slack baselines)
  contingencies:
    - another changed carrier contains the number: EXECUTABLE(replace it with a pointer to the gate script)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: "—"
```

```yaml
FINDING:
  id: CL-CAP1-003
  severity: S1
  category: AT-6
  clause: C-012
  disposition: REMEDIATED (live carriers point to the gates; frozen FNP-0 receives an append-only note)
  claim: live maps, one helper docstring, and the active Spark-function design still describe the retired source-size defaults as current
  evidence: repository sweep found present-tense 1500/2500 ceiling claims in the session, dataframe, call, and campaign carriers
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-cap-1-actor-remediate-3
  agent: Actor
  action: true current source-size references while preserving dated historical measurements
  charter_trace: C-012
  preconditions:
    - occurrences classified: SATISFIED (present-tense instructions separated from dated history)
    - affected maps identified: SATISFIED (session, dataframe, call, docs/design, and staging)
  success_condition: current guidance points to exact gate baselines; dated historical records remain explicit history
  step_risks:
    - historical evidence is silently rewritten: HANDLED(keep dated old measurements or label them then-current)
    - another campaign's frozen clause is edited in place: HANDLED(append a dated compatibility note instead)
  contingencies:
    - a current stale phrase survives: EXECUTABLE(reclassify it and update its owning carrier before close)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: "—"
```

## CCC final cycle — coverage and convergence

```yaml
CONTEXT_BREAK:
  id: CB-cap-1-final-quality
  mechanism: PROCEDURAL_IN_SESSION
  critic_phase: Critic-1 quality and coverage skeptic
  handed_to_critic: [C-001..C-012, final staged diff, two source guards, CAP-1 test module]
  withheld: [actor rationale and self-logic reviews]
  declaration_logged: "Context break executed; attacking artifacts, not memory."
  honesty_note: procedural, not amnesia; attacked a fresh scratch clone
```

```yaml
CONTEXT_BREAK:
  id: CB-cap-1-final-security
  mechanism: PROCEDURAL_IN_SESSION
  critic_phase: Critic-2 security and safety
  handed_to_critic: [final staged diff, workflow delta, source-path handling, safety boundaries]
  withheld: [Critic-1 conclusions]
  declaration_logged: "Context break executed; attacking artifacts, not memory."
  honesty_note: procedural, not amnesia; no runtime, credential, AWS, or write surface found
```

```yaml
CONTEXT_BREAK:
  id: CB-cap-1-final-logic
  mechanism: PROCEDURAL_IN_SESSION
  critic_phase: Critic-3 pure logic
  handed_to_critic: [two source guards, boundary and fail-closed provocations, exact census]
  withheld: [Critic-1 and Critic-2 conclusions]
  declaration_logged: "Context break executed; attacking artifacts, not memory."
  honesty_note: procedural, not amnesia; branches attacked through named counterexamples
```

```yaml
CONTEXT_BREAK:
  id: CB-cap-1-final-claims
  mechanism: PROCEDURAL_IN_SESSION
  critic_phase: Critic-4 claims and records
  handed_to_critic: [charter clauses, final staged diff, live counts, navigation and design carriers]
  withheld: [earlier Critic conclusions]
  declaration_logged: "Context break executed; attacking artifacts, not memory."
  honesty_note: procedural, not amnesia; every quantitative and wiring claim re-measured
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cap-1-source-file-line-cap
  cycle: final
  risk_tier: standard
  critic_engine: ccc
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 through C-012 were walked against the final scripts, pins, wiring, and carriers.
      artifacts: [proposition ledger, scripts/check_rust_file_size.py, scripts/check_lib_py.py, test_cap_1_source_file_line_cap.py]
    - id: AT-2
      status: ATTACKED
      evidence: The attack covered 999/1000/1001, exact and adjacent baselines, blank lines, malformed rows, exemptions, and near misses.
      artifacts: [test_cap_1_rust_boundary_counts_blank_lines, test_cap_1_python_boundary_counts_blank_lines, test_cap_1_malformed_exception_records_fail, test_cap_1_fixture_exemptions_are_wired_into_the_scan]
    - id: AT-3
      status: ATTACKED
      evidence: Missing roots, missing and outside-scan rows, unreadable paths, and empty scans all fail closed.
      artifacts: [test_cap_1_fail_closed_conditions, test_cap_1_missing_scan_roots_fail]
    - id: AT-4
      status: N/A
      justification: The guards are stateless read-only tree scans with no shared state, ordering, retry, or concurrency surface.
    - id: AT-5
      status: ATTACKED
      evidence: Path membership, symlink-independent relative reporting, workflow commands, secret patterns, and prohibited surfaces were inspected.
      artifacts: [scripts/check_rust_file_size.py, scripts/check_lib_py.py, .github/workflows/ci.yml, added-line secret scan]
    - id: AT-6
      status: ATTACKED
      evidence: Literal base sets prevent migration drift; facade no-stub behavior and active campaign compatibility remain intact.
      artifacts: [test_cap_1_exception_tables_equal_the_measured_debt, test_cap_1_facade_no_stub_scope_is_unchanged, fnp-0-charter-ledger.md compatibility note]
    - id: AT-7
      status: N/A
      justification: Each guard performs one finite linear repository scan; the unit adds no runtime or system-breaking performance surface.
    - id: AT-8
      status: ATTACKED
      evidence: Stable Make and CI commands still call the existing guards; current prose points at their numeric SSOT.
      artifacts: [test_cap_1_existing_gate_surfaces_remain_dual_wired, test_cap_1_prose_and_navigation_name_the_generalized_gate]
    - id: AT-9
      status: ATTACKED
      evidence: Every new failure names the path, measured or recorded boundary, and required repair; summary output stays stable.
      artifacts: [test_cap_1_growth_above_exact_baseline_fails, test_cap_1_shrink_requires_baseline_update, test_cap_1_fail_closed_conditions]
    - id: AT-10
      status: ATTACKED
      evidence: Twenty-three focused cases pass; removing validation, scan membership, or fixture filtering makes the owning tests fail.
      artifacts: [test_cap_1_source_file_line_cap.py, cycle-2 and final-cycle scratch mutation runs]
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-8, AT-10]
  finding_dispositions: [Q-CAP1-001 REMEDIATED, L-CAP1-001 REMEDIATED, CL-CAP1-001 REMEDIATED, Q-CAP1-002 REMEDIATED, CL-CAP1-002 REMEDIATED, CL-CAP1-003 REMEDIATED]
  verdict: CONVERGED
```

Critic-1: CLEAN after remediation. Critic-2: CLEAN. Critic-3: CLEAN after remediation.
Critic-4: CLEAN after remediation. No OPEN or SUSTAINED finding remains at the S1 floor.

## PR readiness and delivery handoff

```yaml
PR_READINESS_CHECKLIST:
  id: RA-cap-1-source-file-line-cap
  self_run_by_orchestrator: false
  auditor: independent STANDARD-path readiness agent
  checks:
    ci_green: PASS (make verify; make preflight; focused CAP-1 module 23 passed)
    unit_clauses_proven: PASS (C-001..C-012 are PROVEN with live pins)
    coverage_attestation_attached: PASS (final CCC cycle; AT-1..AT-10 complete)
    findings_ledger_closed: PASS (six S1 findings REMEDIATED with regression evidence)
    clause_trace_complete: PASS (all 21 staged artifacts map to C-001..C-012)
  verdict: READY
  send_back_target: "N/A"
```

Independent re-checks: the focused CAP-1 module passed 23 tests; the Rust guard scanned 273 files
with 45 exact exceptions; the Python guard scanned 391 files with 35 exact exceptions; ledger
grammar reported five live ledgers clean. The auditor confirmed that CAP-1 contains no `map.md`
deletion, JSON-ledger CLI, comment-policy, SEPMO-role, or source-split work.

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: cap-1-source-file-line-cap
  flags: []
  count: 0
```

Pre-merge evidence on 2026-08-26:

- `make verify`: PASS, including all Rust workspace tests and mechanical gates.
- `make preflight`: PASS; facade 3,721 passed / 71 skipped; Rust and Python dependency audits
  clean; 13 workflows parsed; zizmor returned no findings.
- Disk checks before `make verify` and `make preflight`: 364 GB and 363 GB free, respectively.

Delivery remains pending the owner's explicit verdict after PR handoff; this ledger does not claim
`DELIVERY_SIGNOFF: ACCEPTED` on the owner's behalf.
