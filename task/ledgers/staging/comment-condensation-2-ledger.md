# CC-2 — repository-wide source comment condensation

**Date:** 2026-08-28 · **Branch:** `codex/comment-condensation` · **Base:** `73af134`
(`#256`) · **Path:** STANDARD (repository-wide Rust and Python documentation surface; no intended
runtime change) · **Prior sweep:** [CC-1 / PR #249](../archive/2026-08/2026-08-27-comment-compaction-ledger.md).

**Retires:** move this ledger to `../completed/` in this PR's final commit after every clause is
proved, the closing Critic converges, and the preflight gate passes.

The owner rejected the broad exemptions used by CC-1 and requested one much deeper PR. CC-2
audits comments and docstrings sentence by sentence. It removes narration, history, repeated
examples, implementation walkthroughs, and text the code already says. It retains only the
shortest required contract, non-obvious reason, invariant, safety condition, oracle discriminator,
directive, attribution, or public documentation.

The owner's cited `execute_ctas_service_managed` block is the calibration case. The seventeen-line
walkthrough does not survive as a retained protocol block. Any required function documentation is
reduced to the same compact style as the owner's accepted one-line example.

## Measured population

The 2026-08-28 read-only census at base `73af134` enumerates tracked hand-authored `.rs` and `.py`
files under `crates/` and `python/`. It excludes generated, vendor, golden, and fixture directories;
none contains a Rust or Python source file. The 220 excluded artifacts are 60 Spark fixtures and
160 TA goldens.

| Sequential slice | Files | Source lines | Comment/doc lines | Blocks | Blocks >2 lines |
|---|---:|---:|---:|---:|---:|
| `crates/repark-common` | 4 | 705 | 286 | 105 | 29 |
| `crates/repark-iceberg` | 40 | 28,018 | 4,442 | 1,254 | 453 |
| `crates/repark-core` | 39 | 14,373 | 2,909 | 647 | 332 |
| `crates/repark-functions` | 30 | 16,520 | 2,636 | 797 | 281 |
| `crates/repark-ta` | 21 | 10,940 | 1,916 | 632 | 174 |
| `crates/repark-spark` | 74 | 45,846 | 6,383 | 1,965 | 684 |
| `crates/repark-sql` | 52 | 18,215 | 2,795 | 901 | 247 |
| `crates/repark-ml` | 6 | 1,744 | 315 | 129 | 37 |
| `crates/repark-python` | 14 | 8,407 | 1,859 | 474 | 209 |
| `python/repark` | 302 | 135,248 | 26,799 | 10,164 | 2,500 |
| `python/repark-parity` | 100 | 26,258 | 3,308 | 1,407 | 298 |
| **Total** | **682** | **306,274** | **53,648** | **18,475** | **5,244** |

The finite file enumeration is `git ls-files 'crates/**/*.rs' 'python/**/*.py'` at the frozen
base. Rust contributes 280 files; Python contributes 402 files.

## Disposition rubric

Each Luna Actor reads every file in its slice and assigns every contiguous comment or docstring
block one disposition.

**Keep byte-exact**

- all 28 `Model:` provenance lines across 15 Rust files;
- all valid `pins:` citations;
- licenses, tool directives, `noqa` reasons, and generated-code controls;
- literal examples whose bytes are an oracle or parser input.

**Keep or condense to the shortest complete form**

- required Python public docstrings and Rust public API contracts;
- required 91-`=` Rust section banners, with their body judged separately;
- safety, durability, cleanup, concurrency, FFI, compatibility, refusal, and resource-bound
  reasons that a competent reader cannot derive from code;
- oracle comments that identify the exact behavior a test discriminates.

**Delete**

- narration of the next statement or an obvious function name;
- implementation walkthroughs, call-by-call recipes, and assertion-by-assertion test narration;
- change history, review rounds, actor/model workflow narration other than `Model:` provenance;
- repeated examples, preambles, emphasis, and duplicated contracts;
- commented-out code and prose already single-homed in a map, design, ADR, ledger, or test name.

A broad category never exempts a whole block. Every sentence must independently earn its place.
The default maximum is one sentence. Two short sentences are allowed when the second carries a
distinct failure mode or invariant. Longer blocks require a specific public contract, formal
`Args`/`Returns`/`Raises` section, directive, license, oracle payload, or an explicit retained-block
reason in this ledger's slice record.

## PROPOSITION LEDGER — CC-2 — 2026-08-28

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The finite code population is frozen before edits begin. | Owner chooses whether the 682-file `crates/**` + `python/**` population is complete or whether `scripts/**`, workflows, and root source-like configuration join it. | **OPEN** | Does “entire codebase” mean the explicitly sequenced Rust and Python roots only, or also `scripts/**`, `.github/workflows/**`, and root TOML/YAML/Makefile files? |
| C-002 | Every source file and every comment/docstring block in the frozen population receives a KEEP, CONDENSE, or DELETE disposition. | Per-slice file totals and before/after block census; Actor coverage record; independent closing census. | **PROVEN** | The owner's “entire code base” requirement plus the measured finite roster above states the acceptance domain. |
| C-003 | No whole-block exemption preserves narration merely because a block is documentation, a banner body, a test oracle, or runtime protocol prose. | Sentence-level review; every retained block over the default maximum has an allowed structural reason or a retained-block record. | **PROVEN** | The owner rejected the CTAS block; CC-1's category-level exemption is explicitly superseded for this unit. |
| C-004 | Required provenance, citations, licenses, directives, public contracts, banners, invariants, and safety conditions survive in their shortest complete form. | Byte-exact inventories for `Model:` and `pins:` forms; presence gates; retained-contract review; before/after counts. | **PROVEN** | AGENTS.md, `task/lessons.md` 2026-08-27, and the owner's accepted compact example define the preservation boundary. |
| C-005 | The PR changes no executable Rust or Python behavior, identifier, signature, control flow, literal data, test input, assertion, or dependency. | Rust token equivalence after comment/doc-token removal; Python token and AST equivalence after normalizing comments and approved docstrings; full diff audit. | **PROVEN** | `docs/testing.md` exempts pure comment/doc changes from new tests but requires existing gates; equivalence is an explicit unit deliverable. |
| C-006 | Python docstring condensation preserves each callable's public behavior, parameters, return, raises, and non-obvious PySpark compatibility contract. | Public-docstring presence gate; per-directory review; normalized AST proof; facade and parity suites. | **PROVEN** | AGENTS.md and the Python quality skill require these contracts; runtime `__doc__` changes are limited to the approved condensation. |
| C-007 | The sweep runs sequentially: one Rust crate at a time in the table's order, then `python/repark`, then `python/repark-parity`, using Luna High Actors for the edits. | Agent record per slice; no overlapping Actor edits; Orchestrator review before the next slice. | **PROVEN** | Direct owner instruction on model, role, and order. |
| C-008 | Every changed source directory updates its own `map.md`, and durable rationale removed from code remains single-homed where required. | Changed-directory-to-map comparison; map sync and map lockstep gates; rationale relocation audit. | **PROVEN** | AGENTS.md map and document-lifecycle contracts. |
| C-009 | One PR contains the complete coherent sweep, including pickup and departure lifecycle commits. | One branch/PR; no second delivery branch; ledger moved with the sanctioned lifecycle tool. | **PROVEN** | Direct owner request for one PR; unit pickup contract requires the lifecycle boundary commits. |
| C-010 | The final branch passes focused syntax/format checks, `make verify`, required Python suites, `make preflight`, and language-aware equivalence. | Exact commands and exit codes recorded after a fresh disk check. | **PROVEN** | AGENTS.md verification contract and binding-manifest green commands. |
| C-011 | A fresh closing Critic attacks the whole assembled diff, rechecks every slice, and leaves no open S0/S1 finding. | Complete AT-1..AT-10 attestation, findings dispositions, quantitative claim remeasurement, and readiness ledger. | **PROVEN** | SEPMO STANDARD/HIGH review contract; repository-wide blast radius selects HIGH review intensity. |

VERDICT: FAIL (OPEN=1, REJECTED=0). LOGIC_SCORE = 10/11.

## Actor plan

- [ ] Freeze C-001 from the owner's population ruling and run PRE_EXECUTION_REVIEW.
- [ ] Record the base census and equivalence tools in durable evidence.
- [ ] Complete and review each Rust crate in the measured order.
- [ ] Complete and review `python/repark` one directory at a time.
- [ ] Complete and review `python/repark-parity` one directory at a time.
- [ ] Update maps, retained-block records, census, and language-aware equivalence evidence.
- [ ] Run broad gates, closing Critic, readiness audit, and departure lifecycle move.
- [ ] Commit, push, open the single PR, and watch its checks to a terminal result.

## Scope-audit self-logic review

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-CC2-AUDIT-1
  agent: Orchestrator
  action: file the measured CC-2 proposition ledger before implementation
  charter_trace: C-001..C-011
  preconditions:
    - current base is fetched and equals origin/main at 73af134: SATISFIED
    - pickup lifecycle and structural gates are green: SATISFIED
    - Rust and Python census is finite and reproducible: SATISFIED
  success_condition: the ledger records every requirement and exposes the only unresolved population boundary as C-001
  step_risks:
    - silently repeat CC-1's broad exemptions: HANDLED (sentence-level rubric and calibration case)
    - start edits before the population is approved: HANDLED (C-001 keeps the gate closed)
  contingencies:
    - amend the OPEN ledger after the owner rules: EXECUTABLE (additive scope-audit rewrite)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
KILLED_ASSUMPTIONS:
  - every runtime-protocol block deserves retention: REMOVED (each sentence earns retention)
  - Rust documentation and Python docstrings are outside comment compaction: REMOVED (both are audited)
  - the prior PR measured the desired end state: REMOVED (it changed only 12 Rust/Python source files)
RISK_HEATMAP:
  - required invariant or safety reason is deleted: S1, mitigated by per-slice review and closing Critic
  - executable token changes ride with prose edits: S1, mitigated by language-aware equivalence and full gates
  - public Python documentation loses a behavior contract: S1, mitigated by callable-level review and facade tests
  - huge map churn replaces code narration with documentation narration: S2, mitigated by single-home and shortest-form review
CLARIFYING_QUESTIONS:
  - Does the population stop at crates/** and python/**, or include scripts/workflows/root configuration?
```
