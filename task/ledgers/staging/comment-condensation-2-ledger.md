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
files under `crates/`, `python/`, and `scripts/`. It excludes generated, vendor, golden, fixture,
archive, history, and task-evidence directories. The 220 excluded artifacts under the source roots
are 60 Spark fixtures and 160 TA goldens.

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
| `scripts` | 16 | 5,550 | 872 | 246 | 62 |
| **Total** | **698** | **311,824** | **54,520** | **18,721** | **5,306** |

The finite file enumeration is
`git ls-files 'crates/**/*.rs' 'python/**/*.py' 'scripts/*.py'` at the frozen base. Rust
contributes 280 files; Python contributes 418 files.

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
| C-001 | The finite code population is frozen before edits begin. | Owner rules on the measured source roots and exclusions. | **PROVEN** | Owner ruling 2026-08-28: Rust and Python source only. Frozen population is 698 `.rs`/`.py` files under `crates/`, `python/`, and `scripts/`; workflows, shell, configuration, fixtures, goldens, archives, history, and task evidence are excluded. |
| C-002 | Every source file and every comment/docstring block in the frozen population receives a KEEP, CONDENSE, or DELETE disposition. | Per-slice file totals and before/after block census; Actor coverage record; independent closing census. | **PROVEN** | The owner's “entire code base” requirement plus the measured finite roster above states the acceptance domain. |
| C-003 | No whole-block exemption preserves narration merely because a block is documentation, a banner body, a test oracle, or runtime protocol prose. | Sentence-level review; every retained block over the default maximum has an allowed structural reason or a retained-block record. | **PROVEN** | The owner rejected the CTAS block; CC-1's category-level exemption is explicitly superseded for this unit. |
| C-004 | Required provenance, citations, licenses, directives, public contracts, banners, invariants, and safety conditions survive in their shortest complete form. | Byte-exact inventories for `Model:` and `pins:` forms; presence gates; retained-contract review; before/after counts. | **PROVEN** | AGENTS.md, `task/lessons.md` 2026-08-27, and the owner's accepted compact example define the preservation boundary. |
| C-005 | The PR changes no product or test executable behavior, identifier, signature, control flow, literal data, test input, assertion, or dependency. Exact downward source-size baseline numbers are the only allowed non-comment code changes. | Rust token equivalence after comment/doc-token removal; Python token and AST equivalence after normalizing comments and approved docstrings; exact-baseline audit; full diff audit. | **PROVEN** | `docs/testing.md` exempts pure comment/doc changes from new tests but requires existing gates. Source-size gates require their existing numeric ceilings to ratchet down to each condensed file's new physical length. |
| C-006 | Python docstring condensation preserves each callable's public behavior, parameters, return, raises, and non-obvious PySpark compatibility contract. | Public-docstring presence gate; per-directory review; normalized AST proof; facade and parity suites. | **PROVEN** | AGENTS.md and the Python quality skill require these contracts; runtime `__doc__` changes are limited to the approved condensation. |
| C-007 | The sweep runs sequentially: one Rust crate at a time in the table's order, then `python/repark`, `python/repark-parity`, and `scripts`, using Luna High Actors for the edits. | Agent record per slice; no overlapping Actor edits; Orchestrator review before the next slice. | **PROVEN** | Direct owner instruction on model, role, and order; 2026-08-28 source-only scope ruling. |
| C-008 | Every changed source directory updates its own `map.md`, and durable rationale removed from code remains single-homed where required. | Changed-directory-to-map comparison; map sync and map lockstep gates; rationale relocation audit. | **PROVEN** | AGENTS.md map and document-lifecycle contracts. |
| C-009 | One PR contains the complete coherent sweep, including pickup and departure lifecycle commits. | One branch/PR; no second delivery branch; ledger moved with the sanctioned lifecycle tool. | **PROVEN** | Direct owner request for one PR; unit pickup contract requires the lifecycle boundary commits. |
| C-010 | The final branch passes focused syntax/format checks, `make verify`, required Python suites, `make preflight`, and language-aware equivalence. | Exact commands and exit codes recorded after a fresh disk check. | **PROVEN** | AGENTS.md verification contract and binding-manifest green commands. |
| C-011 | A fresh closing Critic attacks the whole assembled diff, rechecks every slice, and leaves no open S0/S1 finding. | Complete AT-1..AT-10 attestation, findings dispositions, quantitative claim remeasurement, and readiness ledger. | **PROVEN** | SEPMO STANDARD/HIGH review contract; repository-wide blast radius selects HIGH review intensity. |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 11/11.

**APPROVAL_GATE:** passed 2026-08-28. The owner approved the Rust/Python-source-only population
after reviewing the measured charter.

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-CC2
  pr_unit: comment-condensation-2
  criteria:
    blast_radius: FAIL (698 source files across every delivered crate and Python package)
    reversibility: PASS (comment and docstring-only branch; one normal revert restores it)
    size: FAIL (well above 150 changed lines and five files)
    novelty: PASS (no dependency, external call, interface, or architecture change)
    sensitivity: FAIL (comments on write, FFI, concurrency, and compatibility paths require review)
    clarity: PASS (11/11 charter clauses PROVEN after the owner's population ruling)
  path: STANDARD
  recorded_by: Orchestrator
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PER-CC2
  agent: Orchestrator
  action: execute the approved CC-2 charter as one sequentially assembled PR
  charter_trace: C-001..C-011
  preconditions:
    - charter frozen by the owner's 2026-08-28 source-only ruling: SATISFIED
    - PR carving maps every clause to the single CC-2 unit: SATISFIED
    - STANDARD rubric is recorded above: SATISFIED
    - Luna High Actors, HIGH closing review, and green commands resolve from the user and binding manifest: SATISFIED
    - failure handling uses additive remediation or a normal revert and needs no destructive authority: SATISFIED
  success_condition: all 698 files are dispositioned, equivalence and gates pass, and the closing Critic leaves no open S0/S1 finding
  step_risks:
    - narrative survives behind a broad exemption: HANDLED (sentence-level rubric and independent closing census)
    - invariant or public contract is lost: HANDLED (per-slice Orchestrator review and HIGH closing Critic)
    - executable content changes: HANDLED (language-aware equivalence plus full test gates)
  contingencies:
    - remediate a rejected edit with a scoped forward patch: EXECUTABLE (additive)
    - revert a committed slice if it cannot converge: EXECUTABLE (additive)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
PRE_EXECUTION_REVIEW:
  id: PER-CC2
  slr: SLR-PER-CC2
  plan_checklist:
    charter_frozen: SATISFIED (owner approval 2026-08-28)
    carving_clause_complete:
      forward: SATISFIED (C-001..C-011 map to CC-2)
      backward: SATISFIED (CC-2 traces to C-001..C-011)
    rubric_recorded: SATISFIED (1/1 unit has RUBRIC-CC2)
    bindings_resolved: SATISFIED (gpt-5.6-luna high Actors; make verify and make preflight)
    contingencies_executable: SATISFIED (forward remediation or normal revert)
  verdict: PROCEED
  gap_route: —
  gap_detail: —
```

## Actor plan

- [x] Freeze C-001 from the owner's population ruling and run PRE_EXECUTION_REVIEW.
- [x] Record the base census and equivalence tools in durable evidence.
- [x] Complete and review each Rust crate in the measured order.
- [ ] Complete and review `python/repark` one directory at a time.
- [ ] Complete and review `python/repark-parity` one directory at a time.
- [ ] Update maps, retained-block records, census, and language-aware equivalence evidence.
- [ ] Run broad gates, closing Critic, readiness audit, and departure lifecycle move.
- [ ] Commit, push, open the single PR, and watch its checks to a terminal result.

## Sequential slice evidence

| Slice | Luna High Actor | Comment lines | Blocks | Blocks >2 lines | Executable equivalence | Focused green evidence | Independent Critic |
|---|---|---:|---:|---:|---|---|---|
| `crates/repark-common` | `cc2_repark_common` | 286 → 154 | 105 → 105 | 29 → 8 | All four Rust token streams are identical after lexical comment removal. | `cargo fmt --check`; `cargo test -p repark-common` (13 passed); `make check-rust-file-size`; `git diff --check` | `cc2_common_critic`: CONVERGED after remediation; no open S0/S1 findings. |
| `crates/repark-iceberg` | `cc2_repark_iceberg` | 4,442 → 3,827 | 1,254 → 1,251 | 453 → 441 | All 40 Rust token streams and protected inventories are identical; current source-set SHA-256 `b3cf9fcf6520790fdce10e9b717fb1cd54e8c178ee6c24eaf85c48062aea45bf`. | `cargo fmt --check`; `cargo test -p repark-iceberg` (355 passed); `make check-rust-file-size`; `make check-map-sync`; `git diff --check` | `cc2_iceberg_critic`: CONVERGED after remediation; no open finding. |
| `crates/repark-core` | `cc2_repark_core` | 2,909 → 1,837 | 647 → 644 | 332 → 256 | All 39 Rust token streams and protected inventories are identical; current source-set SHA-256 `bf108675f6ee1f6c4e4a19b770b432aed3bf2eb63d008e752d0ab14f16289562`. | `cargo fmt --check`; `cargo test -p repark-core` (245 passed); `make check-rust-file-size`; `make check-map-sync`; `git diff --check` | `cc2_core_critic`: CONVERGED after four remediation cycles; no open finding. |
| `crates/repark-functions` | `cc2_repark_functions` | 2,636 → 1,137 | 797 → 609 | 281 → 167 | All 30 Rust token streams and protected inventories are identical; current source-set SHA-256 `0a0ae2825e13da2b1121e8c4f5beb9ef5760beb555fcb27d4d4ca617e6f9cf6e`. | `cargo fmt --check`; `cargo test -p repark-functions` (233 passed); `make check-rust-file-size`; `make check-map-sync`; `git diff --check` | `cc2_functions_critic`: CONVERGED after remediation; no open slice finding. |
| `crates/repark-ta` | `cc2_repark_ta` | 1,916 → 1,009 | 632 → 299 | 174 → 115 | All 21 Rust token streams and protected inventories are identical; current source-set SHA-256 `04922af508e9d7066d8af13ac0b5817b0d89fcb52fbaf81bcbd51ff8382dec58`. | `cargo fmt --check`; `cargo test -p repark-ta` (159 passed); `cargo test -p repark-ta --features datafusion` (195 passed); `make check-rust-file-size`; `make check-map-sync`; `git diff --check` | Numeric Critic CONVERGED; UDF Critic REMANDED exactly one verified base-only S0, with all unit findings remediated. |
| `crates/repark-spark` | `cc2_repark_spark` + remediation | 6,383 → 4,890 | 1,965 → 1,954 | 684 → 592 | All 74 Rust token streams and protected inventories are identical; current source-set SHA-256 `211fb455e6d7f23d790dae20e9c3dc35c8c2925a1668b87afd7ca98304b55acc`. | `cargo fmt --check`; `cargo test -p repark-spark` (655 passed); `make check-rust-file-size`; `make check-map-sync`; `git diff --check` | Logic, safety, and aggressive-compaction Critics CONVERGED after five remediation cycles. |
| `crates/repark-sql` | `cc2_repark_sql` + remediation | 2,795 → 1,052 | 901 → 882 | 247 → 62 | All 52 Rust token streams and protected inventories are identical; current source-set SHA-256 `0c0c8f5c636595e3d560ab486fa4acd48bc5b606d176bf7e3c34dae949050652`. | `cargo fmt --check`; `cargo test -p repark-sql` (356 passed); `make check-rust-file-size`; `make check-map-sync`; `git diff --check` | Logic, safety, and aggressive-compaction Critics CONVERGED after four remediation cycles. |
| `crates/repark-ml` | `cc2_repark_ml_actor` + remediation | 315 → 221 | 129 → 85 | 37 → 34 | All six Rust token streams and protected inventories are identical; current source-set SHA-256 `bba252408edf7a2439d9ab3c3b124f37555c88f05e13410e33e9d49964491e6a`. | `cargo fmt --check`; `cargo test -p repark-ml` (35 passed); `make check-rust-file-size`; `make check-map-sync`; `git diff --check` | Logic, safety, and aggressive-compaction Critics CONVERGED after three remediation cycles. |
| `crates/repark-python` | `cc2_repark_ml_actor` + remediation | 1,859 → 1,043 | 474 → 376 | 209 → 134 | All 14 Rust token streams and protected inventories are identical; current source-set SHA-256 `44f0cbc69be2935e8eef92fc52f674f26832440a5021efbd7a02263ab03ef9cd`. | `cargo fmt --check`; `cargo test -p repark-python` (63 passed); `make check-rust-file-size`; `make check-map-sync`; `git diff --check` | Logic, safety, and aggressive-compaction Critics CONVERGED after four remediation cycles. |
| `python/repark/src/repark` direct files | `cc2_repark_ml_actor` + remediation | 207 → 92 | 49 → 47 | 16 → 4 | All three Python executable token streams, canonical ASTs, and protected inventories are identical; current source-set SHA-256 `7fedb7373d5b841b5548746efa670f8752b66657f1db7f53014cb53935784202`. | `make py-lint`; `make py-format-check`; `make check-python-conventions`; `make check-docstring-presence`; `make check-lib-py`; `make check-map-sync`; `git diff --check` | Logic and aggressive-compaction Critics CONVERGED after taxonomy remediation; no open finding. |
| `python/repark/src/repark/spark` direct files | `cc2_repark_ml_actor` + remediation | 4,623 → 4,185 | 1,598 → 1,512 | 413 → 377 | All 27 Python executable token streams, canonical ASTs, and protected inventories are identical; current source-set SHA-256 `66c9cbfb721e9b6223d6a205d24f975f1dbdbb2c4134333c81a2197df7b8ad0f`. | `make py-lint`; `make py-format-check`; `make check-python-conventions`; `make check-docstring-presence`; `make check-lib-py`; `make check-map-sync`; `git diff --check` | Logic/safety and aggressive-compaction Critics CONVERGED after two remediation cycles. |
| `python/repark/src/repark/spark/dataframe` | `cc2_repark_ml_actor`, `cc2_df_grouped_actor`, `cc2_df_writer_actor` + remediation | 3,276 → 2,014 | 935 → 814 | 326 → 199 | All seven Python executable token streams, canonical ASTs, and protected inventories are identical; current source-set SHA-256 `f87b715e69dd46f9e0222b9a11c4e9becb3a5dc5242a5f67640131bbcb58798f`. | `make py-lint`; `make py-format-check`; `make check-python-conventions`; `make check-docstring-presence`; `make check-lib-py`; `make check-map-sync`; `git diff --check` | Logic/safety and aggressive-compaction Critics CONVERGED after remediation; no open finding. |
| `python/repark/src/repark/spark/ml` direct files | `cc2_py_ml_actor` + remediation | 637 → 496 | 418 → 356 | 37 → 37 | All 11 Python executable token streams, canonical ASTs, and protected inventories are identical; current source-set SHA-256 `d785f686f4d844559b51d5f1de948a2fea088f5aceda98a9e3ae93bd4a504152`. | `make py-lint`; `make py-format-check`; `make check-python-conventions`; `make check-docstring-presence`; `make check-lib-py`; `make check-map-sync`; `git diff --check` | Logic and aggressive-compaction Critics CONVERGED after two remediation cycles; no open finding. |

The eight retained long blocks in `repark-common` are required module or API banners and structured
public documentation. Every `Model:`, `pins:`, `MUTATION:`, and `#[cfg]` inventory entry remains
byte-identical.

The Iceberg Actor condensed 19 files and deliberately kept 21 after reviewing all 40. The 441
retained long blocks were independently classified as required banners, structured/public
contracts, safety/runtime invariants, directives/provenance, or oracle/test discriminators. Six
Rust file-size exception baselines ratcheted down to their exact current lengths: `alter.rs` 1,725,
`append.rs` 2,193, `merge/mod.rs` 2,565, `overwrite.rs` 1,148, `position_delete.rs` 1,033, and
`predicate_dml.rs` 1,285.

The Core Actor changed 34 files and kept five after reviewing all 39. The Critic classified all 256
retained long blocks and preserved the DataFusion guard, session construction, AWS finalization,
temp-view identity, time-travel plan-shape, schema-tightening, dynamic-flatten, error-classification,
timezone, runtime-ownership, and backend-seam contracts. Four exact file-size baselines now match
`catalog_config.rs` 1,105, `session.rs` 1,261, `session/tests.rs` 1,415, and
`tests/declared_sorted.rs` 1,381.

The Functions Actor condensed 29 files and kept `session_time_zone/tests.rs` after reviewing all
30. The Critic classified all 167 retained long blocks and preserved analyzer order and fixpoint,
ANSI and decimal semantics, LTZ/NTZ/DST behavior, Java regex and URI compatibility, random-stream
and allocation bounds, collection safety, structured errors, and shared SQL/facade kernels. Exact
file-size baselines now match `analyzer.rs` 1,194 and `datetime.rs` 1,783.

The TA Actor condensed 20 files and kept `src/tests.rs`, which has no comments, after reviewing all
21. The Critics classified all 115 retained long blocks and preserved TA-Lib operation order,
lookbacks, guards, split-family routing, full-partition evaluation, cache pinning, null-to-NaN
densification, strict-bit goldens, and runtime fixture resolution. Exact file-size baselines now
match `momentum.rs` 2,284, `overlap.rs` 1,676, and `udf/mod.rs` 1,873. Six navigation maps were
trued up, including stale UDF paths and the current golden-test count.

The Spark Actors condensed 69 files and kept five after reviewing all 74. The pass preserves parser
altitude, per-door routing, staged-write and destructive-operation guards, time-travel lifecycle,
window-frame refusal rules, session ordering, fixture provenance, and exact protected payloads.
Eight navigation maps were trued up; all twelve crate maps are current. Thirteen exact file-size
baselines now match `alter.rs` 1,885, `metadata_tables.rs` 1,132,
`ref_ddl.rs` 1,078, `tests/alter.rs` 1,445, `tests/call.rs` 1,407,
`tests/ctas.rs` 1,414, `tests/dml.rs` 1,216, `tests/insert_overwrite.rs` 1,288,
`tests/merge.rs` 1,349, `tests/partitioned_merge.rs` 1,170,
`tests/transform_overwrite.rs` 1,242,
`window_range.rs` 1,320, and
`tests/session_timezone.rs` 1,018. `call.rs` (975) and `normalize.rs` (987) returned below the
default ceiling, and `tests/describe_show.rs` (994) joined them. Their exception rows were removed.

The SQL Actors condensed all 52 files. Three independent Critics inspected all 62 retained long
blocks and all 15 maps. The pass preserves guard and router order, staged publication, the exact
service-managed abort boundary, time-travel cleanup and its core lookup residual, path traversal
defense, parser offset preservation, refusal contracts, and oracle discriminators. Exact file-size
baselines now match `src/guards/tests.rs` 1,207, `src/tests.rs` 1,523, and
`tests/cross_door.rs` 1,259.

The ML Actor condensed all six files. The Critics classified all 34 retained long blocks and
preserved streaming-memory bounds, Cholesky failure contracts, KMeans termination and numeric
conversion reasons, stable sigmoid evaluation, zero-iteration behavior, and public caller
preconditions. The three ML maps now state current ownership and single-home eight verified
base-only findings. No ML source exceeds the default Rust file-size ceiling.

The Python-binding Actor condensed all 14 files. The Critics classified every comment block and
all 134 retained long blocks. The pass preserves panic fences, GIL and runtime ownership, Arrow C
Stream lifetime and memory bounds, typed exception contracts, namespace mirroring, ML boundary
validation, and public binding behavior. Four maps are current. Exact file-size baselines now
match `src/dataframe.rs` 1,321 and `src/session.rs` 1,331; `src/column/function_dispatch.rs` and
`src/column/mod.rs` returned below the default ceiling. Four verified base-only findings are
single-homed in `src/map.md`.

The first Python package slice condensed all three direct modules and its map. The Critics reviewed
all 49 frozen blocks and preserved native exception identity, structured facade errors, the
process-wide ANSI session, facade import ordering, and public re-exports. The exception taxonomy now
states the full `IllegalArgumentException` boundary, including ML, schema, value, and stream-input
validation. Focused pytest files exist but did not run because the plain interpreter lacks pytest;
the repository Python gates passed without installing dependencies.

The direct Spark-facade slice reviewed all 27 modules, changed 26, and kept `__init__.py`. The
Critics inspected all 1,512 retained blocks and preserved SQL escaping, secret redaction,
temporary-view ownership, column identity, UDF and UDTF boundaries, random and window semantics, TA
lookbacks, catalog liveness, Row and StorageLevel behavior, and optional-dependency failures. The
local map replaced 1,632 lines of campaign history with current ownership, durable contracts, and
links. Exact Python file-size baselines now match `column.py` 1,589, `functions.py` 1,985,
`functions_expr.py` 2,265, `functions_udf.py` 1,300, `ta.py` 1,818, and `types.py` 1,834. Focused
pytest was unavailable because the plain interpreter lacks pytest; repository Python gates passed.

The DataFrame slice reviewed all seven direct modules and condensed 1,262 comment/doc lines. The
pass preserves liveness and spawn rules, display-to-engine identity, SQL identifier quoting,
aggregate and generator routing, bridge replay, cache behavior, join-origin handling, output
privacy, and writer contracts. The local map is 68 lines of current ownership and limitations.
Exact Python file-size baselines now match `core.py` 6,371, `joins_columns.py` 1,239,
`plan_collapse.py` 1,168, and `writer_readwriter.py` 1,117. Repository Python gates passed.

The direct ML slice reviewed all 11 modules and all 356 retained blocks. The pass preserves
estimator defaults and refusals, vector wire shapes, parameter identity, stable numeric and tie
handling, evaluator guards, pipeline replacement races, and cross-validation concurrency. The
local map is 42 lines of current ownership and limitations. Every module remains below the default
Python file-size ceiling, so no baseline changed. Repository Python gates passed.

The reusable equivalence harness is `/tmp/cc2_equivalence.py`, SHA-256
`a083b40c6bda55daea9e95771efe3e82538c85dea21185a59fb39db2ed5aca4b`; its twelve-test
self-suite passes. It compares ordered Rust token kind and spelling after nested-comment-aware
lexing. For Python it compares executable `tokenize` output and a canonical AST after normalizing
only recognized module, class, function, and async-function docstrings. It ignores comment-only
`NL` tokens and `TypeIgnore` source positions because both move when comments shrink; the protected
inventory still compares every directive byte. It also inventories protected provenance, pins,
mutation payloads, licenses, directives, and compiler controls. The temporary harness is evidence
tooling, not a shipped product change, and is removed at departure.

```yaml
SLICE_REMEDIATION:
  pr_unit: comment-condensation-2/repark-common
  findings:
    - id: F-CC2-COMMON-001
      severity: S1
      disposition: REMEDIATED
      evidence: The wrong-door contract now limits sniffing to parse or plan failures.
      testability: Documentation-only finding; independent text review and lexical equivalence apply.
    - id: F-CC2-COMMON-002
      severity: S1
      disposition: REMEDIATED
      evidence: TwoSession now requires separate native and Spark-extended sessions through their own doors and explains why one extended session cannot prove equivalence.
      testability: Documentation-only finding; independent text review and lexical equivalence apply.
    - id: F-CC2-COMMON-003
      severity: S3
      disposition: REMEDIATED
      evidence: The Error banner now keeps only the non-obvious boundary-fold contract.
      testability: Documentation-only finding; independent text review and lexical equivalence apply.
  critic_reattestation: CONVERGED
```

```yaml
SLICE_REMEDIATION:
  pr_unit: comment-condensation-2/repark-iceberg
  findings:
    - id: F-CC2-ICEBERG-001
      disposition: REMEDIATED
      evidence: insert_gate retains only synthesized-cast gating, explicit-cast exclusion, and the literal-VALUES residual.
    - id: F-CC2-ICEBERG-002
      disposition: REMEDIATED
      evidence: alter retains only public API scope, atomic transaction behavior, and failure contracts.
    - id: F-CC2-ICEBERG-003
      disposition: REMEDIATED
      evidence: name_resolution retains only case-insensitive zero/one/many collision behavior and its scope boundary.
    - id: F-CC2-ICEBERG-004
      disposition: REMEDIATED
      evidence: catalog/mod retains only frozen free-SQL snapshots, live facade listing, and O(1) product invalidation.
    - id: F-CC2-ICEBERG-005
      disposition: REMEDIATED
      evidence: builders extraction history was removed.
    - id: F-CC2-ICEBERG-006
      disposition: REMEDIATED
      evidence: provider workflow history was removed while its snapshot and invalidation invariant remains.
    - id: F-CC2-ICEBERG-007
      disposition: REMEDIATED
      evidence: test_tracing retains only the global-subscriber invariant and worker-thread reason.
    - id: F-CC2-ICEBERG-008
      disposition: REMEDIATED
      evidence: position_delete retains the identity and allocation-sharing reason without revision history.
    - id: F-CC2-ICEBERG-009
      disposition: WITHDRAWN
      evidence: The frozen charter does not require check-comment-density; PR #247 deliberately pins that absent target and assigns compliance to review.
  critic_reattestation: CONVERGED
```

```yaml
SLICE_REMEDIATION:
  pr_unit: comment-condensation-2/repark-core
  findings:
    - id: F-CC2-CORE-001
      disposition: REMEDIATED
      evidence: Removed stale timezone-consumption wording from the local map.
    - id: F-CC2-CORE-002
      disposition: REMEDIATED
      evidence: Reduced DataFusion guard history to its rule-order and Unnest-scoped failure contracts.
    - id: F-CC2-CORE-003
      disposition: REMEDIATED
      evidence: Reduced temp-view registration narration to home, provider, and live-default contracts.
    - id: F-CC2-CORE-004
      disposition: REMEDIATED
      evidence: Removed declared-sorted round history and assertion narration.
    - id: F-CC2-CORE-005
      disposition: REMEDIATED
      evidence: Condensed session and temp-view-door test narration to exact oracle discriminators.
    - id: F-CC2-CORE-006
      disposition: REMEDIATED
      evidence: Removed catalog matrix and read-options extraction history.
    - id: F-CC2-CORE-007
      disposition: REMEDIATED
      evidence: Removed guard fuzzer, phase, and performance narration.
    - id: F-CC2-CORE-008
      disposition: REMEDIATED
      evidence: Reduced error mapping to structured classification and bounded peeling.
    - id: F-CC2-CORE-009
      disposition: REMEDIATED
      evidence: Reduced pre-execution prose to side-effect-free plan, guard, and execute ordering.
    - id: F-CC2-CORE-010
      disposition: REMEDIATED
      evidence: Removed further declared-sorted BASE and round history.
    - id: F-CC2-CORE-011
      disposition: REMEDIATED
      evidence: Further condensed the temp-view resolver contract.
    - id: F-CC2-CORE-012
      disposition: REMEDIATED
      evidence: Corrected and shortened the accepted temp-view name forms and errors.
    - id: F-CC2-CORE-013
      disposition: REMEDIATED
      evidence: Removed dialect round history while retaining plan-to-execute ordering.
    - id: F-CC2-CORE-014
      disposition: REMEDIATED
      evidence: Removed DataFusion guard-test split and measurement history.
    - id: F-CC2-CORE-015
      disposition: REMEDIATED
      evidence: Removed temp-view test-location and round history.
    - id: F-CC2-CORE-016
      disposition: REMEDIATED
      evidence: Removed session module-split and phase history while retaining build contracts.
    - id: F-CC2-CORE-017
      disposition: REMEDIATED
      evidence: Removed runtime round labels while retaining pinned-home and parsed-segment invariants.
    - id: F-CC2-CORE-018
      disposition: REMEDIATED
      evidence: Reduced time-travel helper history to its error-conversion contract.
    - id: F-CC2-CORE-019
      disposition: REMEDIATED
      evidence: Removed a dated performance preamble from the configuration-precedence oracle.
    - id: F-CC2-CORE-020
      disposition: REMEDIATED
      evidence: Reduced the declared-sorted fixture note to its `LIMIT 0` condition.
    - id: F-CC2-CORE-021
      disposition: REMEDIATED
      evidence: Removed temp-view caller walkthrough and round history.
    - id: F-CC2-CORE-022
      disposition: REMEDIATED
      evidence: Removed the final extraction, phase, former-implementation, split, and prior-fix history batch.
  critic_reattestation:
    verdict: CONVERGED
    coverage: AT-1..AT-10 PASS
    retained_long_blocks: 256 classified
    new_findings: 0
```

```yaml
SLICE_REMEDIATION:
  pr_unit: comment-condensation-2/repark-functions
  findings:
    - { id: F-CC2-FUNCTIONS-001, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-002, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-003, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-004, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-005, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-006, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-007, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-008, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-009, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-010, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-011, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-012, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-013, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-014, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-015, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-016, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-017, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-018, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-019, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-020, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-021, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-022, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-023, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-024, disposition: REMEDIATED }
    - { id: F-CC2-FUNCTIONS-025, disposition: REMEDIATED }
  remediation_evidence:
    - Removed duplicate, temporal, campaign, benchmark, and implementation-walkthrough prose.
    - Corrected seed-zero, ANSI, microsecond timestamp, LTZ/NTZ/DST, and idempotence claims.
    - Kept exact provenance, pins, safety bounds, compatibility contracts, and oracle discriminators.
  critic_reattestation:
    verdict: CONVERGED
    coverage: AT-1..AT-10 ATTACKED
    retained_long_blocks: 167 classified
    new_findings: 0
```

```yaml
SLICE_REMEDIATION:
  pr_unit: comment-condensation-2/repark-ta
  findings:
    - { id: F-CC2-TA-UDF-001, disposition: REMEDIATED, evidence: "Five changed source-directory maps and the crate map are current." }
    - { id: F-CC2-TA-UDF-002, disposition: REMEDIATED, evidence: "The BBANDS benchmark now says one-run-plus-clones." }
    - { id: F-CC2-TA-UDF-003, disposition: REMEDIATED, evidence: "Removed redundant TaFn enum narration." }
    - { id: F-CC2-TA-UDF-005, disposition: REMEDIATED, evidence: "Corrected stale udf.rs navigation paths." }
    - { id: F-CC2-TA-UDF-006, disposition: REMEDIATED, evidence: "Corrected the golden file to 41 tests: 39 golden plus two fixture/manifest checks." }
  critic_reattestation:
    verdict: REMANDED
    coverage: AT-1..AT-10 ATTACKED
    retained_long_blocks: 115 classified
    open_findings: [F-CC2-TA-UDF-004]
    new_unit_findings: 0
```

```yaml
SLICE_REMEDIATION:
  pr_unit: comment-condensation-2/repark-spark
  findings:
    - { id: F-CC2-SPARK-001, disposition: REMEDIATED, evidence: "Corrected stale CALL procedure and test-count claims." }
    - { id: F-CC2-SPARK-002, disposition: REMEDIATED, evidence: "Reduced router, parser, extension, and pinned-view narration to live ordering and lifecycle contracts." }
    - { id: F-CC2-SPARK-003, disposition: REMEDIATED, evidence: "Reduced CTAS and overwrite walkthroughs to validation, no-wipe, stage, publish, and rollback invariants." }
    - { id: F-CC2-SPARK-004, disposition: REMEDIATED, evidence: "Kept CALL destructive defaults, deletion-vector refusal, lineage, schema, and honest-count contracts without fork history." }
    - { id: F-CC2-SPARK-005, disposition: REMEDIATED, evidence: "Condensed DESCRIBE/SHOW parsing, redaction, regex, and oracle prose to observable contracts." }
    - { id: F-CC2-SPARK-006, disposition: REMEDIATED, evidence: "Condensed time-travel, RANGE, literal, and cast prose while retaining failure and cleanup invariants." }
    - { id: F-CC2-SPARK-007, disposition: REMEDIATED, evidence: "Removed port, phase, PR, round, group, audit, and pre-fix history from source and maps." }
    - { id: F-CC2-SPARK-008, disposition: REMEDIATED, evidence: "Reduced test narration to the oracle discriminator or failure mode that makes each pin load-bearing." }
    - { id: F-CC2-SPARK-009, disposition: REMEDIATED, evidence: "Restored explicit call_orphan.rs and time_travel.rs map links; strict mode has no new omissions." }
    - { id: F-CC2-SPARK-010, disposition: REMEDIATED, evidence: "Fixed duplicate, malformed, stale, and misleading comment and map claims." }
    - { id: F-CC2-SPARK-011, disposition: REMEDIATED, evidence: "Restored every transient protected-marker or literal drift before accepting a remediation cycle." }
    - { id: F-CC2-SPARK-012, disposition: WITHDRAWN, evidence: "A stale expect_err payload is executable test text and outside the comment-only charter." }
  critic_reattestation:
    verdict: CONVERGED
    critics: [logic, safety, aggressive-compaction]
    coverage: 74/74 Rust files; 12/12 maps
    retained_long_blocks: 592 classified
    new_findings: 0
```

```yaml
SLICE_REMEDIATION:
  pr_unit: comment-condensation-2/repark-sql
  findings:
    - { id: F-CC2-SQL-001, disposition: REMEDIATED, evidence: "Completed truncated and dangling source and test documentation without restoring narration." }
    - { id: F-CC2-SQL-002, disposition: REMEDIATED, evidence: "Corrected CTAS plan-before-publication, staged atomicity, and service-managed abort boundaries." }
    - { id: F-CC2-SQL-003, disposition: REMEDIATED, evidence: "Documented both time-travel names, successful cleanup order, and the core register-then-lookup residual." }
    - { id: F-CC2-SQL-004, disposition: REMEDIATED, evidence: "Restored branch, merge-on-read, path traversal, and byte-offset safety reasons in compact form." }
    - { id: F-CC2-SQL-005, disposition: REMEDIATED, evidence: "Removed phase, milestone, round, campaign, and former-implementation history from source and navigation prose." }
    - { id: F-CC2-SQL-006, disposition: REMEDIATED, evidence: "Corrected false counts, stale paths, overbroad lifecycle claims, and inaccurate test descriptions." }
    - { id: F-CC2-SQL-007, disposition: REMEDIATED, evidence: "Reduced parser, guard, DDL, DML, and test walkthroughs to current reasons and observable contracts." }
    - { id: F-CC2-SQL-008, disposition: REMEDIATED, evidence: "Kept the float fixture's position-dependent partition invariant while removing per-element narration." }
    - { id: F-CC2-SQL-009, disposition: REMEDIATED, evidence: "Trued up all 15 SQL maps and single-homed the four verified base-only findings." }
    - { id: F-CC2-SQL-010, disposition: WITHDRAWN, evidence: "The introspection mutation sentence continues onto the next doc line and already ends with a period; its payload remains byte-exact." }
  critic_reattestation:
    verdict: CONVERGED
    critics: [logic, safety, aggressive-compaction]
    coverage: 52/52 Rust files; 15/15 maps
    retained_long_blocks: 62 classified
    new_unit_findings: 0
```

```yaml
SLICE_REMEDIATION:
  pr_unit: comment-condensation-2/repark-ml
  findings:
    - { id: F-CC2-ML-001, disposition: REMEDIATED, evidence: "Corrected IRLS storage wording to weighted normal equations and parameters." }
    - { id: F-CC2-ML-002, disposition: REMEDIATED, evidence: "Qualified KMeans support to random initialization and explicit default-mode refusal." }
    - { id: F-CC2-ML-003, disposition: REMEDIATED, evidence: "Completed Cholesky and fit error contracts, including zero-iteration behavior." }
    - { id: F-CC2-ML-004, disposition: REMEDIATED, evidence: "Restored cast, overflow, stable-sigmoid, and bounded-sampling reasons in compact form." }
    - { id: F-CC2-ML-005, disposition: REMEDIATED, evidence: "Removed phase, port, PR, SAF, and pointer-change history from source and maps." }
    - { id: F-CC2-ML-006, disposition: REMEDIATED, evidence: "Removed duplicate and private implementation narration while retaining public contracts." }
    - { id: F-CC2-ML-007, disposition: REMEDIATED, evidence: "Trued up all ML maps and single-homed eight verified base-only findings." }
  critic_reattestation:
    verdict: CONVERGED
    critics: [logic, safety, aggressive-compaction]
    coverage: 6/6 Rust files; 3/3 maps
    retained_long_blocks: 34 classified
    new_unit_findings: 0
```

```yaml
SLICE_REMEDIATION:
  pr_unit: comment-condensation-2/repark-python
  findings:
    - { id: F-CC2-PYBIND-001, disposition: REMEDIATED, evidence: "Corrected sort, join, higher-order, window, and Arrow-import error contracts." }
    - { id: F-CC2-PYBIND-002, disposition: REMEDIATED, evidence: "Corrected capsule validation, ownership, GIL, namespace, and empty-stream invariants." }
    - { id: F-CC2-PYBIND-003, disposition: REMEDIATED, evidence: "Removed campaign labels, dated history, stale line references, and implementation walkthroughs." }
    - { id: F-CC2-PYBIND-004, disposition: REMEDIATED, evidence: "Removed dangling, duplicate, malformed, and over-width comment prose." }
    - { id: F-CC2-PYBIND-005, disposition: REMEDIATED, evidence: "Trued up four maps and single-homed four verified base-only findings." }
  critic_reattestation:
    verdict: CONVERGED
    critics: [logic, safety, aggressive-compaction]
    coverage: 14/14 Rust files; 4/4 maps; every current comment block
    retained_long_blocks: 134 classified
    new_unit_findings: 0
```

```yaml
REMAND_RECORD:
  pr_unit: comment-condensation-2/repark-ta
  disposition: REMANDED
  open_findings:
    - { id: F-CC2-TA-UDF-004, severity: S0, origin: BASE_ONLY }
  downstream_disjointness:
    - Later slices are restricted to comment and docstring edits with exact executable-token equivalence.
    - No downstream slice touches or depends on TA cache-key construction or array identity.
    - The Critic verified all 113 downstream Rust paths are executable-token and protected-inventory equivalent.
  closing_authority:
    - Disposition F-CC2-TA-UDF-004 item by item before PR readiness.
    - Require a separate executable fix or a recorded owner merge decision; do not silently accept it.
```

```yaml
REMAND_CLOSING_DECISION:
  finding: F-CC2-TA-UDF-004 / BF-CC2-TA-001 (S0, BASE_ONLY)
  decision: ACCEPT_WITH_RECORD
  decided_by: Owner
  decided_on: 2026-08-29
  channel: Interactive orchestrator session on codex/comment-condensation
  record_home: crates/repark-ta/map.md Known limitations (entry dated 2026-08-29)
  terms:
    - The comment-only unit ships with the defect unfixed; C-005 forbids an executable change here.
    - The executable fix (cache complete array identity or bypass caching) is its own future unit with shared-buffer regression pins.
    - The PR description names this decision as an explicit merge gate per R13.
  item_disposition: CLOSED_BY_USER_DECISION
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-TA-001
  source_finding: F-CC2-TA-UDF-004
  severity: S0
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Shared Float64 values with different validity bitmaps collide and return a stale sibling result.
    - Shared dictionary keys with different child dictionaries collide and return stale values.
    - BBANDS, MACD/FIX/EXT, STOCH/F/RSI, AROON, and MAMA split families are affected.
  disposition: REMANDED
  current_home: crates/repark-ta/map.md Known limitations
  rationale: C-005 forbids an executable fix in this comment-only unit; executable tokens match the defective base exactly.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-FUNCTIONS-001
  severity: S1
  source_ref: 73af134
  unit_finding: false
  evidence:
    - A reachable `i64::MIN` negative sequence stride panics in debug and can undercount in release.
    - Reachable `i128::MIN / -1` and `% -1` constant folds panic in production planner paths.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-functions/map.md Known limitations
  rationale: C-005 forbids an executable fix in this comment-only unit; the diff does not activate or alter the defect.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-SQL-001
  severity: S1
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Quoted catalog identifiers bypass the text-level read-only DML guard.
    - The parsed DML path does not restore the missed catalog refusal.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-sql/map.md Known limitations
  rationale: C-005 forbids an executable guard repair in this comment-only unit; the diff preserves the defective base token stream exactly.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-SQL-002
  severity: S1
  source_ref: 73af134
  unit_finding: false
  evidence:
    - A service-managed CREATE commits the table before catalog invalidation.
    - An invalidation failure returns an error without undoing the committed table.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-sql/map.md Known limitations
  rationale: C-005 forbids changing create or rollback behavior in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-SQL-003
  severity: S2
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Quoted branch targets bypass the text-level write-to-branch guard.
    - The current planner rejects the resulting four-part target before execution.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-sql/map.md Known limitations
  rationale: C-005 forbids changing the guard in this comment-only unit; the planner currently contains the latent bypass.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-SQL-004
  severity: S2
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Time-travel registration uses predictable reserved names in the session catalog.
    - Registration or cleanup can remove a user table that already owns one of those names.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-sql/map.md Known limitations
  rationale: C-005 forbids changing name allocation or ownership behavior in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-ML-001
  severity: S2
  source_ref: 73af134
  unit_finding: false
  evidence:
    - `observe_dense` accepts non-empty features with empty labels and observes no rows.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-ml/src/map.md Known limitations
  rationale: C-005 forbids changing batch-length validation in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-ML-002
  severity: S2
  source_ref: 73af134
  unit_finding: false
  evidence:
    - NaN `elastic_net_param` passes the absolute-value comparison and is accepted.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-ml/src/map.md Known limitations
  rationale: C-005 forbids changing parameter validation in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-ML-003
  severity: S2
  source_ref: 73af134
  unit_finding: false
  evidence:
    - `predict_probability` silently truncates mismatched coefficient and feature lengths through `zip`.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-ml/src/map.md Known limitations
  rationale: C-005 forbids changing prediction behavior in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-ML-004
  severity: S2
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Public `XorShift64::next_index(0)` performs modulo zero and panics.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-ml/src/map.md Known limitations
  rationale: C-005 forbids changing the public random-index API in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-ML-005
  severity: S2
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Squared distance over finite coordinates can overflow and select the wrong first center.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-ml/src/map.md Known limitations
  rationale: C-005 forbids changing distance arithmetic in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-ML-006
  severity: S1
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Repeated same-sign finite KMeans values can overflow a cluster sum.
    - With `max_iter=1`, the fit can return a non-finite center.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-ml/src/map.md Known limitations
  rationale: C-005 forbids changing KMeans accumulation in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-ML-007
  severity: S2
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Public `cholesky_solve` allocates dimension-sized buffers before validating the dimension and inputs.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-ml/src/map.md Known limitations
  rationale: C-005 forbids reordering allocation and validation in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-ML-008
  severity: S1
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Finite OLS values can overflow `Xᵀy` and return non-finite coefficients.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-ml/src/map.md Known limitations
  rationale: C-005 forbids changing normal-equation arithmetic in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-PYBIND-001
  severity: S1
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Direct low-level callers can register an arbitrary local write root without facade validation.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-python/src/map.md Known limitations
  rationale: C-005 forbids changing the local-write trust boundary in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-PYBIND-002
  severity: S1
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Logistic regression with `max_iter=0` does not inspect a missing label column.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-python/src/map.md Known limitations
  rationale: C-005 forbids changing zero-iteration validation in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-PYBIND-003
  severity: S1
  source_ref: 73af134
  unit_finding: false
  evidence:
    - Int64 ML values beyond the exact f64 range can round during conversion.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-python/src/map.md Known limitations
  rationale: C-005 forbids changing ML numeric conversion in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-PYBIND-004
  severity: S2
  source_ref: 73af134
  unit_finding: false
  evidence:
    - The generated `IllegalArgumentException` description mentions only invalid configuration.
    - ML errors also map to that exception.
  disposition: REPORTED_SEPARATE_FIX
  current_home: crates/repark-python/src/map.md Known limitations
  rationale: C-005 forbids changing the runtime-visible Rust string literal in this comment-only unit.
```

```yaml
BASE_ONLY_FINDING:
  id: BF-CC2-PYFACADE-001
  severity: S2
  source_ref: 73af134
  unit_finding: false
  evidence:
    - `struct_type_from_arrow` uses `assert` to validate that its input is a `pyarrow.Schema`.
    - Optimized Python removes that validation.
  disposition: REPORTED_SEPARATE_FIX
  current_home: python/repark/src/repark/spark/map.md Known limitations
  rationale: C-005 forbids changing executable schema validation in this comment-only unit.
```

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
  []
```
