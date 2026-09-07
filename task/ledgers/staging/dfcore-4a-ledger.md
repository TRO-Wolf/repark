# Unit ledger — DFCORE-4a · sampling out of `core.py`

**Date:** 2026-09-07 · **Branch:** `refactor/dfcore-4a` · **Base:** `origin/main`
`7df1f9a9` (PR #417, DFCORE-3) · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DFCORE-4a` **FIXED**.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** The decomposition plan
(`task/roadmap/epic-term/dataframe-core-decomposition-plan-2026-09-07.md`, §4 row DFCORE-4a)
opens the fourth slice: the three sampling bodies (`sample`, `randomSplit`,
`sampleBy`) plus the argument normalization (`_prepare_sample_args`) and the
seed coercion (`_coerce_sample_seed`) leave `core.py` for one leaf module
`sampling.py`; the public methods stay as one-line wrappers so the class
surface moves exactly as declared, and an extended export pin plus
same-seed determinism pins prove the move.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`,
`Cargo.lock`, dependency lists, completed ledgers, any behaviour change,
`show` / repr / HTML (DFCORE-4b owns them), the `approxQuantile` batching
(DFCORE-5 owns it).

**Environment.** The lane native was stale on arrival: `test_dataframe_actions.py`
and `test_g2_window_rand_sampleby.py` failed with `module 'repark._native' has
no attribute 'rows_from_record_batch'`. Rebuilt via `make develop` (exit 0); both
suites then pass (68 passed). This mirrors DFCORE-1 round 2, DFCORE-2, and
DFCORE-3 (R2-S2 class).

## PROPOSITION LEDGER — DFCORE-4a — 2026-09-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The package export surface equals the pre-slice surface under the DFCORE-1 filter; the unfiltered delta is exactly the gain of `sampling`; nothing lost. | `test_dfcore_1_exports.py::test_package_export_set_unchanged`. | **OPEN** | Pin extended before the production edit; pre-move provocation reds exactly the 4 declared deltas (4 failed, 6 passed). |
| C-002 | The core export surface equals the pre-slice surface under the same filter; the unfiltered delta is exactly the `sampling` module binding; `__annotations__` still absent. | `test_dfcore_1_exports.py::test_core_export_set_unchanged`. | **OPEN** | Pin extended before the production edit; pre-move provocation reds exactly the 4 declared deltas (4 failed, 6 passed). |
| C-003 | `DataFrame.__module__`, `__slots__`, slot-only storage, all 38 aliases, the `head` overload pair, and the `stat` accessor are unchanged; `sorted(dir(DataFrame))` loses exactly `_prepare_sample_args`. | Identity/alias/overload/dir pin tests; the T0 freeze test. | **OPEN** | `EXPECTED_DATAFRAME_DIR` loses exactly `_prepare_sample_args`; pre-slice class `dir()` 251 (non-dunder 221). |
| C-004 | The three sampling bodies live in `sampling.py` as frame-first module functions; the class keeps one-line wrappers; `_prepare_sample_args` moves verbatim (receiverless, keeps its parameter list); `_coerce_sample_seed` moves and is re-imported by `core` by identity; bodies AST-identical; docstrings byte-identical; stripped-comment reasons live in `dataframe/map.md`. | `test_moved_sampling_helpers_live_in_new_home`; the AST proof; the sampling suites. | **OPEN** | Ownership test written; the move lands in the next commit. |
| C-005 | `core.py` 5060 → the new count in `scripts/check_lib_py.py` and the CAP-1 test; the new file carries no row; no sibling ceiling rises; the dataframe `map.md` exact-row sentence stays true. | `make check-lib-py`; the parity harness (holds CAP-1). | **OPEN** | Pre-slice `core.py` is exactly 5060 lines. |
| C-006 | The same seed answers the same rows before and after the move: `sample(0.3, seed=7)`, `randomSplit([0.5, 0.5], seed=7)`, and `sampleBy('k', {…}, seed=7)` on a fixed 1,000-row frame answer the row sets recorded on the pre-slice tree. | `test_dfcore_4a_*_determinism.py` (4 pins). | **OPEN** | Row sets recorded pre-slice: sample 302 ids, split 500/500 partitioning `range(1000)`, strata 500 ids; pins pass pre-move (4 passed). |
| C-007 | Every sampling suite is green; one mutation per moved function reds an existing behavioural pin; collected IDs are preserved plus the five new pin tests. | Gate-list run; M1–M5; `--collect-only` diff. | **OPEN** | Pre-slice collect: 6103 IDs. |
| C-008 | Leaf-before-core import order holds with no cycle; no nested defs; zero new `#` bytes beyond `noqa`; ruff lint and format clean. | `-X importtime`; `make check-python-conventions`; `make py-lint`; `make py-format-check`. | **OPEN** | The move lands in the next commit. |

VERDICT: 8 clauses, 0 PROVEN, 8 OPEN, 0 REJECTED.

## Evidence

**Pre-slice counts (DFCORE-3 tip `7df1f9a9`).** `core.py` 5060 lines.
`dir(DataFrame)` 251 (non-dunder 221). Package `dir()` 164; core `dir()` 153.
Collected test IDs: 6103. Determinism probe (`/tmp/dfcore4a-determinism.py`,
throwaway): `sample(0.3, seed=7)` 302 ids, `randomSplit([0.5, 0.5], seed=7)`
500/500 partitioning `range(1000)`, `sampleBy` 500 ids; full row sets in
`/tmp/dfcore4a-pre.json`, embedded in the four `test_dfcore_4a_*_determinism.py`
pins (one file per function: the 1,302 ids exceed the default ceiling as one
file, so the cohesive split is the sanctioned out).
