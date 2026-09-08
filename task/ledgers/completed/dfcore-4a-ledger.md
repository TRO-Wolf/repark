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
| C-001 | The package export surface equals the pre-slice surface under the DFCORE-1 filter; the unfiltered delta is exactly the gain of `sampling`; nothing lost. | `test_dfcore_1_exports.py::test_package_export_set_unchanged`. | **PROVEN** | Pin 10 passed. `dir()` 164 → 165; gained exactly the eight submodule names (DFCORE-1's four plus DFCORE-2's two plus `statistics` plus `sampling`); lost none. Pre-move provocation reds exactly the declared deltas. |
| C-002 | The core export surface equals the pre-slice surface under the same filter; the unfiltered delta is exactly the `sampling` module binding; `__annotations__` still absent. | `test_dfcore_1_exports.py::test_core_export_set_unchanged`. | **PROVEN** | Pin 10 passed. `dir()` 153 → 154; gained exactly `EXPECTED_NEW_CORE_SUBMODULES`; lost none. |
| C-003 | `DataFrame.__module__`, `__slots__`, slot-only storage, all 38 aliases, the `head` overload pair, and the `stat` accessor are unchanged; `sorted(dir(DataFrame))` loses exactly `_prepare_sample_args`. | Identity/alias/overload/dir pin tests; the T0 freeze test. | **PROVEN** | Pin 10 passed; freeze 4 passed (14 combined). Class `dir()` 251 → 250, non-dunder 221 → 220, lost exactly `_prepare_sample_args`, gained none. `random_split` alias still binds `randomSplit`. |
| C-004 | The three sampling bodies live in `sampling.py` as frame-first module functions; the class keeps one-line wrappers; `_prepare_sample_args` moves verbatim (receiverless, keeps its parameter list); `_coerce_sample_seed` moves and is re-imported by `core` by identity; bodies AST-identical; docstrings byte-identical; stripped-comment reasons live in `dataframe/map.md`. | `test_moved_sampling_helpers_live_in_new_home`; the AST proof; the sampling suites. | **PROVEN** | 288 lines, no row. 3 receivers renamed `self` → `frame`; `frame: DataFrame` annotated (contract). One declared call rewrite: `self._prepare_sample_args(…)` → module-local call. `DataFrame` and `Column` under `TYPE_CHECKING` only. |
| C-005 | `core.py` 5060 → 4819 in `scripts/check_lib_py.py` and the CAP-1 test; the new file carries no row; no sibling ceiling rises; the dataframe `map.md` exact-row sentence stays true. | `make check-lib-py`; the parity harness (holds CAP-1). | **PROVEN** | `make check-lib-py` clean (583 files). `joins_columns.py` 1238 ± 0, `plan_collapse.py` 1168 ± 0, `writer_readwriter.py` 1111 ± 0, all other rows untouched. |
| C-006 | The same seed answers the same rows before and after the move: `sample(0.3, seed=7)`, `randomSplit([0.5, 0.5], seed=7)`, and `sampleBy('k', {…}, seed=7)` on a fixed 1,000-row frame answer the row sets recorded on the pre-slice tree. | `test_dfcore_4a_*_determinism.py` (4 pins). | **PROVEN** | 4 passed on the tip: sample 302 ids, split 500/500 partitioning `range(1000)`, strata 500 ids — all equal to the pre-slice recording. |
| C-007 | Every sampling suite is green; one mutation per moved function reds an existing behavioural pin; collected IDs are preserved plus the five new pin tests. | Gate-list run; M1–M5; `--collect-only` diff. | **PROVEN** | Battery 254 passed; mutations red 5/1/1/1/1 existing pins; collect 6103 → 6108 (+5, all preserved, delta exactly the new pin tests). |
| C-008 | Leaf-before-core import order holds with no cycle; no nested defs; zero new `#` bytes beyond `noqa`; ruff lint and format clean. | `-X importtime`; `make check-python-conventions`; `make py-lint`; `make py-format-check`. | **PROVEN** | `sampling` → `core` with no cycle. Conventions 268 files clean; ruff check and format clean (793 files). The two `#` in the new file are the `noqa: N803` pair both `withReplacement` params need. |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## Evidence

**Counts.** `core.py` 5060 → 4819 (−241 = −246/+5 by numstat: five removed
bodies, three one-line delegations, two bottom import lines, and the two-line
redundant local re-import the move orphaned). New: `sampling.py` 288, under the
default ceiling with no exception row. Pin file 913 → 958 (+45: declared deltas
plus the ownership test). Determinism pins: four files, 333/532/533/534 lines,
one test each.

**Snapshot deltas.** Class loses exactly `_prepare_sample_args` and gains
nothing: `dir(DataFrame)` is 251 → 250 (non-dunder 221 → 220). Package 164 → 165
and core 153 → 154, each gaining exactly `{sampling}` and losing nothing.
`_coerce_sample_seed` stays on both surfaces by identity re-import.

**AST proof** (`/tmp/dfcore4a-ast.py`, throwaway). Normalizations: (a) receiver
`self`/`frame` → `RECV` (every `Name` load); (b) the one declared call rewrite
restored (`self._prepare_sample_args(…)` → module-local `_prepare_sample_args(…)`,
same arguments); (c) signatures compared past the first arg for the three
frame-first functions and whole for the two receiverless helpers, annotations
and defaults included; (d) docstrings compared separately (parsed content plus
raw source modulo the uniform 8→4 dedent). Result: all four bodies IDENTICAL,
all five signatures IDENTICAL, all four module docstrings IDENTICAL (raw shifts
`{0,4}`), all three wrapper docstrings byte-identical, `_coerce_sample_seed`
node IDENTICAL (verbatim move). Zero `self` references remain in `sampling.py`.

**Mutations** (each applied, run, reverted; restore verified by empty `git diff`
on `sampling.py` plus byte-compare against a pristine copy):

| id | home | mutation | reds (existing pins) |
|---|---|---|---|
| M1 | sample | flip the replacement flag (`if not replacement_flag`) | 5 failed: `test_sample_seed_deterministic`, `test_sample_plan_seed_stable`, `test_sample_missing_args_error_class`, `test_sample_positional_fraction_seed_overload`, `test_sample_without_seed_is_action_stable` |
| M2 | split | always take the unseeded `random()` path (drop seed derivation) | 1 failed: `test_random_split_seed_sensitivity` |
| M3 | strata | drop the NaN arm from the fraction guard | 1 failed: `test_stat_sample_by_fraction_range` |
| M4 | normalize | default plan seed 42 → 43 | 1 failed: `test_sample_without_seed_is_action_stable` |
| M5 | coerce | always answer 42 (drop the seed derivation) | 1 failed: `test_sample_positional_fraction_seed_overload` |

**Move-forced mechanical edits** (no behaviour change): (1) the three `frame`
params carry `DataFrame` annotations (the contract requires every parameter
typed; `self` was exempt); (2) `_prepare_sample_args` keeps its parameter list
verbatim (it never
took a receiver, so no rename applies — the ownership pin asserts the exact
list); (3) the `_sample` body calls module-local `_prepare_sample_args` instead
of `self._prepare_sample_args` (the one declared rewrite, same arguments);
(4) the N802 `noqa` marks stay on the camelCase wrappers and drop from the
snake_case module functions, while both `withReplacement` params keep their
N803 `noqa` (the new file's only two `#` bytes); (5) the core bottom block gains
two lines (the `sampling` module binding plus the `_coerce_sample_seed`
re-import — the existing module line is 96 chars and cannot absorb a fourth
name); (6) staying code loses its two-line function-local `IllegalArgumentException`
re-import: the moved bodies held `core`'s last module-scope uses, so the shadowing
local import became an F811 redefinition of an unused global (same object, the
surface import stays).

**Comment census** (R-1). Whole-file `#` lines: pre `core.py` 508 → tip `core.py`
484 + `sampling.py` 2. The core diff removes 26 `#` lines (the 25 stripped
comments, all inside the five moved spans, plus the moved N803 param line) and
adds 2 (the E402 bottom imports). Zero comments deleted from staying code; every
stripped reason is recorded under the `sampling.py` entry in `dataframe/map.md`.
The one stale marker line above `sampleBy`'s local imports (naming validation
that no longer exists at that site) carried no load-bearing reason; the imports
themselves moved with the body.

**Gates** (counts read from pytest summary lines): sampling battery 254 passed;
parity harness 624 passed; facade 5759 passed + 354 skipped (carries the
live-mirror gate); dbt 59 passed + 1 skipped. Static/doc gates
(`check-python-conventions`, `check-docstring-presence`, `make check-lib-py`,
`make py-lint`, `make py-format-check`, `check-map-sync`, `check-ledger-grammar`,
`check-ledgers`, `check-docs-compaction`, `ledger_lifecycle check`) all exit 0.

**Out of scope observed** (pre-existing, the move weakens nothing — no test
removed, no behaviour changed): (a) the stale-native signature mismatch on
arrival (`rows_from_record_batch` absent), resolved by the rebuild, same class as
the DFCORE-1/DFCORE-2/DFCORE-3 lane stales; (b) the CAP-1 prose gate forbids the
`1,000` literal in carrier maps, so the staging row spells the determinism frame
as "thousand-row" — the pins themselves keep the exact `range(1000)` call.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dfcore-4a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Export surfaces compared as recorded pre-slice lists with exact declared deltas; the pin reds pre-move on exactly those deltas. Bodies proven identical by normalized AST; signatures and docstrings byte-identical.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/src/repark/spark/dataframe/sampling.py]
    - id: AT-2
      status: ATTACKED
      evidence: Sample, split, and stratified paths ride the moved bodies; the sampling battery reports 254 passed on the fresh native, and five mutations prove the pins live (5/1/1/1/1 reds).
      artifacts: [python/repark/tests/test_df_easy.py, python/repark/tests/test_dataframe_x3_census.py, python/repark/tests/test_g1_stat_and_expander.py, python/repark/tests/test_g2_window_rand_sampleby.py, python/repark/tests/test_examples_dataframe_c.py, python/repark/tests/test_mapinarrow.py]
    - id: AT-3
      status: ATTACKED
      evidence: All refusal branches (replacement flag, fraction domain, overload shape, seed type, weights shape, fractions map shape and range, stratum and seed types) moved verbatim; M1/M3/M4/M5 prove four refusal-adjacent pins live; the suite pins the rest.
      artifacts: [python/repark/src/repark/spark/dataframe/sampling.py]
    - id: AT-4
      status: N/A
      justification: Move-only relocation of stateless sampling bodies. No shared mutable state, no ordering change, no async spawn, no lock; module imports execute once at load in leaf-before-core order.
    - id: AT-5
      status: N/A
      justification: No privileged action, no auth surface, no secret, no deserialization, no path handling in the moved code; the import graph gains one leaf module with no new dependency.
    - id: AT-6
      status: ATTACKED
      evidence: The module binding surface is the compatibility contract; the pin freezes it (class minus the one declared leaver, modules plus one, ownership by identity). The verbatim receiverless helpers and the F811 local-import removal are the declared deviations.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/src/repark/spark/dataframe/core.py]
    - id: AT-7
      status: N/A
      justification: No new execution path, no new loop, no new allocation shape. Import count grows by one leaf module at package load; per-call cost adds one module-attribute load on sampling calls only. Determinism row sets equal pre and tip.
    - id: AT-8
      status: ATTACKED
      evidence: Names, logic, docstrings, and error texts preserved verbatim; wrappers delegate in one line each; core binds the module plus the identity re-import. Ceilings ratcheted down only.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py, scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: N/A
      justification: No logging, metric, or alarm surface in the moved sampling code; failure messages and exception types are unchanged, so diagnosis paths are unchanged.
    - id: AT-10
      status: ATTACKED
      evidence: The pin fails pre-move on exactly the declared deltas (4 reds) and passes post-move (10 passed). Five mutations red existing behavioural pins. Collect IDs preserved plus the five declared pin tests (6103 -> 6108).
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/tests/test_dfcore_4a_sample_determinism.py, python/repark/tests/test_dfcore_4a_split_left_determinism.py, python/repark/tests/test_dfcore_4a_split_right_determinism.py, python/repark/tests/test_dfcore_4a_sampleby_determinism.py]
  complete: true
```
