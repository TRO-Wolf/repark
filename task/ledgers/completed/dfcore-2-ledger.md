# Unit ledger — DFCORE-2 · the four UDF projection rewrites out of `core.py`

**Date:** 2026-09-07 · **Branch:** `refactor/dfcore-2` · **Base:** `origin/main`
`c292b996` (PR #414, DFCORE-1) · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DFCORE-2` **FIXED**.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** The decomposition plan
(`task/roadmap/epic-term/dataframe-core-decomposition-plan-2026-09-07.md`, §4 row DFCORE-2)
opens the second slice: the four scalar / classic / window UDF projection rewrites (677
method lines) leave `core.py` for two leaf modules, `select` delegates, and an extended
export pin proves the surface moved exactly as declared.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.lock`,
dependency lists, completed ledgers, any behaviour change, the two coverage gaps the
mutations surfaced (recorded under "Out of scope observed", DFCORE-1-F1 class, not repaired
here).

**Environment.** The lane native was stale on arrival: the gate list failed 196 tests with
`RecursionError` on both trees (byte-identical failure IDs pre/post — see C-007). Rebuilt via
`make develop` (exit 0); the same gate list then reports 331 passed. This mirrors DFCORE-1
round 2 (R2-S2).

## PROPOSITION LEDGER — DFCORE-2 — 2026-09-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The package export surface equals the pre-slice surface under the DFCORE-1 filter; the unfiltered delta is exactly the gain of `udf_projection` and `udf_window_projection`; nothing lost. | `test_dfcore_1_exports.py::test_package_export_set_unchanged`. | **PROVEN** | Pin 8 passed. `dir()` 161 → 163; gained exactly the six submodule names (DFCORE-1's four plus the two new); lost none. Pre-move provocation reds with exactly the two missing names. |
| C-002 | The core export surface equals the pre-slice surface under the same filter; the unfiltered delta is exactly the two new module bindings; `__annotations__` still absent. | `test_dfcore_1_exports.py::test_core_export_set_unchanged`. | **PROVEN** | Pin 8 passed. `dir()` 150 → 152; gained exactly `EXPECTED_NEW_CORE_SUBMODULES`; lost none. |
| C-003 | `DataFrame.__module__`, `__slots__`, slot-only storage, all 38 aliases, the `head` overload pair are unchanged; `sorted(dir(DataFrame))` shrinks by exactly the four moved names. | Identity/alias/overload/dir pin tests; the T0 freeze test. | **PROVEN** | Pin 8 passed; freeze 4 passed (12 combined). Class `dir()` 255 → 251, non-dunder 225 → 221. |
| C-004 | `_select_with_pandas_udfs` and `_select_with_python_udfs` live in `udf_projection.py` as frame-first module functions; `select` delegates one line each; bodies AST-identical; docstrings byte-identical; stripped-comment reasons live in `dataframe/map.md`. | `test_moved_select_helpers_live_in_new_homes`; the AST proof; the UDF suites. | **PROVEN** | 349 lines, no row. 30 receivers renamed `self` → `frame`; `frame: DataFrame` annotated (contract). One cross-module delegation; `DataFrame` under `TYPE_CHECKING` only. |
| C-005 | The two window variants live in `udf_window_projection.py` under the same guarantees; `udf_bridge.py` keeps callback execution untouched. | Ownership test; the AST proof; the window suites. | **PROVEN** | 337 lines, no row. The ordered dispatch arrives as a same-module call with `frame` first. `udf_bridge.py` has no diff. |
| C-006 | `core.py` 5954 → 5263 in `scripts/check_lib_py.py` and the CAP-1 test; new files carry no row; no sibling ceiling rises; the dataframe `map.md` exact-row sentence stays true. | `make check-lib-py`; the parity harness (holds CAP-1). | **PROVEN** | `make check-lib-py` clean (577 files). `functions_udf.py` 1300 ± 0, `test_pandas_udf.py` 1478 ± 0, all other rows untouched. |
| C-007 | Every UDF suite is green; one mutation per moved function reds an existing behavioural pin; collected IDs are preserved plus the one new pin test; the stale-native failure sets are byte-identical pre/post. | Gate-list run; UDF-grep suites; M1/M2b/M3/M4b; `--collect-only` diff. | **PROVEN** | Gate list 331 passed; UDF-grep 100 passed + 2 JVM-oracle skips; mutations red 5/1/1/2 existing pins; collect 6101 → 6102 (+1, all preserved); stale sets 196 == 196. |
| C-008 | Leaf-before-core import order holds with no cycle; no nested defs; zero `#` bytes in the new files; ruff lint and format clean. | `-X importtime`; `make check-python-conventions`; `make py-lint`; `make py-format-check`. | **PROVEN** | `udf_window_projection` → `udf_projection` → `core`. Conventions 266 files clean; ruff check and format clean (787 files). |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## Evidence

**Counts.** `core.py` 5954 → 5263 (−691 = −692 for the moved span plus one bottom
module-import line). New: `udf_projection.py` 349, `udf_window_projection.py` 337, both
under the default ceiling with no exception row. Pin file 833 → 870 (+37: declared
deltas plus the ownership test).

**Snapshot deltas.** Class loses exactly
`{_select_with_ordered_window_pandas_udfs, _select_with_pandas_udfs,
_select_with_python_udfs, _select_with_window_pandas_udfs}`. Package and core each gain
exactly `{udf_projection, udf_window_projection}` and lose nothing.

**AST proof** (`/tmp/dfcore2-ast.py`, throwaway). Normalizations: (a) receiver
`self`/`frame` → `RECV` (first arg plus every `Name` load); (b) the new first-arg
`DataFrame` annotation stripped; (c) the two delegation rewrites restored to method-call
shape; (d) docstrings compared separately (cleaned content plus raw source modulo the
uniform 4-space dedent). Result: all four bodies IDENTICAL, all four docstrings
IDENTICAL. The ruff-format joins (one boolean-operand join, one implicit f-string join)
are parser-invisible, confirmed by the identical dumps. The two declared rewrites:

- `return self._select_with_window_pandas_udfs(items)` →
  `return udf_window_projection._select_with_window_pandas_udfs(frame, items)`
- `return self._select_with_ordered_window_pandas_udfs(items=..., ...)` →
  `return _select_with_ordered_window_pandas_udfs(frame, items=..., ...)`

**Mutations** (each applied, run, reverted; restore verified by `diff -q` against
pristine copies plus a remnant grep with exit 1):

| id | home | mutation | reds (existing pins) |
|---|---|---|---|
| M1 | pandas | `needs_scalar_iter = False` | 5 failed: the `scalar_iter_basic`, `multi_arg`, `with_pass_through`, `wrong_batch_count_loud`, `dual_udf_independent_streams` pins |
| M2 | python | skip the generator-input refusal | none in `test_udf.py`, `test_explode_rewrite.py`, `test_pandas_udf.py` (87 + 123 passed) — pre-existing gap, see below |
| M2b | python | skip the aggregate-input refusal | 1 failed: `test_udf_aggregate_input_refused` |
| M3 | window | drop the last-wins dedup | 1 failed: `test_pandas_udf_window_select_alias_overwrites_source_column` |
| M4 | ordered | `order_cols=[]` | none (7 passed) — test input is pre-sorted, see below |
| M4b | ordered | `start_bound=None, end_bound=None` | 2 failed: `test_pandas_udf_window_order_by_default_frame`, `test_pandas_udf_window_rows_between_duck_typed` |

**Move-forced mechanical edits** (no behaviour change): (1) `test_pandas_udf.py`'s
plan-time import pin reads the moved helper's source (2 lines swapped, count 1478
unchanged); (2) the `pandas_udf` docstring cross-reference follows the rewrite to
`repark.dataframe.udf_projection` (1 line, 95 chars, count 1300 unchanged); (3) ruff
format dedents the staying `# Cache and persist.` comment 8 → 4 (the pre-move file is
format-clean, so the position change forced it; wording untouched); (4) the four `frame`
params carry `DataFrame` annotations (the contract requires every parameter typed; `self`
was exempt).

**Out of scope observed** (pre-existing, the move weakens nothing — no test removed, no
behaviour changed): (a) the classic-UDF generator-input refusal ("unnest first") is
pinned nowhere — M2 stays green across the UDF and explode suites; (b) the ordered-window
sort is unobserved — M4 stays green because the ordered pins feed pre-sorted input, so no
existing pin proves `order_cols` carries; (c) the stale-native `RecursionError` mechanism:
a property getter raising `AttributeError` inside re-invokes `__getattr__`, which reads
`self.columns` — resolved by the rebuild, identical on both trees before it.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dfcore-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Export surfaces compared as recorded pre-slice lists with exact declared deltas; the pin reds pre-move on exactly those deltas. Bodies proven identical by normalized AST; docstrings byte-identical.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/src/repark/spark/dataframe/udf_projection.py, python/repark/src/repark/spark/dataframe/udf_window_projection.py]
    - id: AT-2
      status: ATTACKED
      evidence: Scalar, scalar-iter, classic, unbounded-window, and ordered-window paths ride the moved bodies; the UDF suites report green on the fresh native, and four mutations prove the pins live (5/1/1/2 reds).
      artifacts: [python/repark/tests/test_udf.py, python/repark/tests/test_pandas_udf.py, python/repark/tests/test_mapinarrow.py, python/repark/tests/test_applyinpandas.py]
    - id: AT-3
      status: ATTACKED
      evidence: All refusal branches (mix, aggregate/generator input, duplicate names, hostile return types, window-shape mismatches) moved verbatim; M2b and M3 prove two refusal pins live; the suite pins the rest.
      artifacts: [python/repark/src/repark/spark/dataframe/udf_projection.py, python/repark/src/repark/spark/dataframe/udf_window_projection.py]
    - id: AT-4
      status: N/A
      justification: Move-only relocation of pure projection rewrites. No shared mutable state, no ordering change, no async spawn, no lock; module imports execute once at load in leaf-before-core order.
    - id: AT-5
      status: N/A
      justification: No privileged action, no auth surface, no secret, no deserialization, no path handling in the moved code; the import graph gains two leaf modules with no new dependency.
    - id: AT-6
      status: ATTACKED
      evidence: The module binding surface is the compatibility contract; the pin freezes it (class minus four, modules plus two, ownership by identity). The source-inspecting test and the doc cross-reference follow the move.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/tests/test_pandas_udf.py, python/repark/src/repark/spark/functions_udf.py]
    - id: AT-7
      status: N/A
      justification: No new execution path, no new loop, no new allocation shape. Import count grows by two leaf modules at package load; per-call cost adds one module-attribute load on UDF selects only.
    - id: AT-8
      status: ATTACKED
      evidence: Names, logic, docstrings, and error texts preserved verbatim; select keeps one-line delegations; core binds only the two modules. Ceilings ratcheted down only; line-neutral edits hold their rows.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py, scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: N/A
      justification: No logging, metric, or alarm surface in the moved rewrites; failure messages and exception types are unchanged, so diagnosis paths are unchanged.
    - id: AT-10
      status: ATTACKED
      evidence: The pin fails pre-move on exactly the declared deltas (3 reds) and passes post-move (8 passed). Four mutations red existing behavioural pins. The stale-native failure sets are byte-identical pre/post (196 == 196).
      artifacts: [python/repark/tests/test_dfcore_1_exports.py]
  complete: true
```
