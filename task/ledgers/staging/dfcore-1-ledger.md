# Unit ledger — DFCORE-1 · leaf helpers out of `core.py`

**Date:** 2026-09-07 · **Branch:** `refactor/dfcore-1` · **Base:** `origin/main`
`f464a520` (round 2; round 1 `81e409ea`) · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DFCORE-1` **FIXED**.

## Round 2 — full-suite order dependence + stale native (2026-09-07)

The first full `make py-test-facade` run (which rebuilds the native module via
`maturin develop`) surfaced two actor-side findings, both measured, neither a
regression from the move.

| id | sev | disposition |
|---|---|---|
| R2-S1 | S2 | **FIXED.** The pin's raw `dir()` asserts red in the full suite: `copyreg` memoizes `__slotnames__` on the class at the first pickle or copy of a frame, and the warnings machinery records `__warningregistry__` on the module — both order-dependent dunder state. The pin now asserts the non-dunder surface (dunders stay pinned explicitly where they are contract: `__module__`, `__slots__`, no instance `__dict__`, `__annotations__` absent). Mutation re-proven: dropping one core re-import reds 2 pin tests, restore greens all 6. |
| R2-S2 | S3 | **RESTATED.** The 170 "pre-existing" gate-list failures were the lane venv's stale native module, not suite state: after the facade rebuild the same gate list reports **352 passed**. The round-1 before/after identity proof (identical failure IDs on both trees) stands as the move's no-regression evidence; the failures it compared are gone at the fresh native. |

C-001 / C-002 / C-003 re-**PROVEN** below against the order-robust pin: 45-file
pre-pin set **777 passed, 15 skipped**; gate list **352 passed**; collect-only
6099 IDs still identical.

---

**Date:** 2026-09-07 · **Branch:** `refactor/dfcore-1` · **Base:** `origin/main`
`f464a520` (PR #413, the plan) · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DFCORE-1` **FIXED**.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** `core.py` is 6,302 lines with all leaf helpers above the class. The
decomposition plan (`task/roadmap/epic-term/dataframe-core-decomposition-plan-2026-09-07.md`)
opens the slate with this move-only slice: the helpers relocate to their owning leaf
modules, `core.py` re-exports every moved name, and an export-snapshot pin proves the
package surface did not move.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.lock`,
dependency lists, completed ledgers, any behaviour change (DFCORE-5/6 own the perf work),
the pre-existing `RecursionError` in the join/udf suites (identical before/after, recorded
under C-008, not repaired here).

## PROPOSITION LEDGER — DFCORE-1 — 2026-09-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The package export surface `sorted(dir(repark.spark.dataframe))`, recorded pre-slice (157 names), equals the post-slice surface minus interpreter dunders and import-system submodule attributes. | `test_dfcore_1_exports.py::test_package_export_set_unchanged`. | **PROVEN** | 6 passed. Filtered surfaces equal. Accepted unfiltered delta, asserted exactly: the four new submodule attributes `export_errors`, `grouped_udf`, `rows_export`, `udf_schema`; nothing lost. |
| C-002 | The core export surface `sorted(dir(repark.spark.dataframe.core))`, recorded pre-slice (151 names), equals the post-slice surface under the same filter. | `test_dfcore_1_exports.py::test_core_export_set_unchanged`. | **PROVEN** | 6 passed. Filtered surfaces equal. Accepted unfiltered delta, asserted exactly: `__annotations__` left with the last annotated module-level assignments; nothing gained. |
| C-003 | `DataFrame.__module__`, `__slots__`, slot-only storage (no instance `__dict__`), all 38 alias bindings, the `head` overload pair, and `sorted(dir(DataFrame))` (255 names) are unchanged. | `test_dfcore_1_exports.py` identity/alias/overload/dir tests; the T0 freeze test. | **PROVEN** | Pin 6 passed; `test_t0_df_regions_import_freeze.py` 4 passed (10 passed combined). `__module__` still `repark.spark.dataframe.core`; slots tuple byte-identical. |
| C-004 | `_arrow_map_pairs`, `_arrow_cell_to_spark_python`, `_refuse_calendar_interval_python_value` live in `rows_export.py` with names and signatures; `core.py` re-exports them; `rows_export.py` drops its `core` imports; empty maps still become `{}`. | The move; `test_mapinarrow.py`, `test_dataframe_actions.py` collect paths; before/after failure-set diff. | **PROVEN** | Bodies byte-identical except the one stripped comment, whose reason now lives in `dataframe/map.md` (R-1); docstrings moved unchanged (R-2). Map/interval collect behaviour covered by the identical 170-failure/182-pass before/after sets. |
| C-005 | `_export_error_message_is_noise`, `_export_error_message`, `_export_engine_error` and both marker constants live in new `export_errors.py`; error classes, chaining, and the memory advice text unchanged. | The move; `test_t2_sort_memory.py` noise-strip pins; before/after failure-set diff. | **PROVEN** | Bodies byte-identical except stripped comments, whose reasons now live in `dataframe/map.md` (R-1); docstrings moved unchanged (R-2). `test_t2_sort_memory.py` imports still resolve through the package. |
| C-006 | `_coerce_map_in_arrow_schema` and `_validate_map_in_arrow_batch` live in new `udf_schema.py`; the sentinel and six grouped-UDF helpers live in new `grouped_udf.py`; `joins_columns.py` imports them directly, not through `core`; empty groups, NaN keys, and batch-boundary stitching unchanged. | The move; `test_applyinpandas.py`, `test_pandas_udf.py`, `test_mapinarrow.py`; `udtf.py` package import; before/after failure-set diff. | **PROVEN** | Bodies byte-identical except stripped comments, whose reasons now live in `dataframe/map.md` (R-1); docstrings moved unchanged (R-2). `grouped_udf` arrives via a module import with qualified call sites: the canonical two-name from-import costs two lines the exact ceiling cannot spare (sibling ceilings never rise). |
| C-007 | `core.py`'s exact ceiling reads the new count in `scripts/check_lib_py.py` and the CAP-1 test; no sibling ceiling rises; new files carry no row; the dataframe `map.md` exact-row sentence stays true. | `make check-lib-py`; the parity harness (holds CAP-1). | **PROVEN** | `core.py` 6302 → 5954; `joins_columns.py` 1239 → 1238 (downward ratchet, the sanctioned direction). `make check-lib-py` clean; parity harness **624 passed**. |
| C-008 | Every collected test ID stays under its current ID (pure relocation); the pre-existing suite failures are byte-identical before and after. | `--collect-only` diff; gate-list failure-set diff on base vs changed tree. | **PROVEN** | Collect-only: 6099 IDs, empty diff. Gate list: `170 failed, 182 passed` on both trees with identical failure IDs (pre-existing `RecursionError` in `__getattr__`/`columns`, untouched by this slice). |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dfcore-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked against the diff. Export surfaces compared as recorded pre-slice lists, not paraphrases; the two mechanical deltas (submodule attrs, __annotations__) are asserted exactly, not absorbed.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/src/repark/spark/dataframe/map.md]
    - id: AT-2
      status: ATTACKED
      evidence: Empty maps ({} not []), empty groups, NaN keys, empty wrong-name frames, and batch-boundary stitching ride the moved bodies byte-identically; the suites exercising them report identical before/after sets.
      artifacts: [python/repark/src/repark/spark/dataframe/grouped_udf.py, python/repark/src/repark/spark/dataframe/rows_export.py]
    - id: AT-3
      status: ATTACKED
      evidence: Export-error mapping (noise filter, longest-candidate choice, External error shell strip, memory advice) moved verbatim; error classes and chaining preserved; t2 sort-memory pins still resolve through the package.
      artifacts: [python/repark/src/repark/spark/dataframe/export_errors.py, python/repark/tests/test_t2_sort_memory.py]
    - id: AT-4
      status: N/A
      justification: Move-only relocation of pure module-level helpers. No shared mutable state, no ordering change, no async spawn, no lock; re-export imports execute once at module load.
    - id: AT-5
      status: N/A
      justification: No privileged action, no auth surface, no secret, no deserialization, no path handling in the moved code; import graph gains leaf modules with no new dependency.
    - id: AT-6
      status: ATTACKED
      evidence: The package re-export surface is the compatibility contract; the pin freezes it (157 + 151 names, 255 class attributes, 38 aliases, overloads). udtf.py and the test-suite package imports resolve unchanged.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/src/repark/spark/udtf.py]
    - id: AT-7
      status: N/A
      justification: No new execution path, no new loop, no new allocation shape. Import count grows by four leaf modules at package load; nothing system-breaking.
    - id: AT-8
      status: ATTACKED
      evidence: Names, signatures, docstrings, and constants preserved verbatim; core re-imports every moved private name; PySparkNotImplementedError import kept because it is part of the export surface. Ceilings ratcheted down only.
      artifacts: [python/repark/src/repark/spark/dataframe/core.py, scripts/check_lib_py.py]
    - id: AT-9
      status: N/A
      justification: No logging, metric, or alarm surface in the moved helpers; failure messages and exception types are unchanged, so diagnosis paths are unchanged.
    - id: AT-10
      status: ATTACKED
      evidence: The pin fails on a dropped re-export (filtered surface mismatch) and on any alias/overload/slot drift. Mutation check: removing one core re-import reds the pin; the pre-existing 170 failures are proven unrelated by identical base/changed failure IDs.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py]
  complete: true
```
