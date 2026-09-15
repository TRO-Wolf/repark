# Charter ledger — DF-SURFACE-B-1 · `foreach` / `foreachPartition` / `observe` / `Observation`

**Date:** 2026-09-14 · **Branch:** `feat/df-surface-b-1` · **Base:** `e98e899d`
· **Model:** grok-4.6 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DF-FOREACH-1` and `DF-OBSERVE-1` filed DECLARED.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 1.5 PySpark-parity campaign requires every public name on the PySpark
4.1.2 DataFrame surface to either answer Spark or carry a dated declared refusal with
Spark's own error class. `foreach`, `foreachPartition`, `observe`, and `Observation`
were `_oos` / missing names; this unit lands the driver-side callable path and the
second-pass metrics path.

**Not in this unit:** `freqItems` (DF-FREQITEMS-1); JVM/RDD executor execution of
`foreach`; a CollectMetrics plan node; blocking `Observation.get`.

## PROPOSITION LEDGER — DF-SURFACE-B-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `foreach(f)` returns `None`, calls `f(row)` once per repark `Row` through `toLocalIterator` (never `collect()`), raises `PySparkTypeError` `NOT_CALLABLE` for a non-callable `f` at the call, and lets an exception from `f` propagate unchanged. Registry `DF-FOREACH-1` DECLARED (driver-side execution). | `test_df_surface_b_1.py` foreach pins, cells `foreach_return` / `foreach_not_callable` / `foreach_raises`. | **PROVEN** | Red on the base: `test_foreach_returns_none_and_visits_each_row` FAILED `UnsupportedOperationException: DataFrame.foreach is not supported: foreach is out of scope until the UDF campaign (use collect + Python)`. Green after: visits `[1, 3]`, collect-spy stays 0, `NOT_CALLABLE` at the call, `ZeroDivisionError` propagates. pins: df-surface-b-1/C-001 |
| C-002 | `foreachPartition(f)` returns `None`, calls `f` once per Arrow batch with an iterator of `Row`s, and calls `f` at least once on an empty frame (`f(iter([]))`). Same `NOT_CALLABLE` rule. `DF-FOREACH-1` covers the partition-count difference. | `test_foreach_partition_*` pins, cell `foreachPartition_return`. | **PROVEN** | Red on the base: `test_foreach_partition_returns_none_and_visits_batches` FAILED `UnsupportedOperationException: DataFrame.foreachPartition is not supported: foreachPartition is out of scope until the UDF campaign (use to_arrow / to_polars)`. Green after: two-row frame flattened `[1, 3]`; empty frame `len(calls) >= 1` and every call `[]`. pins: df-surface-b-1/C-002 |
| C-003 | `Observation` constructor and `observe` argument errors match Spark's classes: empty name `VALUE_NOT_NON_EMPTY_STR`; non-str `NOT_STR`; no exprs `CANNOT_BE_EMPTY`; reuse `REUSE_OBSERVATION`; neither Observation nor str `NOT_LIST_OF_COLUMN` (R-3); non-aggregate at first action `INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_ATTRIBUTE` via `_integral` attach helpers. | Constructor / observe error pins, cells `observation_empty_name` / `observation_noname` / `observe_no_exprs` / `observe_obs_reuse_error` / `observe_name_str_type` / `observe_non_agg`. | **PROVEN** | Red on the base: Observation tests `ModuleNotFoundError: No module named 'repark.spark.observation'`; observe tests `PySparkAttributeError: [ATTRIBUTE_NOT_SUPPORTED] Attribute \`observe\` is not supported.` Green after: empty-name and no-exprs cells byte-exact; reuse and non-agg first-line + condition; bad first arg is `PySparkTypeError` `NOT_LIST_OF_COLUMN` (R-3, not Spark's formatter crash). pins: df-surface-b-1/C-003 |
| C-004 | The observed frame keeps source rows/schema; `obs.get` after the first action is the metric dict (`observe_obs` `{'c': 2, 's': 4}`); later actions do not overwrite (exactly one extra `agg`); unaliased names follow the column display name; `get` before any action raises `NO_OBSERVE_BEFORE_GET`. Registry `DF-OBSERVE-1` DECLARED. | Value-path pins, cells `observe_name` / `observe_unaliased` / `observe_obs` / `observe_obs_twice_get`. | **PROVEN** | Red on the base: `observe` `ATTRIBUTE_NOT_SUPPORTED`. Green after: name/unaliased cells match; spy on `DataFrame.agg` records one call across `collect` then `count`; unaliased metric `{'count(1)': 2}`; both never-attached and attached-no-action `get` raise `NO_OBSERVE_BEFORE_GET`. pins: df-surface-b-1/C-004 |
| C-005 | Registry rows `DF-FOREACH-1` and `DF-OBSERVE-1` sit at the end of §5; the fixture copy is byte-identical; example coverage includes the new names; every touched `map.md` is in lockstep; the staging ledger is listed. | The files. | **PROVEN** | `cmp` confirms `python/repark/tests/facade_dataframe_surface_oracle.json` is a byte-identical copy of `/tmp/oc-worker/run15b/oracle/facade_dataframe_surface_oracle.json`. Example `docs/examples/dataframe/foreach_observe.py` covers `DataFrame.foreach` / `foreachPartition` / `observe` / `Observation.get`. Enumerator 948 → 952; inventory 941 → 945 after exclusions. Maps: `dataframe/`, `spark/`, `spark/sql/`, `tests/`, `docs/examples/dataframe/`, `task/ledgers/staging/`. pins: df-surface-b-1/C-005 |
| C-006 | No regression: `test_dataframe*.py`, facade hygiene, frozen DataFrame dir / exports (sorted), the API inventory. | The suites. | **PROVEN** | Frozen DIR gains `foreach`, `foreachPartition`, `observe`, `_observations`; package/core gain `surface_b`. Hygiene OOS pin now calls `foreach` instead of expecting the `_oos` refusal. `core.py` 4044 → 4043 (down-only ratchet). C-006 subset 121 passed. New file 23 passed. Full facade `python/repark/tests` 6408 passed, 368 skipped, 175 failed — all 175 are `array_append`/`array_prepend` on this clone's release native module (ARRAY-NULL-1), none in foreach/observe/hygiene/exports. `python/repark-parity/tests` 757 passed after the new files were tracked. pins: df-surface-b-1/C-006 |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Per-name decisions

| Name | Verdict | Reason |
|---|---|---|
| `foreach` | implemented (declared execution site) | Driver-side `toLocalIterator`; Spark wraps failures in a Py4J abort (`DF-FOREACH-1`). Python is correct: the name runs a user callable. |
| `foreachPartition` | implemented (declared partition count) | One call per Arrow batch, including once on empty; Spark's partition count is RDD-shaped (`DF-FOREACH-1`). Python is correct: the name runs a user callable. |
| `observe` | implemented (declared second pass) | Same rows/schema; metrics via one extra `agg` on first `_action_inner`; str-name has no Python-visible metrics (`DF-OBSERVE-1`). Python is correct: metrics record an aggregation the engine already computes. |
| `Observation` | implemented (declared `get` before action) | Constructor errors match Spark; `get` before an action raises `NO_OBSERVE_BEFORE_GET` instead of blocking (`DF-OBSERVE-1`). |

## Out of scope, observed

- `freqItems` is DF-FREQITEMS-1 on the build clone; not implemented here.
- Spark's `observe_name_str_type` cell is a formatter crash (`AssertionError`); R-3 raises `PySparkTypeError` `NOT_LIST_OF_COLUMN` with Spark's intended params.
- Spark `get` after `observe` with no action blocks forever; that cell was not recorded (`pass` in the oracle script).
