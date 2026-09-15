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

**Ruling R-5 (orchestrator, binding, 2026-09-15):** the Observation belongs to the
frame `observe()` returned; its metrics are ONE extra aggregation over THAT frame —
never the actioned descendant, never a `limit(n)` frame. Children carry a reference
to the same attachment object so a descendant's action can trigger it; the first
row-producing action on the observed frame or any descendant fills it exactly once.

## PROPOSITION LEDGER — DF-SURFACE-B-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `foreach(f)` returns `None`, calls `f(row)` once per repark `Row` through `toLocalIterator` (never `collect()`), raises `PySparkTypeError` `NOT_CALLABLE` for a non-callable `f` at the call, and lets an exception from `f` propagate unchanged. Registry `DF-FOREACH-1` DECLARED (driver-side execution). | `test_df_surface_b_1.py` foreach pins, cells `foreach_return` / `foreach_not_callable` / `foreach_raises`. | **PROVEN** | Red on the base: `test_foreach_returns_none_and_visits_each_row` FAILED `UnsupportedOperationException: DataFrame.foreach is not supported: foreach is out of scope until the UDF campaign (use collect + Python)`. Green after: visits `[1, 3]`, collect-spy stays 0, `NOT_CALLABLE` at the call, `ZeroDivisionError` propagates. pins: df-surface-b-1/C-001 |
| C-002 | `foreachPartition(f)` returns `None`, calls `f` once per Arrow batch with an iterator of `Row`s, and calls `f` at least once on an empty frame (`f(iter([]))`). Same `NOT_CALLABLE` rule. `DF-FOREACH-1` covers the partition-count difference. | `test_foreach_partition_*` pins, cell `foreachPartition_return`. | **PROVEN** | Red on the base: `test_foreach_partition_returns_none_and_visits_batches` FAILED `UnsupportedOperationException: DataFrame.foreachPartition is not supported: foreachPartition is out of scope until the UDF campaign (use to_arrow / to_polars)`. Green after: two-row frame flattened `[1, 3]`; empty frame `len(calls) >= 1` and every call `[]`. pins: df-surface-b-1/C-002 |
| C-003 | `Observation` constructor and `observe` argument errors match Spark's classes: empty name `VALUE_NOT_NON_EMPTY_STR`; non-str `NOT_STR`; no exprs `CANNOT_BE_EMPTY`; reuse `REUSE_OBSERVATION`; neither Observation nor str `NOT_LIST_OF_COLUMN` (R-3); non-aggregate at first action `INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_ATTRIBUTE` via `_integral` attach helpers. | Constructor / observe error pins, cells `observation_empty_name` / `observation_noname` / `observe_no_exprs` / `observe_obs_reuse_error` / `observe_name_str_type` / `observe_non_agg`. | **PROVEN** | Red on the base: Observation tests `ModuleNotFoundError: No module named 'repark.spark.observation'`; observe tests `PySparkAttributeError: [ATTRIBUTE_NOT_SUPPORTED] Attribute \`observe\` is not supported.` Green after: empty-name and no-exprs cells byte-exact; reuse and non-agg first-line + condition; bad first arg is `PySparkTypeError` `NOT_LIST_OF_COLUMN` (R-3, not Spark's formatter crash). pins: df-surface-b-1/C-003 |
| C-004 | The observed frame keeps source rows/schema; `obs.get` after the first action is the metric dict (`observe_obs` `{'c': 2, 's': 4}`); later actions do not overwrite (exactly one extra `agg`); unaliased names follow the column display name; `get` before any action raises `NO_OBSERVE_BEFORE_GET`. Registry `DF-OBSERVE-1` DECLARED. | Value-path pins, cells `observe_name` / `observe_unaliased` / `observe_obs` / `observe_obs_twice_get`. | **PROVEN** | Red on the base: `observe` `ATTRIBUTE_NOT_SUPPORTED`. Green after: name/unaliased cells match; spy on `DataFrame.agg` records one call across `collect` then `count`; unaliased metric `{'count(1)': 2}`; both never-attached and attached-no-action `get` raise `NO_OBSERVE_BEFORE_GET`. pins: df-surface-b-1/C-004 |
| C-005 | Registry rows `DF-FOREACH-1` and `DF-OBSERVE-1` sit at the end of §5; the fixture copy is byte-identical; example coverage includes the new names; every touched `map.md` is in lockstep; the staging ledger is listed. | The files. | **PROVEN** | `cmp` confirms `python/repark/tests/facade_dataframe_surface_oracle.json` is a byte-identical copy of `/tmp/oc-worker/run15b/oracle/facade_dataframe_surface_oracle.json`. Example `docs/examples/dataframe/foreach_observe.py` covers `DataFrame.foreach` / `foreachPartition` / `observe` / `Observation.get`. Enumerator 948 → 952; inventory 941 → 945 after exclusions. Maps: `dataframe/`, `spark/`, `spark/sql/`, `tests/`, `docs/examples/dataframe/`, `task/ledgers/staging/`. pins: df-surface-b-1/C-005 |
| C-006 | No regression: `test_dataframe*.py`, facade hygiene, frozen DataFrame dir / exports (sorted), the API inventory. | The suites. | **PROVEN** | Frozen DIR gains `foreach`, `foreachPartition`, `observe`, `_observations`; package/core gain `surface_b`. Hygiene OOS pin now calls `foreach` instead of expecting the `_oos` refusal. `core.py` 4044 → 4043 (down-only ratchet). C-006 subset 121 passed. New file 23 passed. Full facade `python/repark/tests` 6408 passed, 368 skipped, 175 failed — all 175 are `array_append`/`array_prepend` on this clone's release native module (ARRAY-NULL-1), none in foreach/observe/hygiene/exports. `python/repark-parity/tests` 757 passed after the new files were tracked. pins: df-surface-b-1/C-006 |
| C-007 | Critic round 1 (L-001..L-007) under ruling R-5: the Observation belongs to the frame `observe()` returned; its metrics are ONE extra `agg(*exprs)` over THAT frame, triggered by the first row-producing action on it or any descendant; later actions never overwrite; the process-global `_FILL_DEPTH` becomes a per-attachment lock + in-progress flag; literal metrics are allowed; non-`Column` exprs refuse `NOT_LIST_OF_COLUMN` `{"arg_name": "exprs"}` at `observe`. | New pins in `test_df_surface_b_1.py` naming each finding; all red before the fix. | **PROVEN** | Red on 7d017a72: 13 of 14 new pins fail on the first run (`take`/`head`/`first`/`isEmpty`/`show` fill `{'c': 1, 's': 1}` not `{'c': 2, 's': 4}`; filter/union/limit/select/drop descendants aggregate the transformed frame; mapInArrow `take` never fills; literal `lit(42)` refused; `"count(1)"` accepted at `observe`; the old spy could not go red on peek paths); `test_observe_concurrent_fills_are_independent` is flaky-red (standalone run: `NO_OBSERVE_BEFORE_GET` on the losing thread, matching L-004's 20-repeat shape). Green after: `_ObservedMetricsAttachment` carries `(observation, exprs, observed_frame)` shared by `_spawn` children (both parents' attachments merge); `fill_on_action` is the single helper, invoked from `_action_inner`, `_consume_map_in_arrow_batches` (covers `take`/`isEmpty`/`show`/`repr`/`_repr_html` bridge peeks), and `take`'s `take(0)` early return; literal-only metric lists get a uuid-named `count(lit(1))` sentinel expr popped from the result so `agg` always has one aggregate and even an empty observed frame fills literal values; core.py stays 4034 (the bridge-peek predicate deduped to `display._use_bridge_peek`). pins: df-surface-b-1/C-007 |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

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

## Orchestrator fix after the re-check (run 15b, G-2)

- R-6 (2026-09-15): the Grok re-check marked L-001..L-007 FIXED and found L-101 (P1: `explain` filled the Observation through the
  scratch temp view and the EXPLAIN plan's iteration) and L-102 (P2: `createOrReplaceTempView` filled). The orchestrator fixed both
  red-first with a thread-local fill suppression around plan-only work (`register_view_without_fill`, `rows_without_fill`), and made
  L-103's `tail(0)` fill like `take(0)`; writers register through the session method directly and still fill. L-104 (P3: two unaliased
  `lit(1)` metrics collide on the display name `"1"`) is recorded, not fixed. `core.py` swaps three call sites line for line.

| Clause | Proposition | Proof | Verdict | Evidence |
|---|---|---|---|---|
| C-008 | Plan-only work never fills an Observation: `explain()`, `explain(True)` and `createOrReplaceTempView` leave `get` raising; `tail(0)` and a parquet write fill it. | `test_observe_explain_and_temp_view_do_not_fill`. | **PROVEN** | Red on 8dc64ae1 (1 failed: `explain()` filled the Observation), green after with the unit, frozen-surface and hygiene pins and every explain / temp_view / tail / writer / cte pin in the facade suite. pins: df-surface-b-1/C-008 |
- R-7 (2026-09-15): no further critic round — the re-check's P1 and P2 were fixed red-first by the orchestrator and are held by
  `test_observe_explain_and_temp_view_do_not_fill` plus 245 explain, temp-view, tail, writer and CTE pins in the facade suite.
- R-8 (2026-09-15): the S2-21 perf reviewers do not run on this unit — foreach/foreachPartition stream the existing iterators and
  observe adds one aggregation per Observation by contract (DF-OBSERVE-1); no new data path.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: df-surface-b-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the card and the recorded PySpark 4.1.2 foreach, observe and observation cells; freqItems is split out to a Rust unit and listed as moved.
      artifacts: [python/repark/tests/test_df_surface_b_1.py, python/repark/tests/facade_dataframe_surface_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Every row-producing action and descendant (take, head, first, isEmpty, show in both styles, tail(0), filter, union, limit, select, drop, mapInArrow take, writes), plan-only work (explain, createOrReplaceTempView), two threads, literal and mixed metrics, non-Column exprs, reuse, an empty name, get before an action.
      artifacts: [python/repark/tests/test_df_surface_b_1.py]
    - id: AT-3
      status: N/A
      justification: Python facade plumbing; no Rust, no unwrap.
    - id: AT-4
      status: ATTACKED
      evidence: One attachment per Observation with a lock and an in-progress flag; a thread-local suppression for plan-only work; the concurrency pin passed 15 of 15 standalone runs.
      artifacts: [python/repark/src/repark/spark/dataframe/surface_b.py]
    - id: AT-5
      status: N/A
      justification: No authn/authz, deserialization, path, credential or network surface; foreach runs the user's own callable on the driver.
    - id: AT-6
      status: ATTACKED
      evidence: Grok critic-logic round 1 (3 P1, 4 P2) led to ruling R-5 and a Devin round; the re-check found every finding fixed plus L-101 (P1) and L-102 (P2), fixed red-first by the orchestrator (R-6).
      artifacts: [task/ledgers/completed/df-surface-b-1-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: Full facade suite (6583 passed on a fresh native before the re-check fix) and the parity suite; after the fix the unit, frozen-surface, hygiene and 245 related pins; ruff 0.15.22, check_lib_py with core.py at 4034, ledger grammar, example coverage; comment-ban grep zero hits.
      artifacts: [docs/examples/dataframe/foreach_observe.py]
    - id: AT-8
      status: ATTACKED
      evidence: Spark's contracts were read, not assumed - pyspark/sql/observation.py, classic dataframe.py foreach, foreachPartition and observe argument checks (1135-1169), and the CollectMetrics plan position behind the descendant rule.
      artifacts: [python/repark/src/repark/spark/observation.py, python/repark/src/repark/spark/dataframe/surface_b.py]
    - id: AT-9
      status: ATTACKED
      evidence: Refusals carry Spark's classes and texts (NOT_CALLABLE, CANNOT_BE_EMPTY, REUSE_OBSERVATION, NOT_LIST_OF_COLUMN, VALUE_NOT_NON_EMPTY_STR, INVALID_OBSERVED_METRICS, NO_OBSERVE_BEFORE_GET); the declared differences are DF-FOREACH-1 and DF-OBSERVE-1 with pins.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation guards run - aggregating the action frame reds the take and descendant pins, a global depth counter reds the concurrency pin, removing the suppression reds the explain pin, the spy pin counts exactly one aggregation.
      artifacts: [python/repark/tests/test_df_surface_b_1.py]
  complete: true
```

