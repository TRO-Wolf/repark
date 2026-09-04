# Charter ledger — WIN-SLIDE-1 · the thirteen aggregates that refuse over a sliding frame

**Date:** 2026-09-04 · **Branch:** `feat/win-slide-1` · **Base:** `origin/main`
`55652ca` · **Model:** opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** thirteen `WIN-SLIDE-*` rows BACKLOG → **FIXED**; `WIN-RANGE-DF-1` and
`WIN-COLLECT-DOOR-1` filed and FIXED in the same diff; `WIN-SLIDE-PCT-ACC-1` filed BACKLOG.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** W-0 measured thirteen Spark 4.1.2 built-in aggregates that plan as window functions
and then refuse at execution — `Aggregate can not be used as a sliding accumulator because
retract_batch is not implemented`. Spark accepts every one and re-scans the frame. W-1 was
chartered to pick the fallback. This unit picks it, builds it and pins it.

**Not in this unit:** the non-window aggregate paths (no group-by spelling changes); a
Greenwald-Khanna sketch for `percentile_approx` (`WIN-SLIDE-PCT-ACC-1`, `PERF-APPROXPCT-1`); the
segment-tree fallback W-1 named as the other candidate (rejected on measurement, §7); any
DataFusion fork change.

## PROPOSITION LEDGER — WIN-SLIDE-1 — 2026-09-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every one of the thirteen answers Spark-equal over a sliding frame on the **SQL door**, on at least a ROWS frame with both bounds, a RANGE frame, a frame with NULLs in the column, an empty frame, and a partition boundary. | `test_win_slide_1.py::test_sql_door_matches_the_spark_pin`, 65 cells, red on the base. | **PROVEN** | 13 aggregates x 5 shapes. All 65 raised the sliding-accumulator refusal on the base build; all 65 green after. Goldens recorded from live PySpark 4.1.2 on 2026-09-04. pins: win-slide-1/C-001 |
| C-002 | The same thirteen answer the same columns on the **DataFrame door** through `over(Window.…rowsBetween/rangeBetween)`. | `test_win_slide_1.py::test_dataframe_door_matches_the_spark_pin`, 65 cells. | **PROVEN** | 55 of the 65 red on the base (the 10 `bool_and`/`bool_or` cells were already green — that door is a `min`/`max` shim, not the UDAF), all 65 green after. Two door bugs had to be fixed to get there: `WIN-COLLECT-DOOR-1` and `WIN-RANGE-DF-1` (C-003). pins: win-slide-1/C-002 |
| C-003 | The RANGE-frame cells are a real value range on both doors, not a silently widened frame. | The `[range_frame-*]` cells; the `WIN-RANGE-DF-1` measurement. | **PROVEN** | `Column.over` emitted the RANGE offset as `Int64`; DataFusion's coercion passes a non-`Utf8`, non-null scalar through untouched and a bound whose type does not match the ORDER BY key degrades to UNBOUNDED PRECEDING, so `rangeBetween(-2, 0)` over an `IntegerType` or `DoubleType` key was **cumulative**. Measured on `sum(v)` over 1..6: `df 1, 3, 6, 10, 15, 21` vs `sql 1, 3, 6, 9, 12, 15`; BIGINT keys were already correct. Fixed by emitting `Utf8`, the shape DataFusion's own SQL planner produces. pins: win-slide-1/C-003 |
| C-004 | The named per-aggregate contracts hold, Spark-measured: `collect_list` preserves frame order, `collect_set` is the frame multiset, `try_sum` answers NULL for a row whose frame overflows, an empty frame answers Spark's empty-frame value, and `CURRENT ROW … UNBOUNDED FOLLOWING` (also a sliding frame) answers. | The five dedicated pins. | **PROVEN** | `collect_list` frame order pinned on the `2 PRECEDING` and the `UNBOUNDED FOLLOWING` frames; `collect_set` compared as a sorted multiset (Spark leaves the order unspecified); `try_sum(ov)` over BIGINT at `Long.MaxValue` answers `[…, None, None, …]` exactly where Spark does; empty frame answers `[]` for `collect_list`/`collect_set` and `0` for `approx_count_distinct`, both Spark-measured. pins: win-slide-1/C-004 |
| C-005 | The fallback is **reusable by capability, not by name**: an aggregate the rule has never heard of, with no `retract_batch`, gets it automatically. | `crates/repark-core/src/session/tests/window_rescan.rs`. | **PROVEN** | A throwaway `winslide_probe_sum` UDAF registered inside the test answers `1, 3, 2, 4` over `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW` and plans as `rescan:winslide_probe_sum`. Its empty frame answers NULL (the fresh accumulator), not its `default_value` sentinel `-1.0`. `FILTER (WHERE v > 1.5)` answers the masked frame. pins: win-slide-1/C-005 |
| C-006 | **No regression on the retractable path.** Aggregates that can retract keep DataFusion's sliding accumulator, and the whole window suite plus the whole facade suite stay green. | The plan pins; the suites. | **PROVEN** | `sum` over the same frame plans as `aggregate:sum`, and an ever-expanding frame is never rewritten. A mid-build mutation proved the pin is live: probing `accumulator` instead of `create_sliding_accumulator` rewrote `sum` too and the pin reded. Window suites 102 passed; full facade suite **4738 passed, 193 skipped**, exit 0. pins: win-slide-1/C-006 |
| C-007 | The design is **measured, not guessed**: the per-aggregate retract decision is read off DataFusion 54.1, the closed alternative is named with the exact API gap, the re-scan's cost is measured against the non-window aggregate and against Spark on the same shape, and the mutation score is run. | §7, §8, §9. | **PROVEN** | Per-aggregate probe table in §7 (all thirteen `supports_retract_batch = false`; the eight controls `true`). The physical `WindowExpr` route is **closed** in DF 54.1 and the gap is named exactly (§7). Perf and mutation numbers in §8 / §9. pins: win-slide-1/C-007 |
| C-008 | Docs: thirteen registry rows flipped with date and unit id, the frozen-roster pin flipped to the empty set with its guard kept, the two door bugs and the accuracy divergence filed, `map.md` lockstep, `STATUS.md` and `briefs/next-sequence.md` untouched. | The gates. | **PROVEN** | Thirteen `FIXED 2026-09-04 (WIN-SLIDE-1)` rows; `REFUSING_SLIDING_NAMES = ()` with the thirteen moved to `RESCANNED_SLIDING_NAMES` and a new registry guard reading it; `WIN-RANGE-DF-1`, `WIN-COLLECT-DOOR-1`, `WIN-SLIDE-PCT-ACC-1` filed. Seven maps in lockstep. `git diff` on `STATUS.md` and `briefs/next-sequence.md` is empty. pins: win-slide-1/C-008 |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: win-slide-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the brief. The deliverable is the 130-cell two-door matrix plus the five named contracts, all against goldens recorded from live PySpark 4.1.2 in one session, and every one of them was run red on the base build before the engine change existed.
      artifacts: [python/repark/tests/test_win_slide_1.py, crates/repark-core/src/session/tests/window_rescan.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Empty frame, all-NULL column, NULLs interleaved with values, one-row frame, a partition boundary, an ever-receding frame (CURRENT ROW to UNBOUNDED FOLLOWING), an ever-expanding frame (never rewritten), BIGINT overflow inside a frame, FILTER, DISTINCT, IGNORE NULLS, a two-argument aggregate, a literal argument (percentile), and an aggregate whose accumulator cannot even be constructed (the rewrite declines and the pre-existing refusal stands).
      artifacts: [python/repark/tests/test_win_slide_1.py, crates/repark-core/src/session/tests/window_rescan.rs]
    - id: AT-3
      status: ATTACKED
      evidence: No unwrap, expect or panic in the new module; the two internal invariants (a lost FILTER column, a non-boolean predicate) return DataFusionError::Internal rather than indexing. make rust-panic-ban exit 0. Slicing is bounded by the range DataFusion computed against the same value arrays.
      artifacts: [crates/repark-core/src/session/window_rescan.rs]
    - id: AT-4
      status: N/A
      justification: The rule is pure plan rewriting and the evaluator holds no shared or mutable state across partitions - it builds a fresh accumulator inside each evaluate call and keeps no index state, which is precisely why it is safe under BoundedWindowAggExec's front-pruning of the retained batch. No async, no locks, no spawn.
    - id: AT-5
      status: N/A
      justification: No authn/authz, no deserialization, no path, credential or network surface. unsafe_code stays workspace-forbidden and this unit adds none.
    - id: AT-6
      status: ATTACKED
      evidence: This is the correctness unit. Every answer is compared against a live-Spark-recorded column, cell by cell, not against a summary; floats compare to 1e-9 relative and the re-scan meets it exactly because it never retracts. The DataFrame-door RANGE bug found here was silently returning a wider frame for every aggregate, retractable ones included, and is now pinned on both doors.
      artifacts: [python/repark/tests/test_win_slide_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: ATTACKED
      evidence: Not a perf unit, but the cost is measured rather than assumed - the re-scan against the same aggregate with no window, and against Spark's wall on the same 1e5-row 100-row-frame shape, on a RELEASE native module. Section 8. The retractable path is untouched, so no existing window shape pays anything.
      artifacts: [task/ledgers/staging/win-slide-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1 contracts were read from the vendored source before use, not assumed - create_window_expr's Sliding/Plain split, AggregateFunctionExpr::create_sliding_accumulator's refusal site, StandardWindowExpr::evaluate and evaluate_stateful's values layout and index shifting, PartitionEvaluator's implementation table, coerce_scalar's Utf8-or-null-only cast, and the WindowFn export gap that closes the physical route.
      artifacts: [crates/repark-core/src/session/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: The rewrite is visible in the logical plan as a WindowUDF carrying the aggregate's own name, which is what the plan pins read; the original column name is restored with NamePreserver so no user-visible name changes. The two internal errors name the exact invariant.
      artifacts: [crates/repark-core/src/session/tests/window_rescan.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Four mutations were built and run, not reasoned, and each names the pins it reds. One of them (probing accumulator instead of create_sliding_accumulator) was hit for real during the build and caught by the pin before any measurement was taken.
      artifacts: [task/ledgers/staging/win-slide-1-ledger.md, crates/repark-core/src/session/tests/map.md]
  complete: true
```

## 6. What changed

| File | Change |
|---|---|
| `crates/repark-core/src/session/window_rescan.rs` | New, 290 lines. The `sliding_frame_rescan` analyzer rule and the `WindowUDF` / `PartitionEvaluator` pair it swaps in. |
| `crates/repark-core/src/session/df_guards.rs` | `.with_analyzer_rules(analyzer_rules_with_sliding_rescan())` — DataFusion's own list plus the new rule, on every core session. |
| `crates/repark-core/src/session.rs` | `mod window_rescan;`. |
| `crates/repark-core/src/session/tests/window_rescan.rs` | New. Six capability pins built on a throwaway non-retractable UDAF. |
| `crates/repark-python/src/column/window.rs` | RANGE frame offsets emit `ScalarValue::Utf8`, not `Int64` (`WIN-RANGE-DF-1`). |
| `crates/repark-python/src/column/expr_build.rs` | `single_wrapped_aggregate` / `replace_wrapped_aggregate`. |
| `crates/repark-python/src/column/mod.rs` | `Column.over` pushes the window into the one aggregate inside a scalar wrapper (`WIN-COLLECT-DOOR-1`). |
| `python/repark/tests/test_win_slide_1.py` | New. The 130-cell two-door matrix, the five contract pins, two live legs. |
| `python/repark-parity/bench/windows/roster.py` | `REFUSING_SLIDING_NAMES = ()`; the thirteen move to `RESCANNED_SLIDING_NAMES`. |
| `python/repark-parity/tests/test_w0_window_bench.py` | The frozen-set pin reads the new tuple; two pins added (a FIXED registry row per name; the refuse set is empty). |
| `docs/spark-sql-iceberg-parity.md` | Thirteen rows FIXED; `WIN-RANGE-DF-1`, `WIN-COLLECT-DOOR-1` FIXED; `WIN-SLIDE-PCT-ACC-1` BACKLOG. |
| `map.md` x 7 | Lockstep. |
| `STATUS.md`, `briefs/next-sequence.md` | Untouched. |

No public API change: no new crate dependency, no `Cargo.lock` edit, no `lib.rs` edit, no
`.github/` edit.

## 7. Design — measured, and the route that is closed

### 7.1 The per-aggregate decision, read off DataFusion 54.1

The brief asks for a per-aggregate choice between (a) an exact retractable accumulator and
(b) a frame re-scan. The decision is not a judgement call: `AggregateFunctionExpr::create_sliding_accumulator`
refuses on exactly one predicate — `!accumulator.supports_retract_batch()` — so the classification
was **read**, by building each registered UDAF's sliding accumulator on a live Spark-door session
and asking it. Probe run 2026-09-04 on this branch (`crates/repark-spark/tests`, scratch, deleted
after the reading):

| aggregate | `create_sliding_accumulator` | `supports_retract_batch` |
|---|---|---|
| `approx_count_distinct` | built | **false** |
| `approx_percentile` | built | **false** |
| `bit_and` | built | **false** |
| `bit_or` | built | **false** |
| `bool_and` | built | **false** |
| `bool_or` | built | **false** |
| `collect_list` | built | **false** |
| `collect_set` | built | **false** |
| `corr` | built | **false** |
| `covar_pop` | built | **false** |
| `covar_samp` | built | **false** |
| `percentile_approx` | built | **false** |
| `try_sum` | built | **false** |
| `sum` / `avg` / `min` / `max` / `count` | built | true |
| `stddev_samp` / `var_pop` / `regr_slope` | built | true |
| `array_agg` / `bit_xor` / `median` | built | true |
| `first_value` / `last_value` | built | false (never reached — the SQL planner resolves these to the **window** UDF, which is why W-0 classed them `ok`) |

Thirteen `false`, and the roster is exactly the thirteen W-0 named. The interesting controls are
`bit_xor` (retracts — XOR is self-inverse, so DataFusion wrote the inverse and `bit_and` / `bit_or`
have none to write) and `array_agg` (retracts via a front offset, which is why the DataFrame door's
`collect_list` — built as `coalesce(array_agg(x) IGNORE NULLS, make_array())` — never needed the
fallback once `over()` would accept it at all).

**Decision: (b), the frame re-scan, for all thirteen — one mechanism, not thirteen.** Per aggregate:

| aggregate | why not (a) |
|---|---|
| `approx_count_distinct` | HLL registers hold a max per bucket. Removing a value is not defined without the multiset behind it, which is the sketch's whole point. |
| `approx_percentile`, `percentile_approx` | repark's discrete percentile keeps the values and picks a rank; retraction would mean a multiset with removal, i.e. re-scanning with extra bookkeeping. Spark's own answer is the sketch's per-frame answer, re-derived per frame. |
| `collect_list`, `collect_set` | Frame ORDER is the answer for `collect_list`; a set has no inverse for `collect_set` without per-element counts. |
| `bit_and`, `bit_or`, `bool_and`, `bool_or` | A count-per-bit (64 counters) or a count-of-false **is** writable, and it would be O(1) per row. It was rejected on two measured grounds: it requires replacing DataFusion's registered `bit_and` / `bit_or` / `bool_and` / `bool_or` UDAF with a house one, which moves the **group-by** path this unit is chartered not to touch; and the re-scan already costs 9.6x the whole-table aggregate at a 100-row frame (section 8), against Spark's own 7.1x for the same shape. Buying O(1) here would be paid for in group-by risk. |
| `corr`, `covar_pop`, `covar_samp` | This is the one where (a) is not merely more expensive but **wrong for parity**. Spark re-scans, so Spark's answer carries no retraction drift. A Welford-style retract accumulates cancellation error, and the brief's own bar is a 1e-9 relative match at 1e5 rows. The re-scan sums each frame from its own rows and therefore matches Spark **exactly**, which is what the 65-cell pin asserts with no tolerance consumed. |
| `try_sum` | Overflow is the answer: a frame that overflows is NULL and the next frame is not. A retracting sum would have to carry "did any prefix overflow" and un-overflow on retract; a fresh accumulator per frame gets it for free. |

### 7.2 The route that is closed in DataFusion 54.1 (the API gap, named)

The obvious implementation is a physical `WindowExpr` that reuses DataFusion's own frame
machinery: implement the public `AggregateWindowExpr` trait, whose
`get_aggregate_result_inside_range(last_range, cur_range, values, accumulator, filter_mask)` is
exactly the re-scan seam (`SlidingAggregateWindowExpr` implements it with retraction,
`PlainAggregateWindowExpr` with update-only), and install it with a `PhysicalOptimizerRule`.

**That route cannot be taken from outside DataFusion 54.1.** `WindowExpr::create_window_fn(&self)
-> Result<WindowFn>` is a **required** trait method, and `WindowFn` is a `pub` enum in the
**private** module `datafusion_physical_expr::window::window_expr`, which
`datafusion_physical_expr::window::mod.rs` does not re-export (it re-exports
`PlainAggregateWindowExpr`, `SlidingAggregateWindowExpr`, `StandardWindowExpr`,
`StandardWindowFunctionExpr`, `PartitionBatches`, `PartitionKey`, `PartitionWindowAggStates`,
`WindowExpr`, `WindowState` — and neither `WindowFn` nor `AggregateWindowExpr`). The return type
of a required method cannot be spelled, so no out-of-tree type can implement `WindowExpr` at all.
`filter_array` and `is_end_bound_safe` are `pub(crate)` for the same reason. Upstreaming an export
would be the fix; forking is out of scope, and the unit did not need either, because the brief's
other named route — `WindowUDFImpl` + `PartitionEvaluator::evaluate(values, range)` with
`uses_window_frame() == true` — is fully public and is the one DataFusion's own user-defined-window
documentation points at. No halt.

### 7.3 The alternatives inside the chosen route, and why each was rejected

| alternative | why it was rejected |
|---|---|
| Wrap **every** sliding aggregate window function, and have the evaluator retract when it can | one code path, but it moves `sum` / `avg` / `min` / `max` / `count` off DataFusion's tuned sliding path onto a per-row `ScalarValue` loop, for aggregates that already work. The rule fires only where DataFusion would refuse. |
| Cache the accumulator across rows when the frame only grows (`cur.start == last.start`) | **unsound here.** `StandardWindowExpr::evaluate_stateful` hands the evaluator a retained batch that `BoundedWindowAggExec` prunes from the front between calls, shifting every index, and DataFusion's `PartitionEvaluator` contract is index-stateless (its own evaluators keep no `last_range`). It is also nearly worthless for a genuinely sliding frame, where the start moves every row. |
| Give the wrapper the Window node's input schema, captured at rewrite time, for `AccumulatorArgs::schema` | the optimizer prunes columns under the Window node after the analyzer runs, so the captured schema's indices no longer match the physical arg exprs, and `Column::data_type(schema)` — which `array_agg`, `first_last`, `nth_value` and datafusion-spark's `avg` all call — would read the wrong field or run off the end. The synthetic schema is index-free and self-consistent. |
| Fix `collect_list` by registering datafusion-spark's `collect_list` UDAF on the DataFrame door instead of `coalesce(array_agg(...), make_array())` | it unifies the doors, but it replaces the **group-by** spelling and its pins, which is the one thing the brief rules out. `over()` now pushes the window inside the wrapper instead. |
| Emit the `RANGE` bound as the order key's own type instead of `Utf8` | `Column.over` has no schema — it is called on a bare `Column`. `Utf8` defers the decision to DataFusion's own coercion, which is what its SQL planner already relies on. |
| Leave `FILTER (WHERE ...)` refusing (the frame re-scan's `WindowUDF` branch drops the filter in `create_window_expr`) | it is a silent hole class, not a one-off: `bit_and(vi) FILTER (WHERE ...) OVER (sliding)` refused while the unfiltered form answered. Carrying the predicate as a trailing argument and masking each frame closes it in ~30 lines and is pinned. |

## 8. Measurement — the re-scan's cost (C-007)

Not a perf unit. The question the brief asks is narrow: does the re-scan cost so much more than
the same aggregate with no window that it is a finding? The bar it names is 100x.

RELEASE native module (`repark._native.__debug_assertions__ == False`, 163 MB), 1e5 rows, a
100-row frame (`ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW`), one warmup then the
median of three, repark and Spark 4.1.2 in the same process, same seed, same box, same session.
Every window query is wrapped in a numeric sink so the operator cannot be elided. Milliseconds:

| shape | repark | Spark 4.1.2 | repark / Spark |
|---|---:|---:|---:|
| `sum(v)` over the frame — **retract path, untouched** | 46.02 | 413.95 | 0.11x |
| `bit_and(vi)` over the frame — re-scan | 48.76 | 451.89 | 0.11x |
| `corr(v, v2)` over the frame — re-scan | 210.75 | 1802.38 | 0.12x |
| `collect_list(v)` over the frame — re-scan | 142.34 | 456.98 | 0.31x |
| `bit_and(vi)`, whole table, no window | 5.10 | 63.94 | 0.08x |
| `corr(v, v2)`, whole table, no window | 7.02 | 87.48 | 0.08x |

The re-scan against the same aggregate with no window: **`bit_and` 9.6x, `corr` 30.0x**. Spark's
own multiplier for the identical pair of queries is **7.1x** and **20.6x**. So the re-scan is
within a small constant of the cost model Spark itself pays for the same strategy, an order of
magnitude below the 100x bar, and repark is 3x to 9x faster than Spark in absolute wall time on
every one of the six. **No finding to file.**

Provenance: taken 2026-09-04 21:26 UTC on the release native of this branch, with no other JVM
on the box (checked before the run) and a 1-minute load of ~11 from the sibling lanes' Rust
builds — which is why the absolute milliseconds are a same-box, same-process comparison and not a
hardware claim. Two edits landed after the reading, neither on a measured path: the rule's module
moved from `session/window_rescan.rs` to `session/df_guards/window_rescan.rs` (a relocation), and
`Column.over`'s body moved into `column/window.rs` (the DataFrame door; every query above is the
SQL door). The re-run to re-confirm them was abandoned rather than reported, because a sibling
lane had taken the JVM by then and a contended number is worse than a stated provenance.

Read the multiplier honestly: the no-window baseline produces ONE row and the window query
produces 1e5, so a large ratio is expected of any engine — which is exactly why Spark's own ratio,
not a threshold in the abstract, is the yardstick. `sum` is the control that shows the retractable
path did not move: 46.02 ms, and its plan still carries DataFusion's sliding accumulator.

## 9. Mutation score (C-007)

Four mutations, each built and run. "Rust" is
`cargo test -p repark-core --lib session::tests::window_rescan` (6 pins); "facade" is
`pytest python/repark/tests/test_win_slide_1.py` (137 pins).

| # | mutation | Rust | facade | which pins |
|---|---|---|---|---|
| M1 | `needs_frame_rescan` always returns `false` (the fallback is never installed) | **4 of 6 red** | **117 of 137 red** | Everything except the DataFrame-door `bool_and` / `bool_or` cells (10 — that door is a `min` / `max` shim) and the DataFrame-door `collect_list` / `collect_set` cells (10 — that door builds `coalesce(array_agg(x) IGNORE NULLS, make_array())`, and `array_agg` retracts). The surviving 20 are the two cases the fallback genuinely does not serve, which is the point of measuring instead of predicting. |
| M2 | probe `accumulator` instead of `create_sliding_accumulator` | **1 of 6 red** | not run | `a_retractable_aggregate_keeps_datafusions_sliding_accumulator`. This mutation was written by accident during the build (it is the obvious wrong constructor) and the pin caught it before any measurement was taken — `sum` had been silently moved onto the re-scan. |
| M3 | an empty frame answers `AggregateUDF::default_value` (what DataFusion's own sliding path does) instead of a fresh accumulator's `evaluate()` | **1 of 6 red** | not run | `an_empty_frame_answers_a_fresh_accumulator_not_the_aggregate_default`. The throwaway UDAF's `default_value` is the sentinel `-1.0` precisely so the two are distinguishable. The facade half of the same contract is the `collect_list` / `collect_set` `[]` and `approx_count_distinct` `0` cells. |
| M4 | drop the `FILTER (WHERE ...)` mask (evaluate the unmasked frame) | **1 of 6 red** | not run | `a_filtered_non_retractable_aggregate_answers_the_masked_frame`. |

One more mutation was measured as a **state of the tree** rather than injected, because it is the
code the unit replaced and the state existed on the way:

| # | state | facade | which pins |
|---|---|---|---|
| M5 | `Column.over` emits the RANGE offset as `Int64` (the re-scan and the `over()` push-down present, `WIN-RANGE-DF-1` not yet fixed) | **11 of 137 red** | every `test_dataframe_door_matches_the_spark_pin[range_frame-*]` except `bool_and` / `bool_or`, which are `min` / `max` shims and answered the widened frame identically — a cumulative `min` over this fixture happens to agree with the 2-preceding one once the single `False` appears, and a cumulative `max` is `True` throughout. That is luck of the fixture, not coverage, and it is why the RANGE contract is pinned on the other eleven. (The same run also reded one pin for a defect in the pin module itself — a helper that projected `id` against a view that has no `id` — fixed in the same session; it is not an M5 red.) |

`WIN-COLLECT-DOOR-1` has **no isolated mutation number**: the `over()` push-down and the re-scan
rule landed in the same build, so the state "re-scan present, `over()` still refusing" never
existed to be measured. What is measured is the base-of-branch control below, where those ten
cells fail with `ValueError: over() applies only to a window or aggregate function column` — a
different message from the other 117, which is the evidence that they are a distinct defect.

Base-of-branch control, the red-first record: on `origin/main` `55652ca` with the pin module and
nothing else, **127 of 137 red, 10 green** — the 10 being the DataFrame-door `bool_and` /
`bool_or` cells, which were already Spark-equal.
