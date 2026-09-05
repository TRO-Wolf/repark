# Unit ledger — PERF-AGG-AVG-1 · a `GroupsAccumulator` for the Spark `avg` / `try_avg` UDAF

**Date:** 2026-09-05 · **Branch:** `perf/agg-avg-1` · **Base:** `origin/main` `6eaccd5e` ·
**Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `PERF-AGG-AVG-1` to file as FIXED with before/after (C-006).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** PERF-ANALYSIS-1 §2 row 10 measured the Spark `avg` UDAF at 389 ms on
`avg(l_quantity) GROUP BY l_partkey` (200 k groups, 6 M rows) where `sum` on the same
grouping costs 88 ms, and §7.7 measured TPC-H Q17 at 11.9× DuckDB with
`elapsed_compute=2.46 s` in `AggregateExec Partial avg(l_quantity)`. Cause:
`SparkAvgWithRetract` implements `accumulator()` only, so DataFusion boxes one
accumulator per group. Slate item 8 queues the `GroupsAccumulator`.

**Not in this unit:** the retract path (`SparkAvgWithRetract` stays byte-identical for
window frames — C-002 guards it); `avg(DISTINCT)` (still refused, C-003 pins the refusal);
any other aggregate; any public API change; fork or dependency changes; `STATUS.md` and
`briefs/next-sequence.md`.

**Writable paths:**
`crates/repark-functions/src/{avg_groups.rs,aggregate.rs,lib.rs}`,
`crates/repark-functions/map.md`,
`python/repark/tests/{test_perf_agg_avg_1.py,map.md}`,
`docs/perf/{aggregate-baseline.md,map.md}`,
`docs/spark-sql-iceberg-parity.md` §7 (one registry row),
this ledger and its `staging/map.md` row.
Closed: `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.lock`, every
dependency, every other ledger, every size-gate baseline (all touched files stay ≤ 1000
lines, so no `EXCEPTIONS` row and no CAP-1 mirror edit).

## PROPOSITION LEDGER — PERF-AGG-AVG-1 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The UDAF serves grouped aggregation through a `GroupsAccumulator` on every input it serves today: Float64 (int/float coerce as before) and Decimal32/64/128/256 with Spark's `(min(38,p+4), min(38,s+4))` result rules; `try_avg` decimal overflow yields NULL per group; `avg(DISTINCT)` still refuses. | `crates/repark-functions/src/avg_groups.rs`; `groups_accumulator_supported` / `create_groups_accumulator` in `aggregate.rs`; the Rust unit tests (`update_batch` / `merge_batch` / `evaluate` / `state` / `size`, `EmitTo::First`). | **OPEN** | Which threshold separates base from fixed on the many-groups probe? Base ratio ≈ 4.4× avg/sum; fixed ratio unmeasured. State the bound in C-004 once both are measured. |
| C-002 | The retract path is unchanged: `SparkAvgWithRetract` keeps its `retract_batch` arms, window-frame `avg` (float and decimal) answers as before, and every existing avg pin stays green. | The untouched `Accumulator` impls; `test_perf_agg_avg_1.py` window control; the full `repark-functions` Rust suite. | **OPEN** | Window control unmeasured; existing pins unrun on this lane. |
| C-003 | Grouped `avg` / `try_avg` answers are Spark-equal on int, float and decimal inputs with NULLs, on empty input, on ≥ 1e5 groups, and on decimal result precision/scale — every expectation recorded from live PySpark 4.1.2, value AND Arrow-path type. | `python/repark/tests/test_perf_agg_avg_1.py` (always-run pins + `REPARK_PARITY_LIVE=1` legs); the recorded Spark outputs in §8. | **OPEN** | Spark goldens unrecorded; the JVM slot is held by sibling lanes. |
| C-004 | The many-groups probe is red on the base and green after: `avg` over ≥ 1e5 groups costs no more than the stated multiple of `sum` over the same grouping, measured back to back in one process. | The probe in `test_perf_agg_avg_1.py`; the base red run and the after green run with loads in §8. | **OPEN** | Bound TBD from the measured base/fixed margins (see C-001). |
| C-005 | The delivery gates are met on the analysis' own cells at 8-thread parity on a release module: `decimal/sf1/avg_decimal_by_partkey` ≤ 1.3× `sum_decimal_by_partkey`, and TPC-H Q17 ≤ 3× DuckDB with DuckDB recorded on the same box. | §8; `docs/perf/aggregate-baseline.md`; the TPC-H runner output. | **OPEN** | Before numbers unmeasured on this lane. |
| C-006 | Docs and gates: registry row `PERF-AGG-AVG-1` FIXED with before/after, a `docs/perf` aggregate baseline with the machine/profile header and a reproduce block, `map.md` lockstep for every directory touched, the brief's full gate list exit 0, and the three named mutations red. | §6, §7, §9; the gates table. | **OPEN** | Nothing filed yet. |

VERDICT: 6 clauses, 0 PROVEN, 6 OPEN, 0 REJECTED.

## 6. Red-first and mutation (docs/testing.md "Gate provocation proofs")

**Red first (2026-09-05, base `6eaccd5e` release module `163,478,728 B`, load 6.12).**
`python/repark/tests/test_perf_agg_avg_1.py`: **1 failed, 23 passed, 6 skipped**.
The failure is the many-groups probe, red where the fix changes the answer path:
`avg 133.9 ms vs sum 33.0 ms` (4.06×, bound 2.5) on the 2e5-group single-partition
fixture. The single partition is load-bearing: at 8 partitions the same shape measures
2.77× and at 1e5 groups 1.93×, both under the bound — the per-group boxing cost is
only visible when one thread carries all the groups. Shape curve (base, median of 3):
1e5×10 1.93×, 2e5×10 single-partition 4.06×, 2e5×10 8-partition 2.77×, 2e5×30 1.87×.

**Mutation score (2026-09-05, `cargo test -p repark-functions --lib`, 355 green at
rest).** M1 wrong group index (update writes sums/counts to `(index + 1) % len`) →
8 red: `float_groups_update_then_evaluate_averages_per_group`,
`float_groups_filter_marks_fully_filtered_group_null`,
`float_groups_state_layout_is_sum_then_int64_count`,
`float_groups_emit_first_shifts_remainder`,
`decimal128_groups_update_then_evaluate_scales_result`,
`decimal128_groups_empty_group_evaluates_null`,
`decimal128_groups_filtered_group_evaluates_null`,
`group_by_avg_sql_answers_through_session`. M2a ignored input NULL mask (the
`(true, None)` accumulate arm drops its validity check) → 4 red:
`float_groups_update_then_evaluate_averages_per_group`,
`float_groups_int_input_coerces_to_float`, `float_groups_merge_combines_two_partials`,
`groups_null_state::tests::null_state_build_first_splits_mask`. M3 decimal scale off
by one (`try_new` with `target_scale + 1`) → 8 red: all four width value pins, the
two new decimal empty/filtered pins, the decimal state round-trip, and the SQL wiring
test. M1 additionally proven at the Python level against a faulty release build
(`_native.abi3.so` `163,721,640 B`): 5 red — `test_avg_grouped_small_value_and_type`,
`test_avg_grouped_small_dataframe_door`, `test_avg_decimal128_grouped_type_and_values`,
`test_avg_all_null_group_is_null`, `test_many_groups_answers_match_pinned_checksum`
(checksum `3150034.90` vs `3150001.75`). Global, window, overflow and probe pins stay
green — they do not use the groups path. The fault was reverted and the module
rebuilt before any further measurement.

**S1 found by the M2 analysis (2026-09-05).** The first `evaluate` computed the
average closure for every group before applying the null mask; a decimal group with
count zero (never-seen index, fully-filtered group) then divided by zero inside
`DecimalAverager::avg` and panicked (`core::num` divide by zero, proven by dropping
the guard: `decimal128_groups_empty_group_evaluates_null` panics). Fix, two layers:
`decimal_average` returns NULL on count zero (the float arm and the `Accumulator`
already did), and `evaluate` skips the closure for mask-null groups exactly the way
DataFusion's own `evaluate` does. Pinned by `decimal128_groups_empty_group_evaluates_null`
and `decimal128_groups_filtered_group_evaluates_null`. The analysis also showed the
output mask is subsumed by the count guard for `avg` (mask-null ⟺ count zero, both
set in the same closure): dropping the mask in `build` reds nothing by construction,
so M2 is the input-mask mutation above. The mask stays as the reference shape and
defense in depth.

**Split note (declared rename).** `avg_groups.rs` passed the 1000-line ceiling with
the fix, so the null tracker moved to `groups_null_state.rs` (867 + 182 lines); the
only test that moved with it is
`avg_groups::tests::null_state_build_first_splits_mask` →
`groups_null_state::tests::null_state_build_first_splits_mask`.

## 7. Registry

To file: `PERF-AGG-AVG-1` in `docs/spark-sql-iceberg-parity.md` §7, measured-perf form.

## 8. Numbers

**Before (2026-09-05, base `6eaccd5e`, release `163,478,728 B`,
`__debug_assertions__ False`, load 14.6–15.5, 5 timed + 1 warm-up):**

| cell | median | min | spread | load |
|---|---|---:|---:|---|
| `decimal/sf1/avg_decimal_by_partkey/tp8` | 400.6 | 361.4 | 54.5 | 14.78 |
| `decimal/sf1/avg_double_by_partkey/tp8` | 433.3 | 429.9 | 66.0 | 14.78–14.64 |
| `decimal/sf1/sum_decimal_by_partkey/tp8` | 90.0 | 72.0 | 26.1 | 14.64 |
| floor (6 repeated sum medians) | — | — | **3.3** | 14.64–15.13 |
| DuckDB `avg_decimal_by_partkey` (arrow fetch) | 102.1 | — | 7.1 | — |
| DuckDB `sum_decimal_by_partkey` (arrow fetch) | 114.7 | — | 6.0 | — |

Isolated avg cost 310.6 ms = 94× the 3.3 ms floor; avg/sum ratio 4.45×. The analysis
(389/437/88 ms, floor ~11 ms, DuckDB 102/113) reproduces within load noise. DuckDB
timed with `to_arrow_table`, not `fetchall` — materializing 2e5 Python decimals costs
480 ms and is not the engine. TPC-H Q17 (`run_tpch.py --sf 1 --repeats 3 --queries
17`, two runs): repark 0.721 s / 0.521 s vs DuckDB 0.040 s / 0.038 s → 18.25× / 13.84×
(status OK, 1 row each).

**Oracle record (2026-09-05, live PySpark 4.1.2 `local[2]`, ANSI on, UTC, shuffle 2,
beside one sibling JVM).** Every literal in `test_perf_agg_avg_1.py` was printed by
`/tmp/aggavg_record.py` (throwaway, imports the test module's own SQL constants) and
is quoted here verbatim: int global `double [2.75]`; float global `double
[1.1666666666666667]`; grouped small `(string not-null, double) [(a,2.0), (b,4.0),
(c,None)]`; decimal grouped `(int32 not-null, decimal128(14,6)) [(1,1.650000),
(2,3.300000)]`; decimal global `decimal128(14,6) [1.650000]`; `try_avg` overflow
`decimal128(38,4) [None]`; plain-avg overflow RAISES `ArithmeticException
[NUMERIC_VALUE_OUT_OF_RANGE...] cannot be represented as Decimal(38, 4)`; empty
double global `double [None]`; all-NULL grouped `[(a,None), (b,1.0)]`; window
`[1.0, 1.5, 2.0, 4.0]`; single distinct decimal `decimal128(6,5) [1.50000]` and int
`double [1.5]`; multi-distinct `(double, int64) [(1.5, 10)]`; all six input widths
`double [3.0]`; many-groups checksum `(groups 200000, checksum 3150001.7499999637)`
and head rows `[(0,17.5), (1,14.833333333333334), (2,19.27777777777778)]`, 2e5 rows,
0 nulls.

To record: the after medians on the same cells.

## 9. Gates

To record: the brief's gate list with exit codes.
