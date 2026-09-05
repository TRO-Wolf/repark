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

To record: the base red run of the many-groups probe, and the three named mutations
(wrong group index, ignored NULL mask, decimal scale off by one) with which pins red.

## 7. Registry

To file: `PERF-AGG-AVG-1` in `docs/spark-sql-iceberg-parity.md` §7, measured-perf form.

## 8. Numbers

To record: before/after medians, spreads, floors, × floor, loads, release proofs.

## 9. Gates

To record: the brief's gate list with exit codes.
