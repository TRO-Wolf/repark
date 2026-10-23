# map — python/repark/tests/perf

## Purpose

Performance and resource budgets: perf gates, spill and memory pools, eager and cache retention, small-stack gates, and the TPC-H / TPC-DS / write / window / scale bench harnesses.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_cache_persist.py](test_cache_persist.py) — R-PERF-CACHE + CACHE1 cache-honesty — cache / persist / unpersist / clearCache.
- [test_dynflatten_bed_gate.py](test_dynflatten_bed_gate.py) — PERF-DYNFLATTEN-1: gate-scale bed flatten on repark.
- [test_dynflatten_null_mask.py](test_dynflatten_null_mask.py) — PERF-DYNFLATTEN-2: gate-scale null-mask row set and schema.
- [test_eager_budget_1.py](test_eager_budget_1.py) — EAGER-BUDGET-1 pins: retained bytes, the read-only conf, and the total budget refusal.
- [test_eager_own_1.py](test_eager_own_1.py) — EAGER-OWN-1 step-1 pins: a refcounted handle owns every ``__repark_cache_*`` view.
- [test_grown_stack_gate_1.py](test_grown_stack_gate_1.py) — GROWN-STACK-GATE-1 pins — the 34 gated sites answer on small stacks.
- [test_h3_spill_matrix.py](test_h3_spill_matrix.py) — H3-SPILL-1 Never-OOM pins: a bounded pool spills, degrades or refuses; never aborts or lies.
- [test_mw7_scale_smoke.py](test_mw7_scale_smoke.py) — MW-7: prove the scale-measurement driver's metric collection tells the truth.
- [test_perf_agg_avg_1.py](test_perf_agg_avg_1.py) — Grouped avg/try_avg answer pins plus the many-groups cost probe (PERF-AGG-AVG-1).
- [test_perf_approxpct_1.py](test_perf_approxpct_1.py) — PERF-APPROXPCT-1 sketch pins.
- [test_perf_describe_1.py](test_perf_describe_1.py) — PERF-DESCRIBE-1 — describe()/summary() aggregate the source in one lazy pass.
- [test_perf_facade_cdf_1.py](test_perf_facade_cdf_1.py) — PERF-FACADE-CDF-1 — the column-wise createDataFrame path equals the legacy path.
- [test_perf_facade_collect_rows.py](test_perf_facade_collect_rows.py) — PERF-FACADE-COLLECT-1 — the native row fast path equals the Python converter, cell for cell.
- [test_perf_facade_logical_names.py](test_perf_facade_logical_names.py) — PERF-FACADE-WITHCOLUMN-1 — logical-schema column names are byte-equal to the analyzed names.
- [test_perf_ice_catalog_io_1.py](test_perf_ice_catalog_io_1.py) — PERF-ICE-CATALOG-IO-1 memory pin: repeated reads and DML parse no metadata document beyond the commit.
- [test_perf_ice_scan_1.py](test_perf_ice_scan_1.py) — PERF-ICE-SCAN-1 — count(*) folds without a scan; small tables scan in parallel.
- [test_perf_ice_writepath_1.py](test_perf_ice_writepath_1.py) — PERF-ICE-WRITEPATH-1 — the CTAS write node: file layout, determinism, and wall.
- [test_perf_unpivot_1.py](test_perf_unpivot_1.py) — PERF-UNPIVOT-1 — native stack() unpivot; step 2 puts describe back on a pure plan.
- [test_t2_sort_memory.py](test_t2_sort_memory.py) — T2 — ExternalSorter / datafusion conf passthrough / export error UX.
- [test_t2_spill_reach.py](test_t2_spill_reach.py) — S-1 spill-reach battery — recon §3 operator classes under a small FairSpillPool. SPILL-REACH-FAIR-POOL-FLAKE-1 (2026-10-22): the session helper's default pool is 72M, up from 64M, because the join probe refused 7 of 280 loaded-type runs at 64M and 0 of 221 at 72M, and every passing run still spills; the runner half was not measured (the diagnostic push was denied on 2026-10-22), so the first CI run of the shard that holds this file is the only runner sample. The assertions, the batch size and the two 16M pins are unchanged ([ledger](../../../../task/ledgers/staging/spill-reach-fair-pool-flake-1-ledger.md)). Orchestrator ruling 2026-10-22 (under the owner's "go with your recommendations"): the default is 80M, not 72M, because the ordered criterion could not separate 64M from 72M on a quieter box and the stress table decides: a CI hang costs a 45-minute job timeout and a re-run while a refusal costs one red cell, and 80M cut the pool-dependent hangs from 16 (64M) and 9 (72M) to 1 of 200 with zero refusals and the join still spilling 12 to 14 times; the `spill_count > 0` assertions are unchanged; the runner half was still not measured.
- [test_tpcds_compare_unit.py](test_tpcds_compare_unit.py) — Unit pins for the TPC-DS compare kernel + runner helpers.
- [test_tpcds_smoke.py](test_tpcds_smoke.py) — R-TPCDS-HARNESS — SF0.01 facade oracle battery (DuckDB differential).
- [test_tpch_compare_unit.py](test_tpch_compare_unit.py) — Unit pins for the TPC-H compare kernel.
- [test_tpch_smoke.py](test_tpch_smoke.py) — R-TPCH-HARNESS — SF0.01 facade oracle battery (DuckDB differential).
- [test_w0_window_bench_smoke.py](test_w0_window_bench_smoke.py) — W-0: run the window-shape driver at gate scale against the native module.
- [test_write_bench_unit.py](test_write_bench_unit.py) — Unit pins for the write-bench harness (no SF1 wall; pure helpers + tiny smoke).

## Path expressions

The move put every file here one level below the old flat directory. These files had a
path expression repointed in the same unit and no other line changed:

| file | change |
|---|---|
| `test_dynflatten_bed_gate.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_dynflatten_null_mask.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_mw7_scale_smoke.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_perf_approxpct_1.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_tpcds_compare_unit.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_tpcds_smoke.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_tpch_compare_unit.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_tpch_smoke.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_w0_window_bench_smoke.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_write_bench_unit.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |

## Pointers

- Up: [../map.md](../map.md)
