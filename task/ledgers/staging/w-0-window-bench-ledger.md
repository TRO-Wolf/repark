# Unit ledger — W-0 · window-shape measurement (Track A opener; no product change)

**Date:** 2026-08-31 · **Branch:** `feat/w-0-window-bench` · **Base:**
`60225cc427673cbc2e4bf23e90db376e602773dd` · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Verify before done" and
[../../../docs/testing.md](../../../docs/testing.md) · **Path:** STANDARD ·
**risk_tier:** standard · **Measure-only.**

**Charter:** intake Track A row W-0 in
[../../roadmap/mid-term/roadmap-intake-2026-08-23.md](../../roadmap/mid-term/roadmap-intake-2026-08-23.md)
(read-only). **Retires:** moved to `completed/` in this unit's departure commit.

**Closed homes:** `crates/**`, engine behaviour, `[patch.crates-io]`, `.github/`,
`briefs/next-sequence.md`, ledger `completed/` / archive.

Results: [../../window-bench-report-2026-08-31.md](../../window-bench-report-2026-08-31.md).

## PROPOSITION LEDGER — W-0 — 2026-08-31

The probe roster below is the finite domain C-002 and C-009 quantify over.
Spark 4.1.2 built-in aggregates used as
`fn OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)`.
Excluded: `grouping` / `grouping_id` (not window aggregates) and registry §9
DECLARED-absent sketches (`hll_*`, theta, KLL, bitmap, `count_min_sketch`).

**Unary over numeric `v`:** `any`, `any_value`, `approx_count_distinct`,
`array_agg`, `avg`, `bit_and`, `bit_or`, `bit_xor`, `bool_and`, `bool_or`,
`collect_list`, `collect_set`, `count`, `count_if`, `every`, `first`,
`first_value`, `kurtosis`, `last`, `last_value`, `max`, `mean`, `median`,
`min`, `mode`, `skewness`, `some`, `std`, `stddev`, `stddev_pop`,
`stddev_samp`, `sum`, `try_avg`, `try_sum`, `var_pop`, `var_samp`, `variance`.

**With extra arguments:** `approx_percentile(v, 0.5)`, `corr(v, v2)`,
`covar_pop(v, v2)`, `covar_samp(v, v2)`, `max_by(v, id)`, `min_by(v, id)`,
`percentile(v, 0.5)`, `percentile_approx(v, 0.5)`, `regr_avgx(v, v2)`,
`regr_avgy(v, v2)`, `regr_count(v, v2)`, `regr_intercept(v, v2)`,
`regr_r2(v, v2)`, `regr_slope(v, v2)`, `regr_sxx(v, v2)`, `regr_sxy(v, v2)`,
`regr_syy(v, v2)`.

Retractable class the intake names: `sum`, `count`, `avg`, `min`, `max`,
`stddev`, `var` (and the Spark aliases `mean`, `std`, `stddev_pop`,
`stddev_samp`, `var_pop`, `var_samp`, `variance`). Everything else that
plans is the non-retractable class.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | One CLI entry point lives under `python/repark-parity/bench/windows/` with seeded data generation (no wall clock in the seed) and a machine-profile record (cpu, cores, governor, ram). | Import the driver without the native module; assert the generator is deterministic and the profile keys exist. | PROVEN | `run_w0.py`; `test_seed_table_is_deterministic_and_typed`; `test_hardware_profile_has_required_keys`. pins: w-0-window-bench/C-001 |
| C-002 | Sliding-frame cells exist for every probe-roster name, classified retract vs not vs absent vs refuse. | Parametrized pin over the roster; each name has exactly one class. | PROVEN | `PROBE_NAMES == CHARTER_NAMES` (54); live probe in the gate smoke covers the roster. pins: w-0-window-bench/C-002 |
| C-003 | A constant-frame cell runs `UNBOUNDED PRECEDING` / `UNBOUNDED FOLLOWING` (or `OVER ()`) on a retractable aggregate. | Smoke asserts the cell SQL and that the result record carries the constant-frame label. | PROVEN | `constant_select` + full-run cell `constant_sum` (WindowAggExec, no SortExec). pins: w-0-window-bench/C-003 |
| C-004 | An unpartitioned `ORDER BY` cell exists whose full-scale default is 10_000_000 rows. Gate scale is smaller and does not assert wall clock. | Constant `FULL_UNPARTITIONED_ROWS == 10_000_000`; smoke runs the cell at gate scale. | PROVEN | `test_full_unpartitioned_rows_is_ten_million`; full run cell `unpartitioned_order_by` 10_000_000 rows. pins: w-0-window-bench/C-004 |
| C-005 | A `lead` / `lag` cell reads an unsorted Iceberg table (memory catalog, no sort order) and times both functions. | Smoke writes the table, runs `lead`/`lag` `OVER (ORDER BY ts)`, records plan-shape tokens including whether a sort ran. | PROVEN | `test_iceberg_lead_lag_runs_over_an_unsorted_table`; full run plan tokens `SortExec` + `Iceberg`. pins: w-0-window-bench/C-005 |
| C-006 | A window cell runs with a session `memory_limit` below the working set and records one outcome class: `ok`, `spill`, `oom`, `error`, or `crash`. The driver never retries a different query to hide the class. | Smoke with a 1 MiB floor and a working set above it; the recorded class is one of the five; an injected raise becomes `error`, not a rewritten query. | PROVEN | Full run `memory_limit_16M` at 2e6 rows: outcome `oom` (FairSpillPool / ExternalSorter). pins: w-0-window-bench/C-006 |
| C-007 | Two oracle adapters exist: DuckDB at the workspace pin `duckdb==1.5.5`, and PySpark `4.1.2`. A missing extra or JVM is skip-loud and named in the results, never a silent omit. | Adapter constructors; DuckDB version pin in the bench requirements; PySpark skip records `oracle=pyspark reason=...`. | PROVEN | `requirements.txt`; full run DuckDB 1.5.5 and PySpark 4.1.2 both `ok` on timed cells. pins: w-0-window-bench/C-007 |
| C-008 | A dated results document under `task/` carries engine version, DuckDB version, PySpark version-or-skip, machine profile, raw cell numbers, and generated-dataset byte sizes. | The result model requires those fields; the filed markdown exists and names them. | PROVEN | [../../window-bench-report-2026-08-31.md](../../window-bench-report-2026-08-31.md). pins: w-0-window-bench/C-008 |
| C-009 | Every probe-roster name whose live class is **refuse** (plans as a window aggregate, then DataFusion sliding-accumulator `not_impl`) is a registry BACKLOG row `WIN-SLIDE-<name>` with repark / Spark / pin / rationale. Absent names are not W-0 rows. | Gate test: the refuse set equals the `WIN-SLIDE-` headings; each pin asserts the sliding query raises. | PROVEN | Twelve names, twelve headings. `test_sliding_refuse_set_matches_the_frozen_roster`; `test_registry_has_a_heading_per_sliding_refuse`. pins: w-0-window-bench/C-009 |
| C-010 | This unit does not modify `crates/`. An engine crash or loud refuse is recorded as an outcome (and C-009 row when it is a sliding refuse), never patched around. | `git diff` against base has no `crates/` path; the crash/error path has a pin. | PROVEN | `test_run_repark_sql_does_not_retry_a_different_query`; no `crates/` in the unit diff. pins: w-0-window-bench/C-010 |
| C-011 | Generated datasets land under the caller scratch directory and are deleted after the measurement run; the results document records the sizes taken before delete. `--keep-scratch` is the only opt-out. | Smoke writes, records `dataset_bytes`, deletes; a second listing of the scratch is empty. | PROVEN | `test_cleanup_scratch_deletes_unless_keep`; full run `scratch_deleted: True`; sizes in the report. pins: w-0-window-bench/C-011 |

VERDICT: PROVEN — 11 clauses, 11 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
KILLED_ASSUMPTIONS:
  - "DuckDB must be added to the workspace": REMOVED (root pyproject.toml already pins duckdb==1.5.5)
  - "make py-test exercises the native window operator": REMOVED (engine-free pins in python/repark-parity/tests; native smoke in python/repark/tests)
  - "SELECT count(*) FROM (SELECT <window>) forces the window to run": REMOVED (DataFusion elides it to PlaceholderRowExec / a constant n. Probe uses the bare SELECT; timed cells use sum(w) as a sink that cannot be dropped.)
CLARIFYING_QUESTIONS: []
```

## 1. MEASUREMENTS — full scale, 2026-08-31

One host (`schedutil`, Ryzen Threadripper 3970X, 64 threads, 125.7 GiB). Release
native `size_bytes=159402864`. Engine `repark-0.5.0`, DuckDB `1.5.5`, PySpark
`4.1.2` (zulu-17). Wall **139.9 s**. Peak RSS **2,859,900,928 B** (process-wide,
includes the in-process PySpark driver). Ratios over absolutes. Raw cells:
[../../window-bench-report-2026-08-31.md](../../window-bench-report-2026-08-31.md).

| cell | rows | repark median_ms | DuckDB | PySpark | repark/DuckDB | repark/PySpark |
|---|---:|---:|---:|---:|---:|---:|
| sliding_sum | 1e6 | 564.9 | 75.5 | 2277.1 | 7.5x | 0.25x |
| sliding_avg | 1e6 | 794.8 | 76.8 | 2989.8 | 10.3x | 0.27x |
| sliding_min | 1e6 | 654.0 | 91.9 | 2192.9 | 7.1x | 0.30x |
| sliding_max | 1e6 | 660.4 | 86.7 | 2176.3 | 7.6x | 0.30x |
| sliding_count | 1e6 | 485.2 | 79.5 | 2195.6 | 6.1x | 0.22x |
| constant_sum | 1e6 | 18.7 | 10.9 | 366.1 | 1.7x | 0.05x |
| unpartitioned_order_by | **1e7** | 4409.9 | 367.1 | 9588.9 | 12.0x | 0.46x |
| iceberg_lead_lag | 1e6 | 225.0 | 140.4 | 665.6 | 1.6x | 0.34x |
| memory_limit_16M | 2e6 | — | — | — | oom | — |

Constant-frame `sum` is ~30x the sliding `sum` on RePark (18.7 vs 564.9 ms) and
the plan is `WindowAggExec` without `SortExec` — the DataFusion constant-aggregation
path is taken. Unpartitioned `ORDER BY` at 1e7 still sorts (`SortExec` +
`BoundedWindowAggExec`). Iceberg lead/lag plan tokens include `Iceberg` and
`SortExec` (scan advertises no order). Over-limit: `datafusion.runtime.memory_limit=16M`
on 2e6 rows fails as **`oom`** on `ExternalSorter` / FairSpillPool — the window
exec itself does not spill. That is the W-3 row, recorded, not patched.

Scratch sizes before delete: unpartitioned parquet **108,701,433 B**; iceberg
warehouse **7,147,598 B**; memory seed **21,740,637 B**. Directory `/tmp/w0-full`
was empty after the run.

Sliding-frame **refuse** (plans, then `retract_batch` not implemented), each a
registry `WIN-SLIDE-<name>` row: `approx_percentile`, `bit_and`, `bit_or`,
`bool_and`, `bool_or`, `collect_list`, `collect_set`, `corr`, `covar_pop`,
`covar_samp`, `percentile_approx`, `try_sum`. Names that do not plan are
**absent**, not W-0 rows. `array_agg` / `bit_xor` / `first_value` / `last_value`
/ `median` / the regr family **succeed** on the sliding frame (they have a
sliding accumulator).

## 2. Out of scope (unchanged)

- W-1 fallback `WindowExpr` (re-scan vs segment tree).
- W-2 Iceberg sort-order provenance / `BoundedWindowAggExec`.
- W-3 spill-coverage matrix guide (W-0 recorded the over-limit class: sort-path `oom`).
- W-U upstream issues.
- TA kernel numerics, window frame R4 (`EXCLUDE`).
- Any change under `crates/`.

## 3. Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: w-0-window-bench
  cycle: actor
  risk_tier: standard
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Walked C-001..C-011 against the bench, the 54-name roster, the twelve
        WIN-SLIDE registry headings, and the dated full-scale report.
      artifacts: [python/repark-parity/tests/test_w0_window_bench.py, python/repark/tests/test_w0_window_bench_smoke.py, task/window-bench-report-2026-08-31.md]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Probe covers absent / refuse / ok; scratch keep vs delete; missing
        DuckDB/PySpark is skip-loud.
      artifacts: [python/repark-parity/tests/test_w0_window_bench.py, python/repark/tests/test_w0_window_bench_smoke.py]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Sliding not_impl, FairSpillPool oom, and AST pin that run_repark_sql
        does not retry make_session or a different query.
      artifacts: [test_run_repark_sql_does_not_retry_a_different_query, task/window-bench-report-2026-08-31.md]
    - id: AT-4
      status: N/A
      justification: measure-only single-process bench; no shared mutable session protocol
    - id: AT-5
      status: N/A
      justification: local filesystem only; never AWS, never credentials
    - id: AT-6
      status: ATTACKED
      evidence: >
        Seeded generator is deterministic; refuse set frozen and equal to
        registry headings so a DataFusion sliding-accumulator change reds.
      artifacts: [test_seed_table_is_deterministic_and_typed, test_sliding_refuse_set_matches_the_frozen_roster]
    - id: AT-7
      status: ATTACKED
      evidence: >
        1e7 unpartitioned cell and 16M memory_limit cell were executed; oom
        is recorded rather than left as a hang. Not a performance review.
      artifacts: [task/window-bench-report-2026-08-31.md]
    - id: AT-8
      status: ATTACKED
      evidence: >
        DuckDB 1.5.5 and PySpark 4.1.2 pins; fetch_arrow_table vs the 1.5
        RecordBatchReader; count(*) wrap elision vs sum(w) sink.
      artifacts: [python/repark-parity/bench/windows/oracles.py, python/repark-parity/bench/windows/roster.py]
    - id: AT-9
      status: ATTACKED
      evidence: >
        Outcome classes and engine messages are in the dated report; oom
        message names FairSpillPool and datafusion.runtime.memory_limit.
      artifacts: [task/window-bench-report-2026-08-31.md]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Engine-free pins on make py-test; native smoke on the facade tree;
        live refuse set equals the frozen twelve.
      artifacts: [python/repark-parity/tests/test_w0_window_bench.py, python/repark/tests/test_w0_window_bench_smoke.py]
  reattested: []
```

## 4. Self Logic Review (conclude)

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-W0-CONCLUDE
  agent: Actor
  action: file measurements, registry rows, and PROVEN ledger; run gates
  charter_trace: C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
  preconditions:
    - full-scale run completed with scratch deleted: SATISFIED (report wall_seconds 139.9, scratch_deleted True)
    - no crates/ edit: SATISFIED (unit diff)
    - twelve WIN-SLIDE headings match the live refuse set: SATISFIED (smoke pin)
  success_condition: every clause PROVEN with a pins citation; make py-test, check-map-sync, check-ledger-grammar, ledger_lifecycle check exit 0
  step_risks:
    - count(*) wrap silent false-ok on sliding refuse: HANDLED (bare SELECT + sum(w) sink)
    - 1e7 leftover data: HANDLED (cleanup_scratch; listing empty)
  contingencies:
    - over-limit oom: EXECUTABLE (recorded as outcome class oom; no engine patch)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```
