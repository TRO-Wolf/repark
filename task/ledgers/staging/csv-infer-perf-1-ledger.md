# Unit ledger — CSV-INFER-PERF-1 · CSV `inferSchema` without per-candidate materialization

**Date:** 2026-09-06 · **Branch:** `perf/csv-infer-perf-1` · **Base:** `origin/main`
`628f3322` · **Model:** grok-4.6 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `CSV-INFER-PERF-1` **FIXED**.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** NULLABILITY-2 round 4 made every local `inferSchema` CSV (not only `nullValue`)
go through an all-Utf8 scan plus per-string-column trial casts, each calling `to_arrow()`
on the whole frame. On a 300k × 8 CSV: `inferSchema=False` 0.086 s median;
`inferSchema=True` 2.339 s, of which 2.261 s is plan-time trials (34 `to_arrow` calls).
Correctness is Spark-equal; this unit is cost.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.lock`,
dependency lists, completed ledgers.

## PROPOSITION LEDGER — CSV-INFER-PERF-1 — 2026-09-06

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Baseline first on a release native: 300k × 8 CSV, five `read.csv`+`to_arrow` runs, medians and plan-time share recorded. | §3; `docs/perf/csv-infer-baseline.md`. | **PROVEN** | `__debug_assertions__` False, native `164,981,968 B`. False median 0.086 s (plan 0.003 s, 0 `to_arrow`). True median 2.339 s (plan 2.261 s, 34 `to_arrow`). Load 28.4 / 16.7 / 14.0. |
| C-002 | The cheapest correct shape is (a): native DataFusion inference, Utf8-force only Timestamp columns, CAST raw text so offsets survive; full-file `try_cast` promotion only when `nullValue` is set. (b) and (c) are named and measured. | The code path; §2. | **PROVEN** | (b) one agg of `try_cast` failure counts: after 0.176 s / 2.21×, missed the 2× bar, plan-time `to_arrow` = 1. (c) sample-then-validate is DataFusion's 1000-row infer already; a second sample cannot replace the `nullValue` full scan. (a) after: True 0.079 s / 0.95×, plan-time `to_arrow` = 0. |
| C-003 | Every existing pin in `test_nullability_2.py` and `test_cutover_schema_1.py` stays green unchanged. | Those files, no assertion edits. | **PROVEN** | `pytest python/repark/tests/test_nullability_2.py python/repark/tests/test_cutover_schema_1.py -q` — 40 passed with the new file, 0 failed. Offset / Z / date / `nullValue` cells unchanged. |
| C-004 | The critic's 19 shapes are re-run against live PySpark 4.1.2; shapes the suite lacked are added as pins on the DataFrame door and the temp-view SQL door (`csv.\`path\`` does not exist). | `test_csv_infer_perf_1.py`; live leg. | **PROVEN** | 11 new always-run shapes (int-then-double, late-bad-int, 007→bigint, NA-without-nullValue, bool, string, offset, Z, date, `nullValue` date, `nullValue` timestamp) plus the NULLABILITY-2 zone × offset/plain/Z/DST cells. SQL door is `createOrReplaceTempView` + `SELECT *`. `csv.\`path\`` raises `table not found`. |
| C-005 | After: `inferSchema=True` within 2× of `inferSchema=False` on the 300k file; no per-column full materialization. | Wall pin; call-count pin. | **PROVEN** | Wall: True 0.079 s vs False 0.083 s (0.95×). Call-count: no-`nullValue` plan-time `to_arrow`/`collect` = 0/0; `nullValue` `to_arrow` ≤ 1. `REPARK_PARITY_LIVE=1 pytest python/repark/tests/test_nullability_2.py python/repark/tests/test_csv_infer_perf_1.py -q -rs` → **38 passed in 29.80 s**, exit 0. |
| C-006 | Registry row FIXED with before/after; `docs/perf/csv-infer-baseline.md` + map row; every touched directory's `map.md` lockstep; no code comments added. | The files; the gates. | **PROVEN** | `CSV-INFER-PERF-1` under the reader section. `session.rs` 1002 → 988, CAP-1 exception retired. `reader.py` stays 1026. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: csv-infer-perf-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the brief. The 2× bar is measured, not paraphrased, and (b) is reported as a miss that selected (a).
      artifacts: [docs/perf/csv-infer-baseline.md, task/ledgers/staging/csv-infer-perf-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Offset, Z, date-only, nullValue date/timestamp, int-then-double, late-bad-int, 007, NA-without-nullValue, bool, string, DST gap, three session zones.
      artifacts: [python/repark/tests/test_csv_infer_perf_1.py, python/repark/tests/test_nullability_2.py]
    - id: AT-3
      status: ATTACKED
      evidence: utf8_columns rewrite keeps Arrow Utf8 nullable; nullValue still Utf8-forces so NA is not type-parsed; remote/gzip still skip the local first-line scan.
      artifacts: [crates/repark-core/src/read_options.rs]
    - id: AT-4
      status: N/A
      justification: Synchronous single-session CSV read. No shared mutable state, no async spawn, no lock.
    - id: AT-5
      status: ATTACKED
      evidence: No new unsafe. Path stays in the existing CSV reader. utf8_columns is an internal option from the facade, not a user semantic option.
      artifacts: [crates/repark-core/src/read_options.rs, python/repark/src/repark/spark/session/reader_support.py]
    - id: AT-6
      status: ATTACKED
      evidence: Existing nullability-2 and cutover-schema-1 pins green; new shapes pin value AND dtypes; 007 width stays EX-IO-3 bigint vs Spark int with equal rows.
      artifacts: [python/repark/tests/test_csv_infer_perf_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Before and after from a release module on the same 300k × 8 file, five runs, medians. Call-count pin is structural.
      artifacts: [docs/perf/csv-infer-baseline.md, python/repark/tests/test_csv_infer_perf_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion CsvReadOptions.schema_infer_max_records default 1000 was read from datafusion-datasource 54.1. CAST(str AS TIMESTAMP) remains the session-zone entry point.
      artifacts: [crates/repark-core/src/read_options.rs]
    - id: AT-9
      status: ATTACKED
      evidence: No new user-facing error. utf8_columns is not a Spark option and is not rejected as unknown because it is filtered through the native option allow-list from the facade only.
      artifacts: [python/repark/src/repark/spark/session/reader_support.py]
    - id: AT-10
      status: ATTACKED
      evidence: Reverting csv_utf8_column_schema to a no-op would leave offset timestamps as timestamp_ntz and red the offset/Z pins. Reverting the call-count pin's 0 assertion reds if per-column to_arrow returns.
      artifacts: [python/repark/tests/test_csv_infer_perf_1.py, python/repark/tests/test_nullability_2.py]
  complete: true
```
