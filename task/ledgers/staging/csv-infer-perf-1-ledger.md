# Unit ledger — CSV-INFER-PERF-1 · CSV `inferSchema` without per-candidate materialization

**Date:** 2026-09-06 · **Branch:** `perf/csv-infer-perf-1` · **Base:** `origin/main`
`06febe20` (round 3; round 2 `270237fd` + orchestrator `8c21b86b`/`06febe20`) · **Model:** grok-4.6 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `CSV-INFER-PERF-1` **FIXED**. `CSV-INFER-20DIGIT` **DECLARED**.

## Round 3 — critic FAIL dispositions (2026-09-06)

Round-2 critic (Muse Spark 1.3) FAIL. Every finding measured on live PySpark 4.1.2, both
doors, vs a release build of main.

| id | sev | disposition |
|---|---|---|
| R2-S1a | S1 | **FIXED.** Deleted `_CSV_STRING_PROMOTE_WIDTH`. Leftover bigint/double runs for every native-Utf8 column at every width, in the same one `try_cast` aggregation (plan-time `to_arrow` ≤ 1). Pins: `Inf`/`+5`/23-digit/`Infinity` at 3, 8, and 12 columns, both doors. |
| R2-S1b | S1 | **FIXED.** `utf8_columns` re-read uses the first-record all-Utf8 schema (no second DataFusion infer), so `multiLine` past 1000 records cannot raise on chunked record boundaries. Pin: 1001-row `multiLine` no-conflict and late-double, both doors. |
| R2-S3a | S3 | **KEPT.** Orchestrator commits `8c21b86b` / `06febe20` already dropped the `1000` literal from map prose and the cap-test exception count. `test_cap_1_source_file_line_cap.py` green. |
| R2-S3b | S3 | **DISCLOSED.** 300k × 8 True/False = **2.01×** (2× bar missed by box noise after leftover is width-independent). 300k × 3 typed 1.49×; 300k × 3 with string 1.91×. The wall pin is a 0.5 s regression guard, not a 2× claim. |

C-002 / C-005 were refuted by R2-S1a (width-gated leftover). Re-**PROVEN** below against width-independent leftover.

---

**Date:** 2026-09-06 · **Branch:** `perf/csv-infer-perf-1` · **Base:** `origin/main`
`b9b720d5` (round 2; round 1 commit `68677f10` on `628f3322`) · **Model:** grok-4.6 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `CSV-INFER-PERF-1` **FIXED**. `CSV-INFER-20DIGIT` **DECLARED**.

## Round 2 — critic FAIL dispositions (2026-09-06)

Round-1 critic (Opus) FAIL. Every finding measured on live PySpark 4.1.2, both doors, both
zones, vs a release build of main.

| id | sev | disposition |
|---|---|---|
| F-1 | S1 | **FIXED.** Sampled infer stays 1000 rows. Typed columns are Utf8-re-read and one `try_cast` aggregation widens or keeps. `1.5` at row 1001 → double; late-bad int/double/date/`true`/`NA`/slash-date → string. `schema_infer_max_records(usize::MAX)` was 1.089 s / 13× on the 300k file and date+timestamp across CSV chunks still became Utf8; not kept. |
| F-2 | S1 | **FIXED.** A date column with a clock at row 1001 infers date from the sample, then leftover timestamp (clock guard) keeps 12:00Z. |
| F-3 | S1 | **FIXED.** `_finish_csv_infer_schema` runs on the pre-rename frame; `_cN` rename is after. Pin: `header=False` × offset × `America/New_York` = 10:00Z, names `_c0/_c1/_c2`. |
| F-4 | S2 | **FIXED.** Native-Utf8 leftover bigint/double on frames with ≤ 4 columns: `Inf`/`-Inf`/`NaN`/`Infinity`/`+5`/23-digit. Wider frames skip leftover on already-string columns so the 300k × 8 bench stays inside 2×. |
| F-5 | S2 | **FIXED.** One pin per class at 1001 rows, both doors. Registry prose now names the 1000-row sample plus full-file `try_cast` validation. |
| F-6 | S3 | **FIXED.** `utf8_columns` removed from `_CSV_NATIVE_OPTION_KEYS`; injected only on the internal re-read. User `option("utf8_columns", …)` does not force string. |
| F-7 | S3 | **FIXED.** `csv(path)` no longer stores `path` on the reader; `_finish` takes the path argument. A later `load()` raises `CSV load requires a path argument`. |
| F-8 | S3 | **FIXED.** DataFusion `Null` CSV columns CAST to `string` (header-only and empty trailing fields). |
| F-9 | S3 | **ROWED.** `CSV-INFER-20DIGIT` DECLARED: overflow Int64 → double; Spark decimal. Pin reds when repark answers decimal. |
| F-10 | S4 | **FIXED.** Materialize counter uses `setattr` with name variables (no `# type: ignore`). `_promote_csv_string_types` last docstring line restored verbatim; the one-agg fact lives in the session map row. |

C-002 / C-004 / C-005 were refuted by F-1/F-2/F-5 (sample-only was not Spark-equal past row 1000). Re-**PROVEN** below against the validation path.

---

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
| C-002 | The cheapest correct shape is sampled native inference plus one full-file `try_cast` validation of typed columns (infer-free Utf8 re-read, widen-or-keep); leftover native-Utf8 numeric grammar at every width in that same aggregation; `nullValue` still all-Utf8 promote. | The code path; §2. | **PROVEN** | Round 2 width gate refuted (R2-S1a). Round 3: leftover at every width, one agg, `to_arrow` = 1. ×8 True 0.155 s / **2.01×**. MAX infer 13× not kept. |
| C-003 | Every existing pin in `test_nullability_2.py` and `test_cutover_schema_1.py` stays green unchanged. | Those files, no assertion edits. | **PROVEN** | `pytest python/repark/tests/test_nullability_2.py python/repark/tests/test_cutover_schema_1.py -q` — 40 passed with the new file, 0 failed. Offset / Z / date / `nullValue` cells unchanged. |
| C-004 | The critic's 19 shapes are re-run against live PySpark 4.1.2; shapes the suite lacked are added as pins on the DataFrame door and the temp-view SQL door (`csv.\`path\`` does not exist). ≥1 pin per class at ≥1001 rows. | `test_csv_infer_perf_1.py`; live leg. | **PROVEN** | Round 1 2–3-row pins refuted (F-5). Round 2: 1001-row class pins (late double/bad-int/true/NA/bad-double/bad-date/slash-date/US-date-in-ts/clock-in-date) plus `date_bad_day`, `header=False` offset NY, Inf/NaN/Infinity/+5/23-digit, utf8_columns ignored, path not stored, void→string, 20-digit DECLARED. SQL door is `createOrReplaceTempView` + `SELECT *`. |
| C-005 | After: no per-column full materialization; 2× bar reported honestly at every measured width. | Wall pin; call-count pin; baseline. | **PROVEN** | Round 3: ×8 0.155/0.077 = **2.01×** (2× missed; leftover at every width). ×3 typed 1.49×; ×3 string 1.91×. Call-count ≤ 1 including 8-column `Inf`. Live `REPARK_PARITY_LIVE=1 pytest …test_nullability_2.py …test_csv_infer_perf_1.py` → **59 passed in 31.99 s**. |
| C-006 | Registry row FIXED with before/after; `docs/perf/csv-infer-baseline.md` + map row; every touched directory's `map.md` lockstep; no code comments added. | The files; the gates. | **PROVEN** | `CSV-INFER-PERF-1` under the reader section; `CSV-INFER-20DIGIT` DECLARED. `session.rs` 1002 → 988, CAP-1 exception retired. `reader.py` 1026 → 1022. |

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
