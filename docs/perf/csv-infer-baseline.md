# CSV `inferSchema` baseline — plan-time trials (CSV-INFER-PERF-1)

Scope: local `spark.read.csv(..., header=True, inferSchema=True)` after NULLABILITY-2
round 4 forced every inferSchema CSV through an all-Utf8 scan plus per-column trial
casts. Each trial called `to_arrow()` on the whole frame. The offset-timestamp defect
that scan exists for is timestamp-only.

This file closes when the H-3 campaign archives to `docs/history/`.

pins: csv-infer-perf-1/C-001, C-005, C-006

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X 32-Core (64 threads) |
| ram | 125 GiB |
| native | `164,981,968 B` |
| release proof | `repark._native.__debug_assertions__ is False` |
| fixture | 300,000 × 8 CSV (int, double, naive timestamp, offset timestamp, date, string, NA-int, bool), 28,630,418 B |
| iterations | 5 timed `read.csv` + `to_arrow()` |
| after load1 | 13.77, 13.10, 11.89 |

## Before / after (same fixture, release module)

| cell | before (NULLABILITY-2 tree) | round 1 | round 2 | round 3 |
|---|---:|---:|---:|---:|
| `inferSchema=False` median total | 0.086 s | 0.083 s | 0.080 s | 0.077 s |
| `inferSchema=True` median total | 2.339 s | 0.079 s | 0.153 s | **0.155 s** |
| True / False | 27.2× | 0.95× | 1.90× | **2.01×** |
| plan-time `to_arrow` / `collect` | 34 / 0 | 0 / 0 | 1 / 0 | **1 / 0** |

Round 3 (2026-09-06), same release native, five `read.csv`+`to_arrow` medians:

| fixture | False | True | True/False | plan `to_arrow` |
|---|---:|---:|---:|---:|
| 300k × 8 (`i,d,nts,ots,dt,s,ni,b`, 28,630,418 B) | 0.077 s | 0.155 s | **2.01×** | 1 |
| 300k × 3 typed (`i,d,nts`) | 0.097 s | 0.145 s | 1.49× | 1 |
| 300k × 3 with string (`i,s,ni`) | 0.050 s | 0.096 s | 1.91× | 1 |

The 2× bar is missed on the ×8 fixture by box noise after leftover numeric grammar
runs at every width (round 2 met 1.90× by skipping leftover on native-Utf8 columns
when the file had more than four columns — Spark-wrong at width 8). The wall pin is
a 0.5 s regression guard against the 2.3 s NULLABILITY-2 path, not a 2× claim.
`schema_infer_max_records(usize::MAX)` on the ×8 file was 1.089 s (13× False) and
was not kept.

`nullValue` still uses one aggregation of `try_cast` failure counts (one `to_arrow` of a
1-row stats frame), not per-column trials.

## Choice

Round 3 (critic R2-S1a/S1b): leftover numeric grammar on every Utf8 column at every
width, one aggregation; `utf8_columns` re-read uses the first-record all-Utf8 schema
so `multiLine` past 1000 records does not infer again. Offsets stay `try_cast`
timestamp on the raw text. `nullValue` still Utf8-forces the whole scan.
(a) sample only — Spark-wrong past row 1000 (round-1 FAIL).
(b) all-Utf8 + one agg — 2.21× on this box.
Width-gated leftover — Spark-wrong `Inf`/`+5` at 8 columns (round-2 FAIL).
MAX infer — 13× on this box.

## Reproduce

```
cd python/repark && VIRTUAL_ENV=$PWD/../../.venv maturin develop --release
# then the 300k fixture + five read.csv+to_arrow timings in
# python/repark/tests/test_csv_infer_perf_1.py::test_infer_schema_true_stays_within_twice_false
```
