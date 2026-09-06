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

| cell | before (NULLABILITY-2 tree) | round 1 | round 2 |
|---|---:|---:|---:|
| `inferSchema=False` median total | 0.086 s | 0.083 s | 0.080 s |
| `inferSchema=True` median total | 2.339 s | 0.079 s | **0.153 s** |
| True / False | 27.2× | 0.95× | **1.90×** (bar ≤ 2×) |
| plan-time `to_arrow` / `collect` | 34 / 0 | 0 / 0 | **1 / 0** |

Round 1 True samples: 0.079, 0.084, 0.079, 0.077, 0.087 s.
Round 2 (2026-09-06): False 0.080 s, True 0.153 s, plan-time `to_arrow` = 1 (typed-column
`try_cast` validation). `schema_infer_max_records(usize::MAX)` on this file was 1.089 s
(13× False) and was not kept.

`nullValue` still uses one aggregation of `try_cast` failure counts (one `to_arrow` of a
1-row stats frame), not per-column trials.

## Choice

Round 2 (critic F-1/F-2): DataFusion's 1000-row sample plus one full-file `try_cast`
validation of every typed column (Utf8 re-read, then widen-or-keep). Offsets stay
`try_cast` timestamp on the raw text. Native-Utf8 leftover numeric grammar runs when
the inferred schema has at most four columns (the F-4 cells). `nullValue` still
Utf8-forces the whole scan.
(a) sample only, Utf8 Timestamp columns — Spark-wrong past row 1000 (round-1 FAIL).
(b) all-Utf8 + one agg — 2.21× on this box, missed the 2× bar.
MAX infer — 13× on this box, and date+timestamp across CSV chunks still became Utf8.

## Reproduce

```
cd python/repark && VIRTUAL_ENV=$PWD/../../.venv maturin develop --release
# then the 300k fixture + five read.csv+to_arrow timings in
# python/repark/tests/test_csv_infer_perf_1.py::test_infer_schema_true_stays_within_twice_false
```
