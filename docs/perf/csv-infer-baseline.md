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

| cell | before (NULLABILITY-2 tree) | after (CSV-INFER-PERF-1) |
|---|---:|---:|
| `inferSchema=False` median total | 0.086 s | 0.083 s |
| `inferSchema=True` median total | 2.339 s | **0.079 s** |
| True / False | 27.2× | **0.95×** (bar ≤ 2×) |
| True plan-time share | 2.261 s (96.7 %) | 0.006 s |
| plan-time `to_arrow` / `collect` | 34 / 0 | **0 / 0** |

Before False samples: 0.333, 0.096, 0.084, 0.086, 0.082 s. True: 2.320, 2.460, 2.192, 2.339, 2.414 s.
After False: 0.122, 0.090, 0.083, 0.078, 0.077 s. True: 0.079, 0.084, 0.079, 0.077, 0.087 s.

`nullValue` still uses one aggregation of `try_cast` failure counts (one `to_arrow` of a
1-row stats frame), not per-column trials.

## Choice

(a) native DataFusion inference, Utf8 only for columns it inferred as Timestamp, CAST the
raw text so offsets survive; full-file promotion only when `nullValue` is set.
(b) one `try_cast` failure-count aggregation (measured 2.21× on this box — missed the 2× bar).
(c) sample-then-validate — DataFusion already samples 1000 rows; a second sample would miss
late type conflicts the `nullValue` path must still catch by scanning.

## Reproduce

```
cd python/repark && VIRTUAL_ENV=$PWD/../../.venv maturin develop --release
# then the 300k fixture + five read.csv+to_arrow timings in
# python/repark/tests/test_csv_infer_perf_1.py::test_infer_schema_true_stays_within_twice_false
```
