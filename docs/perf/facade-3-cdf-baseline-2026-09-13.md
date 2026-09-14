# FACADE-3 createDataFrame baseline (step 1, 2026-09-13)

Release-module baseline for the `createDataFrame` dispatch shapes the FACADE-3 unit
weighs. Produced by the committed runner
[facade-3-cdf-baseline-2026-09-13/run_facade_3_cdf.py](facade-3-cdf-baseline-2026-09-13/run_facade_3_cdf.py);
the runner refuses to measure a debug module, waits for an idle box (no
cargo/rustc/maturin) before every timed cell, builds fixtures outside the timed
region, and reports medians of three reps after one warmup.

pins: facade-3/C-001, C-007

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X 32-Core (64 threads) |
| kernel | 6.8.0-138-generic |
| python / pyarrow | 3.12.3 / 25.0.0 |
| native | `167,472,456 B`; `repark._native.__debug_assertions__ is False` |
| build | `cd python/repark && VIRTUAL_ENV=$PWD/../../.venv uvx maturin@1.14.1 develop --release` |
| session | `spark.sql.shuffle.partitions = 8`, `spark.sql.session.timeZone = UTC` |
| run load1 | **12.19 → 4.44** — a sibling Rust build drained during the run; per-cell loads are in the JSON |
| floor | **17.45 ms** — spread of 3 repeated medians of `pandas/100000/count` |

Fixture: deterministic 7-column rows — int, float, string, bool, date, timestamp
(naive), `Decimal` scale 2 — at 1e4 and 1e5 rows. `nested` rows are
`(int, list<int>, dict, tuple)` so the slow-column arm is exercised. The pandas
control uses typed columns only (`int64`/`float64`/`string`/`bool`/`datetime64[us]` +
`ArrowDtype` date32/decimal128); polars is the equivalent typed frame. **This is a
richer fixture than the audit's** (facade-boundary §4 used ints/double/string), so
absolute numbers are not comparable across the two docs — decimal and timestamp
columns are the expensive ones everywhere below.

## How to reproduce

```bash
cd python/repark && VIRTUAL_ENV=$PWD/../../.venv uvx maturin@1.14.1 develop --release
cd ../.. && .venv/bin/python docs/perf/facade-3-cdf-baseline-2026-09-13/run_facade_3_cdf.py \
  /tmp/facade-3-cdf-baseline.json
```

## The shape table (median ms)

| cell | 1e4 create | 1e4 +count | 1e5 create | 1e5 +count |
|---|---:|---:|---:|---:|
| tuples | 74.49 | 72.83 | 735.61 | 737.65 |
| rows | 101.85 | 105.55 | 1,060.81 | 1,068.40 |
| dicts | 89.97 | 91.97 | 947.00 | 929.81 |
| tuples + DDL | 203.29 | 207.26 | 2,057.67 | 2,077.70 |
| tuples + StructType | 202.45 | 201.72 | 2,089.21 | 2,044.63 |
| nested | 254.78 | 259.19 | 2,852.45 | 2,827.12 |
| pandas (control) | 45.65 | 47.44 | 433.66 | 433.67 |
| polars (control) | 42.74 | 43.04 | 424.07 | 418.59 |

`create` and `create`+`count` are indistinguishable within noise: the Arrow table
build is the wall and the MemTable `count()` scan adds single-digit milliseconds.
All eight shapes are roughly linear in row count (1e5 ≈ 10× the 1e4 median).

## cProfile split — the three slowest 1e5 shapes

Profiled once each; wall under the profiler inflates ~3×, so read cumulative shares,
not absolute seconds.

**nested/100000 (wall 7,017 ms).** Per-cell Python everywhere:
`_prepare_nested_cell` 3,304 ms cumulative across 900k/300k calls,
`_normalize_create_dataframe_cell` 1,444 ms across 500k/300k calls,
`_slow_column_array` → the per-column tuple converter 5,222 ms,
`_infer_struct_arrow_from_dict_samples` 483 ms, `isinstance` alone 760 ms.
The fast census delegates all three nested columns to the unchanged per-cell path —
the transpose was never the wall; the per-cell normalize+prepare is.

**tuples_struct/100000 (wall 6,432 ms)** and **tuples_ddl/100000 (wall 6,400 ms)** —
the explicit-schema pair dispatches to the kept legacy path
(`_arrow_table_from_raw_tuples_legacy`) and is nearly identical: `_arrow_table_from_tuples`
3.7 s cumulative of which `_prepare_nested_cell` 3.06–3.08 s (700k calls),
the row-normalize genexpr (`create_dataframe_rows.py:713`) 2.4 s,
`_normalize_create_dataframe_cell` 2.2 s (700k calls),
`localize_naive_datetime_to_utc` 1.1 s + `active_session_time_zone` 0.84 s (one
zone-info lookup per timestamp cell), `builder_conf.get` 0.43 s,
`_validate_decimal_envelope` 0.23 s (one Decimal quantize per cell).

## What the table says (the C-007 target list)

Every shape is Python-loop bound; nothing is transport bound. Ordered by the 1e5
create median:

1. **nested — 2,852 ms.** The worst shape: three slow columns each re-walking every
   cell through `_prepare_nested_cell` and `_normalize_create_dataframe_cell`.
2. **tuples + StructType — 2,089 ms** and **tuples + DDL — 2,058 ms.** The
   explicit-schema path the audit named; on this fixture it is the per-cell
   normalize (`_normalize_create_dataframe_cell`), per-cell `_prepare_nested_cell`,
   per-cell `localize_naive_datetime_to_utc`, and per-cell decimal-envelope
   quantize — all loops a Rust inference pass would do in one pass.
3. **rows — 1,061 ms** and **dicts — 947 ms.** Same per-cell costs plus the
   `asDict()`/key-union front end; the conversion loops dominate.
4. **tuples — 736 ms.** The CDF-1 fast census handles five of seven columns; the
   timestamp column still pays `_prepare_nested_cell` per cell and the decimal
   column pays the envelope scan per cell — that is the residual wall.
5. **pandas — 434 ms** and **polars — 424 ms.** The "columnar ceiling" only holds
   for purely typed frames: the naive-timestamp column pays a per-cell
   `to_pylist()` + `localize_naive_datetime_to_utc` loop
   (`_localize_naive_timestamp_column`) and the decimal column pays a per-cell
   `to_pylist()` envelope scan (`_validate_decimal_column_envelope`). With this
   fixture the controls are Python-bound too — ~10× the audit's 3–4 ms typed-only
   anchor, and honest evidence that the per-cell helpers are the wall even where
   Arrow hands off directly.

**Step-2 targets:** the explicit-schema tuple paths (`tuples_ddl`,
`tuples_struct`) and the nested shape, then `rows`/`dicts`, through the capsule
seam — they are the largest walls and share the same per-cell helpers
(`_prepare_nested_cell`, `_normalize_create_dataframe_cell`, the decimal envelope,
the timezone localize). `tuples` and the pandas/polars controls keep their Python
census/export but their residual wall is the same per-cell timestamp/decimal
helpers, so they are covered by the same move if it is engine-generic.

## What this baseline is not

- **Not an idle-host baseline.** Load fell from 12.19 to 4.44 across the run as a
  sibling build drained; per-cell `load_start`/`load_end` are recorded in the JSON
  and the floor cell is re-measured per run.
- **Not a Spark comparison.** No cell runs a JVM.
- **Not comparable to facade-boundary §4's create cells.** Different fixture: §4's
  7 columns were int/double/string only; this doc's carry date, timestamp and
  decimal — the columns whose per-cell helpers are the wall.

## Pointers

- Up: [map.md](map.md)
- Runner: [facade-3-cdf-baseline-2026-09-13/map.md](facade-3-cdf-baseline-2026-09-13/map.md)
- Ledger: [../../task/ledgers/completed/facade-3-ledger.md](../../task/ledgers/completed/facade-3-ledger.md)
