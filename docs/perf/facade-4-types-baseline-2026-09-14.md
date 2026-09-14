# FACADE-4 type-conversion baseline (step 0, 2026-09-14)

Release-module baseline for the type-conversion boundary the FACADE-4 unit
weighs — Arrow schema ↔ Spark types ↔ DDL. Produced by the committed runner
[facade-4-types-baseline-2026-09-14/run_baseline.py](facade-4-types-baseline-2026-09-14/run_baseline.py);
each cell waited for an idle box (no cargo/rustc/maturin) before timing,
fixtures were built outside the timed region, and every number is a median of
5 reps after one warmup. The whole run ran under
`systemd-run --user --scope -p MemoryMax=8G -p MemorySwapMax=0` with
`OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8`. No product code changed.

pins: facade-4/C-005

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X 32-Core (64 threads) |
| kernel | 6.8.0-138-generic |
| python / pyarrow | 3.12.3 / 25.0.0 |
| native | `167,915,168 B`; `repark._native.__debug_assertions__ is False` |
| session | `spark.sql.session.timeZone = UTC`, `spark.sql.timestampType = TIMESTAMP_LTZ` |
| run load1 | 2.26 → 2.44 at cell starts (per-cell loads in the JSON) |

Schema set (the card's F1 set): `flat7` (int, bigint, double, string, boolean,
date, timestamp), `wide50` (50 columns cycling ten types), `nested3`
(`struct<top:int, mid:array<struct<m:map<string,struct<leaf:decimal(10,2)>>, arr:array<bigint>>>>` —
depth 3 `struct<array<map>>`), `decimal_variants` ((10,2)/(38,18)/(38,0)),
`timestamp_variants` (LTZ + NTZ), `interval_char_varchar` (year-month,
day-time, calendar, `char(8)`, `varchar(32)`).

## How to reproduce

```bash
OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 \
systemd-run --user --scope -p MemoryMax=8G -p MemorySwapMax=0 \
  .venv/bin/python docs/perf/facade-4-types-baseline-2026-09-14/run_baseline.py
```

## Cell (a) — `repark_type_to_arrow` per schema and per user op

Per-call cost (µs, median):

| schema | µs/call |
|---|---:|
| flat7 | 9.46 |
| wide50 | 59.87 |
| nested3 | 17.23 |
| decimal_variants | 7.08 |
| timestamp_variants | 7.36 |
| interval_char_varchar | 8.72 |

Calls per user op on a 1e5-row frame (spy on `types.repark_type_to_arrow`;
ops timed at reps=3, count divided by 4 invocations):

| op | calls per op | op wall (ms) | conversion share |
|---|---:|---:|---:|
| `createDataFrame(pandas)` 1e5 | 0 | 475.79 | 0 % |
| `createDataFrame(rows, StructType)` nested 1e5 | 16 | 2,089.44 | ≈0.16 ms → 0.008 % |
| `collect` 1e5 | 0 | 345.70 | 0 % |
| `to_arrow` 1e5 | 0 | 0.28 | 0 % |
| `show` 1e5 | 0 | 3.18 | 0 % |
| `df.schema` | 0 | 0.017 | 0 % |

`repark_type_to_arrow` is reached only by the explicit-schema
`createDataFrame` path (`_data_type_to_sql_type` emits ARRAY/MAP/STRUCT
markers that `_sql_type_to_arrow` routes through it — 16 calls for the
nested schema). Every other measured op pays zero Python conversion calls:
the pandas path converts dtypes in `_arrow_table_from_pandas`, and
`collect`/`to_arrow`/`show`/`df.schema` read the already-materialized Arrow
schema.

## Cell (b) — `struct_type_from_arrow` round-trip per schema

| schema | arrow→spark µs | spark→arrow→spark µs |
|---|---:|---:|
| flat7 | 14.74 | 19.59 |
| wide50 | 99.30 | 134.42 |
| nested3 | 20.39 | 36.33 |
| decimal_variants | 8.45 | 12.13 |
| timestamp_variants | 8.23 | 12.35 |
| interval_char_varchar | 10.34 | 13.59 |

## Cell (c) — DDL parse and DDL write walls

Per-call µs (median): `fromDDL`/`_parse_datatype_string` are the same function
(`fromDDL` wraps it); the `fromddl_ddl` column parses `StructType.toDDL()`
output — which for non-nullable fields emits `NOT NULL` that the parser
refuses, so that column's cost is the refusal path (see census D15).

| schema | parse simpleString µs | parse toDDL µs | `simpleString()` µs | `toDDL()` µs |
|---|---:|---:|---:|---:|
| flat7 | 16.65 | 19.03 | 3.16 | 3.43 |
| wide50 | 124.72 | 81.35 | 19.96 | 25.21 |
| nested3 | 33.83 | 15.46 | 2.86 | 7.06 |
| decimal_variants | 14.31 | 15.47 | 1.60 | 3.20 |
| timestamp_variants | 10.07 | 11.30 | 1.76 | 2.15 |
| interval_char_varchar | 12.65 | 15.33 | 2.05 | 5.99 |

Atomic parses: `timestamp` 1.65 µs, `decimal(38,18)` 2.15 µs, `char(8)`
2.00 µs, `interval day to second` 8.65 µs (refusal path — unparsable on this
base, census D14).

## Cell (d) — conversion share of end-to-end walls (cProfile cumulative)

Conversion set: every function in `types.py`, `create_dataframe_inference.py`,
`create_dataframe_schema.py`, `_csv_smart.py`, `reader.py`/`reader_support.py`
whose name is a conversion entry (`_arrow_type_to_repark`,
`repark_type_to_arrow`, `struct_type_from_arrow`, `_parse_datatype_string`,
`fromDDL`, `simpleString`, `toDDL`, `typeName`, `jsonValue`,
`_data_type_to_sql_type`, `_sql_type_to_arrow`, `resolve_column_type`,
`rung_to_*`, `try_rung`, `resolve_cell_rung`, `_engine_type`,
`_normalize_nested_sql_type_aliases`). cProfile cumulative time summed over
that set; op wall is the profiled region.

| op | wall (ms) | conversion cum (ms) | share |
|---|---:|---:|---:|
| `df.schema` | 0.066 | 0.033 | 50 % of a 66 µs op |
| `createDataFrame(pandas)` 1e5 | 1,650.21 | 0.0 | 0 % |
| `spark.read.csv(inferSchema)` 1e5 | 2,270.22 | 0.14 | 0.006 % |

## Wall verdict — none

The card's wall test is *conversion ≥ 5 % of an end-to-end wall, or ≥ 1 ms per
user call*. **No cell is a wall.**

- ≥ 1 ms per user call: nothing. The dearest single conversion is a `wide50`
  `struct_type_from_arrow` round-trip at 134 µs and a `wide50`
  `_parse_datatype_string` at 125 µs — an order of magnitude under the bar.
- ≥ 5 % of an end-to-end wall: only `df.schema` trips the ratio (0.033 ms of
  0.066 ms), and the whole op is 66 µs — the share is of a sub-100 µs call, so
  the absolute cost stays ~0.03 ms per user call. The 1e5-row walls
  (`createDataFrame` 1,650 ms, `read.csv` 2,270 ms) carry 0.000–0.006 %
  conversion — schema conversion is a per-call cost the row count does not
  multiply.

Step-1 consequence: option (B) — no measured wall; FACADE-4 ships as the
correctness consolidation. The entry points and the census disagreements
needing an owner ruling are in the ledger's step-1 target list
([../../task/ledgers/staging/facade-4-ledger.md](../../task/ledgers/staging/facade-4-ledger.md)).
