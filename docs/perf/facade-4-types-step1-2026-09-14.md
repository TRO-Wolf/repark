# FACADE-4 type-conversion before/after (step 1, 2026-09-14)

Step-1 re-measure of the type-conversion boundary after the consolidation:
Arrow schema ↔ Spark descriptor ↔ DDL now read one Rust table
(`crates/repark-spark/src/type_table.rs`) through `repark-python` native
functions. Same runner as the step-0 baseline —
[facade-4-types-baseline-2026-09-14/run_baseline.py](facade-4-types-baseline-2026-09-14/run_baseline.py) —
run back to back on this box, base `/tmp/f-types4` (release native of the
step-0 product) then branch `/tmp/f-types4s1`, each cell behind an idle-box
wait, medians of 5 reps after one warmup, the whole run under
`systemd-run --user --scope -p MemoryMax=8G -p MemorySwapMax=0` with
`OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8`.

The bar (from the unit brief): no end-to-end cell slower by more than 5 %,
and no per-call conversion slower by more than 1 ms. **Both bars pass.**

pins: facade-4/C-014

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X 32-Core (64 threads) |
| kernel | 6.8.0-138-generic |
| python / pyarrow | 3.12.3 / 25.0.0 |
| base native | `167,915,168 B` (release, step-0 product) |
| branch native | `168,011,280 B` (release, `__debug_assertions__ is False`) |
| session | `spark.sql.session.timeZone = UTC`, `spark.sql.timestampType = TIMESTAMP_LTZ` |

## Cell (a) — `repark_type_to_arrow`

Per-call µs (median of 5):

| schema | base µs | branch µs | Δ |
|---|---:|---:|---:|
| flat7 | 9.4 | 27.2 | +17.8 µs |
| wide50 | 60.7 | 170.2 | +109.5 µs |
| nested3 | 17.4 | 31.8 | +14.4 µs |
| decimal_variants | 7.1 | 14.8 | +7.6 µs |
| timestamp_variants | 7.4 | 13.8 | +6.4 µs |
| interval_char_varchar | 8.8 | 21.6 | +12.8 µs |

Calls per user op on a 1e5-row frame (spy on `types.repark_type_to_arrow`):

| op | calls base → branch | wall base ms | wall branch ms | Δ |
|---|---:|---:|---:|---:|
| `createDataFrame(pandas)` 1e5 | 0 → 0 | 478.2 | 479.8 | +0.3 % |
| `createDataFrame(rows, StructType)` nested 1e5 | 16 → 6 | 2149.9 | 2098.1 | −2.4 % |
| `collect` 1e5 | 0 → 0 | 349.7 | 350.2 | +0.1 % |
| `to_arrow` 1e5 | 0 → 0 | 0.28 | 0.29 | +3.5 % |
| `show` 1e5 | 0 → 0 | 4.02 | 3.36 | −16.2 % |
| `df.schema` | 0 → 0 | 0.013 | 0.013 | +3.2 % |

The nested createDataFrame now makes 6 table calls per op instead of 16 — the
per-field Arrow walk collapsed into one schema conversion plus descriptor
reads. Its wall is unchanged within noise.

## Cell (b) — `struct_type_from_arrow` and round-trip

| schema | metric | base µs | branch µs | Δ |
|---|---|---:|---:|---:|
| flat7 | from_arrow | 14.9 | 28.1 | +13.3 µs |
| flat7 | round_trip | 19.7 | 52.2 | +32.5 µs |
| wide50 | from_arrow | 97.6 | 173.0 | +75.3 µs |
| wide50 | round_trip | 133.9 | 330.5 | +196.5 µs |
| nested3 | from_arrow | 20.5 | 36.0 | +15.5 µs |
| nested3 | round_trip | 36.3 | 72.0 | +35.7 µs |
| decimal_variants | from_arrow | 8.4 | 12.6 | +4.2 µs |
| decimal_variants | round_trip | 12.2 | 24.0 | +11.8 µs |
| timestamp_variants | from_arrow | 8.3 | 14.0 | +5.6 µs |
| timestamp_variants | round_trip | 12.4 | 24.0 | +11.6 µs |
| interval_char_varchar | from_arrow | 10.4 | 20.3 | +10.0 µs |
| interval_char_varchar | round_trip | 13.6 | 36.7 | +23.0 µs |

## Cell (c) — DDL parse and write

| schema | metric | base µs | branch µs | Δ |
|---|---|---:|---:|---:|
| atomic `char(8)` | fromDDL | 2.01 | 2.03 | +0.02 µs |
| atomic `decimal(38,18)` | fromDDL | 2.12 | 2.32 | +0.20 µs |
| atomic `interval day to second` | fromDDL | 8.88 | 3.57 | −5.31 µs |
| atomic `timestamp` | fromDDL | 1.72 | 1.61 | −0.11 µs |
| flat7 | fromDDL | 17.09 | 13.92 | −3.17 µs |
| flat7 | `_parse_datatype_string` | 16.98 | 13.85 | −3.13 µs |
| flat7 | toDDL | 19.31 | 14.92 | −4.39 µs |
| flat7 | simpleString | 3.15 | 17.07 | +13.91 µs |
| flat7 | ddl_token | 3.52 | 16.96 | +13.44 µs |
| wide50 | fromDDL | 125.67 | 83.94 | −41.73 µs |
| wide50 | `_parse_datatype_string` | 125.04 | 83.80 | −41.23 µs |
| wide50 | toDDL | 82.00 | 6.06 | −75.95 µs |
| wide50 | simpleString | 19.47 | 107.25 | +87.78 µs |
| wide50 | ddl_token | 25.07 | 106.79 | +81.71 µs |
| nested3 | fromDDL | 33.97 | 16.72 | −17.25 µs |
| nested3 | `_parse_datatype_string` | 33.49 | 16.19 | −17.30 µs |
| nested3 | toDDL | 15.73 | 3.79 | −11.93 µs |
| nested3 | simpleString | 2.83 | 19.51 | +16.68 µs |
| nested3 | ddl_token | 6.96 | 19.38 | +12.42 µs |
| decimal_variants | fromDDL | 14.46 | 9.83 | −4.63 µs |
| decimal_variants | `_parse_datatype_string` | 14.41 | 9.71 | −4.70 µs |
| decimal_variants | toDDL | 15.54 | 10.14 | −5.40 µs |
| decimal_variants | simpleString | 1.60 | 10.10 | +8.50 µs |
| decimal_variants | ddl_token | 3.09 | 9.93 | +6.84 µs |
| timestamp_variants | fromDDL | 10.31 | 7.72 | −2.58 µs |
| timestamp_variants | `_parse_datatype_string` | 10.20 | 7.62 | −2.58 µs |
| timestamp_variants | toDDL | 11.38 | 8.04 | −3.34 µs |
| timestamp_variants | simpleString | 1.75 | 8.52 | +6.77 µs |
| timestamp_variants | ddl_token | 2.24 | 8.41 | +6.17 µs |
| interval_char_varchar | fromDDL | 13.00 | 2.97 | −10.03 µs |
| interval_char_varchar | `_parse_datatype_string` | 12.75 | 2.78 | −9.97 µs |
| interval_char_varchar | toDDL | 15.36 | 3.83 | −11.52 µs |
| interval_char_varchar | simpleString | 2.05 | 14.64 | +12.60 µs |
| interval_char_varchar | ddl_token | 6.03 | 14.58 | +8.55 µs |

Parsing is faster than the Python regex path on every schema (the table's
regexes are compiled once into a process-local cache). `simpleString` /
`ddl_token` pay one descriptor build plus one native call where the old code
joined Python strings; worst case +87.8 µs on wide50, under the 1 ms bar.

## Cell (d) — end-to-end share on 1e5 rows

| op | metric | base | branch | Δ |
|---|---|---:|---:|---:|
| `createDataFrame(pandas)` | wall ms | 1676.7 | 1655.4 | −1.3 % |
| `createDataFrame(pandas)` | conversion cum ms | 0.000 | 0.000 | 0 |
| `df.schema` | wall ms | 0.073 | 0.062 | −14.5 % |
| `df.schema` | conversion cum ms | 0.032 | 0.017 | −45.7 % |
| `spark.read.csv(inferSchema)` | wall ms | 2284.3 | 2284.9 | +0.0 % |
| `spark.read.csv(inferSchema)` | conversion cum ms | 0.144 | 0.203 | +40.8 % (+0.059 ms) |

Every end-to-end wall is within ±5 %; the worst per-call conversion delta is
wide50's round-trip at +196.5 µs, five times under the 1 ms bar. The CSV
inference conversion share rose by 0.059 ms absolute — recorded, under both
bars.

## Thinned line counts

| file | step-0 lines | step-1 lines | Δ |
|---|---:|---:|---:|
| `spark/types.py` | 1834 | 1833 | −1 |
| `spark/_csv_smart.py` | 899 | 868 | −31 |
| `session/timestamp_type.py` | 97 | 99 | +2 |
| `session/create_dataframe_values.py` | 569 | 513 | −56 |
| `session/create_dataframe_inference.py` | 737 | 711 | −26 |

`types.py` net is −1 against its exact-baseline ratchet: ~280 lines of
per-class `simpleString`/`_engine_type` overrides and the Python DDL parser
came out; roughly the same number went back in as descriptor encode/decode
helpers, native-call wrappers and the Python residue paths (JSON surfaces,
unknown-subtype fallbacks, wide-decimal handling). `check_lib_py.py` records
the new exact baseline 1833. Body-hash baselines were refreshed only for the
two functions whose bodies moved: `_data_type_to_sql_type`
(`a57ec32664f64e2427a8853d70ae0316a915b024593a1562ffa3d19326d2dcf4`) and
`_sql_type_to_arrow`
(`55d6c07af323e4ee1bce844d0d9f92ade8baa2468088fad9f3a0856205d52fe0`).

## Notes for the record

- A first run of this comparison (before the regex cache landed) showed
  `fromDDL` at 100–300× the base — the parser compiled each regex per call.
  The table now compiles each pattern once behind a `OnceLock` cache; the
  numbers above are post-fix. The lesson generalizes: any Rust port of a
  Python regex path must cache compilation or it loses to `re`'s cache.
- The first branch run also printed `createDataFrame(pandas)` +30.9 % and
  `collect` +9.6 % on cells where the spy counts zero conversion calls —
  ambient load from a parallel lane's build, not conversion cost. A second
  back-to-back run (the numbers above) shows +0.3 % / +0.1 % on the same
  cells.
- Remaining Python-side residue (by design, not by performance): JSON
  surfaces, descriptor trees containing foreign `DataType` subtypes, and
  decimals outside the Arrow FFI storage envelope. Enumerated in the ledger
  residue table (C-015).
