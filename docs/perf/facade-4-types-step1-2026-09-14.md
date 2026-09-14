# FACADE-4 type-conversion before/after (step 1, 2026-09-14)

Step 1 consolidates the three per-surface conversion tables (Arrow ↔ repark
types, DDL parse/write, SQL/engine/simpleString token spellings) into one
Rust table (`crates/repark-spark/src/type_table.rs` +
`type_table/parse.rs`) crossed through a PyO3 bridge
(`crates/repark-python/src/type_bridge.rs`); `spark/types.py` keeps the
public classes and thins to checks plus conversion calls
(`spark/_type_table.py`).

This document is the run-14b round-3 re-measure: the whole set — end-to-end
cells, the surface table, the micro table and the error-path shapes — run
back to back against the step-0 base release after the round-3
`dtypes` fix (literal per-class `simpleString`/`_engine_type`, ruling
R14b-D-2).

## Method

- Base: `/tmp/f-types4` release venv (`repark._native` step-0 product);
  branch: `/tmp/f-types4s1r` release venv at `ace2e19b` (round-3 fix) —
  the native is `d82c5cc6`'s build, unchanged by the Python-only fix.
- Harness: one persistent worker per side (session + 1e5-row fixtures up
  front), a driver alternating **base, branch, base, branch** per cell —
  7 repetitions each, medians, `/proc/loadavg` recorded per rep. Each cell
  started behind a no-cargo/rustc/maturin wait. A cell whose base median
  moved >5 % between its two base halves was re-run; µs-scale metrics use
  batched inner calls (50–300) per rep so timer noise stays under the
  signal.
- Both workers under `systemd-run --user --scope -p MemoryMax=8G
  -p MemorySwapMax=0`; `OPENBLAS_NUM_THREADS=8`, `OMP_NUM_THREADS=8`.
- The box is shared: pass 1 ran at load ≈ 6.1 (ambient), the flagged
  re-runs at load ≈ 3.2–3.8. Where a metric's sign flipped between passes
  (`df_schema` single-call wall, `flat7.printSchema`) a third batched
  measurement settled it — noted per table.

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X 32-Core (64 threads) |
| kernel | 6.8.0-138-generic |
| python / pyarrow | 3.12.3 / 25.0.0 |
| base native | `167,915,168 B` (release, step-0 product) |
| branch native | `168,075,016 B` (release, `__debug_assertions__ is False`) |
| session | `spark.sql.session.timeZone = UTC`, `spark.sql.timestampType = TIMESTAMP_LTZ` |

## Surface table — the ±5 % bar cells

Per-call µs, medians of 7 ABAB reps (100–300 inner calls; DESCRIBE in ms,
one call per rep). Faster-than-base is fine.

| surface | flat7 base | flat7 branch | Δ | wide50 base | wide50 branch | Δ | nested3 base | nested3 branch | Δ |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `dtypes` | 10.92 | 10.45 | −4.3 % | 62.30 | 60.14 | −3.5 % | 47.26 | 33.24 | −29.7 % |
| `df.schema` | 8.47 | 7.86 | −7.3 % | 46.78 | 44.48 | −4.9 % | 41.17 | 20.28 | −50.8 % |
| `printSchema` | 17.14 | 16.52 | −3.6 % | 99.92 | 98.10 | −1.8 % | 59.80 | 38.87 | −35.0 % |
| DESCRIBE (ms) | 0.258 | 0.254 | −1.4 % | 0.282 | 0.280 | −0.8 % | 0.254 | 0.256 | +0.7 % |

Every surface cell is inside ±5 % — most are faster than base because the
Rust table beats the Python parse that `df.schema`/`dtypes`/`printSchema`
used to run per access. `flat7.printSchema` and `frame_pd.schema` were
timer-noise-unstable at single-call granularity (−16 %/+35 % and
+17 %/−7 % across passes); batched third measurements gave −3.6 % and
+0.4 % (3.84 → 3.86 µs/call) — both inside.

nested3's base is itself anomalous (47 µs `dtypes` vs flat7's 11 µs —
the Python composition walk); the table's cached decode makes the branch
uniformly cheaper.

## Cell (a) — end-to-end walls and spy counts on 1e5 rows

Walls in ms, medians of 7 ABAB reps (one timed call each):

| op | wall base ms | wall branch ms | Δ |
|---|---:|---:|---:|
| `createDataFrame(pandas)` 1e5 | 445.6 | 445.3 | −0.1 % |
| `createDataFrame(rows, StructType)` nested 1e5 | 2139.7 | 2125.3 | −0.7 % |
| `collect` 1e5 | 345.7 | 351.9 | +1.8 % |
| `to_arrow` 1e5 | 0.294 | 0.299 | +1.4 % |
| `show` 1e5 | 3.04 | 3.10 | +2.0 % |
| `df.schema` (frame_pd, single wall) | 0.181 | 0.169 | −6.6 % |

Spy counts per op (six-name spy, identical on both sides): nested
`createDataFrame` calls `DataType.fromDDL`/`_parse_datatype_string`/
`_sql_type_to_arrow` 6× per op both sides; `repark_type_to_arrow` 16 → 6
(the table resolves once per field, not once per descriptor rebuild);
`df.schema` calls `fromDDL`/`_parse_datatype_string` once per op both
sides; `collect`/`to_arrow`/`show`/pandas-create stay 0 on all six.

## Cell (b) — `struct_type_from_arrow` and round-trip

Per-call µs, medians of 7 ABAB reps (20–50 inner calls):

| schema | metric | base µs | branch µs | Δ |
|---|---|---:|---:|---:|
| flat7 | from_arrow | 14.96 | 11.81 | −21.0 % |
| flat7 | round_trip | 25.17 | 31.84 | +26.5 % |
| wide50 | from_arrow | 101.64 | 69.08 | −32.0 % |
| wide50 | round_trip | 163.67 | 191.35 | +16.9 % |
| nested3 | from_arrow | 28.96 | 18.96 | −34.5 % |
| nested3 | round_trip | 54.36 | 53.71 | −1.2 % |

Inbound conversion is now **faster than base** on every schema — the
tagged-tuple descriptor wire plus cached kind→class decode maps removed
the per-node `PyDict` lookups (PY-P2-002). Round-trips stay under the
1 ms bar (worst wide50 +27.7 µs absolute).

## Cell (c) — DDL parse and write

Per-call µs, medians of 7 ABAB reps (50–100 inner calls). **`toddl_us`
below is the real `schema.toDDL()` series** — the earlier revision of
this document labelled `fromddl_ddl_us` rows (parsing `toDDL`'s output,
which refuses `NOT NULL` fields and so timed a refusal) as `toDDL`;
that was the PY-P3-003 mislabel, now corrected.

| schema | metric | base µs | branch µs | Δ |
|---|---|---:|---:|---:|
| flat7 | fromDDL | 20.65 | 12.99 | −37.1 % |
| flat7 | `_parse_datatype_string` | 17.11 | 10.40 | −39.2 % |
| flat7 | fromDDL(toDDL output) | 19.32 | 11.35 | −41.3 % |
| flat7 | simpleString | 3.09 | 3.87 | +25.0 % |
| flat7 | **toDDL** | 3.62 | 14.87 | +311 % |
| wide50 | fromDDL | 125.59 | 64.23 | −48.9 % |
| wide50 | `_parse_datatype_string` | 124.63 | 64.05 | −48.6 % |
| wide50 | fromDDL(toDDL output) | 81.26 | 7.31 | −91.0 % |
| wide50 | simpleString | 19.69 | 19.82 | +0.7 % |
| wide50 | **toDDL** | 25.73 | 98.52 | +283 % |
| nested3 | fromDDL | 47.42 | 17.13 | −63.9 % |
| nested3 | `_parse_datatype_string` | 47.10 | 16.92 | −64.1 % |
| nested3 | fromDDL(toDDL output) | 50.70 | 17.25 | −66.0 % |
| nested3 | simpleString | 4.61 | 9.60 | +108 % |
| nested3 | **toDDL** | 9.52 | 22.75 | +139 % |
| decimal_variants | fromDDL | 14.11 | 7.20 | −49.0 % |
| decimal_variants | toDDL | 3.15 | 8.07 | +157 % |
| timestamp_variants | fromDDL | 9.77 | 5.74 | −41.2 % |
| timestamp_variants | toDDL | 2.21 | 7.29 | +230 % |
| interval_char_varchar | fromDDL | 17.07 | 3.92 | −77.0 % |
| interval_char_varchar | toDDL | 6.03 | 12.25 | +103 % |
| atomic `timestamp` | fromDDL | 1.73 | 1.11 | −35.8 % |
| atomic `decimal(38,18)` | fromDDL | 2.14 | 1.64 | −23.2 % |
| atomic `char(8)` | fromDDL | 2.04 | 1.44 | −29.6 % |
| atomic `interval day to second` | fromDDL | 8.91 | 3.33 | −62.7 % |

Parsing is faster than the Python regex path on every schema (the table's
regexes compile once into a process-local `OnceLock` cache). `toDDL` pays
one descriptor build plus one native call per field where the old code
joined Python strings — worst case +72.8 µs on wide50, an order under the
1 ms per-call bar.

## Cell (d) — end-to-end share on 1e5 rows

cProfile walls, medians of 7 ABAB reps (one profiled call each):

| op | metric | base | branch | Δ |
|---|---|---:|---:|---:|
| `createDataFrame(pandas)` | wall ms | 1665.4 | 1650.2 | −0.9 % |
| `createDataFrame(pandas)` | conversion cum ms | 0.000 | 0.000 | 0 |
| `df.schema` | wall ms | 0.192 | 0.210 | +9.7 % * |
| `df.schema` | conversion cum ms | 0.080 | 0.079 | −1.4 % |
| `spark.read.csv(inferSchema)` | wall ms | 2265.7 | 2278.2 | +0.6 % |
| `spark.read.csv(inferSchema)` | conversion cum ms | 0.131 | 0.130 | −0.9 % |

\* the `df.schema` wall is a single 0.2 ms profiled call — its sign
flipped between passes (+16.9 % → −6.6 % → +9.7 %); the batched third
measurement is 3.84 → 3.86 µs/call (+0.4 %), inside the bar. Every
row-scale wall is inside ±5 %.

## Error-path shapes

Per-call µs, medians of 7 ABAB reps (50 inner calls); every sample's
return value verified, not just timed:

| shape | base µs | branch µs | answer |
|---|---:|---:|---|
| `fromDDL` `…not null` | 17.72 | 4.70 | refusal |
| `fromDDL` `bogus` | 5.54 | 2.57 | refusal |
| `fromDDL` `map<int>` | 4.96 | 2.39 | refusal |
| `fromDDL` `%%%not-a-type%%%` | 5.73 | 2.95 | refusal |
| `fromDDL` `interval day to second` | 15.21 | 4.30 | refusal |
| `fromDDL` `decimal(2^63,0)` | 4.12 | 5.43 | `DecimalType(9223372036854775808,0)` (Python fallback) |
| `fromDDL` control char `\x00` | 5.48 | 4.43 | refusal, `repr()` bytes exact |
| `repark_type_to_arrow` `DecimalType(76,10)` | 3.34 | 3.71 | `ValueError: precision should be between 1 and 38` |
| `repark_type_to_arrow` depth-64 array | 183.05 | 215.33 | type returned via Python fallback |

## Micro table — per-call conversion cost

µs, medians of 7 ABAB reps:

| cell | flat7 base | flat7 branch | wide50 base | wide50 branch | nested3 base | nested3 branch |
|---|---:|---:|---:|---:|---:|---:|
| `repark_type_to_arrow` | 9.56 | 19.64 | 58.70 | 118.22 | 22.60 | 30.52 |
| `struct_type_from_arrow` | 14.96 | 11.81 | 101.64 | 69.08 | 28.96 | 18.96 |
| `fromDDL` | 20.51 | 15.03 | 148.07 | 86.98 | 53.57 | 29.52 |
| `toDDL` | 3.74 | 15.30 | 26.34 | 99.48 | 9.78 | 23.20 |
| `simpleString` | 3.02 | 3.88 | 19.40 | 20.27 | 4.59 | 9.71 |

Outbound (`repark_type_to_arrow`, `toDDL`) pays the descriptor-build plus
native-call design cost — worst +59.5 µs on wide50 — while inbound is
faster than base everywhere. Every per-call figure is an order under the
1 ms bar; per-call leaf `simpleString` is back at base's ~0.05 µs (the
literal methods), which is what the surface table reflects.

## Round-3 remediation record (R14b-D-2)

The run-14b round-2 state failed the surface bar: `dtypes` measured
+13.0 % flat7 / +15.5 % wide50 on the idle-gated probe and +16–19 %
across three same-load A/B re-measures, because every atomic
`simpleString()` routed through `_atomic_token`'s row-table dict path at
~0.24 µs/leaf vs base's ~0.05 µs per-class literal method — ×50 leaves =
+12 µs on wide50. The earlier "+2 %" evidence recorded for that design
was un-reproducible.

Ruling R14b-D-2 (Q1 → option a): restore base's dispatch shape —
per-class literal `simpleString`/`_engine_type`/`typeName` methods on
every constant-answer atomic class, the five dynamic-answer classes
(Binary, Boolean, Date, Timestamp, Double) plus `DataType` keeping base's
`type(self).typeName()` bodies, parameterised classes formatting locally.
`_atomic_token`'s dict path stays only where a literal method cannot
answer: nested composition, foreign subclasses, and the SQL/DDL marker
columns. The Rust-table agreement pin and the subclass golden still pin
every answer; skipping the MRO scan still turns the marker pins red
(5 golden cells + 7 SQL markers).

Post-fix: `dtypes` is −3.5 % to −4.3 % on flat7/wide50 and −29.7 % on
nested3 (surface table above); leaf `simpleString` measures 0.052 µs vs
base's 0.053 µs. `types.py` exact baseline moved 1610 → 1772, still
below main's 1834.

## Thinned line counts

| file | step-0 lines | step-1 lines | Δ |
|---|---:|---:|---:|
| `spark/types.py` | 1834 | 1772 | −62 |
| `spark/_type_table.py` | — | 475 | +475 (new) |
| `spark/_csv_smart.py` | 899 | 875 | −24 |
| `session/timestamp_type.py` | 97 | 94 | −3 |
| `session/create_dataframe_values.py` | 569 | 518 | −51 |
| `session/create_dataframe_inference.py` | 737 | 713 | −24 |
| `crates/repark-spark/src/type_table.rs` | — | 563 | +563 (new) |
| `crates/repark-spark/src/type_table/parse.rs` | — | 473 | +473 (new) |
| `crates/repark-python/src/type_bridge.rs` | — | 408 | +408 (new) |

`types.py` net is −62 against main's 1834 ceiling: the per-class literal
methods returned under R14b-D-2 (~160 lines) after the descriptor bridge
and token table moved to `_type_table.py`. `check_lib_py.py` and the
CAP-1 mirror record the exact baseline 1772. Body-hash baselines were
refreshed only for the functions whose bodies moved
(`_data_type_to_sql_type`, `_sql_type_to_arrow`).

## Notes for the record

- A first run of this comparison (before the regex cache landed) showed
  `fromDDL` at 100–300× the base — the parser compiled each regex per
  call. The table now compiles each pattern once behind a `OnceLock`
  cache; the numbers above are post-fix. The lesson generalizes: any
  Rust port of a Python regex path must cache compilation or it loses to
  `re`'s cache.
- The shared box cycled load ≈ 3–10 through the session; every recorded
  cell carries its loadavg lines in `/tmp/r14c_abab*.json`, and cells
  whose base halves drifted >5 % were re-run per protocol.
- `repark_type_to_arrow` spy count on nested `createDataFrame` dropped
  16 → 6: the table resolves a nested schema's Arrow type in one call per
  field instead of rebuilding descriptors per node — a call-count
  improvement, not a surface change.
