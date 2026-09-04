# dynamicFlatten baseline (PERF-DYNFLATTEN-1)

Measured 2026-09-04 on this clone. Not an H-3b hard-gated baseline (reference-host
choice still blocks that). Native module is **release**.

pins: perf-dynflatten-1-measure/C-003, C-004

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X 32-Core (64 threads) |
| governor | schedutil |
| ram | 125.7 GiB |
| native | `release_or_stripped size_bytes=162505344` |
| build line | `maturin develop --release` → `Finished \`release\` profile [optimized]` |
| repark | 1.0.1 |
| pyspark | 4.1.2 |
| JAVA_HOME | `zulu-17-amd64` |
| TZ | UTC |
| seed | 42 |
| load path | Arrow parquet → `createDataFrame` (see `DYNFLATTEN-QUALNAME-1`) |

Every number below is a release number. A debug (`make develop`) module is 637 MB
against this module's 162 MB and inflates the rewrite so far that the ranking
inverts: `struct_d6` measured 5.88× Spark in debug and 0.33× in release. Debug
numbers are not evidence for this unit and are not carried here.

## 1e5 fixtures (quick; 3 iterations, 1 warmup; repark cell = one subprocess)

| shape | rows_in | repark_ms | rewrite_ms | execute_ms | rss_MiB | spark_ms | ratio | rows_out |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| struct_d3 | 100000 | 19.1 | 0.23 | 18.9 | 359 | 198.8 | 0.10 | 100000 |
| struct_d6 | 100000 | 69.5 | 0.38 | 69.2 | 388 | 209.4 | 0.33 | 100000 |
| list_struct_1 | 100000 | 30.7 | 0.21 | 30.5 | 370 | 208.7 | 0.15 | 100000 |
| list_struct_8 | 100000 | 38.3 | 0.35 | 38.0 | 858 | 364.1 | 0.11 | 589888 |
| list_struct_64 | 100000 | 109.8 | 0.34 | 109.4 | 3483 | 1968.5 | 0.06 | 4505338 |
| cartesian_two_lists | 100000 | 76.0 | 0.36 | 75.6 | 646 | 653.9 | 0.12 | 961708 |
| null_typed_list | 100000 | 8.2 | 0.18 | 8.0 | 336 | 171.7 | 0.05 | 100000 |

`ratio` is repark ÷ Spark: repark is faster than Spark on every 1e5 fixture in
release. `rss_MiB` is the repark cell's process peak; the Spark column has no RSS
because one JVM serves every fixture.

Walk counts are schema-only (Rust pins; independent of row count):

| shape | rewrite_passes | schema_walks | struct_expansions | list_explodes | unnest_nodes |
|---|---:|---:|---:|---:|---:|
| struct_d3 | 4 | 10 | 3 | 0 | 0 |
| cartesian_two_lists | — | — | 0 | 2 | 2 |

Mutation (run 2026-09-04): delete the `has_struct_columns` walk → `schema_walks`
falls 10 → 6 and `flatten_stats_depth_three_struct_counts_repeated_schema_walks`
reds, **1 red of 2** under `cargo test -p repark-core flatten_stats`. The sibling
Unnest pin stays green, so the counter is walk-specific, not a blanket assertion.

## 1e6 repark cells (one shot, warmup 0, iterations 1)

| shape | repark_ms | rewrite_ms | rows_out | peak_rss_GiB | note |
|---|---:|---:|---:|---:|---|
| struct_d3 | 60.9 | 0.45 | 1000000 | 1.4 | |
| struct_d6 | 148.0 | 0.48 | 1000000 | 1.5 | |
| list_struct_1 | 114.0 | 0.51 | 1000000 | 1.4 | |
| list_struct_8 | 229.8 | 0.56 | 5899069 | 5.0 | |
| list_struct_64 | — | — | — | — | skipped: 1e5 already yields 4505338 rows / 3.5 GiB, so 1e6 is ≈ 45e6 rows / ≈ 35 GiB |
| cartesian_two_lists | 303.0 | 0.57 | 9604966 | 3.3 | |
| null_typed_list | 35.9 | 0.43 | 1000000 | 1.3 | |

One shot with no warmup, so these carry more variance than the 1e5 medians; they
are here to show the shape of the curve, not to rank. The rewrite stays flat
(0.43–0.57 ms) across a 10× row count, which is the point: the rewrite is
schema-bound, the wall is execution-bound.

## Gate (64 rows) row-set equality vs Spark explode+struct expand

| shape | row_set_equal | why |
|---|---|---|
| struct_d3 | True | |
| struct_d6 | True | |
| list_struct_1 | False | `DYNFLATTEN-LISTNULL-1` |
| list_struct_8 | False | `DYNFLATTEN-LISTNULL-1` |
| list_struct_64 | False | `DYNFLATTEN-LISTNULL-1` |
| cartesian_two_lists | False | `DYNFLATTEN-LISTNULL-1` |
| null_typed_list | False | `DYNFLATTEN-LISTNULL-1` |

Every False is the same single cause and nothing else: the five False shapes are
exactly the five that carry a `user_properties ARRAY<VOID>` column, and the two
True shapes are the two that do not. Measured at 16 rows: parquet holds
`list<element: null>`; repark drops the column, Spark reads it as
`ArrayType(IntegerType())` and `explode_outer` keeps it as an all-null `int32`.
On the shared columns the two agree. Live co-collect is
`test_live_dynflatten_matches_spark_explode` (`REPARK_PARITY_LIVE=1`, 3 shapes).

## Candidate ranking (share of repark 1e5 wall, total 351.6 ms)

| rank | candidate | wall_share | verdict | evidence |
|---:|---|---:|---|---|
| 1 | null_mask_struct_extractor | 0.250 | **queued** PERF-DYNFLATTEN-2 | struct-shape execute 88.1 ms of 351.6 ms. `CASE WHEN parent IS NULL` per field. Projected: validity-bitmap extract. Not a <150-line contained fix. |
| 2 | cartesian_multi_list_operator | 0.215 | **queued** PERF-DYNFLATTEN-2 | cartesian execute 75.6 ms of 351.6 ms; two sequential Unnests. Zip/pad is not a substitute. Projected: one Cartesian operator. Not a <150-line contained fix. |
| 3 | optimizer_wrapper_walks | 0.006 | **not worth it** | rewrite 2.1 ms of 351.6 ms, and flat at 1e6. Depth-3 pin: 10 walks, 4 passes, 3 expansions. |

The release ranking is not the debug ranking: on debug the top two tied at 0.233
each, and release separates them (0.250 / 0.215) while dropping the walk share
from 0.002 to 0.006. Ranks 1 and 2 swap. No product optimization lands in this
unit; both are queued as PERF-DYNFLATTEN-2.

## How to reproduce

```bash
cd python/repark && maturin develop --release
cd ../.. && python python/repark-parity/bench/dynflatten/run_dynflatten.py \
  --scale quick --out /tmp/oc-dynflatten-bed \
  --json /tmp/oc-dynflatten-bed/run.json
```

`make dynflatten-bench` runs `--scale gate` (`SCALE=quick` for this table) and
writes its rendered report under the bed, not over this note.
