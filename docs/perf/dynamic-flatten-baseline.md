# dynamicFlatten baseline (PERF-DYNFLATTEN-1)

Measured 2026-09-04 on this clone. Not an H-3b hard-gated baseline (reference-host
choice still blocks that). Native module is **debug** (`make develop`).

pins: perf-dynflatten-1-measure/C-003, C-004

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X 32-Core (64 threads) |
| governor | schedutil |
| ram | 125.7 GiB |
| native | debug_or_unstripped, 637713896 bytes |
| repark | 1.0.1 |
| pyspark | 4.1.2 |
| JAVA_HOME | `/usr/lib/jvm/zulu-17-amd64` |
| TZ | UTC |
| seed | 42 |
| load path | Arrow parquet → `createDataFrame` (facade `read.parquet` + flatten hits a qualified-name clash at ≥3 struct expands with a keep column; queued) |

## 1e5 fixtures (quick; 3 iterations, 1 warmup; repark cell = one subprocess)

| shape | rows_in | repark_ms | rewrite_ms | execute_ms | rss_MiB | spark_ms | ratio | rows_out |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| struct_d3 | 100000 | 258 | 1.6 | 257 | 461 | 218 | 1.19 | 100000 |
| struct_d6 | 100000 | 976 | 1.7 | 974 | 476 | 166 | 5.88 | 100000 |
| list_struct_1 | 100000 | 248 | 2.0 | 246 | 477 | 212 | 1.17 | 100000 |
| list_struct_8 | 100000 | 449 | 2.0 | 447 | 912 | 366 | 1.23 | 589888 |
| list_struct_64 | 100000 | 1999 | 1.9 | 1997 | 3539 | 1941 | 1.03 | 4505338 |
| cartesian_two_lists | 100000 | 1234 | 2.6 | 1232 | 719 | 634 | 1.95 | 961708 |
| null_typed_list | 100000 | 116 | 1.3 | 115 | 421 | 160 | 0.73 | 100000 |

Plan-node / walk counts are schema-only (Rust pins; independent of row count):

| shape | rewrite_passes | schema_walks | struct_expansions | list_explodes | unnest_nodes |
|---|---:|---:|---:|---:|---:|
| struct_d3 | 4 | 10 | 3 | 0 | 0 |
| cartesian_two_lists | (list pass) | (measured) | 0 | 2 | 2 |

`list_struct_64` at 1e6 is skipped (file size / exploded rows). Generator writes 1e6;
one-shot `struct_d3` 1e6 repark cell: **327 ms** wall (1.5 rewrite / 325 execute),
1.51 GiB peak RSS, 1e6 rows out.

## Gate (64 rows) row-set equality vs Spark explode+struct expand

| shape | row_set_equal |
|---|---|
| struct_d3 | True |
| struct_d6 | True |
| list_struct_1 | False |
| list_struct_8 | False |
| list_struct_64 | False |
| cartesian_two_lists | False |
| null_typed_list | False |

Struct shapes match. List shapes do not: Spark parquet types/nullability and
`explode_outer` vs repark preserve-null Unnest still differ on the Arrow schema
signature. Live co-collect at 16 rows is `test_live_dynflatten_matches_spark_explode`.

## Candidate ranking (share of repark 1e5 wall, total 5280 ms)

| rank | candidate | wall_share | verdict | evidence |
|---:|---|---:|---|---|
| 1 | cartesian_multi_list_operator | 0.233 | **queued** PERF-DYNFLATTEN-2 | cartesian execute 1232 ms; two sequential Unnests. Zip/pad is not a substitute. Projected: one Cartesian operator. Not a <150-line contained fix. |
| 2 | null_mask_struct_extractor | 0.233 | **queued** PERF-DYNFLATTEN-2 | struct execute 1231 ms; `struct_d6` is 5.88× Spark. CASE WHEN parent IS NULL per field. Projected: validity-bitmap extract. Not a <150-line contained fix. |
| 3 | optimizer_wrapper_walks | 0.002 | **not worth it** | rewrite 13 ms of 5280 ms. Depth-3 pin: 10 schema walks, 4 passes, 3 expansions. Mutation: drop a `has_struct` walk → the 10-walk pin reds. |

No product optimization lands in this unit.

## How to reproduce

```bash
make develop
python python/repark-parity/bench/dynflatten/run_dynflatten.py \
  --scale quick --out /tmp/oc-dynflatten-bed \
  --json /tmp/oc-dynflatten-bed/run.json \
  --report docs/perf/dynamic-flatten-baseline.md
```

`make dynflatten-bench` runs `--scale gate` by default (`SCALE=quick` to match this table).
