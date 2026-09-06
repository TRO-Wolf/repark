# dynamicFlatten baseline (PERF-DYNFLATTEN-1)

Measured 2026-09-04 on this clone. Not an H-3b hard-gated baseline: the reference-host choice
is still open, and this host was **not idle** (see "What this baseline is not").

pins: perf-dynflatten-1-measure/C-003, C-004

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X 32-Core (64 threads) |
| governor | schedutil |
| ram | 125.7 GiB |
| native | `release_or_stripped size_bytes=163093528` |
| release proof | `repark._native.__debug_assertions__ is False` — the runner refuses to write a report otherwise |
| repark | 1.0.1 |
| pyspark | 4.1.2 |
| JAVA_HOME | `zulu-17-amd64` |
| TZ | UTC |
| seed | 42 |

## How the two engines are compared

Both engines are given the **same materialized input** and the timed region is
**flatten + collect only**, so neither pays for a parquet scan the other avoids:

| | repark | Apache Spark |
|---|---|---|
| input | `createDataFrame` of the parquet (in memory) | `read.parquet(...).cache()` then `.count()` |
| timed | `dynamicFlatten()` + `to_arrow()` | `explode`/struct expand + `toArrow()` |
| threads | `spark.sql.shuffle.partitions = 8` | `local[8]` |

Thread parity is the point of the second row. repark's DataFusion default is
`target_partitions = 64` on this box, which is not a fair comparison against `local[1]`; both
engines are pinned to 8. The `allcores` column reports repark at its 64-thread default **for
information only** — it is not the comparison, and it is frequently *slower* than 8 threads.

5 iterations, 1 warmup, one subprocess per repark cell. Median and min are both reported
because the medians move under load.

## 1e5 fixtures

| shape | iso | repark_med | repark_min | rewrite | spark_med | spark_min | ratio | allcores | rows_out |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| struct_d3 | | 25.4 | 24.5 | 0.20 | 154.7 | 136.1 | 0.16 | 24.0 | 100000 |
| struct_d6 | | 66.7 | 65.1 | 0.24 | 161.1 | 143.9 | 0.41 | 70.4 | 100000 |
| list_struct_1 | | 30.6 | 28.9 | 0.23 | 182.2 | 158.8 | 0.17 | 35.1 | 100000 |
| list_struct_8 | | 43.3 | 41.5 | 0.35 | 274.5 | 271.6 | 0.16 | 40.7 | 589888 |
| list_struct_64 | | 141.5 | 104.6 | 0.33 | 531.6 | 506.6 | 0.27 | 113.8 | 4505338 |
| cartesian_two_lists | | 90.2 | 71.2 | 0.41 | 550.7 | 503.8 | 0.16 | 74.2 | 961708 |
| null_typed_list | | 11.7 | 9.0 | 0.13 | 130.0 | 120.7 | 0.09 | 9.3 | 100000 |
| struct_d3_nonull | y | 2.3 | 2.3 | 0.13 | 77.6 | 73.2 | 0.03 | — | 100000 |
| struct_d6_nonull | y | 6.7 | 6.5 | 0.22 | 100.4 | 90.8 | 0.07 | — | 100000 |
| cartesian_legs_only | y | 37.1 | 33.5 | 0.33 | 199.5 | 189.2 | 0.19 | — | 310150 |
| cartesian_tags_only | y | 26.6 | 20.2 | 0.43 | 159.0 | 145.4 | 0.17 | — | 310150 |

`iso` marks the isolation fixtures that exist only to subtract one cost from another; they are
not part of any headline. repark's number is lower on every fixture (ratios 0.09–0.41), but
that is **not** a clean engine comparison: Spark's timed region includes a JVM→Python Arrow
transfer of the full result that repark's in-process `to_arrow()` never pays (do-not #4).

## Noise floor

The `struct_d3` cell was run 6 times back to back: 25.4, 26.6, 25.1, 26.3, 15.8, 26.5 ms.
**Noise floor = 10.81 ms**, the spread of those medians. It is that wide because sibling lanes
were loading the box (1-minute load average 25–45 during these runs).

An earlier estimator used a single |A − B| of two medians. It is discarded: across two runs it
produced 8.65 ms and 0.12 ms, and dividing by it flipped every verdict, once ranking a 0.95 ms
cost as "queued" at 7.7×. A single difference is not a dispersion statistic.

## Candidates, measured in isolation

Each candidate is timed as **itself**, not as the wall of the fixture family that contains it:

Each cost below is **one fixture's** number. Costs are never summed across fixtures: a sum
against a single-fixture floor is the same aggregation error as a share of family wall.

| candidate | fixture | how it is isolated | cost | ×noise | verdict |
|---|---|---|---:|---:|---|
| null_mask_struct_extractor | **struct_d6** | 30 % null parents minus 0 % nulls | **59.98 ms** | **5.5** | **queued** |
| null_mask_struct_extractor | struct_d3 | same, shallower | 23.01 ms | 2.1 | below bar alone |
| cartesian_multi_list_operator | cartesian_two_lists | minus (legs-only + tags-only) | 26.91 ms | 2.5 | **not worth it** |
| optimizer_wrapper_walks | cartesian_two_lists | rewrite wall | 0.41 ms | 0.04 | **not worth it** |
| optimizer_wrapper_walks | struct_d6 | rewrite wall | 0.24 ms | 0.02 | **not worth it** |
| optimizer_wrapper_walks | struct_d3 | rewrite wall | 0.20 ms | 0.02 | **not worth it** |

A candidate is **queued only when a single fixture's isolated cost exceeds the noise floor by
3×**. `null_mask_struct_extractor` qualifies on `struct_d6` alone (5.5×); `struct_d3` at 2.1×
would not have carried it. No other candidate clears the bar on any fixture.

Reproducibility across three independent runs of the whole battery:

| candidate (fixture) | run A | run B | run C | stable? |
|---|---:|---:|---:|---|
| null_mask (struct_d6) | 62.76 | 62.93 | 59.98 | yes (±3 %) |
| null_mask (struct_d3) | 14.63 | 12.73 | 23.01 | no (1.8× spread) |
| cartesian (cartesian_two_lists) | 6.09 | 16.64 | 26.91 | **no** (4.4× spread) |
| walks (cartesian_two_lists) | 0.47 | 0.48 | 0.41 | yes |

`struct_d6` is the fixture the verdict rests on and it is the steadiest number in the table.
`struct_d3` is not steady and does not clear the bar in any run, which is why the queued
verdict is stated on `struct_d6` alone rather than on the two added together.

Only `null_mask_struct_extractor` is both large and reproducible, and it is the one unit this
measurement queues. Null-parent handling dominates the struct path: `struct_d6` costs 66.7 ms
at 30 % nulls and 6.7 ms at 0 %, a 10× difference on the same rows and schema.

`cartesian_multi_list_operator` is **not** queued, reversing an earlier reading that ranked it
second on 21.5 % of fixture wall. Isolated, the second Unnest adds 6–27 ms — the same order as
the 10.81 ms floor. Not shown to be worth a unit, not shown to be free: re-measure on a quiet
host before closing it for good.

## 1e6 repark cells (5 iterations, 1 warmup, 8 threads)

| shape | repark_med | repark_min | rewrite | rows_out | peak_rss_GiB |
|---|---:|---:|---:|---:|---:|
| struct_d3 | 51.9 | 32.4 | 0.28 | 1000000 | 1.4 |
| struct_d6 | 149.5 | 139.1 | 0.43 | 1000000 | 1.5 |
| list_struct_1 | 172.5 | 129.1 | 0.33 | 1000000 | 1.4 |
| list_struct_8 | 194.4 | 179.8 | 0.38 | 5899069 | 5.1 |
| list_struct_64 | — | — | — | — | skipped: 1e5 already yields 4505338 rows / 3.5 GiB, so 1e6 is ≈ 45e6 rows / ≈ 35 GiB |
| cartesian_two_lists | 276.0 | 257.4 | 0.46 | 9604966 | 3.3 |
| null_typed_list | 27.0 | 24.6 | 0.32 | 1000000 | 1.3 |

The rewrite stays 0.28–0.46 ms across a 10× row count: it is schema-bound, the wall is
execution-bound. That, not its share of any total, is why the walk candidate is closed.

## Walk counts (schema-only Rust pins)

| shape | rewrite_passes | schema_walks | struct_expansions | list_explodes | unnest_nodes |
|---|---:|---:|---:|---:|---:|
| struct_d3 | 4 | 10 | 3 | 0 | 0 |
| cartesian_two_lists | — | — | 0 | 2 | 2 |

Mutation (run 2026-09-04): delete the `has_struct_columns` walk → `schema_walks` 10 → 6 and
`flatten_stats_depth_three_struct_counts_repeated_schema_walks` reds, **1 red of 2**. The
sibling Unnest pin stays green, so the counter is walk-specific.

## Row-set equality vs Spark explode+struct expand

Computed at **gate scale, 64 rows** — full Arrow row-set equality only runs when a fixture's
output is at or under `EQUALITY_ROW_CAP` (20,000 rows), which the 1e5 fixtures exceed.

| shape | equal | why |
|---|---|---|
| struct_d3, struct_d6 | True | |
| the other five | False | `DYNFLATTEN-LISTNULL-1`, one cause |

The five False shapes are exactly the five carrying `user_properties ARRAY<VOID>` on the
bench's `createDataFrame(ARRAY<VOID>)` load path. Live co-collect is
`test_live_dynflatten_matches_spark_explode`, now symmetric (**both** engines
`read.parquet` of one file), which surfaced `DYNFLATTEN-READNULL-1`.
**DYNFLATTEN-LISTNULL-1 (2026-09-06):** the live `read.parquet` path now matches Spark on
those five shapes (parquet Null logical type → int32). The bench load path is unchanged.

## Do not (binding)

1. Do not open an implementation unit for `optimizer_wrapper_walks`. Measured at 0.45 ms on its strongest single fixture (0.04× the floor) and
   flat at 1e6.
2. Do not substitute DataFusion multi-column Unnest zip/pad for sequential Cartesian
   expansion. It changes the row set.
3. Do not quote a ratio against Spark without stating the thread count and that both engines
   were handed a materialized frame. A `local[1]` Spark against a 64-thread repark is not a
   comparison.
4. Do not read the ratios as a clean engine comparison at all. Spark's timed region includes a
   JVM→Python Arrow transfer of the whole result (`toArrow()`); repark's `to_arrow()` is
   in-process. Spark pays a cost repark does not, and it is not subtracted here.
5. Do not treat these numbers as an H-3b baseline or use them to gate a release.
6. Do not close `DYNFLATTEN-QUALNAME-1` by changing the bed.

## What this baseline is not

The host ran 3–4 sibling lanes throughout (load average 25–45, other JVMs and cargo builds
live). The floor is measured and reported rather than assumed away; the one queued candidate
clears it by 5.5× on `struct_d6` alone and survives the contention. Anything within one order of the floor — the
Cartesian candidate especially — needs a quiet host before it is called either way.

## How to reproduce

```bash
cd python/repark && maturin develop --release
cd ../.. && python python/repark-parity/bench/dynflatten/run_dynflatten.py \
  --scale quick --out /tmp/oc-dynflatten-bed --json /tmp/oc-dynflatten-bed/run.json
```

The runner raises rather than writing a report if the native module is a debug build.
`make dynflatten-bench` runs `--scale gate` and writes its rendered report under the bed.

## After PERF-DYNFLATTEN-2 — the null-mask struct extractor

pins: perf-dynflatten-2-null-mask/C-001, C-003

Everything above is PERF-DYNFLATTEN-1's and is untouched. What follows is a **second,
self-contained before/after pair**, both halves measured the same evening on the same box with
the same command (`run_dynflatten.py --scale quick --iterations 5 --warmup 1`, 8 threads on
both engines, release module proved from `__debug_assertions__`). It does not extend the tables
above: that run sat under 3-4 sibling lanes and this one did not, so the two noise floors are
numbers about different hours and a cost is only ever read against the floor of the run it came
from.

| key | before | after |
|---|---|---|
| 1-minute load at run start | 4.50 | 4.53 |
| noise floor (`struct_d3`, 6 repeats) | 4.077 ms | 0.0585 ms |
| native | `release_or_stripped size_bytes=163106944`, release `True` | `release_or_stripped size_bytes=163145824`, release `True` |

The floor collapses because it is the spread of the cell it is measured on, and that cell went
from ~25 ms to ~1 ms. A 0.0585 ms floor is a floor for a 1 ms cell and says nothing about a
25 ms one; the control below measures the dispersion at that scale instead.

### Per-fixture execute median / min (ms) and rows out

`execute` is `to_arrow()` only — the same column the candidate isolation subtracts.

| shape | iso | before med | before min | after med | after min | rows_out (both) |
|---|---|---:|---:|---:|---:|---:|
| struct_d3 | | 25.31 | 25.16 | **0.95** | 0.94 | 100000 |
| struct_d6 | | 71.22 | 68.79 | **1.50** | 1.43 | 100000 |
| list_struct_1 | | 30.02 | 28.08 | 17.24 | 16.67 | 100000 |
| list_struct_8 | | 44.09 | 27.41 | 33.55 | 21.74 | 589888 |
| list_struct_64 | | 98.54 | 96.74 | 64.92 | 57.91 | 4505338 |
| cartesian_two_lists | | 85.88 | 63.85 | 53.37 | 52.09 | 961708 |
| null_typed_list | | 12.50 | 11.83 | 0.99 | 0.96 | 100000 |
| struct_d3_nonull | y | 2.47 | 2.18 | 1.00 | 0.98 | 100000 |
| struct_d6_nonull | y | 6.39 | 6.29 | 1.50 | 1.47 | 100000 |
| cartesian_legs_only | y | 26.17 | 24.88 | 30.28 | 28.40 | 310150 |
| cartesian_tags_only | y | 18.83 | 14.88 | 23.71 | 18.82 | 310150 |

`rows_out` is identical on all eleven fixtures. Row-set identity is pinned separately and more
strictly: `python/repark/tests/test_dynflatten_null_mask.py` holds the gate-scale row count,
the Arrow schema string (names, types, nullability) and a SHA-256 over the ordered rows for
each of the eleven shapes, captured before the extractor existed.

### The candidate

| candidate | fixture | how it is isolated | before | after | after ÷ after-floor | verdict |
|---|---|---|---:|---:|---:|---|
| null_mask_struct_extractor | **struct_d6** | 30 % null parents minus 0 % nulls | 64.83 ms | **0.01 ms** | **0.1** | **delivered** |
| null_mask_struct_extractor | struct_d3 | same, shallower | 22.84 ms | −0.05 ms | −0.9 | delivered |

The queue bar was "a single fixture's isolated cost above 3× the floor". `struct_d6`'s cost is
now 0.1× its run's floor — the null-parent handling is no longer measurable as a cost at all,
which is what a validity-bitmap extractor should do: the work stopped being proportional to the
rows and became proportional to the validity buffer.

### The two fixtures that moved up, and the control that explains them

`cartesian_legs_only` (+4.1 ms) and `cartesian_tags_only` (+4.9 ms) are the only fixtures whose
median rose. Neither is a regression the diff can be charged with, and the evidence is measured,
not argued:

1. **`cartesian_tags_only` has no struct anywhere in its schema** (`id`, `Tags ARRAY<STRING>`,
   `user_properties ARRAY<VOID>`), so `expand_structs` never runs on it and the extractor is
   never constructed. It cannot have been slowed by this change. Six back-to-back repeats of
   that cell on the after build: 25.28, 25.96, 23.31, 28.69, 28.36, 29.67 ms — **spread
   6.36 ms**, larger than the movement being explained.
2. `cartesian_legs_only` does use the extractor. Six repeats: 31.26, 29.33, 32.72, 31.04,
   30.57, 32.92 ms — spread 3.59 ms, again the order of its movement.
3. `cartesian_two_lists` contains **both** the legs work and the tags work and fell 85.88 →
   53.37 ms. Work the diff had genuinely added to the legs path would have to show up there too.

So the dispersion at the 20-30 ms scale on this box is ~6 ms, not the 0.0585 ms floor measured
on a 1 ms cell, and no fixture regresses beyond the dispersion measured at its own scale.

### What the extractor is

`crates/repark-core/src/dynamic_flatten/null_mask.rs`. Per struct leaf per level the rewrite
used to emit `CASE WHEN parent IS NULL THEN <typed null> ELSE get_field(parent, 'field')`.
DataFusion plans that as `EvalMethod::ExpressionOrExpression`, which **filters the whole record
batch twice** (the then-side rows and the else-side rows), evaluates each side, and zips — per
leaf, per level. `struct_d6` pays it eight times over six projections. The extractor is one
scalar UDF over the parent column that takes the child array `get_field` would have returned
and unions the parent's validity into it with `arrow::compute::nullif`: one bit-and over the
validity buffer, no row copied. A parent with no nulls short-circuits to the child untouched.

`Dictionary(_, Struct)` parents keep the CASE: `get_field` there returns
`Dictionary(K, child)` while the typed null is `child`, and the CASE's coercion between the two
is the shipped output type. Contract and the two rejected alternatives (both measured):
`crates/repark-core/src/dynamic_flatten/map.md`.

### `DYNFLATTEN-QUALNAME-1` closed, and not by changing the bed

Do-not #6 above stands and was honoured. The refusal was `push_down_leaf_projections` choking
on the `get_field` inside the per-leaf CASE; the extractor leaves no `get_field` in the plan for
that rule to hoist, so the plan it could not rewrite is no longer built. All twelve cells of the
keep × depth matrix now collect and equal live PySpark 4.1.2. The registry row is FIXED and its
pin is now an answer pin. The bed still nests the row id inside the leaf; that workaround is now
unnecessary and is deliberately left alone, because changing the bed would move every pinned
number above.

## After DYNFLATTEN-LISTNULL-1 — parquet Null logical type reads as int32

pins: dynflatten-listnull-1/C-005

Everything above is untouched. The fix is in `read_parquet_nullable`, not in
`dynamicFlatten` / `drop_null_lists`. The bench still loads repark through
`createDataFrame(ARRAY<VOID>)`, so its flatten wall and `rows_out` on the list shapes do
not pay for an extra int32 column; struct-only shapes never had `user_properties`.
Re-run numbers for the struct-only bed land in the unit ledger, not here, so the tables
above stay the hour they were measured.
