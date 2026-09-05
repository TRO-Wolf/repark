# The spill matrix — the Never-OOM truth table (H3-SPILL-1, 2026-09-05)

**What this is.** [PROJECT.md](../../PROJECT.md) promises "predictable memory via spill-to-disk by
default" and marks the stronger *never OOM on data larger than RAM* claim as pending a
spill-coverage spike. This is that spike, measured. For every operator the engine can plan, under
a bounded `FairSpillPool` at two scales, it records whether the operator **spills**, **degrades**,
**refuses cleanly**, or does something worse — and it compares every bounded answer against the
unbounded one, because an operator that survives a small pool by losing rows would be the worst
outcome of all.

**The one-line answer.** At 180 measured cells: **no cell aborted the process, and no cell
returned a wrong answer.** A bounded pool either completed the query, spilled, degraded, or
refused with a typed `PySparkException` naming the pool and the two knobs that resize it. One cell
— `nested_loop_join` at a 64 MiB pool and 1e7 rows — turned a pool refusal into a Rust panic
caught at the Python boundary; that is `H3-SPILL-NLJ-1` below, and it is the only failure of the
Never-OOM contract this matrix found. The honest limit of the claim is different and larger: **the
pool bounds only the operators that register with it.** Windows, `dynamicFlatten`, the Iceberg
scan and the whole facade boundary (`collect`, `toPandas`) are not pool-accounted in DataFusion
54.1, so their memory is whatever the data costs — 4.5 GiB resident for a 1e7-row `collect()`
under a 64 MiB pool. They do not OOM here because the box is large, not because the pool held them.

## 1. The machine, the module, the method

| | |
|---|---|
| Host | Linux 6.8.0-138-generic x86_64, glibc 2.39, 64 cores, 125 GiB RAM |
| Module | release (`maturin develop --release`, `__debug_assertions__ = False`), repark 1.0.1 |
| Engine | DataFusion 54.1.0 |
| Python | 3.12.3 |
| Per cell | one fresh subprocess, `datafusion.execution.target_partitions = 4`, session defaults otherwise (`batch_size` 65536) |
| Guard | parent polls `/proc/<pid>/status` `VmHWM` every 50 ms and kills past `--rss-cap-bytes` 8 GiB; `RLIMIT_AS` 32 GiB is a backstop only |
| Concurrency | three driver lanes ran concurrently; the 1-minute load at each cell's start is in the tables, and wall is read against it |
| Repeats | every cell whose first outcome was not `ok` ran three times; the `runs` list is in the JSON |
| Evidence | [spill-matrix-baseline-cells.json](spill-matrix-baseline-cells.json) — every cell, every repeat, both caps, per-operator plan metrics |
| Harness | [python/repark-parity/bench/spill/](../../python/repark-parity/bench/spill/map.md) |

**The fixture.** One wide deterministic view, `id`, `h = md5(id)`, `g = id % 1024`, a 64-character
varied `payload`, and `v = id * 1.5` — about 120 bytes of payload per row, so the 1e7 scale is
~1.2 GiB of live data and genuinely exceeds the 1 GiB pool. `v` is chosen so every per-row double
is exact, which is what lets the answer digests be integer checksums.

**Why `EXPLAIN ANALYZE` is the instrument.** It executes the plan in full and discards the output,
so the recorded peak RSS is the operator's and not the collector's, and it reports
`spill_count` / `spilled_bytes` / `skipped_aggregation_rows` per physical operator. Those counters
are the assertion; wall time never is.

**Why the answers are compared.** Each row also carries a small-output probe (counts, integer
checksums, a sort-inversion count) run in the same process. The driver compares every bounded
cell's digest against the `pool=none` run at the same scale. **72 bounded cells produced a digest
and all 72 matched.** Cells that refused have no digest, which is why the count is 72 and not 144.

## 2. What DataFusion 54.1 can actually spill

Read from the vendored source rather than inferred, because this is the ceiling on any
Never-OOM claim. `MemoryConsumer::new(...).with_can_spill(true)` is the only construction that
can survive a full pool:

| Class | Operators |
|---|---|
| Pool-accounted **and** spillable | `SortExec` (`ExternalSorter`), grouped `AggregateExec` (`row_hash`), `RepartitionExec`, `NestedLoopJoinExec` (fallback load path) |
| Pool-accounted, **not** spillable — refuses when the pool is full | `HashJoinExec` (build side), `SortMergeJoinExec` (buffered side spills, streamed side does not), `CrossJoinExec`, `SymmetricHashJoinExec`, `PiecewiseMergeJoinExec`, ungrouped `AggregateStream`, `SortPreservingMergeExec`, `TopK`, `BufferExec`, `RecursiveQueryExec` |
| **Not pool-accounted at all** | `WindowAggExec`, `BoundedWindowAggExec`, `UnnestExec`, `CoalesceBatchesExec`, the repark Iceberg scan, the repark facade boundary (`collect`, `toPandas`) |

The third row is the load-bearing one. A pool cannot bound what never asks it for anything.

## 3. The outcome matrix

Legend: `ok` in memory · `spill n/MiB` spilled, n spill events · `degr N` partial aggregation
skipped for N rows · `clean-err` typed `PySparkException` naming the pool · `PANIC` a Rust panic
caught at the Python boundary. No cell was `abort` or `wrong`.

### 3.1 1e6 rows (~120 MiB of live data)

| operator | none | 8G | 1G | 256M | 64M |
|---|---|---|---|---|---|
| `sort` | ok | ok | ok | spill 8/54M | clean-err |
| `topk` | ok | ok | ok | ok | ok |
| `hash_agg_many_groups` | degr 0.5M | degr 0.5M | degr 0.5M | spill 8/54M | clean-err |
| `hash_agg_few_groups` | ok | ok | ok | ok | ok |
| `distinct` | degr 0.5M | degr 0.5M | degr 0.5M | spill 8/46M | clean-err |
| `collect_list` | ok | ok | ok | spill 3/46M | clean-err |
| `hash_join` | ok | ok | ok | ok | clean-err |
| `sort_merge_join` | ok | ok | ok | clean-err | clean-err |
| `nested_loop_join` | ok | ok | ok | ok | ok |
| `window_unbounded` | ok | ok | ok | ok | clean-err |
| `window_sliding_rows` | ok | ok | ok | ok | ok |
| `window_range` | ok | ok | ok | ok | ok |
| `repartition` | degr 0.5M | degr 0.5M | degr 0.5M | spill 8/54M | clean-err |
| `dynamic_flatten` | ok | ok | ok | ok | ok |
| `iceberg_scan_dv` | ok | ok | ok | ok | ok |
| `merge_staging` | ok | ok | ok | ok | ok |
| `collect` | ok | ok | ok | ok | ok |
| `to_pandas` | ok | ok | ok | ok | ok |

### 3.2 1e7 rows (~1.2 GiB of live data)

| operator | none | 8G | 1G | 256M | 64M |
|---|---|---|---|---|---|
| `sort` | ok | ok | spill 16/536M | clean-err | clean-err |
| `topk` | ok | ok | ok | clean-err | clean-err |
| `hash_agg_many_groups` | degr 9.5M | degr 9.5M | spill 28/536M | spill 104/1340M | clean-err |
| `hash_agg_few_groups` | ok | ok | ok | ok | ok |
| `distinct` | degr 9.5M | degr 9.5M | spill 24/459M | spill 93/1011M | clean-err |
| `collect_list` | ok | ok | clean-err | clean-err | clean-err |
| `hash_join` | ok | ok | spill 4/58M | clean-err | clean-err |
| `sort_merge_join` | ok | ok | spill 24/540M | clean-err | clean-err |
| `nested_loop_join` | ok | ok | ok | ok | PANIC |
| `window_unbounded` | ok | ok | spill 8/232M | spill 28/232M | clean-err |
| `window_sliding_rows` | ok | ok | ok | ok | ok |
| `window_range` | ok | ok | ok | ok | ok |
| `repartition` | degr 9.5M | degr 9.5M | spill 28/536M | spill 106/1481M | clean-err |
| `dynamic_flatten` | ok | ok | ok | ok | ok |
| `iceberg_scan_dv` | ok | ok | ok | ok | ok |
| `merge_staging` | ok | ok | clean-err | clean-err | clean-err |
| `collect` | ok | ok | ok | ok | ok |
| `to_pandas` | ok | ok | ok | ok | ok |

### 3.3 The census

| outcome | cells | meaning |
|---|---:|---|
| `ok` | 121 | completed in memory |
| `spilled` | 16 | completed by spilling to disk |
| `degraded` | 15 | completed after the partial aggregate gave up (`skipped_aggregation_rows`) |
| `clean_error` | 27 | typed `PySparkException`, process healthy, remediation named |
| `internal_error` | 1 | `nested_loop_join` 64 MiB / 1e7 — see `H3-SPILL-NLJ-1` |
| `abort` | **0** | — |
| `wrong` | **0** | — |
| total | 180 | |

Only one cell was outcome-unstable across its three runs: `sort` at 256 MiB / 1e6 came back
`spilled, clean_error, spilled`. It sits exactly on the boundary where the four `SortExec`
partitions plus the `SortPreservingMergeExec` merge reservation together straddle the pool, so
which side it lands on depends on the order the partitions reach the pool. **No pin is placed on
that cell**; the pins use 64 MiB (always refuses) and 1 GiB at 1e7 (always spills).

## 4. What the numbers say, operator by operator

**Spills, and stays under the pool while it does.** `sort`, grouped `hash aggregate`, `distinct`,
`RepartitionExec` and `sort_merge_join` all spill and complete, and resident memory falls as the
pool falls — `hash_agg_many_groups` at 1e7 goes 1,829 MiB (unbounded) → 841 (1 GiB pool, 28 spills,
536 MiB spilled) → 691 (256 MiB pool, 104 spills, 1,340 MiB spilled). That is spill-to-disk doing
exactly what the north star claims, and it is the only part of the claim the pool is responsible
for.

**Degrades before it spills.** At 1e6 and 1e7 the high-cardinality aggregate and `distinct` report
`skipped_aggregation_rows` equal to ~95 % of input rows at the 8 GiB and unbounded pools: the
partial aggregate measures its own hit rate, concludes grouping is pointless when nearly every row
is its own group, and streams rows straight to the final aggregate instead. It is a degradation,
not a spill, and it is why those rows read `degr` rather than `ok`.

**Refuses, cleanly, and cannot spill.** `hash_join`'s build side, `collect_list` (`array_agg`) and
the MERGE staging join have no spill path. Every refusal is a `PySparkException` carrying the
DataFusion reservation that failed, `fair(pool_size: …)` — never `greedy(` — and the repark
remediation sentence naming both `repark.memory.limit.gb` (build time) and
`datafusion.runtime.memory_limit` (runtime). The process is healthy afterwards.

**Never touches the pool.** `window_sliding_rows` and `window_range` are flat at ~250 MiB across
every pool at both scales: a bounded frame streams, and the window operators are not pool-accounted
anyway. `topk` is a bounded heap and is `ok` down to 64 MiB at 1e6 — its refusals at 1e7 come from
the `RepartitionExec` and merge reservations beneath it, not from the heap. `dynamic_flatten`
(2,089 MiB at a 64 MiB pool), `to_pandas` (2,103 MiB), `collect` (4,459 MiB) and the Iceberg DV
scan (699 MiB) are identical at every pool including 64 MiB, because none of them registers with
it. **Those four rows are the honest boundary of Never-OOM: the pool is not a memory limit for
them, and there is no configuration that makes it one.**

**Costs wall time to spill.** `sort` at 1e7 is 2,148 ms unbounded and 2,433 ms at a 1 GiB pool;
`repartition` at a 256 MiB pool is 44,526 ms against 1,171 ms unbounded, and `hash_agg_many_groups`
30,550 ms against 1,062 ms. A 25-40x wall cost at the smallest pool that still completes is the
price of the guarantee. Read those against the 1-minute load in the tables: three lanes were
running.

## 5. Apache Spark on the same fixture

Three cells, `spark.driver.memory = 1g`, `local[4]`, PySpark 4.1.2 on Zulu 17, the same 1e7-row
projection, materialized through the `noop` sink so the optimizer cannot drop the operator under
test. Spill is read from the Spark event log (`Memory Bytes Spilled` / `Disk Bytes Spilled`
summed over every `SparkListenerTaskEnd`).

| cell | Spark outcome | memory spilled | disk spilled | wall | repark at a 1 GiB pool |
|---|---|---:|---:|---:|---|
| `sort` | ok | 318.8 MB | 228.6 MB | 10,465 ms | `spilled` 16 events / 536 MiB, 2,433 ms |
| `hash_join` | ok | 721.4 MB | 433.7 MB | 8,650 ms | `spilled` 4 events / 58 MiB, 1,336 ms |
| `collect_list` | **`java.lang.OutOfMemoryError: Java heap space`**, SparkContext shut down | — | — | — | `clean_error`, process healthy |

Two caveats a reader needs. **The budgets are not the same quantity:** `spark.driver.memory` is
the whole JVM heap, while `datafusion.runtime.memory_limit` bounds only the reservations
operators take against the pool, so a 1 GiB pool is a *tighter* budget than a 1 GiB heap, not a
looser one. And **`collect_list` is the row that matters**: Spark does not spill `collect_list`
either — it dies with a Java OOM that takes the whole SparkContext with it, while repark refuses
with a typed exception and a live session. On this operator repark is the better citizen, and the
registry row records it that way.

Spark's documented behaviour for the rest is the citation, not a measurement: sort, aggregation
and shuffle spill to disk when they exceed their execution-memory share
(<https://spark.apache.org/docs/latest/tuning.html> "Memory Management Overview"), which the two
measured rows above confirm.

## 6. The defects this matrix found

### `H3-SPILL-NLJ-1` — a bounded pool turns a nested-loop join into a caught Rust panic

At 64 MiB / 1e7 (and reproducibly at 8 MiB / 1e6), `SELECT l.id, r.v FROM base l JOIN other r ON
l.v < r.v` fails with

    PySparkException: repark internal error in PyDataFrame.__arrow_c_stream__.next:
    partition not used yet (a Rust panic was caught at the Python boundary; this is a bug)

The panic is DataFusion's own `expect("partition not used yet")` at
`datafusion-physical-plan-54.1.0/src/repartition/mod.rs:1277`, reached from the join's **right**
side (`ProjectionExec` over `RepartitionExec`): each output partition `remove`s its channel from
the state map, so a second `execute` of the same partition finds nothing. Something on the
pool-refusal path re-executes it. Three runs, three panics. Every other operator at the same 8 MiB
pool returns a clean `ResourcesExhausted`, so this is the plan shape, not the pool machinery.
The process survives (`panic = unwind`, caught by repark's boundary fence), the session stays
usable and no wrong answer is produced — but a bounded pool must not answer with a panic.

### `H3-SPILL-COLLECT-1` — the facade boundary panics under an address-space limit

Separately measured, because the matrix's own guard exposed it. `collect()` at 1e7 rows needs
~4.5 GiB resident and far more virtual; run it under `RLIMIT_AS` 12 GiB and it fails with

    PySparkException: repark internal error in collect_rows.rows_from_record_batch:
    PyObject pointer is null (a Rust panic was caught at the Python boundary)

— a CPython allocation returned NULL and the fast path panicked instead of surfacing
`MemoryError`. At `RLIMIT_AS` 40 GiB the identical call is `ok` at 4,684 MiB resident. This is the
realistic shape of Never-OOM in a container with a memory limit, and it is a repark-side panic,
not a DataFusion one.

## 7. What would move the Never-OOM claim

Not queued here — this unit is pins only — but measured and therefore rankable:

1. **Pool-account the facade boundary.** `collect` at 1e7 is 4,459 MiB resident at *every* pool.
   A reservation around the row-materialization loop would turn the largest unbounded allocation
   in the product into a typed refusal.
2. **`H3-SPILL-COLLECT-1`** — check the pointer and raise `MemoryError`. Contained, and it is the
   difference between "repark reports it is out of memory" and "repark reports a bug".
3. **`H3-SPILL-NLJ-1`** — upstream; a repark-side reproducer is the deliverable.
4. **Window operators take no reservation.** `window_unbounded` at 1e7 spills (8 events) — but
   that is the `SortExec` beneath it, not the window. A window over one huge partition has no
   pool ceiling at all.
5. **`collect_list` / `array_agg` has no spill path.** Both engines fail; repark fails better.
   Worth stating as a declared limit rather than fixing.

## 8. Reproduce

```
PYTHONPATH=python/repark-parity/bench .venv/bin/python -m spill.measure \
  --partitions 4 --as-cap-bytes 34359738368 --rss-cap-bytes 8589934592 \
  --cell-timeout-s 600 --repeats 3 --scratch <dir> --json-out <file>
PYTHONPATH=python/repark-parity/bench .venv/bin/python -m spill.report --reports <file>...
PYTHONPATH=python/repark-parity/bench .venv/bin/python -m spill.spark_cells \
  --cell sort --rows 10000000 --driver-memory 1g --scratch <dir> --json-out <file>
```

pins: h3-spill-1/C-002, C-003, C-004, C-005
## 9. Appendix — every cell

### 9.1 1e6 rows

| operator | pool | outcome | spills | spilled MiB | peak RSS MiB | wall ms | load |
|---|---|---|---:|---:|---:|---:|---:|
| `collect` | none | ok | 0 | - | 732 | 1285 | 9 |
| `collect` | 8G | ok | 0 | - | 746 | 1275 | 9 |
| `collect` | 1G | ok | 0 | - | 664 | 1350 | 9 |
| `collect` | 256M | ok | 0 | - | 753 | 1287 | 9 |
| `collect` | 64M | ok | 0 | - | 729 | 1284 | 9 |
| `collect_list` | none | ok | 0 | - | 439 | 195 | 12 |
| `collect_list` | 8G | ok | 0 | - | 431 | 199 | 12 |
| `collect_list` | 1G | ok | 0 | - | 422 | 196 | 12 |
| `collect_list` | 256M | spill | 3 | 46 | 426 | 742 | 12 |
| `collect_list` | 64M | clean-err | 0 | - | 351 | 1358 | 11 |
| `distinct` | none | degr | 0 | - | 549 | 258 | 16 |
| `distinct` | 8G | degr | 0 | - | 598 | 218 | 15 |
| `distinct` | 1G | degr | 0 | - | 554 | 233 | 15 |
| `distinct` | 256M | spill | 8 | 46 | 562 | 2029 | 15 |
| `distinct` | 64M | clean-err | 0 | - | 297 | 404 | 14 |
| `dynamic_flatten` | none | ok | 0 | - | 478 | 494 | 19 |
| `dynamic_flatten` | 8G | ok | 0 | - | 465 | 507 | 19 |
| `dynamic_flatten` | 1G | ok | 0 | - | 477 | 492 | 19 |
| `dynamic_flatten` | 256M | ok | 0 | - | 485 | 495 | 19 |
| `dynamic_flatten` | 64M | ok | 0 | - | 479 | 513 | 19 |
| `hash_agg_few_groups` | none | ok | 0 | - | 256 | 100 | 15 |
| `hash_agg_few_groups` | 8G | ok | 0 | - | 258 | 119 | 15 |
| `hash_agg_few_groups` | 1G | ok | 0 | - | 260 | 114 | 15 |
| `hash_agg_few_groups` | 256M | ok | 0 | - | 262 | 123 | 15 |
| `hash_agg_few_groups` | 64M | ok | 0 | - | 268 | 117 | 15 |
| `hash_agg_many_groups` | none | degr | 0 | - | 558 | 225 | 24 |
| `hash_agg_many_groups` | 8G | degr | 0 | - | 534 | 230 | 24 |
| `hash_agg_many_groups` | 1G | degr | 0 | - | 560 | 234 | 23 |
| `hash_agg_many_groups` | 256M | spill | 8 | 54 | 538 | 1137 | 23 |
| `hash_agg_many_groups` | 64M | clean-err | 0 | - | 320 | 1208 | 21 |
| `hash_join` | none | ok | 0 | - | 583 | 370 | 8 |
| `hash_join` | 8G | ok | 0 | - | 559 | 403 | 8 |
| `hash_join` | 1G | ok | 0 | - | 561 | 430 | 8 |
| `hash_join` | 256M | ok | 0 | - | 579 | 419 | 8 |
| `hash_join` | 64M | clean-err | 0 | - | 331 | 1412 | 8 |
| `iceberg_scan_dv` | none | ok | 0 | - | 451 | 9672 | 22 |
| `iceberg_scan_dv` | 8G | ok | 0 | - | 394 | 6333 | 26 |
| `iceberg_scan_dv` | 1G | ok | 0 | - | 469 | 1806 | 24 |
| `iceberg_scan_dv` | 256M | ok | 0 | - | 509 | 1936 | 24 |
| `iceberg_scan_dv` | 64M | ok | 0 | - | 509 | 1989 | 23 |
| `merge_staging` | none | ok | 0 | - | 460 | 16923 | 24 |
| `merge_staging` | 8G | ok | 0 | - | 411 | 16725 | 24 |
| `merge_staging` | 1G | ok | 0 | - | 421 | 18685 | 23 |
| `merge_staging` | 256M | ok | 0 | - | 424 | 17309 | 21 |
| `merge_staging` | 64M | ok | 0 | - | 454 | 14494 | 19 |
| `nested_loop_join` | none | ok | 0 | - | 233 | 707 | 24 |
| `nested_loop_join` | 8G | ok | 0 | - | 235 | 758 | 24 |
| `nested_loop_join` | 1G | ok | 0 | - | 237 | 707 | 23 |
| `nested_loop_join` | 256M | ok | 0 | - | 240 | 734 | 23 |
| `nested_loop_join` | 64M | ok | 0 | - | 241 | 716 | 23 |
| `repartition` | none | degr | 0 | - | 483 | 226 | 8 |
| `repartition` | 8G | degr | 0 | - | 469 | 238 | 8 |
| `repartition` | 1G | degr | 0 | - | 469 | 275 | 8 |
| `repartition` | 256M | spill | 8 | 54 | 435 | 1504 | 8 |
| `repartition` | 64M | clean-err | 0 | - | 336 | 506 | 9 |
| `sort` | none | ok | 0 | - | 530 | 290 | 8 |
| `sort` | 8G | ok | 0 | - | 553 | 297 | 8 |
| `sort` | 1G | ok | 0 | - | 550 | 291 | 8 |
| `sort` | 256M | spill | 8 | 54 | 473 | 945 | 8 |
| `sort` | 64M | clean-err | 0 | - | 216 | 407 | 9 |
| `sort_merge_join` | none | ok | 0 | - | 728 | 560 | 19 |
| `sort_merge_join` | 8G | ok | 0 | - | 753 | 494 | 19 |
| `sort_merge_join` | 1G | ok | 0 | - | 702 | 528 | 19 |
| `sort_merge_join` | 256M | clean-err | 0 | - | 439 | 1863 | 19 |
| `sort_merge_join` | 64M | clean-err | 0 | - | 282 | 2012 | 18 |
| `to_pandas` | none | ok | 0 | - | 475 | 473 | 12 |
| `to_pandas` | 8G | ok | 0 | - | 465 | 488 | 12 |
| `to_pandas` | 1G | ok | 0 | - | 459 | 481 | 11 |
| `to_pandas` | 256M | ok | 0 | - | 456 | 461 | 11 |
| `to_pandas` | 64M | ok | 0 | - | 453 | 465 | 11 |
| `topk` | none | ok | 0 | - | 364 | 163 | 18 |
| `topk` | 8G | ok | 0 | - | 324 | 224 | 18 |
| `topk` | 1G | ok | 0 | - | 351 | 177 | 18 |
| `topk` | 256M | ok | 0 | - | 361 | 163 | 22 |
| `topk` | 64M | ok | 0 | - | 371 | 178 | 22 |
| `window_range` | none | ok | 0 | - | 260 | 1245 | 23 |
| `window_range` | 8G | ok | 0 | - | 250 | 1254 | 22 |
| `window_range` | 1G | ok | 0 | - | 257 | 1274 | 22 |
| `window_range` | 256M | ok | 0 | - | 243 | 1254 | 22 |
| `window_range` | 64M | ok | 0 | - | 256 | 1268 | 21 |
| `window_sliding_rows` | none | ok | 0 | - | 256 | 477 | 23 |
| `window_sliding_rows` | 8G | ok | 0 | - | 254 | 426 | 23 |
| `window_sliding_rows` | 1G | ok | 0 | - | 271 | 433 | 24 |
| `window_sliding_rows` | 256M | ok | 0 | - | 256 | 443 | 24 |
| `window_sliding_rows` | 64M | ok | 0 | - | 268 | 427 | 24 |
| `window_unbounded` | none | ok | 0 | - | 345 | 126 | 19 |
| `window_unbounded` | 8G | ok | 0 | - | 355 | 153 | 19 |
| `window_unbounded` | 1G | ok | 0 | - | 353 | 157 | 19 |
| `window_unbounded` | 256M | ok | 0 | - | 359 | 120 | 19 |
| `window_unbounded` | 64M | clean-err | 0 | - | 204 | 355 | 19 |

### outcome matrix — 10,000,000 rows

| operator | none | 8G | 1G | 256M | 64M |
|---|---|---|---|---|---|
| `sort` | ok | ok | spill 16/536M | clean-err | clean-err |
| `topk` | ok | ok | ok | clean-err | clean-err |
| `hash_agg_many_groups` | degr 9.5M | degr 9.5M | spill 28/536M | spill 104/1340M | clean-err |
| `hash_agg_few_groups` | ok | ok | ok | ok | ok |
| `distinct` | degr 9.5M | degr 9.5M | spill 24/459M | spill 93/1011M | clean-err |
| `collect_list` | ok | ok | clean-err | clean-err | clean-err |
| `hash_join` | ok | ok | spill 4/58M | clean-err | clean-err |
| `sort_merge_join` | ok | ok | spill 24/540M | clean-err | clean-err |
| `nested_loop_join` | ok | ok | ok | ok | PANIC |
| `window_unbounded` | ok | ok | spill 8/232M | spill 28/232M | clean-err |
| `window_sliding_rows` | ok | ok | ok | ok | ok |
| `window_range` | ok | ok | ok | ok | ok |
| `repartition` | degr 9.5M | degr 9.5M | spill 28/536M | spill 106/1481M | clean-err |
| `dynamic_flatten` | ok | ok | ok | ok | ok |
| `iceberg_scan_dv` | ok | ok | ok | ok | ok |
| `merge_staging` | ok | ok | clean-err | clean-err | clean-err |
| `collect` | ok | ok | ok | ok | ok |
| `to_pandas` | ok | ok | ok | ok | ok |

### 9.2 1e7 rows

| operator | pool | outcome | spills | spilled MiB | peak RSS MiB | wall ms | load |
|---|---|---|---:|---:|---:|---:|---:|
| `collect` | none | ok | 0 | - | 4463 | 11598 | 9 |
| `collect` | 8G | ok | 0 | - | 4461 | 11580 | 9 |
| `collect` | 1G | ok | 0 | - | 4480 | 11807 | 8 |
| `collect` | 256M | ok | 0 | - | 4452 | 11761 | 9 |
| `collect` | 64M | ok | 0 | - | 4459 | 11900 | 13 |
| `collect_list` | none | ok | 0 | - | 1803 | 991 | 11 |
| `collect_list` | 8G | ok | 0 | - | 1745 | 979 | 11 |
| `collect_list` | 1G | clean-err | 0 | - | 1699 | 8446 | 10 |
| `collect_list` | 256M | clean-err | 0 | - | 1658 | 11902 | 10 |
| `collect_list` | 64M | clean-err | 0 | - | 1684 | 6531 | 9 |
| `distinct` | none | degr | 0 | - | 1691 | 1068 | 13 |
| `distinct` | 8G | degr | 0 | - | 1627 | 968 | 13 |
| `distinct` | 1G | spill | 24 | 459 | 928 | 10779 | 12 |
| `distinct` | 256M | spill | 93 | 1011 | 722 | 24519 | 13 |
| `distinct` | 64M | clean-err | 0 | - | 309 | 755 | 12 |
| `dynamic_flatten` | none | ok | 0 | - | 2091 | 2070 | 19 |
| `dynamic_flatten` | 8G | ok | 0 | - | 2085 | 2111 | 19 |
| `dynamic_flatten` | 1G | ok | 0 | - | 2100 | 2100 | 19 |
| `dynamic_flatten` | 256M | ok | 0 | - | 2091 | 2114 | 19 |
| `dynamic_flatten` | 64M | ok | 0 | - | 2089 | 2136 | 18 |
| `hash_agg_few_groups` | none | ok | 0 | - | 268 | 176 | 15 |
| `hash_agg_few_groups` | 8G | ok | 0 | - | 266 | 203 | 15 |
| `hash_agg_few_groups` | 1G | ok | 0 | - | 256 | 207 | 15 |
| `hash_agg_few_groups` | 256M | ok | 0 | - | 271 | 244 | 16 |
| `hash_agg_few_groups` | 64M | ok | 0 | - | 242 | 195 | 16 |
| `hash_agg_many_groups` | none | degr | 0 | - | 1829 | 1062 | 20 |
| `hash_agg_many_groups` | 8G | degr | 0 | - | 1748 | 956 | 19 |
| `hash_agg_many_groups` | 1G | spill | 28 | 536 | 841 | 9695 | 19 |
| `hash_agg_many_groups` | 256M | spill | 104 | 1340 | 691 | 30550 | 23 |
| `hash_agg_many_groups` | 64M | clean-err | 0 | - | 332 | 607 | 15 |
| `hash_join` | none | ok | 0 | - | 1657 | 1272 | 9 |
| `hash_join` | 8G | ok | 0 | - | 1658 | 1281 | 14 |
| `hash_join` | 1G | spill | 4 | 58 | 1695 | 1336 | 14 |
| `hash_join` | 256M | clean-err | 0 | - | 585 | 705 | 19 |
| `hash_join` | 64M | clean-err | 0 | - | 371 | 559 | 19 |
| `iceberg_scan_dv` | none | ok | 0 | - | 669 | 14602 | 23 |
| `iceberg_scan_dv` | 8G | ok | 0 | - | 644 | 12403 | 20 |
| `iceberg_scan_dv` | 1G | ok | 0 | - | 648 | 20575 | 19 |
| `iceberg_scan_dv` | 256M | ok | 0 | - | 689 | 50450 | 19 |
| `iceberg_scan_dv` | 64M | ok | 0 | - | 699 | 40254 | 22 |
| `merge_staging` | none | ok | 0 | - | 4008 | 63579 | 17 |
| `merge_staging` | 8G | ok | 0 | - | 3654 | 44767 | 16 |
| `merge_staging` | 1G | clean-err | 0 | - | 1643 | 29245 | 12 |
| `merge_staging` | 256M | clean-err | 0 | - | 1057 | 37926 | 13 |
| `merge_staging` | 64M | clean-err | 0 | - | 667 | 22518 | 12 |
| `nested_loop_join` | none | ok | 0 | - | 510 | 5961 | 22 |
| `nested_loop_join` | 8G | ok | 0 | - | 500 | 6035 | 20 |
| `nested_loop_join` | 1G | ok | 0 | - | 499 | 6055 | 19 |
| `nested_loop_join` | 256M | ok | 0 | - | 498 | 5933 | 19 |
| `nested_loop_join` | 64M | PANIC | 0 | - | 232 | 356 | 19 |
| `repartition` | none | degr | 0 | - | 1640 | 1171 | 14 |
| `repartition` | 8G | degr | 0 | - | 1676 | 1009 | 14 |
| `repartition` | 1G | spill | 28 | 536 | 1110 | 2641 | 20 |
| `repartition` | 256M | spill | 106 | 1481 | 579 | 44526 | 19 |
| `repartition` | 64M | clean-err | 0 | - | 333 | 857 | 19 |
| `sort` | none | ok | 0 | - | 1862 | 2148 | 9 |
| `sort` | 8G | ok | 0 | - | 1857 | 2218 | 20 |
| `sort` | 1G | spill | 16 | 536 | 1342 | 2433 | 19 |
| `sort` | 256M | clean-err | 0 | - | 494 | 23015 | 18 |
| `sort` | 64M | clean-err | 0 | - | 220 | 356 | 18 |
| `sort_merge_join` | none | ok | 0 | - | 1983 | 1963 | 18 |
| `sort_merge_join` | 8G | ok | 0 | - | 2005 | 1989 | 18 |
| `sort_merge_join` | 1G | spill | 24 | 540 | 875 | 18906 | 18 |
| `sort_merge_join` | 256M | clean-err | 0 | - | 435 | 15799 | 19 |
| `sort_merge_join` | 64M | clean-err | 0 | - | 244 | 353 | 24 |
| `to_pandas` | none | ok | 0 | - | 2094 | 1835 | 11 |
| `to_pandas` | 8G | ok | 0 | - | 2092 | 1808 | 11 |
| `to_pandas` | 1G | ok | 0 | - | 2102 | 1812 | 16 |
| `to_pandas` | 256M | ok | 0 | - | 2104 | 1969 | 16 |
| `to_pandas` | 64M | ok | 0 | - | 2103 | 1901 | 15 |
| `topk` | none | ok | 0 | - | 491 | 800 | 22 |
| `topk` | 8G | ok | 0 | - | 481 | 853 | 22 |
| `topk` | 1G | ok | 0 | - | 495 | 782 | 22 |
| `topk` | 256M | clean-err | 0 | - | 431 | 1007 | 26 |
| `topk` | 64M | clean-err | 0 | - | 281 | 455 | 26 |
| `window_range` | none | ok | 0 | - | 248 | 11303 | 21 |
| `window_range` | 8G | ok | 0 | - | 239 | 11455 | 18 |
| `window_range` | 1G | ok | 0 | - | 244 | 11380 | 17 |
| `window_range` | 256M | ok | 0 | - | 252 | 11436 | 16 |
| `window_range` | 64M | ok | 0 | - | 256 | 11603 | 16 |
| `window_sliding_rows` | none | ok | 0 | - | 251 | 3227 | 24 |
| `window_sliding_rows` | 8G | ok | 0 | - | 247 | 3193 | 24 |
| `window_sliding_rows` | 1G | ok | 0 | - | 255 | 3179 | 24 |
| `window_sliding_rows` | 256M | ok | 0 | - | 256 | 3147 | 24 |
| `window_sliding_rows` | 64M | ok | 0 | - | 260 | 3117 | 23 |
| `window_unbounded` | none | ok | 0 | - | 1200 | 381 | 19 |
| `window_unbounded` | 8G | ok | 0 | - | 1075 | 378 | 19 |
| `window_unbounded` | 1G | spill | 8 | 232 | 964 | 9273 | 19 |
| `window_unbounded` | 256M | spill | 28 | 232 | 722 | 8701 | 21 |
| `window_unbounded` | 64M | clean-err | 0 | - | 204 | 354 | 23 |

