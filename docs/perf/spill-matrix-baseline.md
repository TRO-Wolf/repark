# The spill matrix — the Never-OOM truth table (H3-SPILL-1, 2026-09-05)

**What this is.** [PROJECT.md](../../PROJECT.md) promises "predictable memory via spill-to-disk by
default" and holds the stronger *never OOM on data larger than RAM* claim to what has been
measured. This is that measurement. For every operator the engine can plan, under a bounded
`FairSpillPool` at two scales, it records whether the operator **spills**, **degrades**,
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
under a 64 MiB pool. They do not OOM here because the box is large, not because the pool held them
— though each does return the identical answer digest at every pool, so what the pool does not
bound, it also does not corrupt.

## 1. The machine, the module, the method

| | |
|---|---|
| Host | Linux 6.8.0-138-generic x86_64, glibc 2.39, 64 cores, 125 GiB RAM |
| Module | release (`maturin develop --release`, `__debug_assertions__ = False`), repark 1.0.1 |
| Engine | DataFusion 54.1.0 |
| Python | 3.12.3 |
| Per cell | one fresh subprocess, `datafusion.execution.target_partitions = 4`, session defaults otherwise (`batch_size` 65536) |
| Guard | parent polls `/proc/<pid>/status` `VmHWM` every 50 ms and kills past `--rss-cap-bytes` 8 GiB; `RLIMIT_AS` 32 GiB is a backstop only |
| Peak RSS | `VmHWM` on both sides, never `ru_maxrss` — rusage is retained across `execve`, so a child would report its parent's high-water mark |
| Concurrency | three driver lanes ran concurrently; the 1-minute load at each cell's start is in the tables, and wall is read against it |
| Repeats | every cell whose first outcome was not `ok` ran three times; **every repeat keeps its own answer digest**, and all of them are checked |
| Evidence | [spill-matrix-baseline-cells.json](spill-matrix-baseline-cells.json) — every cell, every repeat, both caps, per-operator plan metrics, and the three Spark cells with the JVM's own stderr |
| Harness | [python/repark-parity/bench/spill/](../../python/repark-parity/bench/spill/map.md) |

**The fixture.** One wide deterministic view, `id`, `h = md5(id)`, `g = id % 1024`, a 64-character
varied `payload`, and `v = id * 1.5` — about 120 bytes of payload per row, so the 1e7 scale is
~1.2 GiB of live data and genuinely exceeds the 1 GiB pool. `v` is chosen so every per-row double
is exact, which is what lets the answer digests be integer checksums.

**Why `EXPLAIN ANALYZE` is the instrument.** It executes the plan in full and discards the output,
so the recorded peak RSS is the operator's and not the collector's, and it reports
`spill_count` / `spilled_bytes` / `skipped_aggregation_rows` per physical operator. Those counters
are the assertion; wall time never is.

### 1.1 How each answer is checked, and how strong the check is

A row count is not an answer check. Every row carries a content digest, every digest is
commutative (so it is order-independent — verified by re-running at 1, 4 and 8 target partitions
and getting one value), and every digest moves when a row is dropped, duplicated or altered. The
kind is recorded per cell in the evidence file:

| operator | digest kind |
|---|---|
| `sort` | `engine_checksum` |
| `topk` | `engine_checksum` |
| `hash_agg_many_groups` | `engine_checksum` |
| `hash_agg_few_groups` | `engine_checksum` |
| `distinct` | `engine_checksum` |
| `collect_list` | `engine_checksum` |
| `hash_join` | `engine_checksum` |
| `sort_merge_join` | `engine_checksum` |
| `nested_loop_join` | `engine_checksum` |
| `window_unbounded` | `engine_checksum` |
| `window_sliding_rows` | `engine_checksum` |
| `window_range` | `engine_checksum` |
| `repartition` | `engine_checksum` |
| `dynamic_flatten` | `engine_checksum_over_flattened_frame` |
| `iceberg_scan_dv` | `engine_checksum_over_scanned_table` |
| `merge_staging` | `engine_checksum_over_merged_table` |
| `collect` | `python_row_crc32_sum_and_xor` |
| `to_pandas` | `pandas_hash_pandas_object_sum` |

`engine_checksum` is the row's own `digest_sql`: counts, integer checksums, `sum(crc32(...))` over
the key columns, and a sort-inversion count where row order is contractual. The three
`engine_checksum_over_*` kinds are `count(*)` plus `sum(crc32(...))` over **every** column of the
flattened frame, the scanned table or the merged table. `collect` and `toPandas` are digested in
Python from the materialized objects rather than from a query, because for those two rows the
thing under test is what crossed the facade boundary.

**The probe must not distort the cell it measures.** The `toPandas` digest is taken in 100 000-row
chunks for exactly this reason: hashing a 1e7-row frame whole pushed the worker past the 8 GiB
resident cap, and the matrix would then have recorded the probe's memory as the operator's. As
taken, the probe costs about 30 MiB on the 2.1 GiB cell. `collect`'s digest is a streaming fold
and costs nothing measurable.

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

### 3.1 1 000 000 rows

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

### 3.2 10 000 000 rows

| operator | none | 8G | 1G | 256M | 64M |
|---|---|---|---|---|---|
| `sort` | ok | ok | spill 16/536M | clean-err | clean-err |
| `topk` | ok | ok | ok | clean-err | clean-err |
| `hash_agg_many_groups` | degr 9.5M | degr 9.5M | spill 28/536M | spill 103/1210M | clean-err |
| `hash_agg_few_groups` | ok | ok | ok | ok | ok |
| `distinct` | degr 9.5M | degr 9.5M | spill 24/459M | spill 94/1047M | clean-err |
| `collect_list` | ok | ok | clean-err | clean-err | clean-err |
| `hash_join` | ok | ok | spill 4/87M | clean-err | clean-err |
| `sort_merge_join` | ok | ok | spill 24/536M | clean-err | clean-err |
| `nested_loop_join` | ok | ok | ok | ok | PANIC |
| `window_unbounded` | ok | ok | spill 8/232M | spill 26/232M | clean-err |
| `window_sliding_rows` | ok | ok | ok | ok | ok |
| `window_range` | ok | ok | ok | ok | ok |
| `repartition` | degr 9.5M | degr 9.5M | spill 28/536M | spill 103/1242M | clean-err |
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

**Answer coverage, exactly.** Of the 180 cells, 144 are bounded (36 are the `pool=none`
baselines). **115 of those 144 bounded cells carry an answer digest and every one of them equals
the unbounded run** — 163 individual run digests once the repeats are counted, because a repeated
cell keeps a digest per run and all of them are checked, not just the first. The 29 bounded cells
with no digest break down as **28 refusals** — a cell that raised has no answer to hash — and
**one probe failure**: `sort` at 256 MiB / 1e6, where the probe query itself exhausted the same
pool. There is no third category: every operator, including `repartition`, has a probe.

Only one cell was outcome-unstable across its three runs: `sort` at 256 MiB / 1e6 came back
`spilled, clean_error, spilled`. It sits exactly on the boundary where the four `SortExec`
partitions plus the `SortPreservingMergeExec` merge reservation together straddle the pool, so
which side it lands on depends on the order the partitions reach the pool. **No pin is placed on
that cell**; the pins use 64 MiB (always refuses) and 1 GiB at 1e7 (always spills).

## 4. What the numbers say, operator by operator

**Spills, and stays under the pool while it does.** `sort`, grouped `hash aggregate`, `distinct`,
`RepartitionExec` and `sort_merge_join` all spill and complete, and resident memory falls as the
pool falls — `hash_agg_many_groups` at 1e7 goes 1,656 MiB (unbounded) → 1,077 (1 GiB pool, 28
spills, 536 MiB spilled) → 695 (256 MiB pool, 103 spills, 1,210 MiB spilled). That is spill-to-disk
doing
exactly what the north star claims, and it is the only part of the claim the pool is responsible
for.

**Degrades before it spills.** The high-cardinality aggregate, `distinct` and `repartition` report
`skipped_aggregation_rows` of 475,700 at 1e6 (48 % of input) and 9,480,000 at 1e7 (95 %) at the
8 GiB and unbounded pools: the
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
anyway. `topk` is `ok` down to 64 MiB at 1e6, and its two refusals at 1e7 are worth reading rather
than assuming: at 256 MiB the reservation that fails is `SortPreservingMergeExec[0]` beneath it,
but at 64 MiB it is `TopK[0]` itself — *"Failed to allocate additional 1856.0 KB for TopK[0] with
14.0 MB already allocated"*. **A bounded-cardinality TopK is not a bounded-memory TopK:** the heap
holds 100 rows but pins the whole 64k-row batches those rows point into, so its footprint scales
with batch size and row width, not with `LIMIT`. `dynamic_flatten`
(2,142 MiB at a 64 MiB pool against 2,119 unbounded), `to_pandas` (2,120 against 2,078), `collect`
(4,393 against 4,413) and the Iceberg DV scan (679 against 699) are flat at every pool including
64 MiB, because none of them registers with
it — and each returns the identical digest at every pool, so they are not merely surviving, they
are answering the same. **Those four rows are the honest boundary of Never-OOM: the pool is not a
memory limit for them, and there is no configuration that makes it one.**

**Costs wall time to spill.** `hash_agg_many_groups` at 1e7 is 2,286 ms unbounded, 11,479 ms at a
1 GiB pool and 42,127 ms at 256 MiB; `repartition` is 1,152 / 9,851 / 21,490 ms. An 8-18x wall cost
at the smallest pool that still completes is the price of the guarantee. Read those against the
1-minute load in the tables: three lanes were running.

## 5. Apache Spark on the same fixture

Three cells, `spark.driver.memory = 1g`, `local[4]`, PySpark 4.1.2 on Zulu 17, the same 1e7-row
projection, materialized through the `noop` sink so the optimizer cannot drop the operator under
test. Spill is read from the Spark event log (`Memory Bytes Spilled` / `Disk Bytes Spilled`
summed over every `SparkListenerTaskEnd`), and the JVM's own stderr is captured at file descriptor
2 so a published failure string is a recorded one.

| cell | Spark outcome | memory spilled | disk spilled | wall | repark at a 1 GiB pool |
|---|---|---:|---:|---:|---|
| `sort` | ok | 318,765,888 B | 228,603,560 B | 9,142 ms | `spilled` 16 events / 536 MiB |
| `hash_join` | ok | 721,418,048 B | 433,678,966 B | 10,047 ms | `spilled` 4 events / 58 MiB |
| `collect_list` | **error** | — | — | — | `clean_error`, process healthy |

The `collect_list` failure, quoted from the captured JVM stderr rather than inferred from the
Python-side exception:

```
Exception in thread "refresh progress" java.lang.OutOfMemoryError: Java heap space
26/09/05 08:41:52 ERROR Executor: Exception in task 2.0 in stage 0.0 (TID 2)
java.lang.OutOfMemoryError: Java heap space
```

Those are the first three of the six lines the run recorded, in the order it recorded them.

The SparkContext does not survive it; the Python side sees only `Job 0 cancelled because
SparkContext was shut down`, which is why the JVM stderr is captured — the interesting sentence is
never the one the driver reports. Every line above is in
[spill-matrix-baseline-cells.json](spill-matrix-baseline-cells.json) under `jvm_error_lines`.

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
~4.5 GiB resident and far more virtual; run it under an `RLIMIT_AS` ceiling and it fails with

    PySparkException: repark internal error in collect_rows.rows_from_record_batch:
    PyObject pointer is null (a Rust panic was caught at the Python boundary)

— a CPython allocation returned NULL and the fast path panicked instead of surfacing
`MemoryError`. With 6 GiB of headroom the identical call is `ok`. This is the realistic shape of
Never-OOM in a container with a memory limit, and it is a repark-side panic, not a DataFusion one.

## 7. What would move the Never-OOM claim

Not queued here — this unit is pins only — but measured and therefore rankable:

1. **Pool-account the facade boundary.** `collect` at 1e7 is ~4.5 GiB resident at *every* pool.
   A reservation around the row-materialization loop would turn the largest unbounded allocation
   in the product into a typed refusal.
2. **`H3-SPILL-COLLECT-1`** — check the pointer and raise `MemoryError`. Contained, and it is the
   difference between "repark reports it is out of memory" and "repark reports a bug".
3. **`H3-SPILL-NLJ-1`** — upstream; a repark-side reproducer is the deliverable.
4. **Window operators take no reservation.** `window_unbounded` at 1e7 spills — but that is the
   `SortExec` beneath it, not the window. A window over one huge partition has no pool ceiling.
5. **`collect_list` / `array_agg` has no spill path.** Both engines fail; repark fails better.
   Worth stating as a declared limit rather than fixing.

## 8. Reproduce

```
PYTHONPATH=python/repark-parity/bench .venv/bin/python -m spill.measure \
  --partitions 4 --as-cap-bytes 34359738368 --rss-cap-bytes 8589934592 \
  --cell-timeout-s 600 --repeats 3 --scratch <dir> --json-out <file>
PYTHONPATH=python/repark-parity/bench .venv/bin/python -m spill.report \
  --reports docs/perf/spill-matrix-baseline-cells.json --section outcomes|numbers|census
PYTHONPATH=python/repark-parity/bench .venv/bin/python -m spill.spark_cells \
  --cell sort --rows 10000000 --driver-memory 1g --scratch <dir> --json-out <file>
```

pins: h3-spill-1/C-002, C-003, C-004, C-005

## 9. Appendix — every cell

### 9.1 1 000 000 rows

| operator | pool | outcome | spills | spilled MiB | peak RSS MiB | wall ms | load |
|---|---|---|---:|---:|---:|---:|---:|
| `collect` | none | ok | 0 | - | 747 | 1043 | 19 |
| `collect` | 8G | ok | 0 | - | 692 | 1175 | 19 |
| `collect` | 1G | ok | 0 | - | 683 | 1363 | 19 |
| `collect` | 256M | ok | 0 | - | 665 | 1193 | 19 |
| `collect` | 64M | ok | 0 | - | 729 | 1072 | 18 |
| `collect_list` | none | ok | 0 | - | 446 | 200 | 23 |
| `collect_list` | 8G | ok | 0 | - | 434 | 205 | 23 |
| `collect_list` | 1G | ok | 0 | - | 436 | 182 | 23 |
| `collect_list` | 256M | spill | 3 | 46 | 438 | 2625 | 22 |
| `collect_list` | 64M | clean-err | 0 | - | 373 | 1059 | 20 |
| `distinct` | none | degr | 0 | - | 587 | 224 | 22 |
| `distinct` | 8G | degr | 0 | - | 551 | 275 | 22 |
| `distinct` | 1G | degr | 0 | - | 533 | 234 | 22 |
| `distinct` | 256M | spill | 8 | 46 | 490 | 1412 | 23 |
| `distinct` | 64M | clean-err | 0 | - | 306 | 555 | 28 |
| `dynamic_flatten` | none | ok | 0 | - | 544 | 476 | 22 |
| `dynamic_flatten` | 8G | ok | 0 | - | 522 | 530 | 22 |
| `dynamic_flatten` | 1G | ok | 0 | - | 546 | 483 | 22 |
| `dynamic_flatten` | 256M | ok | 0 | - | 552 | 482 | 23 |
| `dynamic_flatten` | 64M | ok | 0 | - | 552 | 456 | 23 |
| `hash_agg_few_groups` | none | ok | 0 | - | 250 | 137 | 23 |
| `hash_agg_few_groups` | 8G | ok | 0 | - | 256 | 122 | 23 |
| `hash_agg_few_groups` | 1G | ok | 0 | - | 243 | 124 | 23 |
| `hash_agg_few_groups` | 256M | ok | 0 | - | 253 | 121 | 23 |
| `hash_agg_few_groups` | 64M | ok | 0 | - | 260 | 121 | 23 |
| `hash_agg_many_groups` | none | degr | 0 | - | 517 | 220 | 37 |
| `hash_agg_many_groups` | 8G | degr | 0 | - | 527 | 261 | 36 |
| `hash_agg_many_groups` | 1G | degr | 0 | - | 589 | 240 | 36 |
| `hash_agg_many_groups` | 256M | spill | 8 | 54 | 492 | 2209 | 35 |
| `hash_agg_many_groups` | 64M | clean-err | 0 | - | 362 | 2010 | 34 |
| `hash_join` | none | ok | 0 | - | 516 | 998 | 32 |
| `hash_join` | 8G | ok | 0 | - | 530 | 679 | 33 |
| `hash_join` | 1G | ok | 0 | - | 551 | 402 | 33 |
| `hash_join` | 256M | ok | 0 | - | 550 | 393 | 33 |
| `hash_join` | 64M | clean-err | 0 | - | 331 | 6835 | 33 |
| `iceberg_scan_dv` | none | ok | 0 | - | 470 | 639 | 33 |
| `iceberg_scan_dv` | 8G | ok | 0 | - | 481 | 700 | 37 |
| `iceberg_scan_dv` | 1G | ok | 0 | - | 476 | 604 | 34 |
| `iceberg_scan_dv` | 256M | ok | 0 | - | 468 | 517 | 30 |
| `iceberg_scan_dv` | 64M | ok | 0 | - | 474 | 517 | 29 |
| `merge_staging` | none | ok | 0 | - | 488 | 7396 | 30 |
| `merge_staging` | 8G | ok | 0 | - | 489 | 12938 | 29 |
| `merge_staging` | 1G | ok | 0 | - | 487 | 15965 | 28 |
| `merge_staging` | 256M | ok | 0 | - | 456 | 14269 | 28 |
| `merge_staging` | 64M | ok | 0 | - | 477 | 29674 | 26 |
| `nested_loop_join` | none | ok | 0 | - | 233 | 794 | 31 |
| `nested_loop_join` | 8G | ok | 0 | - | 234 | 746 | 31 |
| `nested_loop_join` | 1G | ok | 0 | - | 243 | 710 | 30 |
| `nested_loop_join` | 256M | ok | 0 | - | 236 | 681 | 30 |
| `nested_loop_join` | 64M | ok | 0 | - | 243 | 716 | 30 |
| `repartition` | none | degr | 0 | - | 529 | 256 | 14 |
| `repartition` | 8G | degr | 0 | - | 548 | 249 | 14 |
| `repartition` | 1G | degr | 0 | - | 559 | 253 | 14 |
| `repartition` | 256M | spill | 8 | 54 | 529 | 1239 | 14 |
| `repartition` | 64M | clean-err | 0 | - | 332 | 957 | 14 |
| `sort` | none | ok | 0 | - | 462 | 617 | 32 |
| `sort` | 8G | ok | 0 | - | 486 | 746 | 32 |
| `sort` | 1G | ok | 0 | - | 463 | 790 | 33 |
| `sort` | 256M | spill | 8 | 54 | 449 | 11913 | 33 |
| `sort` | 64M | clean-err | 0 | - | 213 | 610 | 32 |
| `sort_merge_join` | none | ok | 0 | - | 752 | 510 | 23 |
| `sort_merge_join` | 8G | ok | 0 | - | 739 | 482 | 23 |
| `sort_merge_join` | 1G | ok | 0 | - | 754 | 508 | 23 |
| `sort_merge_join` | 256M | clean-err | 0 | - | 385 | 3366 | 23 |
| `sort_merge_join` | 64M | clean-err | 0 | - | 323 | 2412 | 22 |
| `to_pandas` | none | ok | 0 | - | 537 | 479 | 12 |
| `to_pandas` | 8G | ok | 0 | - | 533 | 461 | 11 |
| `to_pandas` | 1G | ok | 0 | - | 547 | 474 | 11 |
| `to_pandas` | 256M | ok | 0 | - | 549 | 475 | 11 |
| `to_pandas` | 64M | ok | 0 | - | 538 | 478 | 11 |
| `topk` | none | ok | 0 | - | 338 | 166 | 29 |
| `topk` | 8G | ok | 0 | - | 340 | 198 | 28 |
| `topk` | 1G | ok | 0 | - | 344 | 189 | 28 |
| `topk` | 256M | ok | 0 | - | 338 | 183 | 28 |
| `topk` | 64M | ok | 0 | - | 337 | 166 | 28 |
| `window_range` | none | ok | 0 | - | 244 | 1267 | 22 |
| `window_range` | 8G | ok | 0 | - | 254 | 1233 | 22 |
| `window_range` | 1G | ok | 0 | - | 263 | 1268 | 22 |
| `window_range` | 256M | ok | 0 | - | 258 | 1274 | 22 |
| `window_range` | 64M | ok | 0 | - | 257 | 1228 | 22 |
| `window_sliding_rows` | none | ok | 0 | - | 257 | 479 | 24 |
| `window_sliding_rows` | 8G | ok | 0 | - | 271 | 458 | 24 |
| `window_sliding_rows` | 1G | ok | 0 | - | 274 | 426 | 24 |
| `window_sliding_rows` | 256M | ok | 0 | - | 271 | 435 | 24 |
| `window_sliding_rows` | 64M | ok | 0 | - | 264 | 415 | 24 |
| `window_unbounded` | none | ok | 0 | - | 351 | 145 | 26 |
| `window_unbounded` | 8G | ok | 0 | - | 345 | 129 | 26 |
| `window_unbounded` | 1G | ok | 0 | - | 359 | 144 | 26 |
| `window_unbounded` | 256M | ok | 0 | - | 353 | 145 | 25 |
| `window_unbounded` | 64M | clean-err | 0 | - | 203 | 505 | 25 |

### 9.2 10 000 000 rows

| operator | pool | outcome | spills | spilled MiB | peak RSS MiB | wall ms | load |
|---|---|---|---:|---:|---:|---:|---:|
| `collect` | none | ok | 0 | - | 4414 | 10513 | 18 |
| `collect` | 8G | ok | 0 | - | 4472 | 10290 | 16 |
| `collect` | 1G | ok | 0 | - | 4398 | 10550 | 17 |
| `collect` | 256M | ok | 0 | - | 4392 | 10502 | 16 |
| `collect` | 64M | ok | 0 | - | 4394 | 10758 | 14 |
| `collect_list` | none | ok | 0 | - | 1624 | 2221 | 19 |
| `collect_list` | 8G | ok | 0 | - | 1655 | 2242 | 19 |
| `collect_list` | 1G | clean-err | 0 | - | 1753 | 9851 | 19 |
| `collect_list` | 256M | clean-err | 0 | - | 1804 | 11554 | 17 |
| `collect_list` | 64M | clean-err | 0 | - | 1554 | 6990 | 17 |
| `distinct` | none | degr | 0 | - | 1659 | 1094 | 28 |
| `distinct` | 8G | degr | 0 | - | 1662 | 1521 | 28 |
| `distinct` | 1G | spill | 24 | 459 | 926 | 11145 | 27 |
| `distinct` | 256M | spill | 94 | 1047 | 670 | 28918 | 24 |
| `distinct` | 64M | clean-err | 0 | - | 325 | 1060 | 24 |
| `dynamic_flatten` | none | ok | 0 | - | 2119 | 2716 | 23 |
| `dynamic_flatten` | 8G | ok | 0 | - | 2132 | 2460 | 23 |
| `dynamic_flatten` | 1G | ok | 0 | - | 2157 | 2710 | 28 |
| `dynamic_flatten` | 256M | ok | 0 | - | 2145 | 2156 | 28 |
| `dynamic_flatten` | 64M | ok | 0 | - | 2143 | 2063 | 28 |
| `hash_agg_few_groups` | none | ok | 0 | - | 268 | 212 | 23 |
| `hash_agg_few_groups` | 8G | ok | 0 | - | 271 | 198 | 23 |
| `hash_agg_few_groups` | 1G | ok | 0 | - | 254 | 188 | 22 |
| `hash_agg_few_groups` | 256M | ok | 0 | - | 270 | 193 | 22 |
| `hash_agg_few_groups` | 64M | ok | 0 | - | 270 | 190 | 22 |
| `hash_agg_many_groups` | none | degr | 0 | - | 1657 | 2286 | 32 |
| `hash_agg_many_groups` | 8G | degr | 0 | - | 1723 | 1071 | 30 |
| `hash_agg_many_groups` | 1G | spill | 28 | 536 | 1077 | 11479 | 29 |
| `hash_agg_many_groups` | 256M | spill | 103 | 1210 | 695 | 42127 | 23 |
| `hash_agg_many_groups` | 64M | clean-err | 0 | - | 342 | 906 | 23 |
| `hash_join` | none | ok | 0 | - | 1567 | 2268 | 38 |
| `hash_join` | 8G | ok | 0 | - | 1717 | 1602 | 37 |
| `hash_join` | 1G | spill | 4 | 87 | 1624 | 10629 | 37 |
| `hash_join` | 256M | clean-err | 0 | - | 582 | 4823 | 28 |
| `hash_join` | 64M | clean-err | 0 | - | 367 | 6328 | 25 |
| `iceberg_scan_dv` | none | ok | 0 | - | 699 | 1130 | 26 |
| `iceberg_scan_dv` | 8G | ok | 0 | - | 690 | 1056 | 21 |
| `iceberg_scan_dv` | 1G | ok | 0 | - | 709 | 1240 | 25 |
| `iceberg_scan_dv` | 256M | ok | 0 | - | 659 | 1150 | 29 |
| `iceberg_scan_dv` | 64M | ok | 0 | - | 679 | 1076 | 37 |
| `merge_staging` | none | ok | 0 | - | 3516 | 106399 | 23 |
| `merge_staging` | 8G | ok | 0 | - | 3648 | 82445 | 22 |
| `merge_staging` | 1G | clean-err | 0 | - | 1697 | 42274 | 22 |
| `merge_staging` | 256M | clean-err | 0 | - | 1071 | 37288 | 25 |
| `merge_staging` | 64M | clean-err | 0 | - | 695 | 39124 | 28 |
| `nested_loop_join` | none | ok | 0 | - | 493 | 6321 | 29 |
| `nested_loop_join` | 8G | ok | 0 | - | 520 | 5966 | 28 |
| `nested_loop_join` | 1G | ok | 0 | - | 499 | 6004 | 28 |
| `nested_loop_join` | 256M | ok | 0 | - | 499 | 5935 | 27 |
| `nested_loop_join` | 64M | PANIC | 0 | - | 226 | 554 | 26 |
| `repartition` | none | degr | 0 | - | 1798 | 1152 | 18 |
| `repartition` | 8G | degr | 0 | - | 1839 | 972 | 18 |
| `repartition` | 1G | spill | 28 | 536 | 994 | 9851 | 17 |
| `repartition` | 256M | spill | 103 | 1242 | 710 | 21490 | 17 |
| `repartition` | 64M | clean-err | 0 | - | 350 | 856 | 18 |
| `sort` | none | ok | 0 | - | 1861 | 2382 | 31 |
| `sort` | 8G | ok | 0 | - | 1793 | 2013 | 30 |
| `sort` | 1G | spill | 16 | 536 | 1168 | 24657 | 28 |
| `sort` | 256M | clean-err | 0 | - | 493 | 25571 | 24 |
| `sort` | 64M | clean-err | 0 | - | 216 | 357 | 29 |
| `sort_merge_join` | none | ok | 0 | - | 1999 | 2036 | 22 |
| `sort_merge_join` | 8G | ok | 0 | - | 1997 | 1983 | 22 |
| `sort_merge_join` | 1G | spill | 24 | 536 | 887 | 25916 | 22 |
| `sort_merge_join` | 256M | clean-err | 0 | - | 394 | 32245 | 31 |
| `sort_merge_join` | 64M | clean-err | 0 | - | 315 | 3219 | 33 |
| `to_pandas` | none | ok | 0 | - | 2079 | 5077 | 11 |
| `to_pandas` | 8G | ok | 0 | - | 2112 | 4066 | 11 |
| `to_pandas` | 1G | ok | 0 | - | 2128 | 3425 | 11 |
| `to_pandas` | 256M | ok | 0 | - | 2138 | 4227 | 10 |
| `to_pandas` | 64M | ok | 0 | - | 2120 | 4632 | 11 |
| `topk` | none | ok | 0 | - | 481 | 803 | 28 |
| `topk` | 8G | ok | 0 | - | 512 | 815 | 39 |
| `topk` | 1G | ok | 0 | - | 485 | 811 | 39 |
| `topk` | 256M | clean-err | 0 | - | 414 | 1259 | 38 |
| `topk` | 64M | clean-err | 0 | - | 291 | 605 | 37 |
| `window_range` | none | ok | 0 | - | 242 | 11553 | 22 |
| `window_range` | 8G | ok | 0 | - | 247 | 11566 | 21 |
| `window_range` | 1G | ok | 0 | - | 241 | 11412 | 20 |
| `window_range` | 256M | ok | 0 | - | 243 | 11481 | 21 |
| `window_range` | 64M | ok | 0 | - | 244 | 11449 | 23 |
| `window_sliding_rows` | none | ok | 0 | - | 247 | 3151 | 23 |
| `window_sliding_rows` | 8G | ok | 0 | - | 256 | 3158 | 23 |
| `window_sliding_rows` | 1G | ok | 0 | - | 256 | 3162 | 23 |
| `window_sliding_rows` | 256M | ok | 0 | - | 288 | 3102 | 23 |
| `window_sliding_rows` | 64M | ok | 0 | - | 245 | 3246 | 22 |
| `window_unbounded` | none | ok | 0 | - | 1073 | 446 | 25 |
| `window_unbounded` | 8G | ok | 0 | - | 1213 | 400 | 25 |
| `window_unbounded` | 1G | spill | 8 | 232 | 974 | 7900 | 25 |
| `window_unbounded` | 256M | spill | 26 | 232 | 668 | 8624 | 24 |
| `window_unbounded` | 64M | clean-err | 0 | - | 174 | 807 | 24 |

