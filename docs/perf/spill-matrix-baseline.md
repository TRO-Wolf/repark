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
caught at the Python boundary; that is `H3-SPILL-NLJ-1` below, and it was the only failure of the
Never-OOM contract this matrix found. **It is fixed** (H3-SPILL-RESIDUE-1, 2026-09-06): that cell
and every other 8 MiB cell now answers with the typed refusal — see §6 and §7. The honest limit of
the claim is different and larger: **the
pool bounds only the operators that register with it.** Windows, `dynamicFlatten`, the Iceberg
scan and the whole facade boundary (`collect`, `toPandas`) are not pool-accounted in DataFusion
54.1, so their memory is whatever the data costs — 4.5 GiB resident for a 1e7-row `collect()`
under a 64 MiB pool. They do not OOM here because the box is large, not because the pool held them
— though each does return the identical answer digest at every pool, so what the pool does not
bound, it also does not corrupt.

**And one honest limit the census does not carry.** "No cell aborted" is a statement about these
180 cells, not about the boundary in general: outside the matrix, `toPandas()` on a 4e6-row frame
under `RLIMIT_AS` = `VmSize` + 64 MiB **aborts the process** 3/3 — `terminate called after
throwing an instance of 'std::system_error' … Resource temporarily unavailable`, SIGABRT and a
core dump — because pyarrow's C++ thread pool cannot create a thread and a C++ exception crosses
a `noexcept` boundary. It is not repark's panic and no repark code can catch it. At the identical
ceiling `collect()` raises `MemoryError` 3/3 with an empty stderr, which is the whole difference:
repark's own row path is contained (§6, `H3-SPILL-COLLECT-1`), and the pyarrow conversion beneath
`toPandas()` is not.

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
| Concurrency | three driver lanes (A, B, C) ran concurrently; the ten `to_pandas` cells were re-run alone as lane E after the chunked-probe fix; the 1-minute load at each cell's start is in the tables, and wall is read against it |
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
| Pool-accounted **and** spillable | `SortExec` (`ExternalSorter`), grouped `AggregateExec` (`row_hash`), `RepartitionExec`, `NestedLoopJoinExec` (fallback load path — **but the fallback is unsound in 54.1**: it re-executes partition 0 of a build child that `NestedLoopJoinExec::execute` already executed, which `RepartitionExec` answers with `expect("partition not used yet")`. So in practice a nested-loop join at a full pool refuses; it does not spill. See §6.) |
| Pool-accounted, **not** spillable — refuses when the pool is full | `HashJoinExec` (build side), `SortMergeJoinExec` (buffered side spills, streamed side does not), `CrossJoinExec`, `SymmetricHashJoinExec`, `PiecewiseMergeJoinExec`, ungrouped `AggregateStream`, `SortPreservingMergeExec`, `TopK`, `BufferExec`, `RecursiveQueryExec` |
| **Not pool-accounted at all** | `WindowAggExec`, `BoundedWindowAggExec`, `UnnestExec`, `CoalesceBatchesExec`, the repark Iceberg scan, the repark facade boundary (`collect`, `toPandas`) |

The third row is the load-bearing one. A pool cannot bound what never asks it for anything.

**What changed since (H3-SPILL-RESIDUE-1, 2026-09-06).** Two rows above are now qualified by
measurement rather than by reading. The `NestedLoopJoinExec` entry moved from "spillable" to
"spillable in intent only": its fallback is reached but cannot complete, and repark now reports
the pool refusal that triggered it rather than the engine's panic. And the last row — the
un-accounted facade boundary — is *still* un-accounted, but it no longer lies about it: a
`collect()` that runs out of address space raises `MemoryError`. Un-accounted still means
unbounded; it no longer means unreported.

## 3. The outcome matrix

Legend: `ok` in memory · `spill n/MiB` spilled, n spill events · `degr N` partial aggregation
skipped for N rows · `clean-err` typed `PySparkException` naming the pool · `PANIC` a Rust panic
caught at the Python boundary. No cell was `abort` or `wrong`.

**These tables are the 2026-09-05 H3-SPILL-1 measurement and are not re-run here.** Exactly one
cell has moved since: `nested_loop_join` at 64 MiB / 1e7, the single `PANIC`, is now `clean-err`
— re-measured 3/3 on 2026-09-06 by H3-SPILL-RESIDUE-1 (§6). Every other cell's claim stands as
recorded, and the 8 MiB re-run in §6 shows the other 17 operators unchanged.

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
| `nested_loop_join` | ok | ok | ok | ok | PANIC → clean-err (fixed 2026-09-06) |
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
| `internal_error` | 1 → **0** | `nested_loop_join` 64 MiB / 1e7 — `H3-SPILL-NLJ-1`, FIXED 2026-09-06; the cell is now one of the `clean_error` 27 → 28 |
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

## 6. The defects this matrix found — and how they were closed

Both were filed BACKLOG by H3-SPILL-1 (pins only) and **FIXED on 2026-09-06 by
H3-SPILL-RESIDUE-1**. The registry rows are `H3-SPILL-NLJ-1` and `H3-SPILL-COLLECT-1` in
[../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md).

### `H3-SPILL-NLJ-1` — a bounded pool turned a nested-loop join into a caught Rust panic

At 64 MiB / 1e7 (and reproducibly at 8 MiB / 1e6), `SELECT l.id, r.v FROM base l JOIN other r ON
l.v < r.v` failed with

    PySparkException: repark internal error in PyDataFrame.__arrow_c_stream__.next:
    partition not used yet (a Rust panic was caught at the Python boundary; this is a bug)

**The upstream cause, read from the vendored source.** `NestedLoopJoinExec::execute` loads its
build (left) side once through `build_side_data.try_once(|| self.left.execute(0, ctx))`. When the
pool refuses that load, `handle_buffering_left` calls `initiate_fallback`, which executes **the
same child instance** from partition 0 a second time to spill it
(`datafusion-physical-plan-54.1.0/src/joins/nested_loop_join.rs`). `RepartitionExec::execute`
`remove`s each output partition's channel from its shared state on the first call, so the second
finds nothing and hits `expect("partition not used yet")` at
`.../src/repartition/mod.rs:1277`. The join's build side is `SinglePartition`-distributed, so a
`RepartitionExec` is exactly what the enforcer puts there. Every other operator at the same pool
returned a clean `ResourcesExhausted`, because none of them re-executes a child.

**What repark did about it.** A dependency change is out of scope, so repark contains the
consequence rather than reporting a bug. A bounded session's `FairSpillPool` is wrapped in
`RefusalRecordingPool` ([../../crates/repark-core/src/pool_refusals.rs](../../crates/repark-core/src/pool_refusals.rs)),
which delegates every method — including `Display`, so refusal text is byte-identical — and
records the `try_grow` refusals. The Arrow export reader keeps the refusal count it opened with;
when a poll comes back as a *fenced panic* AND the pool has refused since, it reports the
engine's own refusal text plus one disclosure line instead of the internal error.

**Four** gates keep it narrow, and the fourth exists because the first three did not. Round 1
shipped three — a fenced panic, a refusal since the reader opened, a bounded session — and the
critic killed the rule by injecting `panic!("index out of bounds …")` after one refusal and
watching it come back as `Resources exhausted … fair(pool_size …)`. The fix is an allow-list of
the panics DataFusion 54.1 can actually reach on its pool-refusal and spill-fallback paths, each
read from the vendored source and cited line by line in
[../../crates/repark-python/src/map.md](../../crates/repark-python/src/map.md); a payload that is
not on it stays the bug report it is, and both directions are pinned.

**What "after a refusal" means, exactly.** `PoolRefusalLog` is **session-scoped**: the gate is a
counter delta between the reader's open and the failing poll, over the pool the *session* owns.
It is not per-stream, and it cannot be — `MemoryPool::try_grow` receives a `MemoryReservation`,
not a stream identity — so a refusal raised by another query on the same session, or by a
successful spilling query (which refuses and then spills, by design), also arms the rewrite. That
is why the allow-list carries the weight: the counter says *a refusal happened here*, and the
allow-list says *this panic is one the refusal path can produce*.

**The exception is clean; the console is not.** The panic unwinds through DataFusion before
repark converts it, so Rust's default hook prints it first: 4 `thread 'tokio-rt-worker' …
panicked at …/repartition/mod.rs:1277` blocks, one per output partition, 13 stderr lines in all
(measured 2026-09-06). A process-wide `std::panic::set_hook` would silence them, and repark
declines to install one: it is a library inside someone else's Python process, and a global hook
would outrank the host application's. The noise is disclosed here instead, and it ends when the
upstream defect does.

**Measured, 2026-09-06, release module, one fresh subprocess per cell:**

| cell | before | after |
|---|---|---|
| `nested_loop_join` 8 MiB / 1e6 | `internal_error` | `clean_error` |
| `nested_loop_join` 64 MiB / 1e7, 3 runs | `internal_error` ×3 | `clean_error` ×3 |
| the other 17 operators at 8 MiB / 1e6 | 12 `clean_error`, 1 `spilled`, 4 `ok` | identical, cell for cell |

The answer it now gives at 8 MiB / 1e6:

    PySparkException: Resources exhausted: Failed to allocate additional 1024.2 KB for
    NestedLoopJoinLoad[2] with 7.0 MB already allocated for this reservation - 1022.7 KB remain
    available for the total memory pool: fair(pool_size: 8.0 MB)
    REPARK: the bounded memory pool refused this plan; the engine did not survive that refusal,
    so repark reports the refusal itself.
    REPARK: raise the FairSpillPool via SparkSession.builder.config('repark.memory.limit.gb', N)…

**The upstream defect is still open.** The issue text to file against DataFusion is in
`task/ledgers/staging/h3-spill-residue-1-ledger.md`; a fixed upstream would make the containment
dead code, and its pins would say so.

### `H3-SPILL-COLLECT-1` — the facade boundary panicked under an address-space limit

Separately measured, because the matrix's own guard exposed it. `collect()` at 1e7 rows needs
~4.5 GiB resident and far more virtual; run it under an `RLIMIT_AS` ceiling and it failed with

    PySparkException: repark internal error in collect_rows.rows_from_record_batch:
    PyObject pointer is null (a Rust panic was caught at the Python boundary)

— a CPython allocation returned NULL and the fast path panicked instead of surfacing
`MemoryError`. **It now raises `MemoryError`** (measured 2026-09-06: `MemoryError` with no
message, CPython's own, at `RLIMIT_AS` = `VmSize` + 256 MiB over a 4e6-row five-column frame;
the 6 GiB-headroom control still returns all 4e6 rows). Every CPython allocation on the row fast
path goes through `Bound::from_owned_ptr_or_err`. pyo3's safe constructors could not be used:
`PyTuple::new`, `PyList::new` and the scalar `IntoPyObject` impls all reach `assume_owned`, which
panics on NULL **even where the signature returns `PyResult`**.

**The happy path is unchanged.** `make facade-bench CELLS=collect` on a release module, five runs
per module on a loaded box (load1 10-26 throughout):

| cell | before, 5 medians (ms) | after, 5 medians (ms) | before median | after median |
|---|---|---|---:|---:|
| `collect/1000000` | 990.4, 1007.3, 1019.7, 1040.4, 1040.4 | 1004.1, 1019.5, 1028.6, 1031.0, 1054.6 | 1019.7 | 1028.6 |
| `collect/100000` | 93.8, 94.4, 94.5, 95.9, 98.1 | 96.5, 98.3, 98.6, 98.6, 98.7 | 94.5 | 98.6 |

The distributions overlap on both cells; the `collect/100000` gap (+4.1 ms) is inside the
harness's own declared floor for that cell, which measured 3.07-6.18 ms across these runs. A
sixth run after round 2's allow-list gate landed on 1035.7 / 99.7 ms at load1 12.9, inside both
after-ranges — the gate is one `slice::contains` on a path that already caught a panic. The
in-run control `collect_old/1000000` — the pure-Python converter, which this change does not
touch — moved 4906.9 vs 4916.7 between the two modules, so the box was the same on both sides.
The pool still does not bound this path: it is un-accounted, not unreported.

## 7. What would move the Never-OOM claim

Ranked by H3-SPILL-1 from the measurement; **items 2 and 3 are done** (H3-SPILL-RESIDUE-1,
2026-09-06). The rest are still open and still not queued here.

1. **Pool-account the facade boundary.** `collect` at 1e7 is ~4.5 GiB resident at *every* pool.
   A reservation around the row-materialization loop would turn the largest unbounded allocation
   in the product into a typed refusal. **Still open** — and now the highest-ranked item: after
   H3-SPILL-COLLECT-1 the boundary reports honestly when the OS refuses it, but nothing bounds it.
2. ~~**`H3-SPILL-COLLECT-1`** — check the pointer and raise `MemoryError`.~~ **DONE 2026-09-06.**
   It was the difference between "repark reports it is out of memory" and "repark reports a bug".
3. ~~**`H3-SPILL-NLJ-1`** — upstream; a repark-side reproducer is the deliverable.~~
   **CONTAINED 2026-09-06** — the reproducer became a fix for the *shape*: repark reports the pool
   refusal that caused the panic. The DataFusion defect itself is still open upstream.
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

The two §6 re-measurements (H3-SPILL-RESIDUE-1) are the same driver, narrowed:

```
PYTHONPATH=python/repark-parity/bench .venv/bin/python -m spill.measure \
  --pools 8M --scales 1000000 --partitions 4 --repeats 1 \
  --as-cap-bytes 34359738368 --rss-cap-bytes 8589934592 --cell-timeout-s 600 \
  --scratch <dir> --json-out <file>
PYTHONPATH=python/repark-parity/bench .venv/bin/python -m spill.measure \
  --operators nested_loop_join --pools 64M --scales 10000000 --repeats 3 \
  --partitions 4 --cell-timeout-s 900 --scratch <dir> --json-out <file>
CELLS=collect PYTHON=.venv/bin/python make facade-bench
```

pins: h3-spill-1/C-002, C-003, C-004, C-005; h3-spill-residue-1/C-001, C-002, C-003

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

