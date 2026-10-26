# Production performance and memory audit — 2026-10-11

Class: campaign. Archive with the primary assessment when its finding-disposition campaign closes.

**Review outcome: complete. Performance qualification: incomplete.** RePark already has substantial
columnar, streaming and scan-pruning improvements. The highest-value next work is to bound retained
state on ordered writes, merge-on-read MERGE and full-series windows, and to qualify concurrent
workloads against a process memory allowance. Raising parallelism before those paths are measured
can increase peak memory and reduce throughput.

This is a dated review record, not a release baseline or a production approval. It freezes when the
production-audit docs branch lands; later corrections require a dated errata. Product findings stay
OPEN until their own measured implementation units close them. The companion
[production assessment](../cutover/production-audit-2026-10-11.md) owns the correctness and deployment
gate. This report supplies performance evidence and proposed acceptance experiments.

## Scope and evidence rules

Reviewed source: RePark `07be1dc930711bb927204f3df99d08526c9aaef6`, workspace version 1.5.4;
owned Iceberg fork `076d5f982d1d2ef352c3bb5d76d02cff27bc164a`; DataFusion/datafusion-spark
54.1.0 and Arrow/Parquet 58.4, verified against Cargo.toml. Fork source was read from the Cargo
checkout at that exact pin. No dependency changes, product fixes, commits, pushes, cloud operations,
real credentials, live database operations or new builds were performed by this reviewer.

The review plan was: check historical fixes against current source; follow planning, execution and
boundary lifetimes; inspect Iceberg and Postgres budgets; inspect microbatch and cluster costs;
run small isolated diagnostics; rank improvements with reproducible acceptance tests. The
engineering-method skill and contributor read path were applied. The orchestrator owns the unit
ledger and final shared verification.

| Label | Meaning | What it establishes |
|---|---|---|
| D | Current development diagnostic | Observed behavior on the supplied debug native, with environment and checks below. No release speed claim. |
| S | Current source-confirmed cost | A concrete allocation, retained state, extra pass or missing budget in the pinned tree. Its magnitude remains unmeasured unless stated. |
| H | Historical measurement | A dated, linked experiment on its recorded revision/profile. It is not a current result. |
| P | Hypothesis | A plausible bottleneck requiring the stated discriminator before implementation. |

An S finding is enough to require a resource qualification experiment. It is not enough to quote
a speedup or predict a failure at a particular dataset size. Proposed targets below are acceptance
criteria, not achieved outcomes. No Spark or DuckDB comparative benchmark was run in this review.

## Current diagnostics

The supplied module reported `repark.__version__ == '1.5.4'` and
`repark._native.__debug_assertions__ is True`. Its SHA-256 was
`a3e9ff03491ef29251c83b917540d932ad69d71acf15d3f289b20d50d5245b27`.
The orchestrator supplied it as a fresh development build. The module does not expose a build
revision in this probe, so version and checksum are recorded rather than inferring an embedded
commit. A 10-row import/Arrow/collect control succeeded before measurements.

| Environment | Value |
|---|---|
| Native | `<audit-worktree>/python/repark/src/repark/_native.abi3.so` |
| Interpreter | That worktree's `.venv/bin/python`; CPython 3.12.3 |
| Python Arrow | PyArrow 25.0.1 |
| Platform | Linux 6.8.0-146-generic, x86_64, glibc 2.39 |
| CPU | AMD Ryzen Threadripper 3970X; 32 physical cores, 64 logical CPUs; process affinity 64 |
| RAM | `/proc/meminfo` MemTotal 131,760,128 KiB; available RAM at inspection about 88 GiB |
| Threads | `TOKIO_WORKER_THREADS=4`; observed six process threads after warmup; OpenBLAS/OMP set to one |
| Engine settings | Pool `64M`; target partitions 4; configured execution batch size 8192 |
| Configuration | `REPARK_CONFIG=''`; no real catalog or database source configured |
| Fixture | `range(100000)` projected as `id`, `id * 3 AS triple`, `CAST(id AS DOUBLE) / 2 AS half` |
| Result footprint | Three eight-byte numeric columns, 2,400,000 Arrow bytes; maximum yielded batch 196,608 bytes |
| Repetitions | Three fresh child processes per action, interleaved Arrow/stream/collect; one-row warmup per child |
| Host noise | Concurrent orchestrator verification; one-minute load 4.54–4.67 during the nine cells |
| Cache state | No file input; import/session and one-row plan warmup before timing; no cold-storage claim |

| Action | Wall ms, all repetitions | Median wall ms | Median RSS before → after, MiB | Median `ru_maxrss`, MiB |
|---|---|---:|---:|---:|
| `to_arrow()` | 26.90, 32.02, 32.65 | 32.02 | 294.46 → 314.12 | 312.74 |
| Drain `to_arrow_batches()` with three Arrow sums per batch | 33.09, 33.09, 32.73 | 33.09 | 294.39 → 311.87 | 311.13 |
| `collect()` | 143.39, 143.97, 144.20 | 143.97 | 294.35 → 333.16 | 332.16 |

Every child checked 100,000 rows and sums `[4999950000, 14999850000, 2499975000.0]`.
Arrow types were `[int64, int64, double]`; collected Python values had `[int, int, float]` types.
The table and collect checks occurred after the timed action. Stream checks occurred during
consumption, so the stream wall includes that small Arrow aggregation workload. This is an
action comparison, not an isolated C-stream call-overhead measurement.

`/proc/self/status` RSS/HWM snapshots and `getrusage` high-water RSS were recorded separately.
They differ slightly in these short-lived processes; neither is an allocation attribution.
Loaded debug code, runtime initialization, allocator retention and warmup dominate the roughly
294 MiB starting RSS. Do not present that as the production wheel's idle footprint. VmSize rose
by roughly 1 GiB during each action; virtual address space is not resident RAM and is not a leak
measurement. The grown-stack and runtime-stack paths need a release-profile discriminator.

The useful current observation is narrow: row construction adds wall and resident memory on this
fixture, while batch export exposes a much smaller retained Arrow payload. The historical fast
collect path is present, so this is remaining object-materialization cost, not rediscovery of the
old five-second collect implementation.

## Ranked improvement queue

Priority considers resource risk, evidence strength and likely production reach. Impact is
qualitative; none of these entries has a newly measured before/after speedup.

| Order / ID | Evidence | Expected benefit | Owner paths | Correctness risk |
|---|---|---|---|---|
| 1 / PP-01 | S | Bound ordered-write RAM; allow session-controlled spilling; earlier first file | Iceberg adapter `write/distribution.rs`, `merge/row_lineage.rs` | High: sort guarantees, NaN/null order, lineage, staged cleanup |
| 2 / PP-02 | S | Bound matched UPDATE state and delete/cardinality state in MoR MERGE | `write/merge/mod.rs`, `position_delete.rs`, fork delete writers | High: multi-match error, atomicity, positions and v3 lineage |
| 3 / PP-03 | S | Account for full-series window input, concat and outputs; bound concurrent evaluations | Core `parallel_window/exec.rs`, `projection.rs` | High: ordering, exact numeric results, sibling cache and error precedence |
| 4 / PP-04 | S; H limits | Reliable deployment envelope across sessions, caches, buffers and operators | Core `session/spill.rs`, cache admission, runtime and source providers | High: active-query pool replacement, admission races, refusal behavior |
| 5 / PP-05 | S | Avoid full changelog materialization for small projections/limits where semantics permit | Iceberg `catalog/changelog_view.rs` | High: cross-batch net-change semantics and ordering |
| 6 / PP-06 | S | Bound active partition-writer buffers at high partition cardinality | Adapter fanout plus owned fork `fanout_writer.rs` | High: file layout, small files, ordering and publication |
| 7 / PP-07 | S; D boundary | Bound iterator pandas UDF projections; avoid incidental materialization at export/import | Python `dataframe/udf_bridge.py`, native `arrow_export.rs` | Medium/high: UDF iterator lifecycle and length errors |
| 8 / PP-08 | S; P magnitude | Remove metadata-sized work from tiny microbatches; reduce idle catalog polling | Iceberg `microbatch/window.rs`; core source and trigger driver | High: stable offsets, snapshot refresh and recovery |
| 9 / PP-09 | S; P magnitude | Qualify Postgres decode headroom, skew, downstream partitioning and cancellation | Connect COPY decoder, provider scan and partitioned stream | Medium/high: consistent snapshot and type/time placement |
| 10 / PP-10 | H; S configuration | Reduce expensive wide SQL planning; avoid reintroducing fixed facade overhead | DataFusion SqlToRel; Python plan construction and binding | High if optimizer rewrites change attribute identity or semantics |
| 11 / PP-11 | H; S | Target residual Iceberg I/O and scan-statistics costs after existing wins | Fork scan/cache/statistics; adapter catalog instrumentation | High: delete-aware counts, schema evolution, scoped cache keys |
| 12 / PP-12 | S; P magnitude | Tune process-wide CPU concurrency and cancellation; qualify optional cluster overhead | Shared runtime, physical parallel rules, distributed executor | High: starvation, blocking tasks and unsupported codecs |

## PP-01 — ordered writes have an unbounded materialization and sort path

**S, OPEN.** [`sort_batches_by_default_order`](../../crates/repark-iceberg/src/write/distribution.rs)
builds `MemorySourceConfig` from a complete `Vec<RecordBatch>`, runs `SortExec` with
`Arc::new(TaskContext::default())`, then retains every sorted batch in another vector. The
`fanout_sorted_serial`, `fanout_sorted_stream` and ordered `drive_unpartitioned` arms first drain
their streams. The [lineage writer](../../crates/repark-iceberg/src/write/merge/row_lineage.rs)
also drains before sorting when a default sort order is declared.

Pinned DataFusion 54.1.0 `datafusion-execution/src/task.rs:71` constructs a fresh default
RuntimeEnv for `TaskContext::default()`. `runtime_env.rs:472` gives an unspecified pool an
UnboundedMemoryPool. The ordered-write helper therefore does not use the query session's
FairSpillPool or configured spill directory. Input retention also occurs before the sort can
reserve anything. Passing the session context is necessary but insufficient: the initial full
input vector must also become budgeted or spillable.

Concurrent partitioned workers each drain their own routed input. A bounded input channel limits
queued batches; it does not bound what the receiving worker appends to `collected`. The ordered
unpartitioned arm subsequently awaits writer chunks in a loop, so `max_concurrent_files` does
not make those chunks write concurrently. Whether parallelizing them is useful depends on
storage and compression; fix the budget first.

**Acceptance experiment:** local disposable catalog, 100k and 1M rows × 8 columns; ordered and
unordered tables, unpartitioned and 32 identity partitions, writers 1/4, pool 32/128 MiB. Use
Spark SQL overwrite and the reachable native/MERGE write doors. Report input bytes, first-file
latency, wall, process peak RSS, pool peak, spill bytes, output files and per-file order. Under the
small pool, ordered data exceeding it must spill or refuse by name while preserving the old
snapshot. Values/types, per-file sort, signed zero/NaN/null ordering, sort-order IDs, lineage
and unknown-commit cleanup remain required. A faster result with unsorted stamped files fails.

Measured 2026-10-26 (ICE-RESOURCE-BOUND-1 step 0): on the ci profile with debug assertions on, under a 40 GiB scope, statement RSS growth reaches 4.68 times the ordered input bytes at the worst cell (4M rows, v2, 32 partitions, one writer) and 3.22 to 4.08 times at 16M rows; the pool size does not change it. Table: [step-0 bound table](../../task/wo/ice-resource-bound-1/evidence/step-0-bound-table-2026-10-26.md). Status unchanged; the owner rules the bound.

## PP-02 — merge-on-read MERGE retains all matched work

**S, OPEN.** The current [MoR path](../../crates/repark-iceberg/src/write/merge/mod.rs) uses one
matched join, which is a real improvement. `matched_work_mor` still accumulates `update_batches`,
`seen_pair`, `pair_indices`, path interning and unique paths until the matched stream ends. Its
caller starts writing the retained updates afterwards. NOT MATCHED BY SOURCE can add another
retained set. The final delete pair vector and position-delete preparation add further state.

There is no MemoryReservation around these collections. Peak memory depends on matched rows and
updated row width, not just DataFusion join state or writer channel capacity. Delete-only work
still retains cardinality/delete-position state. The current code interns paths once per distinct
file; filing the historical per-row owned-path-string defect again would be incorrect.

**Acceptance experiment:** 200k and 1M rows × 8 columns, 32 data files; MoR update/delete match
fractions 1/10/100%, and a NOT MATCHED BY SOURCE leg. Compare to COW only as a workload control,
not as an instruction to change modes. Pool 32/128 MiB, writers 1/4. Record peak RSS, join peak,
matched cardinality, retained update bytes, deletion state and first staged-file time. Require
budgeted retained state or a typed refusal before process exhaustion. A streaming redesign may
stage updates before cardinality validation completes, but it must publish nothing on multi-match
failure and must preserve CommitStateUnknown semantics. Verify final values, types, delete counts,
row IDs and snapshot contents after failure. Do not remove multi-match validation to save memory.

Measured 2026-10-26 (ICE-RESOURCE-BOUND-1 step 0): on the ci profile with debug assertions on, under a 40 GiB scope, every merge-on-read MERGE measured at the 32 and 128 MiB pools refuses in the in-pool sort with the snapshot unchanged. The two rows that set the proposed bound are 4M rows, v3, 10 percent matched plus NOT MATCHED BY SOURCE at the 2 GiB pool (growth minus pool 8.90 times the target bytes), and 16M rows, v3, 100 percent matched at the 8 GiB pool (6.14 times). Table: [step-0 bound table](../../task/wo/ice-resource-bound-1/evidence/step-0-bound-table-2026-10-26.md). Status unchanged; the owner rules the bound.

## PP-03 — parallel full-series windows trade CPU time for retained RAM

**S, OPEN qualification.** [`ParallelWindowExec`](../../crates/repark-core/src/parallel_window/exec.rs)
collects input batches, concatenates the whole input, drops the original list, computes expression
groups on blocking tasks and assembles one output batch. The partition-index input arm collects
each partition and joins them in deterministic index order. The input/concat overlap and result
arrays have no operator MemoryReservation. A semaphore limits concurrent groups for that one
execution; it does not cap memory across several executions.

This is not a claim that all windows collect. The current plan control below selects
BoundedWindowAggExec for a running sum. The new TA grouping and parallel projection rules are
present and retain important guards: structurally related siblings stay together, volatile
expressions avoid swaps, and the useless RoundRobin removal is restricted to proven one-batch
inputs. Keep those fixes. [`ParallelProjectionExec`](../../crates/repark-core/src/parallel_window/projection.rs)
works per batch; its temporary expression arrays still grow with batch width and parallel tasks.

**Acceptance experiment:** existing deterministic TA series fixture at 100k/1M rows, 2/14
indicators and a chained series; target partitions 1/4/8, one and four simultaneous queries,
pool 32/128 MiB. Compare the current Rust flag-on and flag-off sessions, using a scalar aggregate
consumer and Arrow export separately. Record input, concat, indicator-output and cache bytes,
pool peak, RSS and wall. Require the chosen explicit memory contract: budget/refusal or bounded
spill/state where possible. Require row-order and numeric parity, first-error precedence and
sibling evaluation counts. Recursive indicators cannot be split arbitrarily into chunks; overlap
is insufficient for every indicator. That is a correctness design, not a mechanical optimization.

## PP-04 — the operator pool is only one part of the process budget

**S, OPEN; PA-10 owns deployment detection.** [Default detection](../../crates/repark-core/src/session/spill.rs)
checks a root cgroup-v2 file, then host MemTotal, then an 8 GiB fallback; it derives 60% of detected
RAM capped at 8 GiB. It does not resolve nested membership/ancestor limits or non-Linux RAM.
Each session creates its own pool. Separate sessions can therefore admit several independent
budgets even though Python sessions share one Tokio runtime.

The pool does not directly charge all decoded arrays, final Python Rows/pandas objects, Arrow
tables retained by callers, caches, writer encoders, metadata task lists or full-result providers.
Reserve operational headroom for them. A safe limit is workload-dependent; subtracting a single
fixed percentage does not prove safety.

The [September spill coverage matrix](spill-coverage-matrix-2026-09-11.md) is an important H
limit: 24 of 27 cells were stable, while hash_join at 4× was KILLED and two 2× cells were
UNSTABLE under its recorded process cap. The older
[180-cell report](spill-matrix-baseline.md) also documented boundary exclusions and a toPandas
allocation abort under a constrained address-space test. The contained collect/nested-loop
refusal fixes are present. These are dated experiments, not current reruns; the same DataFusion
version and source-confirmed exclusions mean a universal Never-OOM claim still needs new evidence.

Current positive controls matter: [cache materialization](../../crates/repark-core/src/session/temp_views.rs)
uses execute_stream, enforces optional per-cache and total limits, deduplicates buffers for retained
accounting, and bounds the extra ordered-cache concat. [Cache handles](../../python/repark/src/repark/spark/dataframe/cache_handle.py)
release abandoned eager views. Both cache limits default to unset/zero, so deliberately retained
frames still need an explicit policy. `max_bytes` stops retaining after breach but drains the
remaining stream to calculate the final size; it can keep expensive upstream work running after
failure is already inevitable. `max_total_bytes` refuses during admission.

Two concurrent cache materializations can each read the same retained snapshot before either
registers its result. The source shows no atomic admission reservation around that check. This is
an S concurrency candidate, not an executed overshoot: use a barrier-controlled test before filing
a measured breach. Unpersisting a view also does not free buffers still owned by an exported
Arrow table or another plan. Post-gc RSS staying high alone is not evidence of a leak.

**Acceptance experiment:** one versus four sessions and one versus four concurrent queries,
pool 64 MiB per session, caches retained and discarded, exported tables retained and discarded.
Record pool totals, distinct retained buffer bytes, RSS and active sessions. Provoke cache limits
with deterministic batches, including the concurrent admission barrier. Require that total-limit
admission is atomic or documented as non-atomic with an enforceable concurrency restriction.
Benchmark fail-fast `max_bytes` against today's full drain using an expensive counted source,
without weakening errors or allowing incomplete caches. Deployment detection fixtures and a
disposable constrained integration process belong to PA-10. Do not replace a live pool mid-query
and assume existing reservations migrate; test runtime SET with active queries explicitly.

## PP-05 — changelog limits and projections do not bound the source work

**S, OPEN; PA-11 owns the defect.** [`ChangelogViewProvider`](../../crates/repark-iceberg/src/catalog/changelog_view.rs)
requests every source column with no source filter/limit. Execution collects, concatenates,
transforms and only then projects. A final LIMIT can still read and retain the entire history.
The original batches remain in scope during the concat/transform operation. No reservation covers
the retained result.

**Acceptance experiment:** 100k/1M changelog rows over 10/100 snapshots, narrow and wide schemas;
full result, one-column projection and LIMIT 10 under 32/128 MiB pools. Count source rows/batches,
bytes, first-output latency and RSS. Cross-batch update pairs and net-change cancellation must
remain correct. A safe improvement can project required columns earlier, use state keyed by the
transform's actual needs, spill that state, or refuse at an explicit cap. Blindly pushing LIMIT or
filter predicates through a net-change transform is not valid.

## PP-06 — worker count does not cap partition fanout buffers

**S, OPEN qualification.** The adapter's [fanout dispatch](../../crates/repark-iceberg/src/write/append.rs)
uses small channels and partition-aware routing. In the pinned fork,
[`FanoutWriter`](https://github.com/TRO-Wolf/iceberg-rust/blob/076d5f982d1d2ef352c3bb5d76d02cff27bc164a/crates/iceberg/src/writer/partitioning/fanout_writer.rs)
keeps a HashMap of active partition writers until close. The session's default four concurrent
workers are not a maximum of four active partition encoders. Thousands of interleaved partition
values can retain thousands of encoder/row-group states, even when queued batches are bounded.
The [v3 lineage fanout](../../crates/repark-iceberg/src/write/merge/row_lineage.rs) has the same
partition-cardinality dimension. The exact per-writer memory cost has not been measured here.

**Acceptance experiment:** hold total rows and bytes constant; vary identity partitions
8/128/4096, input order clustered versus interleaved, writers 1/4, codec and row-group size fixed.
Use 200k rows initially. Measure active writer count, RSS, encoded-buffer estimates, wall, file
count and bytes. If memory grows with cardinality, add a reviewed active-writer budget or clustered
write strategy. Closing writers early can create many small files and increase later read and
maintenance costs; acceptance must include that downstream cost. All changes to the table-format
writer belong in the owned fork. Do not undo the fixed partition-distribution layout.

## PP-07 — keep bulk work columnar and remove avoidable UDF retention

**D + S, OPEN qualification.** Current collect drains batches and uses native primitive-cell
conversion, then builds Row objects. `toLocalIterator` avoids retaining the whole result, but
constructs a complete list of Rows for each batch before yielding. `to_arrow()` retains all result
buffers; `to_arrow_batches()` lets the consumer release them incrementally. These API contracts
have different costs. The diagnostic table establishes the distinction on one small numeric bed.

[`arrow_export.rs`](../../crates/repark-python/src/arrow_export.rs) polls one DataFusion batch
per C-stream callback and releases the GIL while waiting. Same-type columns clone references;
Utf8View/BinaryView export converts to compatible Arrow types and can allocate. The
`drain_arrow_c_stream` import helper drains an exporter into a MemTable; accepting a streaming
object there does not make registration streaming or out of core.

The [pandas UDF bridge](../../python/repark/src/repark/spark/dataframe/udf_bridge.py) has a concrete
extra materialization: the presence of any SCALAR_ITER slot converts the full input iterator to
`batch_list`, computes and retains all UDF outputs by slot, then emits results. Pure scalar UDF
projection stays batch-streamed. MapInArrow/ordinary row-UDF paths must be evaluated separately;
the presence of a Python UDF does not imply every path buffers the dataset.

**Acceptance experiment:** release wheel, 100k/1M numeric, string, decimal, nested map and timestamp
fixtures; collect, Arrow table, drain Arrow batches and drain Row iterator. Repeat with one and
two scalar-iterator UDF slots, a scalar control, early-stop and mid-stream-error controls. Record
engine time, conversion time, GIL contention, RSS, output bytes and producer batches consumed before
first output. Require value/type/nullability checks including timezone and nested maps. For the
iterator redesign, preserve one invocation per iterator UDF, mismatched-length errors and early
release. Prefer columnar application terminals when callers do not need Rows; do not change public
collect semantics to obtain a benchmark win. Set explicit import/cache limits for MemTable input.

## PP-08 — microbatch execution is bounded by batch, planning is not by admitted files

**S; P impact, OPEN qualification.** The [driver](../../crates/repark-core/src/microbatch/run.rs)
runs one batch per query at a time and keeps a bounded recent-progress log. These are positive
controls. Source planning reloads table state, walks snapshot ancestry, plans an append snapshot's
tasks into a vector and sorts all tasks by path. Only then does the window enforce admitted file/
row caps. `added_file_count` also calls `added_tasks` when a count alone is needed. A one-file
microbatch from a very large append snapshot can therefore perform metadata work for the entire
snapshot, and repeated windows may repeat it.

The [caps](../../crates/repark-iceberg/src/microbatch/window.rs) are file/row caps, not byte
budgets; a whole file can cross the row threshold. `SourceOptions` does not expose a byte cap.
The default zero processing interval uses a 10 ms polling delay only when no data is found;
source reloads still occur per poll. Real remote round-trip time may dominate that delay, so
"100 requests/second" would be an unsupported exact claim. Catalog request rates and idle cost
need measurement. The ancestry-cycle candidate remains PC-02 in the primary audit; it is not
promoted to a proved failure here.

**Acceptance experiment:** offline planner fixtures with 10/100/1000 snapshots and 100/10k tasks
in one append snapshot, caps 1/10 files; report plan_files calls, files enumerated, planner RSS,
first-window time and repeated-window time. No 10k physical data files are needed for the metadata
discriminator. An idle mock/counting catalog probe with zero/100 ms/1 s trigger intervals should
measure reload calls over ten seconds. Require deterministic offset advancement, refreshed
snapshot identity, expired-source errors and restart/recovery behavior. Reuse snapshot-scoped
task planning or count facts only with a valid identity; do not make stale metadata invisible to
the source-replacement check. Qualify the full sink later in owner-authorized cloud tests.

## PP-09 — Postgres streaming has good backpressure but still needs decode headroom

**S, OPEN qualification; no live database throughput measured.** The
[provider](../../crates/repark-connect/src/provider/map.md) pushes projection, eligible filters
and LIMIT, opens COPY on first poll, meters bytes/decode/first byte, charges yielded batches and
cancels dropped leases. Parallel reads import a common repeatable-read snapshot before COPY.
Strides run through at most `min(strides, pool_max_size)` connections, default pool size four.
The parallel stream has a bounded channel and transfers a reservation with each queued batch.
These controls rule out an unbounded row-result queue in the reviewed provider.

Reservations are acquired after decode and timestamp placement. The COPY decoder has its own
carry and appender buffers; the default batch byte threshold is 64 MiB and individual field
admission permits up to 1 GiB. Those thresholds are not a pre-allocation memory reservation.
Several lanes can be decoding while earlier batches occupy the pool. The correct resource
envelope must include decoder/carry/localisation transients as well as reserved output batches.

The scan advertises one DataFusion output partition even when its connection lanes run in
parallel. This may require downstream redistribution, but does not establish that all downstream
execution is serial. Equal numeric stride ranges can be badly skewed; bounds describe partition
cuts, not a row restriction. Unsupported predicates correctly remain residual and may require
more network bytes. Source resolution discovers schema afresh per resolution; caching that work
without an invalidation rule would trade performance for stale types.

**Acceptance experiment:** disposable local Postgres, 200k rows with uniform and 90%-skew integer
keys, numeric/wide-text/timestamp columns. Sweep connections 1/4, batches 1024/8192/32768,
projection width, pushed versus residual filters and LIMIT 10. Record time to first byte/batch,
COPY bytes, decode/placement time, lane skew, downstream plan, pool peak and RSS. Feed fragmented
COPY decoder fixtures with 1/16/64 MiB fields separately under a small test budget; do not provoke
a host OOM. Require correct cancellation, no held transaction after early LIMIT, consistent
snapshot, value/type/timezone checks and a named oversized-input refusal where necessary. Add
pre-reservation or explicit decode headroom before claiming the connector is pool-bounded.

## PP-10 — separate SQL planning cost from execution

**H + S, OPEN remeasurement.** The
[CAST report](cast-cost-2026-09-12.md) already attributed a superlinear wide aggregate-CAST
planning phase to stock DataFusion SqlToRel. Its release measurements were 0.013/0.088/4.452 s
for 50/250/2500 casts over aggregate, with a planning exponent 1.511. The report owns those
numbers and the upstream issue. The current dependency remains 54.1.0; this audit did not rerun
that expensive grid or prove every source change leaves its absolute wall identical.

The old cubic facade-chain measurement is not a current finding. Projection construction,
frame identity, name-rule caching and executable caching have changed. In particular the current
binding caches NameRule per frame, and [its map](../../crates/repark-python/src/dataframe/map.md)
records the October fix that removed repeated TaskContext registry cloning per bound column.
Keep attribute identity, duplicate names, volatility, qualifier and self-join checks when comparing
`withColumn` chains to a bulk `withColumns`/select alternative. A plan is not a memoized query
result: repeated terminal actions still execute unless the caller caches deliberately.

**Acceptance experiment:** release wheel; 50/250 wide CAST SQL columns initially, depth 10/100
withColumn chains versus equivalent bulk projection. Measure build, analyze, optimize, physical
plan and drain independently; capture plan node/expression counts. Use 2500 only after the small
grid establishes enough disk/time headroom. Validate values/types and duplicate-name/self-join
controls. Optimize the phase a profiler attributes; do not lower optimizer passes merely because
planning becomes faster. A DataFusion upgrade needs both the existing grid and semantic gates.

### Current plan controls

A separate small EXPLAIN-only probe used `range(100000)`, a temporary view, target partitions 4,
pool 64M and otherwise default batch settings. Its range generator displayed batch size 65536;
it is not the timing probe's configured 8192-row fixture.

| Query shape | Current observed physical structure | Implication |
|---|---|---|
| Group by `id % 100`, AVG(id) | Partial Aggregate → hash repartition(4) → FinalPartitioned Aggregate | Current avg uses a parallel grouped plan; do not file the pre-GroupsAccumulator path again. |
| ORDER BY id DESC LIMIT 20 | SortExec TopK(fetch=20) | This limit avoids a full-output sort; retaining all Rows is not inherent to this shape. |
| Running SUM with ordered ROWS frame | BoundedWindowAggExec, Sorted | This window streams according to its bounded operator; PP-03 concerns other full-series shapes. |
| Self equijoin, id < 10 | Filter on both inputs; HashJoin Partitioned with four-way hash repartitions | Predicate propagation works on this control; extra shuffles may dominate tiny results, but no wall was measured. |

EXPLAIN observes plan shape, not actual spill or execution time. These controls do not prove that
every Iceberg or Postgres plan has equally useful statistics or repartitioning.

## PP-11 — evaluate Iceberg residue after preserving delivered I/O wins

**H + S, OPEN requalification.** Current fork scan source exposes multiple output partitions,
projection/predicate plumbing and exact row totals where valid. RePark wires session metadata,
manifest and footer caches into memory, Glue and S3 Tables catalogs. The September reports are
not evidence that current count(*) decodes all columns or that every scan has one partition.

The [September read report](ice-read-perf-baseline-2026-09-19.md) includes a 200-file, 10M-row
local bed and an owner-authorized AWS pair. In the AWS pair, warm selective queries reduced
requests/bytes dramatically; the full non-prunable projection still read about 1.43 GB. Its AWS
timings have one sample per setting, so request/byte counts are stronger evidence than precise
speed ratios. The local full-projection residue moved differently from selective scans. That
supports a workload-specific investigation, not a generic claim that all Iceberg scans are slow.

The current fork scan's `partition_statistics` starts with unknown column statistics and adds an
exact total row count only when valid. Useful column bounds/null/distinct/selectivity facts are
not generally supplied there. Whether improved statistics change join choice or avoid a count(col)
scan is P, not established by the old count(*) win. Delete-aware and evolved-schema facts must
stay conservative. Plan-file vectors and delete-bearing scan state add another memory dimension
when the file count is large even if returned rows are few.

[`CatalogCaches`](../../crates/repark-iceberg/src/catalog/caches.rs) defaults to 32 MiB manifest
and 64 MiB footer cache settings plus metadata caching. At this pin, the fork's metadata cache
is byte-weighted: RePark's 512-entry parameter translates to `512 * 64 KiB`, or 32 MiB, of
serialized-metadata weight. It is not literally an unbounded cache nor an exact 512-object cap.
Its weight uses JSON body size, not a measured Rust object graph. The mapping's RAM multiplier is
unmeasured. This is distinct from the already-fixed manifest-object weight defect recorded under
RP-16. Cache budgets are outside FairSpillPool; cloned snapshots/plans may retain data after cache
eviction. Treat summed cache settings as configured weights, not an RSS guarantee.

**Acceptance experiment:** reuse the committed local `ice_read_perf` runner at a smaller initial
bed (1M rows, 20 and 200 files) before its 10M release gate. Capture cold-new-session and warm
same-session modes without mislabeling OS page cache as cold. Record planning, first batch, wall,
data/footer/manifest requests and bytes, cache hit/miss/charged weight, pool peak and RSS. Compare
identical Parquet projection/compression/file layout as a floor. Include no-delete, v2 position
delete and v3 DV beds, count(*), count(nullable), selective filter and full projection. Require
actual values/types and schema-evolution checks. For cache weight, generate snapshot-rich metadata
with equal serialized size and different object structure, settle cache maintenance and compare
charged weight to isolated process RAM growth. Do not disable caches globally to fix a memory
test without measuring the resulting cloud round trips. Cloud remeasurement remains owner-gated.

## PP-12 — tune concurrency as one deployment budget

**S; P impact, OPEN qualification.** Python sessions share a process-wide Tokio runtime. The
[runtime builder](../../crates/repark-python/src/deep_stack.rs) sets 32 MiB thread stacks but no
explicit worker count; Tokio uses its startup setting/default. Target partitions, writer workers,
scan concurrency, Postgres connections and parallel-expression semaphores are separate controls.
The measured four-worker runtime does not imply the default runtime has four workers. Large
stacks reserve virtual space; do not multiply stack size by threads and call that resident RAM.

Parallel window/projection semaphores are per execution, so four queries can each spawn work up
to their own allowance. A task queued to the blocking pool can be cancelled; an already-running
blocking closure is not forcibly stopped by aborting its handle. Driver ownership is a good
lifecycle property, but prompt physical CPU reclamation still requires a cancellation experiment.
Large or many sessions may contend for the same runtime while retaining independent pools.

The optional [cluster path](../../crates/repark-distributed/src/map.md) starts an in-process
scheduler and executors and adds plan encoding, shuffle/IPC, metrics and multiple execution
contexts. It is off by default and was not benchmarked here. The session provider correctly
refuses the parallel-single-partition configuration because its codecs do not carry those nodes.
Writes stay coordinator-side. Distribution will not repair a coordinator's materialized ordered
write or MoR update buffers. Neither in-process executors nor same-host timings establish
multi-host scalability. The transport qualification is PC-01 in the primary report.

**Acceptance experiment:** release module, target partitions 1/4/8, runtime workers 4/8 fixed
before initialization, one/four simultaneous independent aggregate/scan/TA queries. Report
throughput, per-query p50/p95, first output, CPU, threads, RSS and spill. Cancel a long TA query
after work begins and time CPU return to baseline. Add a process-wide work admission limit only
if the experiment establishes oversubscription or memory amplification. Keep the current volatility,
ordering, error-order and codec guards. Optional cluster qualification should compare the same
query on local executor and cluster 1/2 executors with stage rows/shuffle bytes and result digests;
start with a 100k in-memory fixture. A throughput benefit must exceed startup/serialization/noise
cost and preserve frozen Iceberg snapshots. No cluster or broad concurrency measurement was run.

## Historical findings checked against current code

| Historical concern | Current disposition in this audit | Evidence home |
|---|---|---|
| Five-second primitive collect; repeated field-name/column conversion | Native batch converter and batched collect are present; remaining Row cost is PP-07 | [facade baseline](facade-boundary-baseline.md), native collect_rows.rs |
| Orphan eager registrations | Refcounted CacheHandle is present; intentional retention is PP-04 | [EAGER-OWN](eager-own-1-2026-09-13/map.md), [EAGER-BUDGET](eager-budget-1-2026-09-13/map.md) |
| Cache collection before size admission | Streamed admission and ordered-concat guard are present | core temp_views.rs and cache_budget.rs |
| Ungrouped per-group avg overhead | GroupsAccumulator fix is recorded; current plan has partial/final aggregation | [aggregate baseline](aggregate-baseline.md), current EXPLAIN control |
| Exact sorting for approximate percentile | GK sketch fix is recorded; no new sort-all claim | [percentile baseline](approx-percentile-baseline.md) |
| CSV inference repeated per-column actions | Batched inference fix is recorded; no new inference regression measured | [CSV baseline](csv-infer-baseline.md) |
| Preview counting/materializing full plain input | Bounded preview fixes are recorded | [preview baseline](eager-preview-baseline.md) |
| Single-partition ordinary Iceberg scan and full-column count(*) | Pinned scan supports partitioned work and count statistics; delete-bearing exceptions need measurement | [scan baseline](iceberg-scan-baseline.md), [read remeasure](ice-read-perf-baseline-2026-09-19.md) |
| Manifest cache wrong retained-object weighting | Historical RP-16 object-graph charge fix is recorded; metadata JSON weight is a separate candidate | [catalog baseline](iceberg-catalog-io-baseline.md), pinned cache source |
| Writer count multiplied files per partition | Current partition-aware routing/hash distribution is present | [write baseline](iceberg-write-baseline.md), adapter distribution and partition-write paths |
| TA multi-output siblings recomputed independently | Current parallel groups retain shared sibling evaluation | core parallel_window map and rule |
| Repeated per-column name-rule TaskContext clones | Current PyDataFrame OnceLock/derived inheritance fix is present | native dataframe map, October STAMP-2 follow-up |

This table prevents a stale September before-column becoming an October bug claim. It does not
declare all descendants of a fixed path equally fast or memory-bounded.

## Measurement and implementation sequence

1. Close correctness/deployment blockers under the primary assessment. Freeze the intended
   production workload, wheel identity, catalog, file layout and semantics before timing it.
2. Run inexpensive local memory discriminators PP-01/02/03/05/06. Use explicit small pools and
   disposable process caps with sufficient interpreter/runtime headroom, not host exhaustion.
3. Qualify PP-04 multi-session and cache admission. Establish a process RAM policy that includes
   caches, decode, final results and encoder buffers; keep operator refusal distinct from process
   OOM and kernel kill.
4. Run release-only CPU and I/O comparisons PP-07/08/09/10/11/12, alternating before/after on one
   environment with at least five repetitions and the same input/cache conditions. Profile only
   the phase that dominates; start with 100k/200k or metadata-only fixtures.
5. Broaden to the existing 1M/10M beds and owner-approved remote workloads only after the small
   discriminator proves the change's intended effect. Require complete result/type checks and
   failure/recovery controls alongside speed/RSS. Run the existing spill matrix when changing
   DataFusion or pool semantics.

For every experiment record wheel/native checksum and debug flag, RePark/fork/DataFusion pins,
platform/CPU affinity/cgroup, runtime workers, target partitions, batch size, all memory/cache/
writer/scan knobs, input rows/columns/types/bytes/files/row groups/delete state, compression,
cache conditions, full samples/load and process outcome. Separate plan, first output, execution,
conversion, publication and peak RAM. Acceptance should name the resource behavior, then use
relative wall targets only after measuring the fixture's noise floor. Do not fabricate a universal
speed multiplier or a single recommended partition count from historical mixed workloads.

## Commands, artifacts and limits

Executed commands included git status/revision and Cargo pin inspection; contributor/skill/map/
performance-report reads; targeted `rg` and `sed` reads of the owner paths above and pinned fork/
DataFusion source; `df -h /tmp`, `lscpu`, `/proc/meminfo`, `/proc/self/cgroup`, loadavg inspection;
native SHA-256; the import/control process; nine isolated diagnostics; and four EXPLAIN controls.
No release benchmark or product test-suite result is claimed for this reviewer.

Local document checks passed: `make spell-check` (pinned typos 1.47.2), whitespace checks and a
relative-link existence check of this report (zero missing targets). Final tracked-document
anchor/ledger gates remain with the orchestrator after transfer.

Diagnostics were run through this environment and command shape from the audit worktree root:

```bash
audit_worktree="$PWD"
REPARK_CONFIG='' TOKIO_WORKER_THREADS=4 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
PYTHONPATH="$audit_worktree/python/repark/src" \
"$audit_worktree/.venv/bin/python" \
/tmp/repark-production-audit-2026-10-11/performance-probe.py arrow 100000
```

The controller repeated the same command in fresh children with `arrow`, `stream`, `collect`,
three times each, using `subprocess.run(check=True, capture_output=True)` and recording JSON.
The exact script is also recorded in the companion evidence appendix. Nine full environment/sample objects and plan text are retained under the
task-owned scratch root as `performance-probe.py`, `performance-probe-results.json` and
`performance-plans.txt`. The report contains the fixture, all wall samples, memory medians and
value/type checks so its conclusions do not depend on ephemeral file availability.

Disk inspection showed 702 GiB available before probes and 700 GiB at final review while the
orchestrator's shared verification continued. The probes created no data fixtures,
new worktrees, wheels, target trees, database containers or cloud artifacts. Only small task-owned
scripts/results and this scratch clone/report were retained for orchestrator review. No shared
cache or unrelated worktree was cleaned. Full `make verify` and final document gates are the
orchestrator's responsibility on the attached branch; a duplicate heavy gate was intentionally
not run while the shared gate was active. Their authoritative result is the
[production evidence appendix](../cutover/production-audit-evidence-2026-10-11.md).

Unmeasured limits: current release-wheel CPU/RSS, real AWS read/write latency and spend,
live Postgres throughput, constrained-cgroup product behavior, large ordered writes/MoR updates/
TA windows, prolonged multi-session soak, cluster execution and cancellation latency. No claimed
percentage improvement or broad production-performance qualification extends beyond the evidence
labels and fixtures recorded here.
