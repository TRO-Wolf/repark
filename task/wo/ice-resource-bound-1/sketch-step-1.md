# ICE-RESOURCE-BOUND-1 (U-11) step 1 — the sketch (2026-10-26)

Written by Claude (claude-opus-5-5), sketcher, on head `e168f29c` (branch `feat/ice-resource-bound-1`).
Read only: nothing was built or run. Every `file:line` below is on that head. The owner's rulings Q1 to
Q5 of 2026-10-26 ([order](order.md), Step 0 outcome) bind this sketch; the bound numbers are the
[step-0 table](evidence/step-0-bound-table-2026-10-26.md)'s proposal lines, as ruled.

**Verdict: one seam per path; no halt.** The PP-01 seam is one new drain function that the four
existing drain loops call (§1.1). If the orchestrator reads those four call sites as four seams,
this sketch is a HALT under order §6 instead; question O-1 asks.

## 0. The three paths at a glance

| path | what is held outside the pool | where the size is known | refuse | the one seam |
|---|---|---|---|---|
| PP-01 ordered write | the whole conformed input, drained before the sort | only while draining (a plan stream) | at the crossing, before any file is staged | a new `drain_ordered_input` that replaces the four drain loops |
| PP-02 MoR MERGE | `update_batches` of the matched leg plus the NOT MATCHED BY SOURCE leg | only while the matched stream runs | at the crossing, before any file is staged | the loop of `matched_work_mor`, `merge_on_read.rs:134-147`, with one budget across both legs |
| PA-11 changelog view | the collected source window, then its concatenated and transformed copies | only while collecting | at the crossing; nothing is staged (a read) | `ChangelogViewPartition::execute`, `changelog_view.rs:104-120` |

None of the three paths knows its input size before execution: each one consumes a DataFusion
plan stream, so each one refuses at the crossing. On PP-01 and PP-02 the crossing comes before the
first data or delete file is written, so a refusal stages nothing (§3.4).

## 1. The charge point, per path

### 1.1 PP-01 — ordered write

- The sort is `sort_batches_by_default_order`, `crates/repark-iceberg/src/write/distribution.rs:209-228`.
  It receives a complete `Vec<RecordBatch>` and sorts it under `TaskContext::default()` (line 222),
  which carries an unbounded pool. That is why PP-01 is not charged to the session pool. Spilling it
  stays out of scope (order §0).
- The input is held by four drain loops, each before the sort:
  - `fanout_sorted_serial`, `distribution.rs:428-431` (the serial partitioned arm, called from `append.rs:84`);
  - `fanout_sorted_stream`, `distribution.rs:456-459` (each worker of the parallel partitioned arm, called from `append.rs:97`);
  - `drive_unpartitioned`, `distribution.rs:486-490` (the ordered unpartitioned arm; callers `write/write_options.rs:224`, `merge/file_sink.rs:75`, `merge/session_staging.rs:59`);
  - `drain`, `crates/repark-iceberg/src/write/merge/row_lineage.rs:222-233`, called at line 195 by `write_partitioned_lineage_files_with` (the v3 partitioned MERGE, UPDATE and copy-on-write writer; caller `merge/session_staging.rs:67`).
- **Charge point:** each batch as the drain receives it. The size is not known before: the input is a
  plan stream on every reached door (D2, D3, D4, D5, D10 and D8 copy-on-write, per the census row).
- **Seam:** a new `drain_ordered_input(stream, budget) -> Result<Vec<RecordBatch>>` in the new module
  `crates/repark-iceberg/src/write/resource_bound.rs`. It is the only place that adds a batch's bytes to
  the budget and the only place the PP-01 refusal is raised. The four loops above become calls to it;
  `sort_batches_by_default_order` is unchanged.
- **The parallel arm.** Workers of `fanout_sorted_stream` each drain their routed share
  (`append.rs:89-100`), so a bound checked per worker would admit `writers` × the bound. The budget
  therefore carries one shared counter (`Arc<AtomicUsize>`) per staging call, created in
  `fanout_conformed_stream_with_concurrency` (`append.rs:73-148`) and handed to every worker. Question
  O-6 asks how a refusal in one worker stops its siblings before they write.

### 1.2 PP-02 — merge-on-read MERGE

- `plan_and_commit_mor`, `crates/repark-iceberg/src/write/merge/merge_on_read.rs:23-114`, calls
  `matched_work_mor` for the matched leg (lines 41-58) and again for the NOT MATCHED BY SOURCE leg
  (lines 59-74). Each call accumulates `update_batches` (pushed at line 262 by
  `consume_matched_work_batch`, lines 156-265) until its stream ends. Staging starts only afterwards,
  at lines 84-91.
- **Charge point:** each batch `consume_matched_work_batch` pushes, read in the `matched_work_mor` loop
  (lines 134-147) as the growth of `update_batches` after each call.
- **Seam:** that loop. The ruled bound covers matched plus NOT MATCHED BY SOURCE together, so one
  `UpdatedRowsBudget` is created once in `plan_and_commit_mor` before line 41 and passed to both calls.
  `consume_matched_work_batch` keeps its signature, so its two unit tests (`merge/tests/merge.rs:351`, `:407`) stay unchanged.
- The MoR delete state (`seen_pair`, `pair_indices`, `path_intern`) is not counted: the ruled bound is
  updated-row bytes. This is recorded in out of scope, observed.

### 1.3 PA-11 — changelog view

- `ChangelogViewProvider::scan`, `crates/repark-iceberg/src/catalog/changelog_view.rs:51-79`, asks its
  source for every column with no filter and no limit (line 58). `ChangelogViewPartition::execute`
  (lines 104-120) then calls `collect(source_plan, ctx)` (line 111), concatenates (line 112) and
  transforms (line 113) before projecting.
- **Charge point:** each source batch as it is collected. Line 111 becomes a drain over
  `execute_stream(source_plan, ctx)` that charges every batch.
- **Seam:** `ChangelogViewPartition::execute`. The `TaskContext` it receives (line 104) carries both
  the live pool (`ctx.memory_pool().memory_limit()`) and the session config extensions, so PA-11 needs
  no carrier.

## 2. One seam per path — the check

| path | seam (file:line on this head) | call sites that change | second seam? |
|---|---|---|---|
| PP-01 | new `resource_bound::drain_ordered_input`; replaces `distribution.rs:428-431`, `:456-459`, `:486-490`, `row_lineage.rs:222-233` | the four loops | no: one check, one raise (question O-1) |
| PP-02 | `merge_on_read.rs:134-147` (the `matched_work_mor` loop) | `merge_on_read.rs:44`, `:60` pass the shared budget | no |
| PA-11 | `changelog_view.rs:110-118` (`ChangelogViewPartition::execute`) | line 111 | no |

A merge-on-read MERGE into a table with a declared sort order passes both seams: PP-02 on its
updated rows, then PP-01 when `session_staging` drains them for the sort. Each path still has one
seam, and the statement refuses at whichever bound it crosses first. Question O-8 asks whether that
is the reading the owner wants.

## 3. The refusal, as ruled (Q1)

### 3.1 Where today's pool refusal is raised

- DataFusion's `FairSpillPool::try_grow` raises `DataFusionError::ResourcesExhausted` (the pinned
  dependency `datafusion-execution` 54.1.0, `memory_pool/pool.rs:236`; not in this repository).
- RePark observes it in `RefusalRecordingPool::try_grow`, `crates/repark-core/src/pool_refusals.rs:90-98`,
  and re-raises a recorded refusal as `DataFusionError::ResourcesExhausted` in
  `crates/repark-core/src/nlj_build_reset.rs:73`.
- Either way the error reaches `engine_err`, `crates/repark-core/src/error_map.rs:214`. Nothing
  classifies `ResourcesExhausted`, so it falls to `EngineErrorKind::Other` (line 252), becomes
  `Error::DataFusion` (Display `datafusion engine error: …`, `crates/repark-common/src/lib.rs:27-28`),
  maps to `ErrorClass::Base` (`lib.rs:93`) and surfaces as `PySparkException`.

### 3.2 The new refusal

All three seams raise `DataFusionError::ResourcesExhausted(text)`, built by one function in
`resource_bound.rs`. It therefore shares the pool refusal's class on every door with no taxonomy
change: `Other` at `error_map.rs:252`, then `ErrorClass::Base`, then `PySparkException`. It does not
pass through `RefusalRecordingPool`, so `PoolRefusalLog` does not count it; it is not a pool refusal.

Text template (one line; `{…}` are filled at the crossing):

```
REPARK: the {statement kind} holds {what} in memory outside the memory pool; {what} reached at least {estimate} bytes, above the bound {fraction} x pool {pool} bytes = {limit} bytes. Raise `{key}` (0 = no bound) or raise the memory pool.
```

| path | statement kind | what | key |
|---|---|---|---|
| PP-01 | ordered write | its input before the sort | `repark.write.ordered.max-input-fraction` |
| PP-02 | merge-on-read MERGE | its updated rows | `repark.merge.mor.max-updated-fraction` |
| PA-11 | changelog read | the changelog window | `repark.changelog.max-source-fraction` |

- With DataFusion's prefix and `engine_err`'s, a facade user reads
  `datafusion engine error: Resources exhausted: REPARK: the ordered write holds …`.
- The estimate is the counted bytes at the crossing, a lower bound on the full input; the text says
  "at least".
- The text names no data-deleting action (ruling 76(a)). Its only remedies are raising the key or the
  pool. The pins assert that the text contains none of `delete`, `drop`, `truncate`, `expire`,
  `remove` or `purge` (§7.1).
- On the export path only (`to_arrow` of a changelog read), `_export_engine_error`
  (`python/repark/src/repark/spark/dataframe/export_errors.py:98-107`) also matches
  `resources exhausted` and appends its "raise the FairSpillPool" hint. Raising the pool raises the
  bound too, so the hint stays true; the PA-11 facade cell asserts that both lines are present.
- "Statement kind" is the path's label, not the door's SQL verb: the PP-01 seam is ctx-less and is
  reached by INSERT, INSERT OVERWRITE, replace-where, UPDATE and MERGE alike. Question O-3 asks.

### 3.3 Counted bytes

The seam counts the slice-aware Arrow size of each held batch: the sum over columns of
`ArrayData::get_slice_memory_size` (arrow-data 58.4, `data.rs:507`). It does not use
`RecordBatch::get_array_memory_size`, which counts whole shared buffers and would over-count every
slice of one large batch, so a statement below the bound could refuse (ruling 70). The probe's
"input MiB" is pyarrow `nbytes` of the eight-column source (`probe_bound.py:718`, `:735-741`). The
seam's batches can carry more: v3 lineage columns on MERGE and UPDATE, and changelog metadata
columns on PA-11. Question O-4 asks how to reconcile the two; §7's pool choices keep at least 1.4×
margin either side of the bound for that reason.

### 3.4 What a refusal leaves behind

- PP-01 serial arms, `drive_unpartitioned` and the lineage writer: the drain precedes every writer, so
  no data file exists at the refusal. Snapshot unchanged, zero orphans.
- PP-01 parallel arm: zero orphans only if siblings never write after a refusal (question O-6). Today
  the abort flag (`append.rs:89`, `:113`, `:122`; checked per batch at `append_fanout_serial.rs:91-103`)
  is set only when the dispatcher's send fails.
- PP-02: `matched_work_mor` runs before `write_new_data_files_from_stream_with` (`merge_on_read.rs:84`),
  so no data or delete file exists at the refusal.
- PA-11: a read; nothing is staged.

## 4. The three keys, as ruled (Q2)

### 4.1 Where session keys are parsed and defaulted today

- The builder map is resolved once in `ReparkSessionBuilder::build`, `crates/repark-core/src/session.rs:238`.
  The pool comes from `spill::resolve_build_time_pool_bytes` (line 242; `spill.rs:135-160`, default at
  `spill.rs:148`).
- Write knobs are parsed from the map by `concurrency_from_config_map`
  (`crates/repark-iceberg/src/write/concurrency.rs:100-113`, called at `session.rs:259`). They are
  attached as the `ReparkWriteConfig` extension by `with_write_concurrency` (`concurrency.rs:75-82`,
  `session.rs:293`) and read per statement by `concurrency_from_ctx` (`concurrency.rs:86-95`, 27 product
  call sites). The MERGE knobs follow the same pattern: `scan_prune.rs:396-407` and `:465-475`.
- The live pool can change at runtime (`SET datafusion.runtime.memory_limit`, `spill.rs:190-235`).
  Every seam therefore reads the pool at the statement, never at build:
  `ctx.runtime_env().memory_pool.memory_limit()`, or `TaskContext::memory_pool()` on PA-11.
- `crates/repark-core/src/session.rs` is at exactly 1000 lines, the default ceiling, with no exception
  row; it cannot grow (question O-9).

### 4.2 How each key reaches its seam (lean; question O-2)

- **Parse.** `concurrency_from_config_map` also reads the three hyphen keys, with the measured defaults
  0.10, 0.05 and 0.0625. `with_write_concurrency` stores them on `ReparkWriteConfig`. Both calls already
  exist in `session.rs`, so the file does not grow. The fractions are stored as parts per million, so
  `WriteConcurrency` stays `Copy + Eq`.
- **PP-01.** The seam has no ctx, so the bound rides `WriteConcurrency`. `concurrency_from_ctx` computes
  `ordered_input_limit: Option<usize>` as fraction × the live pool. Every ctx-bearing door already calls
  it. The drains receive it through `fanout_conformed_stream_with_concurrency`, `drive_unpartitioned`
  (`max_concurrent: usize` becomes `WriteConcurrency`) and `write_partitioned_lineage_files_with` (a
  new parameter from `session_staging.rs:67`). `WriterStagingOverrides` was rejected as the carrier: the
  empty-session D3 branch skips it (`crates/repark-spark/src/insert_by_name.rs:238-256`), as do
  `append.rs:67` and `partitioned_files.rs:83`, so it would fail open on a reached door.
- **Sites that build `WriteConcurrency` without ctx**, and so would carry no bound:
  - `predicate_dml/cow_commit.rs:64` (`new(1)`, D10 UPDATE copy-on-write) and
    `partition_write.rs:151` (`new(1)` per partition). Both are reached doors, so both change to keep
    the resolved bound (`with_max_concurrent_files(1)` on the ctx- or `TaskContext`-resolved value).
  - `append.rs:46`, `partitioned_files.rs:22`, `:52`, `file_sink.rs:20` and `:43`. These are the ctx-less
    public APIs (`append`, `write_data_files`, `write_partitioned_data_files`). They have no session
    and so no bound, and are declared so in the key's doc row.
- **PP-02.** `plan_and_commit_mor` reads `concurrency_from_ctx(ctx)` before line 41 (today it reads it
  at line 81, after the matched work) and builds the budget from the merge fraction × the live pool.
- **PA-11.** `ChangelogViewPartition::execute` reads `ReparkWriteConfig` from
  `ctx.session_config().options().extensions` and the pool from `ctx.memory_pool()`.

### 4.3 What 0 does, and invalid values

- A fraction of 0 means no bound on that path: the seam skips the check.
- A pool that is not `MemoryLimit::Finite` (`memory_limit = 0`, `UnboundedMemoryPool`, `Unknown`)
  means no bound on any path. The DataFusion enum is `Infinite`, `Finite(usize)`, `Unknown`.
- A non-number, negative or non-finite value refuses at build as `Error::Config` naming the key. That
  maps to `ErrorClass::IllegalArgument` (`repark-common/src/lib.rs:89`), the same class as
  `repark.write.max-concurrent-files`' refusal. Fractions above 1 are accepted.
- Runtime `conf.set` of the keys: question O-5.

## 5. The PA-10 detector, as ruled (Q3)

### 5.1 Where it lives today

`detect_host_memory_bytes`, `crates/repark-core/src/session/spill.rs:65-75`, reads only
`/sys/fs/cgroup/memory.max` and then `/proc/meminfo` `MemTotal`. `default_memory_limit_bytes`
(lines 26-28) clamps 0.6 × that to the range 1 MiB to 8 GiB (`clamp_default_memory_limit_bytes`,
lines 31-37). The result is the pool when no budget is set (`spill.rs:148`). The audit text cites
lines 69–78; on this head the function is 65–75.

### 5.2 The rule (cgroup v2)

1. Read `/proc/self/cgroup` and take the unified line `0::<path>`.
2. For `<path>` and each ancestor up to `/`, read `/sys/fs/cgroup/<dir>/memory.max` with the
   existing `parse_cgroup_memory_max` (lines 40-49; `max` or empty is unlimited).
3. The cgroup limit is the minimum of the finite values. In a cgroup namespace, `0::/` makes the
   walk read the namespace root's file, which is today's behaviour.
4. If no finite value is found, fall back to `MemTotal` as today. Question O-10 asks whether a finite
   cgroup limit is further capped by `MemTotal`.

### 5.3 The shape

- The walk is a pure function over the `/proc/self/cgroup` text and a reader closure, so tests use
  fixtures.
- It goes in `spill.rs` (673 lines now, about 50 added) beside the two parsers, with its unit tests in
  the existing inline `spill_unit_tests` module (`spill.rs:357`, about 60 added). `spill.rs` stays
  under the 1000 default; no new directory and no `session.rs` line.
- Unit cells:
  - a leaf-only limit;
  - a smaller ancestor;
  - every level `max`, which falls back to meminfo;
  - a namespace root `0::/`;
  - a v1-only file (`4:memory:/x`, no `0::` line), which takes the declared path;
  - malformed text, which falls back.
  - Each cell also asserts the clamped pool, for example a 4 GiB ancestor gives a 2.4 GiB pool.

### 5.4 Declared rows (dated 2026-10-26; an explicit pool budget required)

| platform | what the detector does | declared requirement |
|---|---|---|
| cgroup v1 (no unified `0::` line) | ignores v1 `memory.limit_in_bytes`; uses `MemTotal` | set `repark.memory.limit.gb` or `datafusion.runtime.memory_limit` |
| macOS | no `/proc/meminfo`; falls back to the 8 GiB cap | the same |
| Windows | the same as macOS | the same |

These rows go in the PA-10 dated row (`docs/cutover/production-audit-2026-10-11.md:221`) and in
`docs/guide/session-and-conf.md` "Partitions, batches and memory" (line 287). Whether the session
should refuse to build without an explicit budget on those platforms is question O-11.

## 6. The PA-11 declaration and where it is read

Proposed text:

> **PA-11 — declared bound (2026-10-26).** The changelog view (`create_changelog_view`, `t.changes`
> through `ChangelogViewProvider`) collects its whole source window, concatenates it and transforms it
> in memory outside the session memory pool. Measured statement memory is 7.4 to 7.9 times the window's
> source bytes, and neither `LIMIT` nor a narrow projection reduces it. The window's source bytes are
> bounded at `repark.changelog.max-source-fraction` × the pool, 0.0625 by default (512 MiB at the 8 GiB
> default pool). Above it the read refuses with `Resources exhausted: REPARK: the changelog read …`
> before the window is fully held; 0 means no bound. The pool does not account for these buffers.

Where it is read:

- **Code:** the key is read in `ChangelogViewPartition::execute` (`changelog_view.rs:104`), §4.2.
- **Docs:**
  - the PA-11 dated row (`docs/cutover/production-audit-2026-10-11.md:225`);
  - the user guide `docs/guide/session-and-conf.md`, a new "Resource bounds" section beside "Partitions,
    batches and memory" (line 287) holding all three keys and their declarations;
  - the new dated bound file under `docs/perf/` (step 6).

## 7. Red-first test plan

### 7.1 Shape

- **Rust pins.** A new module `crates/repark-spark/src/tests/resource_bound.rs`, registered in
  `tests/mod.rs` (189 lines), with its `map.md` row.
  - The repark-spark test sessions use DataFusion's default runtime, which is an unbounded pool
    (`tests/common.rs:99-111`, `:113-170`), so no existing Rust pin can reach the bound.
  - The new module builds its ctx on a `RuntimeEnvBuilder` with `FairSpillPool::new(pool)`, the keys
    on `ReparkWriteConfig`, and the `setup_v3_at` catalog shape. That needs one `pub(super)` setup in
    `common.rs` (813 lines) that takes a runtime.
- **Every refusal cell asserts:**
  - the error class (`DataFusionError::ResourcesExhausted` in Rust; `PySparkException` in the facade);
  - the text: `REPARK: the {kind}`, the key, ` x pool `, `bytes`, and none of the deny-listed verbs;
  - the snapshot id and the rows unchanged, the Arrow types unchanged, and zero orphans counted the way
    pin K counts them (files under the table location minus the reachable set,
    `kill_mid_write_hold.rs` `reachable`).
- **Every control asserts** a commit with values, Arrow types and per-file order equal to main's answer
  for the same statement. "Per-file order" is the row order within each data file against the
  declared sort.
- **The facade cell.** `python/repark/tests/writes/test_ice_resource_bound_1.py`, with its `map.md` row:
  - builder `datafusion.runtime.memory_limit = '32M'` against `'128M'`;
  - one ordered INSERT, one MoR MERGE and one changelog read;
  - `PySparkException` with the ruled text;
  - the key's build-time refusal for an invalid value (`IllegalArgumentException` naming the key).
- **Detector unit tests:** §5.3, in `repark-core`.

### 7.2 Cells, pools and what each proves

Bounds at the chosen pools (MiB): PP-01 0.10 ×; PP-02 0.05 ×; PA-11 0.0625 ×.

| cell | grid | rows | pool MiB | bound MiB | counted (table) MiB | main | head | margin |
|---|---|---|---|---|---|---|---|---|
| PP-01-R1 ordered INSERT (D2), unpart and 32 parts, writers 1 and 4 | 1× PR | 100k | 32 | 3.2 | 5.4 | commits | **refuses** | 1.69× above |
| PP-01-C1 the same | 1× PR | 100k | 128 | 12.8 | 5.4 | commits | commits | 2.37× below |
| PP-01-R2 the same | 1× PR | 1M | 128 | 12.8 | 54.5 | commits | **refuses** | 4.3× above |
| PP-01-C2 the same | 1× PR | 1M | 2048 | 204.8 | 54.5 | commits | commits | 3.8× below |
| PP-01-K0 R1 with the key at 0 | 1× PR | 100k | 32 | none | 5.4 | commits | commits | the 0 rule |
| PP-01-D door row: D3, D4, D5, D10, each R1 and C1 | 1× PR | 100k | 32 / 128 | 3.2 / 12.8 | 5.4 | commits | refuses / commits | as R1 / C1 |
| PP-01-M D8 copy-on-write MERGE on an ordered table, key 0.01 against the default | 1× PR | 1M | 2048 | 20.5 / 204.8 | 54.5 | commits | refuses / commits | 2.66× above / 3.8× below (at 32 MiB main's in-pool sort refuses first, census-seeded row) |
| PP-01-U unordered table, the R2 shape | 1× PR | 1M | 32 | 3.2 | 54.5 | commits | commits | seam not reached |
| PP-02-C1 MoR MERGE (D9) 100 % match, 32 files, writers 1 and 4 | 1× PR | 1M | 2048 | 102.4 | 54.5 | commits | commits | 1.88× below |
| PP-02-R1 the same with the key at 0.01 | 1× PR | 1M | 2048 | 20.5 | 54.5 | commits | **refuses** | 2.66× above |
| PP-02-R2 10 % + NOT MATCHED BY SOURCE, key 0.01 | 1× PR | 1M | 2048 | 20.5 | 54.5 | commits | **refuses** | 2.66× above (both legs counted) |
| PP-02-C2 10 % + NOT MATCHED BY SOURCE, key 0.01 | 1× PR | 200k | 2048 | 20.5 | 10.9 | commits | commits | 1.88× below |
| PP-02-P the grid's 32 and 128 MiB cells | 1× PR | 200k | 32 / 128 | 1.6 / 6.4 | ≤ 10.9 | pool refusal | a refusal | the in-pool sort refuses on main; the cell asserts the class and an unchanged snapshot and records which text fires |
| PA-11-R1 `SELECT id … LIMIT 10` and `count(id)` | 1× PR | 1M | 32 | 2.0 | 54.5 | answers | **refuses** | 27× above |
| PA-11-C1 the same | 1× PR | 1M | 2048 | 128 | 54.5 | answers | answers | 2.35× below |
| PP-01 ordered-scale | 4× and 16× nightly | 4M, 16M | 32 | 3.2 | 218, 872 | commits | refuses | above |
| PP-01 ordered-scale controls | 4× nightly | 4M | 8192 | 819 | 218 | commits | commits | 3.8× below |
| PP-02 merge-scale, default fraction | 4× nightly | 4M | 2048 | 102.4 | 218 (10 % + NOT MATCHED BY SOURCE) | commits | **refuses** | 2.1× above |
| PP-02 merge-scale controls | 4× nightly | 4M | 8192 | 409.6 | 218 | commits | commits | 1.88× below |
| PP-02 merge-scale | 16× nightly | 16M | 8192 | 409.6 | 872 | commits | **refuses** | 2.1× above |
| PA-11 changelog | 4× and 16× nightly | 4M, 16M | 32 / 8192 | 2 / 512 | 218, 872 | answers | refuses; 4M at 8192 answers | as shown |

- **Formats.** Every Rust cell runs on format v2 and v3. Each facade cell runs on v2 and v3.
- **Red on main.** Main ignores the keys and commits or answers, so every **refuses** cell above is red
  on main and every control is green on main.
- **Ruling 70.** No cell is placed so that head refuses a statement main answers below the bound.
- **PP-02 at 1×.** At the default fraction no audit-grid cell can refuse: below 512 MiB the in-pool
  sort refuses first (table, every 200k and 1M cell at 32 and 128 MiB), and at 512 and 2048 MiB every
  updated set is below the bound. So the PR's PP-02 refusal cells lower the key to 0.01. The
  default-fraction PP-02 refusal is first reached at 4× (4M rows, pool 2048 MiB). Question O-7 asks
  whether the owner accepts that split.
- **Sizes.** Pools are chosen per Q5; there is no pool-peak reader. 1× runs in the pull request; 4× and
  16× run in the acceptance nightly only (Q4). The 16× PP-02 cells at 8192 MiB measured 11 to 14 GiB
  process peak RSS on main, so question O-12 asks where the nightly runs.

### 7.3 Kill against refusal (pin K)

U-9's `crates/repark-spark/src/tests/kill_mid_write.rs` stays byte-unchanged.

- **Why a second child entry.** The child entry `kill_mid_write_child` (lines 710-750) runs only the
  fixed `CELLS` table (lines 38-120), whose tables are unordered, and `Cell`'s fields are private. So
  the pair lives in a second new module, `crates/repark-spark/src/tests/resource_bound_kill.rs`.
- **The child.** It has its own child entry and environment variable, and reuses the `pub(super)`
  helpers: `HoldCatalog`, `HoldMode`, `local_path`, `reachable`, `snapshot_ids` from
  `kill_mid_write_hold.rs`; `read_marker`, `await_marker_or_exit`, `child_output` and `newest_metadata`
  from `kill_mid_write.rs`.
- **KR-1, ordered INSERT (D2).** Statement S on a table with a declared sort order, run at pool 32 MiB:
  - (a) Above the bound, in-process: S refuses with the ruled text, the process lives, the snapshot is
    unchanged and zero orphans are counted.
  - (b) The same S with the key at 0, in the child under `HoldMode::Hold`: S reaches the commit and is
    killed. The parent sees `signal() == Some(9)`, no refusal text in the child's output, the snapshot
    unchanged on reopen, and orphans that are exactly the marker's attempted files, counted per kind as
    pin K counts them (the `orphan_counts` shape, `kill_mid_write.rs:392`). The literal K-1 numbers do
    not apply: this table is larger and ordered.
- **KR-2, MoR MERGE (D9), 1M rows, 100 % match, 32 files.** The same pair: refusal with the key at
  0.01 at pool 2048 MiB, kill with the key at 0. The kill side expects data and delete orphans, as
  K-5 does (a position delete on v2, a deletion vector on v3), counted against the marker.
- Both run on v2 and v3, `#[cfg(unix)]`. PA-11 has no kill pair: it is a read.

## 8. The cost gate plan

Order §5 leaves "whether it binds here" to the owner; the plan is ready either way.

- **The write set below the bound**, at the default 8 GiB pool and default keys:
  - an ordered INSERT, 100k rows, unpartitioned and 32 partitions, writers 1 and 4 (PP-01, through the new drain);
  - an unordered INSERT, the same sizes (the carrier change only);
  - a MoR MERGE, 200k rows, 10 % match, and the same with NOT MATCHED BY SOURCE (PP-02);
  - a changelog `SELECT count(id)` over 1M rows (PA-11);
  - an UPDATE copy-on-write on an ordered table, 100k rows (D10 through `cow_commit.rs:64`).
- **The method**, per the 2026-10-26 lesson (`task/lessons.md:504`):
  - one release build each of base `e168f29c` and of head;
  - each statement in its own process, with base and head order alternated per round;
  - 30-run medians per statement per round, at least 12 rounds;
  - start load recorded; ratio head/base per statement, each ≤ 1.05;
  - the format of `task/wo/write-spine-1/evidence/step-2-cost-2026-10-26.md`.
- **The probe** is a `cost` subcommand of `probe_bound.py`, or a sibling `probe_cost.py` beside the
  order. The output is a dated evidence file with the run ids.
- **Expected.** The added work is one slice-size sum per held batch (O(columns) per batch) and one
  atomic add. The changelog drain replaces `collect`'s per-partition buffering with one merged stream.
  That changes partition concurrency and is the cell most likely to move, so it is measured, not assumed.

## 9. Files, sizes and ceilings

Ceilings are from `scripts/check_rust_file_size.py`: default 1000, plus its EXCEPTIONS. The crate roots
are under `scripts/check_lib_rs.py`. No ceiling rises.

| file | lines now | ceiling | change |
|---|---|---|---|
| `crates/repark-iceberg/src/write/resource_bound.rs` (new) | 0 | 1000 | **holds the bound check**: the three keys and defaults, parts-per-million fractions, the limit from fraction × live pool, the counted-bytes measure, `drain_ordered_input`, `UpdatedRowsBudget`, the changelog drain helper, the one refusal-text builder |
| `crates/repark-iceberg/src/write/distribution.rs` | 505 | 1000 | the three drain loops call the seam; `drive_unpartitioned` takes `WriteConcurrency` |
| `crates/repark-iceberg/src/write/merge/row_lineage.rs` | 317 | 1000 | `drain` replaced; new budget parameter |
| `crates/repark-iceberg/src/write/merge/merge_on_read.rs` | 265 | 1000 | budget across both legs; `concurrency_from_ctx` moved before line 41 |
| `crates/repark-iceberg/src/catalog/changelog_view.rs` | 121 | 1000 | charged drain at line 111 |
| `crates/repark-iceberg/src/write/concurrency.rs` | 161 | 1000 | `WriteConcurrency` gains the limit; the config-map parse and the extension gain the three fractions |
| `crates/repark-iceberg/src/write/append.rs` | 1737 | **1737 (exception, at ceiling)** | call sites at lines 84 and 97 pass the bound; must stay line-neutral or shrink with the baseline lowered in the same commit |
| `crates/repark-iceberg/src/write/merge/session_staging.rs` | 128 | 1000 | passes the bound to the lineage writer |
| `crates/repark-iceberg/src/write/write_options.rs` | 642 | 1000 | `drive_unpartitioned` call |
| `crates/repark-iceberg/src/write/merge/file_sink.rs` | 76 | 1000 | `drive_unpartitioned` call |
| `crates/repark-iceberg/src/write/predicate_dml/cow_commit.rs` | 151 | 1000 | line 64 keeps the resolved bound |
| `crates/repark-iceberg/src/write/partition_write.rs` | 796 | 1000 | line 151 keeps the `TaskContext`-resolved bound |
| `crates/repark-iceberg/src/write/mod.rs` | 193 | 1000 | `mod resource_bound;` and re-exports of the key constants |
| `crates/repark-iceberg/src/lib.rs` | 48 | 150 (lib.rs default) | the key constants beside `MAX_CONCURRENT_FILES_KEY` (line 29) |
| `crates/repark-core/src/session/spill.rs` | 673 | 1000 | the cgroup v2 walk and its unit cells |
| `crates/repark-core/src/session.rs` | 1000 | **1000 (default, at ceiling)** | unchanged under the lean of §4.2 |
| `crates/repark-core/src/lib.rs` | 155 | **155 (exception, at ceiling)** | unchanged (the detector is crate-internal) |
| `crates/repark-spark/src/tests/resource_bound.rs` (new) | 0 | 1000 | the §7.2 Rust cells |
| `crates/repark-spark/src/tests/resource_bound_kill.rs` (new) | 0 | 1000 | the §7.3 pair |
| `crates/repark-spark/src/tests/common.rs` | 813 | 1000 | one `pub(super)` setup taking a runtime |
| `crates/repark-spark/src/tests/mod.rs` | 189 | 1000 | two `mod` lines |
| `crates/repark-iceberg/src/write/merge/tests/writer_metrics.rs`, `merge/tests/lineage_stream.rs`, `distribution/tests.rs`, `distribution/sort_order_tests.rs` | — | — | call sites of the changed signatures |
| `python/repark/tests/writes/test_ice_resource_bound_1.py` (new) | 0 | `scripts/check_lib_py.py` default | the facade cell |
| `crates/repark-spark/src/tests/kill_mid_write.rs` | 840 | — | **unchanged** |

Every directory touched carries its `map.md` row in the same commit, including
`crates/repark-iceberg/src/write/`, `merge/`, `catalog/`, `crates/repark-core/src/session/`,
`crates/repark-spark/src/tests/` and `python/repark/tests/writes/`. Docs and records follow order §2
and step 6.

## 10. Open questions for the orchestrator

Each has a premise and a lean. The bound, the text, the keys, PA-10's rule and the sizes are ruled
and are not reopened here.

- **O-1, is the PP-01 drain one seam?**
  - Premise: four drain loops hold the input today (§1.1). The sketch replaces them with calls to one
    function that alone counts and refuses.
  - Lean: one seam (one check, one raise, one text). If the orchestrator counts the call sites, this is
    order §6's second-seam halt; the alternative single site, `sort_batches_by_default_order`, sees the
    input only after the full drain and so cannot refuse before the whole input is held.
- **O-2, the PP-01 carrier.**
  - Premise: the seam has no ctx. `WriterStagingOverrides` skips D3's empty-session branch and two
    other sites (§4.2).
  - Lean: `WriteConcurrency` carries the computed limit (`Copy + Eq` kept). The two reached sites that
    build it without ctx change. The ctx-less public APIs are declared unbounded.
- **O-3, "statement kind".**
  - Premise: Q1 says the text names the statement kind; the PP-01 seam serves five SQL verbs.
  - Lean: the path's label (ordered write, merge-on-read MERGE, changelog read), with no door threading.
    The alternative passes the verb through every PP-01 door, a wider change.
- **O-4, counted bytes against the table's bytes.**
  - Premise: the table's input is pyarrow `nbytes` of eight columns. The seam counts the batches it
    holds, which can carry lineage or changelog metadata columns.
  - Lean: count the slice-aware size of what the seam holds. Step 2's first red run prints counted bytes
    for the 100k and 1M cells beside the table's. If the ratio exceeds 1.4, the pins' pools move and
    the owner hears before step 3.
- **O-5, runtime `conf.set` of the keys.**
  - Premise: today the sibling keys `repark.write.max-concurrent-files` and `repark.merge.scan-pruning`
    are build-time only. A runtime `conf.set` of an unknown `repark.*` key is stored by the facade and
    not forwarded (`python/repark/src/repark/spark/session/builder_conf.py:245-306`).
  - Lean: build-time like the siblings. A runtime `conf.set` of the three keys refuses loudly, naming
    the builder spelling, rather than being silently ignored. The live pool still moves the bound at
    runtime.
- **O-6, the parallel ordered arm after a refusal.**
  - Premise: sibling workers can finish their drains and write files after one worker refuses, because
    the abort flag is set only on a failed send (§3.4). That would make the orphan count of a writers-4
    refusal nondeterministic.
  - Lean: the shared budget sets the existing `aborted` flag, and each worker awaits the other workers'
    drain ends (a barrier sized to the worker count) before it sorts. Ordered workers already cannot
    write before their drain ends, so the cost is the skew between drain ends; the cost gate measures it.
- **O-7, PP-02 refusal cells at 1× use the key.**
  - Premise: no audit-grid cell refuses at the default 0.05 (§7.2).
  - Lean: the PR pins a key-lowered refusal (0.01) plus the default-fraction control. The
    default-fraction refusal is pinned at 4× in the nightly.
- **O-8, a MoR MERGE into an ordered table passes two bounds.**
  - Premise: the updated rows pass the PP-02 seam, then the PP-01 drain (§2).
  - Lean: keep both. The refusal names whichever key is crossed first. One pin cell (PP-02-R1 on an
    ordered target) asserts the PP-02 text.
- **O-9, `session.rs` is at its 1000-line ceiling.**
  - Premise: a new parse-and-attach line in `build` would red the size gate.
  - Lean: ride `concurrency_from_config_map` and `with_write_concurrency`. The alternative is a line in
    `session.rs` paid for by moving an equal block out (moved code sheds its comments).
- **O-10, cgroup limit against `MemTotal`.**
  - Premise: Q3 rules the minimum over the cgroup walk; a cgroup can declare more than the machine has.
  - Lean: `min(cgroup minimum, MemTotal)`.
- **O-11, the declared platforms.**
  - Premise: Q3 says cgroup v1, macOS and Windows "require an explicit pool budget".
  - Lean: declare it in the dated rows and the guide, with no build refusal. A refusal would stop every
    macOS session that sets no budget, a statement main answers (ruling 70).
- **O-12, where the 4× and 16× nightly runs.**
  - Premise: on main the 16× cells reach 11 to 14 GiB process RSS. On head they refuse at the crossing,
    and the largest head-side control is the 4M MoR MERGE at 8192 MiB (about 4.4 GiB in main's table).
    The tier-1 hosted runners hold about 16 GiB. `aws-acceptance.yml` is tier 2 (live AWS), not the
    place for a local-memory grid.
  - Lean: the nightly runs on head as a tier-1 scheduled job over `probe_bound.py`'s scale grids with an
    expected-outcome column. The red-on-main run of the 4× and 16× cells is made once, locally under
    the memory scope the lane brief names, with its run ids recorded in the step-6 bound file.
- **O-13, the constrained-process integration test PA-10's required result names.**
  - Premise: it needs a cgroup the test can create.
  - Lean: acceptance only (a `systemd-run --scope -p MemoryMax=…` cell in the nightly). PR coverage is
    the fixture unit cells.

## 11. Out of scope, observed

- The MoR delete state (`seen_pair`, `pair_indices`, interning) stays uncounted. The audit's PP-02 text
  names it; the ruled bound is updated rows only.
- `sort_batches_by_default_order` still sorts under `TaskContext::default()` (`distribution.rs:222`),
  an unbounded pool with no spill directory. Spilling is out of scope (order §0).
- The PA-10 audit text cites `spill.rs` lines 69–78; on this head the detector is lines 65–75.
- `_export_engine_error`'s memory hint applies only on the export path; write statements carry the
  engine text alone (§3.2).
- `partition_write.rs:151` and `cow_commit.rs:64` build a fresh `WriteConcurrency::new(1)` and so drop
  every session-resolved write knob, not only this bound. Today that knob is the file count, which is
  forced to 1 there anyway.
