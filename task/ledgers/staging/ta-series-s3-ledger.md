# Unit ledger — TA-SINGLE-SERIES-PARALLEL-1 S3 · a one-partition projection runs in parallel, and the useless RoundRobin goes

**Date:** 2026-10-04 · **Branch:** `perf/ta-series-s3-parallel-projection` · **Base:** `978a3efd`
(S2b, PR #940) · **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** `/tmp/oc-worker/direct/wo/ta-series/s3-order.md` (grade A-minus), with the design in the
series sketch §3.2 ("The parallel projection"), §3.3, §5, §6 and §7.

**Orchestrator rulings on the executor's halt (2026-10-04).** Head's plan showed the 30 ms L1/L2
sandwich under two projections with one non-column expression each, so R-S3-1's "at least two"
could never reach it. Q-S3-1 = A: R-S3-1 splits into **R-S3-1a**, the RoundRobin drop (any
projection, or chain of projections, directly over a `RoundRobinBatch` over one partition, whatever
the expression count, under the parent-requirement check and the `repark.parallel` flag), and
**R-S3-1b**, the parallel swap (at least two non-column expressions plus the row threshold). This
folds the earlier sketch's S4 ("drop `SPM ← RepartitionExec(RoundRobin, input_partitions=1)`",
`/tmp/oc-worker/direct/wo/ta-single-series/sketch.md`) into S3. Q-S3-2 = yes: neither rule fires
on a volatile expression (new pin P-S3-7). A later note, measured on S1 (PR #944): with the
ordered cache the L1 RoundRobin reads `maintains_sort_order=true` and spills the whole 95 MB batch;
R-S3-1a must drop that shape too (new pin P-S3-8).

**Orchestrator ruling Q-S3-3 = A (2026-10-04, narrowing R-S3-1a).** As first ruled, the drop also
removed the RoundRobin over single-partition sources that emit many batches. `spark.range(n)`
emits 65,536-row batches, and DataFusion fans them out with `RoundRobinBatch(target_partitions)`.
Without that fan-out, 9 facade memory tests lost their pool pressure and failed in
`test_t2_sort_memory.py`, `test_t2_spill_reach.py` and `test_h3_spill_matrix.py`. Disabling only
the drop made those three files pass, 51 of 51. **Ruled:** a `RoundRobinBatch` over one input
partition is dropped **only** when its input emits exactly one batch by construction: a
`WindowAggExec` or `ParallelWindowExec`, directly or through `ProjectionExec` /
`ParallelProjectionExec`. Every other single-partition source keeps its fan-out: `range()`, file
scans, a `DataSourceExec`, or a `MemTable` with several batches. The parent check, the flag and the
volatility refusal stand. P-S3-2, P-S3-4 and P-S3-8 were rebuilt over window inputs, and P-S3-9 is
new.

**Why.** On the owner's `ta.*` shape, DataFusion puts a `RepartitionExec RoundRobinBatch(16)` over
each one-batch window output. Between levels 1 and 2, a `SortPreservingMergeExec` (explicit
spelling) or `CoalescePartitionsExec` (bare spelling) merges the 16 partitions back: the 30 ms
"middle sandwich". At the top, the 13 × `round`, 3 × `/` and `CASE` post-window expressions run
serially on one batch. Out of scope: S2a (the rewrite, `null_prefix`, `series_order`), S1, S5
fusion, any kernel or facade change.

## PROPOSITION LEDGER — TA-SERIES-S3 — 2026-10-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | P-S3-1: a projection of 13 × `round`, 3 × `/` and a `CASE` over a one-partition window output gives, as a `ParallelProjectionExec`, the same columns bit for bit, in the same order, as the serial `ProjectionExec` of the flag-off session. | `parallel_projection_bit_identical`; mutation: assemble the columns in completion order. | PROVEN | Green: 102,400 rows (three source batches, one window batch), 19 columns, `to_bits` and validity equal; one `ParallelProjectionExec`, no RoundRobin (one in the off plan), one batch on the parallel path. Red under the mutation: `projection_tests.rs:227`, `left: 18444492273895866368, right: 0`. §2. |
| C-002 | P-S3-2: on the owner's two RoundRobins, both over window outputs (L1/L2 under a two-projection chain below a `SortPreservingMergeExec` or a `CoalescePartitionsExec`, and the top one under the post-window projections), both RoundRobins are dropped. The merge then sees one partition, and the answer is unchanged. | `parallel_projection_removes_rr_spm_sandwich` (both merge kinds); mutation: skip the drop. | PROVEN | Green: two RoundRobins become zero, the chain stays `ProjectionExec ← ProjectionExec ← WindowAggExec`, the two qualifying projections become `ParallelProjectionExec`, and the output is bit-identical (sorted by id for the coalesce case). Red under the mutation: `projection_tests.rs:396`, `left: 2, right: 0`. §2. |
| C-003 | P-S3-3: a batch below `PARALLEL_PROJECTION_MIN_ROWS` is evaluated serially in line; a batch at or above it takes the parallel path. The output is equal either way. | `parallel_projection_small_input_serial` (the `parallel_batches` counter); mutation: remove the threshold. | PROVEN | Green: three small batches count 0; a 49,152-row batch plus a 300-row batch count 1. Red under the mutation: `projection_tests.rs:442`, `left: 3, right: 0`. The threshold value is provisional; its measurement belongs to the speed phase (C-011). §2. |
| C-004 | P-S3-4: the drop is refused when the parent's requirement would not hold afterwards. The parent's distribution must be unspecified or single-partition (a hash requirement is refused: one partition would satisfy it but break co-partitioning), and its ordering must be satisfied by the rebuilt child. | `parallel_projection_respects_parent_ordering` (a merge on an ordering the child lacks; a partitioned hash join; a satisfiable merge as control); mutation: drop the requirement check. | PROVEN | Green: both refused plans print unchanged and keep their RoundRobin; the control drops it. Red under the mutation: `projection_tests.rs:472`, the RoundRobin over the window under `SortPreservingMergeExec: [a@1 ASC]` disappears. §2. |
| C-005 | P-S3-5: with the `repark.parallel` carrier off (by `ConfigOptions` or `ReparkSessionBuilder::parallel_single_partition(false)`), both passes are no-ops. EXPLAIN on the on-session prints `ParallelProjectionExec: expr=[…]` with `ProjectionExec`'s list. | `parallel_projection_flag_off`; mutation: ignore the flag. | PROVEN | Green. Red under the mutation: `projection_tests.rs:508`, the sandwich plan turns into `ParallelProjectionExec` with no RoundRobin. §2. |
| C-006 | P-S3-6: through the facade, on release wheels, a 50,000-row slice of `test_futures.parquet` in both owner spellings is bit-identical to base `978a3efd`, and head's plans carry no one-partition RoundRobin. | `probe.py` and `compare.py` on base and head release wheels. | OPEN | Pending the release wheels (§3). |
| C-007 | P-S3-7: neither pass fires when any expression of the projection, or of any projection in a chain, is volatile. Plan and values stay identical to the flag-off session. | `parallel_projection_volatile_unchanged` (a seeded volatile probe UDF through SQL, and a manual chain); mutation: ignore volatility. | PROVEN | Green: the on and off plans print identically with one RoundRobin, and values are equal by id. Red under the mutation: `projection_tests.rs:580`, `ParallelProjectionExec: expr=[id@0 as id, seeded_rand(42) as r, …]` with no RoundRobin above the window. §2. |
| C-008 | The S2b machinery carries over: the error of the lowest expression index wins whatever finishes first, and dropping the stream cancels the queued expression tasks. | `parallel_projection_lowest_index_error`, `parallel_projection_drop_cancels`; mutations: first-to-finish error; run the batch future on a detached `tokio::spawn`. | PROVEN | Green. Red: `Execution error: probe failure second`; `projection_tests.rs:792`, `left: 1, right: 0`. §2. |
| C-009 | P-S3-8: a RoundRobin over one ordered window output (EXPLAIN `maintains_sort_order=true`, the S1 spill shape above the L1 window) is dropped too, over `WindowAggExec` and over `ParallelWindowExec`. The rebuilt projection keeps the window's ordering, so the `SortPreservingMergeExec` above it passes its input through. | `parallel_projection_drops_order_preserving_round_robin` (a sorted single-batch source under each window kind); mutation: refuse when the RoundRobin's input carries an ordering. | PROVEN | Green on both window kinds: zero RoundRobins, and the merge's child is a one-partition `ParallelProjectionExec` ordered by `id`; the output is bit-identical. Red under the mutation: `projection_tests.rs:831`, `left: 1, right: 0`. §2. |
| C-012 | P-S3-9 (ruling Q-S3-3): a single-partition source that emits several batches keeps its RoundRobin, whether under a `CoalescePartitionsExec`, an order-preserving merge, or at the root. | `parallel_projection_keeps_multi_batch_fan_out`; mutation: drop the one-batch test. The 9 facade memory tests are the end-to-end check. | PROVEN | Green: all three plans print unchanged with one RoundRobin, and the memory files pass 51 of 51. Red under the mutation: `projection_tests.rs:871`, the coalesce plan becomes `ParallelProjectionExec` over the source. §2. |
| C-010 | Gates: goldens unchanged; `repark-core --lib`, `repark-ta`, the facade's projection- and window-heavy files and the parity `-k "ta or window or projection"` match base apart from listed plan-text assertions; clippy, fmt, panic ban, file sizes, comment ban, maps, ledgers, crate DAG, no Cargo change. | The commands in §4. | PROVEN | §4: every gate green, no plan-text assertion needed an update. `test_deep_filter_chain_crash_1.py` runs on the release wheel (§3). |
| C-011 | Speed (the orchestrator's later run): on release wheels built with the same command, head is not slower than base on any shape, and partitioned stays ≤ 0.18 s; the row threshold and the per-expression task grouping are set by measurement; the kernel race stays ≤ 1.02×. | Interleaved median of 5; kernel race median of 3. | OPEN | Held at READY_FOR_SPEED by the order. |

## 1. Design record (2026-10-04)

- **Where the drop is decided.** Bottom-up, at the first non-projection parent of a projection
  chain (or at the root). The chain is rebuilt with `ProjectionExec::try_new` over the
  RoundRobin's input, so DataFusion recomputes its properties. A RoundRobin over one partition
  keeps that partition's ordering in its equivalence properties
  (`RepartitionExec::maintains_input_order_helper`), so the rebuilt chain carries the same
  orderings. The parent's ordering is re-checked with the test `SanityCheckPlan` uses, because
  this rule runs after it.
- **Batches.** `RoundRobinBatch` moves whole batches, and `ParallelProjectionExec` projects batch
  by batch, so every expression sees the batches it saw before. That keeps per-batch functions
  such as `rand(seed)` unchanged, even though they are refused anyway (C-007).
- **Tasks.** One `spawn_blocking` task per non-column expression, under a semaphore of
  `min(tasks, target_partitions)`. Column expressions are evaluated in line.
- **Root partition count.** A root chain over a window output loses its RoundRobin too, so the
  plan's output partition count drops from n to 1. `write_data_files_from_plan` (`repark-iceberg`)
  sizes its writers from that count. A window emits one batch, so before the drop only
  partition 0 carried rows; the file count is unchanged. Multi-batch sources keep their fan-out
  (ruling Q-S3-3, C-012).

## 2. Pin and mutation record (2026-10-04)

Mutations were applied by `/tmp/oc-worker/direct/wo/ta-series/s3-evidence/mutations/mutate.py`.
It saves the source, rebuilds the pin's test binary through the build lock, restores the source
and touches it. Outputs are `m*.out` beside it (the first round, before the Q-S3-3 narrowing, is in `round1/`).
After the narrowing all ten were re-run, and all ten went red. After them,
`cargo test -p repark-core --lib` ran 991 passed, 1 ignored. A first run of the test-only
contexts found that `DataSourceExec` splits its batches to the session `batch_size` (8,192 by
default), which kept every probe batch below the threshold. The test contexts now set a 1 Mi-row
batch size; the owner's configuration uses 64,000.

## 4. Gates (2026-10-05, head)

All cargo commands ran under the opus-cargo flock in `repark.slice`.
- Goldens: hash `af74ab17a59b0452a92b03e085bb6e0106f8ae74bbd0ec9a32db01239f5e87a8`, unchanged.
  `cargo test -p repark-ta`: 106 + 11 + 41 + 1. `--features datafusion`: 151 lib, 11 contract,
  41 goldens, 2 `parallel_window`, 26 `prefix_goldens`.
- `cargo test -p repark-core --lib`: 991 passed, 1 ignored (10 S3 pins). `repark-python --lib`:
  119. `repark-spark --test ta_window`: 10. `repark-sql --test ta_toll`: 6.
- Facade, debug build in the clone's venv, `-n 8`. 110 files: S2b's 70 window/TA files plus
  every test file naming `withColumns`, `round(`, or column arithmetic in `col(...) op` or
  `.select(` form (the list is in the hand-back). Result: 3,247 passed, 135 skipped,
  103 xfailed, 0 failed. No plan-text assertion changed. The three memory files: 51 passed.
- Parity `python/repark-parity/tests -k "ta or window or projection"`: 276 passed.
- Hygiene: workspace clippy `--all-targets -D warnings -A clippy::disallowed_methods`,
  `cargo fmt --all --check`, `make rust-panic-ban`, `check_rust_file_size.py`, comment ban
  `hits=0`, `check_map_md.sh`, `check-ledgers`, `check-ledger-grammar`, the crate DAG, and
  `git diff --exit-code 978a3efd -- Cargo.lock '**/Cargo.toml'`.
