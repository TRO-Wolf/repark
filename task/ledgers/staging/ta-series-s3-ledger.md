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
| C-003 | P-S3-3: a batch below `PARALLEL_PROJECTION_MIN_ROWS` is evaluated serially in line; a batch at or above it takes the parallel path. The output is equal either way. | `parallel_projection_small_input_serial` (the `parallel_batches` counter); mutation: remove the threshold. | PROVEN | Green: three small batches count 0; a 49,152-row batch plus a 300-row batch count 1. Red under the mutation: `projection_tests.rs:442`, `left: 3, right: 0`. The threshold value was settled by measurement in the speed phase (C-011, §5): kept at 16,384. §2. |
| C-004 | P-S3-4: the drop is refused when the parent's requirement would not hold afterwards. The parent's distribution must be unspecified or single-partition (a hash requirement is refused: one partition would satisfy it but break co-partitioning), and its ordering must be satisfied by the rebuilt child. | `parallel_projection_respects_parent_ordering` (a merge on an ordering the child lacks; a partitioned hash join; a satisfiable merge as control); mutation: drop the requirement check. | PROVEN | Green: both refused plans print unchanged and keep their RoundRobin; the control drops it. Red under the mutation: `projection_tests.rs:472`, the RoundRobin over the window under `SortPreservingMergeExec: [a@1 ASC]` disappears. §2. |
| C-005 | P-S3-5: with the `repark.parallel` carrier off (by `ConfigOptions` or `ReparkSessionBuilder::parallel_single_partition(false)`), both passes are no-ops. EXPLAIN on the on-session prints `ParallelProjectionExec: expr=[…]` with `ProjectionExec`'s list. | `parallel_projection_flag_off`; mutation: ignore the flag. | PROVEN | Green. Red under the mutation: `projection_tests.rs:508`, the sandwich plan turns into `ParallelProjectionExec` with no RoundRobin. §2. |
| C-006 | P-S3-6: through the facade, on release wheels, a 50,000-row slice of `test_futures.parquet` in both owner spellings is bit-identical to base `978a3efd`. On the full file, head's plans for both spellings carry no `RoundRobinBatch`. | `probe.py` and `compare.py` (`s3-evidence/p6/`) on release wheels of base and head, both built with `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 maturin build --release`. | PROVEN | 50k slice: bare and explicit, 50,000 × 28, no difference in bits, validity or values; a seeded `rand(42)` projection is identical too. Head plans carry 2 `ParallelProjectionExec` per spelling. Full file: base 2 RoundRobins per spelling, head 0. The explicit spelling is bit-identical to base at 1,000,000 rows. Bare and `rand` are not comparable at 1 M, because base differs from base run to run there (the cache read order S1 fixes). §3. |
| C-007 | P-S3-7: neither pass fires when any expression of the projection, or of any projection in a chain, is volatile. Plan and values stay identical to the flag-off session. | `parallel_projection_volatile_unchanged` (a seeded volatile probe UDF through SQL, and a manual chain); mutation: ignore volatility. | PROVEN | Green: the on and off plans print identically with one RoundRobin, and values are equal by id. Red under the mutation: `projection_tests.rs:580`, `ParallelProjectionExec: expr=[id@0 as id, seeded_rand(42) as r, …]` with no RoundRobin above the window. §2. |
| C-008 | The S2b machinery carries over: the error of the lowest expression index wins whatever finishes first, and dropping the stream cancels the queued expression tasks. | `parallel_projection_lowest_index_error`, `parallel_projection_drop_cancels`; mutations: first-to-finish error; run the batch future on a detached `tokio::spawn`. | PROVEN | Green. Red: `Execution error: probe failure second`; `projection_tests.rs:792`, `left: 1, right: 0`. §2. |
| C-009 | P-S3-8: a RoundRobin over one ordered window output (EXPLAIN `maintains_sort_order=true`, the S1 spill shape above the L1 window) is dropped too, over `WindowAggExec` and over `ParallelWindowExec`. The rebuilt projection keeps the window's ordering, so the `SortPreservingMergeExec` above it passes its input through. | `parallel_projection_drops_order_preserving_round_robin` (a sorted single-batch source under each window kind); mutation: refuse when the RoundRobin's input carries an ordering. | PROVEN | Green on both window kinds: zero RoundRobins, and the merge's child is a one-partition `ParallelProjectionExec` ordered by `id`; the output is bit-identical. Red under the mutation: `projection_tests.rs:831`, `left: 1, right: 0`. §2. |
| C-012 | P-S3-9 (ruling Q-S3-3): a single-partition source that emits several batches keeps its RoundRobin, whether under a `CoalescePartitionsExec`, an order-preserving merge, or at the root. | `parallel_projection_keeps_multi_batch_fan_out`; mutation: drop the one-batch test. The 9 facade memory tests are the end-to-end check. | PROVEN | Green: all three plans print unchanged with one RoundRobin, and the memory files pass 51 of 51. Red under the mutation: `projection_tests.rs:871`, the coalesce plan becomes `ParallelProjectionExec` over the source. §2. |
| C-010 | Gates: goldens unchanged; `repark-core --lib`, `repark-ta`, the facade's projection- and window-heavy files and the parity `-k "ta or window or projection"` match base apart from listed plan-text assertions; clippy, fmt, panic ban, file sizes, comment ban, maps, ledgers, crate DAG, no Cargo change. | The commands in §4. | PROVEN | §4: every gate green, no plan-text assertion needed an update. `test_deep_filter_chain_crash_1.py` runs on the release wheel (§3). |
| C-011 | Speed: on release wheels built with the same command, head is not slower than base on any shape, and partitioned stays ≤ 0.18 s. The row threshold and the per-expression task grouping are set by measurement. The kernel race stays ≤ 1.02×. | Five interleaved rounds over base, head and S3+S1; a nine-sample partitioned re-measure; the kernel race median of 3; the threshold probe and a 1k/50k facade check (§5). | PROVEN | Medians: bare 0.3297 → 0.2888 s (−41 ms), explicit 0.4433 → 0.3256 s (−118 ms), partitioned 0.1365 → 0.1299 s (nine-sample 0.1363 → 0.1290). Kernel race head ≤ 1.010×. The threshold stays 16,384 and the grouping stays one task per expression. S3+S1: bare 0.2877, explicit 0.2523, partitioned 0.1621 (nine-sample 0.1417). §5. |

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

## 3. Facade identity record (2026-10-05, P-S3-6)

The slice is the first 50,000 rows of `test_futures.parquet`, read in place and written to the
session scratch area. Release wheels were built from separate worktrees with separate target
directories (the S2b stale-fingerprint lesson): base `978a3efd` and head `ec6807d2`. At 50,000
rows neither base plan has a RoundRobin; DataFusion adds none for that size. On the full file,
base has the two RoundRobins (L1/L2 and top) in each spelling, and head has none.

**S3 + S1** (`perf/ta-series-s3-with-s1` = `ec6807d2` + `b1337ead`, local only, release wheel
built): on the full file both spellings have no RoundRobin. Both are deterministic across two
runs, and the bare output is time-sorted. The explicit spelling is bit-identical to S3 head
alone. Its seeded `rand` frame keeps its RoundRobin, because the source is not a window and the
expression is volatile.

`test_deep_filter_chain_crash_1.py` on the head release wheel: 9 passed in 105 s.

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

## 5. Speed record (2026-10-05, S3 speed phase)

The orchestrator released the box with no other lanes and no cargo running. Background Airflow
and desktop processes held load1 at 2.9–3.4 during the rounds and 6.3–6.6 during the 1k/50k
check (recorded per round in the evidence). The wheels are the P-S3-6 release wheels: base
`978a3efd`, head `ec6807d2`, and S3+S1 `701d9870`. The venvs carry polars 1.43.1,
polars-runtime-32 1.43.1, polars_talib 0.1.6, numpy 2.5.3 and pyarrow 25.0.1. Each round is one
process per side, with one warm-up per shape and then one timed run, in the owner's
configuration (`target_partitions` 16, `batch_size` 64,000). Side order rotates every round.
Evidence is in `/tmp/oc-worker/direct/wo/ta-series/s3-evidence/speed/`.

| shape | base (s) | head S3 (s) | S3+S1 (s) | base median | S3 median | S3+S1 median |
|---|---|---|---|---|---|---|
| bare | 0.3217 0.3704 0.3297 0.3292 0.3683 | 0.3001 0.2971 0.2638 0.2621 0.2888 | 0.2741 0.2857 0.3380 0.2877 0.2882 | 0.3297 | 0.2888 | 0.2877 |
| explicit | 0.4806 0.4612 0.4386 0.4433 0.4238 | 0.3256 0.3180 0.3494 0.2822 0.3370 | 0.2453 0.2573 0.2234 0.2652 0.2523 | 0.4433 | 0.3256 | 0.2523 |
| partitioned | 0.1375 0.1356 0.1365 0.1351 0.1413 | 0.1455 0.1305 0.1299 0.1279 0.1202 | 0.1523 0.1647 0.1475 0.1621 0.1659 | 0.1365 | 0.1299 | 0.1621 |
| polars_talib | 0.1295 0.1256 0.1323 0.1372 0.1193 | 0.1319 0.1233 0.1318 0.1312 0.1148 | 0.1243 0.1208 0.1400 0.1331 0.1222 | 0.1295 | 0.1312 | 0.1243 |

- **S3 against base:** bare −41 ms (0.876×), explicit −118 ms (0.734×). The sketch's bound was
  about −70 ms on both, so bare falls short of it and explicit beats it. Head is faster on every
  shape. The ratio to polars_talib in the same rounds is 2.20 (bare) and 2.48 (explicit).
- **Partitioned, nine samples per process, five interleaved rounds** (`partitioned.jsonl`).
  Round medians: base 0.1655 0.1294 0.1363 0.1374 0.1235, head 0.1324 0.1373 0.1262 0.1232
  0.1290, S3+S1 0.1417 0.1391 0.1467 0.1449 0.1266. Median of medians: base 0.1363, head 0.1290,
  S3+S1 0.1417. All are ≤ 0.18 s.
- **S3+S1:** bare equals S3 alone (0.2877). Explicit is a further −73 ms (0.2523, ratio 2.03).
  Partitioned is +4 % against base on the nine-sample re-measure (0.1417 against 0.1363), and
  +19 % on the one-sample rounds. It stays under 0.18 s.
- **Kernel race** (`--quick`, three interleaved runs per side, `kernel_race/`). Head median
  against the previous lane: sma 0.008019 (0.920×), ema 0.008415 (0.909×), rsi 0.008555
  (0.981×), bbands 0.010488 (1.010×). Base in the same runs: sma 0.008241, ema 0.008804, rsi
  0.008597, bbands 0.010741.
- **Threshold and grouping** (`threshold/`).
  - A release-profile Rust probe evaluated 16 expressions, `x * c` (cheap) and `round(x * c, 4)`
    (heavy), serial against one task per expression with 16 permits, median of 21. Parallel
    loses only at 1,000 rows with cheap expressions (5.1×). It wins from 4,096 rows upward (cheap
    0.37×, heavy 0.43×), and by 16,384 rows it is 0.13–0.29×. The probe runs on glibc, not the
    wheel's mimalloc, so the serial side carries page-fault cost; the crossover is read
    conservatively.
  - Through the facade (median of 7 per process, three interleaved rounds), head is not slower
    than base at 1,000 rows (bare 14.98 against 15.55 ms, explicit 16.39 against 16.76 ms) or
    at 50,000 rows (bare 26.83 against 29.29 ms, explicit 30.38 against 32.15 ms).
  - **Kept:** `PARALLEL_PROJECTION_MIN_ROWS` = 16,384 (safe on both sides of the crossover).
    One task per expression is kept as well: even cheap expressions gain from 4,096 rows, so
    batching them would gain nothing. No code change, so P-S3-3 and the kernel race stand as
    measured.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ta-series-s3
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: R-S3-1a (as narrowed by Q-S3-3), R-S3-1b, R-S3-2..5, Q-S3-2 and the S1 note each map to a clause; every firing condition, the parent check, the flag, the volatility refusal and the one-batch test have a pin with a measured red.
      artifacts: [crates/repark-core/src/parallel_window/projection_tests.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Small batches take the serial path; multi-batch single-partition sources, a root chain, a two-projection chain, merge and coalesce parents, an unsatisfiable ordering and a partitioned hash join parent are exercised.
      artifacts: [crates/repark-core/src/parallel_window/projection.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Two failing expressions return the lowest index; a panicking task resumes its panic through join_unwind; the semaphore error maps to DataFusionError.
      artifacts: [crates/repark-core/src/parallel_window/projection.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Columns are placed by expression index; batches are processed one at a time in input order; dropping the stream aborts queued expression tasks; concurrency is bounded by min(tasks, target_partitions).
      artifacts: [crates/repark-core/src/parallel_window/projection_tests.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, network access or deserialization; the flag is the S2b carrier, settable only by the Rust builder.
    - id: AT-6
      status: ATTACKED
      evidence: Bit identity against serial ProjectionExec in Rust, against base through the facade on release wheels (50k slice and the full file explicit spelling), goldens unchanged, volatile expressions never moved.
      artifacts: [crates/repark-core/src/parallel_window/projection_tests.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Bare -41 ms and explicit -118 ms against base, partitioned unchanged, kernel race at most 1.010x, threshold and grouping measured at 1k, 4k-64k and 1M rows; multi-batch sources keep their fan-out so range() pool behaviour is unchanged.
      artifacts: [python/repark-parity/bench/ta/bench_kernel_race.py]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency or Cargo.lock change; SpawnedTask, tokio Semaphore and the S2b flag carrier are reused; the distributed provider refuses a session with the flag on, which covers both rules.
      artifacts: [crates/repark-core/src/session/df_guards.rs]
    - id: AT-9
      status: ATTACKED
      evidence: EXPLAIN prints ParallelProjectionExec with ProjectionExec's expression list; a parallel_batches counter metric records which batches took the parallel path.
      artifacts: [crates/repark-core/src/parallel_window/projection.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Ten pins, each red under its own mutation after the narrowing (m1..m10), the tree restored and the full suites green afterwards.
      artifacts: [crates/repark-core/src/parallel_window/projection_tests.rs]
  complete: true
```
