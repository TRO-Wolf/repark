# Unit ledger — TA-SINGLE-SERIES-PARALLEL-1 S2b · a one-partition window runs its expression groups in parallel

**Date:** 2026-10-04 · **Branch:** `perf/ta-series-s2b-parallel-window` · **Base:** `origin/main` `ac8a72da`
· **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** `/tmp/oc-worker/direct/wo/ta-series/s2b-order.md` (grade A-minus), with the design in the
series sketch §3.2 (design B) and §7. Orchestrator rulings on the executor's H7 halt (2026-10-04):
Q1 = A, a single-node opt-out on the core session (`ReparkSessionBuilder::parallel_single_partition`,
on by default, read by the rule from a `repark.parallel` config carrier; distributed sessions build
with it off; new pin P-S2b-10); Q2 = the glue commit goes first; Q3 = the `NljBuildSideExec`
Ballista encode gap is carded separately and not touched here.

**Why.** On one long series (the owner's 14-indicator `ta.*` level over 1 M rows) DataFusion's
`WindowAggExec` evaluates every window expression serially on one thread. This slice runs the
expression groups of such a node on the runtime's blocking pool and drops two copies from the TA
glue. Out of scope: the series rewrite, `null_prefix`, `series_order.rs` and the partition-index
input (S2a); the parallel projection (S3); fusion (S5); the ANSI guard (S0); the ordered cache (S1).

## PROPOSITION LEDGER — TA-SERIES-S2B — 2026-10-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | P-S2b-1: on the owner's 14-expression level-3 TA plan, `ParallelWindowExec` output equals the serial `WindowAggExec` output bit for bit, column names and order included. | `crates/repark-ta/tests/parallel_window.rs::parallel_window_bit_identical`; mutation: assemble columns in completion order. | PROVEN | Green: 50,000 rows × 17 columns, `to_bits` + validity equal; plan carries `ParallelWindowExec` with 14 expressions, the off session `WindowAggExec` with 14, no `BoundedWindowAggExec`. Red under the mutation: both TA tests fail at `parallel_window.rs:133` (`left: 9221120237041090560`, a NaN, against a finite value). §2. |
| C-002 | P-S2b-2: expressions with identical argument lists share one group, so multi-output siblings (BBANDS × 3, MACD/signal, STOCH k/d) run on one thread and the family computes once. | `parallel_window_keeps_multi_output_siblings` (core, probe UDF counting computes) and `parallel_window_multi_output_siblings_bit_identical` (TA); mutation: one group per expression index. | PROVEN | Green: groups `[[0, 1, 2], [3]]`, compute count 1, output bit-identical to serial; TA siblings interleaved with other indicators are bit-identical. Red under the mutation: `left: [[0], [1], [2], [3]]`. §2. |
| C-003 | P-S2b-3: the rule returns the plan unchanged for a `PARTITION BY` window, for a window whose input has more than one partition, and for a one-group window. | `parallel_window_skips_partitioned_and_single_group`; mutations: drop each of the three conditions. | PROVEN | Green on all four cases (three skips plus the qualifying control). Red: dropping the `partition_by` test fails at `tests.rs:409`, dropping the one-partition test at `:416`, dropping the two-group test at `:388`. §2. |
| C-004 | P-S2b-4: when several expressions fail, the error of the lowest expression index is returned, whatever finishes first. | `parallel_window_lowest_index_error` (index 0 fails after 300 ms, index 2 at once); mutation: first-to-finish error. | PROVEN | Green: the error names `probe failure first`. Red under the mutation: `Execution error: probe failure second`. §2. |
| C-005 | P-S2b-5: dropping the output stream cancels the outstanding work; a queued group never starts. | `parallel_window_drop_cancels` (one permit, group 0 blocked, stream dropped, group 0 released); mutation: run the driver on a detached `tokio::spawn`. | PROVEN | Green: the counting group ran 0 times after 300 ms. Red under the mutation: `left: 1, right: 0`. §2. |
| C-006 | P-S2b-6: the group tasks run on the runtime's threads, so a deep argument evaluates on the 32 MiB runtime stacks (`repark-python/src/deep_stack.rs`, `RUNTIME_THREAD_STACK_BYTES`). | `parallel_window_deep_arg_runtime_stack` (4,000-deep `x + 1.0`, runtime with 32 MiB thread stacks, remaining stack checked inside the evaluator); mutation: evaluate the group on a default-stack `std::thread`. | PROVEN | Green: 2,000 rows, remaining stack above 16 MiB. Red under the mutation: `thread '<unknown>' has overflowed its stack`, SIGABRT. §2. |
| C-007 | P-S2b-7: the zero-copy output (`float64_array_from_vec`) and the all-input borrow (`try_borrow_all_null_free`) give the same bits as the old copy-and-densify glue. | `udf::glue::tests::ta_glue_zero_copy_and_borrow_bit_identical` (ADX, TRANGE, ATR, WILLR, EMA, RSI, STOCH k and d; borrow pointers; moved buffer); mutation: borrow an off-by-one slice. | PROVEN | Green. Red under the mutation: `glue.rs:114` `left: 511, right: 512`. `udf/mod.rs` 1818 → 1801 lines; its exact baseline and the CAP-1 mirror ratchet to 1801. §2. |
| C-008 | P-S2b-8: through the facade, the owner's benchmark shape in both spellings, on a 50,000-row slice of the real file, gives output bit-identical to base (`origin/main` `ac8a72da`). | Probe on the release wheels of base and head (`probe.py`, `compare.py`); mutation: a release wheel built with completion-order assembly. | PROVEN | Green: bare and explicit, 50,000 rows × 28 columns, no difference in any column's bits, validity or values; head plans `ParallelWindowExec` with 14 (bare) and 15 (explicit) expressions. Red under the mutation wheel: bare differs in 9 columns (`ema5`, `rsi34`, `sma20`, `ETR5`, `ETR13`, `ETR21`, `ADX5`, `ADX13`, `ADX21`); explicit raises `column types must match schema types, expected Float64 but found UInt64`. §3. |
| C-009 | P-S2b-9: with the rule on, non-TA one-partition windows are bit-identical to the rule off. | `parallel_window_non_ta_windows_bit_identical` (four queries over three input batches); mutation: evaluate only the first input batch. | PROVEN | Green. `sum`/`avg`/`max`/`count OVER ()` and a whole-frame `sum`/`first_value` with `lag` fire; `lag`/`row_number`/running `sum` over `ORDER BY id` plans as `BoundedWindowAggExec` and is untouched. Red under the mutation: `left: 700, right: 2000`. §2. |
| C-010 | P-S2b-10: with `parallel_single_partition(false)` (or the carrier's `enabled = false`), a qualifying plan stays a `WindowAggExec`. Every `ReparkSession` the `repark-distributed` tests build sets it. | `parallel_window_flag_off_keeps_window_agg_exec`, `parallel_window_flag_off_session_keeps_window_agg_exec`; mutation: ignore the flag. | PROVEN | Green. Red under the mutation: both tests fail (`tests.rs:432`, `:601`). Ten distributed construction sites pass `false`; `repark-distributed --features cluster` matches base test for test (three tests red on both, §4). §2. |
| C-011 | The kernel layer is untouched: the 158 kernel and 13 prefix goldens hash to the sketch's value, and `bench_kernel_race.py --quick` stays within 1.02× of the previous lane's medians. | `sha256sum crates/repark-ta/tests/goldens/*.bin crates/repark-ta/tests/goldens/prefix/* \| sha256sum`; `cargo test -p repark-ta` and `--features datafusion --test prefix_goldens`; three interleaved race runs per side. | PROVEN | Hash `af74ab17a59b0452a92b03e085bb6e0106f8ae74bbd0ec9a32db01239f5e87a8`; 159 + 26 tests green. Race, head median of 3: sma 0.008279 (0.950×), ema 0.008386 (0.906×), rsi 0.008696 (0.997×), bbands 0.010447 (1.006×). §4. |
| C-012 | Speed: on release wheels built with the same command, head is faster than base on both owner spellings, and the partitioned shape is not slower than base and stays ≤ 0.18 s. | Five interleaved rounds per side (`round.py`), plus a nine-sample re-measure of the partitioned shape (`part.py`). | PROVEN | Bare 0.4501 → 0.3442 s (−106 ms); explicit 0.5720 → 0.4499 s (−122 ms). Partitioned: one sample per round gave 0.1335 → 0.1436 s; nine samples per round gave 0.1346 → 0.1322 s (pooled 45 samples: 0.1320 → 0.1321). §4. |
| C-013 | EXPLAIN prints `ParallelWindowExec: wdw=[…]` with the expression list `WindowAggExec` prints; the plan-shape facade pins that count window operators count both names, and nothing else in the facade or parity suites changes. | `parallel_window_flag_off_session_keeps_window_agg_exec` (the EXPLAIN line); facade files naming `ta`/`Window`/`over(`/`window`, `-n 8`; `python/repark-parity` `-k "ta or window"`. | PROVEN | 2,068 facade tests: 8 plan-count assertions failed before the update and pass after (`test_n2_plan_collapse.py` × 6, `test_ta.py` × 2); parity 276 passed. §4. |

## 1. R-S2b-6 record (2026-10-04)

Measured, not assumed: `ReparkSessionProvider::from_session` clones `session.context().state()`,
and every plan the Ballista tests submit is created on that session's own context
(`multi_stage.rs:108`), so stripping the rule from the provider would not be enough. The executor
halted (H7). The orchestrator ruled Q1 = A. The flag lives in a `ConfigExtension` with prefix
`repark.parallel`, shaped like `repark.ansi`: `SET` refuses it, it never lists, and an absent
carrier means on. `ParallelWindowRule::optimize` returns the plan unchanged when it is off.

## 2. Pin and mutation record (2026-10-04)

Every mutation was applied by a script that saved the source, rebuilt the pin's test binary and
restored the source (a stale-binary trap from the restored mtime was caught and fixed: the
restored files are touched, and `cargo test -p repark-core --lib` then ran 981 green). P-S2b-1's
mutation broke both TA tests; P-S2b-3's three mutations each broke the skip test at a different
assertion; P-S2b-6's mutation aborted the process with a stack overflow, as expected on a
default 2 MiB thread.

## 3. Facade identity record (2026-10-04, P-S2b-8)

The 50,000-row slice is the first 50,000 rows of `test_futures.parquet` (read in place, written
to the session scratch area). The owner's `load_repark` sorts and `.eager()`s it; at 50,000 rows
that is one 64,000-row batch, so the bare spelling's row order is deterministic. The first
mutant wheel came out identical to base: building base with the head clone's
`CARGO_TARGET_DIR` left release fingerprints that pointed at the base worktree, so cargo
called the mutated head "fresh". The workspace members' release artifacts were cleaned and the
mutant rebuilt; its plans then showed `ParallelWindowExec`, and it went red. The head wheel
predates the base build and carries `ParallelWindowExec` (plan check), so the green result
stands.

## 4. Gates (2026-10-04, head)

- Goldens: hash above; `cargo test -p repark-ta` 106 + 11 + 41 + 1; `--features datafusion`:
  151 lib, 2 `parallel_window`, 26 `prefix_goldens`.
- Units: `cargo test -p repark-core --lib` 981 passed, 1 ignored (9 in `parallel_window`);
  `cargo test -p repark-python --lib` 119 passed; `repark-spark --test ta_window` 10 and
  `repark-sql --test ta_toll` 6 passed (their `WindowAggExec` counts are `PARTITION BY` plans).
- Distributed (`--features cluster`, head and base): 6 + 3/4 + 10/11 + 8 + 3 + 4/5. The same
  three tests fail on base `ac8a72da`: `cancel_mid_flight_…`, `session_spill_dir_…` (Ballista
  cannot encode `range()`'s `LazyMemTableExec`), and `date_and_timestamp_predicates_…`.
- Facade, debug build in a fresh venv: 70 files, `-n 8`, 2,060 passed / 8 failed / 117 skipped
  before the plan-count update; all 8 green after it. Parity `-k "ta or window"`: 276 passed
  after `test_ta.py` kept its 1,020-line exact baseline (an extra helper first broke CAP-1).
- Kernel race (`--quick`, release, interleaved, load1 6.2): per run head / base, s:

  | run | sma | ema | rsi | bbands |
  |---|---|---|---|---|
  | head 1 | 0.008419 | 0.008840 | 0.008954 | 0.010447 |
  | head 2 | 0.008279 | 0.008360 | 0.008696 | 0.010240 |
  | head 3 | 0.008024 | 0.008386 | 0.008574 | 0.010464 |
  | base 1 | 0.008077 | 0.007702 | 0.008886 | 0.010417 |
  | base 2 | 0.008446 | 0.008215 | 0.008723 | 0.010531 |
  | base 3 | 0.008455 | 0.008053 | 0.008542 | 0.010568 |

- Speed: release wheels built with `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 maturin build --release`,
  one venv per side (polars 1.43.1, polars-runtime-32 1.43.1, polars_talib 0.1.6, numpy 2.5.1),
  owner conf (`target_partitions` 16, `batch_size` 64,000), one warm-up per shape per process,
  then one timed run; order alternates by round; load1 5.05–5.70 throughout (above 2, recorded).

  | shape | base rounds (s) | head rounds (s) | base median | head median |
  |---|---|---|---|---|
  | bare | 0.4521 0.4633 0.4501 0.4489 0.4288 | 0.3628 0.3428 0.3442 0.3070 0.3499 | 0.4501 | 0.3442 |
  | explicit | 0.5690 0.5720 0.5591 0.6004 0.5819 | 0.4315 0.4499 0.4243 0.4501 0.4549 | 0.5720 | 0.4499 |
  | partitioned | 0.1302 0.1400 0.1364 0.1174 0.1335 | 0.1436 0.1409 0.1349 0.1568 0.1530 | 0.1335 | 0.1436 |
  | polars_talib | 0.1265 0.1303 0.1305 0.1598 0.1098 | 0.1329 0.1250 0.1252 0.1207 0.1161 | 0.1303 | 0.1250 |

  The partitioned plan does not change (its level 3 is still `WindowAggExec`, plan check). The
  1.08× in the one-sample rounds is spread: single samples range 0.117–0.157 s. Re-measured
  with nine samples per process, five interleaved rounds: base round medians 0.1284 0.1353
  0.1232 0.1399 0.1346, head 0.1285 0.1322 0.1358 0.1379 0.1300; median 0.1346 against 0.1322
  (0.982×); pooled 0.1320 against 0.1321.
- Hygiene: workspace clippy `--all-targets -D warnings -A clippy::disallowed_methods`,
  `cargo fmt --all --check`, `make rust-panic-ban`, `check_rust_file_size.py`, `check_lib_rs.py`
  (`built_with_debug_assertions` moved unchanged to `runtime.rs` to hold the 155 ceiling),
  `check_lib_py.py`, `check_crate_dag.sh`, `git diff --exit-code origin/main -- Cargo.lock
  '**/Cargo.toml'`, the comment-ban scan and the map check, all clean.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ta-series-s2b
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each ruling R-S2b-1 to R-S2b-8 maps to a clause above; the firing condition, grouping, execution, error, stack, single-node and EXPLAIN rulings each have a pin with a measured red.
      artifacts: [crates/repark-core/src/parallel_window/tests.rs, crates/repark-ta/tests/parallel_window.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Empty input emits no batch as WindowAggExec does; NULL inputs keep the densify path; multi-batch input, PARTITION BY over one partition, a three-partition input and a one-group node are exercised.
      artifacts: [crates/repark-core/src/parallel_window/exec.rs, crates/repark-ta/src/udf/glue.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Two failing expressions return the lowest index; a group stops at its first error; a panicking task resumes its panic through join_unwind, as the serial loop would (by construction, not pinned).
      artifacts: [crates/repark-core/src/parallel_window/exec.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Columns are placed by expression index, never by completion; the thread-local sibling cache stays on one thread per group; dropping the stream aborts queued tasks and the semaphore bounds concurrency to min(groups, target_partitions).
      artifacts: [crates/repark-core/src/parallel_window/tests.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, network access or deserialization; the carrier refuses SET and is set only by the Rust builder.
    - id: AT-6
      status: ATTACKED
      evidence: Bit identity against serial windows in Rust, against base through the facade on the real file, the golden hash unchanged and the kernel layer untouched.
      artifacts: [crates/repark-ta/tests/parallel_window.rs, crates/repark-ta/tests/goldens.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Both spellings drop about 106 and 122 ms; the kernel race is within 1.02; the partitioned shape is unchanged on the nine-sample re-measure; one concatenated batch is held as WindowAggStream already does.
      artifacts: [python/repark-parity/bench/ta/bench_kernel_race.py]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency or Cargo.lock change; SpawnedTask and tokio Semaphore come from existing direct dependencies; distributed sessions build with the flag off because the codec has no arm for the new node.
      artifacts: [crates/repark-distributed/map.md, crates/repark-core/src/session.rs]
    - id: AT-9
      status: ATTACKED
      evidence: EXPLAIN names ParallelWindowExec with WindowAggExec's expression list; the exec reports baseline metrics (elapsed compute, output rows).
      artifacts: [crates/repark-core/src/parallel_window/exec.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Ten pins, each red under its own mutation (twelve mutations in all, recorded in section 2 and the clause rows), the tree restored and the full suites green afterwards.
      artifacts: [crates/repark-core/src/parallel_window/tests.rs, crates/repark-ta/src/udf/glue.rs]
  complete: true
```
