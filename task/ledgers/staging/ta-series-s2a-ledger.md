# Unit ledger — TA-SINGLE-SERIES-PARALLEL-1 S2a + D · a bare `ta.*` column is a series over the resolved order, with a native `null_lookback`

**Date:** 2026-10-05 · **Branch:** `perf/ta-series-s2a-series-order` · **Base:** `74b0900a`
(S3, PR #945) · **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** `/tmp/oc-worker/direct/wo/ta-series/s2a-order.md` (grade A-minus), with the design in the
series sketch §0.6, §1.2–§1.5, §3.2, §7.1, §7.3 and §9. **Owner, 2026-10-04:** "I think your
defaults are all good", which accepts Q1 (order (a) → (b) → (c), no refusal, a once-per-session
warning and the docs note), Q2 (within (b) the first `TIMESTAMP` / `TIMESTAMP_NTZ` column, `DATE`
only without one), Q5 (native `null_lookback` for both spellings) and Q7 (single node only).

**Why.** Today a bare `ta.*` column (no `.over(...)`) is an `OVER ()` window: its answer depends on
the order a split eager read happens to deliver (wrong on the owner's frame, sketch §1.2), and it
ignores `null_lookback=True`. This slice binds it as the explicit `.over(Window.orderBy(K))`
spelling, by construction, and makes `null_lookback` a property of the window function. Out of
scope: S1 (#944, held), S5 fusion, any kernel, any new crate.

## PROPOSITION LEDGER — TA-SERIES-S2A — 2026-10-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | P-S2a-1: the owner's levels (L1 `ema21` / `test_21`, L2 `tr`, L3 indicators including `ETR13` with `null_lookback=True`, L4 post-columns), bare against `.over(Window.orderBy(ts))`, are `to_bits`-equal in every column, on a lazily sorted frame (case (a)) and on a sorted eager frame (case (b) on this base). | `python/repark/tests/test_ta_series.py::test_series_equals_explicit_orderby` (both legs); mutation: resolve K as DESC (`series.rs` builds `!key.asc`). | PROVEN | Green: 4,000 rows × 25 columns, bits and validity equal, `ETR13` 12 NULL + 1 NaN. Red under the mutation: both legs fail, `ema21 bits` (`test_ta_series.py:121`). §2. |
| C-002 | P-S2a-2: a frame sorted by a later timestamp column is ordered by that column, without a warning; (a) wins over (b). The declared order is found through `Projection` (plain or aliased column), `Filter`, `SubqueryAlias`, `Window` and `Limit`, down to a `Sort` (aliased keys included) or a `MemTable` scan with a `sort_order`; a dropped key, a computed key or any other node falls through. | `crates/repark-core/src/series_order/tests.rs::series_order_declared_beats_temporal`, `series_order_declared_lost_falls_through`; `test_ta_series.py::test_series_order_declared_beats_temporal`; mutation: try (b) before (a). | PROVEN | Green. Red under the mutation: `series_order/tests.rs:96`, `left: FirstTemporal("ts1") right: Declared`. §2. |
| C-003 | P-S2a-3 / 3b: with no declared order, a schema with a `DATE` column first and a `TIMESTAMP` later resolves to the `TIMESTAMP` (Q2); with no timestamp, the first `DATE`. The bare answer equals the explicit spelling over that column. One `UserWarning` per session, with the sketch §1.3 text, across `withColumn`, `withColumns` and `select`; `spark.conf.set` keeps the flag. | `series_order_timestamp_before_date`, `series_order_date_fallback`, `series_order_notice_claims_once_per_session` (core); `test_series_order_timestamp_before_date_and_warns_once`, `test_series_order_date_fallback` (facade); mutations: the first temporal column of any kind; warn on every call. | PROVEN | Green. Red: any-kind → `series_order/tests.rs:178` and `:161` (`FirstTemporal("day")`); every call → `test_ta_series.py:197`, two extra warnings. §2. |
| C-004 | P-S2a-4: with no order and no temporal column, the bare window stays `OVER ()` and `ParallelWindowExec`'s `InputOrder::PartitionIndex` arm drops the `CoalescePartitionsExec` and reads its input's partitions in index order: deterministic, equal to source order. Aggregate, mixed and fetching-coalesce `OVER ()` windows keep today's plan. | `crates/repark-core/src/parallel_window/tests.rs::series_order_current_row_order_partition_index` (four partitions finishing in reverse order), `partition_index_keeps_aggregate_over_coalesce`; `test_ta_series.py::test_bare_ta_current_row_order_reads_partitions_in_index_order`; mutations: read through `CoalescePartitionsExec` (arm disabled; arm kept but reading through a coalesce). | PROVEN | Green, three runs in source order. Red: arm disabled → `tests.rs:717` (`Single` vs `PartitionIndex`); coalesce read → `tests.rs:734`, ids `300…399, 200…, 100…, 0…99`. §1, §2. |
| C-005 | P-S2a-5: native `null_lookback` gives exactly the NULL and NaN cells of base's `row_number() > lookback` + CASE output, per window partition, in the explicit and the bare spelling and through `with_indicators(null_lookback=True)`: `ETR13` is 12 NULL + 1 NaN per symbol; values are unchanged. | `crates/repark-ta/tests/null_prefix.rs::null_lookback_native_matches_row_number_rewrite`; `test_ta_series.py::test_null_lookback_native_matches_row_number_rewrite`; mutation: apply the prefix to the whole frame, not per partition. | PROVEN | Green. Red under the mutation: `null_prefix.rs:130` (`partitioned validity`) and `:278` (`[("CL", 12, 1), ("ES", 0, 13)]`). §2. |
| C-006 | P-S2a-6: `ema(tr, 13)` with and without the prefix in one `select` stay two columns (NaN prefix and NULL prefix); prefixed and plain instances differ in equality and in hash, and two prefixes differ from each other. | `null_prefix.rs::null_prefix_instances_never_cse`; mutations: drop `null_prefix` from `Hash`; drop it from `PartialEq`. | PROVEN | Green. Red: Hash → `null_prefix.rs:242` (`10466024109604911595` twice); PartialEq → `:236` and the CASE comparison. §2. |
| C-007 | P-S2a-7: a series `ta.*` and a partitioned `ta.*` in one `withColumns` are each bit-equal to computing them alone, at 1 and at 16 shuffle partitions; the partitioned operator stays `WindowAggExec`, the series one becomes `ParallelWindowExec`. | `test_ta_series.py::test_mixing_series_and_partitioned_window[1, 16]`; mutation: let the rule fire on the partitioned operator (drop the `partition_by` test). | PROVEN | Green. Red under the mutation: both parametrizations, the `PARTITION BY` node printed as `ParallelWindowExec`. §2. |
| C-008 | P-S2a-8: SQL `ta_*() OVER (…)` answers are unchanged: `ta_ema(close, 13) OVER (PARTITION BY sym ORDER BY ts)` equals the kernel per symbol, NaN prefix, no NULL; no registered name carries a prefix. | `null_prefix.rs::sql_ta_unchanged`; `repark-spark --test ta_window`, `repark-sql --test ta_toll`; mutation: register the prefixed UDF in SQL. | PROVEN | Green. Red under the mutation: `null_prefix.rs:286`. §2, §3. |
| C-009 | Gates: goldens byte-identical; `repark-core`, `repark-ta` (with and without `datafusion`), `repark-python` (lib and `bindings`), `repark-spark` `ta_window`, `repark-sql` `ta_toll` green; facade 111 files equal to base apart from the new file; parity `-k "ta or window or projection"` 276; clippy, fmt, panic ban, file sizes, comment ban, maps, ledgers, crate DAG, no Cargo change, docs links. | The commands in §3. | PROVEN | §3. |
| C-010 | Correctness on the owner's real file (release wheels): sorted eager frame, the bare spelling equals the explicit one bit for bit and polars_talib 0.1.6 with 0 differing rows; unsorted lazy read, the bare spelling resolves (b) `event_timestamp_utc` and equals the explicit spelling over `Window.orderBy("event_timestamp_utc")`; the explicit spelling equals base on both frames. | `s2a-evidence/owner/owner_run.py` + `compare.py` on the base and head release wheels. | PROVEN | 1,000,000 rows × 28 columns, every comparison bit-identical; 17 TA columns against polars_talib with 0 differing values and 0 NULL/NaN mismatches; the bare output is time-sorted as produced. §4. |
| C-011 | Speed on the stacked branch (release wheels, interleaved, median of 5, uptime every round): H-a head bare ≤ 1.03 × base-main bare; head against base-now on both spellings and the partitioned shape; head's ratio to polars_talib ≤ 1.00 on both spellings; partitioned ≤ 0.18 s; kernel race ≤ 1.02×. | `s2a-evidence/speed/rounds.sh` (`round.py`, the S3 harness: one warm-up and one timed run per shape per process) and `rounds9.sh` (`round9.py`, nine alternating samples per process); `s2a-evidence/kernel_race/`. | PROVEN | Bare 0.1190 s against base-main 0.1677 (0.71×; nine-sample 0.65×); polars ratio bare 0.86, explicit 0.68 (nine-sample 0.75, 0.74); explicit 0.0941 against base-now 0.0933 (1.009×); partitioned 0.1187 s. Kernel race re-measured, all ≤ 1.0× (§6). |
| C-013 | P-S2a-9 (orchestrator ruling 2026-10-05, S2a stacked on S1): on the combined branch a sorted eager frame carries its declared order through S1's ordered cache, so the owner's levels bare resolve by (a) with no warning and equal the explicit spelling over the declared key bit for bit. | `test_ta_series.py::test_series_order_sorted_eager_frame_is_declared` (sorted by a later timestamp column `booked`, then `.eager().lazy()`); mutation: drop the `MemTable` `sort_order` arm. | PROVEN | Green on `perf/ta-series-s2a-on-s1`. Red under the mutation (`m9_no_memtable_arm`): the frame falls to (b) and warns naming `event_timestamp_utc`. §5. |
| C-012 | Docs (D): `docs/guide/ta-guide.md` opens "The shape" with the sketch §9 note (Q2 wording), replaces the "Ordering is yours to supply" bullet and describes the native prefix; `ta.py`'s module docstring carries the same words. | `make check-docs-links`; review. | PROVEN | Commit `f660041c`; docs links clean (1,295 files). The "(also through `.eager()`, `.cache()` and `localCheckpoint`)" clause holds once S1 (#944) lands; on this base those frames resolve by (b) with the warning (§4). |

## 1. Design record (2026-10-05)

- **The resolver** (`repark-core/src/series_order.rs`) returns qualified columns of the frame's
  own schema, so the rewrite inserts exactly the column a bound `col(name)` would be. The facade's
  `sort` binds its keys as `?table?.ts AS ts`, so the `Sort` arm strips aliases (found on the first
  facade run; pinned in the core test with an aliased key).
- **The rewrite** (`repark-python/src/column/series.rs`) runs at both bind choke points after
  binding and calls `build_over_expression` with `OverSpec { order_by: keys, .. }`, the builder
  `.over(Window.orderBy(K))` uses, so the bare plan is the explicit plan: measured on a parquet
  frame, the two physical plans print identically.
- **The notice** is a `repark.series` `ConfigExtension` holding an `Arc<AtomicBool>`; cloned
  configs share it, a new session gets a fresh one, and `spark.conf.set` keeps it (measured).
- **Native `null_lookback`.** `TaWindowUdf` gains `null_prefix` and a derived `display` name.
  `PartialEq` / `Hash` (in `udf/glue.rs`) cover the base name, function, signature and prefix,
  not the derived name, so the hash pin can see the prefix. The evaluator is wrapped
  (`NullPrefixEvaluator`) only when the prefix is non-zero, which keeps the 40 `TaEvaluator`
  literals in `udf/mod.rs` tests untouched and the file at 1791 lines (baseline 1801 → 1791).
  The sketch's `float64_array_from_vec(values, null_prefix)` shape became a wrapper that sets
  the validity bitmap over the evaluator's output buffer: same rule, no extra copy.
- **The partition-index arm** fires only when every expression is an unordered, unpartitioned
  window UDF over a coalesce without a fetch. Through the facade only case (c) reaches it,
  because `.over(...)` without `ORDER BY` is refused for every window UDF. Through SQL, an
  unordered window UDF planned as a `WindowAggExec` (for example `ta_*() OVER ()`) also reads in
  index order: deterministic, otherwise unchanged. Aggregates and mixed nodes keep the S2b arm
  over the coalesce (two or more groups) or stay `WindowAggExec`.
- **Measured caveat on (c).** Index order is source order when the partitions are laid out in
  source order. A four-file directory read listed its file groups out of path order once in 30
  fresh sessions (partitions `0, 1, 3, 2`): the order is then deterministic for that plan but not
  the path order. An eager frame stores the order its materializing read delivered. The facade
  test uses a union of four single-file reads, whose partition order is fixed.

## 2. Pin and mutation record (2026-10-05)

`/tmp/oc-worker/direct/wo/ta-series/s2a-evidence/mutations/mutate.py` applies a spec, runs the
pin's command through the build lock (the facade ones rebuild with `make develop`), restores the
source and touches it. Outputs are `m*.out` beside the specs. Eleven mutations, all red:

| Mutation | Pin | Red |
|---|---|---|
| `m1_desc` | P-S2a-1 | both legs, `ema21 bits` |
| `m2_b_before_a` | P-S2a-2 | `FirstTemporal("ts1")` vs `Declared` |
| `m3_any_temporal` | P-S2a-3 | `FirstTemporal("day")` twice |
| `m3w_every_call` | P-S2a-3 | two extra `UserWarning`s |
| `m4_coalesce` | P-S2a-4 | `Single` vs `PartitionIndex` |
| `m4b_coalesce_read` | P-S2a-4 | ids in completion order |
| `m5_whole_frame` | P-S2a-5 | the second symbol has 0 NULL, 13 NaN |
| `m6a_hash` | P-S2a-6 | equal hashes |
| `m6b_eq` | P-S2a-6 | equal instances, CASE comparison breaks |
| `m7_partitioned` | P-S2a-7 | `PARTITION BY` node becomes `ParallelWindowExec` (1 and 16) |
| `m8_sql_prefixed` | P-S2a-8 | a registered prefixed name |

After the facade mutations the extension was rebuilt and `test_ta_series.py` ran 9 passed.

## 3. Gates (2026-10-05, head `6b905539`)

All cargo and maturin commands ran under the opus-cargo flock in `repark.slice`.
- Goldens: hash `af74ab17a59b0452a92b03e085bb6e0106f8ae74bbd0ec9a32db01239f5e87a8`, unchanged.
  `cargo test -p repark-ta`: 106 + 11 + 41 + 1. `--features datafusion`: 151 lib, 11 contract,
  41 goldens, 3 `null_prefix`, 1 microbench, 2 `parallel_window`, 26 `prefix_goldens`.
- `cargo test -p repark-core --lib`: 999 passed, 1 ignored. `repark-python --lib`: 119;
  `--test bindings`: 25. `repark-spark --test ta_window`: 10. `repark-sql --test ta_toll`: 6.
  The first `repark-python --lib` run failed `grown_stack_guard_rejects_bypass_sites` on a raw
  key clone in `series.rs`; the keys now clone through `deep_stack::grown_clone_expr`.
- Facade, debug build in the clone's venv, `-n 8`, 111 files: S3's 110 plus
  `test_ta_series.py` (every file naming `null_lookback`, `ta.` or `TA.` was already in the set).
  3,256 passed, 135 skipped, 103 xfailed, 0 failed. S3 recorded 3,247 / 135 / 103 on the 110
  files at its head, so head equals base apart from the nine new tests; no plan-text assertion
  changed. One chunk run killed by its own 290 s timeout had printed one `F` before the kill; the
  same chunk then ran twice to completion with no failure (1,155 passed, 93 skipped, 102 xfailed).
- Parity `python/repark-parity/tests -k "ta or window or projection"`: 276 passed.
- Hygiene: workspace clippy `--all-targets -D warnings -A clippy::disallowed_methods` (a
  `type_complexity` finding fixed with a named type), `cargo fmt --all --check`,
  `make rust-panic-ban`, `check_rust_file_size.py`, `check_lib_rs.py` (`lib.rs` 153),
  `check_lib_py.py`, comment ban `hits=0`, `check_map_md.sh`, the crate DAG,
  `git diff --exit-code origin/main -- Cargo.lock '**/Cargo.toml'`, `make check-docs-links`.

## 4. Owner-file correctness (2026-10-05, release wheels)

Release wheels were built with `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 uvx maturin@1.14.1 build
--release` from separate worktrees with separate target directories: base `74b0900a`, head
`6b905539`. One venv per side (polars 1.43.1, polars-runtime-32 1.43.1, polars_talib 0.1.6, numpy
2.5.3, pyarrow 25.0.1); the owner's configuration file (`target_partitions` 16, `batch_size`
64,000); `test_futures.parquet` read in place. Evidence: `s2a-evidence/owner/` (`owner_run.py`,
`compare.py`, `compare.json`, `plans-base.txt`, `plans-head.txt`).

- **Sorted eager frame** (the owner's `load_repark`: select, `sort(event_timestamp_utc)`,
  `.eager().lazy()`). The bare spelling resolves by (b) on this base, because the eager cache
  does not declare its order until S1 (#944); it warns once, naming `event_timestamp_utc`. Bare
  equals explicit bit for bit in all 28 columns; the bare output is time-sorted as produced.
- **Against polars_talib 0.1.6** (`benchmark_script.py`'s polars shape over the same sorted
  frame, rows aligned on the unique timestamp): `ema21`, `tr`, `ema5`, `rsi13`, `rsi21`, `rsi34`,
  `sma10`, `sma20`, `sma34`, `ETR5`, `ETR13`, `ETR21`, `DIP21`, `lr5`, `ADX5`, `ADX13`, `ADX21`:
  0 differing values (exact), 0 NULL/NaN mismatches. The owner's two scripts place `round(4)`
  differently on `ETR5` and `ETR21`; the comparison uses RePark's placement for both sides
  (with the scripts' own placement, those two columns differ by rounding only).
- **Unsorted lazy read** (no sort): the bare spelling resolves (b) `event_timestamp_utc`, warns
  with that name, and equals the explicit spelling over `Window.orderBy("event_timestamp_utc")`
  bit for bit; output time-sorted.
- **The explicit spelling did not move (H-b):** head equals base bit for bit on the sorted and on
  the unsorted frame, `ETR13` (native prefix now) included.
- **Plans (EXPLAIN only, no timing):** head bare and head explicit have the same operators
  (one `SortExec`, two `SortPreservingMergeExec`, one `ParallelWindowExec`, two
  `WindowAggExec`, two `ParallelProjectionExec`); base bare had no sort and two
  `CoalescePartitionsExec`. The bare spelling now pays the explicit spelling's sort on this
  base: the sketch §0.6 sequencing trap, which S1 removes. Timing is the orchestrator's next step.
- `test_deep_filter_chain_crash_1.py` on the head release wheel: 9 passed in 104 s.

## 5. Stacked on S1 (2026-10-05, orchestrator ruling)

**Ruling:** S2a ships stacked on S1 (#944) and S3 (#945); the H-a gate compares the combined
head's bare spelling with `origin/main`'s bare spelling. Local branch `perf/ta-series-s2a-on-s1`
= `6d1cc2a7` + `origin/fix/ta-series-s1-ordered-cache` (`b1337ead`), merged without conflicts
(only `map.md` files auto-merged); `origin/perf/ta-series-s3-parallel-projection` had not moved
(`74b0900a`, already in the base). S1 stamps `MemTable::sort_order` with plain
`Column::from_name` keys, which the resolver's `MemTable` arm reads through the eager frame's
projection, so the docs note's "(also through `.eager()`, `.cache()` and `localCheckpoint`)"
clause holds on the combined branch.

- Pins: `test_ta_series.py` 10 passed (P-S2a-9 new) and no `ta.* series` warning remains in the
  file (the eager leg of P-S2a-1 now resolves by (a)); S1's
  `test_ta_series_s1_ordered_cache.py` 6 passed, 3 xfailed (its strict follow-up xfails);
  P-S2a-9 red under `m9_no_memtable_arm`.
- `cargo test -p repark-core --lib`: 1,012 passed, 1 ignored. `repark-ta --features datafusion`
  `null_prefix` 3, `parallel_window` 2, `prefix_goldens` 26.
- Facade, the 111 files plus S1's file, `-n 8`: 3,263 passed, 135 skipped, 106 xfailed,
  0 failed (S2a alone 3,256 / 135 / 103, plus S1's 6 passed and 3 xfailed and P-S2a-9).

**Speed preparation (stacked; nothing timed).** Release wheels, `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
uvx maturin@1.14.1 build --release`, each in its own worktree and target directory: **base-main**
= `origin/main` `c4c363e2` (S0 + S2b + S0b + GSG); **base-s3s1** = `74b0900a` merged with
`b1337ead` (local merge `17a3af35`, no S2a; the S3 lane's `perf/ta-series-s3-with-s1` predates
S3's merge of main, so it was not reused); **head** = `perf/ta-series-s2a-on-s1` `894a9f33`.
EXPLAIN on the owner's shapes (`s2a-evidence/owner/plans-{base-main,base-s3s1,head-on-s1}.txt`):
head's sorted-eager bare plan equals its explicit plan and base-s3s1's explicit plan (one
`SortPreservingMergeExec`, no `SortExec`, `ParallelWindowExec` + two `WindowAggExec`, two
`ParallelProjectionExec`); base-main's bare plan has two `CoalescePartitionsExec` and two
RoundRobins and no sort. On the head wheel the owner's sorted eager frame resolves by (a) with
no warning, bare equals explicit bit for bit, and explicit equals base-s3s1 bit for bit.

**Owner-file outputs.** The nine 1,000,000-row Arrow outputs written for §4 and for this check
(base and head explicit, sorted and unsorted; head bare, sorted and unsorted; on S1: head bare
and explicit, base-s3s1 explicit) all hash to
`9d6e743b5cd5356f50b7493fcd737c4dbcdf501eca4b358afeaa8a57de9ee8dd`: one table, byte for byte
(`s2a-evidence/owner/arrows.sha256`). The files (788 MB) were then deleted.

## 6. On main, and the speed record (2026-10-05)

**Merge.** `origin/main` `538771bd` (S3 #945 squashed with the tp1 fold; S1 #944 squashed
with verifier fold `d798d0d9`, V944-1 and V944-2) merged into `perf/ta-series-s2a-on-s1` as
`8c8a655e`. Exactly six paths conflicted: `projection.rs`, `projection_tests.rs`,
`temp_views.rs`, `ordered_cache.rs`, `test_ta_series_s1_ordered_cache.py` and the S3 ledger.
The branch held the S3 or S1 branch head of each byte for byte, and no S2a commit touches them.
The executor halted on the product-Rust conflicts; orchestrator ruling: take main's version of
the six. Afterwards `git diff origin/main` over the six is empty, and the branch's diff over
main is S2a only (35 paths).

**Re-runs on the merged branch.** `test_ta_series.py` 10 passed (P-S2a-9 included) and S1's
facade file 7 passed, 3 xfailed; `repark-core --lib` 1,015 passed, 1 ignored; `repark-ta`
106 + 11 + 41 + 1 and, with `datafusion`, 151 + 11 + 41 + 3 `null_prefix` + 1 + 2 + 26; golden
hash unchanged; comment ban `hits=0`.

**Wheels** (same command, own worktree and target each; one venv each with polars 1.43.1,
polars-runtime-32 1.43.1, polars_talib 0.1.6, numpy 2.5.3, pyarrow 25.0.1): base-main
`c4c363e2` (kept from §5, sha256 `69a458c3…`), base-now `538771bd` (`87964362…`), head
`8c8a655e` (`3008833a…`).

**Owner file on the head wheel.** Sorted eager: bare resolves (a), no warning, bit-identical to
explicit; unsorted lazy: (b) with the warning, bit-identical to the explicit spelling; 17 TA
columns against polars_talib 0.1.6: 0 differing values, 0 NULL/NaN mismatches. The four outputs
hash to the same `9d6e743b…` as every earlier output (`owner/arrows.sha256`), then deleted.

**Speed** (`s2a-evidence/speed/rounds.jsonl`, the S3 harness; side order rotated each round;
load1 5.5–7.7, mostly the host's Airflow DAG processor):

| median of 5 (s) | base-main | base-now | head |
|---|---|---|---|
| bare (`benchmark_script`) | 0.1677 | 0.0870 | 0.1190 |
| explicit (`benchmark_window_orderby`) | 0.2798 | 0.0933 | 0.0941 |
| partitioned | 0.1073 | 0.1315 | 0.1187 |
| polars_talib (same processes) | 0.1307 | 0.1255 | 0.1381 |

- **H-a:** head bare / base-main bare = 0.71. Pass.
- **Card gate, ratio to polars_talib (head's own rounds):** bare 0.86, explicit 0.68. Pass.
- **Head against base-now:** explicit 1.009×, partitioned 0.90×, bare 1.37×. The one-sample
  bare numbers carry a first-position effect (the bare shape is timed first in each process; head
  bare ranged 0.085–0.202 s while head explicit, the same plan, stayed at 0.094). base-now's bare
  spelling is the old unordered `OVER ()` over a `CoalescePartitionsExec`, which the series
  rewrite replaces with the ordered plan.
- **Nine samples per process** (`rounds9.jsonl`, alternating shape order, median per process,
  then median of 5): bare base-main 0.1490, base-now 0.0922, head 0.0969 (head/base-main 0.65,
  head/base-now 1.051); explicit 0.2789 / 0.0953 / 0.0955 (head/base-now 1.002); partitioned
  0.1031 / 0.1306 / 0.1238; polars 0.1274 / 0.1269 / 0.1288; head ratio to polars bare 0.75,
  explicit 0.74.
- **Partitioned** stays ≤ 0.18 s and is faster than base-now. It is slower than base-main
  (1.11×; nine-sample 1.20×), and so is base-now (1.23× / 1.27×): the change comes with S3/S1
  on main, not with S2a.
- **Kernel race** (`--quick`, release, interleaved with base-now; `kernel_race/`): the first
  three runs per side (load1 8–11) gave head medians sma 0.008986 (1.031× the previous lane's
  0.008716), ema 0.008661, rsi 0.008449, bbands 0.010201. Five further interleaved runs gave
  sma 0.008078 (0.927×), ema 0.008061 (0.871×), rsi 0.008393 (0.963×), bbands 0.010402
  (1.001×); base-now in the same runs 0.008067, 0.008556, 0.008600, 0.010556 (head ≤ 1.001× on
  each). The first triple's sma spread (0.00836–0.00916) is load noise; the S2a path adds one
  expression walk at bind time and nothing at execution for an explicit window.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ta-series-s2a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each order item (resolver, rewrite, warning, native null_lookback, partition-index arm, facade, docs, maps) maps to a clause; every pin P-S2a-1 to P-S2a-9 has a measured red under its named mutation.
      artifacts: [crates/repark-core/src/series_order/tests.rs, crates/repark-ta/tests/null_prefix.rs, python/repark/tests/test_ta_series.py]
    - id: AT-2
      status: ATTACKED
      evidence: No declared order, no temporal column, a date-only schema, a zoned timestamp, aliased sort keys, dropped and computed keys, NULL and NaN prefixes per partition and a fetching coalesce are exercised; a prefix longer than its partition is clamped by construction (not pinned).
      artifacts: [crates/repark-core/src/series_order.rs, crates/repark-ta/src/udf/glue.rs]
    - id: AT-3
      status: ATTACKED
      evidence: A failing rewrite surfaces the builder's error and a failing warning call returns its PyErr (by construction, not pinned); the partition-index arm joins its tasks in index order and propagates the first join error.
      artifacts: [crates/repark-python/src/column/series.rs, crates/repark-core/src/parallel_window/exec.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The notice flag is one AtomicBool shared by every config clone of a session; partition tasks are SpawnedTasks dropped with the stream; output is assembled in partition-index order, never completion order.
      artifacts: [crates/repark-core/src/series_order.rs, crates/repark-core/src/parallel_window/tests.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, network or deserialization; the repark.series carrier refuses SET and is attached only by the Rust session builder.
    - id: AT-6
      status: ATTACKED
      evidence: Bare equals explicit by construction and bit for bit on the owner's file, explicit equals base, 0 rows off polars_talib, goldens unchanged, SQL TA answers unchanged.
      artifacts: [crates/repark-ta/tests/null_prefix.rs, python/repark/tests/test_ta_series.py]
    - id: AT-7
      status: ATTACKED
      evidence: Interleaved release-wheel rounds against main today and main now, single and nine samples per process, kernel race re-measured; one bind-time walk is the only cost on the explicit path.
      artifacts: [crates/repark-python/src/column/series.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency, Cargo.toml or Cargo.lock change; the new core module has no TA knowledge and the crate DAG is unchanged.
      artifacts: [crates/repark-core/src/series_order.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The (b) and (c) cases warn once per session with the column named; EXPLAIN shows the ordered window for the bare spelling.
      artifacts: [crates/repark-python/src/column/series.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Twelve mutations across nine pins, each red, the tree restored and the suites green afterwards.
      artifacts: [crates/repark-core/src/series_order/tests.rs, crates/repark-core/src/parallel_window/tests.rs, crates/repark-ta/tests/null_prefix.rs, python/repark/tests/test_ta_series.py]
  complete: true
```

## 7. Verifier fold V950-1 / V950-2 (2026-10-05)

The scoped Opus verifier passed #950 (`verify950/handback.json`) with one S2 and one S3; both
fold here under the S2a-fold rulings R-F1…R-F4. **C-004 correction:** the clause's determinism
claim as written was false. Joining X's partitions in index order is not enough: DataFusion 54's
`datafusion.execution.enable_file_stream_work_stealing` (default true) lets an idle partition
read files or byte-range morsels assigned to a sibling, so partition order is not file order.
Measured on the pre-fold head: the owner's single-file frame came back in file order 5 of 10
runs, 10 of 10 with stealing off. The corrected claim is that case (c) reads file order because
the arm runs its input with stealing off.

- **V950-1 (S2), R-F1.** `partition_index_stream` (`crates/repark-core/src/parallel_window/exec.rs`)
  now executes X with a `TaskContext` derived from the incoming one (same task and session id,
  same functions, same runtime) whose session config sets
  `enable_file_stream_work_stealing = false`. `DataSourceExec::execute` builds its sibling state
  from the first execute call's config, so the override covers the whole subtree. Only the
  `InputOrder::PartitionIndex` arm uses the derived context; the `Single` arm and every other
  operator keep the incoming context unchanged. No Cargo change, no new crate, no `.github`.
- **V950-1 pins, R-F2.**
  `test_ta_series.py::test_bare_ta_file_stream_stealing_reads_file_order` runs case (c) 10 times
  over an 8-file directory read written by the test (plan shows 8 file groups) and 10 times over
  one 1.2 M-row file (31 MB, 12 row groups) that `target_partitions=16` splits into 16 file
  groups (asserted from the plan), each time bit-equal to the explicit
  `.over(Window.orderBy("rid"))` answer with the identical row order. Post-fix: 10 of 10 on both
  legs (three runs: one pin run, two counting-probe runs). Mutation (the override dropped): the
  pin goes red on the byte-range leg; the counting probe over three runs differs 0, 1 and 3 of 10
  on the multi-file leg and 4, 6 and 3 of 10 on the byte-range leg, in row order and in values.
- **V950-2 (S3), R-F3.** Docs only: the `ta-guide.md` order note now says an order declared only
  through a temp view or a SQL subquery's `ORDER BY` is not a declared order.
- Maps: the `parallel_window` `rule.rs` / `exec.rs` rows name the work-stealing override, the
  facade-tests row carries the new pin (`pins: ta-series-s2a/C-004`), the guide row notes V950-2.

```yaml
FOLD_ATTESTATION:
  fold: V950-1 / V950-2 (#950)
  unit: ta-series-s2a
  date: 2026-10-05
  corrects: C-004
  fix: crates/repark-core/src/parallel_window/exec.rs (partition_index_stream)
  pin: python/repark/tests/test_ta_series.py::test_bare_ta_file_stream_stealing_reads_file_order
  pin_result: 10/10 multi-file (8 groups), 10/10 byte-range (16 groups), bit-equal to file order
  mutation: override dropped, pin red; probe diffs multi-file 0-3/10, byte-range 3-6/10
  docs: docs/guide/ta-guide.md order note (V950-2)
  maps: parallel_window, python/repark/tests, docs/guide
  complete: true
```
