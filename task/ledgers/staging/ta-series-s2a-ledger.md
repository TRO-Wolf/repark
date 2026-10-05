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
| C-011 | Speed (sequencing trap): bare not more than 3 % slower than base; partitioned ≤ 0.18 s and not slower than base; kernel race ≤ 1.02×. | Interleaved median of 5 on release wheels, after READY_FOR_SPEED. | OPEN | Held at READY_FOR_SPEED by the order. |
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
