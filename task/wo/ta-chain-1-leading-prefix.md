# TA-CHAIN-1 — a leading NaN/NULL run on a `ta_*` input is skipped, as polars_talib does; a chained indicator stops answering all-NaN      grade: B   engine band: guided (Muse Spark at MAX)   release: v1.5.2

Written 2026-10-03 by a Claude session (claude-fable-5-1) on the owner's ruling of the same day ("Can we add it to the v1.5.2?"). Every `path:line` is at `origin/main` 3406dbb3. Facts marked **measured** were run on the owner's 1,000,000-row, 14-contract futures file and on hand-built frames with polars_talib 0.1.5 over polars 1.43.1 on 2026-10-03.

## 0. Why, and what is out of scope

`ta.ema(ta.trange(high, low, close), 21)` answers NaN on every row. TRANGE has a one-row lookback, so its output begins with one NaN; TA-Lib C propagates a NaN through EMA forever and `repark-ta` is bit-faithful to C, so every indicator computed from another indicator is all-NaN (**measured**: 999,706 of 1,000,000 rows NaN against finite polars_talib values, on `ema(tr, 21)` partitioned by contract). polars_talib, the crate's stated golden oracle (`crates/repark-ta/map.md` "Golden recorder"), skips each input's leading invalid run before calling the C kernel, so chained indicators work there. This unit aligns the window-UDF wrapper with the oracle. It is a wrong-answer bug in the shipped `repark.ta` namespace, which is why it rides v1.5.2.

**Out of scope, named.** The kernel layer (`repark_ta::ema(&[f64]) -> Vec<f64>` and the other 67) keeps C's semantics unchanged. Interior NaN still propagates as in C (polars_talib does the same, **measured**). The multi-output cache key limitation dated 2026-08-29 in `crates/repark-ta/map.md` is untouched. `null_lookback=True` keeps NULLing exactly the first `lookback` rows by row position (`python/repark/src/repark/spark/ta.py:95-122`); a skipped run ahead of it stays NaN. Any change to the facade's lookback arithmetic is a separate card. No change to `crates/repark-spark` or `crates/repark-python`.

## 1. Rulings already made

| id | ruling |
|---|---|
| R-TC1-1 | Semantics are polars_talib 0.1.5 as **measured** 2026-10-03: for each input series the leading run of NULL or NaN is skipped; the kernel starts at the latest first-valid index across all inputs; output rows before that index plus the kernel's own lookback are NaN; an interior NaN after the start propagates exactly as C. Probe: `ema([NaN,NaN,1..12], 3)` → NaN×4 then 2.0, 3.0, …; `ema([null,null,1..12], 3)` identical; `trange(h=[NaN,3,4,…], l=[1,2,NaN,4,…], c=[…])` → NaN×3 then 1.5 (start = 1, the row-2 NaN is interior and costs rows 2 and 3 only); `atr(…, 2)` on the same inputs all-NaN (recursive, interior NaN). |
| R-TC1-2 | The fix lives in the window-UDF wrapper, `crates/repark-ta/src/udf/`, on both the null-free borrow path and the densified path (`src/udf/mod.rs:661-725`), because NaN arrives with `null_count == 0` (a TRANGE output has no NULLs). Both doors inherit it: SQL `ta_*(…) OVER (…)` and Python `ta.*` build the same `PyColumn.ta_window` (`ta.py:140`). |
| R-TC1-3 | Skipped rows are emitted as NaN, not NULL, matching polars_talib and the existing lookback prefix. Output type stays nullable Float64. |
| R-TC1-4 | The kernel-layer goldens (`crates/repark-ta/tests/goldens.rs`, 158 series, two fixtures) are not edited and not re-recorded. New goldens for this unit are recorded through polars_talib into a separate directory with its own manifest, so `manifest_and_tests_cover_the_same_series` is not touched. |
| R-TC1-5 | Perf cap 1.02x on `python/repark-parity/bench/ta/bench_kernel_race.py --quick` (one symbol, no prefix): the null-free fast path must stay a borrow when the start index is 0. Above the cap is a halt, not a tune. |
| R-TC1-6 | `crates/repark-ta/src/udf/mod.rs` is at its sanctioned ceiling of 1821 lines (`scripts/check_rust_file_size.py:202-206`). It may not grow. The four `compute` / `compute_all` call blocks move into one helper in a new sibling file, which makes `mod.rs` shorter, not longer. |
| R-TC1-7 | Engine and review: Muse Spark at MAX runs the three slices below, one commit each; then DIFF-PROBE base-vs-head and one Opus 5.5 verifier at medium effort (product-Rust PR rule, 2026-09-26). Opus executors are not used; the design is in this order. |
| R-TC1-8 | Merge position: after #879 and #883 in the v1.5.2 queue, before the tag. One release-note line under the v1.5.2 notes: "Chained TA indicators (an indicator computed from another indicator's output) now answer where polars_talib answers; a leading NaN or NULL run on any `ta.*` input is skipped instead of propagating." |

## 2. Files

| path | change | ceiling |
|---|---|---|
| `crates/repark-ta/src/udf/prefix.rs` | new: `leading_invalid_run`, `run_with_prefix_skipped`, `run_all_with_prefix_skipped`, inline unit tests | 1000 (default; comment ceiling 0) |
| `crates/repark-ta/src/udf/mod.rs` | edited: `mod prefix;` and the four call sites in `evaluate_all` route through the helpers; net line count ≤ 1821 | 1821 (sanctioned, not raised) |
| `crates/repark-ta/src/udf/map.md` | edited: `prefix.rs` row, "I want to…" row, debug row "chained indicator all-NaN" | — |
| `crates/repark-ta/map.md` | edited: one line under State & lifecycle, one Debug row | — |
| `python/repark-parity/record_ta_goldens.py` | edited: `prefix_fixture()` + `prefix_cases()` writing `crates/repark-ta/tests/goldens/prefix/*.bin` and `goldens/prefix/manifest.json` | — |
| `crates/repark-ta/tests/goldens/prefix/` | new recorded fixtures (binary) | — |
| `crates/repark-ta/tests/prefix_goldens.rs` | new: wrapper-level bit-exact gate behind `#[cfg(feature = "datafusion")]` | 1000 |
| `crates/repark-ta/tests/map.md` | edited: the new gate and fixture directory | — |
| `python/repark/tests/test_ta_with_indicators.py` | edited: `test_chained_indicator_over_trange_matches_polars_talib_golden`, `test_leading_null_prefix_is_skipped_per_partition` | — |
| `docs/examples/ta/composition.py` | edited: one chained column (`ema21_of_tr`) asserted against the new golden; `COVERS` unchanged | — |
| `task/ledgers/staging/ta-chain-1-ledger.md` | new: clauses C-001…C-007 below | — |
| `task/roadmap/mid-term/ta-chain-1-card-2026-10-03.md` | already filed with this order | — |
| `task/roadmap/mid-term/v1-5-2-release-notes.md` or the file the release clerk names | one line (R-TC1-8) | — |

## 3. Design sketch

```rust
pub(super) fn leading_invalid_run(series: &[&[f64]]) -> usize
```
Returns the latest index, across all series, of the first value that is not NaN (densify has already turned NULL into NaN, `mod.rs:757-760`); a series with no valid value returns its length. Stops scanning each series at its first valid value, so a clean series costs one comparison.

```rust
pub(super) fn run_with_prefix_skipped(
    series: &[&[f64]],
    run: impl FnOnce(&[&[f64]]) -> crate::Result<Vec<f64>>,
) -> crate::Result<Vec<f64>>

pub(super) fn run_all_with_prefix_skipped(
    series: &[&[f64]],
    run: impl FnOnce(&[&[f64]]) -> crate::Result<Vec<Vec<f64>>>,
) -> crate::Result<Vec<Vec<f64>>>
```
Start 0: call `run(series)` and return it unchanged (no allocation, no copy). Start equal to the length: return `vec![f64::NAN; len]` (or one such vector per band) without calling the kernel. Otherwise: build the trimmed slice list `&s[start..]`, call `run`, then return a vector of `start` NaNs followed by the kernel output. Kernel errors pass through unchanged.

`evaluate_all` (`mod.rs:661`): the two single-output call sites become `run_with_prefix_skipped(&slices, |s| self.func.compute(s, &self.params))` and the two multi-output sites `run_all_with_prefix_skipped(…compute_all…)`. The borrow fast path (`try_borrow_null_free_f64`) stays as the way the slice is obtained; the helper runs on the borrowed slice too. The cache (`multi_out_lookup` / `multi_out_store`, keyed on series identity) is unchanged: the trimmed output is a pure function of the same inputs.

**Recorder.** `prefix_fixture()` takes the existing walk fixture and overwrites leading runs with NaN: open 0 rows, high 3, low 7, close 5, volume 2, periods 0, so the start index is 7 and every multi-input kernel exercises a different per-input run. `prefix_cases()` records, through polars_talib: `prefix_ema_21`, `prefix_sma_10`, `prefix_rsi_14`, `prefix_adx_14`, `prefix_trange`, `prefix_atr_14`, `prefix_bbands_upper_20`, `prefix_macd_12_26_9` (the MACD line), `prefix_stoch_slowk` (defaults), `prefix_obv`, `prefix_linearreg_5`, `prefix_mavp_sma`, and the chain `prefix_chain_ema21_of_trange` (`ema(trange(h,l,c), 21)`). Each is written with the same `series_bits` encoding (`record_ta_goldens.py:65-68`). The recorder keeps asserting the TA-Lib 0.4.0 and polars_talib 0.1.5 versions.

**Wrapper gate** (`tests/prefix_goldens.rs`): builds `Float64Array`s from the prefix fixture with the NaN run as NaN in one test and as NULL in a twin test (same expected bits, R-TC1-1), drives `TaEvaluator` through the public `window_udfs()` registration and `evaluate_all`, and asserts `f64::to_bits` equality per element with NaN↔NaN allowed, exactly as `goldens.rs:224` does. The chain golden is asserted by feeding the recorded `prefix_trange` output back in as the `ema` input.

## 4. Steps

Slice S1 — Rust wrapper (one commit: `fix(ta-chain-1): a leading NaN/NULL run on a ta_* input is skipped before the kernel, as polars_talib does`).
1. Red first: add the inline unit test `leading_run_is_skipped_and_reprefixed_for_ema` in `prefix.rs` against the probe values in R-TC1-1 and a wrapper test in `mod.rs`'s existing test module `evaluate_all_chained_trange_into_ema_is_finite_after_lookback`; run `cargo test -p repark-ta --features datafusion udf::`; both fail.
2. Write `prefix.rs`; route the four call sites; `mod prefix;`.
3. `cargo test -p repark-ta --features datafusion` green; `cargo test -p repark-ta` (kernel goldens, no feature) green and the test count unchanged from base.
4. `python scripts/check_rust_file_size.py` green with `udf/mod.rs` at or below 1821.
5. `make check-comment-density`, `cargo clippy -p repark-ta --features datafusion -- -D warnings`, `cargo fmt --check`.
6. map.md lockstep for `udf/` and the crate root.
7. Commit.

Slice S2 — goldens (one commit: `test(ta-chain-1): polars_talib prefix goldens recorded; the wrapper gate pins NaN and NULL runs bit-exact`).
8. Extend the recorder; run it in the parity `record` environment (`record_ta_goldens.py` module docstring names it); confirm `goldens/prefix/manifest.json` lists 13 series and the two version assertions pass.
9. Write `tests/prefix_goldens.rs`; `cargo test -p repark-ta --features datafusion --test prefix_goldens` green (13 series × 2 twins).
10. `cargo test -p repark-ta` unchanged count (goldens.rs untouched).
11. `tests/map.md` lockstep. Commit.

Slice S3 — facade, example, ledger (one commit: `docs(ta-chain-1): chained indicator example and facade pins; ledger and release-note line`).
12. The two facade tests in `test_ta_with_indicators.py` (one over a 14-partition frame with a NULL run on two partitions, asserting per-partition starts); `docs/examples/ta/composition.py` gains `ema21_of_tr` against `prefix_chain_ema21_of_trange`; `scripts/check_example_coverage.py --require-execute` green.
13. `python/repark-parity/bench/ta/bench_kernel_race.py --quick` on base and head, three runs each, median ratio recorded in the ledger (R-TC1-5).
14. Ledger `ta-chain-1-ledger.md`: C-001 R-TC1-1 semantics on single-input kernels (prefix goldens); C-002 multi-input start = max first-valid (trange, adx, atr, stoch, mavp goldens); C-003 NULL run and NaN run bit-identical (twin tests); C-004 interior NaN still propagates (one inline test, atr probe); C-005 kernel layer untouched (`cargo test -p repark-ta` count equal to base, goldens.rs byte-identical); C-006 perf ≤ 1.02x (step 13); C-007 mutation: remove the re-prefix (return the trimmed vector) → `prefix_goldens` fails on length; remove the start computation (always 0) → every prefix golden fails; both reverted, tree clean, cited by test name.
15. Release-note line (R-TC1-8). Map lockstep. Commit.
16. Push; open the PR titled `fix(ta-chain-1): chained TA indicators answer where polars_talib answers — a leading NaN/NULL run on a ta_* input is skipped (v1.5.2)`; body lists C-001…C-007 and the bench ratio.

## 5. Gates and their expected output

| command | green means |
|---|---|
| `cargo test -p repark-ta` | `test result: ok.` with the same test count as base (record the number in the ledger) |
| `cargo test -p repark-ta --features datafusion` | `test result: ok.`; the two new unit tests listed by name |
| `cargo test -p repark-ta --features datafusion --test prefix_goldens` | `26 passed` (13 series × NaN/NULL twins) |
| `python scripts/check_rust_file_size.py` | exit 0; no line naming `udf/mod.rs` above 1821 |
| `make check-comment-density` | exit 0 (new file ceiling is zero) |
| `cargo clippy -p repark-ta --features datafusion -- -D warnings` / `cargo fmt --check` | exit 0 |
| `make rust-panic-ban` | exit 0 |
| `python scripts/check_example_coverage.py --require-execute` | exit 0 |
| `.venv/bin/python -m pytest python/repark/tests/test_ta_with_indicators.py -q` | all passed, two new names present |
| `bench_kernel_race.py --quick` base vs head | median ratio ≤ 1.02 |

## 6. Halt rules

- A polars_talib probe for any recorded family disagrees with R-TC1-1 (for example a family that does not skip, or that re-indexes the lookback differently). Halt with the family name, the probe input and both outputs; do not approximate.
- The two version assertions in the recorder fail (a different TA-Lib or polars_talib build). Halt; never record against another oracle.
- `udf/mod.rs` cannot be kept at or below 1821 lines after routing. Halt with the line count; do not touch the exception table.
- Any edit under `crates/repark-ta/src/` outside `udf/` is needed. Halt: the kernel layer is out of scope (R-TC1-2).
- `manifest_and_tests_cover_the_same_series` goes red. Halt: the prefix goldens were written into the wrong directory.
- Bench ratio above 1.02 (R-TC1-5). Halt with the three-run numbers on both sides.
- Any change to an existing golden's bits or to a facade answer outside the chained/prefixed cases (DIFF-PROBE finds a differing cell that has no leading-invalid run). Halt with the cell.

## 7. Hand-back

```json
{"unit":"TA-CHAIN-1","branch":"fix/ta-chain-1","commits":["<S1>","<S2>","<S3>"],
 "kernel_test_count":{"base":0,"head":0},
 "prefix_goldens":{"series":13,"twins_passed":26},
 "bench_kernel_race_quick":{"base_median_s":0.0,"head_median_s":0.0,"ratio":0.0},
 "mod_rs_lines":{"base":1821,"head":0},
 "ledger":"task/ledgers/staging/ta-chain-1-ledger.md","clauses_proven":["C-001","C-007"],
 "halts":[],"pr":0}
```
