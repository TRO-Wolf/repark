# map — repark-ta/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

The crate's integration gates: bit-exactness against C TA-Lib (goldens) and the crate-wide
argument contract. Tolerance comparisons are deliberately absent from the golden gate; if a
golden fails, the kernel drifted (or the oracle moved), never "close enough".

## Contents

- `null_prefix.rs` — **TA-SINGLE-SERIES-PARALLEL-1 S2a (2026-10-05)**, feature `datafusion`:
  `null_lookback_native_matches_row_number_rewrite` (P-S2a-5: `ta_ema(tr, 13)` with prefix 12
  over `PARTITION BY sym ORDER BY ts` against `CASE WHEN row_number() > 12 THEN … END` on the
  same window, cell for cell, 12 NULL + 1 NaN per symbol, and the whole-frame window);
  `null_prefix_instances_never_cse` (P-S2a-6: prefixed and plain instances differ in equality
  and hash, and both columns survive one `select`); `sql_ta_unchanged` (P-S2a-8: SQL
  `ta_ema(close, 13) OVER (…)` equals the kernel per symbol, NaN prefix, no NULL, and no
  registered name carries a prefix). pins: ta-series-s2a/C-005, C-006, C-008
- `goldens.rs` — **strict `f64::to_bits` equality** per element (NaN ↔ NaN allowed) for every
  kernel × param-set: 41 tests (39 golden checks plus two fixture/manifest checks) over 158 recorded series across two fixtures — the 5000-row
  lognormal walk (happy path, all 5 BBANDS band branches, the WG1 overlap-MA family incl. TRIMA
  odd/even + T3 two vfactors, the WG2 simple-momentum batch incl. the ROC family, WILLR/CCI/CMO,
  BOP, APO/PPO at matype 0 **and matype 7 (MAMA)**, split AROON + AROONOSC, TRIX, ULTOSC, the WG3
  directional family DX/ADXR/PLUS_DI/MINUS_DI/PLUS_DM/MINUS_DM, the split MACD/MACDFIX/MACDEXT
  outputs (incl. MACDEXT all-MAMA + mixed 7/0/1), the MA selector at matype 0/1, the WG4 split
  stochastics STOCH/STOCHF/STOCHRSI at the polars_talib defaults **and matype 7** (all-MAMA +
  mixed 7/0 + fastd=7 legs), the WG5 sweep-up NATR/BETA + the
  O/H/L/C price transforms AVG/MED/TYP/WCL, and the T3 parked four — MAMA's two outputs, SAR,
  SAREXT three ways (auto default + forced-long-offset + forced-short — the negative short-side),
  MAVP over the `fixture_periods` series at SMA + EMA, and the `ma_30_type7` MA-selector-matype-7 =
  MAMA pin) and the 600-row flat-plateau series (drives the `TA_IS_ZERO` epsilon-guard branches,
  incl. KAMA's efficiency-ratio guard, the WG2 CMO/WILLR/CCI/BOP/ULTOSC zero short-circuits, the
  WG3 DX re-emit + DI zero short-circuit, the WG4 STOCHF raw-%K `diff` zero guard, the WG5 BETA
  return/denominator zero guards, and MAMA's steady-state atan/`Re`/`Im` zero guards). Also enforces
  recorder↔test sync via `manifest.json` (`manifest_and_tests_cover_the_same_series`). TA-4
  consumes the volume-family goldens (`ad`/`adosc_3_10`/`obv`/`mfi_14` + flat twins +
  `fixture_volume` / `fixture_flat_volume`) as `f64::to_bits` kernel pins
  (`volume_family_matches_c_talib` / `volume_family_flat_guard_branches_match_c_talib`).
- `contract.rs` — the argument contract for EVERY kernel: below-min period → `InvalidPeriod`;
  above-`MAX_PERIOD` (incl. `usize::MAX`) → `InvalidPeriod`, never overflow; short input →
  full-length all-NaN; empty → empty; multi-series length mismatch → `LengthMismatch`. The shared
  `kernels()` sweep covers every `optInTimePeriod` kernel; `ultosc`/`apo`/`ppo`/`bop` (different
  period-param names / no period) get a dedicated `ultosc_apo_ppo_bop_argument_contract` test that
  pins APO matype-7 short-series all-NaN success + out-of-range `matype`. The WG3 MACD family
  (`optInFast/Slow/SignalPeriod`), the `MA` selector, and the `period == 1`-capable DI/DM functions
  get `macd_ma_directional_contract` (per-name below-min, short/empty, MACDEXT matype-7 short
  all-NaN + out-of-range `UnsupportedMaType`; `ma(..., 7)` bit-equals `mama(..., 0.5, 0.05)` so
  the MA@7 path cannot be a zero stub). The WG4 stochastics (multi-input, multi-output,
  named period + `matype` params) get `stochastics_argument_contract` (STOCH/STOCHF period min 1,
  STOCHRSI's RSI period min 2, above-MAX, short/empty per output, H/L/C length mismatch, and
  matype-7 short-series all-NaN success + out-of-range `UnsupportedMaType`). WG5: `natr`/`beta`
  join the shared `kernels()` sweep (both period-min 1); the no-period O/H/L/C price transforms get
  a dedicated `price_transform_argument_contract` (every-bar output, empty→empty, length mismatch).
  TA-4: `mfi` joins the shared `kernels()` sweep (period-min 2); AD/OBV (no period) and ADOSC
  (two named periods, min 2) get `volume_family_argument_contract` (every-bar / empty / short
  all-NaN / below-min / length mismatch).
  T3: `parked_four_argument_contract` pins the four's real-valued / multi-input parameter ranges
  (MAMA limits `[0.01,0.99]` + NaN-reject; SAR/SAREXT accelerations `[0,3e37]`, SAREXT start
  `[-3e37,3e37]` with a negative start legal; MAVP min/max `[2,MAX_PERIOD]` + matype `0..=8` +
  periods-series length mismatch) and short/empty behavior (`InvalidRealParam` is the new
  non-period error variant).
- `prefix_goldens.rs` (feature `datafusion`) — TA-CHAIN-1 wrapper-level gate: drives each
  `ta_*` window UDF through the public `udf::window_udfs()` registration and `evaluate_all` over
  the prefix fixture (the walk fixture with leading NaN runs: open 0, high 3, low 7, close 5,
  periods 0, volume 2) and asserts `f64::to_bits` equality (NaN ↔ NaN allowed) against the
  polars_talib prefix goldens. 26 tests = 13 series × twins, the run fed as NaN and as NULL with
  the same expected bits; the chain `prefix_chain_ema21_of_trange` feeds the recorded
  `prefix_trange` back in as the `ta_ema` input. The kernel-layer `goldens.rs` is untouched.
  Mutations (TA-CHAIN-1 C-007): returning the trimmed vector without the NaN re-prefix fails all
  26 on length; forcing the start to 0 fails 24 of 26 (the `linearreg_5` twins survive because
  LINEARREG recomputes each window, so the NaN run leaves no trace once out of the window).
  pins: ta-chain-1/C-001, C-002, C-003, C-007
- [goldens/](goldens/map.md) — the recorded fixtures + `manifest.json` (checked in; ~1.5 MB);
  [goldens/prefix/](goldens/prefix/map.md) holds the TA-CHAIN-1 prefix goldens with their own
  manifest.
- `p1c_microbench.rs` — wall microbench (BBANDS 1e6-row, one kernel vs
  three independent sibling runs vs ideal-cached clone cost). Not a correctness gate —
  `cargo test -p repark-ta --release --test p1c_microbench -- --nocapture`.

- `parallel_window.rs` (feature `datafusion`) — TA-SINGLE-SERIES-PARALLEL-1 S2b: two
  `ReparkSession`s with `TaExtension`, one built with the default (`ParallelWindowExec` on) and
  one with `parallel_single_partition(false)`, over the same 50,000-row one-partition table of
  eight batches. `parallel_window_bit_identical` runs the owner benchmark's level 3 (TRANGE in a
  CTE, then the 14 `ORDER BY ts` indicators: EMA5, RSI 13/21/34, SMA 10/20/34, EMA of TR 5/13/21,
  LINEARREG5, ADX 5/13/21), asserts 14 expressions on `ParallelWindowExec` and on the serial
  `WindowAggExec`, no `BoundedWindowAggExec`, and compares every column's `to_bits`, validity,
  name and order. `parallel_window_multi_output_siblings_bit_identical` interleaves BBANDS
  upper/middle/lower, MACD/signal and STOCH slowk/slowd so siblings are not adjacent.
  `stamped_frame_parallel_window_matches_serial_answer` stamps one TA level (SMA 10/20, EMA5)
  with attribute ids, strips the twin for execution, and compares the parallel answer
  bit-identically with the serial answer.
  pins: ta-series-s2b/C-001, C-002

## Pointers

- Up: [../map.md](../map.md)
- Recorder: `python/repark-parity/record_ta_goldens.py` (PEP-723 script; asserts BOTH oracle
  versions — bundled C TA-Lib 0.4.0 and `polars-talib` 0.1.5; atomic temp+rename writes).

## Debug

A golden failure prints the series name, row index, and both bit patterns. Match the series
name to its param set in `goldens.rs`, then follow the playbook in [../map.md#debug](../map.md).

**Stale-fixture-path footgun (fixed):** `goldens_dir()` resolves the fixtures from the RUNTIME
`CARGO_MANIFEST_DIR` (cargo sets it in the test process), not the compile-time `env!`. A shared
`target/` can hand a `cargo test` in worktree B a test binary compiled in worktree A; the old
`env!` baked A's path in, so all goldens failed hunting a (possibly deleted) worktree's fixtures.
`goldens_dir_resolves_from_the_runtime_manifest_dir` pins the resolution AND the cargo-runtime-var
assumption. If goldens ever fail with a path pointing at a foreign/removed worktree, this is why —
`touch crates/repark-ta/tests/goldens.rs` to force a recompile as a last resort.
