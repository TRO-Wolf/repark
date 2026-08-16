# map — repark-ta/src

## Purpose

The kernel implementations, one module per TA-Lib category (the `udf` wrapper's docs reference
the session type under its 2026-07-12 `ReparkSession` casing). Every function is a bit-exact port
of its `ta-lib 0.4.0 src/ta_func/ta_<NAME>.c` counterpart; the crate docs in `lib.rs` state the
numerics contract (no `mul_add`/FMA; incremental accumulators replicated drift-and-all; Wilder
smoothing in C's statement order; `TA_IS_ZERO` ±1e-8 guards; NaN lookback prefixes; short input
→ all-NaN, not an error).

## Contents
- `tests.rs` — kernel smoke tests (r26 LR1 hoist)

- `lib.rs` — crate docs (the numerics contract + attribution; deliberate C-mirrored `*_idx`
  style exception), `TaError`/`Result` (incl. `NonIntegralPeriod` for window-UDF period args),
  shared helpers (`is_zero`/`is_zero_or_neg` = the C epsilon macros, `true_range` = the C macro,
  `as_f64`, `nan_vec`, `check_period`, `check_lengths`), re-exports of every kernel.
- `math_operator.rs` — `min`/`max` (TA-Lib trailing-index rescan: a running extremum + its
  index, rescanning the window only when that index falls off the trailing edge; `<=`/`>=`
  single-bar extension prefers the more recent equal value — the cadence that makes it bit-exact)
  and `sum` (the `sma` add-one/subtract-one running total without the divide).
- `overlap.rs` — `sma` (incremental running total; T5 hot loop is `iter_mut().zip` over incoming + trailing, same add/snapshot/subtract/divide order), `ema` (SMA seed + `(x−prev)*k+prev`; single-write `with_capacity`/`extend` construction, measured −11% — the push form measured +61% slower),
  `wma` (recency-weighted `periodSum`/`periodSub` accumulator), `dema`/`tema` (EMA-of-EMA
  compositions built from `ema` itself — lookbacks `2·(p−1)` / `3·(p−1)`, C's index bookkeeping,
  NOT a re-derived closed form), `trima` (triangular window with C's odd/even period split — the
  divisor, `middleIdx` offset, and per-bar `numeratorAdd` statement order all branch on parity),
  `kama` (efficiency-ratio adaptive EMA; incremental `sumROC1`, `TA_IS_ZERO` ER guard; lookback
  `p`), `t3` (six chained EMA accumulators + Tillson volume-factor constants; `vfactor` param,
  lookback `6·(p−1)`), `midpoint`/`midprice` (per-bar full-window rescan of one / two H-L series),
  `bbands` (SMA middle + the `TA_INT_stddev_using_precalc_ma` deviation variant + C's five
  rounding-distinct band branches, all golden-covered), `ma` (the `TA_MAType` selector over
  `momentum::ma_dispatch` exclusively (no duplicate MAMA arm); `period == 1` is the identity for
  any in-range `matype`, and **matype 7 = MAMA(0.5, 0.05)** — period ignored, FAMA discarded,
  `ta_MA.c:152-154,313-329` — also live in `ma_dispatch` so APO/PPO/MACDEXT and the stochastic
  smoothing legs share the path).
  **T3 — the parked four (all TA-Lib
  "Overlap Studies"):** `mama` (John Ehlers' MESA adaptive MA + FAMA — a 4-period WMA price smoother
  feeding an odd/even Hilbert-transform state machine over 3-slot circular buffers; `HilbertVar` +
  `do_price_wma` port the `ta_utility.h` macros; two outputs, lookback 32, atan/`Re`/`Im` zero
  guards), `sar` (Wilder parabolic stop-and-reverse; initial direction from `minus_dm` period-1,
  lookback 1), `sarext` (SAR extended, 8 params; **short-side output is NEGATIVE** — replicated),
  `mavp` (variable-period MA over a second `periods` series, clamped `[min,max]` + int-truncated;
  each per-period MA via `momentum::ma_range` = C's shifted seeding — NOT a full-array MA).
- `momentum.rs` — `rsi` (Classic seed + Wilder smoothing + zero-guard; T5 hot loop is `iter_mut().zip`, same statement order including the two divides and `is_zero` guard), `adx` (three-phase:
  raw ±DM/TR accumulation → Wilder-decayed DX sum → smoothed output loop; lookback `2p−1`;
  the per-bar ±DM/TR block is the shared `DirectionalState::step`, `decay` selecting the
  phase-1 vs phase-2/3 accumulation — same statements, same order as C). WG2 simple-momentum:
  `mom` + the `roc`/`rocp`/`rocr`/`rocr100` family (one shared trailing-ratio loop, exact `!= 0.0`
  prevPrice guard), `willr` (trailing-index rescan, strict `</>` inside + `<=`/`>=` extension,
  persistent `diff`; output in `[−100,0]`), `cci` (per-window circular-buffer re-sum of typical
  prices — NOT incremental; the physical buffer summation order is load-bearing), `cmo` (`rsi`'s
  gain/loss decomposition, `(gain−loss)/(gain+loss)` final step), `bop` (four-series O/H/L/C, no
  lookback, `TA_IS_ZERO_OR_NEG` range guard), `apo`/`ppo` (`MA(fast) − MA(slow)` via the internal
  `ma_dispatch(matype)` selector — full 0..=8 incl. MAMA(7) = MAMA(0.5,0.05); PPO's percentage
  form + `TA_IS_ZERO` slow guard), `aroon` (split → `(down, up)`; trailing-index rescan with
  `<=`/`>=` in BOTH the rescan and the extension — AROON prefers the most-recent extreme — `i64`
  `−1` sentinel indices; **r22 SAF-005** checked `i64`↔`usize` casts / `InputTooLong`) +
  `aroonosc` (`up − down` simplified to `factor·(highIdx − lowIdx)`),
  `trix` (triple `ema` of dense tails, then period-1 ROC; lookback `3·(p−1)+1`), `ultosc`
  (three-period buying-pressure blend weighted 4/2/1 shortest→longest — periods sorted ascending,
  `optInTimePeriod1` shortest; `TA_IS_ZERO` per-period guards). `ma_dispatch`/`ma_lookback` are
  `pub(crate)`; matype 7 lookback is the fixed 32 (`ta_MA.c:152-154`); out-of-range →
  `TaError::UnsupportedMaType`. WG3 directional + MACD families: `dx`/`adxr`/`plus_di`/`minus_di`/
  `plus_dm`/`minus_dm` all drive the shared `DirectionalState` recurrence (NO second copy) via
  `directional_prime` (raw phase-1 seed) + accessors — lookbacks DX/DI `p`, DM `p−1`, ADXR `3p−2`
  (ADXR reuses `adx`); DI/DM allow `period == 1` (the un-smoothed `DM1`/`DM1÷TR1` fast path). MACD
  (`macd`/`macdfix`) shares `int_macd`, seeding the slow EMA `lookbackSignal` bars early through
  `int_ema_dense` (explicit `k` — MACDFIX pins 0.15/0.075, distinct from `PER_TO_K` MACD);
  `macdext` routes fast/slow/signal through `ma_dispatch` with `ma_range` reproducing C's shifted
  MA computation bit-exactly for windowed/re-seeded matypes; **matype 7 (MAMA) uses full-prefix
  `mama` + index slice** (not a re-based window — Hilbert absolute parity). It slices the
  input at `effStart − lookback`; `signal_period == 1` is the C identity MA — lookback 0, signal
  line equals MACD). `ma_lookback`/`ma_range` are `pub(crate)`. WG4 stochastics:
  `stochf`/`stoch`/`stochrsi` (each split → two outputs). The shared `raw_stoch_k` helper is C's
  `TA_INT_STOCHF` raw %K (`100·(close−LL)/(HH−LL)` over the trailing `fastK` window via the
  trailing-index rescan; `diff != 0.0` flat-window guard → 0.0); the smoothing MAs route through
  the `crate::ma` selector (matypes default 0 = SMA; `period == 1` = identity, so lookbacks use the
  local `ma_selector_lookback`, 0 at `period == 1` **including matype 7** — MAMA is not a special
  32 there; **matype 7 + period > 1** via `ma_lookback` → 32 and full-array `ma` on C's dense temp
  buffer). `stochf` = raw %K + one MA (`fastD`); `stoch` = raw %K + two MAs (`slowK` then `slowD`);
  `stochrsi` = `rsi` then `stochf` over the dense RSI (double lookback trim — RSI lookback +
  STOCHF lookback). Unit first-non-NaN pins for MAMA-composed lookbacks: stochf type7=36, stoch
  mixed 7/0=38, stoch all-MAMA=68, stochrsi type7=50
  (`stochastics_matype7_first_non_nan_lookback_pins`); period-1 + matype 7 identity on stochf /
  stoch / stochrsi (`stoch_path_period1_matype7_is_identity_via_ma_selector_lookback`).
- `volatility.rs` — `trange` (max of three ranges), `atr` (SMA-of-TR seed + Wilder; `period==1`
  delegates to `trange` as C does), `natr` (WG5: reuses `atr` then normalizes `(atr/close)·100`
  with a `TA_IS_ZERO(close)` guard — unreachable for real positive prices; `period==1` returns raw
  `trange` unnormalized, as C does).
- `statistic.rs` — `var` (running Σx/Σx², `E[X²]−E[X]²`; `nbdev` ignored as C ignores it),
  `stddev` (var → guarded `sqrt(v) * nbdev`; C's `nbdev==1` fast path folds in — `x*1.0` is
  bit-identical), `linearreg`/`linearreg_slope`/`linearreg_intercept`/`linearreg_angle`/`tsf`
  (one shared closed-form core, per-variant emit), `correl` (five running sums, guarded
  denominator), `beta` (WG5: two-series rolling covariance slope over per-bar returns — five
  running sums in C's exact statement order, trailing read before the write for in-place aliasing
  safety, `TA_IS_ZERO` return + denominator guards; lookback `p`).
- `price_transform.rs` — WG5: `avgprice`/`medprice`/`typprice`/`wclprice`, the no-period O/H/L/C
  combinations (lookback 0, every bar produces a value — the `trange`-shaped no-lookback family).
  Oracle note (truth-up 2026-08-15): the association follows the RECORDED `polars_talib` 0.1.5
  bits — its `TYPPRICE` series folds `low + close` first (`high + (low + close)`), and the kernel
  matches that. The earlier "wrapper implements these in its own native Rust" claim was false
  (upstream calls C over FFI); fold origin unverified — see the in-module re-record caution.
- `volume.rs` — TA-4: `ad`/`adosc`/`obv`/`mfi`. AD lookback 0, increment only if
  `(high−low) > 0.0` (strict `>`, not `TA_IS_ZERO`), CLV order
  `(((c−l)−(h−c))/tmp)*vol`. ADOSC does **not** call standalone `ema()` — C seeds both EMAs
  with the first AD then `(k*ad)+(one_minus_k*ema)`, `PER_TO_K(p)=2.0/(p+1)`, lookback 9 at
  (3,10). OBV first output is `volume[0]`. MFI is **not** Wilder: rolling pos/neg buffer,
  classify neg-first, hard `pos+neg < 1.0`, do not clamp (drift can go slightly negative).
- `udf/` — **feature `datafusion`** — the DataFusion window-UDF wrapper layer (directory
  module; `pub mod udf` in `lib.rs` resolves `udf/mod.rs`). A single spec table (name →
  kernel) in `udf/mod.rs` drives 81 `WindowUDF`s (the full 81/81 entry-point inventory) —
  the 66 T1–WG4 entry points, the 6 WG5 sweep-up ones, and the 5 T3 parked-four (the split
  `ta_mama`/`ta_fama` — 1 series + 2 real limits each; `ta_sar` — 2 series + 2 real scalars;
  `ta_sarext` — 2 series + 8 real scalars; `ta_mavp` — 2 series, the second being the per-row
  periods column, + 3 integral scalars), plus the 4 TA-4 volume entry points (`ta_ad`/
  `ta_adosc`/`ta_mfi` four-series H/L/C/V; `ta_obv` close+volume; `ta_adosc` two period
  scalars, `ta_mfi` one). Real-valued scalars (MAMA limits, SAR/SAREXT accelerations) bypass
  `period` (no whole-number check); MAVP's min/max/matype go through it. The WG5 sweep-up
  ones (`ta_natr` H/L/C+period, `ta_beta` two-series+period, and the no-period `ta_avgprice`
  O/H/L/C, `ta_medprice` H/L, `ta_typprice`/`ta_wclprice` H/L/C) — (the 17 single-output T1
  kernels + the 8 WG1 overlap-MA kernels `ta_wma`/`ta_dema`/`ta_tema`/`ta_trima`/`ta_kama`/
  `ta_t3`/`ta_midpoint`/`ta_midprice` + the 3 split `BBANDS` outputs + the 16 WG2
  simple-momentum entry points `ta_mom`/`ta_roc`/`ta_rocp`/`ta_rocr`/`ta_rocr100`/`ta_willr`/
  `ta_cci`/`ta_cmo`/`ta_bop`/`ta_apo`/`ta_ppo`/`ta_aroon_down`/`ta_aroon_up`/`ta_aroonosc`/
  `ta_trix`/`ta_ultosc` + the 16 WG3 directional + MACD entry points `ta_dx`/`ta_adxr`/
  `ta_plus_di`/`ta_minus_di`/`ta_plus_dm`/`ta_minus_dm`/the split `ta_macd`/`_signal`/`_hist`,
  `ta_macdfix`/`_signal`/`_hist`, `ta_macdext`/`_signal`/`_hist`, and the `ta_ma` selector +
  the 6 WG4 stochastic entry points — the split `ta_stoch_slowk`/`_slowd`,
  `ta_stochf_fastk`/`_fastd`, `ta_stochrsi_fastk`/`_fastd`). `ta_t3` carries a second scalar
  literal (`vfactor`); `ta_apo`/`ta_ppo` carry three (fast, slow, `matype`); `ta_ultosc`/
  `ta_macd*` carry three periods; `ta_macdext*` carry six (fast/slow/signal period +
  matype); `ta_ma` carries two (period + matype); `ta_bop` is four-series with no scalar;
  `ta_midprice`/`ta_aroon_*`/`ta_plus_dm`/`ta_minus_dm` are two-series; `ta_dx`/`ta_adxr`/
  `ta_plus_di`/`ta_minus_di` are three-series (high, low, close); the STOCH/STOCHF UDFs are
  three-series H/L/C (STOCH + 5 scalars, STOCHF + 3), STOCHRSI is single-series close + 4
  scalars. Each is a stateful full-series function:
  `PartitionEvaluator::evaluate_all` runs the kernel over the whole ordered partition
  (series columns first, scalar params as constant literals extracted at plan time;
  `Float64` out, NaN lookback preserved for `to_bits` parity). `register_all(ctx)` /
  `window_udfs()` (registration) + `window_udf(name)` (the `Arc<WindowUDF>` the Python
  DataFrame builder needs). Shared machinery (cache, densify, param checks, `evaluate_all`,
  SPECS, `TaFn`, statistic + math_operator dispatch, `register_all` / `window_udf*`) stays
  in `udf/mod.rs`; per-family `compute` / `compute_all` arms live in `udf/overlap.rs`,
  `udf/momentum.rs`, `udf/volatility.rs`, `udf/volume.rs`, `udf/price.rs`. Navigation:
  [udf/map.md](udf/map.md).
  **P1c perf (scout #8 + #9, 2026-08-02):** multi-output siblings (BBANDS / MACD* / STOCH* /
  AROON / MAMA) share a single-slot **thread-local** cache keyed by (family, params
  `f64::to_bits`, series values-buffer pointer + len + null_count) — one kernel run per
  partition, bands served to sibling columns. Entry **pins** the series `ArrayRef`s so
  buffer-pointer identity cannot ABA across partitions (allocator reuse). Invalidation =
  single-slot replace on key mismatch (no TTL; no cross-thread share). Densify: null-free
  `Float64` borrows `values()` (zero-copy into the kernel for single-series) or
  `extend_from_slice` into a reused per-evaluator scratch; NULL→NaN only when nulls exist;
  output via `Float64Builder`. Kernels themselves are untouched (goldens absolute; scout
  #28 forbidden).

- `extension.rs` — **feature `datafusion`** — `TaExtension`, a thin
  `repark_core::SessionExtension` whose `register` forwards to `udf::register_all` and whose
  `configure` stays at the trait default (TA installs no `ConfigExtension` and reads no conf key).
  Design SSOT: `docs/design/sql-doors.md` Q11 — the TA set is owned by **neither** SQL door, so
  the crate ships the extension and consumers compose it (`repark-spark`'s `SparkExtension` calls
  it at v1's exact registration position; a native session installs it directly). Tests
  (`extension/tests.rs`): the register side is bit-exact SQL-vs-kernel (`to_bits`) plus a
  whole-registry name-set assertion; the configure side is the trait-wrapping both-sides audit
  (config returned untouched). This is the one module that is NOT a v1 port — it is new at
  phase-2 PR-4 and pulls the crate's only internal dep (`repark-core`, feature-tied).

Scalar period args are coerced via fallible `period(f64)`: non-integral finite values fail loud
(`NonIntegralPeriod`, no silent truncate); negatives/NaN saturate then `check_period` rejects.
Period arguments are validated against `min..=MAX_PERIOD` (TA-Lib's documented 100 000
ceiling) — the cap is also what keeps every internal period expression away from usize
overflow (`tests/contract.rs` probes `usize::MAX`).

Unit tests live in each module (error paths, lookback positions, hand-computable cases);
bit-exactness is proven in [../tests/map.md](../tests/map.md). UDF layer (`udf` feature
`datafusion`) also pins multi-output cache + densify via `evaluate_all` sibling tests
(all multi families incl. STOCHRSI/MACDEXT, two-partition no cross-talk, Int64/Float32 cast,
empty/short, nullable multi-output, sliced borrow, kernel-error leaves cache empty, SPECS multi-family band width, too-few-series).

## I want to...

| ...do this | go to |
|---|---|
| Add a kernel | New fn in the matching category module + re-export in `lib.rs` + golden series in the recorder + `tests/goldens.rs` case — one commit |
| Expose a kernel as a window UDF | Add a `(name, TaFn)` row in `udf/mod.rs` + the family `compute` arm (feature `datafusion`) |
| Install every TA UDF on a session | `extension.rs` — `TaExtension`; do NOT add a second registration path |
| Change error behavior | `lib.rs` (`TaError`) — mirror TA-Lib `TA_BAD_PARAM` semantics only |
| Touch any arithmetic | Re-read the numerics contract in `lib.rs` first; then `cargo test -p repark-ta` |

## Pointers

- Up: [../map.md](../map.md)
- The golden gate: [../tests/map.md](../tests/map.md)

## Debug

See [../map.md#debug](../map.md) — the golden-failure playbook lives there. Clippy pedantic
rejects *used* underscore-prefixed bindings (`used_underscore_binding`) — in tests, destructure
to a plain name instead of `_x` if the assertion reads it (caught in the 2026-07-21 octo review).

## DF 54.1 note (2026-08-01)
as_any trait methods removed (DF54 trait upcasting); Cast uses field-aware API where touched.
