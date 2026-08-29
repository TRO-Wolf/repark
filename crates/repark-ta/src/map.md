# map — repark-ta/src

## Purpose

The kernel implementations, one module per TA-Lib category. Every function is a bit-exact port of
its `ta-lib 0.4.0 src/ta_func/ta_<NAME>.c` counterpart. The crate docs in `lib.rs` define the
numeric contract: no FMA, C-order accumulators and Wilder smoothing, epsilon guards, NaN lookback
prefixes, and all-NaN success for short input.

## Contents
- `tests.rs` — kernel smoke tests.

- `lib.rs` — crate docs (the numerics contract + attribution; deliberate C-mirrored `*_idx`
  style exception), `TaError`/`Result` (incl. `NonIntegralPeriod` for window-UDF period args),
  shared helpers (`is_zero`/`is_zero_or_neg` = the C epsilon macros, `true_range` = the C macro,
  `as_f64`, `nan_vec`, `check_period`, `check_lengths`), re-exports of every kernel.
- `math_operator.rs` — `min`/`max` (TA-Lib trailing-index rescan: a running extremum + its
  index, rescanning the window only when that index falls off the trailing edge; `<=`/`>=`
  single-bar extension prefers the more recent equal value — the cadence that makes it bit-exact)
  and `sum` (the `sma` add-one/subtract-one running total without the divide).
- `overlap.rs` — Moving averages and overlap indicators. Preserve incremental sums, C seed/order,
  odd/even TRIMA branches, BBANDS rounding branches, and MAMA's full-prefix Hilbert state.
  `ma` routes all types through `momentum::ma_dispatch`; period one is identity and type 7 is
  MAMA with fixed limits. `mavp` uses shifted per-row MA seeding; SAR/SAREXT preserve sign rules.
- `momentum.rs` — RSI and directional, rate, oscillator, MACD, and stochastic families. Preserve
  Wilder and C statement order, trailing-extreme tie rules, exact zero guards, circular-buffer
  summation, and period-one DI/DM identity paths. MACD uses its own seeded EMA recurrence; MACDEXT
  uses shifted MA ranges and full-prefix MAMA for absolute Hilbert parity. Stochastic lookbacks
  compose raw %K with selected smoothing, including MAMA type-7 identity at period one.
- `volatility.rs` — `trange`, `atr`, and `natr`, preserving C's range selection, Wilder seed and
  recurrence, period-one delegation, and close-zero guard. A zero close writes `0.0` at the current
  index in this port; upstream C writes index zero.
- `statistic.rs` — variance, standard deviation, regression, forecast, correlation, and beta.
  Running sums, C order, denominator guards, and beta's trailing-before-write alias safety are
  load-bearing.
- `price_transform.rs` — no-period O/H/L/C combinations with lookback 0. Recorded
  `polars_talib` 0.1.5 bits require `TYPPRICE` to fold `low + close` before adding `high`.
- `volume.rs` — AD/ADOSC/OBV/MFI. Preserve strict AD range checks and CLV order, ADOSC's seeded
  inline EMAs, OBV's first-volume seed, and MFI's rolling neg/pos sums with its hard `< 1.0` guard.
- `udf/` — **feature `datafusion`** — DataFusion window-UDF wrappers; see [udf/map.md](udf/map.md)
  for the 81-entry spec table, family dispatch, literal-parameter rules, full-partition semantics,
  ABI, densification, and thread-local multi-output cache. The directory module resolves
  `udf/mod.rs`.

- `extension.rs` — **feature `datafusion`** — `TaExtension` forwards registration to
  `udf::register_all`; its `configure` hook remains the trait default. Tests cover bit-exact
  registration, the complete registry, and configuration pass-through.

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
| Change how a kernel builds its output `Vec` | Bench it in [../benches/ta_kernels.rs](../benches/ta_kernels.rs) first, then compare saved baselines; measure rather than assume |

## Pointers

- Up: [../map.md](../map.md)
- The golden gate: [../tests/map.md](../tests/map.md)

## Debug

See [../map.md#debug](../map.md) — the golden-failure playbook lives there. Clippy pedantic
rejects used underscore-prefixed bindings; use a plain name when the assertion reads it.
