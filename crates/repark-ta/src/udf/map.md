# map — repark-ta/src/udf

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

DataFusion window-UDF wrappers for the TA kernels (feature `datafusion`). Shared machinery lives
in `mod.rs`: the 81-entry SPECS/`TaFn` routing, literal-parameter checks, full-partition
`evaluate_all`, densification, registration, and the thread-local single-slot cache. Cache keys
use family, parameter bits, and series identity; pinned arrays prevent ABA false hits.
Per-family `compute` / `compute_all` dispatch lives in sibling modules matching
the crate's kernel taxonomy. `statistic` + `math_operator` stay in `mod.rs`.
Kernel math is **not** here — it stays in `../overlap.rs` etc.

## Contents

- `mod.rs` — spec table (81 entry points), `TaFn` metadata (arity, multi-family
  band map), shared statistic/math dispatch, cache + densify + `evaluate_all`,
  registration. Inline unit tests pin cache / densify / `evaluate_all` siblings
  plus `compute_routes_every_spec_to_a_family_or_shared_arm` (router and family
  table must remain aligned).
- `overlap.rs` — overlap-family dispatch: `sma`/`ema`/`wma`/`dema`/`tema`/
  `trima`/`kama`/`t3`/`midpoint`/`midprice`/`bbands_*`/`ma`/`mama`/`fama`/
  `sar`/`sarext`/`mavp`, plus `compute_all` for `BBANDS` and `MAMA`.
- `momentum.rs` — momentum-family dispatch: RSI/ADX, ROC family, WILLR/CCI/CMO/
  BOP, APO/PPO, AROON*, TRIX/ULTOSC, directional + MACD*, stochastics; plus
  `compute_all` for MACD* / STOCH* / AROON.
- `volatility.rs` — `trange`/`atr`/`natr`.
- `volume.rs` — TA-4 `ad`/`adosc`/`obv`/`mfi`.
- `prefix.rs` — TA-CHAIN-1 leading-run skip: `leading_invalid_run` (latest first-valid row across
  the inputs), `run_with_prefix_skipped` / `run_all_with_prefix_skipped` (trim the run, call the
  kernel, re-prefix the skipped rows as NaN; start 0 is a pass-through borrow). `evaluate_all`'s
  four kernel calls route through them. Inline tests pin the R-TC1-1 probes, the all-invalid band
  shape, and a chained TRANGE → EMA through `TaEvaluator`; `interior_nan_after_the_start_still_propagates`
  pins that an interior NaN still propagates as in C (TRANGE costs two rows, ATR(2) stays all-NaN).
  A clean input costs one comparison per series and passes the borrowed slices straight through.
  Verifier fold: `evaluate_all_all_invalid_input_answers_nan_for_{ad,plus_dm,aroon_up}` (all-NaN
  and all-NULL through `evaluate_all`; base answered 0.0 there) and
  `evaluate_all_one_input_entirely_invalid_answers_all_nan_for_trange` (one input all-NaN makes
  the start equal the length); the chain test carries a NULL twin. pins: ta-chain-1/C-007
  pins: ta-chain-1/C-004, C-006
- `glue.rs` — TA-SINGLE-SERIES-PARALLEL-1 S2b (2026-10-04): the evaluator's zero-copy output and
  input borrow. `float64_array_from_vec` moves the kernel's `Vec<f64>` into a `Float64Array`
  (no copy; the old `Float64Builder` copy is gone). `try_borrow_all_null_free` borrows every
  series argument when all are null-free `Float64`, so multi-series functions (ADX, TRANGE, ATR,
  WILLR, STOCH, …) no longer densify-copy their inputs; any NULL or non-`Float64` input still
  densifies through `mod.rs`. `try_borrow_null_free_f64` moved here unchanged. `udf/mod.rs` shrinks
  1818 → 1801 and its exact baseline ratchets with it. The inline test pins the borrow pointers, the
  moved buffer, and bit-identity against the old densify-and-copy glue for ADX, TRANGE, ATR, WILLR,
  EMA, RSI and both STOCH bands. pins: ta-series-s2b/C-007
  **S2a (2026-10-05):** the native `null_lookback`. `TaWindowUdf` carries `null_prefix` and a
  derived `display` name (`ta_ema_null_prefix_12` when non-zero, the plain name otherwise);
  its `PartialEq` / `Hash` live here and cover the base name, function, signature and
  `null_prefix`, so two prefixes never compare or hash equal and DataFusion never merges them.
  `make_udf` moved here from `mod.rs`; `window_udf_with_null_prefix` is the facade's
  constructor and `window_udf` delegates to it with 0; `window_udfs` / `register_all` (the SQL
  path) stay unprefixed. `with_null_prefix` wraps the `TaEvaluator` in a `NullPrefixEvaluator`
  only when the prefix is non-zero: it runs the evaluator, then sets the first `null_prefix`
  rows **of each window partition** NULL in the validity bitmap over the same values buffer
  (no copy; values, and every later NaN, unchanged). `is_ta_window` tells the facade's series
  rewrite that a window function is a TA kernel. `udf/mod.rs` shrinks 1801 → 1791 and its exact
  baseline and the CAP-1 mirror ratchet with it. pins: ta-series-s2a/C-005, C-006, C-008
- `price.rs` — price-transform family (`avgprice`/`medprice`/`typprice`/
  `wclprice`).

## I want to...

| ...do this | go to |
|---|---|
| Add a window UDF | SPECS + `TaFn` in `mod.rs` + the matching family `compute` arm |
| Touch cache / densify / evaluate_all | `mod.rs` |
| Touch overlap dispatch | `overlap.rs` |
| Touch momentum dispatch | `momentum.rs` |
| Touch volatility / volume / price dispatch | the matching sibling |
| Change how a leading NaN/NULL run is skipped | `prefix.rs` (semantics: polars_talib 0.1.5, `task/wo/ta-chain-1-leading-prefix.md` R-TC1-1) |
| Change kernel arithmetic | the kernel file in `../` — not these wrappers |

## Pointers

- Up: [../map.md](../map.md)
- Extension install: [../extension/map.md](../extension/map.md)
- Goldens: [../../tests/map.md](../../tests/map.md)

## Debug

| Symptom | First check |
|---|---|
| `ta_*` unknown after register | SPECS row in `mod.rs` — `register_all` iterates `window_udfs()` |
| Bit mismatch vs the kernel | Family `compute` arm vs the public kernel; never edit goldens |
| Three BBANDS columns recompute | Check the TLS cache in `mod.rs`; sibling calls must share the pinned entry |
| `invalid udf family dispatch` | A family `compute` table dropped a variant the router still sends; add the arm |
| Chained indicator all-NaN (e.g. `ta_ema` over a `ta_trange` column) | `prefix.rs` — the input's leading NaN run must be skipped before the kernel; check the call site in `evaluate_all` routes through `run_with_prefix_skipped` / `run_all_with_prefix_skipped` |
| `ta_ema` unknown after `TaExtension::register` | Same SPECS row — not an extension bug ([../extension/map.md](../extension/map.md)) |

First checks: `cargo test -p repark-ta --features datafusion udf::`. Escalate to:
[../map.md#debug](../map.md#debug).
