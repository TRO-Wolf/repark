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
| `ta_ema` unknown after `TaExtension::register` | Same SPECS row — not an extension bug ([../extension/map.md](../extension/map.md)) |

First checks: `cargo test -p repark-ta --features datafusion udf::`. Escalate to:
[../map.md#debug](../map.md#debug).
