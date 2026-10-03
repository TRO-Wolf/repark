# map — repark-ta/tests/goldens/prefix

## Purpose

TA-CHAIN-1 leading-run goldens: polars_talib 0.1.5 (C TA-Lib 0.4.0) outputs over the prefix
fixture, the 5000-row walk fixture with each column's leading rows overwritten by NaN (open 0,
high 3, low 7, close 5, periods 0, volume 2). They pin the window-UDF wrapper's leading-run skip
(`crates/repark-ta/src/udf/prefix.rs`), not the kernels. Same encoding as the parent directory:
raw little-endian `f64` bit patterns, one u64 per row, nulls recorded as NaN.

## Contents

- `manifest.json` — oracle versions + the 13 series → row count. Kept apart from
  `../manifest.json` so `manifest_and_tests_cover_the_same_series` in `../../goldens.rs` is
  untouched (R-TC1-4). The 158 kernel goldens re-record byte-identical when the recorder runs.
  pins: ta-chain-1/C-005
- `prefix_ema_21`, `prefix_sma_10`, `prefix_rsi_14`, `prefix_linearreg_5`,
  `prefix_bbands_upper_20`, `prefix_macd_12_26_9` (the MACD line) — single-input kernels, start 5.
- `prefix_adx_14`, `prefix_trange`, `prefix_atr_14`, `prefix_stoch_slowk` (polars_talib
  defaults) — H/L/C kernels, start 7 (the latest first-valid row across the inputs).
- `prefix_obv` (close + volume, start 5), `prefix_mavp_sma` (close + periods, min 5 / max 20 /
  matype 0, start 5).
- `prefix_chain_ema21_of_trange` — `ema(trange(h, l, c), 21)`, the chained indicator.

**Do not hand-edit.** The recorder regenerates every file deterministically.

## Pointers

- Up: [../map.md](../map.md)
- Gate: `../../prefix_goldens.rs`
- Recorder: `python/repark-parity/record_ta_goldens.py` (`prefix_fixture`, `prefix_cases`)

## Debug

A `prefix_goldens` failure on length means the wrapper lost the NaN re-prefix; a failure on every
series means the start index is not being computed. Either way check `src/udf/prefix.rs` first;
the kernels are not the cause. Escalate to [../../../map.md#debug](../../../map.md).
