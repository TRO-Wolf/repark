# map — repark-ta/tests/goldens

## Purpose

Recorded golden fixtures for the bit-exactness gate: raw little-endian `f64` bit patterns
(one u64 per row; oracle nulls recorded as NaN), produced by
`python/repark-parity/record_ta_goldens.py` from C TA-Lib 0.4.0 (via `polars-talib 0.1.5`).

## Contents

- `manifest.json` — the recorder's ledger: oracle versions + every series → row count. The
  `manifest_and_tests_cover_the_same_series` test keeps it in two-way sync with
  `../goldens.rs`; the loader checks each file's size against it.
- `fixture_{open,high,low,close}.bin` — the 5000-row deterministic OHLC random walk (numpy
  `default_rng(42)`), the shared input for the walk series.
- `fixture_periods.bin` — the 5000-row deterministic per-row period series (cycles 2..30) that is
  MAVP's second input column.
- `fixture_flat_{open,high,low,close}.bin` — the 600-row flat-plateau series (`default_rng(7)`;
  300 dead-flat bars) that drives the `TA_IS_ZERO` guard branches (`open` added for BOP).
- `<indicator>_<params>.bin` / `flat_<indicator>_<params>.bin` — one file per indicator ×
  param-set (148 series total, incl. the T3 parked four — `mama_mama`/`mama_fama`/`flat_mama_*`,
  `sar`, `sarext`/`sarext_long_offset`/`sarext_short`, `mavp`/`mavp_ema`, `ma_30_type7` — the
  matype-7 APO/PPO/MACDEXT set: `apo_12_26_type7`, `ppo_12_26_type7`, `macdext_12_26_9_type7_*`,
  `macdext_mixed_7_0_1_*` — and Group G2 stochastic matype-7:
  `stoch_type7_*`/`stoch_mixed_7_0_*`/`stochf_type7_*`/`stochrsi_type7_*`; the authoritative list is
  `manifest.json`).

**Do not hand-edit.** Re-record only when adding series or deliberately moving the oracle —
the recorder asserts both oracle versions and writes atomically (temp + rename).

## Pointers

- Up: [../map.md](../map.md)

## Debug

Size mismatch vs `manifest.json` → stale mix; re-run the recorder (it regenerates every file
deterministically). Everything else: [../../map.md#debug](../../map.md).
