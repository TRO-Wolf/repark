# map — docs/examples/ta/

## Purpose

Worked examples for `repark.spark.ta` kernels. Examples construct the session
as `repark = ReparkSession.builder…`; see [../map.md](../map.md).

Spark has no TA kernels, so this family's oracle is the recorded C TA-Lib 0.4.0
goldens under `crates/repark-ta/tests/goldens/` — the same little-endian `f64`
`.bin` files `python/repark/tests/test_ta.py` and `test_ta_volume.py` pin
bit-identically, recorded over the 5000-row OHLCV fixture
(`fixture_{open,high,low,close,volume}.bin`). Each example rebuilds that fixture
as a `createDataFrame`, runs the kernel over `Window.orderBy("ts")`, and asserts
the full 5000-row output bit-for-bit against the golden read from the `.bin` at
run time (`expect_bit_exact`: equal length, NaN rows matched positionally, every
other row by `f64` bit pattern — the same property `assert_bit_exact` holds in
[crates/repark-ta/tests/goldens.rs](../../../crates/repark-ta/tests/goldens.rs));
no hand-computed number appears in any example. No JVM and no network. EX-23
(2026-09-04) landed the first 40 backlog names; all 40 measured bit-identical to
their goldens, so none stayed on the backlog and no registry §7 row was filed.
EX-24 (2026-09-04) landed the remaining 45; all 45 measured bit-identical too,
so the `ta.*` backlog row set is empty and still no §7 row exists. The
composition helpers are taught through fused examples whose every produced
column is asserted bit-exact against a golden.

## Contents

- [sma.py](sma.py) — `ta.sma` over an ordered window on a local frame.
- [overlap_studies.py](overlap_studies.py) — `ta.ema`, `ta.dema`, `ta.kama`,
  `ta.ma` (SMA selector), the three `ta.bbands_*` bands, and `ta.fama`.
- [ma_variants.py](ma_variants.py) — `ta.mama`, `ta.mavp` (two-series, over the
  `fixture_periods` column), `ta.midpoint`, `ta.midprice`, `ta.t3`, `ta.tema`,
  `ta.trima`, and `ta.wma` (EX-24).
- [rolling_extremes.py](rolling_extremes.py) — `ta.MAX`, `ta.MIN`, `ta.SUM`
  (the TA-Lib-name aliases of the rolling extremes).
- [math_operators.py](math_operators.py) — `ta.max`, `ta.min`, `ta.sum` (the
  lowercase spellings, at period 21 like the aliases) (EX-24).
- [momentum.py](momentum.py) — `ta.apo`, `ta.aroon_down`, `ta.aroon_up`,
  `ta.aroonosc`, `ta.bop`, `ta.cci`, `ta.cmo`, and the directional-movement
  trio `ta.adx`, `ta.adxr`, `ta.dx`.
- [rate_of_change.py](rate_of_change.py) — `ta.mom`, `ta.roc`, `ta.rocp`,
  `ta.rocr`, `ta.rocr100` (EX-24).
- [oscillators.py](oscillators.py) — `ta.ppo`, `ta.rsi`, `ta.trix`,
  `ta.ultosc`, `ta.willr` (EX-24).
- [stochastics.py](stochastics.py) — the three stochastic variants and their
  split outputs: `ta.stoch_slowk`/`ta.stoch_slowd`, `ta.stochf_fastk`/
  `ta.stochf_fastd`, `ta.stochrsi_fastk`/`ta.stochrsi_fastd` (EX-24).
- [directional_movement.py](directional_movement.py) — `ta.plus_di`,
  `ta.plus_dm`, `ta.minus_di`, `ta.minus_dm` (EX-24).
- [sar.py](sar.py) — `ta.sar` and `ta.sarext` at the recorder's ten-parameter
  spelling (EX-24).
- [macd.py](macd.py) — the MACD family: `ta.macd`, `ta.macdext`, `ta.macdfix`
  and their `_signal`/`_hist` split outputs, one select per variant.
- [regression.py](regression.py) — `ta.linearreg`, `ta.linearreg_slope`,
  `ta.linearreg_intercept`, `ta.linearreg_angle`, `ta.beta`, `ta.correl`.
- [statistics.py](statistics.py) — `ta.stddev`, `ta.tsf`, `ta.var` (EX-24).
- [volatility.py](volatility.py) — `ta.atr`.
- [true_range.py](true_range.py) — `ta.natr` and `ta.trange` (EX-24).
- [volume.py](volume.py) — `ta.ad` and `ta.adosc`.
- [volume_flow.py](volume_flow.py) — `ta.mfi` and `ta.obv` (EX-24).
- [price_transforms.py](price_transforms.py) — `ta.avgprice`.
- [price_averages.py](price_averages.py) — `ta.medprice`, `ta.typprice`,
  `ta.wclprice` (EX-24).
- [composition.py](composition.py) — `ta.over_columns` and
  `ta.with_indicators` fusing several kernels in one window; every produced
  column (`ema_5`, `trima_5`, `rsi_3`, `min_34` goldens) is asserted bit-exact
  (EX-24).

## Pointers

- Up: [../map.md](../map.md)
- Goldens: [../../../crates/repark-ta/tests/goldens/map.md](../../../crates/repark-ta/tests/goldens/map.md)
- Pins: [../../../python/repark/tests/test_ta.py](../../../python/repark/tests/test_ta.py)
