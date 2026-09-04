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

## Contents

- [sma.py](sma.py) — `ta.sma` over an ordered window on a local frame.
- [overlap_studies.py](overlap_studies.py) — `ta.ema`, `ta.dema`, `ta.kama`,
  `ta.ma` (SMA selector), the three `ta.bbands_*` bands, and `ta.fama`.
- [rolling_extremes.py](rolling_extremes.py) — `ta.MAX`, `ta.MIN`, `ta.SUM`
  (the TA-Lib-name aliases of the rolling extremes).
- [momentum.py](momentum.py) — `ta.apo`, `ta.aroon_down`, `ta.aroon_up`,
  `ta.aroonosc`, `ta.bop`, `ta.cci`, `ta.cmo`, and the directional-movement
  trio `ta.adx`, `ta.adxr`, `ta.dx`.
- [macd.py](macd.py) — the MACD family: `ta.macd`, `ta.macdext`, `ta.macdfix`
  and their `_signal`/`_hist` split outputs, one select per variant.
- [regression.py](regression.py) — `ta.linearreg`, `ta.linearreg_slope`,
  `ta.linearreg_intercept`, `ta.linearreg_angle`, `ta.beta`, `ta.correl`.
- [volatility.py](volatility.py) — `ta.atr`.
- [volume.py](volume.py) — `ta.ad` and `ta.adosc`.
- [price_transforms.py](price_transforms.py) — `ta.avgprice`.

## Pointers

- Up: [../map.md](../map.md)
- Goldens: [../../../crates/repark-ta/tests/goldens/map.md](../../../crates/repark-ta/tests/goldens/map.md)
- Pins: [../../../python/repark/tests/test_ta.py](../../../python/repark/tests/test_ta.py)
