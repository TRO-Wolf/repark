# TA-CHAIN-1 — chained TA indicators answer where polars_talib answers: a leading NaN/NULL run on a `ta.*` input is skipped instead of propagating

**Filed:** 2026-10-03 by a Claude session (claude-fable-5-1) on the owner's ruling of the same day ("Can we add it to the v1.5.2?"). **Target: v1.5.2.** Order: [`task/wo/ta-chain-1-leading-prefix.md`](../../wo/ta-chain-1-leading-prefix.md), grade B.

## What the user writes

```python
from repark import Window
from repark.spark import ta

w = Window.partitionBy("contract_symbol").orderBy("event_timestamp_utc")
df = df.withColumns(ta.over_columns(w, {"tr": ta.trange("high", "low", "close")}))
df = df.withColumns(ta.over_columns(w, {"etr21": ta.ema("tr", timeperiod=21)}))
```

Today `etr21` is NaN on every row. Measured on 2026-10-03 against a 1,000,000-row, 14-contract futures file: 999,706 of 1,000,000 rows NaN where polars_talib returns finite values; every other compared column (`ema21`, `tr`, `rsi13`, `sma34`, `lr5`, `adx21`) was bit-identical between the two.

## Cause

TRANGE has a one-row lookback, so its output begins with one NaN per partition. TA-Lib C propagates a NaN through EMA (and every recursive kernel) forever, and `repark-ta` is bit-faithful to C. polars_talib, which is the crate's golden oracle, skips each input's leading invalid run before calling the C kernel, so chained indicators work there. The wrapper layer in `crates/repark-ta/src/udf/` is where the two diverge; the kernels do not.

## polars_talib semantics (measured, 0.1.5 over polars 1.43.1)

- Each input's leading run of NULL or NaN is skipped; NULL and NaN behave identically.
- With several inputs the kernel starts at the latest first-valid index across all of them.
- Output rows before that index plus the kernel's own lookback are NaN.
- An interior NaN after the start propagates exactly as in C (`atr` over a series with a NaN in row 2 is all-NaN).

## Scope

Wrapper only, both doors (SQL `ta_*(…) OVER (…)` and Python `ta.*`), goldens recorded through polars_talib into a separate directory, two facade pins, one chained column in the composition example, perf cap 1.02x on the single-symbol kernel race. The kernel layer, the existing 158 goldens, interior-NaN propagation and the `null_lookback` row arithmetic are out of scope and named so in the order.

## Release note (v1.5.2)

Chained TA indicators (an indicator computed from another indicator's output) now answer where polars_talib answers; a leading NaN or NULL run on any `ta.*` input is skipped instead of propagating.
