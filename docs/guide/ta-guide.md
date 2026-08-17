# Technical analysis

`repark.ta` is a technical-analysis library built into the engine: 68 TA-Lib kernels, hand-ported
to Rust, exposed as window functions you can drop into a `withColumn` or an `OVER (...)` clause. It
is a **repark extension** — PySpark has no equivalent — so nothing in this guide is a parity claim
against Apache Spark. The parity claim it *does* make is against TA-Lib C 0.4.0, and this guide
states it exactly as the repo does.

## The shape

`repark.ta` lives at `repark.spark.ta`, so the mechanical `pyspark` → `repark.spark` swap lands on
it. Every `ta.*` function returns an **un-`OVER`ed** `Column`: the kernels are *stateful,
full-series* functions — every value depends on the whole ordered history — so a column is only
complete once a window supplies the ordering.

```python
from repark import ReparkSession
from repark.spark import Window, ta

spark = ReparkSession.builder.appName("ta").getOrCreate()

bars = [
    ("AAA", 1, 10.0, 10.5,  9.5, 10.2, 1000.0),
    ("AAA", 2, 10.2, 11.0, 10.0, 10.8, 1200.0),
    ("AAA", 3, 10.8, 11.5, 10.6, 11.4,  900.0),
    ("AAA", 4, 11.4, 11.9, 11.0, 11.1, 1500.0),
    ("AAA", 5, 11.1, 11.3, 10.4, 10.6, 1100.0),
    ("BBB", 1, 50.0, 51.0, 49.0, 50.5,  400.0),
    ("BBB", 2, 50.5, 52.0, 50.0, 51.8,  600.0),
    ("BBB", 3, 51.8, 52.5, 51.0, 51.2,  300.0),
    ("BBB", 4, 51.2, 51.6, 50.2, 50.4,  700.0),
    ("BBB", 5, 50.4, 50.9, 49.8, 50.8,  800.0),
]
cols = ["symbol", "ts", "open", "high", "low", "close", "volume"]
df = spark.createDataFrame(bars, cols)

w = Window.partitionBy("symbol").orderBy("ts")
df.withColumn("sma3", ta.sma("close", timeperiod=3).over(w)).select(
    "symbol", "ts", "close", "sma3"
).orderBy("symbol", "ts").show()
```

```text
+--------+----+-------+--------------------+
| symbol | ts | close | sma3               |
+--------+----+-------+--------------------+
| AAA    | 1  | 10.2  | nan                |
| AAA    | 2  | 10.8  | nan                |
| AAA    | 3  | 11.4  | 10.799999999999999 |
| AAA    | 4  | 11.1  | 11.1               |
| AAA    | 5  | 10.6  | 11.033333333333331 |
| BBB    | 1  | 50.5  | nan                |
| BBB    | 2  | 51.8  | nan                |
| BBB    | 3  | 51.2  | 51.166666666666664 |
| BBB    | 4  | 50.4  | 51.13333333333333  |
| BBB    | 5  | 50.8  | 50.800000000000004 |
+--------+----+-------+--------------------+
```

Two rules follow from "the kernel needs the whole ordered history":

- **Ordering is yours to supply.** Without an `ORDER BY` inside `.over(...)` the partition order is
  undefined, exactly as in Spark.
- **So is partitioning.** `Window.orderBy("ts")` alone treats every symbol as one series, so RSI,
  EMA and MACD silently leak across instruments that share timestamps. That footgun is the reason
  the serving helper below refuses to guess column names.

The call shape mirrors TA-Lib / `polars_talib` so notebook code ports by import swap: named keyword
parameters (`timeperiod`, `nbdevup` / `nbdevdn`, `matype`, …), the price series as either a
`Column` or a bare column-name `str`, TA-Lib's own defaults, and multi-output indicators split into
one function per output (`bbands_upper` / `bbands_middle` / `bbands_lower`).

## `over_columns` — many indicators, one pass

Chaining N `withColumn` calls is not the shape to reach for. `ta.over_columns(window, {...})`
attaches one shared `WindowSpec` to a whole dict of indicators, ready for `withColumns`, and
DataFusion emits **one** `WindowAggExec` for the group:

```python
out = df.withColumns(
    ta.over_columns(
        w,
        {
            "sma3": ta.sma("close", timeperiod=3),
            "rsi3": ta.rsi("close", timeperiod=3),
            "atr3": ta.atr("high", "low", "close", timeperiod=3),
        },
    )
)
out.select("symbol", "ts", "sma3", "rsi3", "atr3").orderBy("symbol", "ts").show()
```

```text
+--------+----+--------------------+--------------------+--------------------+
| symbol | ts | sma3               | rsi3               | atr3               |
+--------+----+--------------------+--------------------+--------------------+
| AAA    | 1  | nan                | nan                | nan                |
| AAA    | 2  | nan                | nan                | nan                |
| AAA    | 3  | 10.799999999999999 | nan                | nan                |
| AAA    | 4  | 11.1               | 79.99999999999999  | 0.9333333333333336 |
| AAA    | 5  | 11.033333333333331 | 53.333333333333336 | 0.9222222222222225 |
| BBB    | 1  | nan                | nan                | nan                |
| BBB    | 2  | nan                | nan                | nan                |
| BBB    | 3  | 51.166666666666664 | nan                | nan                |
| BBB    | 4  | 51.13333333333333  | 48.148148148148124 | 1.6333333333333329 |
| BBB    | 5  | 50.800000000000004 | 57.575757575757535 | 1.4555555555555557 |
+--------+----+--------------------+--------------------+--------------------+
```

Since the plan-collapse work, sequential *independent* same-spec `withColumn` calls also merge into
that one operator; only *dependent* stacks (a TA column consumed by a later TA window) still emit
stacked operators, by design. `over_columns` stays the preferred spelling because it does not rely
on the optimizer noticing.

Each value in the dict must be an **un-windowed** column — pass the bare indicator and let the
helper attach the window. Calling `.over` twice fails at the native binder.

## `with_indicators` — the serving door

`ta.with_indicators` is the ETL / serving helper. `partition` and `order` are keyword-only with
**no defaults that guess column names**, so a pipeline cannot forget `partitionBy`.
`last_row=True` keeps the last bar per partition, so a serving call collects `N_symbols` rows
instead of the whole history:

```python
serving = ta.with_indicators(
    df,
    partition="symbol",
    order="ts",
    columns={"sma3": ta.sma("close", timeperiod=3), "rsi3": ta.rsi("close", timeperiod=3)},
    last_row=True,
)
serving.select("symbol", "ts", "sma3", "rsi3").orderBy("symbol").show()
```

```text
+--------+----+--------------------+--------------------+
| symbol | ts | sma3               | rsi3               |
+--------+----+--------------------+--------------------+
| AAA    | 5  | 11.033333333333331 | 53.333333333333336 |
| BBB    | 5  | 50.800000000000004 | 57.575757575757535 |
+--------+----+--------------------+--------------------+
```

Omit `partition` and the signature itself stops you, rather than a window quietly spanning every
symbol:

```python
ta.with_indicators(df, order="ts", columns={"sma3": ta.sma("close", timeperiod=3)})
```

```text
TypeError: with_indicators() missing 1 required keyword-only argument: 'partition'
```

## The lookback prefix

TA-Lib kernels emit a NaN prefix for the rows before an indicator is defined — the `nan` cells in
the outputs above — and repark reproduces it bit for bit, because that is what the goldens pin.
Pass `null_lookback=True` to convert **only that deterministic prefix** to SQL `NULL` after
`.over(...)`, matching `polars_talib`'s null surface:

```python
df.withColumn("sma3", ta.sma("close", timeperiod=3, null_lookback=True).over(w)).select(
    "symbol", "ts", "sma3"
).orderBy("symbol", "ts").show()
```

```text
+--------+----+--------------------+
| symbol | ts | sma3               |
+--------+----+--------------------+
| AAA    | 1  | NULL               |
| AAA    | 2  | NULL               |
| AAA    | 3  | 10.799999999999999 |
| AAA    | 4  | 11.1               |
| AAA    | 5  | 11.033333333333331 |
| BBB    | 1  | NULL               |
| BBB    | 2  | NULL               |
| BBB    | 3  | 51.166666666666664 |
| BBB    | 4  | 51.13333333333333  |
| BBB    | 5  | 50.800000000000004 |
+--------+----+--------------------+
```

The rewrite is by **row position** (`row_number() <= lookback`), never a blanket `isnan`, so a
mid-series NaN is never rewritten — a hole in your data still shows as a hole. The default is
`False` and leaves the kernel output byte-unchanged. `with_indicators(..., null_lookback=True)`
threads the same rewrite through every column in the dict.

## The indicator set

**81 entry points over 68 kernels** — a multi-output kernel (BBANDS, MAMA, AROON, the MACD family,
the stochastics) is split one function per output. Grouped as TA-Lib groups them:

| Group | Functions |
|---|---|
| Overlap studies | `sma` `ema` `wma` `dema` `tema` `trima` `kama` `t3` `ma` `midpoint` `midprice` `bbands_upper` `bbands_middle` `bbands_lower` `mama` `fama` `sar` `sarext` `mavp` |
| Momentum | `rsi` `mom` `roc` `rocp` `rocr` `rocr100` `willr` `cci` `cmo` `bop` `apo` `ppo` `aroon_up` `aroon_down` `aroonosc` `trix` `ultosc` |
| Directional movement | `adx` `adxr` `dx` `plus_di` `minus_di` `plus_dm` `minus_dm` |
| MACD family | `macd` `macd_signal` `macd_hist` `macdfix` `macdfix_signal` `macdfix_hist` `macdext` `macdext_signal` `macdext_hist` |
| Stochastics | `stoch_slowk` `stoch_slowd` `stochf_fastk` `stochf_fastd` `stochrsi_fastk` `stochrsi_fastd` |
| Volatility | `trange` `atr` `natr` |
| Statistics | `var` `stddev` `linearreg` `linearreg_slope` `linearreg_intercept` `linearreg_angle` `tsf` `correl` `beta` |
| Price transforms | `avgprice` `medprice` `typprice` `wclprice` |
| Volume | `ad` `adosc` `obv` `mfi` |
| Math operators | `min` `max` `sum` (also `MIN` / `MAX` / `SUM`) |

`min` / `max` / `sum` deliberately shadow the builtin spellings *inside* `repark.ta`, because
TA-Lib and `polars_talib` name them that way; `F.min` in `repark.functions` is untouched. The
uppercase aliases are the same function objects (`ta.MIN is ta.min` → `True`), there for
import-swap fidelity with the C function names.

`matype` parameters take TA-Lib's MA-type codes — 0 SMA, 1 EMA, 2 WMA, 3 DEMA, 4 TEMA, 5 TRIMA,
6 KAMA, 7 MAMA, 8 T3 — and a window period must be a whole number: a non-integral value fails loud
rather than silently truncating.

## From SQL

The same kernels are registered as window UDFs on the Spark door, named `ta_<function>`:

```python
df.createOrReplaceTempView("bars")
spark.sql(
    "SELECT symbol, ts, ta_ema(close, 3) OVER (PARTITION BY symbol ORDER BY ts) AS ema3 "
    "FROM bars ORDER BY symbol, ts"
).show()
```

```text
+--------+----+--------------------+
| symbol | ts | ema3               |
+--------+----+--------------------+
| AAA    | 1  | nan                |
| AAA    | 2  | nan                |
| AAA    | 3  | 10.799999999999999 |
| AAA    | 4  | 10.95              |
| AAA    | 5  | 10.774999999999999 |
| BBB    | 1  | nan                |
| BBB    | 2  | nan                |
| BBB    | 3  | 51.166666666666664 |
| BBB    | 4  | 50.78333333333333  |
| BBB    | 5  | 50.791666666666664 |
+--------+----+--------------------+
```

They are **not** on the native door. `repark.sql` runs a stock DataFusion session with no repark
extension composed, so the TA UDFs are simply not registered there:

```python
import repark

repark.sql("SELECT ta_ema(1.0, 3)")
```

```text
AnalysisException: Error during planning: Invalid function 'ta_ema'.
Did you mean 'to_char'?
```

See [sql-doors.md](sql-doors.md) for why the two doors do not share a session.

## The parity claim, stated exactly

The kernels are **bit-exact hand-ports of the TA-Lib C 0.4.0 algorithms**. No C is compiled, linked
or vendored, and no third-party TA crate is used. The claim is enforced rather than asserted:
`crates/repark-ta/tests/goldens.rs` compares **`f64::to_bits` equality per element** (NaN ↔ NaN
allowed) for every kernel × parameter set, across 158 recorded series over two fixtures — a
5000-row lognormal walk and a 600-row flat plateau that drives the epsilon-guard branches. There
are no tolerance comparisons in that gate: if a golden fails, the kernel drifted or the oracle
moved, never "close enough".

The oracle is the C TA-Lib 0.4.0 binary bundled with `polars_talib` 0.1.5, and the recorder asserts
both versions before it writes, so oracle drift is caught rather than absorbed.

Two caveats the crate states about its own claim:

- `linearreg_angle` may differ by a few ulp off glibc x86-64, because `atan` is not required by the
  C standard to be correctly rounded. The goldens are recorded and tested on glibc x86-64.
- TA oracle divergences are documented **in-crate**
  ([`crates/repark-ta/map.md`](../../crates/repark-ta/map.md)), which is their authoritative home.
  They are not rows in [the divergence registry](../spark-sql-iceberg-parity.md) — that file
  records differences from *Apache Spark*. A TA question is answered by the crate, not by it.

## Declaring a frame already sorted

Windowed TA is dominated by sorting. `df.declareSorted(...)` is a repark extension that tells the
engine a `createDataFrame` source frame is *already* sorted by the given keys, so DataFusion can
drop the redundant sort — the sort is O(n log n) on every query, the check below is O(n) once.

```python
spark.createDataFrame(bars, cols).declareSorted("symbol", "ts")   # returns self, so it chains
```

**It always verifies.** There is no unverified fast path and no "trust me" switch: declaring runs
an adjacent-pair lexicographic scan over the sort keys, across batch boundaries. A claim the data
disagrees with raises and names the offending rows, and the view is left exactly as it was.

```python
unsorted = spark.createDataFrame([("AAA", 3, 1.0), ("AAA", 1, 2.0)], ["symbol", "ts", "close"])
unsorted.declareSorted("symbol", "ts")
```

```text
AnalysisException: declared-sorted view: rows 0 and 1 are out of order for keys [symbol, ts]
(ASC NULLS LAST) — the data is not sorted as declared
```

The door is deliberately narrow, and every fence refuses loudly rather than quietly doing nothing:

- **Source frames only** — the frame `createDataFrame` handed back. Declare on the source, then
  transform.

  ```python
  spark.createDataFrame(bars, cols).filter("ts > 0").declareSorted("symbol", "ts")
  ```

  ```text
  PySparkValueError: declareSorted applies to source frames only — the frame createDataFrame
  returned, whose rows are already materialized in memory. This frame is a transform of one (or a
  cache/SQL result); declare on the source frame and transform afterwards.
  ```

- **Before `cache()` / `persist()` / `checkpoint`** — caching redirects the frame to a different
  registered view, so declaring afterwards would silently detach the declaration:

  ```python
  spark.createDataFrame(bars, cols).cache().declareSorted("symbol", "ts")
  ```

  ```text
  PySparkValueError: declareSorted must run before cache()/persist()/checkpoint on this frame —
  caching redirects the frame to a cache view, and declaring afterwards would detach it. Call
  declareSorted first, then cache.
  ```

- **In-memory views only.** Parquet `file_sort_order` and Iceberg sort-order metadata are not part
  of this door; a non-`MemTable` provider refuses.

Names resolve case-insensitively through the same machinery `select` uses, so a mixed-case column
declares under any spelling; declaring twice is idempotent; the call returns `self` so it chains.

### The boundary: null placement decides

The engine declares **`ASC NULLS LAST`** per key — DataFusion's `ORDER BY` default. Spark's
`ORDER BY x ASC` is **`NULLS FIRST`**, and repark's `WindowSpec` follows Spark. For a *nullable*
key those are genuinely different orderings, so DataFusion correctly declines to reuse one for the
other. That single fact explains every cell below.

Measured on this build, `repark.target.partitions=1`, three symbols × 300 bars, counting `SortExec`
in the physical plan of the same windowed query:

| window ordering | undeclared | declared |
|---|---|---|
| SQL `ORDER BY ts ASC NULLS LAST` | `SortExec` ×1 | **×0** |
| SQL `ORDER BY ts` | ×1 | **×0** |
| SQL `ORDER BY ts ASC NULLS FIRST` | ×1 | ×1 |
| `Window.partitionBy("symbol").orderBy("ts")` | ×1 | ×1 |

Results are identical in every cell. The declared scan advertises the ordering it was given:

```text
DataSourceExec: partitions=1, partition_sizes=[1],
output_ordering=symbol@0 ASC NULLS LAST, ts@1 ASC NULLS LAST
```

So: **the default door is a pure hint** — SQL windows spelled `NULLS LAST` elide, and the
DataFrame `Window` spec over a *nullable* key does not, because Spark's ascending default is
`NULLS FIRST`. That is still the table above.

`declareSorted(..., tightenNulls=True)` is the opt-in that closes the serving-shape gap. After
the same always-verify scan, a NULL in a declared key refuses (name the key; drop
`tightenNulls` or clean the data). Otherwise those keys become non-nullable on the in-engine
schema (`df.schema`, `to_arrow()`), which is the lever DataFusion needs to treat every null
placement as compatible. That is a plan property the caller asked for by typing the flag, not
a data-contract change: Iceberg CREATE of a tightened frame is refused until the write-boundary
relax (PR-D2) lands. A later `declareSorted(...)` without the flag restores the original
nullability.

```python
spark.createDataFrame(bars, cols).declareSorted("symbol", "ts", tightenNulls=True)
```

## Benchmarking TA

Do not benchmark TA against numbers quoted in a guide. The measurement code and its contract live
in [`python/repark-parity/bench/ta/map.md`](../../python/repark-parity/bench/ta/map.md), which is
the SSOT for the battery; transcribed numbers stay planning-side and are deliberately not restated
here.

Three parts of that contract matter even if you only ever run your own timings:

- **Default conf is the primary.** A primary measurement leaves `repark.target.partitions` unset —
  the engine then uses the machine's cores. An explicit `target_partitions=1` cell is
  **single-core isolation only** and must be labelled as such; reporting it as a default-conf
  number misstates the result in whichever direction suits.
- **Engine knobs are fixed at `getOrCreate`.** `repark.target.partitions` and `batch_size` cannot
  be changed on a live session, so every cell of a sweep must build a *new* session. A sweep that
  reuses one session is measuring one configuration N times. See
  [session-and-conf.md](session-and-conf.md) for which keys are build-time.
- **Measure a release build.** A debug wheel is 10–50× slower and will send you chasing a phantom.

## The allocator

The wheel build links **mimalloc** — `[tool.maturin] features` in the facade `pyproject.toml`,
wired after the AL-1b A/B verdict measured 7 of 9 default-conf primaries ≥5% faster (the serving
shapes by considerably more), with no primary regression, and goldens plus the facade suite green
under it. There is nothing to switch on and no user action of any kind; it is a property of how the
wheel is built. Which published tag first carries it is release state —
[STATUS.md](../../STATUS.md) is the file that says.

## See also

- [dataframe-guide.md](dataframe-guide.md) — windows, `withColumns`, and the action table.
- [session-and-conf.md](session-and-conf.md) — the build-time engine knobs the bench contract
  depends on.
- [sql-doors.md](sql-doors.md) — why `ta_*` exists on one door and not the other.
- [`crates/repark-ta/map.md`](../../crates/repark-ta/map.md) — the kernel crate: the numerics
  contract, the golden gate, the known libm caveat.
- [`python/repark-parity/bench/ta/map.md`](../../python/repark-parity/bench/ta/map.md) — the
  benchmark battery and its measurement contract.
