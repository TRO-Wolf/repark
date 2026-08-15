"""The :mod:`repark.ta` facade — native technical-analysis indicators (TA-Lib parity).

Each function returns a :class:`repark.column.Column` carrying a **window function** over the
kernel that lives in the Rust ``repark-ta`` crate (a bit-exact port of TA-Lib C 0.4.0). The kernels
are *stateful, full-series* functions — every value depends on the whole ordered history — so the
column must be completed with an ``OVER`` window that supplies the ordering::

    from repark.spark import Window
    from repark.spark import ta

    w = Window.orderBy("ts")                      # or partitionBy("symbol").orderBy("ts")
    df.withColumn("ema21", ta.ema("close", timeperiod=21).over(w))
    df.withColumn("ema21", ta.ema(F.col("close"), timeperiod=21).over(w))  # Column form too

The call shape mirrors TA-Lib / ``polars_talib`` so notebook code ports by import swap: function
form with named keyword parameters (``timeperiod``, ``nbdev``, ``nbdevup``/``nbdevdn``), the price
series as either a :class:`~repark.column.Column` or a bare column-name ``str``, and multi-output
indicators split into one function per output (``bbands_upper`` / ``bbands_middle`` /
``bbands_lower``). Defaults match TA-Lib.

**Lookback nulls (opt-in).** Kernels emit a NaN lookback prefix (bit-exact with C TA-Lib /
``to_bits`` goldens). Pass ``null_lookback=True`` to convert only that deterministic prefix to
SQL NULL after ``.over(...)`` — matching ``polars_talib``'s null surface. Default is ``False``
(byte-unchanged). Mid-series NaN is never rewritten; distinction is by lookback length, never
blanket ``isnan``.

**Ordering is the caller's responsibility.** Without an ``ORDER BY`` inside ``.over(...)`` the
partition order is undefined, exactly as in Spark; always pass a window with an ordering.

# === r21 T4: ta-etl ===
**Windowed ETL throughput (measure-first, release wheel).** Same-spec TA windows in a single
``DataFrame.withColumns({...})`` lower to **one** DataFusion ``WindowAggExec`` (fused partition
sort + multi-UDF pass). Since r23b N2, sequential **independent** same-spec ``withColumn`` calls
also merge into that one operator (pinned in ``test_n2_plan_collapse.py``); only *dependent*
stacks (a TA column consumed by a later TA window) still emit stacked operators, by design.
:func:`over_columns` remains the preferred spelling — one fused pass, no reliance on the plan
optimizer. Hour-0 r21 T4: on the Arrow path the fused vs sequential gap is real but modest at
operator scale once ``withColumns`` is already used; kernel work dominates; collect
Row materialization is a separate surface (not this module). See ``task/t4-ta-etl-ledger.md``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from repark import _native
from repark.errors import PySparkTypeError
from repark.spark.column import Column
from repark.spark.functions import lit

if TYPE_CHECKING:
    from repark.spark.window import WindowSpec


def _series(value: Column | str) -> Column:
    """Coerce a price-series argument (a :class:`Column` or a column-name ``str``) to a Column."""
    if isinstance(value, Column):
        return value
    if isinstance(value, str):
        from repark.spark.functions import col

        return col(value)
    raise PySparkTypeError(f"expected a Column or column name (str), got {type(value).__name__}")


# ---- lookback lengths (deterministic prefix NaN count per kernel; TA-Lib / repark-ta) ------------
#
# Rust kernels emit NaN for the lookback prefix (bit-exact ``to_bits`` goldens). ``null_lookback``
# is a Python-side opt-in that converts ONLY that prefix to SQL NULL after ``.over(...)`` — by
# row position (``row_number() <= lookback``), never by blanket ``isnan``. Mid-series NaN is kept.


def _ma_lookback(period: int, matype: int) -> int:
    """TA-Lib ``MA_Lookback(period, matype)`` — matches ``repark_ta::momentum::ma_lookback``."""
    if period <= 1:
        return 0
    if matype in (0, 1, 2, 5):  # SMA / EMA / WMA / TRIMA
        return period - 1
    if matype == 3:  # DEMA
        return 2 * (period - 1)
    if matype == 4:  # TEMA
        return 3 * (period - 1)
    if matype == 6:  # KAMA
        return period
    if matype == 7:  # MAMA(0.5, 0.05) — fixed 32
        return 32
    if matype == 8:  # T3
        return 6 * (period - 1)
    # Unknown matype: kernel will reject; report a safe non-negative prefix.
    return period - 1 if period > 0 else 0


def _period_minus_one(period: int) -> int:
    """Lookback for SMA/EMA/WMA/BBANDS/MIN/MAX/… families: ``period - 1``."""
    return period - 1 if period > 0 else 0


def _nonneg(value: int) -> int:
    """Clamp ``value`` to ``>= 0`` without calling the shadowed :func:`max` TA wrapper."""
    return value if value > 0 else 0


def _max2(left: int, right: int) -> int:
    """Two-arg max without calling the shadowed :func:`max` TA wrapper."""
    return left if left >= right else right


class _NullLookbackColumn(Column):
    """TA window :class:`Column` that nulls the deterministic lookback prefix on ``.over``.

    Default (``null_lookback=False``) paths never construct this class — existing NaN-prefix
    goldens stay byte-unchanged. With the flag, ``.over(w)`` becomes::

        when(row_number().over(w) > lookback, ta_result.over(w))
        # rows 1..lookback → SQL NULL; later rows (incl. mid-series NaN) pass through
    """

    __slots__ = ("_lookback",)

    def __init__(self, inner: object, lookback: int) -> None:
        super().__init__(inner)
        self._lookback = lookback

    def over(self, window: object) -> Column:  # type: ignore[override]
        """Apply the window, then force SQL NULL on the lookback prefix only."""
        from repark.spark.functions import row_number, when
        from repark.spark.window import WindowSpec

        if not isinstance(window, WindowSpec):
            raise PySparkTypeError(f"expected a WindowSpec, got {type(window).__name__}")
        applied = super().over(window)
        if self._lookback <= 0:
            return applied
        # 1-based row_number: prefix length == lookback → indices 1..lookback are NULL.
        # No isnan/is_null on the value path — mid-series NaN is never rewritten.
        return when(row_number().over(window) > self._lookback, applied)


def _window(
    name: str,
    args: list[Column],
    *,
    lookback: int = 0,
    null_lookback: bool = False,
) -> Column:
    """Build the un-``OVER``ed TA window-function :class:`Column` for ``name`` from ``args``.

    When ``null_lookback`` is true, wrap so ``.over(w)`` converts the first ``lookback`` rows
    from kernel NaN to SQL NULL (polars_talib-shaped). Default is false — kernel NaN unchanged.
    """
    column = Column(_native.PyColumn.ta_window(name, [argument._inner for argument in args]))
    if null_lookback:
        return _NullLookbackColumn(column._inner, _nonneg(lookback))
    return column


# === r21 T4: ta-etl ===


def over_columns(window: WindowSpec, columns: dict[str, Column]) -> dict[str, Column]:
    """Apply one shared :class:`~repark.window.WindowSpec` to many un-``OVER``ed window columns.

    Returns a new ``dict`` ready for :meth:`repark.dataframe.DataFrame.withColumns`. Batching
    same-spec TA (and other window) expressions into a single ``withColumns`` is the supported
    fused plan shape — DataFusion emits one ``WindowAggExec`` for the group. Prefer this over
    chaining ``N`` :meth:`~repark.dataframe.DataFrame.withColumn` calls (N stacked window
    operators)::

        from repark.spark import Window, ta

        wd = Window.orderBy("ts")
        df.withColumns(
            ta.over_columns(
                wd,
                {
                    "ema21": ta.ema("close", timeperiod=21),
                    "rsi14": ta.rsi("close", timeperiod=14),
                    "sma10": ta.sma("close", timeperiod=10),
                },
            )
        )

    Each value must be an un-windowed :class:`~repark.column.Column` (as returned by
    ``ta.*`` / ``F.row_number()`` / …). Pass the bare indicator; this helper attaches
    ``window``. Calling ``.over`` twice (already-windowed values) fails at the native binder.

    Raises:
        PySparkTypeError: if ``window`` is not a ``WindowSpec``, ``columns`` is not a ``dict``,
            a key is not a non-empty ``str``, or a value is not a :class:`Column`.
    """
    from repark.spark.window import WindowSpec

    if not isinstance(window, WindowSpec):
        raise PySparkTypeError(
            f"over_columns window must be a WindowSpec, got {type(window).__name__}"
        )
    if not isinstance(columns, dict):
        raise PySparkTypeError(
            f"over_columns columns must be a dict[str, Column], got {type(columns).__name__}"
        )
    result: dict[str, Column] = {}
    for name, column in columns.items():
        if not isinstance(name, str):
            raise PySparkTypeError(
                f"over_columns keys must be str column names, got {type(name).__name__}"
            )
        if name.strip() == "":
            raise PySparkTypeError("over_columns keys must be non-empty column names")
        if not isinstance(column, Column):
            raise PySparkTypeError(
                f"over_columns values must be Column (un-OVER'ed ta.* / window fn), "
                f"got {type(column).__name__} for {name!r}"
            )
        result[name] = column.over(window)
    return result


# ---- overlap studies ----------------------------------------------------------------------------


def sma(real: Column | str, timeperiod: int = 30, *, null_lookback: bool = False) -> Column:
    """Simple moving average (TA-Lib ``SMA``). Complete with ``.over(window)``."""
    return _window(
        "ta_sma",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def ema(real: Column | str, timeperiod: int = 30, *, null_lookback: bool = False) -> Column:
    """Exponential moving average (TA-Lib ``EMA``). Complete with ``.over(window)``."""
    return _window(
        "ta_ema",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def wma(real: Column | str, timeperiod: int = 30, *, null_lookback: bool = False) -> Column:
    """Weighted moving average (TA-Lib ``WMA``). Complete with ``.over(window)``."""
    return _window(
        "ta_wma",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def dema(real: Column | str, timeperiod: int = 30, *, null_lookback: bool = False) -> Column:
    """Double exponential moving average (TA-Lib ``DEMA``). Complete with ``.over(window)``."""
    return _window(
        "ta_dema",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(2 * (timeperiod - 1)),
        null_lookback=null_lookback,
    )


def tema(real: Column | str, timeperiod: int = 30, *, null_lookback: bool = False) -> Column:
    """Triple exponential moving average (TA-Lib ``TEMA``). Complete with ``.over(window)``."""
    return _window(
        "ta_tema",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(3 * (timeperiod - 1)),
        null_lookback=null_lookback,
    )


def trima(real: Column | str, timeperiod: int = 30, *, null_lookback: bool = False) -> Column:
    """Triangular moving average (TA-Lib ``TRIMA``). Complete with ``.over(window)``."""
    return _window(
        "ta_trima",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def kama(real: Column | str, timeperiod: int = 30, *, null_lookback: bool = False) -> Column:
    """Kaufman adaptive moving average (TA-Lib ``KAMA``). Complete with ``.over(window)``."""
    return _window(
        "ta_kama",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def t3(
    real: Column | str, timeperiod: int = 5, vfactor: float = 0.7, *, null_lookback: bool = False
) -> Column:
    """Tillson T3 moving average (TA-Lib ``T3``). ``vfactor`` is the volume factor (default 0.7).

    Complete with ``.over(window)``.
    """
    return _window(
        "ta_t3",
        [_series(real), lit(timeperiod), lit(float(vfactor))],
        lookback=_nonneg(6 * (timeperiod - 1)),
        null_lookback=null_lookback,
    )


def midpoint(real: Column | str, timeperiod: int = 14, *, null_lookback: bool = False) -> Column:
    """MidPoint over period, ``(highest + lowest) / 2`` (TA-Lib ``MIDPOINT``). Use ``.over``."""
    return _window(
        "ta_midpoint",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def midprice(
    high: Column | str, low: Column | str, timeperiod: int = 14, *, null_lookback: bool = False
) -> Column:
    """Midpoint price over period (TA-Lib ``MIDPRICE``, two-series H/L). Use ``.over(window)``."""
    return _window(
        "ta_midprice",
        [_series(high), _series(low), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def bbands_upper(
    real: Column | str,
    timeperiod: int = 5,
    nbdevup: float = 2.0,
    nbdevdn: float = 2.0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Bollinger upper band (TA-Lib ``BBANDS`` upper output). Complete with ``.over(window)``."""
    return _window(
        "ta_bbands_upper",
        [_series(real), lit(timeperiod), lit(float(nbdevup)), lit(float(nbdevdn))],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def bbands_middle(
    real: Column | str,
    timeperiod: int = 5,
    nbdevup: float = 2.0,
    nbdevdn: float = 2.0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Bollinger middle band = SMA (TA-Lib ``BBANDS`` middle). Complete with ``.over(window)``."""
    return _window(
        "ta_bbands_middle",
        [_series(real), lit(timeperiod), lit(float(nbdevup)), lit(float(nbdevdn))],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def bbands_lower(
    real: Column | str,
    timeperiod: int = 5,
    nbdevup: float = 2.0,
    nbdevdn: float = 2.0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Bollinger lower band (TA-Lib ``BBANDS`` lower output). Complete with ``.over(window)``."""
    return _window(
        "ta_bbands_lower",
        [_series(real), lit(timeperiod), lit(float(nbdevup)), lit(float(nbdevdn))],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def mama(
    real: Column | str,
    fastlimit: float = 0.5,
    slowlimit: float = 0.05,
    *,
    null_lookback: bool = False,
) -> Column:
    """MESA adaptive moving average — the MAMA output (TA-Lib ``MAMA`` mama).

    The companion :func:`fama` gives the FAMA output. ``fastlimit`` / ``slowlimit`` ∈ ``[0.01,
    0.99]`` (defaults 0.5 / 0.05). Complete with ``.over(window)``.
    """
    return _window(
        "ta_mama",
        [_series(real), lit(float(fastlimit)), lit(float(slowlimit))],
        lookback=32,
        null_lookback=null_lookback,
    )


def fama(
    real: Column | str,
    fastlimit: float = 0.5,
    slowlimit: float = 0.05,
    *,
    null_lookback: bool = False,
) -> Column:
    """Following adaptive moving average — MAMA's FAMA output (TA-Lib ``MAMA`` fama).

    Split companion of :func:`mama`. Complete with ``.over(window)``.
    """
    return _window(
        "ta_fama",
        [_series(real), lit(float(fastlimit)), lit(float(slowlimit))],
        lookback=32,
        null_lookback=null_lookback,
    )


def sar(
    high: Column | str,
    low: Column | str,
    acceleration: float = 0.02,
    maximum: float = 0.2,
    *,
    null_lookback: bool = False,
) -> Column:
    """Parabolic SAR (TA-Lib ``SAR``, two-series H/L). Complete with ``.over(window)``."""
    return _window(
        "ta_sar",
        [_series(high), _series(low), lit(float(acceleration)), lit(float(maximum))],
        lookback=1,
        null_lookback=null_lookback,
    )


def sarext(
    high: Column | str,
    low: Column | str,
    startvalue: float = 0.0,
    offsetonreverse: float = 0.0,
    accelerationinitlong: float = 0.02,
    accelerationlong: float = 0.02,
    accelerationmaxlong: float = 0.2,
    accelerationinitshort: float = 0.02,
    accelerationshort: float = 0.02,
    accelerationmaxshort: float = 0.2,
    *,
    null_lookback: bool = False,
) -> Column:
    """Parabolic SAR extended (TA-Lib ``SAREXT``, two-series H/L). The short-side output is
    **negative**, so a sign flip marks a reversal. ``startvalue`` = 0 auto, > 0 forces long, < 0
    forces short at ``|startvalue|``. Complete with ``.over(window)``.
    """
    return _window(
        "ta_sarext",
        [
            _series(high),
            _series(low),
            lit(float(startvalue)),
            lit(float(offsetonreverse)),
            lit(float(accelerationinitlong)),
            lit(float(accelerationlong)),
            lit(float(accelerationmaxlong)),
            lit(float(accelerationinitshort)),
            lit(float(accelerationshort)),
            lit(float(accelerationmaxshort)),
        ],
        lookback=1,
        null_lookback=null_lookback,
    )


def mavp(
    real: Column | str,
    periods: Column | str,
    minperiod: int = 2,
    maxperiod: int = 30,
    matype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Moving average with variable period (TA-Lib ``MAVP``).

    ``periods`` is a **second input series** — the per-row period, truncated to an integer and
    clamped to ``[minperiod, maxperiod]``. ``matype`` is a TA-Lib MA-type code (see :func:`apo`;
    7 = MAMA, which ignores the period). Complete with ``.over(window)``.
    """
    return _window(
        "ta_mavp",
        [_series(real), _series(periods), lit(minperiod), lit(maxperiod), lit(matype)],
        lookback=_ma_lookback(maxperiod, matype),
        null_lookback=null_lookback,
    )


# ---- momentum indicators ------------------------------------------------------------------------


def rsi(real: Column | str, timeperiod: int = 14, *, null_lookback: bool = False) -> Column:
    """Relative strength index (TA-Lib ``RSI``). Complete with ``.over(window)``."""
    return _window(
        "ta_rsi",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def adx(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    timeperiod: int = 14,
    *,
    null_lookback: bool = False,
) -> Column:
    """Average directional movement index (TA-Lib ``ADX``). Complete with ``.over(window)``."""
    return _window(
        "ta_adx",
        [_series(high), _series(low), _series(close), lit(timeperiod)],
        lookback=_nonneg(2 * timeperiod - 1),
        null_lookback=null_lookback,
    )


def mom(real: Column | str, timeperiod: int = 10, *, null_lookback: bool = False) -> Column:
    """Momentum, ``price - prevPrice`` (TA-Lib ``MOM``). Complete with ``.over(window)``."""
    return _window(
        "ta_mom",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def roc(real: Column | str, timeperiod: int = 10, *, null_lookback: bool = False) -> Column:
    """Rate of change, ``((price/prevPrice) - 1)·100`` (TA-Lib ``ROC``). Use ``.over(window)``."""
    return _window(
        "ta_roc",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def rocp(real: Column | str, timeperiod: int = 10, *, null_lookback: bool = False) -> Column:
    """Rate of change percentage, ``(price - prevPrice)/prevPrice`` (TA-Lib ``ROCP``)."""
    return _window(
        "ta_rocp",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def rocr(real: Column | str, timeperiod: int = 10, *, null_lookback: bool = False) -> Column:
    """Rate of change ratio, ``price/prevPrice`` (TA-Lib ``ROCR``). Complete with ``.over``."""
    return _window(
        "ta_rocr",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def rocr100(real: Column | str, timeperiod: int = 10, *, null_lookback: bool = False) -> Column:
    """Rate of change ratio x100, ``(price/prevPrice)·100`` (TA-Lib ``ROCR100``). Use ``.over``."""
    return _window(
        "ta_rocr100",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def willr(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    timeperiod: int = 14,
    *,
    null_lookback: bool = False,
) -> Column:
    """Williams %R, in ``[-100, 0]`` (TA-Lib ``WILLR``). Complete with ``.over(window)``."""
    return _window(
        "ta_willr",
        [_series(high), _series(low), _series(close), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def cci(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    timeperiod: int = 14,
    *,
    null_lookback: bool = False,
) -> Column:
    """Commodity channel index (TA-Lib ``CCI``). Complete with ``.over(window)``."""
    return _window(
        "ta_cci",
        [_series(high), _series(low), _series(close), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def cmo(real: Column | str, timeperiod: int = 14, *, null_lookback: bool = False) -> Column:
    """Chande momentum oscillator (TA-Lib ``CMO``). Complete with ``.over(window)``."""
    return _window(
        "ta_cmo",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def bop(
    open: Column | str,
    high: Column | str,
    low: Column | str,
    close: Column | str,
    *,
    null_lookback: bool = False,
) -> Column:
    """Balance of power, ``(close - open)/(high - low)`` (TA-Lib ``BOP``, four-series O/H/L/C).

    Complete with ``.over(window)``.
    """
    return _window(
        "ta_bop",
        [_series(open), _series(high), _series(low), _series(close)],
        lookback=0,
        null_lookback=null_lookback,
    )


def apo(
    real: Column | str,
    fastperiod: int = 12,
    slowperiod: int = 26,
    matype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Absolute price oscillator (TA-Lib ``APO``). ``matype`` is TA-Lib's MA-type code
    (0 SMA / 1 EMA / 2 WMA / 3 DEMA / 4 TEMA / 5 TRIMA / 6 KAMA / 7 MAMA / 8 T3). Matype 7 uses
    ``MAMA(0.5, 0.05)`` on both legs (period ignored; APO is identically zero after lookback 32).

    Complete with ``.over(window)``.
    """
    return _window(
        "ta_apo",
        [_series(real), lit(fastperiod), lit(slowperiod), lit(matype)],
        lookback=_ma_lookback(_max2(fastperiod, slowperiod), matype),
        null_lookback=null_lookback,
    )


def ppo(
    real: Column | str,
    fastperiod: int = 12,
    slowperiod: int = 26,
    matype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Percentage price oscillator (TA-Lib ``PPO``). ``matype`` as in :func:`apo`.

    Complete with ``.over(window)``.
    """
    return _window(
        "ta_ppo",
        [_series(real), lit(fastperiod), lit(slowperiod), lit(matype)],
        lookback=_ma_lookback(_max2(fastperiod, slowperiod), matype),
        null_lookback=null_lookback,
    )


def aroon_down(
    high: Column | str, low: Column | str, timeperiod: int = 14, *, null_lookback: bool = False
) -> Column:
    """Aroon Down (TA-Lib ``AROON`` down output). Complete with ``.over(window)``."""
    return _window(
        "ta_aroon_down",
        [_series(high), _series(low), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def aroon_up(
    high: Column | str, low: Column | str, timeperiod: int = 14, *, null_lookback: bool = False
) -> Column:
    """Aroon Up (TA-Lib ``AROON`` up output). Complete with ``.over(window)``."""
    return _window(
        "ta_aroon_up",
        [_series(high), _series(low), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def aroonosc(
    high: Column | str, low: Column | str, timeperiod: int = 14, *, null_lookback: bool = False
) -> Column:
    """Aroon oscillator, ``AroonUp - AroonDown`` (TA-Lib ``AROONOSC``). Use ``.over(window)``."""
    return _window(
        "ta_aroonosc",
        [_series(high), _series(low), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def trix(real: Column | str, timeperiod: int = 30, *, null_lookback: bool = False) -> Column:
    """1-day ROC of a triple-smoothed EMA (TA-Lib ``TRIX``). Complete with ``.over(window)``."""
    return _window(
        "ta_trix",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(3 * (timeperiod - 1) + 1),
        null_lookback=null_lookback,
    )


def ultosc(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    timeperiod1: int = 7,
    timeperiod2: int = 14,
    timeperiod3: int = 28,
    *,
    null_lookback: bool = False,
) -> Column:
    """Ultimate oscillator over three periods (TA-Lib ``ULTOSC``). Complete with ``.over``."""
    return _window(
        "ta_ultosc",
        [
            _series(high),
            _series(low),
            _series(close),
            lit(timeperiod1),
            lit(timeperiod2),
            lit(timeperiod3),
        ],
        # Kernel sorts periods; lookback is the longest (SMA_Lookback(max) + 1 = max).
        lookback=_nonneg(_max2(_max2(timeperiod1, timeperiod2), timeperiod3)),
        null_lookback=null_lookback,
    )


# ---- directional movement -----------------------------------------------------------------------


def dx(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    timeperiod: int = 14,
    *,
    null_lookback: bool = False,
) -> Column:
    """Directional movement index (TA-Lib ``DX``). Complete with ``.over(window)``."""
    return _window(
        "ta_dx",
        [_series(high), _series(low), _series(close), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def adxr(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    timeperiod: int = 14,
    *,
    null_lookback: bool = False,
) -> Column:
    """Average directional movement rating (TA-Lib ``ADXR``). Complete with ``.over(window)``."""
    return _window(
        "ta_adxr",
        [_series(high), _series(low), _series(close), lit(timeperiod)],
        lookback=_nonneg(3 * timeperiod - 2),
        null_lookback=null_lookback,
    )


def plus_di(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    timeperiod: int = 14,
    *,
    null_lookback: bool = False,
) -> Column:
    """Plus directional indicator (TA-Lib ``PLUS_DI``). Complete with ``.over(window)``."""
    return _window(
        "ta_plus_di",
        [_series(high), _series(low), _series(close), lit(timeperiod)],
        lookback=_nonneg(timeperiod if timeperiod > 1 else 1),
        null_lookback=null_lookback,
    )


def minus_di(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    timeperiod: int = 14,
    *,
    null_lookback: bool = False,
) -> Column:
    """Minus directional indicator (TA-Lib ``MINUS_DI``). Complete with ``.over(window)``."""
    return _window(
        "ta_minus_di",
        [_series(high), _series(low), _series(close), lit(timeperiod)],
        lookback=_nonneg(timeperiod if timeperiod > 1 else 1),
        null_lookback=null_lookback,
    )


def plus_dm(
    high: Column | str, low: Column | str, timeperiod: int = 14, *, null_lookback: bool = False
) -> Column:
    """Plus directional movement (TA-Lib ``PLUS_DM``, two-series H/L). Use ``.over(window)``."""
    return _window(
        "ta_plus_dm",
        [_series(high), _series(low), lit(timeperiod)],
        lookback=_nonneg((timeperiod - 1) if timeperiod > 1 else 1),
        null_lookback=null_lookback,
    )


def minus_dm(
    high: Column | str, low: Column | str, timeperiod: int = 14, *, null_lookback: bool = False
) -> Column:
    """Minus directional movement (TA-Lib ``MINUS_DM``, two-series H/L). Use ``.over(window)``."""
    return _window(
        "ta_minus_dm",
        [_series(high), _series(low), lit(timeperiod)],
        lookback=_nonneg((timeperiod - 1) if timeperiod > 1 else 1),
        null_lookback=null_lookback,
    )


# ---- MACD family --------------------------------------------------------------------------------


def _macd_lookback(fastperiod: int, slowperiod: int, signalperiod: int) -> int:
    """MACD lookback after kernel period-swap: ``(max(fast, slow) - 1) + (signal - 1)``.

    Matches ``int_macd`` in ``repark-ta`` (C TA-Lib swaps so the larger period is slow).
    """
    return _nonneg((_max2(fastperiod, slowperiod) - 1) + (signalperiod - 1))


def macd(
    real: Column | str,
    fastperiod: int = 12,
    slowperiod: int = 26,
    signalperiod: int = 9,
    *,
    null_lookback: bool = False,
) -> Column:
    """MACD line, ``EMA(fast) - EMA(slow)`` (TA-Lib ``MACD`` macd output). Use ``.over(window)``."""
    return _window(
        "ta_macd",
        [_series(real), lit(fastperiod), lit(slowperiod), lit(signalperiod)],
        lookback=_macd_lookback(fastperiod, slowperiod, signalperiod),
        null_lookback=null_lookback,
    )


def macd_signal(
    real: Column | str,
    fastperiod: int = 12,
    slowperiod: int = 26,
    signalperiod: int = 9,
    *,
    null_lookback: bool = False,
) -> Column:
    """MACD signal line, ``EMA(macd)`` (TA-Lib ``MACD`` signal output). Complete with ``.over``."""
    return _window(
        "ta_macd_signal",
        [_series(real), lit(fastperiod), lit(slowperiod), lit(signalperiod)],
        lookback=_macd_lookback(fastperiod, slowperiod, signalperiod),
        null_lookback=null_lookback,
    )


def macd_hist(
    real: Column | str,
    fastperiod: int = 12,
    slowperiod: int = 26,
    signalperiod: int = 9,
    *,
    null_lookback: bool = False,
) -> Column:
    """MACD histogram, ``macd - signal`` (TA-Lib ``MACD`` hist output). Complete with ``.over``."""
    return _window(
        "ta_macd_hist",
        [_series(real), lit(fastperiod), lit(slowperiod), lit(signalperiod)],
        lookback=_macd_lookback(fastperiod, slowperiod, signalperiod),
        null_lookback=null_lookback,
    )


def macdfix(real: Column | str, signalperiod: int = 9, *, null_lookback: bool = False) -> Column:
    """MACD-fix line, 12/26 pinned with fixed constants (TA-Lib ``MACDFIX`` macd). Use ``.over``."""
    return _window(
        "ta_macdfix",
        [_series(real), lit(signalperiod)],
        lookback=_nonneg(25 + (signalperiod - 1)),
        null_lookback=null_lookback,
    )


def macdfix_signal(
    real: Column | str, signalperiod: int = 9, *, null_lookback: bool = False
) -> Column:
    """MACD-fix signal line (TA-Lib ``MACDFIX`` signal output). Complete with ``.over(window)``."""
    return _window(
        "ta_macdfix_signal",
        [_series(real), lit(signalperiod)],
        lookback=_nonneg(25 + (signalperiod - 1)),
        null_lookback=null_lookback,
    )


def macdfix_hist(
    real: Column | str, signalperiod: int = 9, *, null_lookback: bool = False
) -> Column:
    """MACD-fix histogram (TA-Lib ``MACDFIX`` hist output). Complete with ``.over(window)``."""
    return _window(
        "ta_macdfix_hist",
        [_series(real), lit(signalperiod)],
        lookback=_nonneg(25 + (signalperiod - 1)),
        null_lookback=null_lookback,
    )


def macdext(
    real: Column | str,
    fastperiod: int = 12,
    fastmatype: int = 0,
    slowperiod: int = 26,
    slowmatype: int = 0,
    signalperiod: int = 9,
    signalmatype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """MACD line with configurable MA types (TA-Lib ``MACDEXT`` macd output). ``*matype`` are
    TA-Lib MA-type codes 0..=8 including MAMA (7) = ``MAMA(0.5, 0.05)`` (see :func:`apo`).
    Complete with ``.over``.
    """
    return _window(
        "ta_macdext",
        [
            _series(real),
            lit(fastperiod),
            lit(fastmatype),
            lit(slowperiod),
            lit(slowmatype),
            lit(signalperiod),
            lit(signalmatype),
        ],
        lookback=_nonneg(
            _max2(_ma_lookback(fastperiod, fastmatype), _ma_lookback(slowperiod, slowmatype))
            + _ma_lookback(signalperiod, signalmatype)
        ),
        null_lookback=null_lookback,
    )


def macdext_signal(
    real: Column | str,
    fastperiod: int = 12,
    fastmatype: int = 0,
    slowperiod: int = 26,
    slowmatype: int = 0,
    signalperiod: int = 9,
    signalmatype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """MACDEXT signal line (TA-Lib ``MACDEXT`` signal output). Complete with ``.over(window)``."""
    return _window(
        "ta_macdext_signal",
        [
            _series(real),
            lit(fastperiod),
            lit(fastmatype),
            lit(slowperiod),
            lit(slowmatype),
            lit(signalperiod),
            lit(signalmatype),
        ],
        lookback=_nonneg(
            _max2(_ma_lookback(fastperiod, fastmatype), _ma_lookback(slowperiod, slowmatype))
            + _ma_lookback(signalperiod, signalmatype)
        ),
        null_lookback=null_lookback,
    )


def macdext_hist(
    real: Column | str,
    fastperiod: int = 12,
    fastmatype: int = 0,
    slowperiod: int = 26,
    slowmatype: int = 0,
    signalperiod: int = 9,
    signalmatype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """MACDEXT histogram (TA-Lib ``MACDEXT`` hist output). Complete with ``.over(window)``."""
    return _window(
        "ta_macdext_hist",
        [
            _series(real),
            lit(fastperiod),
            lit(fastmatype),
            lit(slowperiod),
            lit(slowmatype),
            lit(signalperiod),
            lit(signalmatype),
        ],
        lookback=_nonneg(
            _max2(_ma_lookback(fastperiod, fastmatype), _ma_lookback(slowperiod, slowmatype))
            + _ma_lookback(signalperiod, signalmatype)
        ),
        null_lookback=null_lookback,
    )


def ma(
    real: Column | str, timeperiod: int = 30, matype: int = 0, *, null_lookback: bool = False
) -> Column:
    """Moving-average selector (TA-Lib ``MA``). ``matype`` picks the MA (see :func:`apo`;
    ``timeperiod == 1`` is the identity for any in-range ``matype``). Complete with ``.over``.
    """
    return _window(
        "ta_ma",
        [_series(real), lit(timeperiod), lit(matype)],
        lookback=_ma_lookback(timeperiod, matype),
        null_lookback=null_lookback,
    )


# ---- stochastics --------------------------------------------------------------------------------


def stoch_slowk(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    fastk_period: int = 5,
    slowk_period: int = 3,
    slowk_matype: int = 0,
    slowd_period: int = 3,
    slowd_matype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Slow stochastic %K (TA-Lib ``STOCH`` slowk output). ``*matype`` are TA-Lib MA-type codes
    0..=8 including MAMA (7) = ``MAMA(0.5, 0.05)`` on the smoothing legs. Complete with
    ``.over(window)``.
    """
    return _window(
        "ta_stoch_slowk",
        [
            _series(high),
            _series(low),
            _series(close),
            lit(fastk_period),
            lit(slowk_period),
            lit(slowk_matype),
            lit(slowd_period),
            lit(slowd_matype),
        ],
        lookback=_nonneg(
            (fastk_period - 1)
            + _ma_lookback(slowk_period, slowk_matype)
            + _ma_lookback(slowd_period, slowd_matype)
        ),
        null_lookback=null_lookback,
    )


def stoch_slowd(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    fastk_period: int = 5,
    slowk_period: int = 3,
    slowk_matype: int = 0,
    slowd_period: int = 3,
    slowd_matype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Slow stochastic %D signal line (TA-Lib ``STOCH`` slowd output). ``*matype`` are TA-Lib
    MA-type codes 0..=8 including MAMA (7). Complete with ``.over``.
    """
    return _window(
        "ta_stoch_slowd",
        [
            _series(high),
            _series(low),
            _series(close),
            lit(fastk_period),
            lit(slowk_period),
            lit(slowk_matype),
            lit(slowd_period),
            lit(slowd_matype),
        ],
        lookback=_nonneg(
            (fastk_period - 1)
            + _ma_lookback(slowk_period, slowk_matype)
            + _ma_lookback(slowd_period, slowd_matype)
        ),
        null_lookback=null_lookback,
    )


def stochf_fastk(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    fastk_period: int = 5,
    fastd_period: int = 3,
    fastd_matype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Fast stochastic %K (TA-Lib ``STOCHF`` fastk output). ``fastd_matype`` is a TA-Lib MA-type
    code 0..=8 including MAMA (7). Complete with ``.over(window)``.
    """
    return _window(
        "ta_stochf_fastk",
        [
            _series(high),
            _series(low),
            _series(close),
            lit(fastk_period),
            lit(fastd_period),
            lit(fastd_matype),
        ],
        lookback=_nonneg((fastk_period - 1) + _ma_lookback(fastd_period, fastd_matype)),
        null_lookback=null_lookback,
    )


def stochf_fastd(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    fastk_period: int = 5,
    fastd_period: int = 3,
    fastd_matype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Fast stochastic %D signal line (TA-Lib ``STOCHF`` fastd output). ``fastd_matype`` is a
    TA-Lib MA-type code 0..=8 including MAMA (7). Complete with ``.over``.
    """
    return _window(
        "ta_stochf_fastd",
        [
            _series(high),
            _series(low),
            _series(close),
            lit(fastk_period),
            lit(fastd_period),
            lit(fastd_matype),
        ],
        lookback=_nonneg((fastk_period - 1) + _ma_lookback(fastd_period, fastd_matype)),
        null_lookback=null_lookback,
    )


def stochrsi_fastk(
    real: Column | str,
    timeperiod: int = 14,
    fastk_period: int = 5,
    fastd_period: int = 3,
    fastd_matype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Stochastic-RSI %K (TA-Lib ``STOCHRSI`` fastk output) — STOCHF over RSI.
    ``fastd_matype`` is a TA-Lib MA-type code 0..=8 including MAMA (7). Use ``.over``.
    """
    return _window(
        "ta_stochrsi_fastk",
        [
            _series(real),
            lit(timeperiod),
            lit(fastk_period),
            lit(fastd_period),
            lit(fastd_matype),
        ],
        lookback=_nonneg(
            timeperiod + (fastk_period - 1) + _ma_lookback(fastd_period, fastd_matype)
        ),
        null_lookback=null_lookback,
    )


def stochrsi_fastd(
    real: Column | str,
    timeperiod: int = 14,
    fastk_period: int = 5,
    fastd_period: int = 3,
    fastd_matype: int = 0,
    *,
    null_lookback: bool = False,
) -> Column:
    """Stochastic-RSI %D signal line (TA-Lib ``STOCHRSI`` fastd output). ``fastd_matype`` is a
    TA-Lib MA-type code 0..=8 including MAMA (7). Complete with ``.over``.
    """
    return _window(
        "ta_stochrsi_fastd",
        [
            _series(real),
            lit(timeperiod),
            lit(fastk_period),
            lit(fastd_period),
            lit(fastd_matype),
        ],
        lookback=_nonneg(
            timeperiod + (fastk_period - 1) + _ma_lookback(fastd_period, fastd_matype)
        ),
        null_lookback=null_lookback,
    )


# ---- volatility ---------------------------------------------------------------------------------


def trange(
    high: Column | str, low: Column | str, close: Column | str, *, null_lookback: bool = False
) -> Column:
    """True range (TA-Lib ``TRANGE``). Complete with ``.over(window)``."""
    return _window(
        "ta_trange",
        [_series(high), _series(low), _series(close)],
        lookback=1,
        null_lookback=null_lookback,
    )


def atr(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    timeperiod: int = 14,
    *,
    null_lookback: bool = False,
) -> Column:
    """Average true range (TA-Lib ``ATR``). Complete with ``.over(window)``."""
    return _window(
        "ta_atr",
        [_series(high), _series(low), _series(close), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


def natr(
    high: Column | str,
    low: Column | str,
    close: Column | str,
    timeperiod: int = 14,
    *,
    null_lookback: bool = False,
) -> Column:
    """Normalized average true range, ``(ATR / close)·100`` (TA-Lib ``NATR``). Use ``.over``."""
    return _window(
        "ta_natr",
        [_series(high), _series(low), _series(close), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


# ---- statistic functions ------------------------------------------------------------------------


def var(
    real: Column | str, timeperiod: int = 5, nbdev: float = 1.0, *, null_lookback: bool = False
) -> Column:
    """Rolling variance (TA-Lib ``VAR``; ``nbdev`` accepted for parity, ignored as in C).

    Complete with ``.over(window)``.
    """
    return _window(
        "ta_var",
        [_series(real), lit(timeperiod), lit(float(nbdev))],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def stddev(
    real: Column | str, timeperiod: int = 5, nbdev: float = 1.0, *, null_lookback: bool = False
) -> Column:
    """Rolling standard deviation (TA-Lib ``STDDEV``). Complete with ``.over(window)``."""
    return _window(
        "ta_stddev",
        [_series(real), lit(timeperiod), lit(float(nbdev))],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def linearreg(real: Column | str, timeperiod: int = 14, *, null_lookback: bool = False) -> Column:
    """Linear-regression value at the last bar (TA-Lib ``LINEARREG``). Use ``.over(window)``."""
    return _window(
        "ta_linearreg",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def linearreg_slope(
    real: Column | str, timeperiod: int = 14, *, null_lookback: bool = False
) -> Column:
    """Linear-regression slope (TA-Lib ``LINEARREG_SLOPE``). Complete with ``.over(window)``."""
    return _window(
        "ta_linearreg_slope",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def linearreg_intercept(
    real: Column | str, timeperiod: int = 14, *, null_lookback: bool = False
) -> Column:
    """Linear-regression intercept (TA-Lib ``LINEARREG_INTERCEPT``). Use ``.over(window)``."""
    return _window(
        "ta_linearreg_intercept",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def linearreg_angle(
    real: Column | str, timeperiod: int = 14, *, null_lookback: bool = False
) -> Column:
    """Regression slope in degrees (TA-Lib ``LINEARREG_ANGLE``). Use ``.over(window)``."""
    return _window(
        "ta_linearreg_angle",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def tsf(real: Column | str, timeperiod: int = 14, *, null_lookback: bool = False) -> Column:
    """Time-series forecast, one bar ahead (TA-Lib ``TSF``). Complete with ``.over(window)``."""
    return _window(
        "ta_tsf",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def correl(
    real0: Column | str, real1: Column | str, timeperiod: int = 30, *, null_lookback: bool = False
) -> Column:
    """Rolling Pearson correlation (TA-Lib ``CORREL``). Complete with ``.over(window)``."""
    return _window(
        "ta_correl",
        [_series(real0), _series(real1), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def beta(
    price0: Column | str, price1: Column | str, timeperiod: int = 5, *, null_lookback: bool = False
) -> Column:
    """Rolling beta of ``price0`` vs ``price1`` (TA-Lib ``BETA``, two-series). Use ``.over``."""
    return _window(
        "ta_beta",
        [_series(price0), _series(price1), lit(timeperiod)],
        lookback=_nonneg(timeperiod),
        null_lookback=null_lookback,
    )


# ---- price transforms ---------------------------------------------------------------------------


def avgprice(
    open: Column | str,
    high: Column | str,
    low: Column | str,
    close: Column | str,
    *,
    null_lookback: bool = False,
) -> Column:
    """Average price, ``(open + high + low + close) / 4`` (TA-Lib ``AVGPRICE``). Use ``.over``."""
    return _window(
        "ta_avgprice",
        [_series(open), _series(high), _series(low), _series(close)],
        lookback=0,
        null_lookback=null_lookback,
    )


def medprice(high: Column | str, low: Column | str, *, null_lookback: bool = False) -> Column:
    """Median price, ``(high + low) / 2`` (TA-Lib ``MEDPRICE``). Complete with ``.over(window)``."""
    return _window(
        "ta_medprice",
        [_series(high), _series(low)],
        lookback=0,
        null_lookback=null_lookback,
    )


def typprice(
    high: Column | str, low: Column | str, close: Column | str, *, null_lookback: bool = False
) -> Column:
    """Typical price, ``(high + low + close) / 3`` (TA-Lib ``TYPPRICE``). Use ``.over(window)``."""
    return _window(
        "ta_typprice",
        [_series(high), _series(low), _series(close)],
        lookback=0,
        null_lookback=null_lookback,
    )


def wclprice(
    high: Column | str, low: Column | str, close: Column | str, *, null_lookback: bool = False
) -> Column:
    """Weighted close price, ``(high + low + close·2) / 4`` (TA-Lib ``WCLPRICE``). Use ``.over``."""
    return _window(
        "ta_wclprice",
        [_series(high), _series(low), _series(close)],
        lookback=0,
        null_lookback=null_lookback,
    )


# ---- math operators -----------------------------------------------------------------------------


def min(real: Column | str, timeperiod: int = 30, *, null_lookback: bool = False) -> Column:
    """Rolling minimum over ``timeperiod`` bars (TA-Lib ``MIN``). Complete with ``.over(window)``.

    Named ``min`` to mirror TA-Lib / ``polars_talib`` (PySpark itself exposes ``F.min``); the
    uppercase ``MIN`` alias matches the TA-Lib function name for import-swap fidelity.
    """
    return _window(
        "ta_min",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def max(real: Column | str, timeperiod: int = 30, *, null_lookback: bool = False) -> Column:
    """Rolling maximum over ``timeperiod`` bars (TA-Lib ``MAX``). Complete with ``.over(window)``.

    Named ``max`` to mirror TA-Lib / ``polars_talib``; the uppercase ``MAX`` alias matches the
    TA-Lib function name.
    """
    return _window(
        "ta_max",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


def sum(real: Column | str, timeperiod: int = 30, *, null_lookback: bool = False) -> Column:
    """Rolling sum over ``timeperiod`` bars (TA-Lib ``SUM``). Complete with ``.over(window)``.

    Named ``sum`` to mirror TA-Lib / ``polars_talib``; the uppercase ``SUM`` alias matches the
    TA-Lib function name.
    """
    return _window(
        "ta_sum",
        [_series(real), lit(timeperiod)],
        lookback=_nonneg(timeperiod - 1),
        null_lookback=null_lookback,
    )


# Uppercase TA-Lib-name aliases (``ta.MIN`` / ``ta.MAX`` / ``ta.SUM``) so notebook code that uses
# the C function names ports by import swap.
MIN = min
MAX = max
SUM = sum


__all__ = [
    "MAX",
    "MIN",
    "SUM",
    "adx",
    "adxr",
    "apo",
    "aroon_down",
    "aroon_up",
    "aroonosc",
    "atr",
    "avgprice",
    "bbands_lower",
    "bbands_middle",
    "bbands_upper",
    "beta",
    "bop",
    "cci",
    "cmo",
    "correl",
    "dema",
    "dx",
    "ema",
    "fama",
    "kama",
    "linearreg",
    "linearreg_angle",
    "linearreg_intercept",
    "linearreg_slope",
    "ma",
    "macd",
    "macd_hist",
    "macd_signal",
    "macdext",
    "macdext_hist",
    "macdext_signal",
    "macdfix",
    "macdfix_hist",
    "macdfix_signal",
    "mama",
    "mavp",
    "max",
    "medprice",
    "midpoint",
    "midprice",
    "min",
    "minus_di",
    "minus_dm",
    "mom",
    "natr",
    "over_columns",
    "plus_di",
    "plus_dm",
    "ppo",
    "roc",
    "rocp",
    "rocr",
    "rocr100",
    "rsi",
    "sar",
    "sarext",
    "sma",
    "stddev",
    "stoch_slowd",
    "stoch_slowk",
    "stochf_fastd",
    "stochf_fastk",
    "stochrsi_fastd",
    "stochrsi_fastk",
    "sum",
    "t3",
    "tema",
    "trange",
    "trima",
    "trix",
    "tsf",
    "typprice",
    "ultosc",
    "var",
    "wclprice",
    "willr",
]
