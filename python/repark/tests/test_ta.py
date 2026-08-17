"""Facade tests for T1b: the ``repark.ta`` technical-analysis surface.

Every op runs against the real native engine (``maturin develop``). The DataFrame route
(``ta.ema(col("close"), timeperiod=21).over(Window.orderBy("ts"))``) must produce output that is
``f64::to_bits``-**identical** to the ``repark-ta`` kernel on the same ordered column. The kernels
are themselves gated bit-exact against C TA-Lib 0.4.0 by the crate's golden suite, so the recorded
golden ``.bin`` files (``crates/repark-ta/tests/goldens``) *are* the kernel outputs — comparing the
engine to a golden proves engine == kernel == C TA-Lib without re-recording anything. C TA-Lib is
the parity oracle here (Spark has no native TA-Lib), which makes this the differential parity case.

Input flows in by writing the fixture OHLC series to a Parquet file and ``spark.read_parquet``-ing
it (the facade has no ``createDataFrame`` from arrays yet).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import Column, ReparkSession, Window, ta

_GOLDENS = Path(__file__).resolve().parents[3] / "crates" / "repark-ta" / "tests" / "goldens"


def _golden(name: str) -> np.ndarray:
    """A recorded golden / fixture ``.bin`` (little-endian ``f64``) as a NumPy array."""
    return np.frombuffer((_GOLDENS / f"{name}.bin").read_bytes(), dtype="<f8")


@pytest.fixture
def spark() -> ReparkSession:
    """A default session (PySpark ``SparkSession.builder.getOrCreate()``)."""
    return ReparkSession.builder.appName("pytest-ta").getOrCreate()


@pytest.fixture
def bars(spark: ReparkSession, tmp_path: Path) -> object:
    """The 5000-row OHLC fixture as a DataFrame (``ts``/``open``/``high``/``low``/``close``).

    Written to Parquet then read back — the same fixture the crate's golden gate uses, so the
    goldens are the exact kernel outputs on this ``close`` / ``high`` / ``low``.
    """
    open_ = _golden("fixture_open")
    high = _golden("fixture_high")
    low = _golden("fixture_low")
    close = _golden("fixture_close")
    periods = _golden("fixture_periods")  # the MAVP per-row period series
    ts = np.arange(len(close), dtype=np.int64)
    table = pa.table(
        {"ts": ts, "open": open_, "high": high, "low": low, "close": close, "periods": periods}
    )
    path = tmp_path / "bars.parquet"
    pq.write_table(table, path)
    return spark.read_parquet(path)


def _engine_column(df: object, indicator: Column, ts_order: str = "ts") -> np.ndarray:
    """Run ``indicator.over(orderBy(ts))`` as a new column and return it as an ``f64`` array in
    ``ts`` order (aligned index-for-index with the goldens)."""
    windowed = indicator.over(Window.orderBy(ts_order))
    table = df.withColumn("out", windowed).to_arrow().sort_by(ts_order)  # type: ignore[attr-defined]
    return table.column("out").to_numpy(zero_copy_only=False)


def _assert_bit_exact(engine: np.ndarray, expected: np.ndarray) -> None:
    """Strict ``to_bits`` equality, ``NaN`` ↔ ``NaN`` allowed (any payload) — the crate's idiom."""
    engine = np.ascontiguousarray(engine, dtype=np.float64)
    expected = np.ascontiguousarray(expected, dtype=np.float64)
    assert engine.shape == expected.shape, f"length {engine.shape} vs {expected.shape}"
    both_nan = np.isnan(engine) & np.isnan(expected)
    mismatch = (engine.view(np.uint64) != expected.view(np.uint64)) & ~both_nan
    if mismatch.any():
        first = int(np.flatnonzero(mismatch)[0])
        raise AssertionError(
            f"bit mismatch at row {first}: engine {engine[first]!r} vs golden {expected[first]!r}"
        )


# ==================================================================================================
# Call-site surface (no engine needed)
# ==================================================================================================


def test_ta_functions_return_columns() -> None:
    assert isinstance(ta.ema("close", timeperiod=21), Column)
    assert isinstance(ta.adx("high", "low", "close", timeperiod=14), Column)
    assert isinstance(ta.bbands_upper("close", timeperiod=20), Column)
    assert isinstance(ta.min("close", timeperiod=21), Column)
    assert isinstance(ta.max("close", timeperiod=21), Column)
    assert isinstance(ta.sum("close", timeperiod=21), Column)
    assert isinstance(ta.wma("close", timeperiod=10), Column)
    assert isinstance(ta.dema("close", timeperiod=10), Column)
    assert isinstance(ta.tema("close", timeperiod=10), Column)
    assert isinstance(ta.trima("close", timeperiod=10), Column)
    assert isinstance(ta.kama("close", timeperiod=10), Column)
    assert isinstance(ta.t3("close", timeperiod=5, vfactor=0.7), Column)
    assert isinstance(ta.midpoint("close", timeperiod=10), Column)
    assert isinstance(ta.midprice("high", "low", timeperiod=10), Column)
    # WG2 simple-momentum surface.
    assert isinstance(ta.mom("close", timeperiod=10), Column)
    assert isinstance(ta.roc("close", timeperiod=10), Column)
    assert isinstance(ta.willr("high", "low", "close", timeperiod=14), Column)
    assert isinstance(ta.cci("high", "low", "close", timeperiod=14), Column)
    assert isinstance(ta.cmo("close", timeperiod=14), Column)
    assert isinstance(ta.bop("open", "high", "low", "close"), Column)
    assert isinstance(ta.apo("close", fastperiod=12, slowperiod=26, matype=0), Column)
    assert isinstance(ta.ppo("close", fastperiod=12, slowperiod=26, matype=0), Column)
    assert isinstance(ta.aroon_down("high", "low", timeperiod=14), Column)
    assert isinstance(ta.aroon_up("high", "low", timeperiod=14), Column)
    assert isinstance(ta.aroonosc("high", "low", timeperiod=14), Column)
    assert isinstance(ta.trix("close", timeperiod=30), Column)
    assert isinstance(ta.ultosc("high", "low", "close", timeperiod1=7), Column)
    # WG3 directional + MACD families.
    assert isinstance(ta.dx("high", "low", "close", timeperiod=14), Column)
    assert isinstance(ta.adxr("high", "low", "close", timeperiod=14), Column)
    assert isinstance(ta.plus_di("high", "low", "close", timeperiod=14), Column)
    assert isinstance(ta.minus_di("high", "low", "close", timeperiod=14), Column)
    assert isinstance(ta.plus_dm("high", "low", timeperiod=14), Column)
    assert isinstance(ta.minus_dm("high", "low", timeperiod=14), Column)
    assert isinstance(ta.macd("close", fastperiod=12, slowperiod=26, signalperiod=9), Column)
    assert isinstance(ta.macd_signal("close"), Column)
    assert isinstance(ta.macd_hist("close"), Column)
    assert isinstance(ta.macdfix("close", signalperiod=9), Column)
    assert isinstance(ta.macdfix_signal("close"), Column)
    assert isinstance(ta.macdfix_hist("close"), Column)
    assert isinstance(ta.macdext("close", fastmatype=0, slowmatype=0, signalmatype=0), Column)
    assert isinstance(ta.macdext_signal("close"), Column)
    assert isinstance(ta.macdext_hist("close"), Column)
    assert isinstance(ta.ma("close", timeperiod=30, matype=0), Column)
    # WG4 stochastics (each split into two outputs).
    assert isinstance(ta.stoch_slowk("high", "low", "close", fastk_period=5), Column)
    assert isinstance(ta.stoch_slowd("high", "low", "close"), Column)
    assert isinstance(ta.stochf_fastk("high", "low", "close", fastk_period=5), Column)
    assert isinstance(ta.stochf_fastd("high", "low", "close"), Column)
    assert isinstance(ta.stochrsi_fastk("close", timeperiod=14), Column)
    assert isinstance(ta.stochrsi_fastd("close"), Column)
    # WG5 sweep-up: NATR, BETA, and the four O/H/L/C price transforms (no period).
    assert isinstance(ta.natr("high", "low", "close", timeperiod=14), Column)
    assert isinstance(ta.beta("high", "low", timeperiod=5), Column)
    assert isinstance(ta.avgprice("open", "high", "low", "close"), Column)
    assert isinstance(ta.medprice("high", "low"), Column)
    assert isinstance(ta.typprice("high", "low", "close"), Column)
    assert isinstance(ta.wclprice("high", "low", "close"), Column)
    # T3 — the parked four: MAMA (split into mama/fama), SAR, SAREXT (8 params), MAVP (two-series,
    # the second being the per-row periods column).
    assert isinstance(ta.mama("close", fastlimit=0.5, slowlimit=0.05), Column)
    assert isinstance(ta.fama("close"), Column)
    assert isinstance(ta.sar("high", "low", acceleration=0.02, maximum=0.2), Column)
    assert isinstance(ta.sarext("high", "low"), Column)
    assert isinstance(ta.mavp("close", "periods", minperiod=5, maxperiod=20, matype=0), Column)


def test_math_operator_uppercase_aliases_are_the_same_functions() -> None:
    # `ta.MIN` / `ta.MAX` / `ta.SUM` are the TA-Lib-name aliases of the lowercase functions.
    assert ta.MIN is ta.min
    assert ta.MAX is ta.max
    assert ta.SUM is ta.sum


def test_unknown_indicator_is_rejected() -> None:
    # A typo'd TA function name is a loud error at build time, not a silent wrong column.
    from repark import _native

    with pytest.raises(ValueError, match="unknown TA window function"):
        _native.PyColumn.ta_window("ta_not_real", [])


# ==================================================================================================
# DataFrame route — bit-exact vs the C-TA-Lib goldens (== the kernel)
# ==================================================================================================


def test_ema_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.ema("close", timeperiod=21))
    _assert_bit_exact(engine, _golden("ema_21"))


def test_sma_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.sma("close", timeperiod=20))
    _assert_bit_exact(engine, _golden("sma_20"))


def test_rsi_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.rsi("close", timeperiod=14))
    _assert_bit_exact(engine, _golden("rsi_14"))


def test_adx_multi_input_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.adx("high", "low", "close", timeperiod=14))
    _assert_bit_exact(engine, _golden("adx_14"))


def test_stddev_with_nbdev_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.stddev("close", timeperiod=5, nbdev=2.0))
    _assert_bit_exact(engine, _golden("stddev_5_nbdev2"))


def test_correl_two_series_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    # CORREL of high vs low — the crate's golden pairing.
    engine = _engine_column(bars, ta.correl("high", "low", timeperiod=14))
    _assert_bit_exact(engine, _golden("correl_14"))


def test_min_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.min("close", timeperiod=21))
    _assert_bit_exact(engine, _golden("min_21"))


def test_max_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.max("close", timeperiod=21))
    _assert_bit_exact(engine, _golden("max_21"))


def test_sum_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.sum("close", timeperiod=21))
    _assert_bit_exact(engine, _golden("sum_21"))


def test_linearreg_angle_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.linearreg_angle("close", timeperiod=14))
    _assert_bit_exact(engine, _golden("linearreg_angle_14"))


def test_bbands_split_outputs_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    # Each split output resolves to the matching golden band.
    _assert_bit_exact(
        _engine_column(bars, ta.bbands_upper("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0)),
        _golden("bbands_20_upper"),
    )
    _assert_bit_exact(
        _engine_column(bars, ta.bbands_middle("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0)),
        _golden("bbands_20_middle"),
    )
    _assert_bit_exact(
        _engine_column(bars, ta.bbands_lower("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0)),
        _golden("bbands_20_lower"),
    )


def test_wma_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.wma("close", timeperiod=10))
    _assert_bit_exact(engine, _golden("wma_10"))


def test_dema_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.dema("close", timeperiod=10))
    _assert_bit_exact(engine, _golden("dema_10"))


def test_tema_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.tema("close", timeperiod=10))
    _assert_bit_exact(engine, _golden("tema_10"))


def test_trima_odd_and_even_routes_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    _assert_bit_exact(_engine_column(bars, ta.trima("close", timeperiod=10)), _golden("trima_10"))
    _assert_bit_exact(_engine_column(bars, ta.trima("close", timeperiod=5)), _golden("trima_5"))


def test_kama_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.kama("close", timeperiod=10))
    _assert_bit_exact(engine, _golden("kama_10"))


def test_t3_vfactor_routes_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    # Default vfactor and a non-default one, proving vfactor threads through the literal args.
    _assert_bit_exact(
        _engine_column(bars, ta.t3("close", timeperiod=5, vfactor=0.7)), _golden("t3_5")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.t3("close", timeperiod=5, vfactor=0.5)), _golden("t3_5_vf05")
    )


def test_midpoint_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.midpoint("close", timeperiod=10))
    _assert_bit_exact(engine, _golden("midpoint_10"))


def test_midprice_two_series_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.midprice("high", "low", timeperiod=10))
    _assert_bit_exact(engine, _golden("midprice_10"))


def test_rate_of_change_family_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    _assert_bit_exact(_engine_column(bars, ta.mom("close", timeperiod=10)), _golden("mom_10"))
    _assert_bit_exact(_engine_column(bars, ta.roc("close", timeperiod=10)), _golden("roc_10"))
    _assert_bit_exact(_engine_column(bars, ta.rocp("close", timeperiod=10)), _golden("rocp_10"))
    _assert_bit_exact(_engine_column(bars, ta.rocr("close", timeperiod=10)), _golden("rocr_10"))
    _assert_bit_exact(
        _engine_column(bars, ta.rocr100("close", timeperiod=10)), _golden("rocr100_10")
    )


def test_willr_cci_cmo_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    _assert_bit_exact(
        _engine_column(bars, ta.willr("high", "low", "close", timeperiod=14)), _golden("willr_14")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.cci("high", "low", "close", timeperiod=14)), _golden("cci_14")
    )
    _assert_bit_exact(_engine_column(bars, ta.cmo("close", timeperiod=14)), _golden("cmo_14"))


def test_bop_four_series_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.bop("open", "high", "low", "close"))
    _assert_bit_exact(engine, _golden("bop"))


def test_apo_ppo_matype_default_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    # matype 0 (SMA) — the polars_talib default the goldens were recorded at.
    _assert_bit_exact(
        _engine_column(bars, ta.apo("close", fastperiod=12, slowperiod=26, matype=0)),
        _golden("apo_12_26"),
    )
    _assert_bit_exact(
        _engine_column(bars, ta.ppo("close", fastperiod=12, slowperiod=26, matype=0)),
        _golden("ppo_12_26"),
    )


def test_apo_ppo_ma_macdext_matype7_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    """Octo C5/C6/C8: product DataFrame path for matype 7 (MAMA) is bit-exact vs kernel goldens."""
    _assert_bit_exact(
        _engine_column(bars, ta.ma("close", timeperiod=30, matype=7)),
        _golden("ma_30_type7"),
    )
    _assert_bit_exact(
        _engine_column(bars, ta.apo("close", fastperiod=12, slowperiod=26, matype=7)),
        _golden("apo_12_26_type7"),
    )
    _assert_bit_exact(
        _engine_column(bars, ta.ppo("close", fastperiod=12, slowperiod=26, matype=7)),
        _golden("ppo_12_26_type7"),
    )
    _assert_bit_exact(
        _engine_column(
            bars,
            ta.macdext(
                "close",
                fastperiod=12,
                fastmatype=7,
                slowperiod=26,
                slowmatype=7,
                signalperiod=9,
                signalmatype=7,
            ),
        ),
        _golden("macdext_12_26_9_type7_macd"),
    )


def test_aroon_split_and_oscillator_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    _assert_bit_exact(
        _engine_column(bars, ta.aroon_down("high", "low", timeperiod=14)), _golden("aroon_14_down")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.aroon_up("high", "low", timeperiod=14)), _golden("aroon_14_up")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.aroonosc("high", "low", timeperiod=14)), _golden("aroonosc_14")
    )


def test_trix_and_ultosc_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    _assert_bit_exact(_engine_column(bars, ta.trix("close", timeperiod=30)), _golden("trix_30"))
    _assert_bit_exact(
        _engine_column(
            bars, ta.ultosc("high", "low", "close", timeperiod1=7, timeperiod2=14, timeperiod3=28)
        ),
        _golden("ultosc_7_14_28"),
    )


def test_directional_family_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    _assert_bit_exact(
        _engine_column(bars, ta.dx("high", "low", "close", timeperiod=14)), _golden("dx_14")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.adxr("high", "low", "close", timeperiod=14)), _golden("adxr_14")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.plus_di("high", "low", "close", timeperiod=14)),
        _golden("plus_di_14"),
    )
    _assert_bit_exact(
        _engine_column(bars, ta.minus_di("high", "low", "close", timeperiod=14)),
        _golden("minus_di_14"),
    )
    _assert_bit_exact(
        _engine_column(bars, ta.plus_dm("high", "low", timeperiod=14)), _golden("plus_dm_14")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.minus_dm("high", "low", timeperiod=14)), _golden("minus_dm_14")
    )


def test_macd_family_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    # MACD split into three outputs (the polars_talib defaults 12/26/9).
    _assert_bit_exact(_engine_column(bars, ta.macd("close")), _golden("macd_12_26_9_macd"))
    _assert_bit_exact(_engine_column(bars, ta.macd_signal("close")), _golden("macd_12_26_9_signal"))
    _assert_bit_exact(_engine_column(bars, ta.macd_hist("close")), _golden("macd_12_26_9_hist"))
    # MACDFIX (12/26 fixed constants; signal only).
    _assert_bit_exact(_engine_column(bars, ta.macdfix("close")), _golden("macdfix_9_macd"))
    _assert_bit_exact(_engine_column(bars, ta.macdfix_signal("close")), _golden("macdfix_9_signal"))
    _assert_bit_exact(_engine_column(bars, ta.macdfix_hist("close")), _golden("macdfix_9_hist"))
    # MACDEXT at the matype-0 (SMA) defaults — the seven-param ergonomics path.
    _assert_bit_exact(_engine_column(bars, ta.macdext("close")), _golden("macdext_12_26_9_macd"))
    _assert_bit_exact(
        _engine_column(bars, ta.macdext_signal("close")), _golden("macdext_12_26_9_signal")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.macdext_hist("close")), _golden("macdext_12_26_9_hist")
    )


def test_ma_selector_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    # matype 0 (SMA) and matype 1 (EMA) prove the selector dispatch through the literal args.
    _assert_bit_exact(
        _engine_column(bars, ta.ma("close", timeperiod=30, matype=0)), _golden("ma_30_type0")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.ma("close", timeperiod=20, matype=1)), _golden("ma_20_type1")
    )


def test_stochastics_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    # STOCH / STOCHF (H/L/C) and STOCHRSI (close), each split into two outputs, at the
    # polars_talib defaults (matype 0 = SMA) the goldens were recorded at.
    _assert_bit_exact(
        _engine_column(bars, ta.stoch_slowk("high", "low", "close")), _golden("stoch_slowk")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.stoch_slowd("high", "low", "close")), _golden("stoch_slowd")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.stochf_fastk("high", "low", "close")), _golden("stochf_fastk")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.stochf_fastd("high", "low", "close")), _golden("stochf_fastd")
    )
    _assert_bit_exact(_engine_column(bars, ta.stochrsi_fastk("close")), _golden("stochrsi_fastk"))
    _assert_bit_exact(_engine_column(bars, ta.stochrsi_fastd("close")), _golden("stochrsi_fastd"))


def test_stochastics_matype7_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    """Group G2: product DataFrame path for stochastic matype 7 (MAMA) is bit-exact vs goldens.

    Pins all 8 recorded bins through the split entry points that actually pass the matype
    kwargs which route MAMA — including ``stoch_slowd`` with ``slowd_matype=7`` and the %K
    facades with ``fastd_matype=7`` (lookback depends on that matype even for the %K line).
    """
    # STOCH all-MAMA: both legs via both split facades (slowk_matype=7, slowd_matype=7).
    _assert_bit_exact(
        _engine_column(
            bars,
            ta.stoch_slowk(
                "high",
                "low",
                "close",
                slowk_matype=7,
                slowd_matype=7,
            ),
        ),
        _golden("stoch_type7_slowk"),
    )
    _assert_bit_exact(
        _engine_column(
            bars,
            ta.stoch_slowd(
                "high",
                "low",
                "close",
                slowk_matype=7,
                slowd_matype=7,
            ),
        ),
        _golden("stoch_type7_slowd"),
    )
    # STOCH mixed 7/0: slowk MAMA + slowd SMA — both legs (pins slowk_matype=7 on slowd facade
    # and slowd_matype=0; the complementary all-MAMA case above pins slowd_matype=7).
    _assert_bit_exact(
        _engine_column(
            bars,
            ta.stoch_slowk(
                "high",
                "low",
                "close",
                slowk_matype=7,
                slowd_matype=0,
            ),
        ),
        _golden("stoch_mixed_7_0_slowk"),
    )
    _assert_bit_exact(
        _engine_column(
            bars,
            ta.stoch_slowd(
                "high",
                "low",
                "close",
                slowk_matype=7,
                slowd_matype=0,
            ),
        ),
        _golden("stoch_mixed_7_0_slowd"),
    )
    # STOCHF type7: fastd_matype=7 on both fastk and fastd facades (fastd matype changes
    # lookback_total and which dense-buffer slice lands in the %K line).
    _assert_bit_exact(
        _engine_column(
            bars,
            ta.stochf_fastk("high", "low", "close", fastd_matype=7),
        ),
        _golden("stochf_type7_fastk"),
    )
    _assert_bit_exact(
        _engine_column(
            bars,
            ta.stochf_fastd("high", "low", "close", fastd_matype=7),
        ),
        _golden("stochf_type7_fastd"),
    )
    # STOCHRSI type7: same fastd_matype=7 on both split facades.
    _assert_bit_exact(
        _engine_column(
            bars,
            ta.stochrsi_fastk("close", fastd_matype=7),
        ),
        _golden("stochrsi_type7_fastk"),
    )
    _assert_bit_exact(
        _engine_column(
            bars,
            ta.stochrsi_fastd("close", fastd_matype=7),
        ),
        _golden("stochrsi_type7_fastd"),
    )


def test_natr_and_beta_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    # NATR (H/L/C + period) and BETA (high vs low, two-series) — the WG5 non-trivial kernels.
    _assert_bit_exact(
        _engine_column(bars, ta.natr("high", "low", "close", timeperiod=14)), _golden("natr_14")
    )
    _assert_bit_exact(_engine_column(bars, ta.beta("high", "low", timeperiod=5)), _golden("beta_5"))


def test_price_transforms_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    # The four no-period O/H/L/C price transforms.
    _assert_bit_exact(
        _engine_column(bars, ta.avgprice("open", "high", "low", "close")), _golden("avgprice")
    )
    _assert_bit_exact(_engine_column(bars, ta.medprice("high", "low")), _golden("medprice"))
    _assert_bit_exact(
        _engine_column(bars, ta.typprice("high", "low", "close")), _golden("typprice")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.wclprice("high", "low", "close")), _golden("wclprice")
    )


def test_mama_split_outputs_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    # MAMA split into its two outputs (mama / fama) at TA-Lib's default limits.
    _assert_bit_exact(
        _engine_column(bars, ta.mama("close", fastlimit=0.5, slowlimit=0.05)), _golden("mama_mama")
    )
    _assert_bit_exact(
        _engine_column(bars, ta.fama("close", fastlimit=0.5, slowlimit=0.05)), _golden("mama_fama")
    )


def test_sar_and_sarext_match_the_kernel(spark: ReparkSession, bars: object) -> None:
    # SAR at the defaults; SAREXT at its defaults (auto direction, symmetric af, no offset — the
    # golden `sarext`, whose short-side output is negative).
    _assert_bit_exact(
        _engine_column(bars, ta.sar("high", "low", acceleration=0.02, maximum=0.2)), _golden("sar")
    )
    _assert_bit_exact(_engine_column(bars, ta.sarext("high", "low")), _golden("sarext"))


def test_mavp_variable_period_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    # MAVP over the per-row `periods` column at SMA and EMA — the EMA case pins C's shifted MA
    # seeding (a full-array port diverges).
    _assert_bit_exact(
        _engine_column(bars, ta.mavp("close", "periods", minperiod=5, maxperiod=20, matype=0)),
        _golden("mavp"),
    )
    _assert_bit_exact(
        _engine_column(bars, ta.mavp("close", "periods", minperiod=5, maxperiod=20, matype=1)),
        _golden("mavp_ema"),
    )


def test_column_form_and_string_shorthand_agree(spark: ReparkSession, bars: object) -> None:
    # `ta.ema("close", ...)` and `ta.ema(F.col("close"), ...)` must produce identical output.
    from repark import functions as F  # noqa: N812 — PySpark idiom

    via_string = _engine_column(bars, ta.ema("close", timeperiod=21))
    via_column = _engine_column(bars, ta.ema(F.col("close"), timeperiod=21))
    _assert_bit_exact(via_string, via_column)


# ==================================================================================================
# G-NAN — null_lookback opt-in (NaN lookback prefix → SQL NULL; mid-series NaN preserved)
# ==================================================================================================


def _engine_arrow_column(df: object, indicator: Column, ts_order: str = "ts"):
    """Run ``indicator.over(orderBy(ts))`` and return the Arrow column (nulls preserved)."""
    windowed = indicator.over(Window.orderBy(ts_order))
    table = df.withColumn("out", windowed).to_arrow().sort_by(ts_order)  # type: ignore[attr-defined]
    return table.column("out")


def test_null_lookback_default_false_keeps_nan_prefix_bit_exact(
    spark: ReparkSession, bars: object
) -> None:
    """Default path keeps kernel NaN prefix as valid slots — never SQL NULL (SC-6 / C5-Q-001).

    ``to_numpy`` maps Arrow null → NaN, so bit-exact goldens alone cannot distinguish SQL NULL
    from kernel NaN. A mutation that always wraps via ``_NullLookbackColumn`` would still pass
    pure ``to_bits`` / NaN↔NaN checks. Pin Arrow validity + ``isnan`` + ``null_count==0`` on the
    lookback prefix under explicit ``null_lookback=False`` *and* omitted kwarg (default).
    """
    cases: list[tuple[Column, str, int]] = [
        (ta.ema("close", timeperiod=21, null_lookback=False), "ema_21", 20),
        # Omitted kwarg must match explicit False (default off).
        (ta.ema("close", timeperiod=21), "ema_21", 20),
        (ta.rsi("close", timeperiod=14, null_lookback=False), "rsi_14", 14),
        (
            ta.bbands_upper("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0, null_lookback=False),
            "bbands_20_upper",
            19,
        ),
    ]
    for indicator, golden_name, lookback in cases:
        arrow_col = _engine_arrow_column(bars, indicator)
        golden = _golden(golden_name)
        assert len(arrow_col) == len(golden)
        assert arrow_col.null_count == 0, (
            f"{golden_name}: null_lookback=False must emit zero SQL NULLs "
            f"(kernel NaN prefix only), got null_count={arrow_col.null_count}"
        )
        for row_index in range(lookback):
            assert arrow_col[row_index].is_valid, (
                f"{golden_name}: row {row_index} must be a valid NaN slot under "
                f"null_lookback=False, not SQL NULL"
            )
            assert np.isnan(arrow_col[row_index].as_py()), (
                f"{golden_name}: row {row_index} must be kernel NaN under default path, "
                f"got {arrow_col[row_index].as_py()!r}"
            )
        engine = arrow_col.to_numpy(zero_copy_only=False)
        _assert_bit_exact(engine, golden)


def test_null_lookback_ema_rsi_bbands_null_prefix_matches_polars_talib_pattern(
    spark: ReparkSession, bars: object
) -> None:
    """With flag, lookback prefix is SQL NULL (polars_talib shape); post-prefix bit-matches golden.

    Goldens were recorded from polars_talib 0.1.5 (null → NaN in the .bin). The null *pattern*
    is the leading-NaN run length; values after the lookback stay bit-exact with the golden.
    Kernels: ema(21) lookback 20, rsi(14) lookback 14, bbands_upper(20) lookback 19.
    """
    cases: list[tuple[Column, str, int]] = [
        (ta.ema("close", timeperiod=21, null_lookback=True), "ema_21", 20),
        (ta.rsi("close", timeperiod=14, null_lookback=True), "rsi_14", 14),
        (
            ta.bbands_upper("close", timeperiod=20, nbdevup=2.0, nbdevdn=2.0, null_lookback=True),
            "bbands_20_upper",
            19,
        ),
    ]
    for indicator, golden_name, lookback in cases:
        arrow_col = _engine_arrow_column(bars, indicator)
        golden = _golden(golden_name)
        assert len(arrow_col) == len(golden)

        # Prefix: SQL NULL (not NaN) for exactly `lookback` rows.
        for row_index in range(lookback):
            assert not arrow_col[row_index].is_valid, (
                f"{golden_name}: row {row_index} should be NULL under null_lookback, "
                f"got {arrow_col[row_index].as_py()!r}"
            )

        # First post-lookback row is valid and finite (goldens have no mid-series NaN on these).
        assert arrow_col[lookback].is_valid, f"{golden_name}: first dense row must be non-null"
        assert arrow_col.null_count == lookback, (
            f"{golden_name}: expected exactly {lookback} nulls (prefix only), "
            f"got {arrow_col.null_count}"
        )

        # Dense values bit-match the golden (NaN↔NaN allowed; nulls already checked).
        engine = arrow_col.to_numpy(zero_copy_only=False)
        # Arrow nulls become NaN in numpy — compare only the dense suffix bit-exactly.
        _assert_bit_exact(engine[lookback:], golden[lookback:])


def test_null_lookback_does_not_convert_mid_series_nan(
    spark: ReparkSession, bars: object, tmp_path: Path
) -> None:
    """Mid-series NaN must stay NaN (valid slot, isnan), never become SQL NULL.

    Distinguishes lookback-by-length from blanket isnan: inject NaN into the *input* past the
    EMA lookback so the kernel propagates mid-series NaN, then assert that slot is non-null
    but NaN under null_lookback=True while the prefix is null.
    """
    close = _golden("fixture_close").copy()
    lookback = 20  # ema timeperiod=21
    inject_at = lookback + 50  # well past the prefix
    close[inject_at] = np.nan
    high = _golden("fixture_high")
    low = _golden("fixture_low")
    open_ = _golden("fixture_open")
    ts = np.arange(len(close), dtype=np.int64)
    table = pa.table({"ts": ts, "open": open_, "high": high, "low": low, "close": close})
    path = tmp_path / "bars_mid_nan.parquet"
    pq.write_table(table, path)
    df = spark.read_parquet(path)

    arrow_col = _engine_arrow_column(df, ta.ema("close", timeperiod=21, null_lookback=True))

    # Prefix still null.
    for row_index in range(lookback):
        assert not arrow_col[row_index].is_valid

    # At/after the injected input NaN the kernel emits NaN; it must be a *valid* NaN slot
    # (not SQL NULL) so consumers can tell lookback-null from data-NaN.
    # EMA propagates NaN from the injection point forward; check inject_at itself.
    assert arrow_col[inject_at].is_valid, (
        "mid-series NaN must remain a valid (non-null) slot under null_lookback"
    )
    assert np.isnan(arrow_col[inject_at].as_py()), "mid-series value must still be NaN"


def test_null_lookback_keyword_is_keyword_only() -> None:
    """null_lookback is keyword-only — positional misuse must fail loud (TypeError)."""
    with pytest.raises(TypeError):
        ta.ema("close", 21, True)  # type: ignore[misc, arg-type]


def _assert_null_lookback_prefix(
    arrow_col: object, lookback: int, label: str, *, golden_suffix: np.ndarray | None = None
) -> None:
    """Prefix is SQL NULL for exactly ``lookback`` rows; optional dense-suffix bit-match."""
    for row_index in range(lookback):
        assert not arrow_col[row_index].is_valid, (  # type: ignore[index]
            f"{label}: row {row_index} should be NULL under null_lookback, "
            f"got {arrow_col[row_index].as_py()!r}"  # type: ignore[index]
        )
    assert arrow_col.null_count == lookback, (  # type: ignore[attr-defined]
        f"{label}: expected exactly {lookback} nulls (prefix only), got {arrow_col.null_count}"  # type: ignore[attr-defined]
    )
    if lookback < len(arrow_col):  # type: ignore[arg-type]
        assert arrow_col[lookback].is_valid, f"{label}: first dense row must be non-null"  # type: ignore[index]
    if golden_suffix is not None:
        engine = arrow_col.to_numpy(zero_copy_only=False)  # type: ignore[attr-defined]
        _assert_bit_exact(engine[lookback:], golden_suffix[lookback:])


def test_null_lookback_macd_respects_kernel_period_swap(spark: ReparkSession, bars: object) -> None:
    """MACD lookback uses max(fast, slow) after kernel swap (C1-Q-001).

    ``fastperiod=26, slowperiod=12, signalperiod=9`` → kernel lookback 33, not 19.
    Default order 12/26/9 also yields 33 — pin both so slow-only and max formulas diverge.
    """
    # Swapped args: without max(), facade would null only (12-1)+(9-1)=19.
    swapped_lookback = 33  # (max(26, 12) - 1) + (9 - 1)
    for factory in (ta.macd, ta.macd_signal, ta.macd_hist):
        arrow_col = _engine_arrow_column(
            bars,
            factory("close", fastperiod=26, slowperiod=12, signalperiod=9, null_lookback=True),
        )
        _assert_null_lookback_prefix(
            arrow_col, swapped_lookback, f"{factory.__name__}_swapped_26_12_9"
        )
        # Kernel NaN path under null_lookback=False must have the same leading-NaN run.
        nan_path = _engine_column(
            bars,
            factory("close", fastperiod=26, slowperiod=12, signalperiod=9, null_lookback=False),
        )
        leading_nans = int(np.argmax(~np.isnan(nan_path))) if np.isnan(nan_path[0]) else 0
        if np.all(np.isnan(nan_path)):
            leading_nans = len(nan_path)
        assert leading_nans == swapped_lookback, (
            f"{factory.__name__}: kernel leading NaN count {leading_nans} != {swapped_lookback}"
        )

    # Canonical 12/26/9 golden path (compound formula; same length as swapped).
    default_lookback = 33  # (26 - 1) + (9 - 1)
    arrow_col = _engine_arrow_column(
        bars, ta.macd("close", fastperiod=12, slowperiod=26, signalperiod=9, null_lookback=True)
    )
    _assert_null_lookback_prefix(
        arrow_col,
        default_lookback,
        "macd_12_26_9",
        golden_suffix=_golden("macd_12_26_9_macd"),
    )


def test_null_lookback_ultosc_uses_max_of_three_periods(spark: ReparkSession, bars: object) -> None:
    """ULTOSC lookback is max(t1, t2, t3), not timeperiod3 alone (C1-Q-002).

    Unordered 28/14/7 → kernel lookback 28; timeperiod3-only would under-null at 7.
    """
    lookback = 28
    arrow_col = _engine_arrow_column(
        bars,
        ta.ultosc(
            "high",
            "low",
            "close",
            timeperiod1=28,
            timeperiod2=14,
            timeperiod3=7,
            null_lookback=True,
        ),
    )
    _assert_null_lookback_prefix(arrow_col, lookback, "ultosc_28_14_7")

    nan_path = _engine_column(
        bars,
        ta.ultosc(
            "high",
            "low",
            "close",
            timeperiod1=28,
            timeperiod2=14,
            timeperiod3=7,
            null_lookback=False,
        ),
    )
    leading_nans = int(np.argmax(~np.isnan(nan_path))) if np.isnan(nan_path[0]) else 0
    if np.all(np.isnan(nan_path)):
        leading_nans = len(nan_path)
    assert leading_nans == lookback, f"ultosc kernel leading NaN count {leading_nans} != {lookback}"

    # Ordered defaults still match the recorded golden after null-prefix conversion.
    ordered = _engine_arrow_column(
        bars,
        ta.ultosc(
            "high",
            "low",
            "close",
            timeperiod1=7,
            timeperiod2=14,
            timeperiod3=28,
            null_lookback=True,
        ),
    )
    _assert_null_lookback_prefix(
        ordered, 28, "ultosc_7_14_28", golden_suffix=_golden("ultosc_7_14_28")
    )


def test_null_lookback_ma_type_and_compound_formulas(spark: ReparkSession, bars: object) -> None:
    """Charter MA-type / compound lookback formulas under null_lookback=True (C1-Q-003).

    Pins fail if:
    - ``_ma_lookback`` DEMA (matype=3) collapses to ``period-1`` instead of ``2*(period-1)``
    - ``dema`` / ``ma(matype=3)`` / ``apo(matype=3)`` stop using the MA-type table
    - ``stoch_slowk`` drops a smoothing-leg ``_ma_lookback`` term
    """
    # DEMA(10): 2*(10-1)=18 — SMA-style period-1 would wrongly yield 9.
    dema_lookback = 18
    dema_col = _engine_arrow_column(bars, ta.dema("close", timeperiod=10, null_lookback=True))
    _assert_null_lookback_prefix(
        dema_col, dema_lookback, "dema_10", golden_suffix=_golden("dema_10")
    )

    # MA selector matype=3 routes through _ma_lookback (same DEMA length as dema()).
    ma_dema_col = _engine_arrow_column(
        bars, ta.ma("close", timeperiod=10, matype=3, null_lookback=True)
    )
    _assert_null_lookback_prefix(ma_dema_col, dema_lookback, "ma_10_matype3_dema")

    # APO matype=3: lookback = MA_Lookback(max(12, 26), DEMA) = 2*(26-1)=50.
    # Using only fastperiod would yield 2*(12-1)=22; SMA matype would yield 25.
    apo_lookback = 50
    apo_col = _engine_arrow_column(
        bars,
        ta.apo("close", fastperiod=12, slowperiod=26, matype=3, null_lookback=True),
    )
    _assert_null_lookback_prefix(apo_col, apo_lookback, "apo_12_26_matype3")
    apo_nan = _engine_column(
        bars,
        ta.apo("close", fastperiod=12, slowperiod=26, matype=3, null_lookback=False),
    )
    apo_leading = int(np.argmax(~np.isnan(apo_nan))) if np.isnan(apo_nan[0]) else 0
    if np.all(np.isnan(apo_nan)):
        apo_leading = len(apo_nan)
    assert apo_leading == apo_lookback, (
        f"apo matype=3 kernel leading NaN {apo_leading} != {apo_lookback}"
    )

    # STOCH slowk defaults: (fastk-1) + MA(slowk) + MA(slowd) = 4+2+2 = 8.
    # Dropping either MA leg yields 6 (or 4) — mutation-proof vs compound sum.
    stoch_lookback = 8
    stoch_col = _engine_arrow_column(
        bars,
        ta.stoch_slowk("high", "low", "close", null_lookback=True),
    )
    _assert_null_lookback_prefix(
        stoch_col, stoch_lookback, "stoch_slowk_defaults", golden_suffix=_golden("stoch_slowk")
    )


# ==================================================================================================
# r21 T4: ta-etl — window fusion plan shape + over_columns helper
# ==================================================================================================


def _physical_plan_text(df: object) -> str:
    """Capture ``DataFrame.explain()`` physical plan body as plain text."""
    import contextlib
    import io
    import re

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain()  # type: ignore[attr-defined]
    text = buffer.getvalue()
    match = re.search(
        r"plan_type='physical_plan', plan='((?:\\'|[^'])*)'",
        text,
    )
    if match is None:
        return text
    body = match.group(1).replace("\\n", "\n").replace("\\'", "'")
    return body


def test_over_columns_type_guards() -> None:
    """``ta.over_columns`` refuses bad window / map / key / value shapes loud."""
    window = Window.orderBy("ts")
    with pytest.raises(Exception, match="WindowSpec"):
        ta.over_columns("not-a-window", {"ema": ta.ema("close", timeperiod=5)})  # type: ignore[arg-type]
    with pytest.raises(Exception, match="dict"):
        ta.over_columns(window, [("ema", ta.ema("close", timeperiod=5))])  # type: ignore[arg-type]
    with pytest.raises(Exception, match="str column names"):
        ta.over_columns(window, {1: ta.ema("close", timeperiod=5)})  # type: ignore[dict-item]
    with pytest.raises(Exception, match="non-empty"):
        ta.over_columns(window, {"  ": ta.ema("close", timeperiod=5)})
    with pytest.raises(Exception, match="Column"):
        ta.over_columns(window, {"ema": "close"})  # type: ignore[dict-item]


def test_over_columns_withcolumns_fuses_window_agg(bars: object) -> None:
    """Same-spec multi-TA via ``over_columns`` + ``withColumns`` → one ``WindowAggExec``.

    r21 T4 hour-0: sequential ``withColumn`` stacks N window operators; batching is the
    fused plan the ETL path should use. Pin the plan shape (not wall time).
    """
    window = Window.orderBy("ts")
    bare = {
        "ema5": ta.ema("close", timeperiod=5),
        "sma10": ta.sma("close", timeperiod=10),
        "rsi14": ta.rsi("close", timeperiod=14),
        "mom10": ta.mom("close", timeperiod=10),
    }
    fused = bars.withColumns(ta.over_columns(window, bare))  # type: ignore[attr-defined]
    plan = _physical_plan_text(fused)
    assert plan.count("WindowAggExec") == 1, plan[:1500]
    # Bit-exact vs sequential withColumn on the Arrow path (value + presence).
    sequential = bars  # type: ignore[assignment]
    for name, column in bare.items():
        sequential = sequential.withColumn(name, column.over(window))  # type: ignore[attr-defined]
    fused_table = fused.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    sequential_table = sequential.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    for name in bare:
        left = fused_table.column(name).to_numpy(zero_copy_only=False)
        right = sequential_table.column(name).to_numpy(zero_copy_only=False)
        _assert_bit_exact(left, right)


def test_sequential_withcolumn_same_spec_merges_window_aggs(bars: object) -> None:
    """N x ``withColumn`` of independent same-spec TA → 1 ``WindowAggExec`` (r23b N2 stage b).

    Pre-N2 this stacked N WindowAggExec nodes (T4 anti-pattern). Adjacent same-spec merge
    collapses independent chains; dependent stacks still pin in ``test_n2_plan_collapse``.
    """
    window = Window.orderBy("ts")
    frame = bars  # type: ignore[assignment]
    names = ("ema5", "sma10", "rsi14", "mom10")
    builders = (
        ta.ema("close", timeperiod=5),
        ta.sma("close", timeperiod=10),
        ta.rsi("close", timeperiod=14),
        ta.mom("close", timeperiod=10),
    )
    for name, column in zip(names, builders, strict=True):
        frame = frame.withColumn(name, column.over(window))  # type: ignore[attr-defined]
    plan = _physical_plan_text(frame)
    assert plan.count("WindowAggExec") == 1, plan[:2000]
    # Bit-exact vs single fused withColumns on the Arrow path.
    bare = {name: column.over(window) for name, column in zip(names, builders, strict=True)}
    fused = bars.withColumns(bare)  # type: ignore[attr-defined]
    left = frame.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    right = fused.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    for name in names:
        left_arr = left.column(name).to_numpy(zero_copy_only=False)
        right_arr = right.column(name).to_numpy(zero_copy_only=False)
        _assert_bit_exact(left_arr, right_arr)


def test_all_covers_every_public_ta_entry_point() -> None:
    """``ta.__all__`` lists every public def — ``wma`` was silently missing (F-4 finding),
    so ``from repark.spark.ta import *`` skipped it. This pin closes the class."""
    import ast
    import inspect

    from repark.spark import ta

    tree = ast.parse(inspect.getsource(ta))
    public = {
        node.name
        for node in ast.iter_child_nodes(tree)
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and not node.name.startswith("_")
    }
    missing = public - set(ta.__all__)
    assert not missing, f"public names absent from ta.__all__: {sorted(missing)}"
    assert "wma" in ta.__all__
