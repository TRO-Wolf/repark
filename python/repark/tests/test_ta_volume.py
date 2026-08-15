"""TA-4 facade tests: volume-family kernels through ``repark.ta``.

Does **not** edit ``test_ta.py``. The DataFrame route
(``ta.ad(...).over(Window.orderBy("ts"))``) must produce output that is
``f64::to_bits``-identical to the TA-3-recorded C TA-Lib 0.4.0 goldens (and therefore
to the ``repark-ta`` kernels). Input is the 5000-row OHLC + volume fixture written to
Parquet and ``read_parquet``-ed.
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
    return ReparkSession.builder.appName("pytest-ta-volume").getOrCreate()


@pytest.fixture
def bars(spark: ReparkSession, tmp_path: Path) -> object:
    """The 5000-row OHLC + volume fixture as a DataFrame."""
    high = _golden("fixture_high")
    low = _golden("fixture_low")
    close = _golden("fixture_close")
    volume = _golden("fixture_volume")
    ts = np.arange(len(close), dtype=np.int64)
    table = pa.table({"ts": ts, "high": high, "low": low, "close": close, "volume": volume})
    path = tmp_path / "bars_volume.parquet"
    pq.write_table(table, path)
    return spark.read_parquet(path)


def _engine_column(df: object, indicator: Column, ts_order: str = "ts") -> np.ndarray:
    """Run ``indicator.over(orderBy(ts))`` and return the ``f64`` column in ``ts`` order."""
    windowed = indicator.over(Window.orderBy(ts_order))
    table = df.withColumn("out", windowed).to_arrow().sort_by(ts_order)  # type: ignore[attr-defined]
    return table.column("out").to_numpy(zero_copy_only=False)


def _assert_bit_exact(engine: np.ndarray, expected: np.ndarray) -> None:
    """Strict ``to_bits`` equality, ``NaN`` ↔ ``NaN`` allowed (any payload)."""
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


def test_volume_functions_return_columns() -> None:
    assert isinstance(ta.ad("high", "low", "close", "volume"), Column)
    assert isinstance(
        ta.adosc("high", "low", "close", "volume", fastperiod=3, slowperiod=10),
        Column,
    )
    assert isinstance(ta.obv("close", "volume"), Column)
    assert isinstance(ta.mfi("high", "low", "close", "volume", timeperiod=14), Column)


def test_volume_keywords_match_polars_talib() -> None:
    """Keyword names are the polars_talib spellings the goldens were recorded with."""
    assert isinstance(
        ta.adosc("high", "low", "close", "volume", fastperiod=10, slowperiod=3), Column
    )
    assert isinstance(ta.mfi("high", "low", "close", "volume", timeperiod=2), Column)


def test_ad_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.ad("high", "low", "close", "volume"))
    _assert_bit_exact(engine, _golden("ad"))


def test_adosc_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(
        bars, ta.adosc("high", "low", "close", "volume", fastperiod=3, slowperiod=10)
    )
    _assert_bit_exact(engine, _golden("adosc_3_10"))


def test_obv_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.obv("close", "volume"))
    _assert_bit_exact(engine, _golden("obv"))


def test_mfi_dataframe_route_matches_the_kernel(spark: ReparkSession, bars: object) -> None:
    engine = _engine_column(bars, ta.mfi("high", "low", "close", "volume", timeperiod=14))
    _assert_bit_exact(engine, _golden("mfi_14"))
