"""TA-SINGLE-SERIES-PARALLEL-1 S2a — a bare ``ta.*`` column is a series over the resolved order.

A bare ``ta.*`` column (no ``.over(...)``) is bound as ``.over(Window.orderBy(K))``: K is the
frame's declared order, else the first timestamp column (else the first date column), else the
frame's current row order, with one ``UserWarning`` per session for the last two. ``null_lookback``
is native: the window function emits the first ``lookback`` rows of each window partition as NULL.
"""

from __future__ import annotations

import contextlib
import io
import re
import warnings
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession, Window, ta
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

ROWS = 4_000
TS = "event_timestamp_utc"


@pytest.fixture
def spark() -> ReparkSession:
    return (
        ReparkSession.builder.appName("pytest-ta-series")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def _bars(seed: int = 7) -> pa.Table:
    rng = np.random.default_rng(seed)
    close = 4_000.0 + np.cumsum(rng.normal(scale=2.0, size=ROWS))
    high = close + np.abs(rng.normal(size=ROWS)) + 0.25
    low = close - np.abs(rng.normal(size=ROWS)) - 0.25
    opened = close + rng.normal(scale=0.5, size=ROWS)
    stamps = np.arange(ROWS, dtype=np.int64) * 60_000_000 + 1_700_000_000_000_000
    order = rng.permutation(ROWS)
    return pa.table(
        {
            "ticker_epoch": pa.array(np.arange(ROWS, dtype=np.int64)[order]),
            "contract_symbol": pa.array(np.where(np.arange(ROWS) % 2 == 0, "ESZ5", "NQZ5")[order]),
            TS: pa.array(stamps[order], pa.timestamp("us")),
            "open": pa.array(opened[order]),
            "high": pa.array(high[order]),
            "low": pa.array(low[order]),
            "close": pa.array(close[order]),
        }
    )


def _read(spark: ReparkSession, tmp_path: Path, table: pa.Table, name: str = "bars") -> object:
    path = tmp_path / f"{name}.parquet"
    pq.write_table(table, path)
    return spark.read.parquet(str(path))


def _owner_levels(frame: object, window: object | None) -> object:
    def windowed(column: object) -> object:
        return column if window is None else column.over(window)

    return (
        frame.withColumns(
            {
                "ema21": windowed(ta.ema("close", timeperiod=21)).round(4),
                "test_21": windowed(ta.ema("close", timeperiod=21)).round(4) / F.col("close"),
            }
        )
        .withColumns({"tr": windowed(ta.trange("high", "low", "close"))})
        .withColumns(
            {
                "ema5": windowed(ta.ema("close", timeperiod=5)).round(4),
                "rsi13": windowed(ta.rsi("close", timeperiod=13)).round(4),
                "sma10": windowed(ta.sma("close", timeperiod=10)).round(4),
                "ETR5": windowed(ta.ema("tr", timeperiod=5)).round(4) / F.col("close"),
                "ETR13": windowed(ta.ema("tr", timeperiod=13, null_lookback=True)).round(4),
                "ETR21": windowed(ta.ema("tr", timeperiod=21)),
                "DIP21": F.col("close") / F.col("ema21"),
                "lr5": windowed(ta.linearreg("close", timeperiod=5)).round(4),
                "ADX5": windowed(ta.adx("high", "low", "close", timeperiod=5)).round(4),
                "ADX21": windowed(ta.adx("high", "low", "close", timeperiod=21)).round(4),
            }
        )
        .withColumns(
            {
                "RSID13": F.col("rsi13") / F.col("close"),
                "new_open": F.coalesce(F.col("open"), F.lit(0.0)),
                "new_high": F.when(F.col("open") > F.col("high"), F.lit("YES")).otherwise(
                    F.lit("NO")
                ),
            }
        )
    )


def _arrow(frame: object, key: str = TS) -> pa.Table:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return frame.toArrow().sort_by(key)  # type: ignore[attr-defined]


def _assert_bit_equal(got: pa.Table, expected: pa.Table) -> None:
    assert got.column_names == expected.column_names
    for name in got.column_names:
        left = got[name].combine_chunks()
        right = expected[name].combine_chunks()
        assert left.type == right.type, name
        assert pc.is_null(left).equals(pc.is_null(right)), f"{name} validity"
        if pa.types.is_floating(left.type):
            valid = ~np.asarray(pc.is_null(left))
            lbits = np.asarray(left.fill_null(0.0)).view(np.uint64)[valid]
            rbits = np.asarray(right.fill_null(0.0)).view(np.uint64)[valid]
            assert np.array_equal(lbits, rbits), f"{name} bits"
        else:
            assert left.equals(right), name


def _series_warnings(record: list[warnings.WarningMessage]) -> list[str]:
    return [
        str(entry.message)
        for entry in record
        if issubclass(entry.category, UserWarning) and str(entry.message).startswith("ta.* series")
    ]


def _physical_plan_text(frame: object) -> str:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        frame.explain()  # type: ignore[attr-defined]
    text = buffer.getvalue()
    match = re.search(r"plan_type='physical_plan', plan='((?:\\'|[^'])*)'", text)
    if match is None:
        return text
    return match.group(1).replace("\\n", "\n").replace("\\'", "'")


@pytest.mark.parametrize("eager", [False, True])
def test_series_equals_explicit_orderby(spark: ReparkSession, tmp_path: Path, eager: bool) -> None:
    """P-S2a-1: the owner's levels, bare against ``.over(Window.orderBy(ts))``, bit for bit."""
    source = _read(spark, tmp_path, _bars()).sort(TS)
    if eager:
        source = source.eager().lazy()
    bare = _arrow(_owner_levels(source, None))
    explicit = _arrow(_owner_levels(source, Window.orderBy(TS)))
    _assert_bit_equal(bare, explicit)
    nulls, nans = _null_and_nan_rows(explicit, "ETR13")
    assert (nulls, nans) == (list(range(12)), [12])
    assert bare.num_rows == ROWS


def test_series_order_declared_beats_temporal(spark: ReparkSession, tmp_path: Path) -> None:
    """P-S2a-2: a frame sorted by a later timestamp column is ordered by it, without a warning."""
    table = _bars().append_column(
        "booked", pa.array(np.arange(ROWS, dtype=np.int64)[::-1] * 1_000_000, pa.timestamp("us"))
    )
    frame = _read(spark, tmp_path, table).sort("booked")
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        bare = frame.withColumn("ema5", ta.ema("close", timeperiod=5))
    assert _series_warnings(record) == []
    explicit = frame.withColumn(
        "ema5", ta.ema("close", timeperiod=5).over(Window.orderBy("booked"))
    )
    _assert_bit_equal(_arrow(bare), _arrow(explicit))
    by_first = frame.withColumn("ema5", ta.ema("close", timeperiod=5).over(Window.orderBy(TS)))
    assert not _arrow(bare)["ema5"].equals(_arrow(by_first)["ema5"])


def test_series_order_timestamp_before_date_and_warns_once(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """P-S2a-3: a DATE first and a TIMESTAMP later resolves to the TIMESTAMP; one warning."""
    bars = _bars()
    days = pa.array((np.asarray(bars[TS].cast(pa.int64())) // 86_400_000_000).astype(np.int32))
    table = pa.table(
        {
            "trade_day": days.cast(pa.date32()),
            "close": bars["close"],
            TS: bars[TS],
        }
    )
    frame = _read(spark, tmp_path, table)
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        first = frame.withColumn("ema5", ta.ema("close", timeperiod=5))
        second = first.withColumns({"rsi5": ta.rsi("close", timeperiod=5)})
        third = frame.select(F.col(TS), ta.sma("close", timeperiod=3).alias("sma3"))
    notices = _series_warnings(record)
    assert notices == [
        f"ta.* series ordered by '{TS}' (the first date/timestamp column) because the frame has "
        "no declared order; sort the frame first or use .over(Window.orderBy(...))"
    ]
    window = Window.orderBy(TS)
    expected = frame.withColumn("ema5", ta.ema("close", timeperiod=5).over(window)).withColumns(
        {"rsi5": ta.rsi("close", timeperiod=5).over(window)}
    )
    _assert_bit_equal(_arrow(second), _arrow(expected))
    _assert_bit_equal(
        _arrow(third),
        _arrow(frame.select(F.col(TS), ta.sma("close", timeperiod=3).over(window).alias("sma3"))),
    )
    by_day = frame.withColumn(
        "ema5", ta.ema("close", timeperiod=5).over(Window.orderBy("trade_day"))
    )
    assert not _arrow(first)["ema5"].equals(_arrow(by_day)["ema5"])


def test_series_order_date_fallback(spark: ReparkSession, tmp_path: Path) -> None:
    """P-S2a-3b: with no timestamp column, the first DATE column orders the series."""
    bars = _bars()
    rank = np.asarray(bars["ticker_epoch"])
    table = pa.table(
        {
            "close": bars["close"],
            "session_day": pa.array(rank.astype(np.int32)).cast(pa.date32()),
            "settle_day": pa.array((ROWS - rank).astype(np.int32)).cast(pa.date32()),
        }
    )
    frame = _read(spark, tmp_path, table)
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        bare = frame.withColumn("ema5", ta.ema("close", timeperiod=5))
    assert [notice.split(" (")[0] for notice in _series_warnings(record)] == [
        "ta.* series ordered by 'session_day'"
    ]
    explicit = frame.withColumn(
        "ema5", ta.ema("close", timeperiod=5).over(Window.orderBy("session_day"))
    )
    _assert_bit_equal(_arrow(bare, "session_day"), _arrow(explicit, "session_day"))


def test_bare_ta_current_row_order_reads_partitions_in_index_order(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """P-S2a-4 (facade side): no order and no temporal column → partition-index order, warned."""
    close = np.asarray(_bars()["close"])
    parts = []
    for index in range(4):
        rows = slice(index * ROWS // 4, (index + 1) * ROWS // 4)
        table = pa.table({"rowid": np.arange(ROWS, dtype=np.int64)[rows], "close": close[rows]})
        parts.append(_read(spark, tmp_path, table, f"part{index}"))
    frame = parts[0].union(parts[1]).union(parts[2]).union(parts[3])
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        bare = frame.withColumn("ema5", ta.ema("close", timeperiod=5))
    assert [notice.split(" because")[0] for notice in _series_warnings(record)] == [
        "ta.* series computed over the frame's current row order"
    ]
    plan = _physical_plan_text(bare)
    assert "ParallelWindowExec" in plan, plan
    assert "CoalescePartitionsExec" not in plan, plan
    expected = _arrow(
        frame.withColumn("ema5", ta.ema("close", timeperiod=5).over(Window.orderBy("rowid"))),
        "rowid",
    )
    for _ in range(3):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            got = bare.toArrow()  # type: ignore[attr-defined]
        assert np.array_equal(np.asarray(got["rowid"]), np.arange(ROWS))
        _assert_bit_equal(got, expected)


def _two_symbol_frame(spark: ReparkSession, tmp_path: Path) -> object:
    return _read(spark, tmp_path, _bars()).sort(TS)


def _null_and_nan_rows(table: pa.Table, column: str) -> tuple[list[int], list[int]]:
    values = table[column].combine_chunks()
    nulls = np.flatnonzero(np.asarray(pc.is_null(values)))
    filled = np.asarray(values.fill_null(0.0))
    nans = np.flatnonzero(np.isnan(filled) & ~np.asarray(pc.is_null(values)))
    return nulls.tolist(), nans.tolist()


def test_null_lookback_native_matches_row_number_rewrite(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """P-S2a-5: native ``null_lookback`` gives the NULL and NaN cells of row_number + CASE."""
    frame = _two_symbol_frame(spark, tmp_path)
    partitioned = Window.partitionBy("contract_symbol").orderBy(TS)
    whole = Window.orderBy(TS)
    staged = frame.withColumns({"tr": ta.trange("high", "low", "close").over(partitioned)})
    lookback = 12

    def rewrite(window: object) -> object:
        return F.when(
            F.row_number().over(window) > lookback, ta.ema("tr", timeperiod=13).over(window)
        )

    result = _arrow(
        staged.withColumns(
            {
                "native": ta.ema("tr", timeperiod=13, null_lookback=True).over(partitioned),
                "rewrite": rewrite(partitioned),
                "whole_native": ta.ema("close", timeperiod=13, null_lookback=True).over(whole),
                "whole_rewrite": F.when(
                    F.row_number().over(whole) > lookback,
                    ta.ema("close", timeperiod=13).over(whole),
                ),
                "bare_native": ta.ema("close", timeperiod=13, null_lookback=True),
            }
        )
    )
    native = _null_and_nan_rows(result, "native")
    assert native == _null_and_nan_rows(result, "rewrite")
    _assert_bit_equal(
        result.select(["native"]), result.select(["rewrite"]).rename_columns(["native"])
    )
    symbols = np.asarray(result["contract_symbol"])
    for symbol in ("ESZ5", "NQZ5"):
        rows = np.flatnonzero(symbols == symbol)
        nulls, nans = _null_and_nan_rows(result.take(pa.array(rows)), "native")
        assert (len(nulls), len(nans)) == (12, 1), symbol
    whole_cells = _null_and_nan_rows(result, "whole_native")
    assert whole_cells == _null_and_nan_rows(result, "whole_rewrite")
    assert whole_cells == _null_and_nan_rows(result, "bare_native")
    assert len(whole_cells[0]) == lookback
    helper = _arrow(
        ta.with_indicators(
            staged,
            partition="contract_symbol",
            order=TS,
            columns={"helper": ta.ema("tr", timeperiod=13)},
            null_lookback=True,
        )
    )
    assert _null_and_nan_rows(helper, "helper") == native


@pytest.mark.parametrize("partitions", ["1", "16"])
def test_mixing_series_and_partitioned_window(tmp_path: Path, partitions: str) -> None:
    """P-S2a-7: a series and a partitioned window in one withColumns equal each alone."""
    session = (
        ReparkSession.builder.appName(f"pytest-ta-series-mix-{partitions}")
        .config("spark.sql.shuffle.partitions", partitions)
        .getOrCreate()
    )
    frame = _read(session, tmp_path, _bars()).sort(TS)
    per_symbol = Window.partitionBy("contract_symbol").orderBy(TS)
    series = {"s_ema": ta.ema("close", timeperiod=5), "s_rsi": ta.rsi("close", timeperiod=7)}
    grouped = {
        "p_ema": ta.ema("close", timeperiod=5).over(per_symbol),
        "p_rsi": ta.rsi("close", timeperiod=7).over(per_symbol),
    }
    mixed = frame.withColumns({**series, **grouped})
    plan = _physical_plan_text(mixed)
    operators = [
        line.strip()
        for line in plan.splitlines()
        if line.strip().startswith(("WindowAggExec", "ParallelWindowExec"))
    ]
    assert len(operators) == 2, plan
    for line in operators:
        if "PARTITION BY" in line:
            assert line.startswith("WindowAggExec"), plan
        else:
            assert line.startswith("ParallelWindowExec"), plan
    got = _arrow(mixed)
    alone_series = _arrow(frame.withColumns(series))
    alone_grouped = _arrow(frame.withColumns(grouped))
    for name in series:
        _assert_bit_equal(got.select([name]), alone_series.select([name]))
    for name in grouped:
        _assert_bit_equal(got.select([name]), alone_grouped.select([name]))
