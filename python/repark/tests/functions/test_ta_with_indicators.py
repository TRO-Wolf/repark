"""TA-2 — ``ta.with_indicators`` serving helper (required partition/order).

Pins the helper against a hand-built ``over_columns`` window on the Arrow path
(value AND type).
"""

from __future__ import annotations

import contextlib
import inspect
import io
import re
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from repark import ReparkSession, Window, ta
from repark.errors import PySparkTypeError
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

GOLDENS = Path(__file__).resolve().parents[3] / "crates" / "repark-ta" / "tests" / "goldens"
PREFIX_RUNS = {"high": 3, "low": 7, "close": 5}


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-ta-with-indicators").getOrCreate()


def _physical_plan_text(df: object) -> str:
    """Capture ``DataFrame.explain()`` physical plan body (N2 mechanic)."""
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
    return match.group(1).replace("\\n", "\n").replace("\\'", "'")


def _assert_bit_exact(left: np.ndarray, right: np.ndarray) -> None:
    """Strict ``to_bits`` equality, NaN ↔ NaN allowed (any payload)."""
    left = np.ascontiguousarray(left, dtype=np.float64)
    right = np.ascontiguousarray(right, dtype=np.float64)
    assert left.shape == right.shape, f"length {left.shape} vs {right.shape}"
    both_nan = np.isnan(left) & np.isnan(right)
    mismatch = (left.view(np.uint64) != right.view(np.uint64)) & ~both_nan
    if mismatch.any():
        first = int(np.flatnonzero(mismatch)[0])
        raise AssertionError(f"bit mismatch at row {first}: {left[first]!r} vs {right[first]!r}")


def _two_symbol_bars(spark: ReparkSession, *, bars_per_symbol: int = 20) -> object:
    """Two symbols, **same timestamps** — missing partition would mix the series.

    Charter fixture: AAA and BBB share ``ts`` 0..N-1 with different closes. A global
    ``orderBy("ts")`` (no ``partitionBy``) folds both instruments into one series —
    the silent cross-symbol RSI footgun this helper exists to prevent.
    """
    rows: list[tuple[str, int, float]] = []
    for index in range(bars_per_symbol):
        rows.append(("AAA", index, 100.0 + float(index)))
        rows.append(("BBB", index, 200.0 - float(index)))
    return spark.createDataFrame(rows, ["symbol", "ts", "close"])


def _indicator_map() -> dict[str, object]:
    return {
        "ema5": ta.ema("close", timeperiod=5),
        "sma10": ta.sma("close", timeperiod=10),
        "rsi14": ta.rsi("close", timeperiod=14),
        "mom10": ta.mom("close", timeperiod=10),
    }


def test_with_indicators_is_exported() -> None:
    """Helper is a public ``ta`` name (``__all__`` + attribute)."""
    assert "with_indicators" in ta.__all__
    assert callable(ta.with_indicators)


def test_with_indicators_partition_and_order_are_required_keyword_only() -> None:
    """No defaults that guess ``symbol`` / ``ts`` — omit ⇒ TypeError (S1 footgun)."""
    signature = inspect.signature(ta.with_indicators)
    for name in ("partition", "order"):
        parameter = signature.parameters[name]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY, name
        assert parameter.default is inspect.Parameter.empty, name
    columns = {"rsi14": ta.rsi("close", timeperiod=14)}
    frame = object()
    with pytest.raises(TypeError, match="partition"):
        ta.with_indicators(frame, order="ts", columns=columns)  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="order"):
        ta.with_indicators(frame, partition="symbol", columns=columns)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        ta.with_indicators(frame, "symbol", "ts", columns)  # type: ignore[misc]


def test_with_indicators_refuses_empty_or_bad_partition(spark: ReparkSession) -> None:
    """Empty / whitespace / wrong-typed partition is the same footgun as omitting it."""
    frame = _two_symbol_bars(spark, bars_per_symbol=5)
    columns = {"ema5": ta.ema("close", timeperiod=5)}
    with pytest.raises(PySparkTypeError, match="partition"):
        ta.with_indicators(frame, partition=[], order="ts", columns=columns)
    with pytest.raises(PySparkTypeError, match="partition"):
        ta.with_indicators(frame, partition="  ", order="ts", columns=columns)
    with pytest.raises(PySparkTypeError, match="order"):
        ta.with_indicators(frame, partition="symbol", order=[], columns=columns)
    with pytest.raises(PySparkTypeError, match="columns"):
        ta.with_indicators(
            frame,
            partition="symbol",
            order="ts",
            columns=[("ema5", ta.ema("close", timeperiod=5))],  # type: ignore[arg-type]
        )
    with pytest.raises(PySparkTypeError, match="DataFrame"):
        ta.with_indicators(
            "not-a-frame",
            partition="symbol",
            order="ts",
            columns=columns,
        )
    with pytest.raises(PySparkTypeError, match="lookback"):
        ta.with_indicators(
            frame,
            partition="symbol",
            order="ts",
            columns={"close_copy": F.col("close")},
            null_lookback=True,
        )


def test_with_indicators_matches_hand_built_over_columns_arrow_value_and_type(
    spark: ReparkSession,
) -> None:
    """Helper ≡ ``withColumns(over_columns(partitionBy.orderBy, …))`` on Arrow value+type."""
    frame = _two_symbol_bars(spark)
    columns = _indicator_map()
    got = ta.with_indicators(
        frame,
        partition="symbol",
        order="ts",
        columns=columns,  # type: ignore[arg-type]
    )
    window = Window.partitionBy("symbol").orderBy("ts")
    hand = frame.withColumns(ta.over_columns(window, columns))  # type: ignore[arg-type]
    got_table = got.to_arrow().sort_by([("symbol", "ascending"), ("ts", "ascending")])
    hand_table = hand.to_arrow().sort_by([("symbol", "ascending"), ("ts", "ascending")])
    assert got_table.num_rows == hand_table.num_rows
    for name in columns:
        assert got_table.schema.field(name).type == hand_table.schema.field(name).type
        assert pa.types.is_floating(got_table.schema.field(name).type)
        _assert_bit_exact(
            got_table.column(name).to_numpy(zero_copy_only=False),
            hand_table.column(name).to_numpy(zero_copy_only=False),
        )


def test_with_indicators_list_partition_and_order_match_str_form(
    spark: ReparkSession,
) -> None:
    """``partition=["symbol"]`` / ``order=["ts"]`` match the scalar-string spelling."""
    frame = _two_symbol_bars(spark)
    columns = {"rsi14": ta.rsi("close", timeperiod=14)}
    as_str = ta.with_indicators(frame, partition="symbol", order="ts", columns=columns)
    as_list = ta.with_indicators(frame, partition=["symbol"], order=["ts"], columns=columns)
    left = as_str.to_arrow().sort_by([("symbol", "ascending"), ("ts", "ascending")])
    right = as_list.to_arrow().sort_by([("symbol", "ascending"), ("ts", "ascending")])
    assert left.schema.field("rsi14").type == right.schema.field("rsi14").type
    _assert_bit_exact(
        left.column("rsi14").to_numpy(zero_copy_only=False),
        right.column("rsi14").to_numpy(zero_copy_only=False),
    )


def test_cross_symbol_rsi_without_partition_leaks_helper_does_not(
    spark: ReparkSession,
) -> None:
    """Same timestamps, two symbols: missing partition leaks RSI; the helper cannot.

    Hand-built ``Window.orderBy("ts")`` (no ``partitionBy``) mixes AAA and BBB into one
    series. The helper requires ``partition`` so ETL cannot take that path by accident.
    """
    frame = _two_symbol_bars(spark, bars_per_symbol=20)
    columns = {"rsi14": ta.rsi("close", timeperiod=14)}
    guarded = ta.with_indicators(frame, partition="symbol", order="ts", columns=columns)
    leaked = frame.withColumns(ta.over_columns(Window.orderBy("ts"), columns))
    guarded_table = guarded.to_arrow().sort_by([("symbol", "ascending"), ("ts", "ascending")])
    leaked_table = leaked.to_arrow().sort_by([("symbol", "ascending"), ("ts", "ascending")])
    guarded_rsi = guarded_table.column("rsi14").to_numpy(zero_copy_only=False)
    leaked_rsi = leaked_table.column("rsi14").to_numpy(zero_copy_only=False)
    both_nan = np.isnan(guarded_rsi) & np.isnan(leaked_rsi)
    differ = (guarded_rsi.view(np.uint64) != leaked_rsi.view(np.uint64)) & ~both_nan
    assert differ.any(), (
        "expected unpartitioned RSI to leak across AAA/BBB — fixture is vacuous if they match"
    )
    # Per-symbol AAA-only RSI matches the helper (partition did its job).
    aaa_only = frame.filter(F.col("symbol") == F.lit("AAA"))
    aaa_hand = aaa_only.withColumns(ta.over_columns(Window.orderBy("ts"), columns))
    aaa_from_helper = guarded.filter(F.col("symbol") == F.lit("AAA"))
    helper_aaa = (
        aaa_from_helper.to_arrow().sort_by("ts").column("rsi14").to_numpy(zero_copy_only=False)
    )
    hand_aaa = aaa_hand.to_arrow().sort_by("ts").column("rsi14").to_numpy(zero_copy_only=False)
    _assert_bit_exact(helper_aaa, hand_aaa)
    assert pa.types.is_floating(guarded_table.schema.field("rsi14").type)


def test_last_row_row_count_and_values(spark: ReparkSession) -> None:
    """``last_row=True`` collects one row per partition; values match the last TA bar."""
    frame = _two_symbol_bars(spark, bars_per_symbol=16)
    columns = {
        "ema5": ta.ema("close", timeperiod=5),
        "rsi14": ta.rsi("close", timeperiod=14),
    }
    full = ta.with_indicators(frame, partition="symbol", order="ts", columns=columns)
    last = ta.with_indicators(frame, partition="symbol", order="ts", columns=columns, last_row=True)
    last_plan = _physical_plan_text(last)
    # Fused TA + row_number, then partition max — two WindowAggExec, not a guess.
    assert last_plan.count("WindowAggExec") == 2, last_plan[:2000]
    assert "ta_ema" in last_plan and "ta_rsi" in last_plan, last_plan[:2000]
    last_table = last.to_arrow().sort_by("symbol")
    assert last_table.num_rows == 2, last_table.num_rows
    assert "__repark_ta_last_row" not in last_table.column_names
    assert "__repark_ta_last_row_max" not in last_table.column_names
    full_table = full.to_arrow().sort_by([("symbol", "ascending"), ("ts", "ascending")])
    for name in columns:
        assert last_table.schema.field(name).type == full_table.schema.field(name).type
        assert pa.types.is_floating(last_table.schema.field(name).type)
    # Last bar per symbol in the full window.
    symbols = full_table.column("symbol").to_pylist()
    expected_last: dict[str, int] = {}
    for row_index, symbol in enumerate(symbols):
        expected_last[symbol] = row_index
    got_symbols = last_table.column("symbol").to_pylist()
    assert sorted(got_symbols) == ["AAA", "BBB"]
    for name in columns:
        full_values = full_table.column(name).to_numpy(zero_copy_only=False)
        last_values = last_table.column(name).to_numpy(zero_copy_only=False)
        last_by_symbol = {symbol: last_values[index] for index, symbol in enumerate(got_symbols)}
        for symbol, full_index in expected_last.items():
            left = np.asarray([last_by_symbol[symbol]], dtype=np.float64)
            right = np.asarray([full_values[full_index]], dtype=np.float64)
            _assert_bit_exact(left, right)


def test_with_indicators_plan_is_one_window_agg_exec(spark: ReparkSession) -> None:
    """Same-spec multi-TA via the helper → one ``WindowAggExec`` (N2 mechanic).

    Function-name tokens must appear so DCE cannot fake a fused count of 1 by
    dropping unused window outputs (TA-1 lesson).
    """
    frame = _two_symbol_bars(spark)
    columns = _indicator_map()
    fused = ta.with_indicators(
        frame,
        partition="symbol",
        order="ts",
        columns=columns,  # type: ignore[arg-type]
    )
    plan = _physical_plan_text(fused)
    assert plan.count("WindowAggExec") == 1, plan[:2000]
    for token in ("ta_ema", "ta_sma", "ta_rsi", "ta_mom"):
        assert token in plan, plan[:2000]


def test_null_lookback_threads_through_existing_helper(spark: ReparkSession) -> None:
    """Helper ``null_lookback=True`` uses ``_NullLookbackColumn`` — prefix is SQL NULL."""
    frame = _two_symbol_bars(spark, bars_per_symbol=24)
    lookback = 4  # ema timeperiod=5
    got = ta.with_indicators(
        frame,
        partition="symbol",
        order="ts",
        columns={"ema5": ta.ema("close", timeperiod=5)},
        null_lookback=True,
    )
    hand = frame.withColumns(
        ta.over_columns(
            Window.partitionBy("symbol").orderBy("ts"),
            {"ema5": ta.ema("close", timeperiod=5, null_lookback=True)},
        )
    )
    got_table = got.to_arrow().sort_by([("symbol", "ascending"), ("ts", "ascending")])
    hand_table = hand.to_arrow().sort_by([("symbol", "ascending"), ("ts", "ascending")])
    assert got_table.schema.field("ema5").type == hand_table.schema.field("ema5").type
    got_col = got_table.column("ema5")
    hand_col = hand_table.column("ema5")
    assert got_col.null_count == hand_col.null_count
    assert got_col.null_count > 0
    # Per-symbol prefix is NULL (lookback rows each).
    symbols = got_table.column("symbol").to_pylist()
    prefix_seen = {"AAA": 0, "BBB": 0}
    for row_index, symbol in enumerate(symbols):
        if prefix_seen[symbol] < lookback:
            assert not got_col[row_index].is_valid, (symbol, row_index)
            prefix_seen[symbol] += 1
    _assert_bit_exact(
        got_col.to_numpy(zero_copy_only=False),
        hand_col.to_numpy(zero_copy_only=False),
    )


def test_last_row_with_null_lookback_keeps_last_bar_values(
    spark: ReparkSession,
) -> None:
    """``last_row`` + ``null_lookback``: last bar is past the prefix and matches the full window."""
    frame = _two_symbol_bars(spark, bars_per_symbol=16)
    columns = {"ema5": ta.ema("close", timeperiod=5)}
    full = ta.with_indicators(
        frame, partition="symbol", order="ts", columns=columns, null_lookback=True
    )
    last = ta.with_indicators(
        frame,
        partition="symbol",
        order="ts",
        columns=columns,
        null_lookback=True,
        last_row=True,
    )
    last_table = last.to_arrow().sort_by("symbol")
    full_table = full.to_arrow().sort_by([("symbol", "ascending"), ("ts", "ascending")])
    assert last_table.num_rows == 2
    assert last_table.schema.field("ema5").type == full_table.schema.field("ema5").type
    last_col = last_table.column("ema5")
    assert last_col.null_count == 0
    symbols = full_table.column("symbol").to_pylist()
    expected_last: dict[str, int] = {}
    for row_index, symbol in enumerate(symbols):
        expected_last[symbol] = row_index
    got_symbols = last_table.column("symbol").to_pylist()
    full_values = full_table.column("ema5").to_numpy(zero_copy_only=False)
    last_values = last_col.to_numpy(zero_copy_only=False)
    last_by_symbol = {symbol: last_values[index] for index, symbol in enumerate(got_symbols)}
    for symbol, full_index in expected_last.items():
        _assert_bit_exact(
            np.asarray([last_by_symbol[symbol]], dtype=np.float64),
            np.asarray([full_values[full_index]], dtype=np.float64),
        )


def _golden(path: Path) -> np.ndarray:
    """Read one recorded golden as little-endian ``float64``."""
    return np.frombuffer(path.read_bytes(), dtype="<f8")


def _prefix_bars(spark: ReparkSession) -> object:
    """The TA-CHAIN-1 prefix fixture: walk H/L/C with leading NaN runs of 3, 7 and 5 rows."""
    series = {}
    for name, run in PREFIX_RUNS.items():
        values = _golden(GOLDENS / f"fixture_{name}.bin").copy()
        values[:run] = np.nan
        series[name] = values
    rows = [
        ("TEST", index, float(high), float(low), float(close))
        for index, (high, low, close) in enumerate(
            zip(series["high"], series["low"], series["close"], strict=True)
        )
    ]
    return spark.createDataFrame(rows, ["symbol", "ts", "high", "low", "close"])


def test_chained_indicator_over_trange_matches_polars_talib_golden(
    spark: ReparkSession,
) -> None:
    """``ema(trange(h, l, c), 21)`` answers where polars_talib answers, bit-exact (TA-CHAIN-1)."""
    with_tr = ta.with_indicators(
        _prefix_bars(spark),
        partition="symbol",
        order="ts",
        columns={"tr": ta.trange("high", "low", "close")},
    )
    chained = ta.with_indicators(
        with_tr,
        partition="symbol",
        order="ts",
        columns={"ema21_of_tr": ta.ema("tr", timeperiod=21)},
    )
    table = chained.to_arrow().sort_by("ts")
    tr = table.column("tr").to_numpy(zero_copy_only=False)
    got = table.column("ema21_of_tr").to_numpy(zero_copy_only=False)
    _assert_bit_exact(tr, _golden(GOLDENS / "prefix" / "prefix_trange.bin"))
    _assert_bit_exact(got, _golden(GOLDENS / "prefix" / "prefix_chain_ema21_of_trange.bin"))
    assert int(np.flatnonzero(~np.isnan(got))[0]) == 28
    assert np.isfinite(got[28:]).all()


def test_leading_null_prefix_is_skipped_per_partition(spark: ReparkSession) -> None:
    """A leading NULL run is skipped per partition; the other partitions start at the lookback."""
    bars_per_symbol = 40
    lookback = 4
    null_runs = {"S03": 3, "S09": 6}
    symbols = [f"S{index:02d}" for index in range(14)]
    closes = {
        symbol: [
            100.0 + float(offset) + float((index * (offset + 3)) % 11)
            for index in range(bars_per_symbol)
        ]
        for offset, symbol in enumerate(symbols)
    }
    rows = [
        (symbol, index, None if index < null_runs.get(symbol, 0) else close)
        for symbol in symbols
        for index, close in enumerate(closes[symbol])
    ]
    frame = spark.createDataFrame(rows, ["symbol", "ts", "close"])
    table = (
        ta.with_indicators(
            frame,
            partition="symbol",
            order="ts",
            columns={"ema5": ta.ema("close", timeperiod=5)},
        )
        .to_arrow()
        .sort_by([("symbol", "ascending"), ("ts", "ascending")])
    )
    assert table.column("ema5").null_count == 0
    got_symbols = table.column("symbol").to_pylist()
    values = table.column("ema5").to_numpy(zero_copy_only=False)
    for symbol in symbols:
        mask = np.asarray([name == symbol for name in got_symbols])
        got = values[mask]
        run = null_runs.get(symbol, 0)
        assert got.size == bars_per_symbol
        assert int(np.flatnonzero(~np.isnan(got))[0]) == run + lookback, symbol
        assert np.isfinite(got[run + lookback :]).all(), symbol
        trimmed = spark.createDataFrame(
            [(symbol, index, close) for index, close in enumerate(closes[symbol][run:])],
            ["symbol", "ts", "close"],
        )
        clean = (
            ta.with_indicators(
                trimmed,
                partition="symbol",
                order="ts",
                columns={"ema5": ta.ema("close", timeperiod=5)},
            )
            .to_arrow()
            .sort_by("ts")
        )
        _assert_bit_exact(got[run:], clean.column("ema5").to_numpy(zero_copy_only=False))
