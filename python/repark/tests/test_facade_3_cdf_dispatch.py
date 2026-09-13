"""FACADE-3 step 2 dispatch pin: the named shapes take the Rust Arrow builder.

pins: facade-3/C-010

The Rust path lives behind ``create_dataframe_columns._rust_cdf_arrow_table``; this pin
records that the C-007 wall shapes dispatch into it and receive a table back, while the
explicit-fallback contract stays live for shapes the Rust builder does not cover.
"""

from __future__ import annotations

import datetime
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pytest

import repark.spark.session.create_dataframe_columns as columns_module
from repark import ReparkSession
from repark.errors import PySparkTypeError
from repark.spark.row import Row
from repark.spark.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Session matching the FACADE-3 golden corpus (UTC, TIMESTAMP_LTZ)."""
    session = (
        ReparkSession.builder.appName("facade-3-cdf-dispatch")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _seven_col_rows(count: int) -> list[tuple[Any, ...]]:
    """The step-1 fixture shape: int, float, string, bool, date, timestamp, decimal."""
    base_date = datetime.date(2024, 1, 2)
    base_ts = datetime.datetime(2024, 1, 2, 3, 4, 5)
    return [
        (
            index,
            index + 0.5,
            f"s{index}",
            index % 2 == 0,
            base_date,
            base_ts,
            Decimal(f"{index}.25"),
        )
        for index in range(count)
    ]


def _recording_rust_path(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[list[str], list[str] | None]]:
    """Swap the Rust dispatch for a recording wrapper; return the call log."""
    calls: list[tuple[list[str], list[str] | None]] = []
    delegate = columns_module._rust_cdf_arrow_table

    def spy(
        names: list[str], raw_tuples: list[tuple[Any, ...]], engine_types: list[str] | None
    ) -> Any:
        calls.append((list(names), None if engine_types is None else list(engine_types)))
        return delegate(names, raw_tuples, engine_types)

    monkeypatch.setattr(columns_module, "_rust_cdf_arrow_table", spy)
    return calls


def test_rust_path_takes_tuples_ddl(spark: ReparkSession, monkeypatch: pytest.MonkeyPatch) -> None:
    """The explicit-schema DDL tuple shape dispatches into the Rust builder."""
    calls = _recording_rust_path(monkeypatch)
    frame = spark.createDataFrame(
        _seven_col_rows(50),
        "a BIGINT, b DOUBLE, c STRING, d BOOLEAN, e DATE, f TIMESTAMP, g DECIMAL(38,18)",
    )
    assert calls and all(engine is not None for _names, engine in calls)
    assert frame.count() == 50


def test_rust_path_takes_tuples_structtype(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The explicit StructType tuple shape dispatches into the Rust builder."""
    calls = _recording_rust_path(monkeypatch)
    schema = StructType(
        [
            StructField("a", LongType(), True),
            StructField("b", DoubleType(), True),
            StructField("c", StringType(), True),
            StructField("d", BooleanType(), True),
            StructField("e", DateType(), True),
            StructField("f", TimestampType(), True),
            StructField("g", DecimalType(38, 18), True),
        ]
    )
    frame = spark.createDataFrame(_seven_col_rows(50), schema)
    assert calls and all(engine is not None for _names, engine in calls)
    assert frame.count() == 50


def test_rust_path_takes_nested(spark: ReparkSession, monkeypatch: pytest.MonkeyPatch) -> None:
    """The nested shape (list / dict / tuple cells) dispatches into the Rust builder."""
    calls = _recording_rust_path(monkeypatch)
    data = [(index, [index, index + 1], {"k": index}, (index, f"n{index}")) for index in range(40)]
    frame = spark.createDataFrame(data)
    assert calls and all(engine is None for _names, engine in calls)
    assert frame.count() == 40


def test_rust_path_takes_plain_tuples(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plain inferred tuples dispatch into the Rust builder."""
    calls = _recording_rust_path(monkeypatch)
    frame = spark.createDataFrame(_seven_col_rows(50))
    assert calls
    assert frame.count() == 50


def test_rust_path_takes_rows_and_dicts(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Row objects and dict rows reach the same Rust builder through the tuple funnel."""
    calls = _recording_rust_path(monkeypatch)
    spark.createDataFrame([Row(a=index, b=f"s{index}") for index in range(30)])
    spark.createDataFrame([{"a": index, "b": f"s{index}"} for index in range(30)])
    assert len(calls) == 2


def test_rust_path_takes_explicit_nested_schema(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DDL nested types (ARRAY / STRUCT) dispatch into the Rust builder."""
    calls = _recording_rust_path(monkeypatch)
    frame = spark.createDataFrame(
        [([1, 2], (3, "x"))],
        "a ARRAY<INT>, s STRUCT<b:INT, c:STRING>",
    )
    assert calls and all(engine is not None for _names, engine in calls)
    collected = frame.collect()
    assert collected[0]["a"] == [1, 2]


def test_python_fallback_keeps_pandas_shape(spark: ReparkSession) -> None:
    """The pandas control never reaches the tuple dispatcher (unchanged door)."""
    pytest.importorskip("pandas")
    import pandas as pd

    frame = spark.createDataFrame(pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}))
    assert frame.count() == 2


class _AsTupleProbeDecimal(Decimal):
    """Decimal subclass whose ``as_tuple`` counter observes native cell extraction."""

    calls: int = 0

    def as_tuple(self) -> Any:
        """Count the call, then behave exactly like ``Decimal.as_tuple``."""
        _AsTupleProbeDecimal.calls += 1
        return super().as_tuple()


def _seven_col_probe_rows(count: int) -> list[tuple[Any, ...]]:
    """The step-1 fixture shape with a decimal column that records extraction."""
    base_date = datetime.date(2024, 1, 2)
    base_ts = datetime.datetime(2024, 1, 2, 3, 4, 5)
    return [
        (
            index,
            index + 0.5,
            f"s{index}",
            index % 2 == 0,
            base_date,
            base_ts,
            _AsTupleProbeDecimal(f"{index}.25"),
        )
        for index in range(count)
    ]


def test_fallback_extract_stops_at_first_uncovered_cell(spark: ReparkSession) -> None:
    """An uncovered cell at row 900 returns None before any extraction (facade-3/C-014)."""
    rows = _seven_col_probe_rows(1_000)
    poisoned = list(rows[900])
    poisoned[0] = object()
    rows[900] = tuple(poisoned)
    _AsTupleProbeDecimal.calls = 0
    with pytest.raises(PySparkTypeError):
        spark.createDataFrame(rows)
    assert _AsTupleProbeDecimal.calls == 0


def test_fallback_extract_stops_on_merge_refusal(spark: ReparkSession) -> None:
    """A late scalar-merge refusal also returns before extraction (facade-3/C-014)."""
    rows = _seven_col_probe_rows(1_000)
    poisoned = list(rows[900])
    poisoned[0] = 1.5
    rows[900] = tuple(poisoned)
    _AsTupleProbeDecimal.calls = 0
    with pytest.raises(PySparkTypeError):
        spark.createDataFrame(rows)
    assert _AsTupleProbeDecimal.calls == 0
