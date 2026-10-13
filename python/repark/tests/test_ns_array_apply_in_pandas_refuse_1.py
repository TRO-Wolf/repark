"""NS-ARRAY-APPLY-IN-PANDAS-REFUSE-1: refuse the nanosecond array applyInPandas stores wrong.

On main, grouped and cogrouped ``applyInPandas`` store an object column of numpy
``datetime64[ns]`` arrays into ``list<timestamp[us]>`` without converting the unit:
``1767323045123456789`` reads back as microseconds where ``1767323045123456`` is
right. The interim fix refuses that shape with a hint. This module pins the
refusal and the controls that keep main's answers.

pins: ns-array-apply-in-pandas-refuse-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import _ice_tsns_merge_wall_1_doors as doors
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import repark.functions as F

from repark.errors import PySparkException, PySparkTypeError

EPOCH = datetime(1970, 1, 1)
US_FIRST = 1767323045123456
NS_ARRAYS: np.ndarray = np.array(
    ["2026-01-02T03:04:05.123456789", "1969-12-31T23:59:59.999999999"],
    dtype="datetime64[ns]",
)
EXPECTED_US: list[int] = [
    value // 1000 if value >= 0 else -(-value // 1000) for value in doors.NANOS
]
EXPECTED_US_FLOOR: list[int] = [value // 1000 for value in doors.NANOS]


@contextlib.contextmanager
def _source_session(tmp_path: Path) -> Iterator[Any]:
    """Yield a fresh UTC session holding the shared nanosecond source table."""
    session = doors.open_session("UTC", tmp_path)
    try:
        yield session
    finally:
        session.stop()


def _sorted_ticks(table: pa.Table, name: str) -> list[int]:
    """Return the sorted int64 ticks of a possibly nested timestamp list column."""
    flat = table.column(name).combine_chunks()
    while pa.types.is_list(flat.type) or pa.types.is_large_list(flat.type):
        flat = flat.flatten()
    return sorted(flat.cast(pa.int64()).to_pylist())


def _assert_ns_array_refusal(error: PySparkTypeError, column: str) -> None:
    """Assert the refusal names the column, the unit and the microsecond hint."""
    assert type(error) is PySparkTypeError
    text = str(error)
    assert f"'{column}'" in text
    assert "datetime64[ns]" in text
    assert "timestamp[us" in text
    assert '.astype("datetime64[us]")' in text


def _identity(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return the group frame unchanged."""
    return pdf


def _return_left(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    """Return the left cogroup frame unchanged."""
    return left


def _ns_array_row(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return one row per group holding the shared nanosecond datetime array."""
    key = pdf["id"].iloc[0]
    return pd.DataFrame({"id": [key], "v": [NS_ARRAYS]})


def _ns_array_row_from_left(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    """Return one row per cogroup holding the shared nanosecond datetime array."""
    key = left["id"].iloc[0]
    return pd.DataFrame({"id": [key], "v": [NS_ARRAYS]})


def _cast_arrays_to_microseconds(pdf: pd.DataFrame) -> pd.DataFrame:
    """Cast every datetime64 array cell of the frame to microseconds."""
    result = pdf.copy()
    for column in result.columns:
        if result[column].dtype != object:
            continue
        cells: list[Any] = []
        for cell in result[column]:
            if isinstance(cell, np.ndarray) and cell.dtype.kind == "M":
                cells.append(cell.astype("datetime64[us]"))
            else:
                cells.append(cell)
        result[column] = cells
    return result


def test_grouped_plain_ns_array_refuses(tmp_path: Path) -> None:
    """Grouped applyInPandas over a plain array(ns) column refuses with the hint."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select("id", F.array(F.col("ns")).alias("v"))
        with pytest.raises(PySparkTypeError) as caught:
            frame.groupBy("id").applyInPandas(_identity, frame.schema).toArrow()
        _assert_ns_array_refusal(caught.value, "v")


def test_grouped_plain_tzns_array_refuses(tmp_path: Path) -> None:
    """Grouped applyInPandas over a plain array(timestamptz_ns) column refuses."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select("id", F.array(F.col("tzns")).alias("v"))
        with pytest.raises(PySparkTypeError) as caught:
            frame.groupBy("id").applyInPandas(_identity, frame.schema).toArrow()
        _assert_ns_array_refusal(caught.value, "v")


def test_cogrouped_plain_ns_array_refuses(tmp_path: Path) -> None:
    """Cogrouped applyInPandas over a plain array(ns) column refuses with the hint."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select("id", F.array(F.col("ns")).alias("v"))
        with pytest.raises(PySparkTypeError) as caught:
            (
                frame.groupBy("id")
                .cogroup(frame.groupBy("id"))
                .applyInPandas(_return_left, frame.schema)
                .toArrow()
            )
        _assert_ns_array_refusal(caught.value, "v")


def test_cogrouped_plain_tzns_array_refuses(tmp_path: Path) -> None:
    """Cogrouped applyInPandas over a plain array(timestamptz_ns) column refuses."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select("id", F.array(F.col("tzns")).alias("v"))
        with pytest.raises(PySparkTypeError) as caught:
            (
                frame.groupBy("id")
                .cogroup(frame.groupBy("id"))
                .applyInPandas(_return_left, frame.schema)
                .toArrow()
            )
        _assert_ns_array_refusal(caught.value, "v")


def test_grouped_user_ns_object_column_refuses(tmp_path: Path) -> None:
    """Grouped applyInPandas over a user-built nanosecond object column refuses."""
    with _source_session(tmp_path) as spark:
        base = spark.createDataFrame(pd.DataFrame({"id": [1, 2]}), "id int")
        with pytest.raises(PySparkTypeError) as caught:
            (
                base.groupBy("id")
                .applyInPandas(_ns_array_row, "id int, v array<timestamp_ntz>")
                .toArrow()
            )
        _assert_ns_array_refusal(caught.value, "v")


def test_cogrouped_user_ns_object_column_refuses(tmp_path: Path) -> None:
    """Cogrouped applyInPandas over a user-built nanosecond object column refuses."""
    with _source_session(tmp_path) as spark:
        base = spark.createDataFrame(pd.DataFrame({"id": [1, 2]}), "id int")
        grouped = base.groupBy("id")
        with pytest.raises(PySparkTypeError) as caught:
            (
                grouped.cogroup(grouped)
                .applyInPandas(_ns_array_row_from_left, "id int, v array<timestamp_ntz>")
                .toArrow()
            )
        _assert_ns_array_refusal(caught.value, "v")


def test_grouped_nested_ns_array_refuses(tmp_path: Path) -> None:
    """Grouped applyInPandas over array(array(ns)) refuses at depth two."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select(
            "id", F.array(F.array(F.col("ns"))).alias("v")
        )
        with pytest.raises(PySparkTypeError) as caught:
            frame.groupBy("id").applyInPandas(_identity, frame.schema).toArrow()
        _assert_ns_array_refusal(caught.value, "v")


def test_grouped_struct_holding_ns_array_refuses(tmp_path: Path) -> None:
    """Grouped applyInPandas over a struct holding a nanosecond array refuses."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select(
            "id", F.struct(F.array(F.col("ns")).alias("a")).alias("v")
        )
        with pytest.raises(PySparkTypeError) as caught:
            frame.groupBy("id").applyInPandas(_identity, frame.schema).toArrow()
        _assert_ns_array_refusal(caught.value, "v")


def test_grouped_microsecond_array_answers(tmp_path: Path) -> None:
    """Grouped applyInPandas over a microsecond array answers with the same ticks."""
    with _source_session(tmp_path) as spark:
        frame = spark.sql(
            "SELECT 1 AS id, array(TIMESTAMP '2026-01-02 03:04:05.123456+00:00', "
            "TIMESTAMP '1970-01-01 00:00:00+00:00') AS v"
        )
        result = frame.groupBy("id").applyInPandas(_identity, frame.schema).toArrow()
        assert str(result.schema.field("v").type) == "list<item: timestamp[us, tz=UTC]>"
        assert _sorted_ticks(result, "v") == [0, US_FIRST]


def test_grouped_hint_cast_answers(tmp_path: Path) -> None:
    """Casting the elements to microseconds in the function answers floored ticks."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select("id", F.array(F.col("ns")).alias("v"))
        result = (
            frame.groupBy("id")
            .applyInPandas(_cast_arrays_to_microseconds, frame.schema)
            .toArrow()
        )
        assert str(result.schema.field("v").type) == "list<item: timestamp[us]>"
        assert _sorted_ticks(result, "v") == sorted(EXPECTED_US_FLOOR)


def test_grouped_top_level_ns_column_keeps_refusal(tmp_path: Path) -> None:
    """Grouped applyInPandas over a top-level ns column keeps main's refusal."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select("id", F.col("ns").alias("v"))
        with pytest.raises(PySparkException) as caught:
            frame.groupBy("id").applyInPandas(_identity, frame.schema).toArrow()
        assert type(caught.value) is PySparkException
        assert "would lose data" in str(caught.value)


def test_grouped_empty_list_answers(tmp_path: Path) -> None:
    """Grouped applyInPandas over an empty list column answers empty lists."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select("id", F.array().alias("v"))
        result = (
            frame.groupBy("id")
            .applyInPandas(_identity, "id int, v array<timestamp_ntz>")
            .toArrow()
        )
        assert str(result.schema.field("v").type) == "list<item: timestamp[us]>"
        rows = result.to_pylist()
        assert len(rows) == len(doors.IDS)
        assert [row["v"] for row in rows] == [[] for _ in doors.IDS]


def test_grouped_list_of_nulls_answers(tmp_path: Path) -> None:
    """Grouped applyInPandas over a list-of-nulls column answers the nulls."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select("id", F.array(F.lit(None)).alias("v"))
        result = (
            frame.groupBy("id")
            .applyInPandas(_identity, "id int, v array<timestamp_ntz>")
            .toArrow()
        )
        assert str(result.schema.field("v").type) == "list<item: timestamp[us]>"
        rows = result.to_pylist()
        assert len(rows) == len(doors.IDS)
        assert [row["v"] for row in rows] == [[None] for _ in doors.IDS]


def test_grouped_nested_microsecond_array_answers(tmp_path: Path) -> None:
    """Grouped applyInPandas over array(array(ntz)) answers the floored ticks."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select(
            "id", F.array(F.array(F.col("ns").cast("timestamp_ntz"))).alias("v")
        )
        result = frame.groupBy("id").applyInPandas(_identity, frame.schema).toArrow()
        assert _sorted_ticks(result, "v") == sorted(EXPECTED_US)


def test_grouped_struct_holding_microsecond_array_answers(tmp_path: Path) -> None:
    """Grouped applyInPandas over a struct holding a microsecond array answers."""
    with _source_session(tmp_path) as spark:
        frame = spark.table(doors.SRC).select(
            "id",
            F.struct(F.array(F.col("ns").cast("timestamp_ntz")).alias("a")).alias("v"),
        )
        result = frame.groupBy("id").applyInPandas(_identity, frame.schema).toArrow()
        rows = {row["id"]: row["v"] for row in result.to_pylist()}
        assert sorted(rows) == list(doors.IDS)
        for position, identifier in enumerate(doors.IDS):
            stamp = EPOCH + timedelta(microseconds=EXPECTED_US[position])
            assert rows[identifier] == {"a": [stamp]}
