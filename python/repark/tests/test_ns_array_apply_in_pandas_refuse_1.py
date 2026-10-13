"""NS-ARRAY-APPLY-IN-PANDAS-REFUSE-1: refuse the nanosecond array applyInPandas stores wrong.

On main, grouped and cogrouped ``applyInPandas`` store an object column of numpy
``datetime64[ns]`` arrays into ``list<timestamp[us]>`` without converting the unit:
``1767323045123456789`` reads back as microseconds where ``1767323045123456`` is
right. The interim fix refuses that shape with a hint. This module pins the
refusal and the controls that keep main's answers.

Fold 1 narrows the refusal per the verifier: only a nanosecond array holding a real
value decides, the scan goes on past anything else, and ArrowDtype columns keep
main's answers byte-identical.

pins: ns-array-apply-in-pandas-refuse-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
pins: ns-array-apply-in-pandas-refuse-1/C-011, C-012, C-013, C-014, C-015
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any

import _ice_tsns_merge_wall_1_doors as doors
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from repark import functions as F  # noqa: N812 — PySpark idiom
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
USARR: np.ndarray = NS_ARRAYS.astype("datetime64[us]")
MSARR: np.ndarray = NS_ARRAYS.astype("datetime64[ms]")
NATS_US: np.ndarray = np.array(["NaT"], dtype="datetime64[us]")
NATS_NS: np.ndarray = np.array(["NaT", "NaT"], dtype="datetime64[ns]")
EMPTY_US: np.ndarray = np.array([], dtype="datetime64[us]")
EMPTY_NS: np.ndarray = np.array([], dtype="datetime64[ns]")
NTZ = "array<timestamp_ntz>"
LTZ = "array<timestamp>"


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
        frame = spark.table(doors.SRC).select("id", F.array(F.array(F.col("ns"))).alias("v"))
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
            frame.groupBy("id").applyInPandas(_cast_arrays_to_microseconds, frame.schema).toArrow()
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
            frame.groupBy("id").applyInPandas(_identity, "id int, v array<timestamp_ntz>").toArrow()
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
            frame.groupBy("id").applyInPandas(_identity, "id int, v array<timestamp_ntz>").toArrow()
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


def _object_series(cells: list[Any]) -> pd.Series:
    """Build a real object Series holding exactly the given cells."""
    series = np.empty(len(cells), dtype=object)
    for index, cell in enumerate(cells):
        series[index] = cell
    return pd.Series(series)


def _user_frame(pdf: pd.DataFrame, cells: list[Any]) -> pd.DataFrame:
    """Return one row per cell holding the given cells for the group key."""
    key = pdf["id"].iloc[0]
    return pd.DataFrame({"id": [key] * len(cells), "v": _object_series(cells)})


def _user_cogroup_frame(left: pd.DataFrame, right: pd.DataFrame, cells: list[Any]) -> pd.DataFrame:
    """Return one row per cell holding the given cells for the cogroup key."""
    return _user_frame(left, cells)


def _arrow_frame(pdf: pd.DataFrame, cells: list[Any], arrow_type: Any) -> pd.DataFrame:
    """Return one row per cell holding the given ArrowDtype cells for the group key."""
    key = pdf["id"].iloc[0]
    series = pd.Series(pa.array(cells, type=arrow_type), dtype=pd.ArrowDtype(arrow_type))
    return pd.DataFrame({"id": [key] * len(series), "v": series})


def _run_user_case(spark: Any, cells: list[Any], declared: str, cogrouped: bool) -> Any:
    """Run one user-built column case through the named door."""
    base = spark.createDataFrame(pd.DataFrame({"id": [1, 2]}), "id int")
    schema = f"id int, v {declared}"
    grouped = base.groupBy("id")
    if cogrouped:
        return (
            grouped.cogroup(grouped)
            .applyInPandas(partial(_user_cogroup_frame, cells=cells), schema)
            .toArrow()
        )
    return grouped.applyInPandas(partial(_user_frame, cells=cells), schema).toArrow()


def _assert_main_refusal(error: PySparkException, fragment: str) -> None:
    """Assert main's own refusal class and text stand unchanged."""
    assert type(error) is PySparkException
    assert fragment in str(error)


H1_COLUMNS: list[tuple[str, list[Any], str]] = [
    ("us_then_ns", [USARR, NS_ARRAYS], NTZ),
    ("us_then_ns_ltz", [USARR, NS_ARRAYS], LTZ),
    ("empty_us_then_ns", [EMPTY_US, NS_ARRAYS], NTZ),
    ("nat_us_then_ns", [NATS_US, NS_ARRAYS], NTZ),
    ("ms_then_ns", [MSARR, NS_ARRAYS], NTZ),
    ("null_then_ns", [None, NS_ARRAYS], NTZ),
    ("emptylist_then_ns", [[], NS_ARRAYS], NTZ),
]

H1_CELLS: list[tuple[str, list[Any], str]] = [
    ("nested_us_then_ns", [[USARR, NS_ARRAYS]], "array<array<timestamp_ntz>>"),
    ("struct_us_then_ns", [{"a": USARR, "b": NS_ARRAYS}], "struct<a:array<timestamp_ntz>,b:array<timestamp_ntz>>"),
    ("map_us_then_ns", [[("a", USARR), ("b", NS_ARRAYS)]], "map<string,array<timestamp_ntz>>"),
]


@pytest.mark.parametrize("cogrouped", [False, True], ids=["grouped", "cogrouped"])
@pytest.mark.parametrize("name,cells,declared", H1_COLUMNS, ids=[case[0] for case in H1_COLUMNS])
def test_h1_mixed_column_refuses(tmp_path: Path, name: str, cells: list[Any], declared: str, cogrouped: bool) -> None:
    """A non-nanosecond lead cell never stops the scan before a nanosecond array."""
    with _source_session(tmp_path) as spark:
        with pytest.raises(PySparkTypeError) as caught:
            _run_user_case(spark, cells, declared, cogrouped)
        _assert_ns_array_refusal(caught.value, "v")


@pytest.mark.parametrize("cogrouped", [False, True], ids=["grouped", "cogrouped"])
@pytest.mark.parametrize("name,cells,declared", H1_CELLS, ids=[case[0] for case in H1_CELLS])
def test_h1_mixed_cell_refuses(tmp_path: Path, name: str, cells: list[Any], declared: str, cogrouped: bool) -> None:
    """A non-nanosecond lead inside one nested cell never stops the scan either."""
    with _source_session(tmp_path) as spark:
        with pytest.raises(PySparkTypeError) as caught:
            _run_user_case(spark, cells, declared, cogrouped)
        _assert_ns_array_refusal(caught.value, "v")


def _keyed_frame(pdf: pd.DataFrame, order: str) -> pd.DataFrame:
    """Return the microsecond or nanosecond cell for one group key."""
    key = int(pdf["id"].iloc[0])
    if (key == 1) == (order == "us_first"):
        cells = [USARR]
    else:
        cells = [NS_ARRAYS]
    return pd.DataFrame({"id": [key], "v": _object_series(cells)})


def _keyed_cogroup_frame(left: pd.DataFrame, right: pd.DataFrame, order: str) -> pd.DataFrame:
    """Return the microsecond or nanosecond cell for one cogroup key."""
    return _keyed_frame(left, order)


@pytest.mark.parametrize("cogrouped", [False, True], ids=["grouped", "cogrouped"])
@pytest.mark.parametrize("order", ["us_first", "ns_first"], ids=["us_first", "ns_first"])
def test_h1_mixed_across_groups_refuses(tmp_path: Path, order: str, cogrouped: bool) -> None:
    """A nanosecond group refuses whatever the other group holds."""
    with _source_session(tmp_path) as spark:
        base = spark.createDataFrame(pd.DataFrame({"id": [1, 2]}), "id int")
        schema = f"id int, v {NTZ}"
        grouped = base.groupBy("id")
        with pytest.raises(PySparkTypeError) as caught:
            if cogrouped:
                grouped.cogroup(grouped).applyInPandas(
                    partial(_keyed_cogroup_frame, order=order), schema
                ).toArrow()
            else:
                grouped.applyInPandas(partial(_keyed_frame, order=order), schema).toArrow()
        _assert_ns_array_refusal(caught.value, "v")


@pytest.mark.parametrize("cogrouped", [False, True], ids=["grouped", "cogrouped"])
def test_h1_many_us_rows_then_ns_last_refuses(tmp_path: Path, cogrouped: bool) -> None:
    """Twenty thousand microsecond rows never stop the scan before the last row."""
    with _source_session(tmp_path) as spark:
        with pytest.raises(PySparkTypeError) as caught:
            _run_user_case(spark, [USARR] * 20000 + [NS_ARRAYS], NTZ, cogrouped)
        _assert_ns_array_refusal(caught.value, "v")


@pytest.mark.parametrize("cogrouped", [False, True], ids=["grouped", "cogrouped"])
def test_h1_many_null_rows_then_ns_last_refuses(tmp_path: Path, cogrouped: bool) -> None:
    """Twenty thousand null rows never stop the scan before the last row."""
    with _source_session(tmp_path) as spark:
        with pytest.raises(PySparkTypeError) as caught:
            _run_user_case(spark, [None] * 20000 + [NS_ARRAYS], NTZ, cogrouped)
        _assert_ns_array_refusal(caught.value, "v")


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id, filter(array(ns), x -> false) AS v FROM {src}",
        "SELECT id, slice(array(ns), 2, 1) AS v FROM {src}",
        "SELECT id, array(CASE WHEN id < 0 THEN ns END) AS v FROM {src}",
    ],
    ids=["filter_empty", "slice_empty", "null_element"],
)
def test_h3_table_empty_ns_arrays_answer(tmp_path: Path, query: str) -> None:
    """Empty and all-null nanosecond arrays from the engine answer as on main."""
    with _source_session(tmp_path) as spark:
        frame = spark.sql(query.format(src=doors.SRC))
        result = frame.groupBy("id").applyInPandas(_identity, frame.schema).toArrow()
        assert str(result.schema.field("v").type) == "list<item: timestamp[us]>"
        rows = result.to_pylist()
        assert len(rows) == len(doors.IDS)
        for row in rows:
            assert row["v"] == [] or row["v"] == [None]


@pytest.mark.parametrize("cogrouped", [False, True], ids=["grouped", "cogrouped"])
@pytest.mark.parametrize(
    "cells,expected",
    [([EMPTY_NS], []), ([NATS_NS], [None, None])],
    ids=["empty_ns", "all_nat_ns"],
)
def test_h3_user_ns_empty_answers(tmp_path: Path, cells: list[Any], expected: list[Any], cogrouped: bool) -> None:
    """User-built empty and all-NaT nanosecond arrays answer as on main."""
    with _source_session(tmp_path) as spark:
        result = _run_user_case(spark, cells, NTZ, cogrouped)
        assert str(result.schema.field("v").type) == "list<item: timestamp[us]>"
        rows = result.to_pylist()
        assert [row["v"] for row in rows] == [expected, expected]


@pytest.mark.parametrize("cogrouped", [False, True], ids=["grouped", "cogrouped"])
@pytest.mark.parametrize(
    "cells,arrow_type",
    [
        ([[1767323045123456000, 1767323045000000000]], pa.list_(pa.timestamp("ns"))),
        ([[]], pa.list_(pa.timestamp("ns"))),
        ([[None]], pa.list_(pa.timestamp("ns"))),
    ],
    ids=["whole", "empty", "nulls"],
)
def test_h3_arrowdtype_ns_lists_answer(tmp_path: Path, cells: list[Any], arrow_type: Any, cogrouped: bool) -> None:
    """ArrowDtype nanosecond lists keep main's safe conversion byte-identical."""
    with _source_session(tmp_path) as spark:
        base = spark.createDataFrame(pd.DataFrame({"id": [1, 2]}), "id int")
        schema = f"id int, v {NTZ}"
        grouped = base.groupBy("id")
        if cogrouped:
            result = (
                grouped.cogroup(grouped)
                .applyInPandas(partial(_arrow_frame, cells=cells, arrow_type=arrow_type), schema)
                .toArrow()
            )
        else:
            result = grouped.applyInPandas(
                partial(_arrow_frame, cells=cells, arrow_type=arrow_type), schema
            ).toArrow()
        assert str(result.schema.field("v").type) == "list<item: timestamp[us]>"
        expected_v = [
            [(EPOCH + timedelta(microseconds=tick // 1000)) if tick is not None else None for tick in cells[0]]
        ]
        rows = sorted(result.to_pylist(), key=lambda row: row["id"])
        assert [row["id"] for row in rows] == [1, 2]
        assert [row["v"] for row in rows] == expected_v * 2


@pytest.mark.parametrize("cogrouped", [False, True], ids=["grouped", "cogrouped"])
def test_h3_arrowdtype_submicrosecond_keeps_refusal(tmp_path: Path, cogrouped: bool) -> None:
    """An ArrowDtype nanosecond list with sub-microsecond digits keeps main's refusal."""
    with _source_session(tmp_path) as spark:
        base = spark.createDataFrame(pd.DataFrame({"id": [1, 2]}), "id int")
        schema = f"id int, v {NTZ}"
        cells = [[1767323045123456789, 1767323045000000001]]
        arrow_type = pa.list_(pa.timestamp("ns"))
        grouped = base.groupBy("id")
        with pytest.raises(PySparkException) as caught:
            if cogrouped:
                grouped.cogroup(grouped).applyInPandas(
                    partial(_arrow_frame, cells=cells, arrow_type=arrow_type), schema
                ).toArrow()
            else:
                grouped.applyInPandas(
                    partial(_arrow_frame, cells=cells, arrow_type=arrow_type), schema
                ).toArrow()
        _assert_main_refusal(caught.value, "would lose data")


@pytest.mark.parametrize("cogrouped", [False, True], ids=["grouped", "cogrouped"])
def test_h3_arrowdtype_struct_bare_ns_answers(tmp_path: Path, cogrouped: bool) -> None:
    """An ArrowDtype struct with a nanosecond field outside a list answers as on main."""
    with _source_session(tmp_path) as spark:
        base = spark.createDataFrame(pd.DataFrame({"id": [1, 2]}), "id int")
        schema = "id int, v struct<a:array<timestamp_ntz>,b:timestamp_ntz>"
        cells = [{"a": [1767323045123456], "b": 1767323045123456}]
        arrow_type = pa.struct(
            [("a", pa.list_(pa.timestamp("us"))), ("b", pa.timestamp("ns"))]
        )
        grouped = base.groupBy("id")
        if cogrouped:
            result = (
                grouped.cogroup(grouped)
                .applyInPandas(partial(_arrow_frame, cells=cells, arrow_type=arrow_type), schema)
                .toArrow()
            )
        else:
            result = grouped.applyInPandas(
                partial(_arrow_frame, cells=cells, arrow_type=arrow_type), schema
            ).toArrow()
        stamp = datetime(2026, 1, 2, 3, 4, 5, 123456)
        rows = result.to_pylist()
        assert [row["v"] for row in rows] == [{"a": [stamp], "b": stamp}] * 2


@pytest.mark.parametrize(
    "build,expected_type",
    [
        ("array_struct", "list<item: struct<t: timestamp[us]>>"),
        ("array_map", "list<item: map<string, timestamp[us]>>"),
    ],
    ids=["array_of_struct_ns", "array_of_map_ns"],
)
def test_ruling4_bare_ns_nesting_equals_main(tmp_path: Path, build: str, expected_type: str) -> None:
    """Bare nanosecond leaves under array(struct) and array(map) equal main, wrong value recorded."""
    with _source_session(tmp_path) as spark:
        if build == "array_struct":
            frame = spark.table(doors.SRC).select(
                "id", F.array(F.struct(F.col("ns").alias("t"))).alias("v")
            )
        else:
            frame = spark.table(doors.SRC).select(
                "id", F.array(F.create_map(F.lit("k"), F.col("ns"))).alias("v")
            )
        result = frame.groupBy("id").applyInPandas(_identity, frame.schema).toArrow()
        ordered = result.sort_by("id")
        assert str(ordered.schema.field("v").type) == expected_type
        flat = ordered.column("v").combine_chunks().flatten()
        if pa.types.is_struct(flat.type):
            values = flat.field("t").cast(pa.int64()).to_pylist()
        else:
            values = flat.items.cast(pa.int64()).to_pylist()
        assert sorted(values) == sorted(doors.NANOS)


@pytest.mark.parametrize("cogrouped", [False, True], ids=["grouped", "cogrouped"])
def test_map_value_ns_array_refuses(tmp_path: Path, cogrouped: bool) -> None:
    """A nanosecond array under a map value refuses on both doors."""
    with _source_session(tmp_path) as spark:
        with pytest.raises(PySparkTypeError) as caught:
            _run_user_case(spark, [{"k": NS_ARRAYS}], "map<string,array<timestamp_ntz>>", cogrouped)
        _assert_ns_array_refusal(caught.value, "v")


@pytest.mark.parametrize(
    "cells,declared,fragment",
    [
        ([pa.array([1767323045123456], pa.timestamp("ns"))], NTZ, "Cannot append scalar of type timestamp[ns]"),
        ([np.stack([NS_ARRAYS, NS_ARRAYS])], "array<array<timestamp_ntz>>", "Can only convert 1-dimensional"),
        ([{"d": NS_ARRAYS, "a": USARR}], "struct<d:array<date>,a:array<timestamp_ntz>>", "Integer too large for date32"),
    ],
    ids=["pa_cell", "two_dimensional", "struct_date_field"],
)
def test_l1_main_refusals_stand(tmp_path: Path, cells: list[Any], declared: str, fragment: str) -> None:
    """Shapes main already refuses keep main's class and text."""
    with _source_session(tmp_path) as spark:
        with pytest.raises(PySparkException) as caught:
            _run_user_case(spark, cells, declared, False)
        _assert_main_refusal(caught.value, fragment)
