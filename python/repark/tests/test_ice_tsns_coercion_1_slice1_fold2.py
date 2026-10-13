"""Slice 1 fold 2 facade pins: nanosecond sites into consumers that refused them.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import functools
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
import _record_ice_tsns_coercion_1_slice1_fold2 as fold2
import _record_ice_tsns_coercion_1_slice1_fold2_spark as fold2_spark
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from _ice_tsns_coercion_1_spellings import SRC

from repark.errors import PySparkException
from repark.spark.dataframe import udf_schema

EXPECTED_FOLD2_CELLS = 68
EXPECTED_SPARK_CELLS = 48
TWINS = {"ns": "ntz", "tzns": "ts"}
KIND_REFUSED = {
    "py|ns|map_in_arrow_ltz": (
        "mapInArrow schema mismatch on field 'v': expected type "
        "timestamp[us, tz=UTC] (timestamp), got timestamp[ns]"
    ),
}


def _load_oracle() -> dict[str, Any]:
    """Return the baseline-recorded fold-2 oracle."""
    text: str = (fold2.TESTS / "ice_tsns_coercion_1_slice1_fold2.json").read_text()
    return json.loads(text)


def _floored_ticks(value: Any) -> Any:
    """Return nanosecond ticks floored to microseconds, at any list depth."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value // 1000
    if isinstance(value, list):
        return [_floored_ticks(item) for item in value]
    step = value["step"]
    return {
        "count": value["count"],
        "first": value["first"] // 1000,
        "last": value["last"] // 1000,
        "step": None if step is None else step // 1000,
    }


def _as_microseconds(outcome: dict[str, Any]) -> dict[str, Any]:
    """Return a nanosecond outcome in its microsecond twin's unit."""
    if "error" in outcome:
        return {"error": outcome["error"].replace("timestamp[ns", "timestamp[us")}
    columns = []
    for column in outcome["columns"]:
        if "timestamp[ns" not in column["type"]:
            columns.append(column)
            continue
        columns.append(
            {
                **column,
                "type": column["type"].replace("timestamp[ns", "timestamp[us"),
                "values": _floored_ticks(column["values"]),
            }
        )
    return {"columns": columns}


def _twin_cell(cell: str) -> str:
    """Return the cell that reads the microsecond twin column."""
    zone, door, source, shape = cell.split("|")
    return f"{zone}|{door}|{TWINS[source]}|{shape}"


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> dict[str, dict[str, Any]]:
    """Return every nanosecond cell and every microsecond twin cell on this module."""
    outcomes = fold2.measure_all(tmp_path_factory.mktemp("site"), fold2.SOURCES)
    twins = fold2.measure_all(tmp_path_factory.mktemp("twin"), tuple(TWINS.values()))
    return {**outcomes, **twins}


def test_cells_main_answered_still_answer(measured: dict[str, dict[str, Any]]) -> None:
    """pins: ice-tsns-coercion-1/C-005 — a cell main answers is answered here."""
    oracle = _load_oracle()
    assert len(oracle) == EXPECTED_FOLD2_CELLS
    refused = []
    for cell, recorded in sorted(oracle.items()):
        if "error" in recorded:
            continue
        outcome = measured[cell]
        kind = cell.split("|", 1)[1]
        if kind in KIND_REFUSED:
            assert outcome == {"error": KIND_REFUSED[kind]}, cell
            continue
        if "error" in outcome:
            refused.append(f"{cell}: {outcome['error']}")
    assert refused == []


def test_nanosecond_cells_equal_the_microsecond_twin(
    measured: dict[str, dict[str, Any]],
) -> None:
    """pins: ice-tsns-coercion-1/C-005 — ticks floored to microseconds equal the twin."""
    oracle = _load_oracle()
    differing = []
    for cell in sorted(oracle):
        if _as_microseconds(measured[cell]) != measured[_twin_cell(cell)]:
            differing.append(cell)
    assert differing == []


def _differs_from_main_by_rule(cell: str) -> bool:
    """True where main's own narrowing, not the consumer, set main's ticks.

    Notes:
        In America/New_York main read the naive ``ns`` site as a zoned wall. In UTC main's
        DataFrame-door site and its Python doors cut ``-1 ns`` toward zero to ``0``.
    """
    zone, door, source, shape = cell.split("|")
    if zone == "America/New_York":
        return source == "ns"
    return door == "py" or shape == "frame"


def test_ticks_differ_from_main_only_where_main_narrowed_the_site(
    measured: dict[str, dict[str, Any]],
) -> None:
    """pins: ice-tsns-coercion-1/C-005 — 39 cells hold main's ticks, 26 the twin's."""
    oracle = _load_oracle()
    equal = []
    differing = []
    for cell, recorded in sorted(oracle.items()):
        outcome = _as_microseconds(measured[cell])
        if "error" in recorded or "error" in outcome:
            continue
        ours = [column["values"] for column in outcome["columns"]]
        theirs = [column["values"] for column in recorded["columns"]]
        (equal if ours == theirs else differing).append(cell)
    assert [cell for cell in equal if _differs_from_main_by_rule(cell)] == []
    assert [cell for cell in differing if not _differs_from_main_by_rule(cell)] == []
    assert (len(equal), len(differing)) == (39, 26)


def _arrow_kind(table: Any) -> str:
    """Return the Spark name of a ``sequence`` column's Arrow type."""
    zoned = "tz=" in str(table.schema.field("v").type)
    return "array<timestamp>" if zoned else "array<timestamp_ntz>"


def test_microsecond_sequence_forms_equal_live_spark(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — NTZ and LTZ lists equal Spark 4.1.2."""
    recorded = json.loads(fold2_spark.OUT.read_text())
    assert len(recorded) == EXPECTED_SPARK_CELLS
    for zone_index, zone in enumerate(fold2_spark.ZONES):
        warehouse = tmp_path / f"spark-{zone_index}"
        warehouse.mkdir()
        spark = doors.open_session(zone, warehouse)
        try:
            for source in fold2_spark.SOURCES:
                for form, template in fold2_spark.FORMS:
                    cell = f"{zone}|{source}|{form}"
                    expression = template.format(c=source)
                    table = spark.sql(fold2_spark.statement(expression, SRC)).toArrow()
                    column = table.column("v").combine_chunks()
                    answer = {
                        "type": _arrow_kind(table),
                        "values": pc.cast(column, pa.list_(pa.int64())).to_pylist(),
                    }
                    assert answer == recorded[cell], cell
                    assert str(column.type.value_type.unit) == "us", cell
        finally:
            spark.stop()


FINE = 1767323045123456789
NANOSECOND_TICKS = [FINE, -1, -1000, -1001, None]
FLOORED_TICKS = [FINE // 1000, -1, -1, -2, None]


def _replace_batches(batches: Iterator[Any], *, table: pa.Table) -> Iterator[Any]:
    """Drain the input and yield ``table`` instead."""
    for _ in batches:
        pass
    yield from table.to_batches()


def _replace_frames(frames: Iterator[Any], *, frame: pd.DataFrame) -> Iterator[Any]:
    """Drain the input and yield ``frame`` instead."""
    for _ in frames:
        pass
    yield frame


def _replace_group(group: pd.DataFrame, *, frame: pd.DataFrame) -> pd.DataFrame:
    """Return ``frame`` for the one group."""
    assert len(group) == 1
    return frame


def _through_map_in_arrow(spark: Any, column: pa.Array, declared: str) -> pa.Table:
    """Return what ``mapInArrow`` stores for one yielded column."""
    table = pa.table({"v": column})
    parent = spark.sql("SELECT 1 AS id")
    return parent.mapInArrow(functools.partial(_replace_batches, table=table), declared).toArrow()


def _ticks(table: pa.Table) -> list[Any]:
    """Return column ``v`` as raw ticks."""
    return pc.cast(table.column("v").combine_chunks(), pa.int64()).to_pylist()


def _nanosecond_frame(zone: str | None) -> pd.DataFrame:
    """Return a pandas frame holding the nanosecond ticks in ``zone``."""
    column = pa.array(NANOSECOND_TICKS, pa.timestamp("ns", zone))
    return pa.table({"v": column}).to_pandas()


def test_map_in_arrow_floors_nanoseconds_to_the_declared_microseconds(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — floor, never toward zero, nulls kept."""
    spark = doors.open_session("UTC", tmp_path)
    try:
        for zone, declared in ((None, "v timestamp_ntz"), ("UTC", "v timestamp")):
            column = pa.array(NANOSECOND_TICKS, pa.timestamp("ns", zone))
            stored = _through_map_in_arrow(spark, column, declared)
            assert stored.schema.field("v").type == pa.timestamp("us", zone), declared
            assert _ticks(stored) == FLOORED_TICKS, declared
            exact = pa.array(FLOORED_TICKS, pa.timestamp("us", zone))
            assert _ticks(_through_map_in_arrow(spark, exact, declared)) == FLOORED_TICKS
    finally:
        spark.stop()


def test_map_in_arrow_floors_nanoseconds_inside_lists(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — list layers, null lists and slices survive."""
    spark = doors.open_session("UTC", tmp_path)
    rows = [[FINE, None], None, [-1], [], [-1001, -1000]]
    floored = [[FINE // 1000, None], None, [-1], [], [-2, -1]]
    try:
        column = pa.array(rows, pa.list_(pa.timestamp("ns")))
        stored = _through_map_in_arrow(spark, column, "v array<timestamp_ntz>")
        values = stored.column("v").combine_chunks()
        assert values.type.value_type == pa.timestamp("us")
        assert pc.cast(values, pa.list_(pa.int64())).to_pylist() == floored
        sliced = _through_map_in_arrow(spark, column.slice(1), "v array<timestamp_ntz>")
        values = sliced.column("v").combine_chunks()
        assert pc.cast(values, pa.list_(pa.int64())).to_pylist() == floored[1:]
        nested = pa.array([rows, None, [[-1]]], pa.list_(pa.list_(pa.timestamp("ns", "UTC"))))
        stored = _through_map_in_arrow(spark, nested, "v array<array<timestamp>>")
        values = stored.column("v").combine_chunks()
        assert values.type.value_type.value_type == pa.timestamp("us", "UTC")
        assert pc.cast(values, pa.list_(pa.list_(pa.int64()))).to_pylist() == [
            floored,
            None,
            [[-1]],
        ]
    finally:
        spark.stop()


def test_map_in_arrow_refuses_a_nanosecond_column_of_another_kind(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — the zone label rule is the microsecond rule."""
    spark = doors.open_session("UTC", tmp_path)
    cases = (
        (None, "v timestamp", "timestamp[us, tz=UTC] (timestamp), got timestamp[ns]"),
        ("UTC", "v timestamp_ntz", "timestamp[us] (timestamp_ntz), got timestamp[ns, tz=UTC]"),
        (
            "America/New_York",
            "v timestamp",
            "timestamp[us, tz=UTC] (timestamp), got timestamp[ns, tz=America/New_York]",
        ),
        (None, "v date", "date32[day] (date), got timestamp[ns]"),
    )
    try:
        for zone, declared, text in cases:
            column = pa.array(NANOSECOND_TICKS, pa.timestamp("ns", zone))
            with pytest.raises(PySparkException) as caught:
                _through_map_in_arrow(spark, column, declared)
            expected = f"mapInArrow schema mismatch on field 'v': expected type {text}"
            assert expected in str(caught.value), declared
            twin = pa.array(FLOORED_TICKS, pa.timestamp("us", zone))
            with pytest.raises(PySparkException) as caught:
                _through_map_in_arrow(spark, twin, declared)
            assert expected.replace("[ns", "[us") in str(caught.value), declared
    finally:
        spark.stop()


def test_pandas_doors_floor_nanoseconds_to_the_declared_microseconds(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — mapInPandas and applyInPandas share the floor."""
    spark = doors.open_session("UTC", tmp_path)
    parent = spark.sql("SELECT 1 AS id")
    try:
        for zone, declared in ((None, "v timestamp_ntz"), ("UTC", "v timestamp")):
            frame = _nanosecond_frame(zone)
            assert str(frame["v"].dtype).startswith("datetime64[ns"), declared
            mapped = parent.mapInPandas(
                functools.partial(_replace_frames, frame=frame), declared
            ).toArrow()
            assert mapped.schema.field("v").type == pa.timestamp("us", zone), declared
            assert _ticks(mapped) == FLOORED_TICKS, declared
            applied = (
                parent.groupBy("id")
                .applyInPandas(functools.partial(_replace_group, frame=frame), declared)
                .toArrow()
            )
            assert applied.schema.field("v").type == pa.timestamp("us", zone), declared
            assert _ticks(applied) == FLOORED_TICKS, declared
    finally:
        spark.stop()


def test_apply_in_pandas_refuses_a_naive_nanosecond_column_under_a_zoned_type(
    tmp_path: Path,
) -> None:
    """pins: ice-tsns-coercion-1/C-005 — a kind mismatch reaches the shared refusal."""
    spark = doors.open_session("UTC", tmp_path)
    grouped = spark.sql("SELECT 1 AS id").groupBy("id")
    naive = functools.partial(_replace_group, frame=_nanosecond_frame(None))
    try:
        with pytest.raises(PySparkException) as caught:
            grouped.applyInPandas(naive, "v timestamp").toArrow()
        assert (
            "mapInArrow schema mismatch on field 'v': expected type "
            "timestamp[us, tz=UTC] (timestamp), got timestamp[ns]"
        ) in str(caught.value)
    finally:
        spark.stop()


FALL_BACK = 1762063800123456789
SPRING_FORWARD = 1741503600000000789
LOWEST = -(2**63)
HIGHEST = 2**63 - 1
EDGE_TICKS = [FALL_BACK, SPRING_FORWARD, LOWEST + 1, HIGHEST, -1, None]
EDGE_FLOORED = [FALL_BACK // 1000, SPRING_FORWARD // 1000, (LOWEST + 1) // 1000, HIGHEST // 1000]
EDGE_FLOORED += [-1, None]


def _checked(column: pa.Array, want: pa.DataType) -> pa.Array:
    """Return ``column`` after the shared check against a one-field declared schema."""
    batch = pa.RecordBatch.from_arrays([column], names=["v"])
    expected = pa.schema([pa.field("v", want)])
    declared = udf_schema._coerce_map_in_arrow_schema("v timestamp")[0]
    return udf_schema._validate_map_in_arrow_batch(batch, expected, declared).column(0)


def test_shared_check_floors_integer_ticks_and_keeps_the_zone_label() -> None:
    """pins: ice-tsns-coercion-1/C-005 — no local time, full int64 range, label kept."""
    ticks = [LOWEST, *EDGE_TICKS]
    floored = [LOWEST // 1000, *EDGE_FLOORED]
    for zone in (None, "UTC", "America/New_York"):
        want = pa.timestamp("us", zone)
        out = _checked(pa.array(ticks, pa.timestamp("ns", zone)), want)
        assert out.type == want, zone
        assert pc.cast(out, pa.int64()).to_pylist() == floored, zone
        listed = pa.array([ticks, None, [-1001]], pa.list_(pa.timestamp("ns", zone)))
        out = _checked(listed, pa.list_(want))
        assert out.type.value_type == want, zone
        assert pc.cast(out, pa.list_(pa.int64())).to_pylist() == [floored, None, [-2]], zone


def test_doors_floor_daylight_saving_and_range_edge_instants(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — fall-back, spring-forward and int64 edges."""
    spark = doors.open_session("America/New_York", tmp_path)
    parent = spark.sql("SELECT 1 AS id")
    try:
        for zone, declared in ((None, "v timestamp_ntz"), ("UTC", "v timestamp")):
            column = pa.array(EDGE_TICKS, pa.timestamp("ns", zone))
            frame = pa.table({"v": column}).to_pandas()
            answers = {
                "mapInArrow": _through_map_in_arrow(spark, column, declared),
                "mapInPandas": parent.mapInPandas(
                    functools.partial(_replace_frames, frame=frame), declared
                ).toArrow(),
                "applyInPandas": parent.groupBy("id")
                .applyInPandas(functools.partial(_replace_group, frame=frame), declared)
                .toArrow(),
            }
            for door, stored in answers.items():
                assert stored.schema.field("v").type == pa.timestamp("us", zone), door
                assert _ticks(stored) == EDGE_FLOORED, (door, declared)
            lowest = pa.array([LOWEST], pa.timestamp("ns", zone))
            assert _ticks(_through_map_in_arrow(spark, lowest, declared)) == [LOWEST // 1000]
    finally:
        spark.stop()
