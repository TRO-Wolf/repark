"""ICE-NTZ-NESTED-REFUSE-1: an instant for a nested NTZ leaf refuses outside UTC.

Outside a UTC session an Arrow frame whose nested leaf carries a time zone is refused by name
on every DataFrame writer, before any file is written. A naive leaf stores its wall, and a UTC
session stores the UTC wall as before. The SQL doors are pinned at the Rust door
(``crates/repark-spark/tests/ntz_nested_refuse.rs``).

Spark 4.1.2 answers 174 recorded cells on twelve of PR #1037's doors
(``ice_ntz_nested_refuse_1_spark.json``: 12 doors, 5 sources, 3 zones, less the two column
sources on ``insert_values``): the session wall of the instant in each of the 138 instant and
cast cells, and the literal's own wall in the 36 ``TIMESTAMP_NTZ`` literal cells. RePark refuses
the instant cells outside UTC
(parity row ICE-TSNS-SQL-1-R-011) and answers Spark's value everywhere else, the hint's
``CAST`` spelling included, except on the plain ``UPDATE`` that assigns the whole struct.

pins: ice-ntz-nested-refuse-1/C-009, C-010, C-015
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import _ice_ntz_nested_refuse_1_doors as cells
import pyarrow as pa
import pytest

from repark import ReparkSession, functions

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
SPARK_CELLS: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_ntz_nested_refuse_1_spark.json").read_text(encoding="utf-8")
)["cells"]
LIVE_CATALOG = "ntzr_live"
INSTANT_MICROS = 1_767_323_045_123_456
NEW_YORK = "America/New_York"
ZONED = pa.timestamp("us", tz="UTC")
NAIVE = pa.timestamp("us")
ONE_INT = pa.array([1], pa.int32())
REFUSED = (
    r"\[INCOMPATIBLE_DATA_FOR_TABLE\.CANNOT_SAFELY_CAST\] Cannot write incompatible data for "
    r"the table `ice`\.`ns`\.`t`: Cannot safely cast {path} to \"TIMESTAMP_NTZ\"\. A value "
    r"that is not provably a TIMESTAMP_NTZ wall time is not written into a nested TIMESTAMP_NTZ "
    r"field while the session time zone is not UTC"
)


def leaf(arrow_type: pa.DataType) -> pa.Array:
    """Return one instant as a one-row array of ``arrow_type``."""
    return pa.array([INSTANT_MICROS], pa.int64()).cast(arrow_type)


def struct_of(arrow_type: pa.DataType) -> pa.Array:
    """Return ``struct<v, n>`` with ``v`` of ``arrow_type``."""
    return pa.StructArray.from_arrays([leaf(arrow_type), ONE_INT], ["v", "n"])


def zoned_dictionary() -> pa.Array:
    """Return ``struct<v, n>`` whose zoned ``v`` is dictionary encoded."""
    return pa.StructArray.from_arrays([leaf(ZONED).dictionary_encode(), ONE_INT], ["v", "n"])


def zoned_run_ends() -> pa.Array:
    """Return ``struct<v, n>`` whose zoned ``v`` is run-end encoded."""
    encoded = pa.RunEndEncodedArray.from_arrays(ONE_INT, leaf(ZONED))
    return pa.StructArray.from_arrays([encoded, ONE_INT], ["v", "n"])


def zoned_list() -> pa.Array:
    """Return ``array<timestamp>`` holding one zoned instant."""
    return pa.array([[INSTANT_MICROS]], pa.list_(pa.int64())).cast(pa.list_(ZONED))


def zoned_map() -> pa.Array:
    """Return ``map<string, timestamp>`` holding one zoned instant."""
    return pa.array([[("k", INSTANT_MICROS)]], pa.map_(pa.string(), pa.int64())).cast(
        pa.map_(pa.string(), ZONED)
    )


LAYOUTS: dict[str, tuple[str, Callable[[], pa.Array], str]] = {
    "struct": ("STRUCT<v: TIMESTAMP_NTZ, n: INT>", lambda: struct_of(ZONED), "`st`.`v`"),
    "dictionary_child": ("STRUCT<v: TIMESTAMP_NTZ, n: INT>", zoned_dictionary, "`st`.`v`"),
    "run_end_encoded_child": ("STRUCT<v: TIMESTAMP_NTZ, n: INT>", zoned_run_ends, "`st`.`v`"),
    "list": ("ARRAY<TIMESTAMP_NTZ>", zoned_list, "`st`.`element`"),
    "map_value": ("MAP<STRING, TIMESTAMP_NTZ>", zoned_map, "`st`.`value`"),
}


def write_from_a_temporary_view(spark: Any, frame: Any) -> None:
    """Hold ``frame`` as a SQL temporary view and INSERT from it."""
    frame.createOrReplaceTempView("held")
    spark.sql("INSERT INTO ice.ns.t SELECT id, st FROM held")


DOORS: dict[str, Callable[[Any, Any], None]] = {
    "df_append": lambda _, frame: frame.writeTo("ice.ns.t").append(),
    "df_overwrite_partitions": lambda _, frame: frame.writeTo("ice.ns.t").overwritePartitions(),
    "df_overwrite_where": lambda _, frame: frame.writeTo("ice.ns.t").overwrite(
        functions.col("id") >= 0
    ),
    "df_insert_into": lambda _, frame: frame.write.insertInto("ice.ns.t"),
    "df_insert_into_overwrite": lambda _, frame: frame.write.insertInto("ice.ns.t", overwrite=True),
    "df_save_append": lambda _, frame: frame.write.mode("append").saveAsTable("ice.ns.t"),
    "cached_frame": lambda _, frame: frame.cache().writeTo("ice.ns.t").append(),
    "temporary_view": write_from_a_temporary_view,
}


def open_session(zone: str, warehouse: Path, column_type: str) -> Any:
    """Open a session in ``zone`` with an empty ``ice.ns.t (id INT, st <column_type>)``."""
    spark = (
        ReparkSession.builder.appName("ice-ntz-nested-refuse-1")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    spark.sql(
        f"CREATE TABLE ice.ns.t (id INT, st {column_type}) USING iceberg "
        "TBLPROPERTIES ('format-version' = '2')"
    )
    return spark


def frame_of(spark: Any, name: str, column: pa.Array) -> Any:
    """Return a DataFrame over ``(id, st)`` that keeps every Arrow type of ``column``."""
    table = pa.table({"id": ONE_INT, "st": column})
    spark._ensure_alive().register_arrow_stream_as_temp_view(name, table)
    return spark.table(name)


def files_under(root: Path) -> list[str]:
    """Return every file below ``root``."""
    return sorted(str(path) for path in root.rglob("*") if path.is_file())


def stored_leaf(spark: Any) -> list[int | None]:
    """Return the int64 ticks of ``ice.ns.t.st.v``."""
    table = spark.sql("SELECT st.v AS v FROM ice.ns.t").toArrow()
    return table.column("v").combine_chunks().cast(pa.int64()).to_pylist()


@pytest.mark.parametrize("door", list(DOORS))
@pytest.mark.parametrize("layout", list(LAYOUTS))
def test_a_zoned_nested_leaf_is_refused_by_name_and_writes_nothing(
    tmp_path: Path, layout: str, door: str
) -> None:
    """Every zoned Arrow layout refuses at every DataFrame writer in New York, with no file."""
    column_type, build, path = LAYOUTS[layout]
    spark = open_session(NEW_YORK, tmp_path, column_type)
    try:
        frame = frame_of(spark, f"frame_{layout}", build())
        before = files_under(tmp_path)
        with pytest.raises(Exception, match=REFUSED.format(path=re.escape(path))):
            DOORS[door](spark, frame)
        assert files_under(tmp_path) == before
        assert spark.sql("SELECT * FROM ice.ns.t.snapshots").toArrow().num_rows == 0
    finally:
        spark.stop()


@pytest.mark.parametrize("door", list(DOORS))
def test_a_naive_nested_leaf_stores_its_wall_outside_utc(tmp_path: Path, door: str) -> None:
    """A naive Arrow leaf stores its own wall in New York on every DataFrame writer."""
    spark = open_session(NEW_YORK, tmp_path, "STRUCT<v: TIMESTAMP_NTZ, n: INT>")
    try:
        DOORS[door](spark, frame_of(spark, "naive", struct_of(NAIVE)))
        assert stored_leaf(spark) == [INSTANT_MICROS]
    finally:
        spark.stop()


@pytest.mark.parametrize("door", list(DOORS))
def test_a_utc_session_stores_the_utc_wall_as_before(tmp_path: Path, door: str) -> None:
    """In a UTC session the zoned leaf stores its UTC wall, the value main stored."""
    spark = open_session("UTC", tmp_path, "STRUCT<v: TIMESTAMP_NTZ, n: INT>")
    try:
        DOORS[door](spark, frame_of(spark, "zoned", struct_of(ZONED)))
        assert stored_leaf(spark) == [INSTANT_MICROS]
    finally:
        spark.stop()


CAST_DOORS = [door for door in DOORS if door != "cached_frame"]
CONTAINER_CAST = (
    "SELECT 1 AS id, CAST(named_struct('v', TIMESTAMP '2026-01-02 03:04:05.123456+00:00', "
    "'n', 1) AS STRUCT<v: TIMESTAMP_NTZ, n: INT>) AS st"
)


@pytest.mark.parametrize("door", CAST_DOORS)
def test_a_frame_holding_a_container_cast_of_an_instant_refuses_outside_utc(
    tmp_path: Path, door: str
) -> None:
    """A frame whose struct is a ``CAST`` of a zoned struct refuses at every writer, no file.

    The planner's cast of a whole struct keeps the UTC wall, so the frame's naive type alone
    would pass; the router reads the cast in the frame's plan. A cached frame holds the cast's
    result, not the cast, and is not in this set.

    pins: ice-ntz-nested-refuse-1/C-015
    """
    spark = open_session(NEW_YORK, tmp_path, "STRUCT<v: TIMESTAMP_NTZ, n: INT>")
    try:
        frame = spark.sql(CONTAINER_CAST)
        before = files_under(tmp_path)
        with pytest.raises(Exception, match=REFUSED.format(path=re.escape("`st`.`v`"))):
            DOORS[door](spark, frame)
        assert files_under(tmp_path) == before
    finally:
        spark.stop()


def repark_cells(zone: str, warehouse: Path) -> dict[str, str]:
    """Run every door cell on RePark in ``zone`` and return the answers by key."""
    spark = (
        ReparkSession.builder.appName("ice-ntz-nested-refuse-1-cells")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", str(warehouse))
        cells.seed(spark, "ice")
        answers = {}
        for index, (door, source) in enumerate(cells.cells()):
            answer = cells.run_cell(spark, "ice", door, source, f"ice.ns.t{index}")
            answers[f"{zone}|{door}|{source}"] = cells.repark_answer(answer)
        return answers
    finally:
        spark.stop()


@pytest.mark.parametrize("zone", cells.ZONES)
def test_repark_refuses_where_spark_stores_an_instant_and_answers_spark_elsewhere(
    tmp_path: Path, zone: str
) -> None:
    """Each recorded Spark cell: RePark refuses outside UTC by the rule, else stores Spark's."""
    answers = repark_cells(zone, tmp_path)
    expected = {
        key: cells.ruled_answer(*key.split("|")[1:], zone, SPARK_CELLS[key]) for key in answers
    }
    assert answers == expected


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_spark_stores_the_recorded_session_wall(tmp_path: Path) -> None:
    """Live Spark 4.1.2 re-derives every recorded cell in all three zones."""
    import _live_parity as live

    spark = live.build_spark_iceberg_engine(tmp_path / "spark", catalog=LIVE_CATALOG).session
    live_cells = {}
    try:
        cells.seed(spark, LIVE_CATALOG)
        index = 0
        for zone in cells.ZONES:
            spark.conf.set("spark.sql.session.timeZone", zone)
            for door, source in cells.cells():
                index += 1
                table = f"{LIVE_CATALOG}.ns.t{index}"
                answer = cells.run_cell(spark, LIVE_CATALOG, door, source, table)
                live_cells[f"{zone}|{door}|{source}"] = answer
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    assert live_cells == SPARK_CELLS
