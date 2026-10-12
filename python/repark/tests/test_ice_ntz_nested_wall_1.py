"""ICE-NTZ-NESTED-WALL-1: a nested microsecond ``TIMESTAMP_NTZ`` leaf stores one wall.

pins: ice-ntz-nested-wall-1/C-001, C-002, C-003, C-004
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from ice_ntz_nested_wall_1_helpers import (
    _V2,
    INSTANT,
    SESSION_WALL,
    WALL,
    WALL_TICK,
    ZONES,
    by_name_from_column,
    by_name_literal,
    create_nested,
    insert_into_overwrite,
    leaf_ticks,
    open_session,
    overwrite_partitions,
    overwrite_select,
    parquet_leaf,
    seed_null,
    stored_rows,
    write_source,
)


@pytest.mark.parametrize("zone", ZONES)
def test_zone_store_doors_store_the_session_wall(tmp_path: Path, zone: str) -> None:
    """By-name and overwrite doors store the session wall of an instant."""
    spark = open_session(zone, tmp_path)
    try:
        write_source(spark)
        expected = SESSION_WALL[zone]
        doors = (
            ("by_name", by_name_from_column),
            ("overwrite", overwrite_select),
            ("partitions", overwrite_partitions),
            ("insert_into", insert_into_overwrite),
        )
        for name, write in doors:
            table = f"ice.ns.{name}"
            create_nested(spark, table, "TIMESTAMP_NTZ")
            write(spark, table)
            assert leaf_ticks(spark, table, "st.v") == [expected], name
        wall = "ice.ns.wall"
        create_nested(spark, wall, "TIMESTAMP_NTZ")
        by_name_literal(spark, wall, WALL)
        assert leaf_ticks(spark, wall, "st.v") == [WALL_TICK]
        literal = "ice.ns.literal"
        create_nested(spark, literal, "TIMESTAMP_NTZ")
        by_name_literal(spark, literal, INSTANT)
        assert leaf_ticks(spark, literal, "st.v") == [expected]
        control = "ice.ns.ltz"
        create_nested(spark, control, "TIMESTAMP")
        by_name_from_column(spark, control)
        assert leaf_ticks(spark, control, "st.v") == [WALL_TICK]
    finally:
        spark.stop()


def merge_insert_column(spark: Any, table: str) -> None:
    """MERGE INSERT of the source column."""
    spark.sql(
        f"MERGE INTO {table} t USING ice.ns.src s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id, st, k) "
        "VALUES (s.id, named_struct('v', s.l, 'n', 1), 0)"
    )


def merge_insert_star(spark: Any, table: str) -> None:
    """MERGE ``INSERT *`` of the source column."""
    spark.sql(
        f"MERGE INTO {table} t USING ("
        "SELECT id, named_struct('v', l, 'n', 1) AS st, 0 AS k FROM ice.ns.src"
        ") s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"
    )


def merge_insert_literal(spark: Any, table: str, literal: str) -> None:
    """MERGE INSERT of one literal."""
    spark.sql(
        f"MERGE INTO {table} t USING (SELECT 1 AS id) s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id, st, k) "
        f"VALUES (s.id, named_struct('v', {literal}, 'n', 1), 0)"
    )


@pytest.mark.parametrize("zone", ZONES)
def test_merge_insert_doors_store_the_session_wall(tmp_path: Path, zone: str) -> None:
    """MERGE INSERT stores the session wall of an instant and leaves a wall literal."""
    spark = open_session(zone, tmp_path)
    try:
        write_source(spark)
        expected = SESSION_WALL[zone]
        doors = (
            ("column", merge_insert_column),
            ("star", merge_insert_star),
        )
        for name, write in doors:
            table = f"ice.ns.{name}"
            create_nested(spark, table, "TIMESTAMP_NTZ")
            write(spark, table)
            assert leaf_ticks(spark, table, "st.v") == [expected], name
        literal = "ice.ns.literal"
        create_nested(spark, literal, "TIMESTAMP_NTZ")
        merge_insert_literal(spark, literal, INSTANT)
        assert leaf_ticks(spark, literal, "st.v") == [expected]
        wall = "ice.ns.wall"
        create_nested(spark, wall, "TIMESTAMP_NTZ")
        merge_insert_literal(spark, wall, WALL)
        assert leaf_ticks(spark, wall, "st.v") == [WALL_TICK]
        control = "ice.ns.ltz"
        create_nested(spark, control, "TIMESTAMP")
        merge_insert_column(spark, control)
        assert leaf_ticks(spark, control, "st.v") == [WALL_TICK]
    finally:
        spark.stop()


def merge_update_whole(spark: Any, table: str) -> None:
    """MERGE UPDATE of the whole struct from the source column."""
    seed_null(spark, table)
    spark.sql(
        f"MERGE INTO {table} t USING ice.ns.src s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.st = named_struct('v', s.l, 'n', 1)"
    )


def update_from_sibling(spark: Any, table: str) -> None:
    """UPDATE the whole struct from a ``TIMESTAMP`` column of the same row."""
    spark.sql(
        f"CREATE TABLE {table} "
        "(id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT, c TIMESTAMP) "
        "USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(
        f"INSERT INTO {table} SELECT 1, "
        "named_struct('v', CAST(NULL AS TIMESTAMP_NTZ), 'n', 0), 0, l FROM ice.ns.src"
    )
    spark.sql(f"UPDATE {table} SET st = named_struct('v', c, 'n', 1) WHERE id >= 0")


def update_whole_where(spark: Any, table: str, literal: str) -> None:
    """UPDATE the whole struct from one literal."""
    seed_null(spark, table)
    spark.sql(f"UPDATE {table} SET st = named_struct('v', {literal}, 'n', 1) WHERE id >= 0")


@pytest.mark.parametrize("zone", ZONES)
def test_whole_struct_update_stores_the_session_wall(tmp_path: Path, zone: str) -> None:
    """Whole-struct assignment stores the session wall of a probed instant."""
    spark = open_session(zone, tmp_path)
    try:
        write_source(spark)
        expected = SESSION_WALL[zone]
        merged = "ice.ns.merged"
        create_nested(spark, merged, "TIMESTAMP_NTZ")
        merge_update_whole(spark, merged)
        assert leaf_ticks(spark, merged, "st.v") == [expected], "merge"
        sibling = "ice.ns.sibling"
        update_from_sibling(spark, sibling)
        assert leaf_ticks(spark, sibling, "st.v") == [expected], "sibling"
        wall = "ice.ns.wall_upd"
        create_nested(spark, wall, "TIMESTAMP_NTZ")
        update_whole_where(spark, wall, WALL)
        assert leaf_ticks(spark, wall, "st.v") == [WALL_TICK], "wall"
        field = "ice.ns.field"
        create_nested(spark, field, "TIMESTAMP_NTZ")
        seed_null(spark, field)
        spark.sql(
            f"MERGE INTO {field} t USING ice.ns.src s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.st.v = s.l"
        )
        assert leaf_ticks(spark, field, "st.v") == [expected], "field"
        control = "ice.ns.ltz"
        create_nested(spark, control, "TIMESTAMP")
        spark.sql(
            f"INSERT INTO {control} VALUES "
            "(1, named_struct('v', CAST(NULL AS TIMESTAMP), 'n', 0), 0)"
        )
        spark.sql(
            f"MERGE INTO {control} t USING ice.ns.src s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.st = named_struct('v', s.l, 'n', 1)"
        )
        assert leaf_ticks(spark, control, "st.v") == [WALL_TICK], "control"
    finally:
        spark.stop()


@pytest.mark.parametrize("zone", ZONES)
def test_identity_update_stores_the_session_wall(tmp_path: Path, zone: str) -> None:
    """Identity UPDATE stores the session wall when the SET source is an instant."""
    spark = open_session(zone, tmp_path)
    try:
        write_source(spark)
        expected = SESSION_WALL[zone]
        sibling = "ice.ns.sibling"
        update_from_sibling(spark, sibling)
        assert leaf_ticks(spark, sibling, "st.v") == [expected], "sibling"
        wall = "ice.ns.wall_upd"
        create_nested(spark, wall, "TIMESTAMP_NTZ")
        update_whole_where(spark, wall, WALL)
        assert leaf_ticks(spark, wall, "st.v") == [WALL_TICK], "wall"
        control = "ice.ns.ltz"
        spark.sql(
            f"CREATE TABLE {control} "
            "(id INT, st STRUCT<v: TIMESTAMP, n: INT>, k INT, c TIMESTAMP) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(
            f"INSERT INTO {control} SELECT 1, "
            "named_struct('v', CAST(NULL AS TIMESTAMP), 'n', 0), 0, l FROM ice.ns.src"
        )
        spark.sql(f"UPDATE {control} SET st = named_struct('v', c, 'n', 1) WHERE id >= 0")
        assert leaf_ticks(spark, control, "st.v") == [WALL_TICK], "control"
    finally:
        spark.stop()


def insert_select_column(spark: Any, table: str) -> None:
    """``INSERT … SELECT`` of the source column."""
    spark.sql(f"INSERT INTO {table} SELECT id, named_struct('v', l, 'n', 1), 0 FROM ice.ns.src")


def append_column(spark: Any, table: str) -> None:
    """DataFrame ``append`` of the source column."""
    frame = spark.sql("SELECT id, named_struct('v', l, 'n', 1) AS st, 0 AS k FROM ice.ns.src")
    frame.writeTo(table).append()


def struct_values(spark: Any, table: str) -> list[Any]:
    """Return the ``st`` column from the table's Parquet data files."""
    files = spark.sql(f"SELECT content, file_path FROM {table}.files").toArrow().to_pylist()
    values: list[Any] = []
    for file in files:
        if file["content"] != 0:
            continue
        column = pq.read_table(file["file_path"], columns=["st"]).column("st")
        values.extend(column.to_pylist())
    return values


def write_null_sources(spark: Any) -> None:
    """Write a null struct and a null inner struct at depth 2.

    The source leaves are ``TIMESTAMP``, so this write is not a nested
    ``TIMESTAMP_NTZ`` store. Main copies these columns unchanged.
    """
    spark.sql(
        "CREATE TABLE ice.ns.null_src (id INT, st STRUCT<v: TIMESTAMP, n: INT>) "
        "USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql("INSERT INTO ice.ns.null_src SELECT 1, CAST(NULL AS STRUCT<v: TIMESTAMP, n: INT>)")
    spark.sql(
        "CREATE TABLE ice.ns.null_inner_src ("
        "id INT, st STRUCT<i: STRUCT<v: TIMESTAMP, n: INT>, n: INT>"
        ") USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(
        "INSERT INTO ice.ns.null_inner_src SELECT 1, "
        "named_struct('i', CAST(NULL AS STRUCT<v: TIMESTAMP, n: INT>), 'n', 7)"
    )


def create_depth2(spark: Any, table: str) -> None:
    """Create a v2 table whose depth-2 leaf is ``TIMESTAMP_NTZ``."""
    spark.sql(
        f"CREATE TABLE {table} ("
        "id INT, st STRUCT<i: STRUCT<v: TIMESTAMP_NTZ, n: INT>, n: INT>, k INT"
        ") USING iceberg TBLPROPERTIES ('format-version'='2')"
    )


def test_a_null_struct_stays_null(tmp_path: Path) -> None:
    """A null struct and a null inner struct read back as null.

    ``INSERT … SELECT`` and DataFrame ``append`` both copy the column. Main
    stores null. The rebuilt projection must store null at every depth.
    """
    spark = open_session("UTC", tmp_path)
    try:
        write_null_sources(spark)
        assert struct_values(spark, "ice.ns.null_src") == [None], "null source"
        assert struct_values(spark, "ice.ns.null_inner_src") == [{"i": None, "n": 7}], (
            "inner source"
        )

        selected = "ice.ns.null_sel"
        create_nested(spark, selected, "TIMESTAMP_NTZ")
        spark.sql(f"INSERT INTO {selected} SELECT id, st, 0 FROM ice.ns.null_src")
        assert struct_values(spark, selected) == [None], "select null struct"

        appended = "ice.ns.null_app"
        create_nested(spark, appended, "TIMESTAMP_NTZ")
        spark.sql("SELECT id, st, 0 AS k FROM ice.ns.null_src").writeTo(appended).append()
        assert struct_values(spark, appended) == [None], "append null struct"

        inner_selected = "ice.ns.inner_sel"
        create_depth2(spark, inner_selected)
        spark.sql(f"INSERT INTO {inner_selected} SELECT id, st, 0 FROM ice.ns.null_inner_src")
        assert struct_values(spark, inner_selected) == [{"i": None, "n": 7}], "select null inner"

        inner_appended = "ice.ns.inner_app"
        create_depth2(spark, inner_appended)
        frame = spark.sql("SELECT id, st, 0 AS k FROM ice.ns.null_inner_src")
        frame.writeTo(inner_appended).append()
        assert struct_values(spark, inner_appended) == [{"i": None, "n": 7}], "append null inner"
    finally:
        spark.stop()


@pytest.mark.parametrize("zone", ZONES)
def test_insert_analysis_stores_the_session_wall(tmp_path: Path, zone: str) -> None:
    """Plain INSERT and DataFrame append store the session wall of a column instant."""
    spark = open_session(zone, tmp_path)
    try:
        write_source(spark)
        expected = SESSION_WALL[zone]
        selected = "ice.ns.selected"
        create_nested(spark, selected, "TIMESTAMP_NTZ")
        insert_select_column(spark, selected)
        assert leaf_ticks(spark, selected, "st.v") == [expected], "select"
        appended = "ice.ns.appended"
        create_nested(spark, appended, "TIMESTAMP_NTZ")
        append_column(spark, appended)
        assert leaf_ticks(spark, appended, "st.v") == [expected], "append"
        literal = "ice.ns.literal"
        create_nested(spark, literal, "TIMESTAMP_NTZ")
        spark.sql(f"INSERT INTO {literal} VALUES (1, named_struct('v', {INSTANT}, 'n', 1), 0)")
        assert leaf_ticks(spark, literal, "st.v") == [WALL_TICK], "literal"
        wall = "ice.ns.wall_ins"
        create_nested(spark, wall, "TIMESTAMP_NTZ")
        spark.sql(f"INSERT INTO {wall} VALUES (1, named_struct('v', {WALL}, 'n', 1), 0)")
        assert leaf_ticks(spark, wall, "st.v") == [WALL_TICK], "wall"
        control = "ice.ns.ltz_ins"
        create_nested(spark, control, "TIMESTAMP")
        spark.sql(
            f"INSERT INTO {control} SELECT id, named_struct('v', l, 'n', 1), 0 FROM ice.ns.src"
        )
        assert leaf_ticks(spark, control, "st.v") == [WALL_TICK], "control"
    finally:
        spark.stop()


def session_wall_of(spark: Any, literal: str) -> int:
    """The existing leaf kernel's session wall for one instant."""
    arrow = spark.sql(f"SELECT __repark_cast_timestamp_ntz__({literal}) AS v").toArrow()
    value = arrow.column("v").cast(pa.int64(), safe=False).to_pylist()[0]
    assert isinstance(value, int)
    return value


def test_positional_members_land_with_mains_cast(tmp_path: Path) -> None:
    """Renamed and case-different members follow main's cast.

    Reordered exact names do not align, so the insert keeps main's expression
    and stores each UTC wall on its name.
    """
    spark = open_session("America/New_York", tmp_path)
    try:
        spark.sql("SET spark.sql.caseSensitive=true")
        other = "TIMESTAMP '2024-01-01 12:00:00+00:00'"
        other_wall = 1_704_110_400_000_000
        instant_wall = WALL_TICK

        renamed = "ice.ns.renamed"
        spark.sql(
            f"CREATE TABLE {renamed} (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(f"INSERT INTO {renamed} SELECT 1, named_struct('a', {INSTANT}, 'n', 4)")
        row = struct_values(spark, renamed)[0]
        assert row["n"] == 4, row
        assert row["v"] is None, row

        ordered = "ice.ns.ordered"
        spark.sql(
            f"CREATE TABLE {ordered} (id INT, st STRUCT<v: TIMESTAMP_NTZ, w: TIMESTAMP_NTZ>) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(f"INSERT INTO {ordered} SELECT 1, named_struct('w', {other}, 'v', {INSTANT})")
        assert leaf_ticks(spark, ordered, "st.v") == [instant_wall]
        assert leaf_ticks(spark, ordered, "st.w") == [other_wall]

        extra = "ice.ns.extra"
        spark.sql(
            f"CREATE TABLE {extra} (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(f"INSERT INTO {extra} SELECT 1, named_struct('v', {INSTANT}, 'n', 4, 'extra', 9)")
        extra_row = struct_values(spark, extra)[0]
        assert set(extra_row) == {"v", "n"}, extra_row
        assert extra_row["n"] == 4
        assert leaf_ticks(spark, extra, "st.v") == [instant_wall]

        cased = "ice.ns.cased"
        spark.sql(
            f"CREATE TABLE {cased} (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(f"INSERT INTO {cased} SELECT 1, named_struct('V', {INSTANT}, 'n', 4)")
        cased_row = struct_values(spark, cased)[0]
        assert cased_row["n"] == 4, cased_row
        assert cased_row["v"] is None, cased_row
    finally:
        spark.stop()


def wide_struct_type() -> str:
    """A 20-field struct: eighteen shared strings, one other string, one NTZ."""
    shared = ", ".join(f"f{index:02d} STRING" for index in range(18))
    return f"STRUCT<{shared}, f18 STRING, v TIMESTAMP_NTZ>"


def wide_subquery() -> str:
    """One subquery whose ``x`` feeds ``f00`` through ``f17``."""
    shared = ", ".join(f"'f{index:02d}', x" for index in range(18))
    return (
        "(SELECT named_struct("
        f"{shared}, 'f18', y, 'v', l) FROM ("
        "SELECT uuid() AS x, uuid() AS y, l FROM ice.ns.src))"
    )


def test_a_volatile_struct_is_evaluated_once(tmp_path: Path) -> None:
    """A 20-field struct from one volatile subquery keeps one draw."""
    spark = open_session("America/New_York", tmp_path)
    try:
        write_source(spark)
        kind = wide_struct_type()
        selected = "ice.ns.wide_sel"
        spark.sql(
            f"CREATE TABLE {selected} (id INT, st {kind}) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        statement = f"INSERT INTO {selected} SELECT 1, {wide_subquery()}"
        explained = spark.sql(f"EXPLAIN {statement}").toArrow().to_pylist()
        plan = " ".join(str(row) for row in explained).lower()
        assert "rand(" not in plan, plan
        assert "random(" not in plan, plan
        spark.sql(statement)
        row = struct_values(spark, selected)[0]
        assert row["f00"] == row["f17"], row
        assert row["f00"] != row["f18"], row
        assert leaf_ticks(spark, selected, "st.v") == [SESSION_WALL["America/New_York"]]

        appended = "ice.ns.wide_app"
        spark.sql(
            f"CREATE TABLE {appended} (id INT, st {kind}) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        frame = spark.sql(f"SELECT 1 AS id, {wide_subquery()} AS st")
        frame.writeTo(appended).append()
        appended_row = struct_values(spark, appended)[0]
        assert appended_row["f00"] == appended_row["f17"], appended_row
        assert appended_row["f00"] != appended_row["f18"], appended_row
        assert leaf_ticks(spark, appended, "st.v") == [SESSION_WALL["America/New_York"]]
    finally:
        spark.stop()


def error_line(error: Exception) -> str:
    """Return the first non-empty line of ``error``, as the step-0 probe does."""
    text = [line.strip() for line in str(error).splitlines() if line.strip()]
    return text[0] if text else ""


def test_merge_update_clauses_store_the_session_wall_before_the_none_site(
    tmp_path: Path,
) -> None:
    """Spark MERGE UPDATE stores the session wall before ``store_assignment_then_sql``."""
    spark = open_session("America/New_York", tmp_path)
    expected = SESSION_WALL["America/New_York"]
    try:
        write_source(spark)
        matched = "ice.ns.q7_matched"
        create_nested(spark, matched, "TIMESTAMP_NTZ")
        seed_null(spark, matched)
        spark.sql(
            f"MERGE INTO {matched} t USING ice.ns.src s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.st = named_struct('v', s.l, 'n', 1)"
        )
        assert leaf_ticks(spark, matched, "st.v") == [expected]
        nmbs = "ice.ns.q7_nmbs"
        spark.sql(
            f"CREATE TABLE {nmbs} "
            "(id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT, c TIMESTAMP) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(
            f"INSERT INTO {nmbs} SELECT 9, "
            f"named_struct('v', CAST(NULL AS TIMESTAMP_NTZ), 'n', 0), 0, {INSTANT}"
        )
        spark.sql(
            f"MERGE INTO {nmbs} t USING ice.ns.src s ON t.id = s.id "
            "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.st = named_struct('v', t.c, 'n', 1)"
        )
        assert leaf_ticks(spark, nmbs, "st.v") == [expected]
    finally:
        spark.stop()


def test_raw_error_doors_still_raise_mains_error(tmp_path: Path) -> None:
    """``UPDATE`` with no ``WHERE`` and ``INSERT OVERWRITE … VALUES`` raise main's error."""
    overwrite_line = (
        "datafusion engine error: Arrow error: Invalid argument error: column types "
        'must match schema types, expected Struct("v": Timestamp(ns), "n": Int32) but '
        'found Struct("v": Timestamp(\u00b5s, "UTC"), "n": Int32) at column index 1'
    )
    update_line = (
        "datafusion engine error: Arrow error: Invalid argument error: "
        "arguments need to have the same data type"
    )
    spark = open_session("UTC", tmp_path)
    try:
        create_nested(spark, "ice.ns.over", "TIMESTAMP_NTZ")
        spark.sql(
            "INSERT INTO ice.ns.over (id, st, k) VALUES "
            "(1, named_struct('v', CAST(NULL AS TIMESTAMP_NTZ), 'n', 0), 0)"
        )
        with pytest.raises(Exception) as caught:
            spark.sql(
                f"INSERT OVERWRITE ice.ns.over VALUES (1, named_struct('v', {INSTANT}, 'n', 1), 0)"
            )
        assert type(caught.value).__name__ == "PySparkException"
        assert error_line(caught.value) == overwrite_line

        create_nested(spark, "ice.ns.upd", "TIMESTAMP_NTZ")
        spark.sql(
            "INSERT INTO ice.ns.upd (id, st, k) VALUES "
            "(1, named_struct('v', CAST(NULL AS TIMESTAMP_NTZ), 'n', 0), 0)"
        )
        with pytest.raises(Exception) as caught:
            spark.sql(f"UPDATE ice.ns.upd SET st = named_struct('v', {INSTANT}, 'n', 1)")
        assert type(caught.value).__name__ == "PySparkException"
        assert error_line(caught.value) == update_line
    finally:
        spark.stop()


def test_a_struct_under_a_list_keeps_the_utc_wall(tmp_path: Path) -> None:
    """A struct under a list stays the physical instant, which is main's cast."""
    spark = open_session("America/New_York", tmp_path)
    try:
        write_source(spark)
        spark.sql(
            "CREATE TABLE ice.ns.arr (id INT, a ARRAY<STRUCT<v: TIMESTAMP_NTZ>>) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql("INSERT INTO ice.ns.arr SELECT 1, array(named_struct('v', l)) FROM ice.ns.src")
        column = spark.sql("SELECT a AS a FROM ice.ns.arr").toArrow().column("a")
        assert column.combine_chunks().to_pylist() == [
            [{"v": datetime(2026, 1, 2, 3, 4, 5, 123456)}]
        ]
    finally:
        spark.stop()


def test_repair_moves_a_main_wall_to_the_session_wall(tmp_path: Path) -> None:
    """An UPDATE of the leaf repairs a row that holds the UTC wall main wrote."""
    spark = open_session("America/New_York", tmp_path)
    try:
        write_source(spark)
        table = "ice.ns.repair"
        spark.sql(
            f"CREATE TABLE {table} "
            "(id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT, c TIMESTAMP) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(
            f"INSERT INTO {table} SELECT 1, named_struct('v', {WALL}, 'n', 1), 0, l FROM ice.ns.src"
        )
        assert parquet_leaf(spark, table, "st", "v") == [WALL_TICK]
        spark.sql(f"UPDATE {table} SET st.v = c WHERE id >= 0")
        assert parquet_leaf(spark, table, "st", "v") == [SESSION_WALL["America/New_York"]]
    finally:
        spark.stop()


DST_INSTANTS = (
    ("America/New_York", "TIMESTAMP '2026-03-08 07:00:00+00:00'", 1_772_938_800_000_000),
    ("America/New_York", "TIMESTAMP '2026-11-01 05:30:00+00:00'", 1_793_496_600_000_000),
    ("Australia/Lord_Howe", "TIMESTAMP '2026-10-03 15:30:00+00:00'", 1_791_081_000_000_000),
    ("Australia/Lord_Howe", "TIMESTAMP '2026-04-04 14:45:00+00:00'", 1_775_353_500_000_000),
)


def test_dst_instants_store_the_session_wall(tmp_path: Path) -> None:
    """A gap instant and an overlap instant store the session wall."""
    for index, (zone, literal, ticks) in enumerate(DST_INSTANTS):
        spark = open_session(zone, tmp_path / f"z{index}")
        try:
            table = "ice.ns.dst"
            create_nested(spark, table, "TIMESTAMP_NTZ")
            spark.sql(f"INSERT INTO {table} SELECT 1, named_struct('v', {literal}, 'n', 1), 0")
            assert leaf_ticks(spark, table, "st.v") == [ticks], (zone, literal)
        finally:
            spark.stop()


JULY = "TIMESTAMP '2026-07-04 11:22:33.654321+00:00'"
JULY_PHYSICAL = 1_783_164_153_654_321
JULY_NY = 1_783_149_753_654_321
SHIFTED_WALL = 1_767_305_045_123_456


def test_mixed_case_wall_row_stays_1767305045123456_until_coercion_unit_lands(
    tmp_path: Path,
) -> None:
    """The wall row of the first mixed CASE stays the shifted wall.

    America/New_York. ``UPDATE t SET st = CASE WHEN id = 1 THEN
    named_struct('v', c, 'n', 100) ELSE st END WHERE id >= 0``. The row that
    keeps the ``TIMESTAMP_NTZ`` wall stores ``1767305045123456``. That value
    must change when the struct coercion unit lands. This unit does not move it.
    """
    spark = open_session("America/New_York", tmp_path)
    try:
        spark.sql(
            "CREATE TABLE ice.ns.t "
            "(id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT, c TIMESTAMP) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(
            "INSERT INTO ice.ns.t VALUES "
            f"(1, named_struct('v', {WALL}, 'n', 1), 0, {JULY}), "
            f"(2, named_struct('v', {WALL}, 'n', 2), 0, {JULY})"
        )
        spark.sql(
            "UPDATE ice.ns.t SET st = CASE WHEN id = 1 "
            "THEN named_struct('v', c, 'n', 100) ELSE st END WHERE id >= 0"
        )
        assert stored_rows(spark, "ice.ns.t") == [
            (1, JULY_NY, 100),
            (2, SHIFTED_WALL, 2),
        ]
    finally:
        spark.stop()


def _id_ticks(spark: Any, table: str, path: tuple[str, ...] = ("v",)) -> list[int | None]:
    files = spark.sql(f"SELECT content, file_path FROM {table}.files").toArrow().to_pylist()
    pairs: list[tuple[int, int | None]] = []
    for file in files:
        if file["content"] != 0:
            continue
        loaded = pq.read_table(file["file_path"], columns=["id", "st"])
        leaf = loaded.column("st").combine_chunks()
        for name in path:
            leaf = leaf.field(name)
        ticks = leaf.cast(pa.int64(), safe=False).to_pylist()
        pairs.extend(zip(loaded.column("id").to_pylist(), ticks, strict=True))
    pairs.sort()
    return [tick for _, tick in pairs]


def _ice(spark: Any, name: str, columns: str) -> None:
    spark.sql(f"CREATE TABLE {name} ({columns}) {_V2}")


def test_case_different_members_store_mains_positional_utc_wall(tmp_path: Path) -> None:
    """Case-different BY NAME and OVERWRITE store main's positional UTC wall."""
    shapes = (
        ("named_struct('V', l, 'W', l2)", WALL_TICK, JULY_PHYSICAL),
        ("named_struct('W', l2, 'V', l)", JULY_PHYSICAL, WALL_TICK),
        ("named_struct('W', l2, 'v', l)", JULY_PHYSICAL, WALL_TICK),
    )
    doors = (
        "INSERT INTO {t} BY NAME SELECT id, {e} AS st, 0 AS k FROM ice.ns.src",
        "INSERT OVERWRITE {t} SELECT id, {e} AS st, 0 AS k FROM ice.ns.src",
    )
    for zone in ("UTC", "America/New_York"):
        for sensitive in (False, True):
            spark = open_session(zone, tmp_path / f"{zone}-{int(sensitive)}")
            try:
                spark.sql(f"SET spark.sql.caseSensitive={str(sensitive).lower()}")
                _ice(spark, "ice.ns.src", "id INT, l TIMESTAMP, l2 TIMESTAMP")
                spark.sql(f"INSERT INTO ice.ns.src VALUES (1, {INSTANT}, {JULY})")
                for index, (expr, left, right) in enumerate(shapes):
                    for door, sql in enumerate(doors):
                        table = f"ice.ns.c{index}{door}{int(sensitive)}"
                        cols = "id INT, st STRUCT<v: TIMESTAMP_NTZ, w: TIMESTAMP_NTZ>, k INT"
                        _ice(spark, table, cols)
                        spark.sql(sql.format(t=table, e=expr))
                        found = (leaf_ticks(spark, table, "st.v"), leaf_ticks(spark, table, "st.w"))
                        assert found == ([left], [right]), (zone, sensitive, expr)
            finally:
                spark.stop()


def test_not_null_members_store_on_the_bare_kernel_doors(tmp_path: Path) -> None:
    """Five NOT NULL layouts by seven doors store Spark's wall where the leaf converts."""
    leaf = "named_struct('v', l, 'n', id)"
    nested = "named_struct('i', named_struct('v', l, 'n', id), 'm', 5)"
    ntz = "TIMESTAMP_NTZ"
    layouts = (
        (f"STRUCT<v: {ntz} NOT NULL, n: INT NOT NULL>", "srcst", ("v",), leaf),
        (f"STRUCT<v: {ntz} NOT NULL, n: INT>", "srcst", ("v",), leaf),
        (f"STRUCT<v: {ntz}, n: INT NOT NULL>", "srcst", ("v",), leaf),
        (
            f"STRUCT<i: STRUCT<v: {ntz} NOT NULL, n: INT NOT NULL>, m: INT>",
            "srcst2",
            ("i", "v"),
            nested,
        ),
        (f"STRUCT<i: STRUCT<v: {ntz}, n: INT NOT NULL>, m: INT>", "srcst2", ("i", "v"), nested),
    )
    expects = (("UTC", [WALL_TICK, JULY_PHYSICAL]), ("America/New_York", [SHIFTED_WALL, JULY_NY]))
    for zone, expected in expects:
        spark = open_session(zone, tmp_path / zone)
        try:
            _ice(spark, "ice.ns.src", "id INT, l TIMESTAMP")
            spark.sql(f"INSERT INTO ice.ns.src VALUES (1, {INSTANT}), (2, {JULY})")
            _ice(spark, "ice.ns.srcst", "id INT, st STRUCT<v: TIMESTAMP, n: INT>, k INT")
            spark.sql(
                "INSERT INTO ice.ns.srcst SELECT id, "
                "named_struct('v', l, 'n', id), 0 FROM ice.ns.src"
            )
            inner = "id INT, st STRUCT<i: STRUCT<v: TIMESTAMP, n: INT>, m: INT>, k INT"
            _ice(spark, "ice.ns.srcst2", inner)
            spark.sql(
                "INSERT INTO ice.ns.srcst2 SELECT id, "
                "named_struct('i', named_struct('v', l, 'n', id), 'm', 5), 0 FROM ice.ns.src"
            )
            index = 0
            for struct, source, path, built in layouts:
                named = built.replace("l,", "s.l,").replace("id)", "s.id)")
                using = f"ice.ns.{source}"
                matched = f"MERGE INTO {{t}} t USING {using} s ON t.id = s.id "
                statements = [
                    f"INSERT INTO {{t}} BY NAME SELECT k, st, id FROM {using}",
                    f"INSERT INTO {{t}} BY NAME SELECT 0 AS k, {built} AS st, id FROM ice.ns.src",
                    f"INSERT OVERWRITE {{t}} SELECT id, st, k FROM {using}",
                    matched + "WHEN NOT MATCHED THEN INSERT (id, st, k) VALUES (s.id, s.st, s.k)",
                    matched + "WHEN NOT MATCHED THEN INSERT *",
                    (
                        "MERGE INTO {t} t USING ice.ns.src s ON t.id = s.id WHEN NOT "
                        f"MATCHED THEN INSERT (id, st, k) VALUES (s.id, {named}, 0)"
                    ),
                    None,
                ]
                for sql in statements:
                    index += 1
                    table = f"ice.ns.n{index}"
                    _ice(spark, table, f"id INT, st {struct}, k INT")
                    if sql is None:
                        spark.table(using).writeTo(table).overwritePartitions()
                    else:
                        spark.sql(sql.format(t=table))
                    assert _id_ticks(spark, table, path) == expected, (zone, index, struct)
        finally:
            spark.stop()


def test_scalar_subquery_update_raises_mains_type_error(tmp_path: Path) -> None:
    """Instant-struct scalar subquery raises main's type error; equal types stay a no-op."""
    line = (
        "datafusion engine error: Arrow error: Invalid argument error: "
        "arguments need to have the same data type"
    )
    spark = open_session("America/New_York", tmp_path)
    try:
        _ice(spark, "ice.ns.src", "id INT, st STRUCT<v: TIMESTAMP, n: INT>, k INT")
        spark.sql(f"INSERT INTO ice.ns.src VALUES (1, named_struct('v', {INSTANT}, 'n', 1), 0)")
        create_nested(spark, "ice.ns.t", "TIMESTAMP_NTZ")
        spark.sql(
            f"INSERT INTO ice.ns.t VALUES (1, named_struct('v', {WALL}, 'n', 77), 9), "
            f"(2, named_struct('v', {WALL}, 'n', 78), 9)"
        )
        before = stored_rows(spark, "ice.ns.t")
        with pytest.raises(Exception) as caught:
            spark.sql(
                "UPDATE ice.ns.t SET st = (SELECT st FROM ice.ns.src WHERE id = 1) WHERE id >= 0"
            )
        assert type(caught.value).__name__ == "PySparkException"
        assert error_line(caught.value) == line
        assert stored_rows(spark, "ice.ns.t") == before
        _ice(spark, "ice.ns.wsrc", "id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>")
        spark.sql(f"INSERT INTO ice.ns.wsrc VALUES (1, named_struct('v', {WALL}, 'n', 1))")
        create_nested(spark, "ice.ns.w", "TIMESTAMP_NTZ")
        spark.sql(f"INSERT INTO ice.ns.w VALUES (1, named_struct('v', {WALL}, 'n', 77), 9)")
        spark.sql(
            "UPDATE ice.ns.w SET st = (SELECT st FROM ice.ns.wsrc WHERE id = 1) WHERE id >= 0"
        )
        assert stored_rows(spark, "ice.ns.w") == [(1, WALL_TICK, 77)]
    finally:
        spark.stop()


def _new_leaf(root: Path, before: set[Path]) -> list[int | None]:
    ticks: list[int | None] = []
    for found in sorted(set(root.rglob("*.parquet")) - before):
        loaded = pq.read_table(found)
        if "st" not in loaded.column_names:
            continue
        column = loaded.column("st").combine_chunks().field("v")
        ticks.extend(column.cast(pa.int64(), safe=False).to_pylist())
    return ticks


def test_branch_merge_update_stores_the_session_wall(tmp_path: Path) -> None:
    """Whole-struct MERGE UPDATE into a branch stores the session wall."""
    spark = open_session("America/New_York", tmp_path)
    try:
        write_source(spark)
        cols = "id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT, c TIMESTAMP"
        _ice(spark, "ice.ns.b", cols)
        spark.sql(
            "INSERT INTO ice.ns.b SELECT 1, "
            "named_struct('v', CAST(NULL AS TIMESTAMP_NTZ), 'n', 0), 0, l FROM ice.ns.src"
        )
        spark.sql("ALTER TABLE ice.ns.b CREATE BRANCH dev")
        before = set(tmp_path.rglob("*.parquet"))
        spark.sql(
            "MERGE INTO ice.ns.b.branch_dev t USING ice.ns.src s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.st = named_struct('v', s.l, 'n', 5)"
        )
        assert _new_leaf(tmp_path, before) == [SHIFTED_WALL]
        spark.sql("ALTER TABLE ice.ns.b CREATE BRANCH wap")
        spark.sql("SET spark.wap.branch=wap")
        before = set(tmp_path.rglob("*.parquet"))
        spark.sql(
            "MERGE INTO ice.ns.b t USING ice.ns.src s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.st = named_struct('v', s.l, 'n', 5)"
        )
        assert _new_leaf(tmp_path, before) == [SHIFTED_WALL]
    finally:
        spark.stop()


def test_punctuation_names_and_a_renamed_sibling_stay_at_mains_values(tmp_path: Path) -> None:
    """Dotted names stay at the UTC wall, and a renamed sibling stays main's row."""
    spark = open_session("America/New_York", tmp_path)
    try:
        write_source(spark)
        odd = (
            "id INT, st STRUCT<`a.b`: TIMESTAMP_NTZ, `c,d`: TIMESTAMP_NTZ, v: TIMESTAMP_NTZ>, k INT"
        )
        _ice(spark, "ice.ns.odd", odd)
        spark.sql(
            "INSERT INTO ice.ns.odd SELECT id, "
            "named_struct('a.b', l, 'c,d', l, 'v', l), 0 FROM ice.ns.src"
        )
        path = next(
            file["file_path"]
            for file in spark.sql("SELECT content, file_path FROM ice.ns.odd.files")
            .toArrow()
            .to_pylist()
            if file["content"] == 0
        )
        struct = pq.read_table(path, columns=["st"]).column("st").combine_chunks()
        assert struct.field("a.b").cast(pa.int64(), safe=False).to_pylist() == [WALL_TICK]
        assert struct.field("c,d").cast(pa.int64(), safe=False).to_pylist() == [WALL_TICK]
        assert struct.field("v").cast(pa.int64(), safe=False).to_pylist() == [SHIFTED_WALL]
        _ice(spark, "ice.ns.src2", "id INT, l TIMESTAMP, l2 TIMESTAMP")
        spark.sql(f"INSERT INTO ice.ns.src2 VALUES (1, {INSTANT}, {JULY})")
        _ice(spark, "ice.ns.ren", "id INT, st STRUCT<v: TIMESTAMP_NTZ, w: TIMESTAMP_NTZ>, k INT")
        spark.sql(
            "INSERT INTO ice.ns.ren SELECT id, named_struct('v', l, 'x', l2), 0 FROM ice.ns.src2"
        )
        assert leaf_ticks(spark, "ice.ns.ren", "st.v") == [WALL_TICK]
        assert leaf_ticks(spark, "ice.ns.ren", "st.w") == [None]
    finally:
        spark.stop()


def _mask_plan(text: str) -> str:
    """Hide snapshot ids and partition counts so two warehouses compare."""
    text = re.sub(r"snapshot_id=\d+", "snapshot_id=#", text)
    return re.sub(r"N=\d+", "N=#", text)


def _explain_text(spark: Any, sql: str) -> str:
    """Return EXPLAIN text, or the error class and its first line."""
    try:
        rows = spark.sql(sql).toArrow().to_pylist()
    except Exception as error:
        return f"{type(error).__name__}: {error_line(error)}"
    return "\n".join(str(row) for row in rows)


def _explain_setup(spark: Any) -> None:
    """Build the tables the recorded EXPLAIN texts were planned against."""
    _ice(spark, "ice.ns.src", "id INT, l TIMESTAMP")
    spark.sql(f"INSERT INTO ice.ns.src VALUES (1, {INSTANT})")
    _ice(spark, "ice.ns.t", "id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT")
    spark.sql(f"INSERT INTO ice.ns.t VALUES (1, named_struct('v', {WALL}, 'n', 77), 9)")
    _ice(spark, "ice.ns.sib", "id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT, c TIMESTAMP")
    spark.sql(
        "INSERT INTO ice.ns.sib SELECT 1, "
        "named_struct('v', CAST(NULL AS TIMESTAMP_NTZ), 'n', 0), 0, l FROM ice.ns.src"
    )
    _ice(spark, "ice.ns.wallsrc", "id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT")
    spark.sql(f"INSERT INTO ice.ns.wallsrc VALUES (1, named_struct('v', {WALL}, 'n', 1), 0)")
    _ice(spark, "ice.ns.flat", "id INT, n INT")
    spark.sql("INSERT INTO ice.ns.flat VALUES (1, 2)")
    spark.sql(f"CREATE TABLE ice.ns.ns (id INT, ts timestamp_ns) {_V2.replace('2', '3')}")
    _ice(spark, "ice.ns.srcst", "id INT, st STRUCT<v: TIMESTAMP, n: INT>, k INT")
    spark.sql("INSERT INTO ice.ns.srcst SELECT id, named_struct('v', l, 'n', 1), 0 FROM ice.ns.src")


def _marks(spark: Any, table: str) -> tuple[Any, Any, Any]:
    """Snapshot id, metadata files, and rows. An EXPLAIN must leave all three."""
    snap = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").toArrow().to_pylist()
    meta = spark.sql(f"SELECT file FROM {table}.metadata_log_entries").toArrow().to_pylist()
    rows = spark.sql(f"SELECT * FROM {table}").toArrow().to_pylist()
    return snap, meta, rows


def test_explain_pins(tmp_path: Path) -> None:
    """Five EXPLAIN pins. Unconverted text was recorded with the arm absent."""
    recorded = json.loads(
        Path(__file__).with_name("ice_ntz_nested_wall_1_explain.json").read_text()
    )
    spark = open_session("America/New_York", tmp_path)
    try:
        _explain_setup(spark)
        for item in recorded["plans"]:
            got = _mask_plan(_explain_text(spark, item["sql"]))
            assert got == _mask_plan(item["text"]), item["name"]
        kernel_sql = (
            "EXPLAIN INSERT INTO ice.ns.t SELECT id, st, k FROM ice.ns.srcst",
            "EXPLAIN UPDATE ice.ns.sib SET st = named_struct('v', c, 'n', 1)",
        )
        for sql in kernel_sql:
            text = _explain_text(spark, sql)
            assert "__repark_cast_nested_ntz__" in text, text
            assert "'v'" in text or 'Utf8("v")' in text, text
        before = (_marks(spark, "ice.ns.t"), _marks(spark, "ice.ns.sib"))
        raised = (
            "EXPLAIN INSERT INTO ice.ns.t SELECT id, "
            "named_struct('v', l, 'n', assert_true(false)), 0 FROM ice.ns.src"
        )
        assert _explain_text(spark, raised).startswith("{'plan_type'")
        updated = "EXPLAIN UPDATE ice.ns.sib SET st = named_struct('v', c, 'n', assert_true(false))"
        assert _explain_text(spark, updated).startswith("{'plan_type'")
        assert (_marks(spark, "ice.ns.t"), _marks(spark, "ice.ns.sib")) == before
        _ice(spark, "ice.ns.leafsrc", "id INT, l TIMESTAMP, w TIMESTAMP_NTZ, i INT")
        spark.sql(f"INSERT INTO ice.ns.leafsrc VALUES (1, {INSTANT}, {WALL}, 4)")
        session = SESSION_WALL["America/New_York"]
        kinds = (("TIMESTAMP_NTZ", "l"), ("TIMESTAMP_NTZ", "w"), ("TIMESTAMP_NTZ", "i"))
        kinds += (("TIMESTAMP", "l"), ("TIMESTAMP", "w"), ("TIMESTAMP", "i"))
        kinds += (("INT", "l"), ("INT", "w"), ("INT", "i"))
        for index, (target, expr) in enumerate(kinds):
            table = f"ice.ns.p{index}"
            _ice(spark, table, f"id INT, st STRUCT<v: {target}, n: INT>, k INT")
            statement = (
                f"INSERT INTO {table} SELECT id, named_struct('v', {expr}, 'n', 1), 0 "
                "FROM ice.ns.leafsrc"
            )
            shown = "__repark_cast_nested_ntz__" in _explain_text(spark, f"EXPLAIN {statement}")
            try:
                spark.sql(statement)
                stored = leaf_ticks(spark, table, "st.v") == [session]
            except Exception:
                stored = False
            assert shown == stored, (target, expr, shown, stored)
        cased = (
            "EXPLAIN INSERT INTO ice.ns.t SELECT id, named_struct('V', l, 'n', 1), 0 "
            "FROM ice.ns.src"
        )
        assert "__repark_cast_nested_ntz__" not in _explain_text(spark, cased)
        sub = (
            "EXPLAIN UPDATE ice.ns.t SET st = (SELECT st FROM ice.ns.wallsrc WHERE id = 1) "
            "WHERE id >= 0"
        )
        assert "__repark_cast_nested_ntz__" not in _explain_text(spark, sub)
        _ice(spark, "ice.ns.an", "id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT")
        spark.sql(
            "EXPLAIN ANALYZE INSERT INTO ice.ns.an SELECT id, "
            "named_struct('v', l, 'n', 1), 0 FROM ice.ns.src"
        ).toArrow()
        assert leaf_ticks(spark, "ice.ns.an", "st.v") == [WALL_TICK]
    finally:
        spark.stop()
