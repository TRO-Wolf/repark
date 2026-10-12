"""ICE-NTZ-NESTED-WALL-1: a nested microsecond ``TIMESTAMP_NTZ`` leaf stores one wall.

The doors wired so far are the ones ``zone_store_frame`` serves (``INSERT … BY NAME``,
``INSERT OVERWRITE … SELECT``, ``overwritePartitions``, ``insertInto(overwrite=True)``),
the MERGE INSERT doors ``zone_wrapping_stream_sql`` serves, and the whole-struct
assignment doors ``store_assignment_cast_sql`` serves when the caller passes an instant
source type (MERGE UPDATE from a column, UPDATE from a sibling ``TIMESTAMP`` column)
and the identity UPDATE ``update_projection_sql`` serves for that same sibling column,
and plain ``INSERT … SELECT`` plus DataFrame ``append`` through
``after_analysis``.
An instant stores the session wall. A wall literal stores ``1767323045123456`` in every
zone. A nested ``TIMESTAMP`` leaf stores the instant. A caller with no source type
passes ``None`` and the expression stays main's.

pins: ice-ntz-nested-wall-1/C-001, C-002, C-003, C-004
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession

ZONES = ("UTC", "America/New_York", "Asia/Kolkata")
SESSION_WALL = {
    "UTC": 1_767_323_045_123_456,
    "America/New_York": 1_767_305_045_123_456,
    "Asia/Kolkata": 1_767_342_845_123_456,
}
INSTANT = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'"
WALL = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"
WALL_TICK = 1_767_323_045_123_456


def open_session(zone: str, warehouse: Path) -> Any:
    """Open one session in ``zone`` with a private memory catalog."""
    spark = (
        ReparkSession.builder.appName("ice-ntz-nested-wall-1")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    return spark


def leaf_ticks(spark: Any, table: str, leaf: str) -> list[int | None]:
    """Return the stored microseconds of ``leaf``, from ``toArrow`` cast to int64."""
    arrow = spark.sql(f"SELECT {leaf} AS v FROM {table}").toArrow()
    column = arrow.column("v").combine_chunks()
    return column.cast(pa.int64(), safe=False).to_pylist()


def create_nested(spark: Any, table: str, leaf_type: str) -> None:
    """Create a v2 table whose struct leaf is ``leaf_type``."""
    spark.sql(
        f"CREATE TABLE {table} (id INT, st STRUCT<v: {leaf_type}, n: INT>, k INT) "
        "USING iceberg TBLPROPERTIES ('format-version'='2')"
    )


def write_source(spark: Any) -> None:
    """Write the instant into ``ice.ns.src.l``."""
    spark.sql(
        "CREATE TABLE ice.ns.src (id INT, l TIMESTAMP) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(f"INSERT INTO ice.ns.src VALUES (1, {INSTANT})")


def by_name_from_column(spark: Any, table: str) -> None:
    """``INSERT … BY NAME`` of the source column."""
    spark.sql(
        f"INSERT INTO {table} BY NAME SELECT 0 AS k, named_struct('v', l, 'n', 1) AS st, id "
        "FROM ice.ns.src"
    )


def by_name_literal(spark: Any, table: str, literal: str) -> None:
    """``INSERT … BY NAME`` of one literal."""
    spark.sql(
        f"INSERT INTO {table} BY NAME SELECT 0 AS k, named_struct('v', {literal}, 'n', 1) AS st, "
        "1 AS id"
    )


def seed_null(spark: Any, table: str) -> None:
    """Write one null struct so an overwrite has a row to replace."""
    spark.sql(
        f"INSERT INTO {table} VALUES (1, named_struct('v', CAST(NULL AS TIMESTAMP_NTZ), 'n', 0), 0)"
    )


def overwrite_select(spark: Any, table: str) -> None:
    """``INSERT OVERWRITE … SELECT`` of the source column."""
    seed_null(spark, table)
    spark.sql(
        f"INSERT OVERWRITE {table} SELECT id, named_struct('v', l, 'n', 1) AS st, 0 AS k "
        "FROM ice.ns.src"
    )


def overwrite_partitions(spark: Any, table: str) -> None:
    """DataFrame ``overwritePartitions`` of the source column."""
    seed_null(spark, table)
    frame = spark.sql("SELECT id, named_struct('v', l, 'n', 1) AS st, 0 AS k FROM ice.ns.src")
    frame.writeTo(table).overwritePartitions()


def insert_into_overwrite(spark: Any, table: str) -> None:
    """DataFrame ``insertInto(overwrite=True)`` of the source column."""
    seed_null(spark, table)
    frame = spark.sql("SELECT id, named_struct('v', l, 'n', 1) AS st, 0 AS k FROM ice.ns.src")
    frame.write.insertInto(table, overwrite=True)


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
    """Return the ``st`` column as Python values from ``toArrow``."""
    arrow = spark.sql(f"SELECT st AS v FROM {table}").toArrow()
    return arrow.column("v").to_pylist()


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
    """Renamed, reordered, extra and case-different members follow main's cast.

    A renamed member is not selected, so main's cast stores NULL in that leaf.
    Reordered names are selected and main's cast puts each session wall on its
    name. An extra source field is dropped. A case-different name under
    ``spark.sql.caseSensitive=true`` does not match, so that leaf is NULL.
    """
    spark = open_session("America/New_York", tmp_path)
    try:
        spark.sql("SET spark.sql.caseSensitive=true")
        other = "TIMESTAMP '2024-01-01 12:00:00+00:00'"
        other_wall = session_wall_of(spark, other)
        instant_wall = session_wall_of(spark, INSTANT)

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
    """A 20-field struct from one volatile subquery keeps one draw.

    ``f00`` equals ``f17`` and differs from ``f18``. ``EXPLAIN`` has no
    ``rand``. DataFrame ``append`` stores the same coupling. Main stores one
    draw because it does not rebuild the struct.
    """
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
    """Spark MERGE UPDATE stores the session wall before ``store_assignment_then_sql``.

    America/New_York, a ``TIMESTAMP`` column into ``struct<v: TIMESTAMP_NTZ, n: INT>``.
    ``nested_assign`` probes the source and wraps it. Main's matched cell is the
    UTC wall ``1767323045123456``. Spark stores ``1767305045123456``. The
    not-matched-by-source cell is not in the step-0 matrix; Spark's rule for this
    assignment is the same session wall.
    """
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
    """``UPDATE`` with no ``WHERE`` and ``INSERT OVERWRITE … VALUES`` raise main's error.

    UTC, ``ntz_nested``, a ``TIMESTAMP`` literal, copy-on-write. The class and the
    first line are the step-0 text at ``2c33f289``. Spark stores the session wall
    on both doors.
    """
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
    """A struct under a list stays the physical instant, which is main's cast.

    The walk does not enter a list. America/New_York stores
    ``1767323045123456``, not the session wall. A map of structs panics in
    Arrow's list cast before a store; the walk does not enter a map either.
    """
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
