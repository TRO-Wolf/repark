"""Pins for a struct coerced from a naive wall to an instant.

Spark 4.1.2 localises the wall leaf ``2026-01-02 03:04:05.123456`` in the session
zone when ``CASE``, ``COALESCE`` or ``UNION ALL`` mixes it with a ``TIMESTAMP``
struct: UTC ``1767323045123456``, America/New_York ``1767341045123456``,
Asia/Kolkata ``1767303245123456``. The July instant is ``1783164153654321`` in
every zone. Those ticks are the step-0 recording of main ``572130dd``
(``task/wo/ntz-struct-coerce-1/evidence/repark-main.jsonl``) and of Spark 4.1.2
(``spark-4.1.2.jsonl``).

A user-written ``CAST(st AS STRUCT<v: TIMESTAMP, n: INT>)`` is planned here as
``timestamp[ns]`` with the UTC wall scaled by 1000 (``1767323045123456000``).
Spark localises that cast to the microsecond ticks above. This file pins the
nanosecond residue and does not claim Spark's value for that shape. Its write
read-back stays the microsecond UTC wall ``1767323045123456`` in every zone;
Spark's write is the localised microsecond tick in America/New_York and
Asia/Kolkata.

pins: ntz-struct-coerce-1/C-001, C-002, C-003, C-004
"""

from __future__ import annotations

import contextlib
import io
from pathlib import Path
from typing import Any

import pyarrow as pa

from repark import ReparkSession

WALL_AS_UTC: int = 1_767_323_045_123_456
INSTANT: int = 1_783_164_153_654_321
LOCAL: dict[str, int] = {
    "UTC": WALL_AS_UTC,
    "America/New_York": 1_767_341_045_123_456,
    "Asia/Kolkata": 1_767_303_245_123_456,
}
ZONES: tuple[str, ...] = ("UTC", "America/New_York", "Asia/Kolkata")
KERNEL: str = "__repark_localize_nested_ntz__"
INSTANT_SQL: str = "TIMESTAMP '2026-07-04 11:22:33.654321+00:00'"
WALL_SQL: str = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"
FIXED_STRUCT: str = f"named_struct('v', {INSTANT_SQL}, 'n', CAST(7 AS INT))"


def _open(zone: str, warehouse: Path) -> ReparkSession:
    """Open one facade session on a fresh memory catalog."""
    from repark.spark.session.session_state import _reset_active_session_for_tests

    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("ntz-struct-coerce-1")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    session.register_memory_catalog("sc", str(warehouse))
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    return session


def _close(session: ReparkSession) -> None:
    """Stop the session and drop the process-wide handle."""
    from repark.spark.session.session_state import _reset_active_session_for_tests

    session.stop()
    _reset_active_session_for_tests()


def _exec(session: ReparkSession, sql: str) -> None:
    """Run one statement."""
    session.sql(sql).collect()


def _install(session: ReparkSession) -> None:
    """Create the wall struct, the instant struct, and the two top-level twins."""
    statements = [
        "DROP TABLE IF EXISTS sc.ns.a",
        "DROP TABLE IF EXISTS sc.ns.b",
        "DROP TABLE IF EXISTS sc.ns.at",
        "DROP TABLE IF EXISTS sc.ns.bt",
        "CREATE TABLE sc.ns.a (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>) USING iceberg",
        "CREATE TABLE sc.ns.b (id INT, st STRUCT<v: TIMESTAMP, n: INT>) USING iceberg",
        "CREATE TABLE sc.ns.at (id INT, v TIMESTAMP_NTZ) USING iceberg",
        "CREATE TABLE sc.ns.bt (id INT, v TIMESTAMP) USING iceberg",
        f"INSERT INTO sc.ns.a SELECT 1, named_struct('v', {WALL_SQL}, 'n', CAST(7 AS INT))",
        "INSERT INTO sc.ns.a SELECT 2, NULL",
        f"INSERT INTO sc.ns.b SELECT 1, named_struct('v', {INSTANT_SQL}, 'n', CAST(7 AS INT))",
        f"INSERT INTO sc.ns.b SELECT 2, named_struct('v', {INSTANT_SQL}, 'n', CAST(7 AS INT))",
        f"INSERT INTO sc.ns.at SELECT 1, {WALL_SQL}",
        "INSERT INTO sc.ns.at SELECT 2, CAST(NULL AS TIMESTAMP_NTZ)",
        f"INSERT INTO sc.ns.bt SELECT 1, {INSTANT_SQL}",
        f"INSERT INTO sc.ns.bt SELECT 2, {INSTANT_SQL}",
    ]
    for sql in statements:
        _exec(session, sql)


def _plain(array: pa.Array, data_type: pa.DataType) -> list[Any]:
    """Reduce one Arrow column to ticks, structs of ticks, or Python values."""
    if pa.types.is_timestamp(data_type):
        return [None if item is None else int(item) for item in array.cast(pa.int64()).to_pylist()]
    if pa.types.is_struct(data_type):
        children = {
            data_type.field(index).name: _plain(array.field(index), data_type.field(index).type)
            for index in range(data_type.num_fields)
        }
        rows: list[Any] = []
        for index in range(len(array)):
            if array.is_null(index):
                rows.append(None)
                continue
            rows.append({name: values[index] for name, values in children.items()})
        return rows
    return array.to_pylist()


def _table(session: ReparkSession, sql: str) -> tuple[pa.Schema, list[dict[str, Any]]]:
    """Collect one query as an Arrow schema and one dict per row."""
    arrow = session.sql(sql).to_arrow()
    columns = {
        name: _plain(arrow.column(name).combine_chunks(), arrow.schema.field(name).type)
        for name in arrow.schema.names
    }
    width = len(next(iter(columns.values()))) if columns else 0
    rows = [{name: values[index] for name, values in columns.items()} for index in range(width)]
    return arrow.schema, rows


def _plan(session: ReparkSession, sql: str) -> str:
    """Return the extended explain text of one query."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        session.sql(sql).explain(extended=True)
    return buffer.getvalue()


def _shape_sql(level: str, shape: str) -> str:
    """Return one mixing query from the step-0 inventory."""
    if level == "struct" and shape == "case":
        return (
            "SELECT a.id AS id, CASE WHEN a.id = 2 THEN b.st ELSE a.st END AS mixed "
            "FROM sc.ns.a a JOIN sc.ns.b b ON a.id = b.id"
        )
    if level == "struct" and shape == "coalesce":
        return (
            "SELECT a.id AS id, COALESCE(a.st, b.st) AS mixed "
            "FROM sc.ns.a a JOIN sc.ns.b b ON a.id = b.id"
        )
    if level == "struct" and shape == "union":
        return "SELECT st AS mixed FROM sc.ns.a UNION ALL SELECT st AS mixed FROM sc.ns.b"
    if level == "struct" and shape == "cast":
        return "SELECT id, CAST(st AS STRUCT<v: TIMESTAMP, n: INT>) AS mixed FROM sc.ns.a"
    if level == "top" and shape == "case":
        return (
            "SELECT a.id AS id, CASE WHEN a.id = 2 THEN b.v ELSE a.v END AS mixed "
            "FROM sc.ns.at a JOIN sc.ns.bt b ON a.id = b.id"
        )
    raise AssertionError(shape)


def _sorted(rows: list[Any]) -> list[Any]:
    """Order rows by their representation so an unordered answer can be compared."""
    return sorted(rows, key=repr)


def _check_coercion(session: ReparkSession, shape: str, wall: int) -> None:
    """Assert one coercion shape localises on read, order, group, write, compare and join."""
    inner = _shape_sql("struct", shape)
    has_id = shape != "union"
    ident = "id, " if has_id else ""
    order_id = " ORDER BY id" if has_id else ""
    schema, rows = _table(session, f"SELECT {ident}mixed.v AS v FROM ({inner}) s{order_id}")
    assert str(schema.field("v").type) == "timestamp[us, tz=UTC]", shape
    if has_id:
        assert [(row["id"], row["v"]) for row in rows] == [(1, wall), (2, INSTANT)], shape
    else:
        assert _sorted([row["v"] for row in rows]) == _sorted([wall, None, INSTANT, INSTANT]), shape
    _, ordered = _table(session, f"SELECT mixed.v AS v FROM ({inner}) s ORDER BY mixed.v")
    if has_id:
        assert [row["v"] for row in ordered] == [wall, INSTANT], shape
    else:
        assert [row["v"] for row in ordered] == [None, wall, INSTANT, INSTANT], shape
    _, grouped = _table(
        session, f"SELECT mixed AS g, count(*) AS c FROM ({inner}) s GROUP BY mixed"
    )
    pairs = _sorted(
        [(None if row["g"] is None else row["g"]["v"], int(row["c"])) for row in grouped]
    )
    if has_id:
        assert pairs == _sorted([(wall, 1), (INSTANT, 1)]), shape
    else:
        assert pairs == _sorted([(wall, 1), (INSTANT, 2), (None, 1)]), shape
    dest = f"sc.ns.w_{shape}"
    _exec(session, f"DROP TABLE IF EXISTS {dest}")
    if has_id:
        _exec(
            session,
            f"CREATE TABLE {dest} (id INT, st STRUCT<v: TIMESTAMP, n: INT>) USING iceberg",
        )
        _exec(session, f"INSERT INTO {dest} SELECT id, mixed FROM ({inner}) s")
        written_schema, written = _table(session, f"SELECT id, st.v AS v FROM {dest} ORDER BY id")
        assert [(row["id"], row["v"]) for row in written] == [(1, wall), (2, INSTANT)], shape
    else:
        _exec(session, f"CREATE TABLE {dest} (st STRUCT<v: TIMESTAMP, n: INT>) USING iceberg")
        _exec(session, f"INSERT INTO {dest} SELECT mixed FROM ({inner}) s")
        written_schema, written = _table(session, f"SELECT st.v AS v FROM {dest}")
        assert _sorted([row["v"] for row in written]) == _sorted([wall, None, INSTANT, INSTANT]), (
            shape
        )
    assert str(written_schema.field("v").type) == "timestamp[us, tz=UTC]", shape
    same_sql = f"SELECT {ident}(mixed = {FIXED_STRUCT}) AS same FROM ({inner}) s{order_id}"
    _, compared = _table(session, same_sql)
    if has_id:
        assert [(row["id"], row["same"]) for row in compared] == [(1, False), (2, True)], shape
    else:
        assert _sorted([row["same"] for row in compared]) == _sorted([False, None, True, True]), (
            shape
        )
    _, joined = _table(
        session,
        f"SELECT s.mixed.v AS v, b.id AS bid FROM ({inner}) s "
        f"JOIN sc.ns.b b ON s.mixed = b.st ORDER BY v, bid",
    )
    joined_rows = [(row["v"], row["bid"]) for row in joined]
    if has_id:
        assert joined_rows == [(INSTANT, 1), (INSTANT, 2)], shape
    else:
        assert joined_rows == [(INSTANT, 1), (INSTANT, 1), (INSTANT, 2), (INSTANT, 2)], shape
    plan = _plan(session, inner)
    assert KERNEL in plan, shape


def _check_top(session: ReparkSession, wall: int) -> None:
    """Assert the top-level control still localises through ``to_timestamp``."""
    inner = _shape_sql("top", "case")
    schema, rows = _table(session, f"SELECT id, mixed AS v FROM ({inner}) s ORDER BY id")
    assert str(schema.field("v").type) == "timestamp[us, tz=UTC]"
    assert [(row["id"], row["v"]) for row in rows] == [(1, wall), (2, INSTANT)]
    plan = _plan(session, inner)
    assert "to_timestamp" in plan, plan
    assert KERNEL not in plan, plan


def _check_user_cast(session: ReparkSession) -> None:
    """Assert the user-written struct cast stays the nanosecond residue of main."""
    inner = _shape_sql("struct", "cast")
    schema, rows = _table(session, f"SELECT id, mixed.v AS v FROM ({inner}) s ORDER BY id")
    assert str(schema.field("v").type) == "timestamp[ns]", schema
    assert [(row["id"], row["v"]) for row in rows] == [(1, WALL_AS_UTC * 1000), (2, None)]
    _, ordered = _table(session, f"SELECT mixed.v AS v FROM ({inner}) s ORDER BY mixed.v")
    assert [row["v"] for row in ordered] == [None, WALL_AS_UTC * 1000]
    plan = _plan(session, inner)
    assert KERNEL not in plan, plan
    _exec(session, "DROP TABLE IF EXISTS sc.ns.w_cast")
    _exec(
        session,
        "CREATE TABLE sc.ns.w_cast (id INT, st STRUCT<v: TIMESTAMP, n: INT>) USING iceberg",
    )
    _exec(session, f"INSERT INTO sc.ns.w_cast SELECT id, mixed FROM ({inner}) s")
    written_schema, written = _table(session, "SELECT id, st.v AS v FROM sc.ns.w_cast ORDER BY id")
    assert str(written_schema.field("v").type) == "timestamp[us, tz=UTC]"
    assert [(row["id"], row["v"]) for row in written] == [(1, WALL_AS_UTC), (2, None)]


def test_mixed_struct_inventory_matches_spark_where_the_arm_applies(
    tmp_path: Path,
) -> None:
    """Replay the step-0 inventory in UTC, America/New_York and Asia/Kolkata."""
    for zone in ZONES:
        warehouse = tmp_path / zone
        warehouse.mkdir()
        session = _open(zone, warehouse)
        try:
            _install(session)
            wall = LOCAL[zone]
            for shape in ("case", "coalesce", "union"):
                _check_coercion(session, shape, wall)
            _check_top(session, wall)
            _check_user_cast(session)
        finally:
            _close(session)
