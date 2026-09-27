"""WO TZ-ASOF-1: an expression named like its ORDER BY key plans.

Spark names ``CAST(ts AS STRING)`` after its column (``ts``), so the scoreboard
cell ``E-TZ-TIMESTAMP-AS-OF`` reads ``SELECT CAST(committed_at AS STRING) FROM
t.snapshots ORDER BY committed_at`` and feeds the first row back as a
``TIMESTAMP AS OF`` literal. RePark refused the snapshots read with
``Projections require unique expression names`` because DataFusion appends the
un-projected sort key to the projection before the sort. This suite pins every
shape of that defect against Spark 4.1.2, measured 2026-09-26 on the probe
table ``sc.ns.y (id INT, ts TIMESTAMP, s STRING)`` with rows ``(2,
2024-01-02 00:00:00, 'b'), (1, 2024-01-01 00:00:00, 'a')`` under session zone
UTC, plus the timestamp rendering rule the ``AS OF`` leg depends on and the
cell's own replay shape.

pins: tz-asof-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from repark import ReparkSession


@pytest.fixture()
def session(tmp_path: Path) -> Iterator[ReparkSession]:
    """Open a UTC facade session over a fresh memory catalog warehouse."""
    opened = (
        ReparkSession.builder.appName("tz-asof-1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    opened.register_memory_catalog("sc", str(tmp_path))
    opened.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    try:
        yield opened
    finally:
        opened.stop()


def seed_y(session: ReparkSession) -> None:
    """Create the probe table with the two probe rows, newest first."""
    session.sql("CREATE TABLE sc.ns.y (id INT, ts TIMESTAMP, s STRING) USING iceberg")
    session.sql(
        "INSERT INTO sc.ns.y VALUES (2, TIMESTAMP '2024-01-02 00:00:00', 'b'), "
        "(1, TIMESTAMP '2024-01-01 00:00:00', 'a')"
    )


def seed_z(session: ReparkSession) -> None:
    """Create the struct-bearing probe table with its three probe rows."""
    session.sql(
        "CREATE TABLE sc.ns.z (id INT, ts TIMESTAMP, s STRING, "
        "st STRUCT<a: INT, s: STRING>) USING iceberg"
    )
    session.sql(
        "INSERT INTO sc.ns.z VALUES "
        "(10, TIMESTAMP '2024-01-02 00:00:00', 'b', named_struct('a', 1, 's', 'x')), "
        "(2, TIMESTAMP '2024-01-01 00:00:00', 'b', named_struct('a', 2, 's', 'z')), "
        "(3, TIMESTAMP '2024-01-03 00:00:00', 'a', named_struct('a', 3, 's', 'y'))"
    )


def observed(session: ReparkSession, sql: str) -> tuple[list[tuple[str, str]], list[list[object]]]:
    """Run sql and return its columns and rows in answer order."""
    frame = session.sql(sql)
    cols: list[tuple[str, str]] = [
        (field.name, field.dataType.simpleString()) for field in frame.schema.fields
    ]
    rows: list[list[object]] = [list(row) for row in frame.collect()]
    return cols, rows


def is_trimmed_stamp(text: object) -> bool:
    """Tell whether text is a Spark timestamp rendering without trailing zeros."""
    if not isinstance(text, str):
        return False
    if len(text) < 19 or not text.startswith("20"):
        return False
    if text[4] != "-" or text[7] != "-" or text[10] != " " or text[13] != ":" or text[16] != ":":
        return False
    if len(text) == 19:
        return True
    if not text[19:].startswith("."):
        return False
    fraction = text[20:]
    if not fraction or len(fraction) > 6 or not fraction.isdigit():
        return False
    return not fraction.endswith("0")


def test_cast_over_its_order_key_answers_like_spark(session: ReparkSession) -> None:
    """Unaliased cast over its own key orders and names like Spark.

    pins: tz-asof-1/C-001
    """
    seed_y(session)
    cols, rows = observed(session, "SELECT CAST(ts AS STRING) FROM sc.ns.y ORDER BY ts")
    assert cols == [("ts", "string")]
    assert rows == [["2024-01-01 00:00:00"], ["2024-01-02 00:00:00"]]


def test_cast_over_its_order_key_desc_reverses(session: ReparkSession) -> None:
    """DESC keeps the Spark name and reverses the rows.

    pins: tz-asof-1/C-001
    """
    seed_y(session)
    cols, rows = observed(session, "SELECT CAST(ts AS STRING) FROM sc.ns.y ORDER BY ts DESC")
    assert cols == [("ts", "string")]
    assert rows == [["2024-01-02 00:00:00"], ["2024-01-01 00:00:00"]]


def test_cast_over_its_order_key_with_limit_keeps_one_row(session: ReparkSession) -> None:
    """LIMIT applies after the sort on the renamed projection.

    pins: tz-asof-1/C-001
    """
    seed_y(session)
    cols, rows = observed(session, "SELECT CAST(ts AS STRING) FROM sc.ns.y ORDER BY ts LIMIT 1")
    assert cols == [("ts", "string")]
    assert rows == [["2024-01-01 00:00:00"]]


def test_cast_aliased_like_the_key_keeps_alias_sort(session: ReparkSession) -> None:
    """The alias shadows the column; Spark sorts by the alias here.

    pins: tz-asof-1/C-002
    """
    seed_y(session)
    cols, rows = observed(session, "SELECT CAST(ts AS STRING) AS ts FROM sc.ns.y ORDER BY ts")
    assert cols == [("ts", "string")]
    assert rows == [["2024-01-01 00:00:00"], ["2024-01-02 00:00:00"]]


def test_cast_aliased_apart_keeps_the_alias(session: ReparkSession) -> None:
    """An explicit alias wins over the key name.

    pins: tz-asof-1/C-002
    """
    seed_y(session)
    cols, rows = observed(session, "SELECT CAST(ts AS STRING) AS t2 FROM sc.ns.y ORDER BY ts")
    assert cols == [("t2", "string")]
    assert rows == [["2024-01-01 00:00:00"], ["2024-01-02 00:00:00"]]


def test_arithmetic_projection_names_the_paren_form(session: ReparkSession) -> None:
    """An unaliased sum over its key shows Spark's parenthesized name.

    pins: tz-asof-1/C-003
    """
    seed_y(session)
    cols, rows = observed(session, "SELECT id + 1 FROM sc.ns.y ORDER BY id")
    assert cols == [("(id + 1)", "int")]
    assert rows == [[2], [3]]


def test_function_projection_names_the_call_form(session: ReparkSession) -> None:
    """An unaliased call over its key shows Spark's call name.

    pins: tz-asof-1/C-003
    """
    seed_y(session)
    cols, rows = observed(session, "SELECT upper(s) FROM sc.ns.y ORDER BY s")
    assert cols == [("upper(s)", "string")]
    assert rows == [["A"], ["B"]]


def test_two_casts_over_two_keys_answer_like_spark(session: ReparkSession) -> None:
    """Two casts name their columns and sort by both keys.

    pins: tz-asof-1/C-004
    """
    seed_y(session)
    cols, rows = observed(
        session,
        "SELECT CAST(ts AS STRING), CAST(id AS STRING) FROM sc.ns.y ORDER BY ts, id",
    )
    assert cols == [("ts", "string"), ("id", "string")]
    assert rows == [["2024-01-01 00:00:00", "1"], ["2024-01-02 00:00:00", "2"]]


def test_distinct_cast_with_unprojected_key_answers(session: ReparkSession) -> None:
    """DISTINCT with an un-projected key answers the distinct rows in order.

    pins: tz-asof-1/C-005
    """
    seed_y(session)
    cols, rows = observed(session, "SELECT DISTINCT CAST(ts AS STRING) FROM sc.ns.y ORDER BY ts")
    assert cols == [("ts", "string")]
    assert rows == [["2024-01-01 00:00:00"], ["2024-01-02 00:00:00"]]


def test_cast_of_aggregate_names_the_full_cast(session: ReparkSession) -> None:
    """A cast of an aggregate shows Spark's full CAST text.

    pins: tz-asof-1/C-006
    """
    seed_y(session)
    cols, rows = observed(
        session,
        "SELECT CAST(max(ts) AS STRING) FROM sc.ns.y GROUP BY s ORDER BY max(ts)",
    )
    assert cols == [("CAST(max(ts) AS STRING)", "string")]
    assert rows == [["2024-01-01 00:00:00"], ["2024-01-02 00:00:00"]]


def test_snapshots_cast_orders_and_trims_like_spark(session: ReparkSession) -> None:
    """The snapshots read orders and trims trailing fractional zeros like Spark.

    pins: tz-asof-1/C-007
    """
    session.sql("CREATE TABLE sc.ns.y (id INT) USING iceberg")
    session.sql("INSERT INTO sc.ns.y VALUES (1)")
    time.sleep(1.1)
    session.sql("INSERT INTO sc.ns.y VALUES (2)")
    cols, rows = observed(
        session,
        "SELECT CAST(committed_at AS STRING) FROM sc.ns.y.snapshots ORDER BY committed_at",
    )
    assert cols == [("committed_at", "string")]
    assert len(rows) == 2
    assert rows[0][0] < rows[1][0]
    for row in rows:
        assert is_trimmed_stamp(row[0]), f"untrimmed rendering: {row[0]}"


def test_timestamp_rendering_trims_like_spark(session: ReparkSession) -> None:
    """Whole, milli and micro instants render with Spark's trimming rule.

    pins: tz-asof-1/C-007
    """
    seed_y(session)
    _, spark_instant = observed(
        session, "SELECT CAST(TIMESTAMP '2026-09-26 19:41:02.56' AS STRING)"
    )
    assert spark_instant == [["2026-09-26 19:41:02.56"]]
    _, full_micros = observed(
        session, "SELECT CAST(TIMESTAMP '2024-06-01 12:34:56.123456' AS STRING)"
    )
    assert full_micros == [["2024-06-01 12:34:56.123456"]]
    _, whole_second = observed(session, "SELECT CAST(TIMESTAMP '2024-01-01 00:00:00' AS STRING)")
    assert whole_second == [["2024-01-01 00:00:00"]]


def test_timestamp_as_of_cell_shape_answers_first_snapshot(session: ReparkSession) -> None:
    """The cell shape reads the first snapshot text back through TIMESTAMP AS OF.

    pins: tz-asof-1/C-008
    """
    session.sql("CREATE TABLE sc.ns.y (id INT) USING iceberg")
    session.sql("INSERT INTO sc.ns.y VALUES (1)")
    time.sleep(1.1)
    session.sql("INSERT INTO sc.ns.y VALUES (2)")
    _, stamps = observed(
        session,
        "SELECT CAST(committed_at AS STRING) FROM sc.ns.y.snapshots ORDER BY committed_at",
    )
    first_stamp = stamps[0][0]
    assert isinstance(first_stamp, str)
    _, rows = observed(session, f"SELECT * FROM sc.ns.y TIMESTAMP AS OF '{first_stamp}'")
    assert rows == [[1]]


def test_projected_keys_stars_and_unions_keep_todays_names(session: ReparkSession) -> None:
    """Shapes the rewrite skips keep their current names and rows.

    pins: tz-asof-1/C-009
    """
    seed_y(session)
    cols, rows = observed(session, "SELECT ts FROM sc.ns.y ORDER BY ts")
    assert cols == [("ts", "timestamp")]
    assert [row[0].isoformat() for row in rows] == ["2024-01-01T00:00:00", "2024-01-02T00:00:00"]
    cols, _ = observed(session, "SELECT * FROM sc.ns.y ORDER BY ts")
    assert cols == [("id", "int"), ("ts", "timestamp"), ("s", "string")]
    cols, _ = observed(
        session,
        "SELECT id FROM sc.ns.y UNION ALL SELECT id FROM sc.ns.y ORDER BY id",
    )
    assert cols == [("id", "int")]
    cols, _ = observed(session, "SELECT count(*) FROM sc.ns.y GROUP BY s ORDER BY s")
    assert cols == [("count(*)", "bigint")]
    cols, _ = observed(session, "SELECT CAST(ts AS STRING) FROM sc.ns.y")
    assert cols == [("sc.ns.y.ts", "string")]


def test_struct_field_key_never_binds_a_same_named_column(session: ReparkSession) -> None:
    """A compound ORDER BY key never binds to a same-named select column.

    ``st.s`` sorts by the struct field, so the rewrite bails and the answer
    matches main: ``b, a, b``.

    pins: tz-asof-1/C-010
    """
    seed_z(session)
    cols, rows = observed(session, "SELECT s FROM sc.ns.z ORDER BY st.s, ts")
    assert cols == [("s", "string")]
    assert rows == [["b"], ["a"], ["b"]]
