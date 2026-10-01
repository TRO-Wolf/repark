"""WO LTZ-STACKED-SIGN-1 re-verify fold (RN3-1): probe strings round-trip.

A string cell holding a backslash directly before a quote (``'x\\''y'``)
refused with ``TokenizerError`` on every table with a ``TIMESTAMP_NTZ`` column
and on every non-literal ``TIMESTAMP`` cell: sqlparser's ``Display`` renders
the value so that it no longer parses back, and the fail-closed VALUES probe
refused the row. Base and Spark 4.1.2 store the exact value. The probe now
renders a string holding a quote as a dollar-quoted literal with the same
value, so every such row stores exactly what Spark stores.

pins: ltz-stacked-sign-1/RN3-1
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from repark import ReparkSession

WALL = "TIMESTAMP_NTZ'2024-01-02 03:04:05'"
LTZ_WALL = "TIMESTAMP'2024-01-02 03:04:05'"
WALL_TEXT = "2024-01-02 03:04:05"


def _open(warehouse: Path) -> ReparkSession:
    """Open a UTC facade session with the RN3-1 tables."""
    session = (
        ReparkSession.builder.appName("pytest-ltz-stacked-sign-1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", warehouse)
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql("CREATE TABLE sc.ns.n (id INT, s STRING, n TIMESTAMP_NTZ) USING iceberg")
    session.sql("CREATE TABLE sc.ns.l (id INT, s STRING, l TIMESTAMP) USING iceberg")
    session.sql("CREATE TABLE sc.ns.p (id INT, s STRING) USING iceberg")
    session.sql(
        "CREATE TABLE sc.ns.c (id INT, m MAP<STRING, STRING>, n TIMESTAMP_NTZ) USING iceberg"
    )
    return session


def _pylist(session: ReparkSession, sql: str) -> list[dict[str, Any]]:
    """Collect a query into plain rows."""
    return session.sql(sql).to_arrow().to_pylist()  # type: ignore[no-any-return]


def test_ntz_door_backslash_quote_strings_store_exact_values(tmp_path: Path) -> None:
    """Backslash-quote strings into an NTZ table store Spark's exact values."""
    session = _open(tmp_path)
    cells = [
        (1, r"""'{"msg": "it\\''s"}'""", '{"msg": "it\\\'s"}'),
        (2, r"""'C:\\dir\\''s file'""", "C:\\dir\\'s file"),
        (3, r"""'end\\'''""", "end\\'"),
        (4, "'no quote'", "no quote"),
        (101, r"'end\\'", "end\\"),
        (102, r"'a\\\\''b'", "a\\\\'b"),
        (103, r"'a\nb\tc'", "a\nb\tc"),
        (104, r"'\u00e9'", "é"),
        (106, r"'-- x\\''y'", "-- x\\'y"),
        (109, r"'a\nb\\''c'", "a\nb\\'c"),
        (110, "'it''s$p'", "it's$p"),
        (111, r"'x\\''y$p'", "x\\'y$p"),
        (112, "'$p$x''y$pp'", "$p$x'y$pp"),
        (113, "'''$p'", "'$p"),
        (1051, r"'a\\''b'", "a\\'b"),
        (1054, r"'\\'''", "\\'"),
    ]
    for row_id, cell, _want in cells:
        session.sql(f"INSERT INTO sc.ns.n VALUES ({row_id}, {cell}, {WALL})")
    session.sql(f"INSERT INTO sc.ns.n (n, id, s) VALUES ({WALL}, 5, 'x\\\\''y')")
    rows = _pylist(session, "SELECT id, s, CAST(n AS STRING) AS n FROM sc.ns.n ORDER BY id")
    expected = sorted([(row_id, want) for row_id, _cell, want in cells] + [(5, "x\\'y")])
    assert rows == [{"id": row_id, "s": want, "n": WALL_TEXT} for row_id, want in expected]


def test_ntz_door_replace_casts_store_the_wall(tmp_path: Path) -> None:
    """Replace-casts holding backslash-quote strings store the wall into NTZ."""
    session = _open(tmp_path)
    session.sql(
        r"INSERT INTO sc.ns.n VALUES (10, 'x', "
        r"CAST(replace('2024-01-02 03:04:05\\''', '\\''', '') AS TIMESTAMP_NTZ))"
    )
    session.sql(
        r"INSERT INTO sc.ns.n VALUES (107, 'x', "
        r"CAST(replace('-- x\\''y2024-01-02 03:04:05', '-- x\\''y', '') AS TIMESTAMP_NTZ))"
    )
    rows = _pylist(session, "SELECT id, s, CAST(n AS STRING) AS n FROM sc.ns.n ORDER BY id")
    assert rows == [
        {"id": 10, "s": "x", "n": WALL_TEXT},
        {"id": 107, "s": "x", "n": WALL_TEXT},
    ]


def test_ntz_door_map_cells_store_exact_values(tmp_path: Path) -> None:
    """Map cells holding backslash-quote strings store Spark's exact maps."""
    session = _open(tmp_path)
    session.sql(f"INSERT INTO sc.ns.c VALUES (6, map('k', 'v\\\\''w'), {WALL})")
    session.sql(f"INSERT INTO sc.ns.c VALUES (401, map('k', '-- x\\\\''y'), {WALL})")
    rows = _pylist(session, "SELECT id, m, CAST(n AS STRING) AS n FROM sc.ns.c ORDER BY id")
    assert rows == [
        {"id": 6, "m": [("k", "v\\'w")], "n": WALL_TEXT},
        {"id": 401, "m": [("k", "-- x\\'y")], "n": WALL_TEXT},
    ]


def test_ltz_door_replace_casts_store_the_wall(tmp_path: Path) -> None:
    """Replace-casts holding backslash-quote strings store the wall into LTZ."""
    session = _open(tmp_path)
    session.sql(f"INSERT INTO sc.ns.l VALUES (8, 'x\\\\''y', {LTZ_WALL})")
    session.sql(
        r"INSERT INTO sc.ns.l VALUES (9, 'x', "
        r"CAST(replace('2024-01-02 03:04:05\\''', '\\''', '') AS TIMESTAMP))"
    )
    session.sql(
        r"INSERT INTO sc.ns.l VALUES (301, 'x', "
        r"CAST(replace('2024-01-02 03:04:05a\\\\''b', 'a\\\\''b', '') AS TIMESTAMP))"
    )
    session.sql(
        r"INSERT INTO sc.ns.l VALUES (302, 'x', "
        r"CAST(replace('-- x\\''y2024-01-02 03:04:05', '-- x\\''y', '') AS TIMESTAMP))"
    )
    rows = _pylist(session, "SELECT id, s, CAST(l AS STRING) AS l FROM sc.ns.l ORDER BY id")
    assert rows == [
        {"id": 8, "s": "x\\'y", "l": WALL_TEXT},
        {"id": 9, "s": "x", "l": WALL_TEXT},
        {"id": 301, "s": "x", "l": WALL_TEXT},
        {"id": 302, "s": "x", "l": WALL_TEXT},
    ]
