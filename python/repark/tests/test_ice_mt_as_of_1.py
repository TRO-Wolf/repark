"""xo55-mt R1 — the Spark door serves ``VERSION AS OF`` on Iceberg metadata tables.

Five inventory cells replayed verbatim over a partitioned merge-on-read seed
(two appends, one delete): ``R-MT-SNAPSHOTS-TT`` (the TT rows equal the
non-TT rows), ``R-MT-FILES-TT`` (three data files at the second snapshot),
``R-MT-ENTRIES-TT`` and ``R-MT-PARTITIONS-TT`` (scoped to the second
snapshot), and ``R-REF-BRANCH-FILES`` (``files VERSION AS OF 'b0'`` reads
the branch head).

Oracle: the run-25/26 inventory harness cells recorded against live PySpark
4.1.2 (``/tmp/oc-worker/scoreboard/2026-09-22/matrix.json``), whose Spark
answers the packet carries as three files at the second snapshot and
snapshots-TT equal to snapshots non-TT.

pins: xo55-mt/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

CATALOG = "mt"
NAMESPACE = "ns"
SEED_DDL = "(id BIGINT, data STRING, cat STRING)"
FIRST_APPEND = "(1, 'a', 'x'), (2, 'b', 'y'), (6, 'f', 'x')"
SECOND_APPEND = "(3, 'c', 'x')"


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with an in-memory Iceberg catalog named ``mt``."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-ice-mt-as-of-1").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _seeded(session: Any, name: str) -> str:
    """Create the two-append MoR table, delete id 1, return its three-part name."""
    table = f"{CATALOG}.{NAMESPACE}.{name}"
    session.sql(
        f"CREATE TABLE {table} {SEED_DDL} USING iceberg PARTITIONED BY (cat) "
        "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')"
    )
    session.sql(f"INSERT INTO {table} VALUES {FIRST_APPEND}")
    session.sql(f"INSERT INTO {table} VALUES {SECOND_APPEND}")
    session.sql(f"DELETE FROM {table} WHERE id = 1")
    return table


def _snapshot_ids(session: Any, table: str) -> list[int]:
    """Snapshot ids oldest first via the table's own snapshots table."""
    rows = session.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at")
    return [int(row[0]) for row in rows.collect()]


def _count(session: Any, query: str) -> int:
    """Collect a ``count(*)`` query as one int."""
    return int(session.sql(query).collect()[0][0])


def _sorted_rows(session: Any, query: str, key: int) -> list[list[Any]]:
    """Collect one query as plain lists sorted by the ``key`` column."""
    return sorted((list(row) for row in session.sql(query).collect()), key=lambda row: row[key])


def test_snapshots_tt_equals_current(spark: Any) -> None:
    """Cell ``R-MT-SNAPSHOTS-TT``: TT rows and schema equal the un-pinned table.

    pins: xo55-mt/C-001
    """
    table = _seeded(spark, "t_snapshots_tt")
    second = _snapshot_ids(spark, table)[1]
    plain = f"SELECT * FROM {table}.snapshots"
    pinned = f"{plain} VERSION AS OF {second}"
    assert _sorted_rows(spark, pinned, 1) == _sorted_rows(spark, plain, 1)
    assert [field.name for field in spark.sql(pinned).schema.fields] == [
        field.name for field in spark.sql(plain).schema.fields
    ]


def test_files_tt_scopes_to_snapshot(spark: Any) -> None:
    """Cell ``R-MT-FILES-TT``: three data files live at the second snapshot.

    pins: xo55-mt/C-002
    """
    table = _seeded(spark, "t_files_tt")
    second = _snapshot_ids(spark, table)[1]
    assert _count(spark, f"SELECT count(*) FROM {table}.files VERSION AS OF {second}") == 3


def test_entries_tt_scopes_to_snapshot(spark: Any) -> None:
    """Cell ``R-MT-ENTRIES-TT``: one entry per live file at the second snapshot.

    pins: xo55-mt/C-003
    """
    table = _seeded(spark, "t_entries_tt")
    second = _snapshot_ids(spark, table)[1]
    assert _count(spark, f"SELECT count(*) FROM {table}.entries VERSION AS OF {second}") == 3


def test_partitions_tt_scopes_to_snapshot(spark: Any) -> None:
    """Cell ``R-MT-PARTITIONS-TT``: per-partition counts at the second snapshot.

    pins: xo55-mt/C-004
    """
    table = _seeded(spark, "t_partitions_tt")
    second = _snapshot_ids(spark, table)[1]
    assert _sorted_rows(
        spark,
        f"SELECT partition.cat, record_count FROM {table}.partitions VERSION AS OF {second}",
        0,
    ) == [["x", 3], ["y", 1]]


def test_branch_files_reads_branch_head(spark: Any) -> None:
    """Cell ``R-REF-BRANCH-FILES``: ``files VERSION AS OF 'b0'`` reads the branch.

    pins: xo55-mt/C-005
    """
    table = _seeded(spark, "t_branch_files")
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH b0")
    assert _count(spark, f"SELECT count(*) FROM {table}.files VERSION AS OF 'b0'") == _count(
        spark, f"SELECT count(*) FROM {table}.files"
    )
