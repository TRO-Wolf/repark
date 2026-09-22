"""IPI-07 R-BRANCH-SCHEMA — a branch read projects the table's current schema.

After ``CREATE BRANCH b0`` plus ``ADD COLUMN z INT``, Spark 4.1.2 answers
``SELECT * FROM t.branch_b0`` with four columns and ``z`` NULL, while the tag,
snapshot-id and timestamp reads keep the three-column snapshot schema. The
offline tier replays those shapes on a memory catalog; the live tier re-derives
them on live Spark and cross-reads the adopted table from RePark.

Oracle: ``/tmp/oc-worker/pc-meas/spark-pc2.json`` ids ``BS-*``
(``BS-ADD-IDENT``, ``BS-ADD-VERSION``, ``BS-ADD-TAG``, ``BS-ADD-SNAPID``,
``BS-DROP``, ``BS-RENAME``, ``BS-MAIN-IDENT``, ``BS-WHERE-NEWCOL``) and
scoreboard cell ``R-BRANCH-SCHEMA``. ``BS-ADD-OPTION-V2/V3`` are broken
harness cells (both engines fail in the helper) and stay out.

The legacy ``tag`` reader option is not a tag read: Spark refuses it, so
RePark refuses it with the pinned text and the suite reads tags through the
``versionAsOf`` built-in instead.

pins: ipi-07-branch-read-schema-1/C-001, C-002, C-003, C-004
pins: ipi-07-branch-read-schema-1/C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException
from repark.spark.session import _reset_active_session_for_tests

_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"
_CATALOG = "bs"
_NAMESPACE = "ns"
_SEED_DDL = "(id BIGINT, data STRING, cat STRING)"
_SEED_ROWS = "(1, 'a', 'x'), (2, 'b', 'y')"
_TAG_REFUSAL = (
    "Time travel option `tag` is no longer supported, "
    "use Spark built-in `versionAsOf` instead"
)
_UNKNOWN_REF = "Cannot find matching snapshot ID or reference name for version nope"


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with an in-memory Iceberg catalog named ``bs``."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-ice-branch-schema-1").getOrCreate()
    session.register_memory_catalog(_CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _seeded(session: Any, name: str) -> str:
    """Create the two-row table with branch ``b0`` and tag ``t0``; return its name."""
    table = f"{_CATALOG}.{_NAMESPACE}.{name}"
    session.sql(f"CREATE TABLE {table} {_SEED_DDL} USING iceberg")
    session.sql(f"INSERT INTO {table} VALUES {_SEED_ROWS}")
    session.sql(f"ALTER TABLE {table} CREATE BRANCH b0")
    session.sql(f"ALTER TABLE {table} CREATE TAG t0")
    return table


def _shape(table: Any) -> tuple[list[str], list[str], list[tuple[Any, ...]]]:
    """Arrow field names, type strings, and sorted row tuples (value AND type)."""
    arrow = table.to_arrow() if hasattr(table, "to_arrow") else table.toArrow()
    names = arrow.schema.names
    types = [str(arrow.schema.field(name).type) for name in names]
    rows = sorted(tuple(row.values()) for row in arrow.to_pylist())
    return names, types, rows


def _committed_plus_one(session: Any, table: str) -> str:
    """Latest snapshot commit time plus one second as a Spark wall clock."""
    stamped = session.sql(f"SELECT committed_at FROM {table}.snapshots").collect()
    latest = max(row[0] for row in stamped)
    return (latest + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")


def test_branch_selector_projects_current_schema(spark: Any) -> None:
    """Cell ``BS-ADD-IDENT``: the branch read has four columns and ``z`` NULL.

    pins: ipi-07-branch-read-schema-1/C-001
    """
    table = _seeded(spark, "t_add")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN z INT")
    names, types, rows = _shape(spark.sql(f"SELECT * FROM {table}.branch_b0"))
    assert names == ["id", "data", "cat", "z"]
    assert types == ["int64", "string", "string", "int32"]
    assert rows == [(1, "a", "x", None), (2, "b", "y", None)]


def test_branch_reader_option_projects_current_schema(spark: Any) -> None:
    """The DataFrame door: ``option("branch", "b0")`` reads four columns.

    pins: ipi-07-branch-read-schema-1/C-002
    """
    table = _seeded(spark, "t_option")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN z INT")
    names, types, rows = _shape(spark.read.option("branch", "b0").table(table))
    assert names == ["id", "data", "cat", "z"]
    assert types == ["int64", "string", "string", "int32"]
    assert rows == [(1, "a", "x", None), (2, "b", "y", None)]


def test_version_as_of_branch_projects_current_schema(spark: Any) -> None:
    """Cell ``BS-ADD-VERSION``: ``VERSION AS OF 'b0'`` reads four columns.

    pins: ipi-07-branch-read-schema-1/C-003
    """
    table = _seeded(spark, "t_version")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN z INT")
    names, _types, rows = _shape(spark.sql(f"SELECT * FROM {table} VERSION AS OF 'b0'"))
    assert names == ["id", "data", "cat", "z"]
    assert rows == [(1, "a", "x", None), (2, "b", "y", None)]


def test_branch_filter_on_new_column(spark: Any) -> None:
    """Cell ``BS-WHERE-NEWCOL``: ``WHERE z IS NULL`` answers both rows.

    pins: ipi-07-branch-read-schema-1/C-004
    """
    table = _seeded(spark, "t_where")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN z INT")
    rows = spark.sql(f"SELECT id FROM {table}.branch_b0 WHERE z IS NULL").to_arrow()
    assert sorted(rows.column("id").to_pylist()) == [1, 2]


def test_branch_drop_and_rename_project_current_schema(spark: Any) -> None:
    """Cells ``BS-DROP`` / ``BS-RENAME``: the branch tracks the live schema.

    pins: ipi-07-branch-read-schema-1/C-005
    """
    dropped = _seeded(spark, "t_drop")
    spark.sql(f"ALTER TABLE {dropped} DROP COLUMN data")
    names, types, rows = _shape(spark.sql(f"SELECT * FROM {dropped}.branch_b0"))
    assert names == ["id", "cat"]
    assert types == ["int64", "string"]
    assert rows == [(1, "x"), (2, "y")]

    renamed = _seeded(spark, "t_rename")
    spark.sql(f"ALTER TABLE {renamed} RENAME COLUMN data TO payload")
    names, types, rows = _shape(spark.sql(f"SELECT * FROM {renamed}.branch_b0"))
    assert names == ["id", "payload", "cat"]
    assert types == ["int64", "string", "string"]
    assert rows == [(1, "a", "x"), (2, "b", "y")]


def test_tag_snapshot_and_timestamp_keep_snapshot_schema(spark: Any) -> None:
    """Cells ``BS-ADD-TAG`` / ``BS-ADD-SNAPID``: pins keep three columns.

    pins: ipi-07-branch-read-schema-1/C-006
    """
    table = _seeded(spark, "t_near")
    snapshot = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").collect()[0][0]
    stamp = _committed_plus_one(spark, table)
    spark.sql(f"ALTER TABLE {table} ADD COLUMN z INT")
    queries = [
        f"SELECT * FROM {table}.tag_t0",
        f"SELECT * FROM {table} VERSION AS OF 't0'",
        f"SELECT * FROM {table}.snapshot_id_{snapshot}",
        f"SELECT * FROM {table} VERSION AS OF {snapshot}",
        f"SELECT * FROM {table} TIMESTAMP AS OF '{stamp}'",
    ]
    for query in queries:
        names, types, rows = _shape(spark.sql(query))
        assert names == ["id", "data", "cat"], query
        assert types == ["int64", "string", "string"], query
        assert rows == [(1, "a", "x"), (2, "b", "y")], query
    names, _types, rows = _shape(spark.read.option("versionAsOf", "t0").table(table))
    assert names == ["id", "data", "cat"]
    assert rows == [(1, "a", "x"), (2, "b", "y")]


def test_tag_reader_option_keeps_refusing(spark: Any) -> None:
    """The legacy ``tag`` option refuses exactly like Spark 4.1.2.

    pins: ipi-07-branch-read-schema-1/C-007
    """
    table = _seeded(spark, "t_tagopt")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN z INT")
    with pytest.raises(IllegalArgumentException) as caught:
        spark.read.option("tag", "t0").table(table)
    assert str(caught.value) == _TAG_REFUSAL


def test_unknown_ref_keeps_refusal(spark: Any) -> None:
    """Unknown refs refuse with the pinned text on both spellings.

    pins: ipi-07-branch-read-schema-1/C-008
    """
    table = _seeded(spark, "t_unknown")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN z INT")
    for query in [
        f"SELECT * FROM {table}.branch_nope",
        f"SELECT * FROM {table} VERSION AS OF 'nope'",
    ]:
        with pytest.raises(IllegalArgumentException) as caught:
            spark.sql(query).collect()
        assert str(caught.value) == _UNKNOWN_REF, query


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_branch_schema_matches_spark(tmp_path: Path) -> None:
    """Live Spark re-derives the cells; RePark cross-reads the adopted table.

    pins: ipi-07-branch-read-schema-1/C-009
    """
    import _live_parity as lp

    warehouse = tmp_path / "spark-warehouse"
    spark = lp.build_spark_iceberg_engine(warehouse, catalog="bs_live").session
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bs_live.ns")
    spark.sql("CREATE TABLE bs_live.ns.t (id BIGINT, data STRING, cat STRING) USING iceberg")
    spark.sql("INSERT INTO bs_live.ns.t VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    spark.sql("ALTER TABLE bs_live.ns.t CREATE BRANCH b0")
    spark.sql("ALTER TABLE bs_live.ns.t CREATE TAG t0")
    spark.sql("ALTER TABLE bs_live.ns.t ADD COLUMN z INT")
    branch = spark.sql("SELECT * FROM bs_live.ns.t.branch_b0").toArrow()
    assert branch.schema.names == ["id", "data", "cat", "z"]
    assert sorted(tuple(row.values()) for row in branch.to_pylist()) == [
        (1, "a", "x", None),
        (2, "b", "y", None),
    ]
    tag = spark.sql("SELECT * FROM bs_live.ns.t.tag_t0").toArrow()
    assert tag.schema.names == ["id", "data", "cat"]

    repark = ReparkSession.builder.appName("ice-branch-schema-1-live").getOrCreate()
    try:
        repark.register_memory_catalog("rp", tmp_path / "repark-warehouse")
        repark.sql("CREATE NAMESPACE rp.ns")
        metas = list((warehouse / "ns" / "t" / "metadata").glob("*.metadata.json"))
        assert metas, "no Spark metadata to adopt"
        newest = max(metas, key=lambda path: path.stat().st_mtime_ns)
        repark.sql(f"CALL rp.system.register_table(table => 'ns.t', metadata_file => '{newest}')")
        names, _types, rows = _shape(repark.sql("SELECT * FROM rp.ns.t.branch_b0"))
        assert names == ["id", "data", "cat", "z"]
        assert rows == [(1, "a", "x", None), (2, "b", "y", None)]
        names, _types, _rows = _shape(repark.sql("SELECT * FROM rp.ns.t.tag_t0"))
        assert names == ["id", "data", "cat"]
        names, _types, _rows = _shape(repark.read.option("branch", "b0").table("rp.ns.t"))
        assert names == ["id", "data", "cat", "z"]
    finally:
        repark.stop()
