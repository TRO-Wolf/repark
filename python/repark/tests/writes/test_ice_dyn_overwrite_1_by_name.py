"""``partitionOverwriteMode`` pins for BY NAME and column-list overwrites (ICE-DYN-OVERWRITE-1).

Spark 4.1.2 + Iceberg 1.11.0 answers were recorded into
``python/repark-parity/fixtures/torture/data/ice_dyn_overwrite_1/spark_byname_dyn_oracle.json``
by ``record_spark_byname_dyn.py``: a ``(id BIGINT, k STRING, v STRING)`` table
partitioned by ``k`` holding ``(1,'a','old'), (2,'b','old')``, overwritten through
``BY NAME``, an explicit column list, and positionally, each under both modes, on
v2 and v3, plus the empty-source and unpartitioned shapes. Every cell pins rows and
the snapshot operation sequence.

pins: ice-dyn-overwrite-1/C-019
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "repark-parity"
    / "fixtures"
    / "torture"
    / "data"
    / "ice_dyn_overwrite_1"
    / "spark_byname_dyn_oracle.json"
)
_KEY = "spark.sql.sources.partitionOverwriteMode"
_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"
_CATALOG = "dynbn_cat"
_NS = "dynbn_ns"


def _cells() -> dict[str, dict[str, Any]]:
    """Load the recorded Spark oracle cells."""
    document = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    return document["cells"]  # type: ignore[no-any-return]


@pytest.fixture
def spark(tmp_path: Path) -> Iterator[ReparkSession]:
    """Fresh session with a memory Iceberg catalog and v3 creation allowed."""
    session = (
        ReparkSession.builder.appName("pytest-ice-dyn-overwrite-1-by-name")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    session.register_memory_catalog(_CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NS}")
    try:
        yield session
    finally:
        session.conf.unset(_KEY)


def _local(sql: str) -> str:
    """Rewrite one recorded statement onto the test catalog."""
    return sql.replace("sc.ns.", f"{_CATALOG}.{_NS}.")


def _table_of(cell: dict[str, Any]) -> str:
    """Return the fully-qualified local table name a cell writes."""
    statement = _local(str(cell["statement"]))
    return statement.split()[2]


def _rows(spark: ReparkSession, table: str) -> list[list[Any]]:
    """Read ``id, k, v`` ordered by id."""
    arrow = spark.sql(f"SELECT id, k, v FROM {table} ORDER BY id").to_arrow()
    return [[row["id"], row["k"], row["v"]] for row in arrow.to_pylist()]


def _operations(spark: ReparkSession, table: str) -> list[list[str]]:
    """Return snapshot operations in commit order, shaped like the fixture."""
    batch = spark.sql(f"SELECT operation FROM {table}.snapshots ORDER BY committed_at").to_arrow()
    return [[str(op)] for op in batch.column("operation").to_pylist()]


def _run_cell(spark: ReparkSession, cell: dict[str, Any]) -> str:
    """Set the cell's mode, replay its setup and statement, and return the table."""
    spark.conf.set(_KEY, str(cell["mode"]))
    for statement in cell["setup"]:
        spark.sql(_local(str(statement)))
    spark.sql(_local(str(cell["statement"])))
    return _table_of(cell)


@pytest.mark.parametrize("name", sorted(_cells()))
def test_by_name_overwrite_matches_oracle(spark: ReparkSession, name: str) -> None:
    """Rows and snapshot operations equal the recorded Spark cell.

    pins: ice-dyn-overwrite-1/C-019
    """
    cell = _cells()[name]
    assert cell["error"] is None
    table = _run_cell(spark, cell)
    assert _rows(spark, table) == cell["rows"]
    assert _operations(spark, table) == cell["snapshot_operations"]


def test_unset_restores_static_on_the_live_session(spark: ReparkSession) -> None:
    """``conf.unset`` of the mode resets the native carrier, so overwrite replaces whole again.

    pins: ice-dyn-overwrite-1/C-022
    """
    cell = _cells()["v2_static_positional_touch_one"]
    spark.conf.set(_KEY, "dynamic")
    spark.conf.unset(_KEY)
    for statement in cell["setup"]:
        spark.sql(_local(str(statement)))
    spark.sql(_local(str(cell["statement"])))
    assert _rows(spark, _table_of(cell)) == cell["rows"]
