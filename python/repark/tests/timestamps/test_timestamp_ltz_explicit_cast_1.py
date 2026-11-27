from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

pytestmark = pytest.mark.slow

ORACLE_PATH: Path = Path(__file__).with_name("timestamp_ltz_explicit_cast_1_spark_oracle.json")
_CELLS: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))["cells"]


def _open(cell: dict[str, Any], warehouse: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("timestamp-ltz-explicit-cast-1")
        .config("spark.sql.session.timeZone", cell["zone"])
        .config("spark.sql.timestampType", cell["timestamp_type"])
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", str(warehouse))
    return session


def _store(session: ReparkSession, cell: dict[str, Any]) -> list[list[Any]]:
    table, target, source = "sc.ns.t", cell["target"], cell["source"]
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    if target == "CTAS":
        session.sql(f"CREATE TABLE {table} USING iceberg AS {source}")
    elif source.startswith("MERGE:"):
        session.sql(f"CREATE TABLE {table} (id INT, v TIMESTAMP_{target}) USING iceberg")
        session.sql(
            f"MERGE INTO {table} t USING ({source[6:]}) x ON t.id = x.id "
            "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (x.id, x.v)"
        )
    else:
        session.sql(f"CREATE TABLE {table} (id INT, v TIMESTAMP_{target}) USING iceberg")
        session.sql(f"INSERT INTO {table} {source}")
    rows = session.sql(f"SELECT id, typeof(v), CAST(v AS STRING) FROM {table} ORDER BY id").collect()
    return [[row[0], row[1], row[2]] for row in rows]


@pytest.mark.parametrize("name", sorted(name for name, cell in _CELLS.items() if cell["kind"] != "TARGET_OR_REFUSE"))
def test_explicit_ltz_values_store_sparks_value(name: str, tmp_path: Path) -> None:
    cell = _CELLS[name]
    session = _open(cell, tmp_path)
    try:
        assert _store(session, cell) == cell["spark"]
    finally:
        session.stop()


@pytest.mark.parametrize("name", sorted(name for name, cell in _CELLS.items() if cell["kind"] == "TARGET_OR_REFUSE"))
def test_explicit_ltz_values_store_sparks_value_or_refuse(name: str, tmp_path: Path) -> None:
    cell = _CELLS[name]
    session = _open(cell, tmp_path)
    try:
        try:
            stored = _store(session, cell)
        except Exception:
            return
        assert stored == cell["spark"]
    finally:
        session.stop()
