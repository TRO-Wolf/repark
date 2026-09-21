"""ICE-CHANGE-COLUMN-RENAME-1 — the Hive-style CHANGE COLUMN rename refusal.

Spark refuses a Hive-style ``CHANGE COLUMN`` whose two column names differ; the
Spark door refuses it at parse time with ``[PARSE_SYNTAX_ERROR]`` /
``SQLSTATE: 42601``. Every ``CHANGE COLUMN`` that does NOT rename (same name,
type and/or COMMENT) keeps working. The refusal must not mint a metadata
document; the near-miss must widen the type and keep the rows.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session import _reset_active_session_for_tests

CATALOG = "sc"
NAMESPACE = "ns"


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with an in-memory Iceberg catalog named ``sc``."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-ice-change-column-rename-1").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _create(session: Any, name: str) -> str:
    """Create one empty seed-shaped Iceberg table and return its three-part name."""
    table = f"{CATALOG}.{NAMESPACE}.{name}"
    session.sql(f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg")
    return table


def _metadata(warehouse: Path, table: str) -> dict[str, Any]:
    """Parse the newest table metadata JSON under the warehouse."""
    metas = sorted(warehouse.rglob(f"{table}/metadata/*.metadata.json"))
    assert metas, f"no metadata found for {table}"
    return json.loads(metas[-1].read_text(encoding="utf-8"))


def _fields(meta: dict[str, Any]) -> list[tuple[str, str]]:
    """Top-level ``(name, type)`` pairs of the metadata's current schema."""
    current = meta["current-schema-id"]
    schema = next(item for item in meta["schemas"] if item["schema-id"] == current)
    return [(field["name"], field["type"]) for field in schema["fields"]]


def _metadata_file_count(warehouse: Path, table: str) -> int:
    """Number of metadata documents minted for one table."""
    return len(list(warehouse.rglob(f"{table}/metadata/*.metadata.json")))


def test_change_column_rename_refuses(spark: Any, tmp_path: Path) -> None:
    """The rename form refuses at parse time and leaves the table untouched."""
    table = _create(spark, "t_change_rename_refused")
    files_before = _metadata_file_count(tmp_path / "wh", "t_change_rename_refused")
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"ALTER TABLE {table} CHANGE COLUMN data payload STRING")
    message = str(caught.value)
    assert "[PARSE_SYNTAX_ERROR]" in message
    assert "42601" in message

    fields = _fields(_metadata(tmp_path / "wh", "t_change_rename_refused"))
    assert ("data", "string") in fields
    assert all(name != "payload" for name, _ in fields)
    assert _metadata_file_count(tmp_path / "wh", "t_change_rename_refused") == files_before


def test_change_column_same_name_type_only_keeps_rows(spark: Any, tmp_path: Path) -> None:
    """The non-rename form still works: the type widens and the row survives."""
    table = f"{CATALOG}.{NAMESPACE}.t_change_same_name_widen"
    spark.sql(f"CREATE TABLE {table} (id INT, data STRING) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a')")
    assert ("id", "int") in _fields(_metadata(tmp_path / "wh", "t_change_same_name_widen"))

    spark.sql(f"ALTER TABLE {table} CHANGE COLUMN id id BIGINT")

    fields = _fields(_metadata(tmp_path / "wh", "t_change_same_name_widen"))
    assert ("id", "long") in fields
    assert ("data", "string") in fields
    arrow = spark.sql(f"SELECT id, data FROM {table} ORDER BY id").to_arrow()
    assert arrow.to_pylist() == [{"id": 1, "data": "a"}]
