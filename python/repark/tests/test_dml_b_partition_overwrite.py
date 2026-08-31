"""Facade SQL INSERT OVERWRITE PARTITION pins (DML-B).

Live PySpark 4.1.2 + Iceberg 1.11.0 (2026-08-30): static ``PARTITION (k=v)`` keeps
sibling files and stamps ``overwrite`` (nonempty) or ``delete`` (empty); Hive
injects the partition columns. ``PARTITION (k)`` without values is Spark's
dynamic replace under ``partitionOverwriteMode=dynamic`` / ``writeTo.overwritePartitions``
(``replace-partitions=true``); Spark's default STATIC mode wipes the table — repark
always takes the dynamic path. Empty dynamic refuses loud (Spark writeTo empty is a
no-op; Spark SQL STATIC empty ``PARTITION (id)`` wipes).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException

CATALOG = "dmlb_cat"
NS = "dmlb_ns"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """Session with an in-memory Iceberg catalog (local, AWS-free)."""
    session = ReparkSession.builder.appName("pytest-dmlb-partition").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NS}")
    return session


def _seed(spark: ReparkSession, table: str) -> None:
    """Create an identity-partitioned table with three id partitions."""
    spark.sql(
        f"CREATE TABLE {table} USING iceberg PARTITIONED BY (id) AS "
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"
    )


def _rows(spark: ReparkSession, table: str) -> pa.Table:
    """Read ``id, name`` ordered by id on the Arrow export path."""
    return spark.sql(f"SELECT id, name FROM {table} ORDER BY id").to_arrow()


def _assert_id_name_types(table: pa.Table) -> None:
    """Pin Arrow types for the identity-partitioned id/name fixture."""
    assert table.schema.field("id").type in (pa.int32(), pa.int64())
    assert table.schema.field("name").type in (pa.string(), pa.large_string())


def _last_operation(spark: ReparkSession, table: str) -> str:
    """Return the current snapshot operation string."""
    arrow = spark.sql(f"SELECT operation FROM {table}.snapshots ORDER BY committed_at").to_arrow()
    ops = arrow.column("operation").to_pylist()
    assert ops, f"{table} has no snapshots"
    last = ops[-1]
    assert isinstance(last, str)
    return last


def _last_summary(spark: ReparkSession, table: str) -> dict[str, str]:
    """Return the current snapshot summary map as string pairs."""
    arrow = spark.sql(f"SELECT summary FROM {table}.snapshots ORDER BY committed_at").to_arrow()
    assert arrow.num_rows >= 1
    parsed: Any = arrow.column("summary")[arrow.num_rows - 1].as_py()
    if parsed is None:
        return {}
    if isinstance(parsed, dict):
        return {str(key): str(value) for key, value in parsed.items()}
    if isinstance(parsed, list):
        return {str(key): str(value) for key, value in parsed}
    raise AssertionError(f"unexpected summary type {type(parsed)}: {parsed!r}")


def test_sql_static_partition_overwrite_keeps_siblings(spark: ReparkSession) -> None:
    """Static PARTITION (k=v) replaces one partition; siblings and types stay.

    pins: dml-b-insert-overwrite/C-001, C-005
    """
    table = f"{CATALOG}.{NS}.static_nonempty"
    _seed(spark, table)
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (id = 1) SELECT 'z'")
    got = _rows(spark, table)
    _assert_id_name_types(got)
    assert got.to_pylist() == [
        {"id": 1, "name": "z"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]
    assert _last_operation(spark, table) == "overwrite"
    assert "replace-partitions" not in _last_summary(spark, table)


def test_sql_empty_static_partition_overwrite_stamps_delete(spark: ReparkSession) -> None:
    """Empty static PARTITION drops only that partition and stamps delete.

    pins: dml-b-insert-overwrite/C-001, C-004, C-005
    """
    table = f"{CATALOG}.{NS}.static_empty"
    _seed(spark, table)
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (id = 1) SELECT name FROM {table} WHERE false")
    got = _rows(spark, table)
    _assert_id_name_types(got)
    assert got.to_pylist() == [{"id": 2, "name": "b"}, {"id": 3, "name": "c"}]
    assert _last_operation(spark, table) == "delete"


def test_sql_static_partition_overwrite_rejects_injected_column(spark: ReparkSession) -> None:
    """Hive static PARTITION injects k; SELECT must not also supply it.

    pins: dml-b-insert-overwrite/C-001
    """
    table = f"{CATALOG}.{NS}.static_arity"
    _seed(spark, table)
    with pytest.raises(PySparkException, match="TOO_MANY_DATA_COLUMNS"):
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (id = 1) SELECT 1 AS id, 'z' AS name")
    got = _rows(spark, table)
    assert got.to_pylist() == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]


def test_sql_dynamic_partition_overwrite_keeps_absent_partitions(spark: ReparkSession) -> None:
    """PARTITION (k) without values replaces only source partitions.

    pins: dml-b-insert-overwrite/C-002
    """
    table = f"{CATALOG}.{NS}.dynamic_nonempty"
    _seed(spark, table)
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (id) SELECT 1 AS id, 'z' AS name")
    got = _rows(spark, table)
    _assert_id_name_types(got)
    assert got.to_pylist() == [
        {"id": 1, "name": "z"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]
    assert _last_operation(spark, table) == "overwrite"
    assert _last_summary(spark, table).get("replace-partitions") == "true"


def test_sql_empty_dynamic_partition_overwrite_refuses(spark: ReparkSession) -> None:
    """Empty dynamic PARTITION refuses; every prior row remains.

    pins: dml-b-insert-overwrite/C-002, C-004
    """
    table = f"{CATALOG}.{NS}.dynamic_empty"
    _seed(spark, table)
    with pytest.raises(
        (AnalysisException, PySparkException),
        match="Cannot dynamically overwrite partitions",
    ):
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (id) SELECT * FROM {table} WHERE false")
    got = _rows(spark, table)
    assert got.to_pylist() == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]
