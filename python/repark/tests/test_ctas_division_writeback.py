"""Facade write-path pins for CTAS integer-division type derivation.

The regression: a CTAS of a union of integer divisions derived an ``Int64`` write schema while
the executed data was ``Float64`` (the passthrough analyzes once for the schema, execution
re-analyzes to the ``Float64`` fixpoint), so the parquet writer failed loud. These tests drive
``ReparkSession.sql`` CTAS into an in-memory Iceberg catalog and read the table back on the
Arrow path (``to_arrow``), asserting the stored column's value AND Arrow type.

Oracle: live PySpark 4.1.2 (non-ANSI, repark's target division semantics;
``JAVA_HOME=/usr/lib/jvm/zulu-17-amd64``, ``local[2]``, ``timeZone=UTC``):

    SELECT 5/2 AS q UNION ALL SELECT 7/2   -> double {2.5, 3.5}
    SELECT 7/2 AS q                        -> double 3.5
    SELECT 5/0 AS q UNION ALL SELECT 7/2   -> double {NULL, 3.5}  (ANSI Spark raises DIVIDE_BY_ZERO)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession

NAMESPACE = "glue_catalog.div_ns"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with an in-memory Iceberg catalog + namespace (local, AWS-free)."""
    # ANSI OFF: the zero-divisor pin needs a NULL branch; default ANSI ON would raise.
    session = (
        ReparkSession.builder.appName("pytest-ctas-division")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    session.register_memory_catalog("glue_catalog", tmp_path)
    session.sql(f"CREATE NAMESPACE {NAMESPACE}")
    return session


def _writeback(spark: ReparkSession, table: str, select_sql: str) -> pa.Table:
    """CTAS ``select_sql`` into ``table`` and read column ``q`` back on the Arrow path."""
    spark.sql(f"CREATE TABLE {NAMESPACE}.{table} AS {select_sql}")
    return spark.sql(f"SELECT q FROM {NAMESPACE}.{table}").to_arrow()


def _sorted_q(table: pa.Table) -> list[object]:
    """Column ``q`` as a list, ascending with NULLs first (Iceberg read order is unspecified)."""
    return sorted(table.column("q").to_pylist(), key=lambda value: (value is not None, value))


def test_ctas_union_of_integer_division_writes_double(spark: ReparkSession) -> None:
    """The union-of-division CTAS lands as double with the fractional values intact — the exact
    shape that failed at the parquet writer (``Field q has type Int64, array has type Float64``).
    """
    table = _writeback(spark, "union_div", "SELECT 5/2 AS q UNION ALL SELECT 7/2")
    assert pa.types.is_float64(table.schema.field("q").type), (
        "stored division column must be double"
    )
    assert _sorted_q(table) == [2.5, 3.5]


def test_ctas_bare_integer_division_writes_double(spark: ReparkSession) -> None:
    """A non-union bare integer division lands as double (the control case)."""
    table = _writeback(spark, "bare_div", "SELECT 7/2 AS q")
    assert pa.types.is_float64(table.schema.field("q").type)
    assert table.column("q").to_pylist() == [3.5]


def test_ctas_union_zero_divisor_writes_null_double(spark: ReparkSession) -> None:
    """A zero divisor is NULL with the promoted double result type (Spark non-ANSI): the UNION
    parent reconciles to double even when one branch is NULL.
    """
    table = _writeback(spark, "zero_div", "SELECT 5/0 AS q UNION ALL SELECT 7/2")
    assert pa.types.is_float64(table.schema.field("q").type), (
        "zero-divisor column keeps double type"
    )
    assert _sorted_q(table) == [None, 3.5]
