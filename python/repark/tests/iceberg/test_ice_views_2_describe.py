"""IPI-40 views PR2 battery — DESCRIBE on an Iceberg view.

``DESCRIBE <catalog>.<ns>.<view>`` answers from the view's stored schema:
column rows only, Spark type spellings, and an empty-string comment where the
column carries no doc. The ``TABLE_OR_VIEW_NOT_FOUND`` refusal stays the
fail-closed answer for a name that is neither a table nor a view and for a
missing namespace; other non-``TableNotFound`` load errors keep propagating
untouched.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

TABLE_OR_VIEW_NOT_FOUND_ABSENT = (
    "[TABLE_OR_VIEW_NOT_FOUND] The table or view `sc`.`ns`.`definitely_absent` "
    "cannot be found. Verify the spelling and correctness of the schema and "
    "catalog. If you did not qualify the name with a schema, verify the "
    "current_schema() output, or qualify the name with the correct schema and "
    "catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
    "TABLE IF EXISTS. SQLSTATE: 42P01"
)

TABLE_OR_VIEW_NOT_FOUND_MISSING_NAMESPACE = (
    "[TABLE_OR_VIEW_NOT_FOUND] The table or view `sc`.`missing_ns`.`t` "
    "cannot be found. Verify the spelling and correctness of the schema and "
    "catalog. If you did not qualify the name with a schema, verify the "
    "current_schema() output, or qualify the name with the correct schema and "
    "catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
    "TABLE IF EXISTS. SQLSTATE: 42P01"
)


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    """Memory catalog ``sc`` with namespace ``ns`` and table ``t(id, data)``."""
    session = ReparkSession.builder.appName("pytest-ice-views-2-describe").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql("CREATE TABLE sc.ns.t (id BIGINT, data STRING) USING iceberg")
    session.sql("INSERT INTO sc.ns.t VALUES (1, 'a'), (2, 'b')")
    return session


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame to plain nested lists for golden comparison."""
    return [list(row) for row in frame.collect()]


def test_describe_view_answers_stored_columns(spark: ReparkSession) -> None:
    """V-DESCRIBE — the measured cell: one row, three columns, empty comment."""
    spark.sql("CREATE VIEW sc.ns.vw_t_v_describe AS SELECT id FROM sc.ns.t")
    table = spark.sql("DESCRIBE sc.ns.vw_t_v_describe").to_arrow()
    assert table.schema.names == ["col_name", "data_type", "comment"]
    assert [field.type for field in table.schema] == [pa.string()] * 3
    assert [field.nullable for field in table.schema] == [False, False, True]
    assert _rows(spark.sql("DESCRIBE sc.ns.vw_t_v_describe")) == [["id", "bigint", ""]]


def test_describe_view_renders_aliases_and_column_comments(
    spark: ReparkSession,
) -> None:
    """Stored-schema rendering: aliases and baked column docs, not body expansion."""
    spark.sql(
        "CREATE VIEW sc.ns.vw_aliased (renamed COMMENT 'the id', payload) "
        "AS SELECT id, data FROM sc.ns.t"
    )
    assert _rows(spark.sql("DESCRIBE sc.ns.vw_aliased")) == [
        ["renamed", "bigint", "the id"],
        ["payload", "string", ""],
    ]


def test_describe_absent_name_keeps_full_refusal(spark: ReparkSession) -> None:
    """Neither table nor view -> unchanged TABLE_OR_VIEW_NOT_FOUND, byte for byte."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("DESCRIBE sc.ns.definitely_absent")
    assert str(caught.value) == f"Error during planning: {TABLE_OR_VIEW_NOT_FOUND_ABSENT}"
    assert caught.value.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
    assert caught.value.getSqlState() == "42P01"


def test_describe_partitioned_table_unchanged(spark: ReparkSession) -> None:
    """D-DESCRIBE-PLAIN stays: null comments and the partition trailer."""
    spark.sql(
        "CREATE TABLE sc.ns.pt (id BIGINT, data STRING) USING iceberg "
        "PARTITIONED BY (bucket(4, id))"
    )
    assert _rows(spark.sql("DESCRIBE sc.ns.pt")) == [
        ["id", "bigint", None],
        ["data", "string", None],
        ["", "", ""],
        ["# Partitioning", "", ""],
        ["Part 0", "bucket(4, id)", ""],
    ]


def test_describe_missing_namespace_is_table_or_view_not_found(
    spark: ReparkSession,
) -> None:
    """A missing namespace answers the plain 42P01 refusal, not the view probe's error."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql("DESCRIBE sc.missing_ns.t")
    assert (
        str(caught.value) == f"Error during planning: {TABLE_OR_VIEW_NOT_FOUND_MISSING_NAMESPACE}"
    )
    assert caught.value.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
    assert caught.value.getSqlState() == "42P01"


def test_describe_extended_view_is_columns_only(spark: ReparkSession) -> None:
    """EXTENDED on a view is the same deterministic columns-only answer."""
    spark.sql("CREATE VIEW sc.ns.vw_ext AS SELECT id FROM sc.ns.t")
    assert _rows(spark.sql("DESCRIBE EXTENDED sc.ns.vw_ext")) == [["id", "bigint", ""]]
