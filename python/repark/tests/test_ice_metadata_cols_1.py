"""IPI-20 PR-1 — the Spark door serves ``_file`` and ``_pos`` on Iceberg reads.

Five inventory cells replayed verbatim: ``R-MC-FILE``, ``R-MC-FILE-DISTINCT``,
``R-MC-POS`` and ``R-MC-FILE-FILTER`` over a two-append plus one-delete seed,
and ``R-MC-POS-MOR`` over the same seed with a merge-on-read delete. The three
columns the fork pin cannot serve yet — ``_spec_id``, ``_partition``,
``_deleted`` — refuse typed with ``[ICE-MC-1]`` instead of the raw planner
error. ``SELECT *`` keeps user columns only.

Oracle: the run-25/26 inventory harness cells recorded against live PySpark
4.1.2 + ``iceberg-spark-runtime-4.1_2.13:1.11.0``
(``/tmp/oc-worker/nc-inventory/matrix.json``), whose Spark answers the packet
carries as ``[[2,true,true],[3,true,true],[4,true,true]]`` (``R-MC-FILE``),
``[[3]]`` (``R-MC-FILE-DISTINCT``), ``[[3]]`` (``R-MC-FILE-FILTER``),
``[[2,0],[3,0],[4,0]]`` (``R-MC-POS``) and ``[[2,0],[3,0],[4,1]]``
(``R-MC-POS-MOR``).

pins: ice-metadata-cols-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session import _reset_active_session_for_tests

CATALOG = "mc"
NAMESPACE = "ns"
SEED_DDL = "(id BIGINT, data STRING, cat STRING)"
PARTITIONED_BY_CAT = "PARTITIONED BY (cat)"
FIRST_APPEND = "(1, 'a', 'x'), (2, 'b', 'y'), (4, 'd', 'x')"
SECOND_APPEND = "(3, 'c', 'x')"
MOR_PROPERTIES = "'write.delete.mode'='merge-on-read'"


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with an in-memory Iceberg catalog named ``mc``."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-ice-metadata-cols-1").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    yield session
    session.stop()
    _reset_active_session_for_tests()


@pytest.fixture
def spark_v3(tmp_path: Path) -> Any:
    """Yield a facade session with v3 CREATE allowed and catalog ``mc``."""
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-ice-metadata-cols-1-v3")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog(CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _seeded(session: Any, name: str, properties: str = "") -> str:
    """Create a two-append table, delete id 1, and return its three-part name."""
    table = f"{CATALOG}.{NAMESPACE}.{name}"
    tail = f", {properties}" if properties else ""
    session.sql(
        f"CREATE TABLE {table} {SEED_DDL} USING iceberg {PARTITIONED_BY_CAT} "
        f"TBLPROPERTIES ('format-version'='2'{tail})"
    )
    session.sql(f"INSERT INTO {table} VALUES {FIRST_APPEND}")
    session.sql(f"INSERT INTO {table} VALUES {SECOND_APPEND}")
    session.sql(f"DELETE FROM {table} WHERE id = 1")
    return table


def _rows(session: Any, query: str) -> list[list[Any]]:
    """Collect one query as plain lists sorted by id."""
    return sorted((list(row) for row in session.sql(query).collect()), key=lambda row: row[0])


def _schema(session: Any, query: str) -> list[tuple[str, str]]:
    """``(name, simpleString)`` per field of a query's schema."""
    frame = session.sql(query)
    return [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]


def test_file_predicates_answer_spark(spark: Any) -> None:
    """Cell ``R-MC-FILE``: every live row names a parquet data file.

    pins: ice-metadata-cols-1/C-001
    """
    table = _seeded(spark, "t_file")
    assert _schema(spark, f"SELECT id, _file FROM {table}") == [
        ("id", "bigint"),
        ("_file", "string"),
    ]
    assert _rows(
        spark,
        f"SELECT id, _file LIKE '%.parquet', _file LIKE '%/data/%' FROM {table}",
    ) == [[2, True, True], [3, True, True], [4, True, True]]


def test_file_distinct_counts_live_files(spark: Any) -> None:
    """Cell ``R-MC-FILE-DISTINCT``: three live data files after the rewrite.

    pins: ice-metadata-cols-1/C-002
    """
    table = _seeded(spark, "t_file_distinct")
    assert _rows(spark, f"SELECT count(DISTINCT _file) FROM {table}") == [[3]]


def test_file_filter_counts_non_null(spark: Any) -> None:
    """Cell ``R-MC-FILE-FILTER``: the filter keeps all three live rows.

    pins: ice-metadata-cols-1/C-003
    """
    table = _seeded(spark, "t_file_filter")
    assert _rows(spark, f"SELECT count(*) FROM {table} WHERE _file IS NOT NULL") == [[3]]


def test_pos_is_zero_based_file_ordinal(spark: Any) -> None:
    """Cell ``R-MC-POS``: one survivor per file, each at position zero.

    pins: ice-metadata-cols-1/C-004
    """
    table = _seeded(spark, "t_pos")
    assert _schema(spark, f"SELECT id, _pos FROM {table}") == [
        ("id", "bigint"),
        ("_pos", "bigint"),
    ]
    assert _rows(spark, f"SELECT id, _pos FROM {table}") == [[2, 0], [3, 0], [4, 0]]


def test_pos_survives_merge_on_read_delete(spark: Any) -> None:
    """Cell ``R-MC-POS-MOR``: ``_pos`` is the file ordinal, kept across the delete.

    pins: ice-metadata-cols-1/C-005
    """
    table = _seeded(spark, "t_pos_mor", MOR_PROPERTIES)
    assert _rows(spark, f"SELECT id, _pos FROM {table}") == [[2, 0], [3, 0], [4, 1]]


def test_star_excludes_served_metadata_columns(spark: Any) -> None:
    """Cell ``R-MC-STAR-EXCLUDES``: ``*`` stays user columns; explicit names compose.

    pins: ice-metadata-cols-1/C-006
    """
    table = _seeded(spark, "t_star")
    assert [name for name, _ in _schema(spark, f"SELECT * FROM {table}")] == [
        "id",
        "data",
        "cat",
    ]
    assert [name for name, _ in _schema(spark, f"SELECT *, _file FROM {table}")] == [
        "id",
        "data",
        "cat",
        "_file",
    ]
    assert [name for name, _ in _schema(spark, f"SELECT *, _pos FROM {table}")] == [
        "id",
        "data",
        "cat",
        "_pos",
    ]


def test_unserved_metadata_columns_refuse_typed(spark: Any) -> None:
    """``_spec_id`` / ``_partition`` / ``_deleted`` refuse ``[ICE-MC-1]``, never raw.

    pins: ice-metadata-cols-1/C-007
    """
    table = _seeded(spark, "t_refuse")
    for column in ["_spec_id", "_partition", "_deleted"]:
        with pytest.raises(AnalysisException) as caught:
            spark.sql(f"SELECT {column} FROM {table}").collect()
        text = str(caught.value)
        assert "[ICE-MC-1]" in text
        assert "No field named" not in text
        assert column in text


def test_file_and_row_id_answer_together_on_v3(spark_v3: Any) -> None:
    """``_file`` + ``_row_id`` on a format-v3 table: the rewrite order serves both.

    pins: ice-metadata-cols-1/C-009
    """
    table = f"{CATALOG}.{NAMESPACE}.t_v3_both"
    spark_v3.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='3')"
    )
    spark_v3.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    assert _schema(spark_v3, f"SELECT _file, _row_id FROM {table}") == [
        ("_file", "string"),
        ("_row_id", "bigint"),
    ]
    rows = sorted(
        (list(row) for row in spark_v3.sql(f"SELECT _file, _row_id FROM {table}").collect()),
        key=lambda row: row[1],
    )
    assert [row[1] for row in rows] == [0, 1]
    assert all(row[0].endswith(".parquet") for row in rows)
