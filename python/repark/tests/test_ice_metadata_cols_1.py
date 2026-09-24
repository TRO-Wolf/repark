"""IPI-20 PR-1 — the Spark door serves the metadata columns ``_file`` … ``_deleted``.

The five served names are ``_file``, ``_pos``, ``_spec_id``, ``_partition`` and ``_deleted``.

Ten inventory cells replayed verbatim: ``R-MC-FILE``, ``R-MC-FILE-DISTINCT``,
``R-MC-POS``, ``R-MC-FILE-FILTER`` and ``R-MC-SPEC-ID`` over a two-append plus
one-delete seed, ``R-MC-POS-MOR`` over the same seed with a merge-on-read
delete, ``R-MC-PARTITION`` over the same seed, ``R-MC-PARTITION-UNPART`` over
the unpartitioned twin, ``R-MC-SPEC-ID-EVO`` over the spec-evolution twin, and
``R-MC-DELETED`` over the merge-on-read seed — projecting ``_deleted``
surfaces every scanned row including the deleted one; not projecting it keeps
the delete filter. ``SELECT *`` keeps user columns only.

Oracle: the run-25/26 inventory harness cells recorded against live PySpark
4.1.2 + ``iceberg-spark-runtime-4.1_2.13:1.11.0``
(``/tmp/oc-worker/nc-inventory/matrix.json``), whose Spark answers the packet
carries as ``[[2,true,true],[3,true,true],[4,true,true]]`` (``R-MC-FILE``),
``[[3]]`` (``R-MC-FILE-DISTINCT``), ``[[3]]`` (``R-MC-FILE-FILTER``),
``[[2,0],[3,0],[4,0]]`` (``R-MC-POS``), ``[[2,0],[3,0],[4,1]]``
(``R-MC-POS-MOR``), ``[[2,0],[3,0],[4,0]]`` (``R-MC-SPEC-ID``),
``[[2,[["cat","y"]]],[3,[["cat","x"]]],[4,[["cat","x"]]]]``
(``R-MC-PARTITION``), ``[[2,null],[3,null],[4,null]]``
(``R-MC-PARTITION-UNPART``), ``[[1,0,[["cat",null]]],[2,1,[["cat","y"]]]]``
(``R-MC-SPEC-ID-EVO``) and ``[[1,true],[2,false],[3,false],[4,false]]``
(``R-MC-DELETED``, row order not significant).

pins: ice-metadata-cols-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010,
  C-015, C-016, C-017, C-018, C-019, C-020, C-021, C-022
pins: u10-mc-deleted-1/C-001, C-002, C-014, C-015
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.types import BooleanType

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


def _seeded_unpartitioned(session: Any, name: str) -> str:
    """Create the unpartitioned two-append twin, delete id 1, return its name."""
    table = f"{CATALOG}.{NAMESPACE}.{name}"
    session.sql(
        f"CREATE TABLE {table} {SEED_DDL} USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    session.sql(f"INSERT INTO {table} VALUES {FIRST_APPEND}")
    session.sql(f"INSERT INTO {table} VALUES {SECOND_APPEND}")
    session.sql(f"DELETE FROM {table} WHERE id = 1")
    return table


def _seeded_evo(session: Any, name: str) -> str:
    """Create the spec-evolution twin: insert, add the field, insert again."""
    table = f"{CATALOG}.{NAMESPACE}.{name}"
    session.sql(
        f"CREATE TABLE {table} (id BIGINT, cat STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    session.sql(f"INSERT INTO {table} VALUES (1, 'x')")
    session.sql(f"ALTER TABLE {table} ADD PARTITION FIELD cat")
    session.sql(f"INSERT INTO {table} VALUES (2, 'y')")
    return table


def _rows(session: Any, query: str) -> list[list[Any]]:
    """Collect one query as plain lists sorted by id."""
    return sorted((list(row) for row in session.sql(query).collect()), key=lambda row: row[0])


def _field_names(session: Any, query: str) -> list[str]:
    """Field names of a query's schema, in order."""
    return [field.name for field in session.sql(query).schema.fields]


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

    pins: ice-metadata-cols-1/C-006, C-017, C-022
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
    assert [name for name, _ in _schema(spark, f"SELECT *, _spec_id FROM {table}")] == [
        "id",
        "data",
        "cat",
        "_spec_id",
    ]
    assert [name for name, _ in _schema(spark, f"SELECT *, _partition FROM {table}")] == [
        "id",
        "data",
        "cat",
        "_partition",
    ]


def test_spec_id_is_zero_on_a_single_spec_table(spark: Any) -> None:
    """Cell ``R-MC-SPEC-ID``: every live row reports spec 0 as ``int``.

    pins: ice-metadata-cols-1/C-015
    """
    table = _seeded(spark, "t_spec_id")
    assert _schema(spark, f"SELECT id, _spec_id FROM {table}") == [
        ("id", "bigint"),
        ("_spec_id", "int"),
    ]
    assert _rows(spark, f"SELECT id, _spec_id FROM {table}") == [[2, 0], [3, 0], [4, 0]]


def test_partition_struct_answers_spark(spark: Any) -> None:
    """Cell ``R-MC-PARTITION``: every live row serves its ``cat`` partition value.

    pins: ice-metadata-cols-1/C-019
    """
    table = _seeded(spark, "t_partition")
    assert _schema(spark, f"SELECT id, _partition FROM {table}") == [
        ("id", "bigint"),
        ("_partition", "struct<cat:string>"),
    ]
    rows = sorted(
        (row.asDict() for row in spark.sql(f"SELECT id, _partition FROM {table}").collect()),
        key=lambda row: row["id"],
    )
    assert rows == [
        {"id": 2, "_partition": {"cat": "y"}},
        {"id": 3, "_partition": {"cat": "x"}},
        {"id": 4, "_partition": {"cat": "x"}},
    ]
    assert _rows(spark, f"SELECT id, _partition.cat FROM {table}") == [
        [2, "y"],
        [3, "x"],
        [4, "x"],
    ]


def test_partition_is_null_on_unpartitioned_table(spark: Any) -> None:
    """Cell ``R-MC-PARTITION-UNPART``: unpartitioned rows serve a NULL struct.

    pins: ice-metadata-cols-1/C-020
    """
    table = _seeded_unpartitioned(spark, "t_partition_unpart")
    assert _schema(spark, f"SELECT id, _partition FROM {table}") == [
        ("id", "bigint"),
        ("_partition", "struct<>"),
    ]
    assert _rows(spark, f"SELECT id, _partition FROM {table}") == [
        [2, None],
        [3, None],
        [4, None],
    ]


def test_spec_id_and_partition_answer_after_evolution(spark: Any) -> None:
    """Cell ``R-MC-SPEC-ID-EVO``: old-spec rows serve null ``cat``, new rows ``y``.

    pins: ice-metadata-cols-1/C-021
    """
    table = _seeded_evo(spark, "t_evo_full")
    assert _schema(spark, f"SELECT id, _spec_id, _partition FROM {table}") == [
        ("id", "bigint"),
        ("_spec_id", "int"),
        ("_partition", "struct<cat:string>"),
    ]
    rows = sorted(
        (
            row.asDict()
            for row in spark.sql(f"SELECT id, _spec_id, _partition FROM {table}").collect()
        ),
        key=lambda row: row["id"],
    )
    assert rows == [
        {"id": 1, "_spec_id": 0, "_partition": {"cat": None}},
        {"id": 2, "_spec_id": 1, "_partition": {"cat": "y"}},
    ]


def test_deleted_marks_merge_on_read_deleted_row(spark: Any) -> None:
    """Cell ``R-MC-DELETED``: every scanned row, deleted ones ``_deleted = true``.

    pins: u10-mc-deleted-1/C-001
    """
    table = _seeded(spark, "t_deleted", MOR_PROPERTIES)
    frame = spark.sql(f"SELECT id, _deleted FROM {table}")
    fields = {field.name: field for field in frame.schema.fields}
    assert list(fields) == ["id", "_deleted"]
    assert isinstance(fields["_deleted"].dataType, BooleanType)
    assert sorted((list(row) for row in frame.collect()), key=lambda row: row[0]) == [
        [1, True],
        [2, False],
        [3, False],
        [4, False],
    ]


def test_not_projecting_deleted_still_filters(spark: Any) -> None:
    """Near miss: without ``_deleted`` in the projection the delete filter stands.

    pins: u10-mc-deleted-1/C-002
    """
    table = _seeded(spark, "t_deleted_noproj", MOR_PROPERTIES)
    assert _field_names(spark, f"SELECT id FROM {table}") == ["id"]
    assert _rows(spark, f"SELECT id FROM {table}") == [[2], [3], [4]]
    assert _field_names(spark, f"SELECT count(*) FROM {table}") == ["count(*)"]
    assert _rows(spark, f"SELECT count(*) FROM {table}") == [[3]]


def test_user_column_named_deleted_refuses_like_spark(spark: Any) -> None:
    """A user column named ``_deleted`` refuses Spark's reserved-name text; ``*`` still serves.

    Spark raises ``org.apache.iceberg.exceptions.ValidationException`` with this text; RePark
    raises ``AnalysisException`` with the same text after its planning prefix (the class gap is
    IPI-51, residue ``R-MC-RESERVED-NAME-CLASS``).

    pins: u10-mc-deleted-1/C-014, C-015
    """
    table = f"{CATALOG}.{NAMESPACE}.t_user_deleted"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, _deleted STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='2', {MOR_PROPERTIES})"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'u1'), (2, 'u2'), (3, 'u3')")
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"SELECT id, _deleted FROM {table} ORDER BY id").collect()
    assert str(caught.value) == (
        "Error during planning: Table column names conflict with names reserved for "
        "Iceberg metadata columns: [_deleted]. Please, use ALTER TABLE statements to "
        "rename the conflicting table columns."
    )
    assert _field_names(spark, f"SELECT * FROM {table} ORDER BY id") == ["id", "_deleted"]
    rows = [list(row) for row in spark.sql(f"SELECT * FROM {table} ORDER BY id").collect()]
    assert rows == [[2, "u2"], [3, "u3"]]


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


def test_file_values_equal_files_metadata_table(spark: Any) -> None:
    """Every live ``_file`` equals a ``file_path`` of the table's ``files`` table.

    pins: ice-metadata-cols-1/C-010
    """
    table = _seeded(spark, "t_file_identity")
    live = sorted(row[0] for row in spark.sql(f"SELECT DISTINCT _file FROM {table}").collect())
    meta = sorted(row[0] for row in spark.sql(f"SELECT file_path FROM {table}.files").collect())
    assert live == meta
