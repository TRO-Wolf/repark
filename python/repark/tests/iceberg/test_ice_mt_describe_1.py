"""md-r1 — DESCRIBE on an Iceberg metadata table answers one row per column.

``DESCRIBE cat.ns.t.<meta>`` (and ``DESCRIBE TABLE`` / ``DESC``, any metadata
table name) answers ``col_name``, ``data_type`` (the Spark DDL type name),
``comment`` NULL — exactly the columns ``SELECT * FROM cat.ns.t.<meta>``
returns, in the same order. The two-part form after ``USE`` is a
``TABLE_OR_VIEW_NOT_FOUND`` refusal, not rows.

Oracle: cell ``R-MT-DESCRIBE`` (``sb-mt/cells_dfmerge.py``) recorded against
live PySpark 4.1.2 + Iceberg 1.11.0 on 2026-09-22: the six snapshots rows below
with no partition section and no blank rows. The live leg re-measures that cell.

pins: ipi-23-mt-describe-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
pins: ipi-23-mt-describe-1/C-010, C-011, C-012
pins: ipi-23-mt-describe-1/C-013, C-014, C-015
pins: ipi-23-mt-describe-1/C-016, C-018, C-019
pins: ipi-23-mt-describe-1/C-020, C-021, C-022
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live describe oracle is skipped (CI is JVM-free)"

CATALOG = "mt"
NAMESPACE = "ns"
TABLE = "t"

SNAPSHOT_ROWS: list[tuple[str, str, str | None]] = [
    ("committed_at", "timestamp", None),
    ("snapshot_id", "bigint", None),
    ("parent_id", "bigint", None),
    ("operation", "string", None),
    ("manifest_list", "string", None),
    ("summary", "map<string,string>", None),
]

METAS = ("snapshots", "files", "history", "refs", "manifests", "entries")


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with a one-column Iceberg table ``mt.ns.t``."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-ice-mt-describe-1").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    session.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.{TABLE} (id BIGINT) USING iceberg")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _rows(session: Any, sql: str) -> list[tuple[str, str, str | None]]:
    """Collect a DESCRIBE query as plain row tuples on the Arrow path."""
    table = session.sql(sql).to_arrow()
    columns = [table.column(name).to_pylist() for name in table.schema.names]
    return list(zip(*columns, strict=True))


def _assert_full_refusal(
    excinfo: pytest.ExceptionInfo[AnalysisException],
    expected: str,
    condition: str | None,
    sqlstate: str | None,
) -> None:
    """Assert an exception's full message text, Spark condition, and SQLSTATE."""
    assert str(excinfo.value) == expected
    assert excinfo.value.getCondition() == condition
    assert excinfo.value.getSqlState() == sqlstate


def _ddl_name(data_type: pa.DataType) -> str:
    """Spell one Arrow type as the Spark DDL name DESCRIBE prints."""
    if pa.types.is_int64(data_type):
        return "bigint"
    if pa.types.is_int32(data_type):
        return "int"
    if pa.types.is_int16(data_type):
        return "smallint"
    if pa.types.is_int8(data_type):
        return "tinyint"
    if pa.types.is_float64(data_type):
        return "double"
    if pa.types.is_float32(data_type):
        return "float"
    if pa.types.is_boolean(data_type):
        return "boolean"
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return "string"
    if pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
        return "binary"
    if pa.types.is_date32(data_type) or pa.types.is_date64(data_type):
        return "date"
    if pa.types.is_timestamp(data_type):
        return "timestamp" if data_type.tz is not None else "timestamp_ntz"
    if pa.types.is_decimal128(data_type) or pa.types.is_decimal256(data_type):
        return f"decimal({data_type.precision},{data_type.scale})"
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return f"array<{_ddl_name(data_type.value_type)}>"
    if pa.types.is_map(data_type):
        return f"map<{_ddl_name(data_type.key_type)},{_ddl_name(data_type.item_type)}>"
    if pa.types.is_struct(data_type):
        parts = [f"{field.name}:{_ddl_name(field.type)}" for field in data_type]
        return f"struct<{','.join(parts)}>"
    raise TypeError(f"no Spark DDL spelling for {data_type}")


def test_snapshots_describe_matches_spark_rows(spark: Any) -> None:
    """Cell ``R-MT-DESCRIBE``: the six Spark rows, names, types, NULL comments.

    pins: ipi-23-mt-describe-1/C-001
    """
    table = spark.sql(f"DESCRIBE {CATALOG}.{NAMESPACE}.{TABLE}.snapshots").to_arrow()
    assert table.schema.names == ["col_name", "data_type", "comment"]
    assert [field.type for field in table.schema] == [pa.string()] * 3
    assert [field.nullable for field in table.schema] == [False, False, True]
    assert _rows(spark, f"DESCRIBE {CATALOG}.{NAMESPACE}.{TABLE}.snapshots") == SNAPSHOT_ROWS


@pytest.mark.parametrize("meta", METAS)
def test_describe_columns_match_select_star(spark: Any, meta: str) -> None:
    """DESCRIBE's rows are SELECT *'s columns with their Spark DDL type names.

    pins: ipi-23-mt-describe-1/C-002
    """
    target = f"{CATALOG}.{NAMESPACE}.{TABLE}.{meta}"
    selected = spark.sql(f"SELECT * FROM {target}").to_arrow()
    expected = [(field.name, _ddl_name(field.type), None) for field in selected.schema]
    assert _rows(spark, f"DESCRIBE {target}") == expected


def test_table_and_desc_upper_spellings_match(spark: Any) -> None:
    """TABLE / DESC / upper-case suffix / EXTENDED / FORMATTED answer test 1's rows.

    pins: ipi-23-mt-describe-1/C-003
    """
    target = f"{CATALOG}.{NAMESPACE}.{TABLE}.snapshots"
    assert _rows(spark, f"DESCRIBE TABLE {target}") == SNAPSHOT_ROWS
    upper = f"{CATALOG}.{NAMESPACE}.{TABLE}.SNAPSHOTS"
    assert _rows(spark, f"DESC {upper}") == SNAPSHOT_ROWS
    assert _rows(spark, f"DESCRIBE EXTENDED {target}") == SNAPSHOT_ROWS
    assert _rows(spark, f"DESCRIBE TABLE FORMATTED {target}") == SNAPSHOT_ROWS


def test_two_part_after_use_is_plain_not_found(spark: Any) -> None:
    """Q1(a): after USE, the two-part form takes the plain path, not recovery.

    pins: ipi-23-mt-describe-1/C-004
    """
    spark.sql(f"USE {CATALOG}.{NAMESPACE}")
    expected = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`{TABLE}`.`snapshots` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE {TABLE}.snapshots")
    _assert_full_refusal(excinfo, expected, "TABLE_OR_VIEW_NOT_FOUND", "42P01")


def test_plain_table_describe_unchanged(spark: Any) -> None:
    """A plain two-column table still describes exactly its two columns.

    pins: ipi-23-mt-describe-1/C-005
    """
    spark.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.two (a BIGINT, b STRING) USING iceberg")
    assert _rows(spark, f"DESCRIBE {CATALOG}.{NAMESPACE}.two") == [
        ("a", "bigint", None),
        ("b", "string", None),
    ]


def test_real_table_named_snapshots_wins(spark: Any) -> None:
    """A real table named ``snapshots`` describes itself, not a metadata table.

    pins: ipi-23-mt-describe-1/C-006
    """
    spark.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.snapshots (id BIGINT) USING iceberg")
    assert _rows(spark, f"DESCRIBE {CATALOG}.{NAMESPACE}.snapshots") == [("id", "bigint", None)]


def test_missing_base_not_found_names_full_name(spark: Any) -> None:
    """A missing base raises TABLE_OR_VIEW_NOT_FOUND naming the full metadata-table name.

    pins: ipi-23-mt-describe-1/C-007
    """
    expected = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`{NAMESPACE}`.`missing`.`snapshots` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE {CATALOG}.{NAMESPACE}.missing.snapshots")
    _assert_full_refusal(excinfo, expected, "TABLE_OR_VIEW_NOT_FOUND", "42P01")
    assert "$" not in str(excinfo.value)


def test_missing_base_not_found_names_written_case(spark: Any) -> None:
    """A missing base names the four written parts, identifier case kept.

    pins: ipi-23-mt-describe-1/C-018
    """
    template = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        "{name} cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    cases = [
        (
            f"DESCRIBE {CATALOG}.{NAMESPACE}.missing.SNAPSHOTS",
            f"`{CATALOG}`.`{NAMESPACE}`.`missing`.`SNAPSHOTS`",
        ),
        (
            f"DESCRIBE {CATALOG}.{NAMESPACE}.Missing.snapshots",
            f"`{CATALOG}`.`{NAMESPACE}`.`Missing`.`snapshots`",
        ),
    ]
    for sql, name in cases:
        with pytest.raises(AnalysisException) as excinfo:
            spark.sql(sql)
        _assert_full_refusal(
            excinfo, template.format(name=name), "TABLE_OR_VIEW_NOT_FOUND", "42P01"
        )


def test_quoted_dollar_missing_base_names_written_name(spark: Any) -> None:
    """A quoted ``t$snapshots`` with a missing base names the written ``$`` name.

    pins: ipi-23-mt-describe-1/C-019
    """
    expected = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`{NAMESPACE}`.`missing$snapshots` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE {CATALOG}.{NAMESPACE}.`missing$snapshots`")
    _assert_full_refusal(excinfo, expected, "TABLE_OR_VIEW_NOT_FOUND", "42P01")


def test_unknown_suffix_is_not_found_naming_full_name(spark: Any) -> None:
    """An unknown suffix raises TABLE_OR_VIEW_NOT_FOUND naming the full four-part name.

    pins: ipi-23-mt-describe-1/C-008
    """
    expected = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`{NAMESPACE}`.`{TABLE}`.`nope` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE {CATALOG}.{NAMESPACE}.{TABLE}.nope")
    _assert_full_refusal(excinfo, expected, "TABLE_OR_VIEW_NOT_FOUND", "42P01")


def test_explicit_three_part_with_real_namespace_falls_through(spark: Any) -> None:
    """V-001: an explicit three-part name never takes default-namespace recovery.

    pins: ipi-23-mt-describe-1/C-010
    """
    spark.sql(f"USE {CATALOG}.{NAMESPACE}")
    spark.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.otherns (id BIGINT) USING iceberg")
    spark.sql(f"CREATE NAMESPACE {CATALOG}.otherns")
    expected = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`otherns`.`snapshots` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE {CATALOG}.otherns.snapshots")
    _assert_full_refusal(excinfo, expected, "TABLE_OR_VIEW_NOT_FOUND", "42P01")


def test_explicit_three_part_missing_namespace_names_full_name(spark: Any) -> None:
    """V-001: an explicit three-part name answers 42P01 naming the expanded name, not rows.

    pins: ipi-23-mt-describe-1/C-011
    """
    spark.sql(f"USE {CATALOG}.{NAMESPACE}")
    expected = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`{TABLE}`.`snapshots` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE {CATALOG}.{TABLE}.snapshots")
    _assert_full_refusal(excinfo, expected, "TABLE_OR_VIEW_NOT_FOUND", "42P01")


def test_missing_namespace_maps_to_not_found_naming_full_name(spark: Any) -> None:
    """V-002: a missing namespace answers 42P01 naming the full metadata-table name, twice spelled.

    pins: ipi-23-mt-describe-1/C-012
    """
    expected = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`nosuchns`.`{TABLE}`.`snapshots` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE {CATALOG}.nosuchns.{TABLE}.snapshots")
    _assert_full_refusal(excinfo, expected, "TABLE_OR_VIEW_NOT_FOUND", "42P01")
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE EXTENDED {CATALOG}.nosuchns.{TABLE}.snapshots")
    _assert_full_refusal(excinfo, expected, "TABLE_OR_VIEW_NOT_FOUND", "42P01")


def test_quoted_dollar_name_answers_metadata_rows(spark: Any) -> None:
    """Sweep: a quoted ``t$snapshots`` name answers the six snapshots rows.

    pins: ipi-23-mt-describe-1/C-013
    """
    assert _rows(spark, f"DESCRIBE {CATALOG}.{NAMESPACE}.`{TABLE}$snapshots`") == SNAPSHOT_ROWS


def test_dollar_in_base_splits_at_last_dollar(spark: Any) -> None:
    """``a$b$snapshots`` splits at the last ``$``: DESCRIBE matches SELECT's schema.

    pins: ipi-23-mt-describe-1/C-020
    """
    spark.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.`a$b` (id BIGINT) USING iceberg")
    spark.sql(f"INSERT INTO {CATALOG}.{NAMESPACE}.`a$b` VALUES (1)")
    selected = spark.sql(f"SELECT * FROM {CATALOG}.{NAMESPACE}.`a$b$snapshots`").to_arrow()
    expected = [(field.name, _ddl_name(field.type), None) for field in selected.schema]
    described = _rows(spark, f"DESCRIBE {CATALOG}.{NAMESPACE}.`a$b$snapshots`")
    assert described == expected
    assert described != _rows(spark, f"DESCRIBE {CATALOG}.{NAMESPACE}.`a$b`")


def test_dollar_base_describes_itself(spark: Any) -> None:
    """DESCRIBE of the ``a$b`` base answers its own columns, not metadata rows.

    pins: ipi-23-mt-describe-1/C-021
    """
    spark.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.`a$b` (id BIGINT) USING iceberg")
    assert _rows(spark, f"DESCRIBE {CATALOG}.{NAMESPACE}.`a$b`") == [("id", "bigint", None)]


def test_dollar_unknown_suffix_keeps_not_found(spark: Any) -> None:
    """``a$b$nonsense`` keeps the measured TABLE_OR_VIEW_NOT_FOUND refusal.

    pins: ipi-23-mt-describe-1/C-022
    """
    spark.sql(f"CREATE TABLE {CATALOG}.{NAMESPACE}.`a$b` (id BIGINT) USING iceberg")
    expected = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`{NAMESPACE}`.`a$b$nonsense` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE {CATALOG}.{NAMESPACE}.`a$b$nonsense`")
    _assert_full_refusal(excinfo, expected, "TABLE_OR_VIEW_NOT_FOUND", "42P01")


def test_explicit_three_part_namespace_shadowing_base_falls_through(spark: Any) -> None:
    """V-001 class: a namespace named like the base table still falls through.

    pins: ipi-23-mt-describe-1/C-014
    """
    spark.sql(f"USE {CATALOG}.{NAMESPACE}")
    spark.sql(f"CREATE NAMESPACE {CATALOG}.{TABLE}")
    expected = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`{TABLE}`.`snapshots` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE {CATALOG}.{TABLE}.snapshots")
    _assert_full_refusal(excinfo, expected, "TABLE_OR_VIEW_NOT_FOUND", "42P01")


def test_four_part_extended_formatted_matrix(spark: Any) -> None:
    """Sweep: EXTENDED/FORMATTED of every settled four-part case match plain.

    pins: ipi-23-mt-describe-1/C-015
    """
    missing = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`{NAMESPACE}`.`missing`.`snapshots` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE EXTENDED {CATALOG}.{NAMESPACE}.missing.snapshots")
    _assert_full_refusal(excinfo, missing, "TABLE_OR_VIEW_NOT_FOUND", "42P01")
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE TABLE FORMATTED {CATALOG}.{NAMESPACE}.missing.snapshots")
    _assert_full_refusal(excinfo, missing, "TABLE_OR_VIEW_NOT_FOUND", "42P01")
    unknown = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`{NAMESPACE}`.`{TABLE}`.`nope` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE EXTENDED {CATALOG}.{NAMESPACE}.{TABLE}.nope")
    _assert_full_refusal(excinfo, unknown, "TABLE_OR_VIEW_NOT_FOUND", "42P01")
    upper = f"{CATALOG}.{NAMESPACE}.{TABLE}.SNAPSHOTS"
    assert _rows(spark, f"DESCRIBE EXTENDED {upper}") == SNAPSHOT_ROWS
    absent_ns = (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        f"`{CATALOG}`.`nosuchns`.`{TABLE}`.`snapshots` cannot be found. Verify the spelling and "
        "correctness of the schema and catalog. If you did not qualify the name with a "
        "schema, verify the current_schema() output, or qualify the name with the correct "
        "schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP "
        "TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"DESCRIBE TABLE FORMATTED {CATALOG}.nosuchns.{TABLE}.snapshots")
    _assert_full_refusal(excinfo, absent_ns, "TABLE_OR_VIEW_NOT_FOUND", "42P01")


@pytest.mark.xfail(
    strict=True,
    reason="AnalysisException: Error during planning: table 'ns.t.snapshots' not found",
)
def test_namespaced_table_meta_after_use_answers_rows(spark: Any) -> None:
    """Spark answers rows for the namespace-qualified meta name after USE.

    pins: ipi-23-mt-describe-1/C-016
    """
    spark.sql(f"USE {CATALOG}.{NAMESPACE}")
    assert _rows(spark, f"DESCRIBE {NAMESPACE}.{TABLE}.snapshots") == SNAPSHOT_ROWS


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_snapshots_describe_live_matches_spark_and_repark(tmp_path: Path) -> None:
    """Live leg: Spark answers the six rows; repark matches them row for row.

    pins: ipi-23-mt-describe-1/C-001
    """
    import _live_parity as live_parity

    oracle = live_parity.build_spark_iceberg_engine(tmp_path / "spark-wh", catalog="mtdesc")
    session = oracle.session
    session.sql("CREATE NAMESPACE IF NOT EXISTS mtdesc.dsns1")
    session.sql("CREATE TABLE mtdesc.dsns1.t1 (id BIGINT) USING iceberg")
    live = [tuple(row) for row in session.sql("DESCRIBE mtdesc.dsns1.t1.snapshots").collect()]
    assert live == SNAPSHOT_ROWS

    engine = ReparkSession.builder.appName("pytest-mt-describe-1-live").getOrCreate()
    engine.register_memory_catalog("livecheck", tmp_path / "repark-wh")
    try:
        engine.sql("CREATE NAMESPACE livecheck.dsns1")
        engine.sql("CREATE TABLE livecheck.dsns1.t1 (id BIGINT) USING iceberg")
        repark_rows = _rows(engine, "DESCRIBE livecheck.dsns1.t1.snapshots")
    finally:
        engine.stop()

    assert repark_rows == live
