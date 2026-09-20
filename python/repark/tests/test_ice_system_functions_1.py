"""IPI-29 system functions rounds 1-3 — the catalog-qualified system-function pins.

Round 1 of 3 on lane xb-sysfn: the Iceberg ``bucket(n, col)`` and
``truncate(w, col)`` system functions as DataFusion scalar UDFs under reserved
internal names. Round 2 adds the temporal functions (``years``, ``months``,
``days``, ``hours``) and ``iceberg_version``. Round 3 resolves the
catalog-qualified ``<cat>.system.<fn>`` spellings through a pre-parse rewrite
and intercepts ``SHOW [USER] FUNCTIONS IN <cat>.system``. Every value pin
replays the inventory fixture from cells_misc.py against module-private memory
catalogs and asserts the exact recorded Spark answer on the Arrow path, value
AND type.

pins: ice-system-functions-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-019, C-020,
C-021, C-022, C-023, C-024, C-025
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException

CATALOG = "sc"
TABLE = f"{CATALOG}.w.t"
BUCKET = "sc.system.bucket"
TRUNCATE = "sc.system.truncate"
YEARS = "sc.system.years"
MONTHS = "sc.system.months"
DAYS = "sc.system.days"
HOURS = "sc.system.hours"
ICEBERG_VERSION = "sc.system.iceberg_version"
INTERNAL_BUCKET = "__iceberg_system_bucket"


def _register_fixture_catalog(session: ReparkSession, warehouse: Path, catalog: str) -> str:
    """Register a catalog carrying the IPI-29 fixture table; return the table name."""
    session.register_memory_catalog(catalog, warehouse)
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.w")
    table = f"{catalog}.w.t"
    session.sql(
        f"CREATE TABLE {table} "
        "(id BIGINT, data STRING, ts TIMESTAMP, d DATE, dec DECIMAL(10,2), b BINARY) "
        "USING iceberg"
    ).collect()
    session.sql(
        f"INSERT INTO {table} VALUES "
        "(1, 'abcdef', TIMESTAMP'2024-03-05 10:11:12', DATE'2024-03-05', 12.34, X'0102'), "
        "(-7, 'z', TIMESTAMP'1969-12-31 23:00:00', DATE'1969-12-31', -5.55, X'FF'), "
        "(NULL, NULL, NULL, NULL, NULL, NULL)"
    ).collect()
    return table


@pytest.fixture()
def engine(tmp_path: Path) -> Iterator[ReparkSession]:
    """Session with the IPI-29 fixture table on catalog sc in UTC."""
    session = (
        ReparkSession.builder.appName("pytest-ice-system-functions-1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    _register_fixture_catalog(session, tmp_path / "wh-sc", CATALOG)
    yield session
    session.stop()


def _nulls_last_key(row: list[Any]) -> list[tuple[bool, Any]]:
    """Sort key with NULL cells after every value."""
    return [(cell is None, cell) for cell in row]


def _sorted_rows(table: pa.Table) -> list[list[Any]]:
    """Rows as lists, ordered with NULLs last so scan order cannot flake the pin."""
    rows = [list(row.values()) for row in table.to_pylist()]
    return sorted(rows, key=_nulls_last_key)


def test_bucket_long_pins_recorded_values(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-001 — F-BUCKET-LONG answers [[4],[5],[null]] as INT."""
    table = engine.sql(f"SELECT {BUCKET}(16, id) AS v FROM {TABLE}").to_arrow()
    assert table.schema.field("v").type == pa.int32()
    assert _sorted_rows(table) == [[4], [5], [None]]


def test_bucket_string_pins_recorded_values(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-002 — F-BUCKET-STRING answers [[5],[7],[null]]."""
    table = engine.sql(f"SELECT {BUCKET}(16, data) AS v FROM {TABLE}").to_arrow()
    assert table.schema.field("v").type == pa.int32()
    assert _sorted_rows(table) == [[5], [7], [None]]


def test_bucket_date_and_timestamp_pin_recorded_values(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-003 — F-BUCKET-DATE answers [[0,2],[5,2],[null,null]]."""
    table = engine.sql(f"SELECT {BUCKET}(8, d) AS a, {BUCKET}(8, ts) AS b FROM {TABLE}").to_arrow()
    assert table.schema.field("a").type == pa.int32()
    assert table.schema.field("b").type == pa.int32()
    assert _sorted_rows(table) == [[0, 2], [5, 2], [None, None]]


def test_bucket_decimal_and_binary_pin_recorded_values(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-004 — F-BUCKET-DECIMAL-BINARY answers the recorded pairs."""
    table = engine.sql(f"SELECT {BUCKET}(8, dec) AS a, {BUCKET}(8, b) AS c FROM {TABLE}").to_arrow()
    assert table.schema.field("a").type == pa.int32()
    assert table.schema.field("c").type == pa.int32()
    assert _sorted_rows(table) == [[0, 6], [5, 5], [None, None]]


def test_truncate_string_pins_recorded_values(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-005 — F-TRUNCATE-STRING answers ab, z and null."""
    table = engine.sql(f"SELECT {TRUNCATE}(2, data) AS v FROM {TABLE}").to_arrow()
    assert table.schema.field("v").type == pa.string()
    assert _sorted_rows(table) == [["ab"], ["z"], [None]]


def test_truncate_long_pins_recorded_values(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-006 — F-TRUNCATE-LONG answers [[-10],[0],[null]]."""
    table = engine.sql(f"SELECT {TRUNCATE}(10, id) AS v FROM {TABLE}").to_arrow()
    assert table.schema.field("v").type == pa.int64()
    assert _sorted_rows(table) == [[-10], [0], [None]]


def test_truncate_decimal_pins_recorded_values_and_schema(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-007 — F-TRUNCATE-DECIMAL answers -6 and 12 as decimal."""
    frame = engine.sql(f"SELECT {TRUNCATE}(100, dec) AS v FROM {TABLE}")
    table = frame.to_arrow()
    assert table.schema.field("v").type == pa.decimal128(10, 2)
    assert str(frame.schema.fields[0].dataType) == "DecimalType(10,2)"
    assert _sorted_rows(table) == [[Decimal("-6")], [Decimal("12")], [None]]


def test_truncate_binary_pins_recorded_values_and_schema(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-008 — F-TRUNCATE-BINARY answers [0x01, 0xff] as binary."""
    frame = engine.sql(f"SELECT {TRUNCATE}(1, b) AS v FROM {TABLE}")
    table = frame.to_arrow()
    assert table.schema.field("v").type == pa.large_binary()
    assert str(frame.schema.fields[0].dataType) == "BinaryType()"
    assert _sorted_rows(table) == [[b"\x01"], [b"\xff"], [None]]


def test_every_function_returns_null_for_null_input(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-009, C-017 — NULL in gives NULL out, all six functions."""
    table = engine.sql(
        f"SELECT {BUCKET}(16, id) AS a, {BUCKET}(16, data) AS b, "
        f"{BUCKET}(8, d) AS c, {BUCKET}(8, ts) AS d, "
        f"{BUCKET}(8, dec) AS e, {BUCKET}(8, b) AS f, "
        f"{TRUNCATE}(2, data) AS g, {TRUNCATE}(10, id) AS h, "
        f"{TRUNCATE}(100, dec) AS i, {TRUNCATE}(1, b) AS j, "
        f"{YEARS}(ts) AS k, {MONTHS}(ts) AS l, {DAYS}(ts) AS m, "
        f"{DAYS}(d) AS n, {HOURS}(ts) AS o "
        f"FROM {TABLE} WHERE id IS NULL"
    ).to_arrow()
    assert table.num_rows == 1
    assert all(cell is None for cell in table.to_pylist()[0].values())


def test_bucket_width_must_be_a_positive_literal(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-010 — zero and negative widths refuse with the fork text."""
    with pytest.raises(PySparkException, match=r"Invalid number of buckets: 0 \(must be > 0\)"):
        engine.sql(f"SELECT {BUCKET}(0, id) AS v FROM {TABLE}").to_arrow()
    with pytest.raises(PySparkException, match=r"Invalid number of buckets: -1 \(must be > 0\)"):
        engine.sql(f"SELECT {BUCKET}(-1, id) AS v FROM {TABLE}").to_arrow()
    with pytest.raises(PySparkException, match=r"Invalid truncate width: 0 \(must be > 0\)"):
        engine.sql(f"SELECT {TRUNCATE}(0, data) AS v FROM {TABLE}").to_arrow()


def test_bucket_old_argument_order_refuses(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-010 — bucket(id, 16) refuses: the width is not a scalar."""
    with pytest.raises(PySparkException, match="scalar"):
        engine.sql(f"SELECT {BUCKET}(id, 16) AS v FROM {TABLE}").to_arrow()


def test_truncate_string_shorter_than_width_returns_whole(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-005 — truncate(2, 'z') keeps the whole short string."""
    table = engine.sql(f"SELECT {TRUNCATE}(2, data) AS v FROM {TABLE} WHERE id = -7").to_arrow()
    assert table.column("v").to_pylist() == ["z"]


def test_truncate_long_negative_floors_down(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-006 — truncate(10, -7) floors to -10, not toward zero."""
    table = engine.sql(f"SELECT {TRUNCATE}(10, id) AS v FROM {TABLE} WHERE id = -7").to_arrow()
    assert table.column("v").to_pylist() == [-10]


def test_years_pins_recorded_values(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-012 — F-YEARS answers [[-1,-1],[54,54],[null,null]]."""
    table = engine.sql(f"SELECT {YEARS}(ts) AS a, {YEARS}(d) AS b FROM {TABLE}").to_arrow()
    assert table.schema.field("a").type == pa.int32()
    assert table.schema.field("b").type == pa.int32()
    assert _sorted_rows(table) == [[-1, -1], [54, 54], [None, None]]


def test_months_pins_recorded_values(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-013 — F-MONTHS answers [[-1,-1],[650,650],[null,null]]."""
    table = engine.sql(f"SELECT {MONTHS}(ts) AS a, {MONTHS}(d) AS b FROM {TABLE}").to_arrow()
    assert table.schema.field("a").type == pa.int32()
    assert table.schema.field("b").type == pa.int32()
    assert _sorted_rows(table) == [[-1, -1], [650, 650], [None, None]]


def test_days_pins_recorded_values_and_schema(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-014 — F-DAYS answers dates as DATE, not int or string."""
    frame = engine.sql(f"SELECT {DAYS}(ts) AS a, {DAYS}(d) AS b FROM {TABLE}")
    table = frame.to_arrow()
    assert table.schema.field("a").type == pa.date32()
    assert table.schema.field("b").type == pa.date32()
    assert [str(field.dataType) for field in frame.schema.fields] == ["DateType()", "DateType()"]
    assert _sorted_rows(table) == [
        [date(1969, 12, 31), date(1969, 12, 31)],
        [date(2024, 3, 5), date(2024, 3, 5)],
        [None, None],
    ]


def test_hours_pins_recorded_values(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-015 — F-HOURS answers [[-1],[474898],[null]]."""
    table = engine.sql(f"SELECT {HOURS}(ts) AS v FROM {TABLE}").to_arrow()
    assert table.schema.field("v").type == pa.int32()
    assert _sorted_rows(table) == [[-1], [474898], [None]]


def test_iceberg_version_is_non_null_per_row(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-016 — the version is a stable non-null string per row."""
    frame = engine.sql(f"SELECT {ICEBERG_VERSION}() AS v FROM {TABLE}")
    table = frame.to_arrow()
    assert table.schema.field("v").type == pa.string()
    assert str(frame.schema.fields[0].dataType) == "StringType()"
    values = table.column("v").to_pylist()
    assert len(values) == 3
    assert all(isinstance(value, str) for value in values)
    assert len(set(values)) == 1
    flags = engine.sql(f"SELECT {ICEBERG_VERSION}() IS NOT NULL AS ok FROM {TABLE}").to_arrow()
    assert flags.column("ok").to_pylist() == [True, True, True]


def test_years_pre_epoch_is_negative(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-012 — years of a 1969 instant is -1, not 0."""
    table = engine.sql(
        f"SELECT {YEARS}(ts) AS a, {YEARS}(d) AS b FROM {TABLE} WHERE id = -7"
    ).to_arrow()
    assert table.column("a").to_pylist() == [-1]
    assert table.column("b").to_pylist() == [-1]


def test_bucket_in_where_pins_recorded_count(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-019 — F-BUCKET-IN-WHERE counts 1 row in a filter."""
    table = engine.sql(
        f"SELECT count(*) FILTER (WHERE {BUCKET}(4, id) = 1) AS v FROM {TABLE}"
    ).to_arrow()
    assert table.column("v").to_pylist() == [1]


def test_show_user_functions_in_system_pins_roster(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-020 — F-SHOW-FUNCTIONS lists 7 qualified names sorted."""
    frame = engine.sql("SHOW USER FUNCTIONS IN sc.system")
    table = frame.to_arrow()
    assert table.schema.field("function").type == pa.string()
    assert table.column("function").to_pylist() == [
        "sc.system.bucket",
        "sc.system.days",
        "sc.system.hours",
        "sc.system.iceberg_version",
        "sc.system.months",
        "sc.system.truncate",
        "sc.system.years",
    ]
    bare = engine.sql("SHOW FUNCTIONS IN sc.system").to_arrow()
    assert bare.column("function").to_pylist() == table.column("function").to_pylist()


def test_bare_truncate_still_resolves_to_the_builtin(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-021 — unqualified truncate never becomes Iceberg."""
    with pytest.raises(AnalysisException, match=r"UNRESOLVED_ROUTINE.*`truncate`"):
        engine.sql("SELECT truncate(1.9) AS v").to_arrow()
    with pytest.raises(AnalysisException, match=r"UNRESOLVED_ROUTINE.*`truncate`"):
        engine.sql(f"SELECT truncate(2, data) AS v FROM {TABLE}").to_arrow()


def test_functions_resolve_under_every_configured_catalog(
    engine: ReparkSession, tmp_path: Path
) -> None:
    """pins: ice-system-functions-1/C-022 — hc resolves calls and SHOW like sc does."""
    table = _register_fixture_catalog(engine, tmp_path / "wh-hc", "hc")
    values = engine.sql(f"SELECT hc.system.bucket(16, id) AS v FROM {table}").to_arrow()
    assert _sorted_rows(values) == [[4], [5], [None]]
    roster = engine.sql("SHOW USER FUNCTIONS IN hc.system").to_arrow()
    assert roster.column("function").to_pylist() == [
        "hc.system.bucket",
        "hc.system.days",
        "hc.system.hours",
        "hc.system.iceberg_version",
        "hc.system.months",
        "hc.system.truncate",
        "hc.system.years",
    ]


def test_show_functions_without_in_is_unchanged(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-023 — SHOW forms without IN keep today's behavior."""
    with pytest.raises(AnalysisException, match=r"information_schema\.parameters"):
        engine.sql("SHOW FUNCTIONS").to_arrow()
    with pytest.raises(AnalysisException, match=r"SHOW \[VARIABLE\] is not supported"):
        engine.sql("SHOW USER FUNCTIONS").to_arrow()
    with pytest.raises(AnalysisException, match=r"SHOW \[VARIABLE\] is not supported"):
        engine.sql("SHOW SYSTEM FUNCTIONS").to_arrow()


def test_show_functions_in_non_system_scope_is_unchanged(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-023 — non-`.system` IN scopes keep today's error."""
    message = re.escape(
        "Error during planning: SHOW [VARIABLE] is not supported "
        "unless information_schema is enabled"
    )
    with pytest.raises(AnalysisException, match=message):
        engine.sql("SHOW USER FUNCTIONS IN sc.sales").to_arrow()
    with pytest.raises(AnalysisException, match=message):
        engine.sql("SHOW USER FUNCTIONS IN sc").to_arrow()
    with pytest.raises(AnalysisException, match=message):
        engine.sql("SHOW USER FUNCTIONS IN sc.system EXTRA").to_arrow()


def test_internal_bucket_name_still_resolves(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-025 — the internal UDF name answers directly."""
    table = engine.sql(f"SELECT {INTERNAL_BUCKET}(16, id) AS v FROM {TABLE}").to_arrow()
    assert table.schema.field("v").type == pa.int32()
    assert _sorted_rows(table) == [[4], [5], [None]]
