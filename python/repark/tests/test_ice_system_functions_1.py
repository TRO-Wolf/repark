"""IPI-29 system functions round 1 — bucket/truncate UDF pins on the recorded Spark values.

Round 1 of 3 on lane xb-sysfn: the Iceberg ``bucket(n, col)`` and
``truncate(w, col)`` system functions as DataFusion scalar UDFs under reserved
internal names. Temporal functions, ``iceberg_version``, catalog-qualified
registration and SHOW arrive in later rounds. Every value pin replays the
inventory fixture from cells_misc.py against a module-private memory catalog
and asserts the exact recorded Spark answer on the Arrow path, value AND type.

pins: ice-system-functions-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
C-009, C-010, C-011
"""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession

CATALOG = "sysfn1"
TABLE = f"{CATALOG}.w.t"
BUCKET = "__iceberg_system_bucket"
TRUNCATE = "__iceberg_system_truncate"


@pytest.fixture()
def engine(tmp_path: Path) -> Iterator[ReparkSession]:
    """Session with the IPI-29 fixture table on a module-private catalog in UTC."""
    warehouse = tmp_path / "wh"
    session = (
        ReparkSession.builder.appName("pytest-ice-system-functions-1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.register_memory_catalog(CATALOG, warehouse)
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
    session.sql(
        f"CREATE TABLE {TABLE} "
        "(id BIGINT, data STRING, ts TIMESTAMP, d DATE, dec DECIMAL(10,2), b BINARY) "
        "USING iceberg"
    ).collect()
    session.sql(
        f"INSERT INTO {TABLE} VALUES "
        "(1, 'abcdef', TIMESTAMP'2024-03-05 10:11:12', DATE'2024-03-05', 12.34, X'0102'), "
        "(-7, 'z', TIMESTAMP'1969-12-31 23:00:00', DATE'1969-12-31', -5.55, X'FF'), "
        "(NULL, NULL, NULL, NULL, NULL, NULL)"
    ).collect()
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
    assert table.schema.field("v").type == pa.binary()
    assert str(frame.schema.fields[0].dataType) == "BinaryType()"
    assert _sorted_rows(table) == [[b"\x01"], [b"\xff"], [None]]


def test_every_function_returns_null_for_null_input(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-009 — NULL in gives NULL out for bucket and truncate."""
    table = engine.sql(
        f"SELECT {BUCKET}(16, id) AS a, {BUCKET}(16, data) AS b, "
        f"{BUCKET}(8, d) AS c, {BUCKET}(8, ts) AS d, "
        f"{BUCKET}(8, dec) AS e, {BUCKET}(8, b) AS f, "
        f"{TRUNCATE}(2, data) AS g, {TRUNCATE}(10, id) AS h, "
        f"{TRUNCATE}(100, dec) AS i, {TRUNCATE}(1, b) AS j "
        f"FROM {TABLE} WHERE id IS NULL"
    ).to_arrow()
    assert table.num_rows == 1
    assert all(cell is None for cell in table.to_pylist()[0].values())


def test_bucket_width_must_be_a_positive_literal(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-010 — zero and negative widths refuse with the fork text."""
    with pytest.raises(Exception, match=r"Invalid number of buckets: 0 \(must be > 0\)"):
        engine.sql(f"SELECT {BUCKET}(0, id) AS v FROM {TABLE}").to_arrow()
    with pytest.raises(Exception, match=r"Invalid number of buckets: -1 \(must be > 0\)"):
        engine.sql(f"SELECT {BUCKET}(-1, id) AS v FROM {TABLE}").to_arrow()
    with pytest.raises(Exception, match=r"Invalid truncate width: 0 \(must be > 0\)"):
        engine.sql(f"SELECT {TRUNCATE}(0, data) AS v FROM {TABLE}").to_arrow()


def test_bucket_old_argument_order_refuses(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-010 — bucket(id, 16) refuses: the width is not a scalar."""
    with pytest.raises(Exception, match="scalar"):
        engine.sql(f"SELECT {BUCKET}(id, 16) AS v FROM {TABLE}").to_arrow()


def test_truncate_string_shorter_than_width_returns_whole(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-005 — truncate(2, 'z') keeps the whole short string."""
    table = engine.sql(f"SELECT {TRUNCATE}(2, data) AS v FROM {TABLE} WHERE id = -7").to_arrow()
    assert table.column("v").to_pylist() == ["z"]


def test_truncate_long_negative_floors_down(engine: ReparkSession) -> None:
    """pins: ice-system-functions-1/C-006 — truncate(10, -7) floors to -10, not toward zero."""
    table = engine.sql(f"SELECT {TRUNCATE}(10, id) AS v FROM {TABLE} WHERE id = -7").to_arrow()
    assert table.column("v").to_pylist() == [-10]
