"""DATE-FN-1 Spark ``date()`` spelling and ``unix_timestamp`` pins."""

from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

from repark.spark import SparkSession
from repark.spark import functions as F  # noqa: N812

_UTC_NOON = 1_718_452_800
_WAIT_MINUTES = 15


def _spark(*, ansi: bool = True, zone: str = "UTC") -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    return (
        SparkSession.builder.appName("date-fn-1")
        .config("spark.sql.ansi.enabled", "true" if ansi else "false")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )


def _sql_arrow(sql: str, *, ansi: bool = True, zone: str = "UTC") -> pa.Table:
    return _spark(ansi=ansi, zone=zone).sql(sql).toArrow()


def _field(table: pa.Table, name: str) -> tuple[str, bool]:
    field = table.schema.field(name)
    return str(field.type), field.nullable


@pytest.mark.parametrize(
    ("sql", "want", "nullable"),
    [
        ("SELECT DATE(TIMESTAMP '2024-06-15 03:00:00') AS d", datetime.date(2024, 6, 15), False),
        ("SELECT DATE(TIMESTAMP '2024-06-15 00:00:00') AS d", datetime.date(2024, 6, 15), False),
        ("SELECT DATE('2024-06-15') AS d", datetime.date(2024, 6, 15), True),
        ("SELECT DATE('2024-06-15 03:00:00') AS d", datetime.date(2024, 6, 15), True),
        ("SELECT DATE(DATE '2024-06-15') AS d", datetime.date(2024, 6, 15), False),
        ("SELECT DATE(NULL) AS d", None, True),
        ("SELECT DATE(CAST(NULL AS TIMESTAMP)) AS d", None, True),
        ("SELECT DATE(CAST(NULL AS STRING)) AS d", None, True),
        ("SELECT DATE(CAST(NULL AS DATE)) AS d", None, True),
    ],
)
def test_date_sql_matches_spark_cast_semantics(
    sql: str, want: datetime.date | None, nullable: bool
) -> None:
    """pins: date-fn-1-spark-date-spelling/C-001, C-003"""
    table = _sql_arrow(sql)
    assert _field(table, "d") == ("date32[day]", nullable)
    assert table.column("d").to_pylist() == [want]


def test_date_invalid_string_errors_when_ansi_on() -> None:
    """pins: date-fn-1-spark-date-spelling/C-001, C-003"""
    with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
        _sql_arrow("SELECT DATE('not-a-date') AS d", ansi=True)


def test_date_invalid_string_is_null_when_ansi_off() -> None:
    """pins: date-fn-1-spark-date-spelling/C-001, C-003"""
    table = _sql_arrow("SELECT DATE('not-a-date') AS d", ansi=False)
    assert _field(table, "d") == ("date32[day]", True)
    assert table.column("d").to_pylist() == [None]


def test_date_equals_cast_on_timestamp() -> None:
    """pins: date-fn-1-spark-date-spelling/C-003"""
    table = _sql_arrow(
        "SELECT DATE(TIMESTAMP '2024-06-15 03:00:00') AS from_date, "
        "CAST(TIMESTAMP '2024-06-15 03:00:00' AS DATE) AS from_cast"
    )
    assert table.column("from_date").to_pylist() == table.column("from_cast").to_pylist()
    assert table.column("from_date").to_pylist() == [datetime.date(2024, 6, 15)]


@pytest.mark.parametrize(
    ("sql", "want", "nullable"),
    [
        ("SELECT unix_timestamp(TIMESTAMP '2024-06-15 12:00:00') AS u", _UTC_NOON, False),
        ("SELECT unix_timestamp(TIMESTAMP '1969-12-31 23:30:00') AS u", -1800, False),
        ("SELECT unix_timestamp('2024-06-15 12:00:00') AS u", _UTC_NOON, True),
        ("SELECT unix_timestamp(CAST(NULL AS TIMESTAMP)) AS u", None, True),
        ("SELECT unix_timestamp(CAST(NULL AS STRING)) AS u", None, True),
        ("SELECT unix_timestamp(NULL) AS u", None, True),
    ],
)
def test_unix_timestamp_sql_matches_spark(sql: str, want: int | None, nullable: bool) -> None:
    """pins: date-fn-1-spark-date-spelling/C-001, C-003"""
    table = _sql_arrow(sql)
    assert _field(table, "u") == ("int64", nullable)
    assert table.column("u").to_pylist() == [want]


def test_unix_timestamp_invalid_string_errors_when_ansi_on() -> None:
    """pins: date-fn-1-spark-date-spelling/C-001, C-003"""
    with pytest.raises(Exception, match="CANNOT_PARSE_TIMESTAMP"):
        _sql_arrow("SELECT unix_timestamp('not-a-timestamp') AS u", ansi=True)


def test_unix_timestamp_invalid_string_is_null_when_ansi_off() -> None:
    """pins: date-fn-1-spark-date-spelling/C-001, C-003"""
    table = _sql_arrow("SELECT unix_timestamp('not-a-timestamp') AS u", ansi=False)
    assert _field(table, "u") == ("int64", True)
    assert table.column("u").to_pylist() == [None]


def test_unix_timestamp_wait_minutes_matches_the_gold_join() -> None:
    """pins: date-fn-1-spark-date-spelling/C-002, C-003"""
    table = _sql_arrow(
        "SELECT CAST((unix_timestamp(TIMESTAMP '2026-01-01 10:15:00') "
        "- unix_timestamp(TIMESTAMP '2026-01-01 10:00:00')) / 60 AS INT) AS w"
    )
    assert str(table.schema.field("w").type) == "int32"
    assert table.column("w").to_pylist() == [_WAIT_MINUTES]


def test_unix_timestamp_zero_arg_is_current_epoch() -> None:
    """pins: date-fn-1-spark-date-spelling/C-003"""
    table = _sql_arrow("SELECT unix_timestamp() AS u")
    assert _field(table, "u") == ("int64", False)
    got = table.column("u").to_pylist()[0]
    assert isinstance(got, int)
    assert got > 1_700_000_000


def test_unix_timestamp_zero_arg_repeats_once_per_input_row() -> None:
    """pins: date-fn-1-spark-date-spelling/C-003"""
    session = _spark()
    sql = session.sql("SELECT unix_timestamp() AS u FROM range(3)").toArrow()
    facade = session.range(3).select(F.unix_timestamp().alias("u")).toArrow()
    sql_rows = sql.column("u").to_pylist()
    facade_rows = facade.column("u").to_pylist()
    assert len(sql_rows) == 3
    assert len(facade_rows) == 3
    assert _field(sql, "u") == ("int64", False)
    assert _field(facade, "u") == ("int64", False)
    assert all(isinstance(value, int) and value > 1_700_000_000 for value in sql_rows)
    assert len(set(sql_rows)) == 1
    assert len(set(facade_rows)) == 1
    assert abs(sql_rows[0] - facade_rows[0]) <= 1


def test_facade_unix_timestamp_matches_sql() -> None:
    """pins: date-fn-1-spark-date-spelling/C-003"""
    session = _spark()
    table = (
        session.range(1).select(F.unix_timestamp(F.lit("2024-06-15 12:00:00")).alias("u")).toArrow()
    )
    assert str(table.schema.field("u").type) == "int64"
    assert table.column("u").to_pylist() == [_UTC_NOON]


def test_facade_has_no_date_name() -> None:
    """pins: date-fn-1-spark-date-spelling/C-001"""
    assert not hasattr(F, "date")
