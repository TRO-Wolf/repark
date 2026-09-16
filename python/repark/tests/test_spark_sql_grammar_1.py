"""SPARK-SQL-GRAMMAR-1 door pins — Spark operators, keywords and type names.

Oracle: PySpark 4.1.2 ``fixtures-batch3.json`` PG-* cells, ``fixtures-batch14``
Q14-* cells and the ``fnp11_spark_oracle.json`` SQL-door bare-unit cells (see
the unit ledger ``task/ledgers/staging/spark-sql-grammar-1-ledger.md``). Every
pin collects on the Arrow path (value AND type/nullability) through
``spark.sql``; ``selectExpr`` legs ride the same router. Literal-only cells
stay nullable on RePark where Spark folds to non-null (the folded-literal
precedent); frame cells carry Spark's nullability exactly.

pins: spark-sql-grammar-1/C-008
"""

from __future__ import annotations

import datetime
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

BOX_ZONE: str = "America/New_York"
FRAME_VIEW: str = "grammar1_q14"


@pytest.fixture(params=[False, True], ids=["ansi-off", "ansi-on"])
def spark(request: pytest.FixtureRequest) -> ReparkSession:
    """A UTC facade session under either ANSI setting."""
    session = (
        ReparkSession.builder.appName("pytest-spark-sql-grammar-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true" if request.param else "false")
        .getOrCreate()
    )
    session.sql(
        "SELECT TIMESTAMP'2024-03-10 01:00:00' AS a, TIMESTAMP'2024-03-11 03:30:00' AS b"
    ).createOrReplaceTempView(FRAME_VIEW)
    yield session
    session.stop()


def _table(session: ReparkSession, sql: str) -> pa.Table:
    """Collect one SQL-door statement on the Arrow path."""
    return session.sql(sql).to_arrow()


def _utc(wall: datetime.datetime) -> datetime.datetime:
    """Read a fixture NY-wall naive datetime as the UTC instant Spark stored."""
    return wall.replace(tzinfo=ZoneInfo(BOX_ZONE)).astimezone(datetime.UTC)


def test_pg_tsadd_bare_unit(spark: ReparkSession) -> None:
    """PG-tsadd: ``timestampadd(DAY, 1, ts)`` answers the next midnight.

    Spark folds the literal-only cell to non-null; RePark keeps it nullable
    (the ``test_folded_literals_stay_nullable`` precedent in
    ``test_fnp11a_temporal.py``).
    """
    table = _table(spark, "SELECT timestampadd(DAY, 1, TIMESTAMP'2024-01-01 00:00:00') AS v")
    assert table.column("v").to_pylist() == [
        datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
    ]
    assert table.schema.field("v").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("v").nullable is True


def test_pg_tsdiff_bare_unit(spark: ReparkSession) -> None:
    """PG-tsdiff: ``timestampdiff(HOUR, a, b)`` answers 25."""
    table = _table(
        spark,
        "SELECT timestampdiff(HOUR, TIMESTAMP'2024-01-01 00:00:00', "
        "TIMESTAMP'2024-01-02 01:00:00') AS v",
    )
    assert table.column("v").to_pylist() == [25]
    assert table.schema.field("v").type == pa.int64()
    assert table.schema.field("v").nullable is True


def test_pg_dateadd_bare_unit(spark: ReparkSession) -> None:
    """PG-dateadd: three-argument ``dateadd(DAY, 1, ts)`` aliases the add answer."""
    table = _table(spark, "SELECT dateadd(DAY, 1, TIMESTAMP'2024-01-01 00:00:00') AS v")
    assert table.column("v").to_pylist() == [
        datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
    ]
    assert table.schema.field("v").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("v").nullable is True


def test_pg_datediff_unit(spark: ReparkSession) -> None:
    """PG-datediff-unit: ``datediff(HOUR, a, b)`` answers 25."""
    table = _table(
        spark,
        "SELECT datediff(HOUR, TIMESTAMP'2024-01-01 00:00:00', "
        "TIMESTAMP'2024-01-02 01:00:00') AS v",
    )
    assert table.column("v").to_pylist() == [25]
    assert table.schema.field("v").type == pa.int64()
    assert table.schema.field("v").nullable is True


Q14_ADD: list[tuple[str, datetime.datetime]] = [
    ("timestampadd(DAY, 1, a)", datetime.datetime(2024, 3, 10, 21, 0)),
    ("timestampadd(HOUR, -2, a)", datetime.datetime(2024, 3, 9, 18, 0)),
    ("timestampadd(MONTH, 1, a)", datetime.datetime(2024, 4, 9, 21, 0)),
    ("timestampadd(day, 1, a)", datetime.datetime(2024, 3, 10, 21, 0)),
    ("dateadd(DAY, 1, a)", datetime.datetime(2024, 3, 10, 21, 0)),
    ("timestampadd(MICROSECOND, 5, a)", datetime.datetime(2024, 3, 9, 20, 0, 0, 5)),
    ("timestampadd(QUARTER, 1, a)", datetime.datetime(2024, 6, 9, 21, 0)),
    ("timestampadd(WEEK, 1, a)", datetime.datetime(2024, 3, 16, 21, 0)),
]


@pytest.mark.parametrize(("expr", "wall"), Q14_ADD)
def test_q14_bare_unit_adds(spark: ReparkSession, expr: str, wall: datetime.datetime) -> None:
    """Q14-22…25/30/37…39: bare-unit adds answer the recorded NY-wall instant."""
    table = _table(spark, f"SELECT {expr} AS v FROM {FRAME_VIEW}")
    assert table.column("v").to_pylist() == [_utc(wall)]
    assert table.schema.field("v").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("v").nullable is True


Q14_DIFF: list[tuple[str, int]] = [
    ("timestampdiff(HOUR, a, b)", 26),
    ("timestampdiff(DAY, a, b)", 1),
    ("timestampdiff(MINUTE, a, b)", 1590),
    ("datediff(HOUR, a, b)", 26),
    ("datediff(DAY, a, b)", 1),
    ("timestampdiff(SECOND, a, b)", 95400),
    ("timestampdiff(YEAR, a, b)", 0),
]


@pytest.mark.parametrize(("expr", "value"), Q14_DIFF)
def test_q14_bare_unit_diffs(spark: ReparkSession, expr: str, value: int) -> None:
    """Q14-27…29/31/32/36/40: bare-unit diffs answer ``bigint`` nullable."""
    table = _table(spark, f"SELECT {expr} AS v FROM {FRAME_VIEW}")
    assert table.column("v").to_pylist() == [value]
    assert table.schema.field("v").type == pa.int64()
    assert table.schema.field("v").nullable is True


def test_q14_two_arg_datediff_stays_int(spark: ReparkSession) -> None:
    """Q14-34: two-argument ``datediff(b, a)`` answers ``int`` nullable."""
    table = _table(
        spark,
        "SELECT datediff(b, a) AS v FROM (SELECT a, b FROM " + FRAME_VIEW + " UNION ALL "
        "SELECT CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP)) AS t",
    )
    assert table.column("v").to_pylist() == [1, None]
    assert table.schema.field("v").type == pa.int32()
    assert table.schema.field("v").nullable is True


def test_q14_quoted_unit_refuses(spark: ReparkSession) -> None:
    """Q14-26: a quoted unit refuses ``INVALID_PARAMETER_VALUE.DATETIME_UNIT``."""
    with pytest.raises(AnalysisException, match=r"\[INVALID_PARAMETER_VALUE\.DATETIME_UNIT\]"):
        _table(spark, "SELECT timestampadd('DAY', 1, a) AS v FROM " + FRAME_VIEW)


def test_q14_quoted_unit_names_the_literal(spark: ReparkSession) -> None:
    """Q14-26: the refusal names the offending string literal."""
    with pytest.raises(AnalysisException, match="got the string literal 'DAY'"):
        _table(spark, "SELECT timestampadd('DAY', 1, a) AS v FROM " + FRAME_VIEW)


def test_q14_unknown_unit_refuses_routine(spark: ReparkSession) -> None:
    """Q14-35: ``FORTNIGHT`` refuses ``UNRESOLVED_ROUTINE`` naming the routine."""
    with pytest.raises(AnalysisException, match=r"\[UNRESOLVED_ROUTINE\]"):
        _table(spark, "SELECT timestampadd(FORTNIGHT, 1, a) AS v FROM " + FRAME_VIEW)


def test_q14_date_add_over_timestamp_divergence(spark: ReparkSession) -> None:
    """Q14-33: Spark answers ``date`` 2024-03-11; RePark still refuses (TZ-8)."""
    with pytest.raises(AnalysisException):
        _table(spark, f"SELECT date_add(a, 1) AS v FROM {FRAME_VIEW}")


def test_bare_unit_select_expr_leg(spark: ReparkSession) -> None:
    """The ``selectExpr`` door shares the bare-unit rewrite."""
    frame = spark.table(FRAME_VIEW)
    table = frame.selectExpr("timestampadd(DAY, 1, a) AS v").to_arrow()
    assert table.column("v").to_pylist() == [_utc(datetime.datetime(2024, 3, 10, 21, 0))]
    assert table.schema.field("v").type == pa.timestamp("us", tz="UTC")
