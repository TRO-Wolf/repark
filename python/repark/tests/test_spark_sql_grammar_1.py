"""SPARK-SQL-GRAMMAR-1 door pins — Spark operators, keywords and type names.

Oracle: PySpark 4.1.2 ``fixtures-batch3.json`` PG-* cells, ``fixtures-batch14``
Q14-* cells and the ``fnp11_spark_oracle.json`` SQL-door bare-unit cells (see
the unit ledger ``task/ledgers/staging/spark-sql-grammar-1-ledger.md``). Every
pin collects on the Arrow path (value AND type/nullability) through
``spark.sql``; ``selectExpr`` legs ride the same router. Literal-only cells
stay nullable on RePark where Spark folds to non-null (the folded-literal
precedent); frame cells carry Spark's nullability exactly.

pins: spark-sql-grammar-1/C-003, C-004, C-005, C-006, C-008, C-010
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
    values = table.column("v").to_pylist()
    assert sorted(value for value in values if value is not None) == [1]
    assert any(value is None for value in values)
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


Q14_REFUSING_BARE: list[str] = [
    "localtimestamp",
    "current_catalog",
    "current_database",
    "current_schema",
    "current_timezone",
    "now",
]


@pytest.mark.parametrize("name", Q14_REFUSING_BARE)
def test_q14_bare_refusing_names(spark: ReparkSession, name: str) -> None:
    """Q14-0/8/10/12/18/20: bare names Spark refuses raise the column class."""
    with pytest.raises(AnalysisException, match=r"\[UNRESOLVED_COLUMN\.WITH_SUGGESTION\]"):
        _table(spark, f"SELECT {name} AS v FROM {FRAME_VIEW}")


@pytest.mark.parametrize("name", Q14_REFUSING_BARE)
def test_q14_bare_refusing_names_frameless(spark: ReparkSession, name: str) -> None:
    """The same refusals carry WITHOUT_SUGGESTION with no frame to suggest from."""
    with pytest.raises(
        AnalysisException, match=r"\[UNRESOLVED_COLUMN\.WITHOUT_SUGGESTION\]"
    ):
        _table(spark, f"SELECT {name} AS v")


def test_q14_real_column_beats_nullary_name(spark: ReparkSession) -> None:
    """A real column still wins over the refusing nullary name."""
    table = _table(spark, "SELECT localtimestamp AS v FROM (SELECT 42 AS localtimestamp) AS t")
    assert table.column("v").to_pylist() == [42]
    assert table.schema.field("v").type == pa.int32()


def test_pg_rlike(spark: ReparkSession) -> None:
    """PG-rlike: ``'abc' RLIKE '^a'`` lowers onto ``regexp_like``."""
    table = _table(spark, "SELECT 'abc' RLIKE '^a' AS v")
    assert table.column("v").to_pylist() == [True]
    assert table.schema.field("v").type == pa.bool_()
    assert table.schema.field("v").nullable is False


def test_pg_not_rlike(spark: ReparkSession) -> None:
    """PG-rlike negated: ``NOT RLIKE`` lowers onto ``NOT regexp_like``."""
    table = _table(spark, "SELECT 'abc' NOT RLIKE '^b' AS v")
    assert table.column("v").to_pylist() == [True]
    assert table.schema.field("v").type == pa.bool_()
    assert table.schema.field("v").nullable is False


def test_pg_ltz_cast(spark: ReparkSession) -> None:
    """PG-ltz-cast: ``CAST(x AS TIMESTAMP_LTZ)`` answers ``TIMESTAMP``.

    The literal-only cell folds to non-null, exactly like a plain
    ``CAST('…' AS TIMESTAMP)`` on this door; Spark types it nullable.
    """
    table = _table(spark, "SELECT CAST('2024-01-02 03:04:05' AS TIMESTAMP_LTZ) AS v")
    assert table.column("v").to_pylist() == [
        datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)
    ]
    assert table.schema.field("v").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("v").nullable is False


def test_pg_ltz_cast_nullable_frame(spark: ReparkSession) -> None:
    """``CAST(x AS TIMESTAMP_LTZ)`` over nullable input stays nullable."""
    table = _table(
        spark,
        "SELECT CAST(s AS TIMESTAMP_LTZ) AS v FROM (SELECT '2024-01-02 03:04:05' AS s "
        "UNION ALL SELECT CAST(NULL AS STRING)) AS t",
    )
    values = table.column("v").to_pylist()
    assert [value for value in values if value is not None] == [
        datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)
    ]
    assert any(value is None for value in values)
    assert table.schema.field("v").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("v").nullable is True


def test_pg_ntz_literal_refuses(spark: ReparkSession) -> None:
    """PG-ntz-lit: the ``TIMESTAMP_NTZ`` literal refuses loud naming TZ-6."""
    with pytest.raises(AnalysisException, match=r"\[UNSUPPORTED_TIMESTAMP_NTZ\]"):
        _table(spark, "SELECT TIMESTAMP_NTZ '2024-01-02 03:04:05' AS v")


def test_pg_ntz_cast_refuses(spark: ReparkSession) -> None:
    """PG-ntz-cast: ``CAST(x AS TIMESTAMP_NTZ)`` refuses loud naming TZ-6."""
    with pytest.raises(AnalysisException, match=r"\[UNSUPPORTED_TIMESTAMP_NTZ\]"):
        _table(spark, "SELECT CAST('2024-01-02 03:04:05' AS TIMESTAMP_NTZ) AS v")


def test_pg_struct_dot(spark: ReparkSession) -> None:
    """PG-struct-dot: ``named_struct('a', 1).a`` answers ``1`` int non-null."""
    table = _table(spark, "SELECT named_struct('a', 1).a AS v")
    assert table.column("v").to_pylist() == [1]
    assert table.schema.field("v").type == pa.int32()
    assert table.schema.field("v").nullable is False


def test_q14_current_date_bare_and_paren(spark: ReparkSession) -> None:
    """Q14-2/3: ``current_date`` resolves bare and parenthesised.

    Spark types both non-null; the DataFusion builtin stays nullable on RePark
    and the pin records the engine shape.
    """
    for expr in ("current_date", "current_date()"):
        table = _table(spark, f"SELECT {expr} AS v")
        assert table.column("v").to_pylist() == [datetime.date.today()]
        assert table.schema.field("v").type == pa.date32()
        assert table.schema.field("v").nullable is True


def test_q14_current_timestamp_bare_and_paren(spark: ReparkSession) -> None:
    """Q14-4/5: ``current_timestamp`` resolves bare and parenthesised."""
    for expr in ("current_timestamp", "current_timestamp()"):
        table = _table(spark, f"SELECT {expr} AS v")
        assert table.column("v").to_pylist()[0] is not None
        assert table.schema.field("v").type == pa.timestamp("us", tz="UTC")
        assert table.schema.field("v").nullable is False


def test_q14_localtimestamp_paren(spark: ReparkSession) -> None:
    """Q14-1: ``localtimestamp()`` answers the naive session-zone wall clock."""
    table = _table(spark, "SELECT localtimestamp() AS v")
    assert table.column("v").to_pylist()[0] is not None
    assert table.schema.field("v").type == pa.timestamp("us")
    assert table.schema.field("v").nullable is False


def test_q14_now_paren(spark: ReparkSession) -> None:
    """Q14-21: ``now()`` answers ``timestamp`` non-null."""
    table = _table(spark, "SELECT now() AS v")
    assert table.column("v").to_pylist()[0] is not None
    assert table.schema.field("v").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("v").nullable is False


def test_q14_current_timezone_paren(spark: ReparkSession) -> None:
    """Q14-19: ``current_timezone()`` answers the session zone."""
    table = _table(spark, "SELECT current_timezone() AS v")
    assert table.column("v").to_pylist() == ["UTC"]
    assert table.schema.field("v").type == pa.string()
    assert table.schema.field("v").nullable is False


Q14_SESSION_NAMES: list[str] = [
    "current_user",
    "user",
    "session_user",
    "current_catalog",
    "current_database",
    "current_schema",
]


@pytest.mark.parametrize("name", Q14_SESSION_NAMES)
def test_q14_session_names_sql_divergence(spark: ReparkSession, name: str) -> None:
    """Q14-6/7/9/11/13/14…17: Spark answers ``string``; the SQL door still refuses.

    The Python door answers these as foldable session strings (another lane's
    design); wiring the SQL door to the same session plumbing is a P2 hand-off
    to run 17a recorded in the ledger. The pin holds the refusal loud.
    """
    with pytest.raises(AnalysisException):
        _table(spark, f"SELECT {name}() AS v")
