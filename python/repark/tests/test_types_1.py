"""TYPES-1 — Spark-door Arrow types: literals, count-likes, rank, from_unixtime."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import _live_parity as lp
import pyarrow as pa
import pytest
from repark import ReparkSession
from repark.spark import functions as F
from repark.spark import Window

SEED_DDL = "i int, b bigint, s string"
SEED_ROWS: list[tuple[int, int, str | None]] = [(1, 10, "a"), (2, 20, "b"), (3, 30, None)]
SEED_VIEW = "types1_probe"
ZONE_KEY = "spark.sql.session.timeZone"
ANSI_KEY = "spark.sql.ansi.enabled"


def _session(**conf: str) -> ReparkSession:
    """pins: types-1/C-007 — one builder spell for every Spark-door session here."""
    builder = ReparkSession.builder.appName("types-1")
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def _seed(session: ReparkSession) -> Any:
    """pins: types-1/C-001 — int, bigint and string columns behind one temp view."""
    frame = session.createDataFrame(SEED_ROWS, SEED_DDL)
    frame.createOrReplaceTempView(SEED_VIEW)
    return frame


def _door_type(session: ReparkSession, query: str) -> tuple[str, bool]:
    """pins: types-1/C-007 — the SQL door's (Arrow type, nullable) for one column."""
    table = session.sql(query).toArrow()
    field = table.schema.field("r")
    return (str(field.type), field.nullable)


def _frame_type(frame: Any) -> tuple[str, bool]:
    """pins: types-1/C-007 — the DataFrame door's (Arrow type, nullable) for `r`."""
    table = frame.toArrow()
    field = table.schema.field("r")
    return (str(field.type), field.nullable)


@pytest.mark.parametrize(
    ("text", "want"),
    [
        ("1", "int32"),
        ("2147483647", "int32"),
        ("-2147483648", "int32"),
        ("2147483648", "int64"),
        ("-2147483649", "int64"),
    ],
)
def test_literal_select_width_follows_spark(text: str, want: str) -> None:
    """pins: types-1/C-001 — SELECT of a bare integer literal types by INT range."""
    assert _door_type(_session(), f"SELECT {text} AS r") == (want, False)


def test_literal_values_width_follows_spark() -> None:
    """pins: types-1/C-001 — VALUES literals type INT; nullability stays the residue."""
    table = _session().sql("SELECT * FROM (VALUES (1), (2147483648)) AS t(v)").toArrow()
    field = table.schema.field("v")
    assert str(field.type) == "int64"
    narrow = _session().sql("SELECT * FROM (VALUES (1), (2)) AS t(v)").toArrow()
    assert str(narrow.schema.field("v").type) == "int32"
    assert narrow.schema.field("v").nullable is True


@pytest.mark.parametrize("value", [1, -5])
def test_literal_dataframe_door_agrees(value: int) -> None:
    """pins: types-1/C-001 — df.select/withColumn of lit(int) agree with the SQL door."""
    frame = _seed(_session())
    assert _frame_type(frame.select(F.lit(value).alias("r"))) == ("int32", False)
    assert _frame_type(frame.withColumn("r", F.lit(value)).select("r")) == ("int32", False)


def test_literal_bigint_stays_wide_on_both_doors() -> None:
    """pins: types-1/C-001 — out-of-INT-range literals stay Int64 on both doors."""
    session = _session()
    frame = _seed(session)
    assert _door_type(session, "SELECT 9223372036854775807 AS r") == ("int64", False)
    assert _frame_type(frame.select(F.lit(2**40).alias("r"))) == ("int64", False)


def test_literal_ctas_stores_spark_int(tmp_path: Path) -> None:
    """pins: types-1/C-001 — CTAS from a bare literal commits an int column."""
    session = _session()
    try:
        session.register_memory_catalog("types1mem", tmp_path)
        session.sql("CREATE NAMESPACE types1mem.ns")
        session.sql("CREATE TABLE types1mem.ns.t USING iceberg AS SELECT 1 AS x")
        table = session.sql("SELECT x FROM types1mem.ns.t").toArrow()
        assert str(table.schema.field("x").type) == "int32"
    finally:
        session.stop()


@pytest.mark.parametrize(
    ("query", "want", "nullable"),
    [
        ("SELECT i + 1 AS r FROM types1_probe", "int32", True),
        ("SELECT b + 1 AS r FROM types1_probe", "int64", True),
        ("SELECT i + b AS r FROM types1_probe", "int64", True),
        ("SELECT 1 + 1 AS r", "int32", False),
        ("SELECT 1 + CAST(1 AS BIGINT) AS r", "int64", False),
    ],
)
def test_arithmetic_widens_like_spark(query: str, want: str, nullable: bool) -> None:
    """pins: types-1/C-002 — integer arithmetic widens the way Spark widens it."""
    session = _session()
    _seed(session)
    assert _door_type(session, query) == (want, nullable)


def test_arithmetic_dataframe_door_widens_like_spark() -> None:
    """pins: types-1/C-002 — col-plus-literal widens identically on the DataFrame door."""
    frame = _seed(_session())
    assert _frame_type(frame.select((F.col("i") + F.lit(1)).alias("r"))) == ("int32", True)
    assert _frame_type(frame.select((F.col("b") + F.lit(1)).alias("r"))) == ("int64", True)


def test_overflow_errors_when_ansi_on() -> None:
    """pins: types-1/C-002 — INT overflow raises ARITHMETIC_OVERFLOW on both doors."""
    session = _session()
    frame = _seed(session)
    with pytest.raises(Exception, match="ARITHMETIC_OVERFLOW"):
        session.sql("SELECT 2147483647 + 1 AS r").toArrow()
    with pytest.raises(Exception, match="ARITHMETIC_OVERFLOW"):
        frame.select((F.col("i") + F.lit(2147483647)).alias("r")).toArrow()


def test_overflow_wraps_when_ansi_off() -> None:
    """pins: types-1/C-002 — INT overflow wraps under ANSI off on both doors."""
    session = _session(**{ANSI_KEY: "false"})
    frame = _seed(session)
    assert session.sql("SELECT 2147483647 + 1 AS r").toArrow().column("r").to_pylist() == [
        -2147483648
    ]
    got = frame.select((F.col("i") + F.lit(2147483647)).alias("r")).toArrow()
    assert got.column("r").to_pylist() == [-2147483648, -2147483647, -2147483646]


@pytest.mark.parametrize(
    "query",
    ["SELECT count(*) AS r FROM types1_probe", "SELECT count(i) AS r FROM types1_probe"],
)
def test_count_family_is_bigint(query: str) -> None:
    """pins: types-1/C-003 — count(*) and count(x) answer Int64 with int cells."""
    session = _session()
    _seed(session)
    assert _door_type(session, query) == ("int64", False)
    cell = session.sql(query).collect()[0][0]
    assert type(cell) is int


def test_count_distinct_is_bigint() -> None:
    """pins: types-1/C-003 — count(DISTINCT x) answers Int64 with an int cell."""
    session = _session()
    _seed(session)
    query = "SELECT count(DISTINCT s) AS r FROM types1_probe"
    assert _door_type(session, query) == ("int64", False)
    assert session.sql(query).collect()[0][0] == 2


def test_approx_and_regr_count_are_bigint() -> None:
    """pins: types-1/C-003 — approx_count_distinct and regr_count answer Int64."""
    session = _session()
    frame = _seed(session)
    for query in [
        "SELECT approx_count_distinct(s) AS r FROM types1_probe",
        "SELECT regr_count(b, i) AS r FROM types1_probe",
    ]:
        assert _door_type(session, query) == ("int64", True)
    facade = frame.select(F.approx_count_distinct("s").alias("r"))
    assert _frame_type(facade) == ("int64", True)


def test_count_if_is_bigint_on_both_doors() -> None:
    """pins: types-1/C-003 — count_if answers Int64 on the SQL door and the facade."""
    session = _session()
    frame = _seed(session)
    query = "SELECT count_if(i > 1) AS r FROM types1_probe"
    assert _door_type(session, query) == ("int64", False)
    assert session.sql(query).collect()[0][0] == 2
    facade = frame.select(F.count_if(F.col("i") > 1).alias("r"))
    assert _frame_type(facade) == ("int64", False)


@pytest.mark.parametrize(
    ("query", "want", "cell"),
    [
        ("SELECT sum(i) AS r FROM types1_probe", "int64", 6),
        ("SELECT sum(b) AS r FROM types1_probe", "int64", 60),
    ],
)
def test_sum_of_integers_is_bigint(query: str, want: str, cell: int) -> None:
    """pins: types-1/C-004 — sum over INT/BIGINT answers BIGINT with int cells."""
    session = _session()
    _seed(session)
    assert _door_type(session, query) == (want, True)
    assert session.sql(query).collect()[0][0] == cell


def test_sum_of_decimal_widens_like_spark() -> None:
    """pins: types-1/C-004 — sum over DECIMAL(10,2) answers DECIMAL(20,2)."""
    session = _session()
    _seed(session)
    query = "SELECT sum(CAST(b AS DECIMAL(10, 2))) AS r FROM types1_probe"
    assert _door_type(session, query) == ("decimal128(20, 2)", True)
    assert session.sql(query).collect()[0][0] == Decimal("60.00")


@pytest.mark.parametrize(
    ("name", "want"),
    [
        ("bit_length", 8),
        ("octet_length", 1),
        ("length", 1),
        ("char_length", 1),
        ("character_length", 1),
    ],
)
def test_length_family_is_int(name: str, want: int) -> None:
    """pins: types-1/C-004 — length-family functions answer INT with int cells."""
    session = _session()
    _seed(session)
    query = f"SELECT {name}(s) AS r FROM types1_probe WHERE s IS NOT NULL"
    assert _door_type(session, query) == ("int32", True)
    assert session.sql(query).collect()[0][0] == want


def test_grouping_is_int() -> None:
    """pins: types-1/C-004 — grouping answers INT."""
    session = _session()
    _seed(session)
    assert _door_type(session, "SELECT grouping(i) AS r FROM types1_probe GROUP BY i") == (
        "int32",
        False,
    )


@pytest.mark.parametrize(
    "call",
    ["rank()", "dense_rank()", "row_number()", "ntile(2)"],
)
def test_rank_family_is_int_on_the_sql_door(call: str) -> None:
    """pins: types-1/C-005 — rank-family window functions answer INT on the SQL door."""
    session = _session()
    _seed(session)
    query = f"SELECT {call} OVER (ORDER BY i) AS r FROM types1_probe"
    assert _door_type(session, query) == ("int32", False)


@pytest.mark.parametrize(
    "call",
    ["rank()", "dense_rank()", "row_number()", "ntile(2)"],
)
def test_rank_family_is_int_partitioned(call: str) -> None:
    """pins: types-1/C-005 — rank-family window functions answer INT with partitions."""
    session = _session()
    _seed(session)
    query = f"SELECT {call} OVER (PARTITION BY s ORDER BY i) AS r FROM types1_probe"
    assert _door_type(session, query) == ("int32", False)


def test_rank_family_is_int_on_the_dataframe_door() -> None:
    """pins: types-1/C-005 — rank-family window functions answer INT on the facade."""
    frame = _seed(_session())
    window = Window.orderBy("i")
    assert _frame_type(frame.select(F.rank().over(window).alias("r"))) == ("int32", False)
    assert _frame_type(frame.select(F.dense_rank().over(window).alias("r"))) == (
        "int32",
        False,
    )
    assert _frame_type(frame.select(F.row_number().over(window).alias("r"))) == (
        "int32",
        False,
    )
    assert _frame_type(frame.select(F.ntile(2).over(window).alias("r"))) == ("int32", False)


def test_rank_family_collects_int_cells() -> None:
    """pins: types-1/C-005 — rank cells collect as Python ints, not wider values."""
    session = _session()
    _seed(session)
    cells = session.sql("SELECT rank() OVER (ORDER BY i) AS r FROM types1_probe").collect()
    assert [type(row[0]) for row in cells] == [int, int, int]
    assert [row[0] for row in cells] == [1, 2, 3]


def test_rank_in_ctas_stores_int(tmp_path: Path) -> None:
    """pins: types-1/C-005 — CTAS over rank() commits an int column."""
    session = _session()
    try:
        _seed(session)
        session.register_memory_catalog("types1rank", tmp_path)
        session.sql("CREATE NAMESPACE types1rank.ns")
        session.sql(
            "CREATE TABLE types1rank.ns.t USING iceberg AS "
            "SELECT rank() OVER (ORDER BY i) AS r FROM types1_probe"
        )
        table = session.sql("SELECT r FROM types1rank.ns.t ORDER BY r").toArrow()
        assert str(table.schema.field("r").type) == "int32"
        assert table.column("r").to_pylist() == [1, 2, 3]
    finally:
        session.stop()


@pytest.mark.parametrize("call", ["percent_rank()", "cume_dist()"])
def test_percent_rank_family_is_float64(call: str) -> None:
    """pins: types-1/C-005 — percent_rank and cume_dist answer DOUBLE on both doors."""
    session = _session()
    frame = _seed(session)
    query = f"SELECT {call} OVER (ORDER BY i) AS r FROM types1_probe"
    assert _door_type(session, query) == ("double", False)
    name = "percent_rank" if call.startswith("percent") else "cume_dist"
    facade = frame.select(getattr(F, name)().over(Window.orderBy("i")).alias("r"))
    assert _frame_type(facade) == ("double", False)


def test_from_unixtime_is_a_session_zone_string() -> None:
    """pins: types-1/C-006 — from_unixtime answers a UTC STRING on both doors."""
    session = _session()
    frame = _seed(session)
    assert _door_type(session, "SELECT from_unixtime(0) AS r") == ("string", True)
    assert session.sql("SELECT from_unixtime(0) AS r").collect()[0][0] == "1970-01-01 00:00:00"
    facade = frame.select(F.from_unixtime(F.lit(0)).alias("r"))
    assert _frame_type(facade) == ("string", True)
    assert type(facade.collect()[0][0]) is str


@pytest.mark.parametrize(
    ("pattern", "want"),
    [
        ("yyyy/MM/dd", "1970/01/01"),
        ("HH:mm", "00:00"),
        ("yyyy-MM-dd HH:mm:ss", "1970-01-01 00:00:00"),
    ],
)
def test_from_unixtime_format_argument(pattern: str, want: str) -> None:
    """pins: types-1/C-006 — from_unixtime renders the optional Java format pattern."""
    session = _session()
    query = f"SELECT from_unixtime(0, '{pattern}') AS r"
    assert _door_type(session, query) == ("string", True)
    assert session.sql(query).collect()[0][0] == want


def test_from_unixtime_follows_the_session_zone() -> None:
    """pins: types-1/C-006 — from_unixtime renders in America/New_York on both doors."""
    session = _session(**{ZONE_KEY: "America/New_York"})
    frame = _seed(session)
    assert session.sql("SELECT from_unixtime(0) AS r").collect()[0][0] == "1969-12-31 19:00:00"
    facade = frame.select(F.from_unixtime(F.lit(0)).alias("r"))
    assert facade.collect()[0][0] == "1969-12-31 19:00:00"


def test_unix_timestamp_and_to_timestamp_stand_still() -> None:
    """pins: types-1/C-006 — unix_timestamp and to_timestamp keep their DATE-FN-1 answers."""
    session = _session()
    assert session.sql("SELECT unix_timestamp() AS r").toArrow().schema.field("r").type == pa.int64()
    stamp = session.sql("SELECT to_timestamp('2024-02-29 01:02:03') AS r").collect()[0][0]
    assert (stamp.year, stamp.month, stamp.day) == (2024, 2, 29)


def _logical_plan(session: ReparkSession, query: str) -> str:
    """pins: types-1/C-007 — the logical-plan text of one EXPLAIN query."""
    rows = session.sql(f"EXPLAIN {query}").collect()
    logical = [row["plan"] for row in rows if row["plan_type"] == "logical_plan"]
    assert logical, f"EXPLAIN produced no logical plan: {rows}"
    return "\n".join(logical)


def test_explain_carries_the_spark_casts() -> None:
    """pins: types-1/C-007 — the analyzed plan carries CASTs, not facade conversions."""
    session = _session()
    _seed(session)
    rank_text = _logical_plan(session, "SELECT rank() OVER (ORDER BY i) AS r FROM types1_probe")
    assert "CAST" in rank_text and "Int32" in rank_text
    count_text = _logical_plan(session, "SELECT regr_count(b, i) AS r FROM types1_probe")
    assert "CAST" in count_text and "Int64" in count_text
    literal_text = _logical_plan(session, "SELECT 1 AS r")
    assert "Int32(1)" in literal_text


def test_ansi_door_keeps_stock_types() -> None:
    """pins: types-1/C-007 — the native ANSI door still types bare literals Int64."""
    import repark

    table = repark.sql("SELECT 1 AS r").to_arrow()
    assert str(table.schema.field("r").type) == "int64"


def _live_engine(session: ReparkSession) -> lp.Engine:
    """pins: types-1/C-008 — the repark side as a live-parity engine handle."""
    return lp.Engine(
        name="repark",
        session=session,
        functions=F,
        types=None,
        window=Window,
        arrow_of=lambda frame: frame.toArrow(),
    )


def _live_type(engine: lp.Engine, query: str) -> tuple[str, list[Any]]:
    """pins: types-1/C-008 — one query's (Arrow type, values) on either live engine."""
    table = engine.arrow_of(engine.session.sql(query))
    return (str(table.schema.field("r").type), table.column("r").to_pylist())


def _seed_oracle(spark_engine: lp.Engine) -> None:
    """pins: types-1/C-008 — the same int/bigint/string seed behind the same view."""
    frame = spark_engine.session.createDataFrame(SEED_ROWS, SEED_DDL)
    frame.createOrReplaceTempView(SEED_VIEW)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_literal_and_arithmetic_match_the_oracle(spark_engine: lp.Engine) -> None:
    """pins: types-1/C-001 — live Spark agrees on literal widths and INT arithmetic."""
    session = _session()
    _seed(session)
    _seed_oracle(spark_engine)
    engine = _live_engine(session)
    for query in [
        "SELECT 1 AS r",
        "SELECT 2147483648 AS r",
        "SELECT i + 1 AS r FROM types1_probe",
        "SELECT b + 1 AS r FROM types1_probe",
        "SELECT 1 + 1 AS r",
    ]:
        assert _live_type(engine, query) == _live_type(spark_engine, query)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_aggregates_and_rank_match_the_oracle(spark_engine: lp.Engine) -> None:
    """pins: types-1/C-003 — live Spark agrees on count-like and rank-family types."""
    session = _session()
    _seed(session)
    _seed_oracle(spark_engine)
    engine = _live_engine(session)
    for query in [
        "SELECT count(*) AS r FROM types1_probe",
        "SELECT count(s) AS r FROM types1_probe",
        "SELECT count(DISTINCT s) AS r FROM types1_probe",
        "SELECT count_if(i > 1) AS r FROM types1_probe",
        "SELECT sum(i) AS r FROM types1_probe",
        "SELECT rank() OVER (ORDER BY i) AS r FROM types1_probe",
        "SELECT row_number() OVER (ORDER BY i) AS r FROM types1_probe",
        "SELECT ntile(2) OVER (ORDER BY i) AS r FROM types1_probe",
        "SELECT from_unixtime(0) AS r",
    ]:
        assert _live_type(engine, query) == _live_type(spark_engine, query)
