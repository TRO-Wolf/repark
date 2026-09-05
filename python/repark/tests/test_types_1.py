"""TYPES-1 — Spark-door Arrow types: literals, count-likes, rank, from_unixtime."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import _live_parity as lp
import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import Window
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

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
    """pins: types-1/C-002 — INT overflow wraps under ANSI off, shape and all."""
    session = _session(**{ANSI_KEY: "false"})
    frame = _seed(session)
    assert _door_type(session, "SELECT 2147483647 + 1 AS r") == ("int32", False)
    assert session.sql("SELECT 2147483647 + 1 AS r").toArrow().column("r").to_pylist() == [
        -2147483648
    ]
    got = frame.select((F.col("i") + F.lit(2147483647)).alias("r")).toArrow()
    assert got.column("r").to_pylist() == [-2147483648, -2147483647, -2147483646]


def test_narrowed_literal_in_case_and_if() -> None:
    """pins: types-1/C-001 — narrowed literals in CASE and IF answer INT, both doors."""
    session = _session()
    frame = _seed(session)
    case = "SELECT CASE WHEN i > 1 THEN 1 ELSE 0 END AS r FROM types1_probe"
    assert _door_type(session, case) == ("int32", False)
    assert session.sql(case).toArrow().column("r").to_pylist() == [0, 1, 1]
    facade_case = frame.select(F.when(F.col("i") > 1, F.lit(1)).otherwise(F.lit(0)).alias("r"))
    assert _frame_type(facade_case) == ("int32", False)
    assert facade_case.toArrow().column("r").to_pylist() == [0, 1, 1]
    conditional = "SELECT IF(i > 1, 1, 0) AS r FROM types1_probe"
    assert _door_type(session, conditional) == ("int32", True)
    assert session.sql(conditional).toArrow().column("r").to_pylist() == [0, 1, 1]


def test_coalesce_with_bigint_stays_wide_on_both_doors() -> None:
    """pins: types-1/C-001 — COALESCE of BIGINT and a narrowed literal answers BIGINT."""
    session = _session()
    frame = _seed(session)
    query = "SELECT COALESCE(CAST(NULL AS BIGINT), 1) AS r"
    assert _door_type(session, query) == ("int64", False)
    assert session.sql(query).collect()[0][0] == 1
    facade = frame.select(F.coalesce(F.lit(None).cast("bigint"), F.lit(1)).alias("r"))
    assert _frame_type(facade) == ("int64", False)
    assert facade.collect()[0][0] == 1


def test_coalesce_with_int_stays_wide_on_repark() -> None:
    """pins: types-1/C-001 — COALESCE of INT and a narrowed literal answers BIGINT (TY-7)."""
    session = _session()
    query = "SELECT COALESCE(CAST(NULL AS INT), 1) AS r"
    assert _door_type(session, query) == ("int64", False)
    assert session.sql(query).collect()[0][0] == 1


def test_narrowed_literal_in_array_struct_map() -> None:
    """pins: types-1/C-001 — array/struct/map of narrowed literals carry INT, both doors."""
    session = _session()
    frame = _seed(session)
    array = "SELECT array(1, 2) AS r"
    assert _door_type(session, array) == ("list<element: int32>", False)
    assert session.sql(array).toArrow().column("r").to_pylist() == [[1, 2]]
    facade_array = frame.select(F.array(F.lit(1), F.lit(2)).alias("r"))
    assert _frame_type(facade_array) == ("list<item: int32>", True)
    assert facade_array.toArrow().column("r").to_pylist() == [[1, 2], [1, 2], [1, 2]]
    struct = "SELECT struct(1, 'a') AS r"
    assert _door_type(session, struct) == ("struct<c0: int32, c1: string>", True)
    assert session.sql(struct).toArrow().column("r").to_pylist() == [{"c0": 1, "c1": "a"}]
    facade_struct = frame.select(F.struct(F.lit(1), F.lit("a")).alias("r"))
    assert _frame_type(facade_struct) == ("struct<1: int32, a: string>", True)
    mapping = "SELECT map(1, 'a') AS r"
    assert _door_type(session, mapping) == ("map<int32, string>", True)
    assert session.sql(mapping).toArrow().column("r").to_pylist() == [[(1, "a")]]


def test_narrowed_literal_in_union_with_bigint() -> None:
    """pins: types-1/C-001 — a narrowed literal unifies with BIGINT, both doors."""
    session = _session()
    union = "SELECT 1 AS r UNION ALL SELECT CAST(2 AS BIGINT) AS r ORDER BY r"
    assert _door_type(session, union) == ("int64", False)
    assert session.sql(union).toArrow().column("r").to_pylist() == [1, 2]
    facade_union = (
        session.sql("SELECT 1 AS r")
        .union(session.sql("SELECT CAST(2 AS BIGINT) AS r"))
        .orderBy("r")
    )
    assert _frame_type(facade_union) == ("int64", False)
    assert facade_union.toArrow().column("r").to_pylist() == [1, 2]


def test_narrowed_literal_against_decimal_matches_on_the_sql_door() -> None:
    """pins: types-1/C-001 — DECIMAL(10,2) beside a narrowed literal answers (11,2)."""
    session = _session()
    _seed(session)
    query = "SELECT CAST(b AS DECIMAL(10, 2)) + 1 AS r FROM types1_probe"
    assert _door_type(session, query) == ("decimal128(11, 2)", True)
    assert session.sql(query).collect()[0][0] == Decimal("11.00")
    flipped = "SELECT 1 + CAST(b AS DECIMAL(10, 2)) AS r FROM types1_probe"
    assert _door_type(session, flipped) == ("decimal128(11, 2)", True)


def test_facade_decimal_plus_literal_skips_min_precision() -> None:
    """pins: types-1/C-001 — facade decimal + lit(1) answers (13,2) (TY-10: Spark (11,2))."""
    frame = _seed(_session())
    facade = frame.select((F.col("b").cast("decimal(10,2)") + F.lit(1)).alias("r"))
    assert _frame_type(facade) == ("decimal128(13, 2)", True)
    assert facade.collect()[0][0] == Decimal("11.00")


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


@pytest.mark.parametrize("width", ["TINYINT", "SMALLINT"])
def test_sum_of_narrow_integers_is_bigint(width: str) -> None:
    """pins: types-1/C-004 — sum over TINYINT and SMALLINT answers BIGINT, both doors."""
    session = _session()
    frame = _seed(session)
    query = f"SELECT sum(CAST(i AS {width})) AS r FROM types1_probe"
    assert _door_type(session, query) == ("int64", True)
    assert session.sql(query).collect()[0][0] == 6
    facade = frame.select(F.sum(F.col("i").cast(width.lower())).alias("r"))
    assert _frame_type(facade) == ("int64", True)
    assert facade.collect()[0][0] == 6


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


def test_grouping_in_rollup_and_sets_answers_int() -> None:
    """pins: types-1/C-004 — grouping under ROLLUP and GROUPING SETS answers INT."""
    session = _session()
    frame = _seed(session)
    for query in [
        "SELECT i, grouping(i) AS r FROM types1_probe GROUP BY ROLLUP(i) ORDER BY i NULLS LAST",
        "SELECT i, grouping(i) AS r FROM types1_probe "
        "GROUP BY GROUPING SETS ((i), ()) ORDER BY i NULLS LAST",
    ]:
        assert _door_type(session, query) == ("int32", False)
        assert session.sql(query).toArrow().column("r").to_pylist() == [0, 0, 0, 1]
    rolled = frame.rollup("i").agg(F.grouping("i").alias("r"))
    assert _frame_type(rolled) == ("int32", False)
    assert sorted(rolled.toArrow().column("r").to_pylist()) == [0, 0, 0, 1]


def test_grouping_under_plain_group_by_is_accepted() -> None:
    """pins: types-1/C-004 — plain GROUP BY accepts grouping (TY-8: Spark raises)."""
    session = _session()
    _seed(session)
    query = "SELECT grouping(i) AS r FROM types1_probe GROUP BY i"
    assert _door_type(session, query) == ("int32", False)
    assert session.sql(query).toArrow().column("r").to_pylist() == [0, 0, 0]


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


def test_ntile_with_bigint_argument_is_accepted() -> None:
    """pins: types-1/C-005 — ntile over a BIGINT bucket count answers INT (TY-9: refuses)."""
    session = _session()
    _seed(session)
    query = "SELECT ntile(CAST(2 AS BIGINT)) OVER (ORDER BY i) AS r FROM types1_probe"
    assert _door_type(session, query) == ("int32", False)
    assert session.sql(query).toArrow().column("r").to_pylist() == [1, 1, 2]


def test_from_unixtime_is_a_session_zone_string() -> None:
    """pins: types-1/C-006 — from_unixtime answers a UTC STRING, nullable like Spark."""
    session = _session()
    frame = _seed(session)
    assert _door_type(session, "SELECT from_unixtime(0) AS r") == ("string", True)
    assert session.sql("SELECT from_unixtime(0) AS r").collect()[0][0] == "1970-01-01 00:00:00"
    facade = frame.select(F.from_unixtime(F.lit(0)).alias("r"))
    assert _frame_type(facade) == ("string", True)
    assert type(facade.collect()[0][0]) is str


def test_from_unixtime_nullable_column_stays_nullable() -> None:
    """pins: types-1/C-006 — from_unixtime over a nullable column stays nullable."""
    session = _session()
    _seed(session)
    query = "SELECT from_unixtime(i) AS r FROM types1_probe"
    assert _door_type(session, query) == ("string", True)


@pytest.mark.parametrize(
    ("pattern", "want"),
    [
        ("yyyy/MM/dd", "1970/01/01"),
        ("HH:mm", "00:00"),
        ("yyyy-MM-dd HH:mm:ss", "1970-01-01 00:00:00"),
        ("EEE", "Thu"),
        ("dd/MM/yyyy HH:mm", "01/01/1970 00:00"),
    ],
)
def test_from_unixtime_format_argument(pattern: str, want: str) -> None:
    """pins: types-1/C-006 — from_unixtime renders the optional Java format pattern."""
    session = _session()
    query = f"SELECT from_unixtime(0, '{pattern}') AS r"
    assert _door_type(session, query) == ("string", True)
    assert session.sql(query).collect()[0][0] == want


def test_from_unixtime_weekday_and_european_formats_on_the_facade() -> None:
    """pins: types-1/C-006 — the facade renders EEE and dd/MM/yyyy HH:mm like Spark."""
    frame = _seed(_session())
    weekday = frame.select(F.from_unixtime(F.lit(0), "EEE").alias("r"))
    assert _frame_type(weekday) == ("string", True)
    assert weekday.collect()[0][0] == "Thu"
    european = frame.select(F.from_unixtime(F.lit(0), "dd/MM/yyyy HH:mm").alias("r"))
    assert _frame_type(european) == ("string", True)
    assert european.collect()[0][0] == "01/01/1970 00:00"


def test_from_unixtime_follows_the_session_zone() -> None:
    """pins: types-1/C-006 — from_unixtime renders in America/New_York on both doors."""
    session = _session(**{ZONE_KEY: "America/New_York"})
    frame = _seed(session)
    assert session.sql("SELECT from_unixtime(0) AS r").collect()[0][0] == "1969-12-31 19:00:00"
    facade = frame.select(F.from_unixtime(F.lit(0)).alias("r"))
    assert facade.collect()[0][0] == "1969-12-31 19:00:00"
    legacy = "SELECT from_unixtime(-77900000000) AS r"
    assert session.sql(legacy).collect()[0][0] == "-0499-06-13 10:10:38"
    legacy_facade = frame.select(F.from_unixtime(F.lit(-77900000000)).alias("r"))
    assert legacy_facade.collect()[0][0] == "-0499-06-13 10:10:38"


@pytest.mark.parametrize(
    ("text", "value", "want"),
    [
        ("15000000000000", 15000000000000, "-107253-01-11 18:38:10"),
        ("20000000000000", 20000000000000, "+51190-09-21 03:31:30"),
        ("-15000000000000", -15000000000000, "+111192-12-21 05:21:49"),
        ("9223372036854775807", 2**63 - 1, "1969-12-31 23:59:59"),
        ("-9223372036854775808", -(2**63), "1970-01-01 00:00:00"),
        ("-1", -1, "1969-12-31 23:59:59"),
        ("1.5", 1.5, "1970-01-01 00:00:01"),
    ],
)
def test_from_unixtime_wraps_extreme_seconds_like_spark(text: str, value: Any, want: str) -> None:
    """pins: types-1/C-006 — out-of-range seconds wrap to Spark's years, both doors."""
    session = _session()
    frame = _seed(session)
    query = f"SELECT from_unixtime({text}) AS r"
    assert _door_type(session, query) == ("string", True)
    assert session.sql(query).collect()[0][0] == want
    facade = frame.select(F.from_unixtime(F.lit(value)).alias("r"))
    assert _frame_type(facade) == ("string", True)
    assert facade.collect()[0][0] == want


@pytest.mark.parametrize(
    ("seconds", "pattern", "want"),
    [
        (-77900000000, None, "-0499-06-13 15:06:40"),
        (-62200000000, None, "-0002-12-17 14:13:20"),
        (-77900000000, "yyyy", "-0499"),
        (-62200000000, "yyyy", "-0002"),
        (-77900000000, "yy", "99"),
        (-62200000000, "yy", "02"),
    ],
)
def test_from_unixtime_pads_negative_years_after_the_sign(
    seconds: int, pattern: str | None, want: str
) -> None:
    """pins: types-1/C-006 — negative 3- and 4-digit years pad after the sign, both doors."""
    session = _session()
    frame = _seed(session)
    if pattern is None:
        query = f"SELECT from_unixtime({seconds}) AS r"
        facade = frame.select(F.from_unixtime(F.lit(seconds)).alias("r"))
    else:
        query = f"SELECT from_unixtime({seconds}, '{pattern}') AS r"
        facade = frame.select(F.from_unixtime(F.lit(seconds), pattern).alias("r"))
    assert _door_type(session, query) == ("string", True)
    assert session.sql(query).collect()[0][0] == want
    assert _frame_type(facade) == ("string", True)
    assert facade.collect()[0][0] == want


def test_from_unixtime_null_stays_null_on_both_doors() -> None:
    """pins: types-1/C-006 — NULL seconds answer NULL, SQL door and facade."""
    session = _session()
    frame = _seed(session)
    query = "SELECT from_unixtime(CAST(NULL AS BIGINT)) AS r"
    assert _door_type(session, query) == ("string", True)
    assert session.sql(query).collect()[0][0] is None
    facade = frame.select(F.from_unixtime(F.lit(None)).alias("r"))
    assert _frame_type(facade) == ("string", True)
    assert facade.collect()[0][0] is None


def test_unix_timestamp_and_to_timestamp_stand_still() -> None:
    """pins: types-1/C-006 — unix_timestamp and to_timestamp keep their DATE-FN-1 answers."""
    session = _session()
    unix_type = session.sql("SELECT unix_timestamp() AS r").toArrow().schema.field("r").type
    assert unix_type == pa.int64()
    stamp = session.sql("SELECT to_timestamp('2024-02-29 01:02:03') AS r").collect()[0][0]
    assert (stamp.year, stamp.month, stamp.day) == (2024, 2, 29)


def _logical_plan(session: ReparkSession, query: str) -> str:
    """pins: types-1/C-007 — the logical-plan text of one EXPLAIN query."""
    rows = session.sql(f"EXPLAIN {query}").collect()
    logical = [row["plan"] for row in rows if row["plan_type"] == "logical_plan"]
    assert logical, f"EXPLAIN produced no logical plan: {rows}"
    return "\n".join(logical)


def test_explain_carries_the_spark_rewrites() -> None:
    """pins: types-1/C-007 — the analyzed plan carries the rewrites, not conversions."""
    session = _session()
    _seed(session)
    literal_text = _logical_plan(session, "SELECT 1 AS r")
    assert "Int32(1)" in literal_text
    overflow_text = _logical_plan(session, "SELECT 2147483647 + 1 AS r")
    assert "__repark_spark_int_add__" in overflow_text
    assert "Int32(2147483647)" in overflow_text


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


def _live_type(engine: lp.Engine, query: str) -> tuple[str, bool, list[Any]]:
    """pins: types-1/C-008 — one query's (Arrow type, nullable, values) either engine."""
    table = engine.arrow_of(engine.session.sql(query))
    field = table.schema.field("r")
    return (str(field.type), field.nullable, table.column("r").to_pylist())


def _seed_oracle(spark_engine: lp.Engine) -> None:
    """pins: types-1/C-008 — the same int/bigint/string seed behind the same view."""
    frame = spark_engine.session.createDataFrame(SEED_ROWS, SEED_DDL)
    frame.createOrReplaceTempView(SEED_VIEW)


def _live_array_cell(table: pa.Table) -> tuple[str, list[Any]]:
    """pins: types-1/C-001 — one array query's (element type, values) either engine."""
    field = table.schema.field("r")
    assert isinstance(field.type, pa.ListType)
    return (str(field.type.value_type), table.column("r").to_pylist())


def _live_struct_cell(table: pa.Table) -> tuple[list[str], list[list[Any]]]:
    """pins: types-1/C-001 — one struct query's (field types, ordered values) either engine."""
    field = table.schema.field("r")
    assert isinstance(field.type, pa.StructType)
    types = [str(field.type.field(index).type) for index in range(field.type.num_fields)]
    rows = [list(row.values()) for row in table.column("r").to_pylist()]
    return (types, rows)


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
        "SELECT sum(CAST(i AS TINYINT)) AS r FROM types1_probe",
        "SELECT sum(CAST(i AS SMALLINT)) AS r FROM types1_probe",
        "SELECT sum(CAST(b AS DECIMAL(10, 2))) AS r FROM types1_probe",
        "SELECT bit_length(s) AS r FROM types1_probe WHERE s IS NOT NULL",
        "SELECT length(s) AS r FROM types1_probe WHERE s IS NOT NULL",
        "SELECT rank() OVER (ORDER BY i) AS r FROM types1_probe",
        "SELECT dense_rank() OVER (ORDER BY i) AS r FROM types1_probe",
        "SELECT row_number() OVER (ORDER BY i) AS r FROM types1_probe",
        "SELECT ntile(2) OVER (ORDER BY i) AS r FROM types1_probe",
        "SELECT percent_rank() OVER (ORDER BY i) AS r FROM types1_probe",
        "SELECT cume_dist() OVER (ORDER BY i) AS r FROM types1_probe",
        "SELECT from_unixtime(0) AS r",
        "SELECT from_unixtime(0, 'yyyy/MM/dd') AS r",
        "SELECT from_unixtime(0, 'EEE') AS r",
        "SELECT from_unixtime(0, 'dd/MM/yyyy HH:mm') AS r",
    ]:
        assert _live_type(engine, query) == _live_type(spark_engine, query)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_sketch_and_regression_counts_match_on_type_and_value(
    spark_engine: lp.Engine,
) -> None:
    """pins: types-1/C-003 — approx and regr_count match Spark on type and value.

    Nullability is carved out: repark derives nullable from the DataFusion
    kernels while Spark marks both non-null (registry row BL-18).
    """
    session = _session()
    _seed(session)
    _seed_oracle(spark_engine)
    engine = _live_engine(session)
    for query in [
        "SELECT approx_count_distinct(s) AS r FROM types1_probe",
        "SELECT regr_count(b, i) AS r FROM types1_probe",
    ]:
        mine = _live_type(engine, query)
        spark = _live_type(spark_engine, query)
        assert (mine[0], mine[2]) == (spark[0], spark[2])
        assert (mine[1], spark[1]) == (True, False)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_recoercion_shapes_match_the_oracle(spark_engine: lp.Engine) -> None:
    """pins: types-1/C-001 — live Spark agrees on narrowed literals in CASE and kin."""
    session = _session()
    _seed(session)
    _seed_oracle(spark_engine)
    engine = _live_engine(session)
    for query in [
        "SELECT CASE WHEN i > 1 THEN 1 ELSE 0 END AS r FROM types1_probe",
        "SELECT COALESCE(CAST(NULL AS BIGINT), 1) AS r",
        "SELECT 1 AS r UNION ALL SELECT CAST(2 AS BIGINT) AS r ORDER BY r",
        "SELECT CAST(b AS DECIMAL(10, 2)) + 1 AS r FROM types1_probe",
        "SELECT 1 + CAST(b AS DECIMAL(10, 2)) AS r FROM types1_probe",
    ]:
        assert _live_type(engine, query) == _live_type(spark_engine, query)
    narrow = "SELECT COALESCE(CAST(NULL AS INT), 1) AS r"
    mine = _live_type(engine, narrow)
    spark = _live_type(spark_engine, narrow)
    assert (mine[0], spark[0]) == ("int64", "int32")
    assert mine[1] is spark[1] is False
    assert mine[2] == spark[2]
    conditional = "SELECT IF(i > 1, 1, 0) AS r FROM types1_probe"
    mine = _live_type(engine, conditional)
    spark = _live_type(spark_engine, conditional)
    assert (mine[0], mine[2]) == (spark[0], spark[2])
    assert (mine[1], spark[1]) == (True, False)
    mapping = "SELECT map(1, 'a') AS r"
    mine = _live_type(engine, mapping)
    spark = _live_type(spark_engine, mapping)
    assert (mine[0], mine[2]) == (spark[0], spark[2])
    assert (mine[1], spark[1]) == (True, False)
    array_query = "SELECT array(1, 2) AS r"
    mine_nested = _live_array_cell(engine.arrow_of(engine.session.sql(array_query)))
    spark_nested = _live_array_cell(spark_engine.arrow_of(spark_engine.session.sql(array_query)))
    assert mine_nested == spark_nested
    struct_query = "SELECT struct(1, 'a') AS r"
    mine_nested = _live_struct_cell(engine.arrow_of(engine.session.sql(struct_query)))
    spark_nested = _live_struct_cell(spark_engine.arrow_of(spark_engine.session.sql(struct_query)))
    assert mine_nested == spark_nested


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_overflow_wraps_when_ansi_off(spark_engine: lp.Engine) -> None:
    """pins: types-1/C-002 — INTMAX+1 wraps identically with ANSI off, both engines."""
    session = _session(**{ANSI_KEY: "false"})
    spark_engine.session.conf.set("spark.sql.ansi.enabled", "false")
    try:
        query = "SELECT 2147483647 + 1 AS r"
        assert _live_type(_live_engine(session), query) == _live_type(spark_engine, query)
    finally:
        spark_engine.session.conf.set("spark.sql.ansi.enabled", "true")


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_from_unixtime_extremes_match_the_oracle(spark_engine: lp.Engine) -> None:
    """pins: types-1/C-006 — live Spark agrees on wrapped years, padded negatives, signs."""
    session = _session()
    _seed(session)
    _seed_oracle(spark_engine)
    engine = _live_engine(session)
    for seconds in [
        "15000000000000",
        "20000000000000",
        "-15000000000000",
        "9223372036854775807",
        "-9223372036854775808",
        "CAST(NULL AS BIGINT)",
        "-1",
        "1.5",
        "-77900000000",
        "-62200000000",
    ]:
        query = f"SELECT from_unixtime({seconds}) AS r"
        assert _live_type(engine, query) == _live_type(spark_engine, query)
    for seconds, pattern in [
        ("-77900000000", "yyyy"),
        ("-62200000000", "yyyy"),
        ("-77900000000", "yy"),
        ("-62200000000", "yy"),
    ]:
        query = f"SELECT from_unixtime({seconds}, '{pattern}') AS r"
        assert _live_type(engine, query) == _live_type(spark_engine, query)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_grouping_sets_match_on_value_with_type_carve_out(
    spark_engine: lp.Engine,
) -> None:
    """pins: types-1/C-004 — grouping values match; the (int32, int8) pair is pinned (TY-8)."""
    session = _session()
    _seed(session)
    _seed_oracle(spark_engine)
    engine = _live_engine(session)
    for query in [
        "SELECT i, grouping(i) AS r FROM types1_probe GROUP BY ROLLUP(i) ORDER BY i NULLS LAST",
        "SELECT i, grouping(i) AS r FROM types1_probe "
        "GROUP BY GROUPING SETS ((i), ()) ORDER BY i NULLS LAST",
    ]:
        mine = _live_type(engine, query)
        spark = _live_type(spark_engine, query)
        assert (mine[0], spark[0]) == ("int32", "int8")
        assert mine[1] is spark[1] is False
        assert mine[2] == spark[2]
