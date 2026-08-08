"""Facade tests for WG2: the ``Window`` / ``row_number`` surface and the 13 date functions.

Every op runs against the real native engine (``maturin develop``); each behavior is pinned to an
**exact Spark-semantics fixture**. The ``test_parity_*`` goldens are recorded differentially from
**live PySpark 4.1.2** (``JAVA_HOME=/usr/lib/jvm/zulu-17-amd64``, Spark 4 needs Java 17 — the stale
"not runnable here" note predated the zulu-17 install) and compared through
``repark_parity.assert_frames_equal`` (name + Arrow type + field nullability + bit-exact value).

Genuine parity requires BOTH engines to infer the pinned type/nullability: the date parity goldens
carry ``nullable=True``, which only holds when the date spine is nullable, so ``_date_spine`` builds
``calendar_date`` via ``createDataFrame`` + ``cast`` (Spark's idiom) rather than an inline non-null
``VALUES (DATE '…')`` — an inline date literal is non-null in Spark but nullable in repark, so the
extractor/date-math/date-format nullability goldens would otherwise pin repark's shape as "Spark"
(the cycle-2 mispin class — see ``_date_spine`` + ``task/lessons.md`` 2026-07-19). The final test
reproduces the ``silver_dim_jobs.py`` dim-dates transform shape end to end (compared as exact rows).
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest

from repark import Column, ReparkSession, Window
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark import types as T  # noqa: N812 — PySpark idiom: `import ...types as T`
from repark_parity import assert_frames_equal


@pytest.fixture
def spark() -> ReparkSession:
    """A default session (PySpark ``SparkSession.builder.getOrCreate()``)."""
    return ReparkSession.builder.appName("pytest-wg2").getOrCreate()


def _date_spine(spark: ReparkSession, dates: list[str]) -> object:
    """A one-column nullable ``calendar_date`` DataFrame from ISO date strings (a dim-dates spine).

    Built via ``createDataFrame`` (string) + ``cast(DateType())`` so ``calendar_date`` is
    ``date32``/**nullable** on BOTH engines — Spark's ``createDataFrame`` idiom. An inline SQL
    ``VALUES (DATE '…')`` column is non-null in Spark but nullable in repark, so date-function
    outputs over it would pin repark's nullable shape as "Spark" (the parity-mispin class the
    cycle-2 remediation closed: the ``test_parity_*`` date goldens carry ``nullable=True``, which
    both engines agree on only when the spine is nullable). Values are identical either way, so the
    value-only date tests are unaffected (verified against live PySpark 4.1.2, both engines).
    """
    rows = [(value,) for value in dates]
    return spark.createDataFrame(rows, ["calendar_date_str"]).select(
        F.col("calendar_date_str").cast(T.DateType()).alias("calendar_date")
    )


def _single(df: object, column: str) -> object:
    """The single value of ``column`` from a one-row DataFrame."""
    return df.to_arrow().column(column).to_pylist()[0]  # type: ignore[attr-defined]


# ==================================================================================================
# Window + row_number
# ==================================================================================================


def test_row_number_is_a_column() -> None:
    assert isinstance(F.row_number(), Column)


def test_row_number_orders_rows_and_is_integer_typed(spark: ReparkSession) -> None:
    # Spark row_number() is IntegerType (the engine casts DataFusion's UInt64). ORDER BY v ascending
    # numbers the rows 1..n in value order.
    df = spark.sql("SELECT * FROM (VALUES (30), (10), (20)) AS t(v)")
    numbered = df.withColumn("rn", F.row_number().over(Window.orderBy(F.col("v").asc())))
    table = numbered.to_arrow()
    assert pa.types.is_int32(table.schema.field("rn").type)
    pairs = sorted((row["v"], row["rn"]) for row in table.to_pylist())
    assert pairs == [(10, 1), (20, 2), (30, 3)]


def test_row_number_restarts_per_partition(spark: ReparkSession) -> None:
    # PARTITION BY g restarts the numbering within each group (ordered by v ascending).
    df = spark.sql("SELECT * FROM (VALUES ('a', 5), ('a', 1), ('b', 9)) AS t(g, v)")
    spec = Window.partitionBy("g").orderBy(F.col("v").asc())
    numbered = df.withColumn("rn", F.row_number().over(spec))
    rows = {(row["g"], row["v"]): row["rn"] for row in numbered.to_arrow().to_pylist()}
    assert rows == {("a", 1): 1, ("a", 5): 2, ("b", 9): 1}


def test_row_number_descending_order(spark: ReparkSession) -> None:
    df = spark.sql("SELECT * FROM (VALUES (30), (10), (20)) AS t(v)")
    numbered = df.withColumn("rn", F.row_number().over(Window.orderBy(F.col("v").desc())))
    pairs = sorted((row["v"], row["rn"]) for row in numbered.to_arrow().to_pylist())
    assert pairs == [(10, 3), (20, 2), (30, 1)]


def test_over_on_a_non_window_column_raises(spark: ReparkSession) -> None:
    # `.over()` applies only to a window function; a plain column must fail loudly, not no-op.
    with pytest.raises(ValueError):
        F.col("v").over(Window.orderBy(F.col("v").asc()))


def test_window_spec_is_immutable() -> None:
    # partitionBy / orderBy each return a new spec (PySpark WindowSpec is immutable).
    base = Window.partitionBy("g")
    extended = base.orderBy("v")
    assert base is not extended


def test_modulo_operator(spark: ReparkSession) -> None:
    # The `%` operator (added so the dim-dates `(dayofweek + 5) % 7 + 1` ISO conversion runs).
    df = spark.sql("SELECT * FROM (VALUES (7), (8), (13)) AS t(a)")
    out = df.withColumn("m", F.col("a") % 7).withColumn("rm", 20 % F.col("a"))
    rows = {row["a"]: (row["m"], row["rm"]) for row in out.to_arrow().to_pylist()}
    assert rows == {7: (0, 6), 8: (1, 4), 13: (6, 7)}


# ==================================================================================================
# Extractors: year / month / quarter / weekofyear / dayofweek / dayofmonth / dayofyear
# ==================================================================================================


def test_calendar_extractors(spark: ReparkSession) -> None:
    # 2024-03-15 is a Friday in leap year 2024; day 75; ISO week 11.
    df = _date_spine(spark, ["2024-03-15"]).select(
        F.year("calendar_date").alias("year"),
        F.month("calendar_date").alias("month"),
        F.quarter("calendar_date").alias("quarter"),
        F.weekofyear("calendar_date").alias("week"),
        F.dayofmonth("calendar_date").alias("dom"),
        F.dayofyear("calendar_date").alias("doy"),
    )
    row = df.to_arrow().to_pylist()[0]
    assert row == {"year": 2024, "month": 3, "quarter": 1, "week": 11, "dom": 15, "doy": 75}


def test_dayofweek_uses_spark_sunday_is_one(spark: ReparkSession) -> None:
    # The headline trap: Spark dayofweek is 1=Sunday..7=Saturday. 2024-01-07 Sun, 08 Mon, 13 Sat.
    df = _date_spine(spark, ["2024-01-07", "2024-01-08", "2024-01-13"]).withColumn(
        "dow", F.dayofweek("calendar_date")
    )
    rows = {
        str(row["calendar_date"]): row["dow"]
        for row in df.select("calendar_date", "dow").to_arrow().to_pylist()
    }
    assert rows == {"2024-01-07": 1, "2024-01-08": 2, "2024-01-13": 7}


def test_extractors_accept_a_column_object_not_only_a_name(spark: ReparkSession) -> None:
    df = _date_spine(spark, ["2021-01-01"]).withColumn("wk", F.weekofyear(F.col("calendar_date")))
    # 2021-01-01 is a Friday in ISO week 53 of 2020.
    assert _single(df.select("wk"), "wk") == 53


# ==================================================================================================
# last_day / add_months / date_add
# ==================================================================================================


def test_last_day(spark: ReparkSession) -> None:
    df = _date_spine(spark, ["2025-02-14"]).withColumn("last", F.last_day("calendar_date"))
    assert _single(df.select("last"), "last") == dt.date(2025, 2, 28)


def test_add_months_clamps_to_month_end(spark: ReparkSession) -> None:
    # Spark add_months clamps to the last day when the start is month-end OR the day overflows.
    df = _date_spine(spark, ["2016-02-29"])
    assert _single(
        df.withColumn("r", F.add_months("calendar_date", 12)).select("r"), "r"
    ) == dt.date(2017, 2, 28)
    assert _single(
        df.withColumn("r", F.add_months("calendar_date", -12)).select("r"), "r"
    ) == dt.date(2015, 2, 28)
    # A mid-month day is carried across unchanged.
    mid = _date_spine(spark, ["2025-03-15"]).withColumn("r", F.add_months("calendar_date", -1))
    assert _single(mid.select("r"), "r") == dt.date(2025, 2, 15)


def test_add_months_preserves_month_end_into_longer_months(spark: ReparkSession) -> None:
    # The disambiguating case for Spark's Hive-derived algorithm: a source on the LAST day of a
    # SHORT month lands on the last day of the (longer) target month — 2015-02-28 + 1 month is
    # 2015-03-31, NOT 03-28. A naive java.time-style plusMonths keeps day 28 and would pass
    # every other fixture in this file; only this case pins the end-of-month branch.
    df = _date_spine(spark, ["2015-02-28"])
    assert _single(
        df.withColumn("r", F.add_months("calendar_date", 1)).select("r"), "r"
    ) == dt.date(2015, 3, 31)
    # And backwards: April 30 (month-end) minus one month is March 31 (month-end), not March 30.
    back = _date_spine(spark, ["2025-04-30"]).withColumn("r", F.add_months("calendar_date", -1))
    assert _single(back.select("r"), "r") == dt.date(2025, 3, 31)


def test_add_months_accepts_a_column_count(spark: ReparkSession) -> None:
    df = _date_spine(spark, ["2025-01-31"]).withColumn("r", F.add_months("calendar_date", F.lit(1)))
    assert _single(df.select("r"), "r") == dt.date(2025, 2, 28)


def test_date_add(spark: ReparkSession) -> None:
    df = _date_spine(spark, ["2025-01-31"])
    assert _single(df.withColumn("r", F.date_add("calendar_date", 1)).select("r"), "r") == dt.date(
        2025, 2, 1
    )
    # Negative days go backwards across the month boundary.
    assert _single(df.withColumn("r", F.date_add("calendar_date", -1)).select("r"), "r") == dt.date(
        2025, 1, 30
    )


# ==================================================================================================
# date_format
# ==================================================================================================


@pytest.mark.parametrize(
    ("date", "pattern", "expected"),
    [
        ("2025-01-08", "yyyyMMdd", "20250108"),
        ("2025-01-08", "yyyyMM", "202501"),
        ("2025-05-14", "yyyy'Q'q", "2025Q2"),  # single-quoted Q literal; q = quarter number
        ("2025-01-08", "MMMM", "January"),
        ("2025-01-08", "MMM", "Jan"),
        ("2025-01-08", "EEEE", "Wednesday"),
        ("2025-01-08", "EEE", "Wed"),
    ],
)
def test_date_format_patterns(spark: ReparkSession, date: str, pattern: str, expected: str) -> None:
    df = _date_spine(spark, [date]).withColumn("f", F.date_format("calendar_date", pattern))
    assert _single(df.select("f"), "f") == expected


def test_date_format_unsupported_pattern_raises(spark: ReparkSession) -> None:
    # An unsupported pattern letter must fail loudly, not emit a silently-wrong string.
    df = _date_spine(spark, ["2025-01-08"]).withColumn("f", F.date_format("calendar_date", "a"))
    with pytest.raises(RuntimeError):
        df.to_arrow()


# ==================================================================================================
# trunc / date_trunc
# ==================================================================================================


def test_trunc_granularities(spark: ReparkSession) -> None:
    # 2025-05-14 is a Wednesday in Q2.
    df = _date_spine(spark, ["2025-05-14"])
    assert _single(df.withColumn("r", F.trunc("calendar_date", "MM")).select("r"), "r") == dt.date(
        2025, 5, 1
    )
    assert _single(
        df.withColumn("r", F.trunc("calendar_date", "YEAR")).select("r"), "r"
    ) == dt.date(2025, 1, 1)
    assert _single(
        df.withColumn("r", F.trunc("calendar_date", "quarter")).select("r"), "r"
    ) == dt.date(2025, 4, 1)
    # WEEK truncates to Monday (ISO); 2025-05-14 (Wed) → 2025-05-12 (Mon).
    assert _single(
        df.withColumn("r", F.trunc("calendar_date", "week")).select("r"), "r"
    ) == dt.date(2025, 5, 12)


def test_trunc_invalid_format_returns_null(spark: ReparkSession) -> None:
    # Spark trunc accepts 'QUARTER', not 'Q' — an invalid format yields NULL, not an error.
    df = _date_spine(spark, ["2025-05-14"]).withColumn("r", F.trunc("calendar_date", "Q"))
    assert _single(df.select("r"), "r") is None


def test_date_trunc_argument_order_and_granularity(spark: ReparkSession) -> None:
    # PySpark date_trunc takes the format FIRST; returns a timestamp. Cast to date to assert.
    df = _date_spine(spark, ["2025-05-14"])
    week = df.withColumn("r", F.date_trunc("week", "calendar_date").cast(T.DateType()))
    assert _single(week.select("r"), "r") == dt.date(2025, 5, 12)
    quarter = df.withColumn("r", F.date_trunc("quarter", "calendar_date").cast(T.DateType()))
    assert _single(quarter.select("r"), "r") == dt.date(2025, 4, 1)
    # The result of date_trunc is a timestamp type.
    assert pa.types.is_timestamp(
        df.withColumn("r", F.date_trunc("month", "calendar_date")).to_arrow().schema.field("r").type
    )


# ==================================================================================================
# Spark-parity fixtures (hand-computed goldens through the differential core)
# ==================================================================================================


def test_parity_extractor_row(spark: ReparkSession) -> None:
    # Two dates across a quarter boundary and a leap day; every extractor pinned at once.
    source = _date_spine(spark, ["2016-02-29", "2025-04-01"])
    result = source.select(
        F.year("calendar_date").alias("year"),
        F.quarter("calendar_date").alias("quarter"),
        F.month("calendar_date").alias("month"),
        F.dayofmonth("calendar_date").alias("day"),
        F.dayofyear("calendar_date").alias("day_of_year"),
        F.dayofweek("calendar_date").alias("day_of_week"),
    )
    golden = pa.table(
        {
            "year": pa.array([2016, 2025], pa.int32()),
            "quarter": pa.array([1, 2], pa.int32()),
            "month": pa.array([2, 4], pa.int32()),
            "day": pa.array([29, 1], pa.int32()),
            "day_of_year": pa.array([60, 91], pa.int32()),
            "day_of_week": pa.array([2, 3], pa.int32()),  # 2016-02-29 Mon, 2025-04-01 Tue
        }
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_parity_date_math_row(spark: ReparkSession) -> None:
    # add_months (month-end clamp), last_day, date_add, trunc — the calendar-math set together.
    source = _date_spine(spark, ["2016-02-29", "2025-01-15"])
    result = source.select(
        F.add_months("calendar_date", -12).alias("prior_year"),
        F.last_day("calendar_date").alias("month_end"),
        F.date_add("calendar_date", 1).alias("next_day"),
        F.trunc("calendar_date", "quarter").alias("quarter_start"),
    )
    golden = pa.table(
        {
            "prior_year": pa.array([dt.date(2015, 2, 28), dt.date(2024, 1, 15)], pa.date32()),
            "month_end": pa.array([dt.date(2016, 2, 29), dt.date(2025, 1, 31)], pa.date32()),
            "next_day": pa.array([dt.date(2016, 3, 1), dt.date(2025, 1, 16)], pa.date32()),
            "quarter_start": pa.array([dt.date(2016, 1, 1), dt.date(2025, 1, 1)], pa.date32()),
        }
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_parity_date_format_row(spark: ReparkSession) -> None:
    source = _date_spine(spark, ["2025-01-08", "2025-05-14"])
    result = source.select(
        F.date_format("calendar_date", "yyyyMMdd").alias("date_key"),
        F.date_format("calendar_date", "yyyy'Q'q").alias("year_quarter"),
        F.date_format("calendar_date", "MMMM").alias("month_name"),
    )
    golden = pa.table(
        {
            "date_key": pa.array(["20250108", "20250514"], pa.string()),
            "year_quarter": pa.array(["2025Q1", "2025Q2"], pa.string()),
            "month_name": pa.array(["January", "May"], pa.string()),
        }
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_parity_row_number_ordered(spark: ReparkSession) -> None:
    # orderBy pins the row order → an order-sensitive differential; row_number is IntegerType.
    # createDataFrame (NOT inline VALUES) so both engines infer v=int64/nullable (live PySpark
    # 4.1.2); an inline VALUES source would type v as int32/non-null on Spark, making the int64/
    # nullable `v` pin repark-only. `rn` (row_number) is int32/non-null on both — a real Spark
    # guarantee.
    source = spark.createDataFrame([(30,), (10,), (20,)], ["v"])
    result = (
        source.withColumn("rn", F.row_number().over(Window.orderBy(F.col("v").asc())))
        .orderBy(F.col("rn").asc())
        .select("rn", "v")
    )
    # row_number is never NULL, so `rn` is non-nullable — and the harness now asserts field
    # nullability as part of the schema signature (name + type + nullable + bit-exact values), so
    # the engine must reproduce this non-null guarantee.
    golden = pa.table(
        [pa.array([1, 2, 3], pa.int32()), pa.array([10, 20, 30], pa.int64())],
        schema=pa.schema(
            [
                pa.field("rn", pa.int32(), nullable=False),
                pa.field("v", pa.int64(), nullable=True),
            ]
        ),
    )
    assert_frames_equal(result.to_arrow(), golden, order_sensitive=True)


# ==================================================================================================
# Acceptance kernel — the silver_dim_jobs.py dim-dates transform shape, end to end
# ==================================================================================================


def test_acceptance_dim_dates_transform_shape(spark: ReparkSession) -> None:
    """Reproduce the ``silver_dim_jobs.py`` dim-dates transform shape on the facade and assert the
    exact output rows.

    A date spine → a surrogate ``date_key`` (``row_number`` over a ``Window`` ordered by the date,
    the ``process_silver.py`` pattern) + the dim-dates date columns: ``date_format`` keys/names,
    ``year``/``quarter``/``month``, the ``dayofweek``→ISO arithmetic (``(dayofweek + 5) % 7 + 1``,
    verbatim from the script), ``trunc``/``date_trunc``/``last_day``/``add_months``/``date_add``
    period boundaries. Goldens are hand-computed from Spark's documented calendar semantics.
    """
    spine = _date_spine(
        spark,
        ["2025-01-05", "2025-01-06", "2025-03-31", "2025-04-01", "2016-02-29"],
    )
    order_by_date = Window.orderBy(F.col("calendar_date").asc())
    quarter_start = F.trunc("calendar_date", "quarter")
    result = (
        spine.withColumn("row_key", F.row_number().over(order_by_date))
        .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast(T.IntegerType()))
        .withColumn("year", F.year("calendar_date"))
        .withColumn("quarter", F.quarter("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("year_quarter", F.date_format("calendar_date", "yyyy'Q'q"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("day_of_week_iso", (F.dayofweek("calendar_date") + 5) % 7 + 1)
        .withColumn("week_of_year_iso", F.weekofyear("calendar_date"))
        .withColumn("month_start", F.trunc("calendar_date", "MM"))
        .withColumn("month_end", F.last_day("calendar_date"))
        .withColumn("quarter_start", quarter_start)
        .withColumn("quarter_end", F.date_add(F.add_months(quarter_start, 3), -1))
        .withColumn("week_start", F.date_trunc("week", "calendar_date").cast(T.DateType()))
        .withColumn("prior_year", F.add_months("calendar_date", -12))
        .orderBy(F.col("row_key").asc())
        .drop("calendar_date")
    )

    # Rows in row_key order (calendar order): 2016-02-29, 2025-01-05, 2025-01-06, 2025-03-31,
    # 2025-04-01. Every value follows Spark's documented calendar semantics. Compared as exact row
    # dicts (values, order-sensitive by row_key) so the ~15-column nullability bookkeeping does not
    # obscure the assertion; the focused `test_parity_*` cases above pin schema/type through the
    # differential core.
    def date(year: int, month: int, day: int) -> dt.date:
        return dt.date(year, month, day)

    expected_rows = [
        {
            "row_key": 1,
            "date_key": 20160229,
            "year": 2016,
            "quarter": 1,
            "month": 2,
            "year_quarter": "2016Q1",
            "month_name": "February",
            "day_of_week_iso": 1,
            "week_of_year_iso": 9,
            "month_start": date(2016, 2, 1),
            "month_end": date(2016, 2, 29),
            "quarter_start": date(2016, 1, 1),
            "quarter_end": date(2016, 3, 31),
            "week_start": date(2016, 2, 29),
            "prior_year": date(2015, 2, 28),
        },
        {
            "row_key": 2,
            "date_key": 20250105,
            "year": 2025,
            "quarter": 1,
            "month": 1,
            "year_quarter": "2025Q1",
            "month_name": "January",
            "day_of_week_iso": 7,
            "week_of_year_iso": 1,
            "month_start": date(2025, 1, 1),
            "month_end": date(2025, 1, 31),
            "quarter_start": date(2025, 1, 1),
            "quarter_end": date(2025, 3, 31),
            "week_start": date(2024, 12, 30),
            "prior_year": date(2024, 1, 5),
        },
        {
            "row_key": 3,
            "date_key": 20250106,
            "year": 2025,
            "quarter": 1,
            "month": 1,
            "year_quarter": "2025Q1",
            "month_name": "January",
            "day_of_week_iso": 1,
            "week_of_year_iso": 2,
            "month_start": date(2025, 1, 1),
            "month_end": date(2025, 1, 31),
            "quarter_start": date(2025, 1, 1),
            "quarter_end": date(2025, 3, 31),
            "week_start": date(2025, 1, 6),
            "prior_year": date(2024, 1, 6),
        },
        {
            "row_key": 4,
            "date_key": 20250331,
            "year": 2025,
            "quarter": 1,
            "month": 3,
            "year_quarter": "2025Q1",
            "month_name": "March",
            "day_of_week_iso": 1,
            "week_of_year_iso": 14,
            "month_start": date(2025, 3, 1),
            "month_end": date(2025, 3, 31),
            "quarter_start": date(2025, 1, 1),
            "quarter_end": date(2025, 3, 31),
            "week_start": date(2025, 3, 31),
            "prior_year": date(2024, 3, 31),
        },
        {
            "row_key": 5,
            "date_key": 20250401,
            "year": 2025,
            "quarter": 2,
            "month": 4,
            "year_quarter": "2025Q2",
            "month_name": "April",
            "day_of_week_iso": 2,
            "week_of_year_iso": 14,
            "month_start": date(2025, 4, 1),
            "month_end": date(2025, 4, 30),
            "quarter_start": date(2025, 4, 1),
            "quarter_end": date(2025, 6, 30),
            "week_start": date(2025, 3, 31),
            "prior_year": date(2024, 4, 1),
        },
    ]
    assert result.to_arrow().to_pylist() == expected_rows
