"""FN-D — datetime facade wrappers (value + Arrow type).

Each new ``functions`` name is pinned through ``ReparkSession`` on the Arrow path
(``to_arrow()``): value AND type. Alias names resolve and share a behavior case
with their canonical. ``unix_seconds`` / ``unix_millis`` pin toward-zero truncate
(not TZ-5 ``CAST`` floor) on a negative fractional instant.

Deferred this batch (no stubs): charter ENGINE-WORK
(``make_timestamp_ltz`` / ``make_timestamp_ntz``, ``make_ym_interval``,
``to_timestamp_ltz``, ``convert_timezone``, ``timestamp_add`` / ``timestamp_diff``)
FN-GT2 later shipped ``make_date`` / ``make_interval`` / ``make_dt_interval`` /
``unix_micros`` / ``date_diff``. Remaining honest-cut / DESIGN-GATED:
``localtimestamp`` / ``to_timestamp_ntz``.
"""

from __future__ import annotations

import datetime
import os

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.session.session_time_zone import SESSION_TIME_ZONE_KEY

_FN_D_DEFERRED: tuple[str, ...] = (
    "convert_timezone",
    "localtimestamp",
    "make_timestamp_ltz",
    "make_timestamp_ntz",
    "make_ym_interval",
    "timestamp_add",
    "timestamp_diff",
    "to_timestamp_ltz",
    "to_timestamp_ntz",
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-d").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


# ==================================================================================================
# Aliases
# ==================================================================================================


def test_day_alias_of_dayofmonth(spark: ReparkSession) -> None:
    assert callable(F.day)
    frame = spark.sql("SELECT DATE '2020-02-29' AS d")
    table = _table(frame.select(F.day("d").alias("a"), F.dayofmonth("d").alias("b")))
    assert table.column("a").to_pylist() == table.column("b").to_pylist() == [29]
    assert table.schema.field("a").type == table.schema.field("b").type
    assert pa.types.is_integer(table.schema.field("a").type)


def test_curdate_alias_of_current_date(spark: ReparkSession) -> None:
    assert callable(F.curdate)
    table = _table(spark.range(1).select(F.curdate().alias("a"), F.current_date().alias("b")))
    assert table.column("a").to_pylist() == table.column("b").to_pylist()
    assert table.schema.field("a").type == table.schema.field("b").type
    assert pa.types.is_date(table.schema.field("a").type)


def test_now_alias_of_current_timestamp(spark: ReparkSession) -> None:
    assert callable(F.now)
    table = _table(spark.range(1).select(F.now().alias("a"), F.current_timestamp().alias("b")))
    assert table.column("a").to_pylist() == table.column("b").to_pylist()
    assert table.schema.field("a").type == table.schema.field("b").type
    assert pa.types.is_timestamp(table.schema.field("a").type)
    assert table.schema.field("a").type.tz is not None


def test_dateadd_alias_of_date_add(spark: ReparkSession) -> None:
    assert callable(F.dateadd)
    frame = spark.sql("SELECT DATE '2020-01-01' AS d")
    table = _table(frame.select(F.dateadd("d", 1).alias("a"), F.date_add("d", 1).alias("b")))
    assert table.column("a").to_pylist() == table.column("b").to_pylist()
    assert table.column("a").to_pylist() == [datetime.date(2020, 1, 2)]
    assert pa.types.is_date(table.schema.field("a").type)


def test_datepart_alias_of_date_part(spark: ReparkSession) -> None:
    assert callable(F.datepart)
    frame = spark.sql("SELECT DATE '2020-02-29' AS d")
    table = _table(
        frame.select(
            F.datepart(F.lit("YEAR"), "d").alias("a"),
            F.date_part(F.lit("YEAR"), "d").alias("b"),
        )
    )
    assert table.column("a").to_pylist() == table.column("b").to_pylist() == [2020]
    assert table.schema.field("a").type == table.schema.field("b").type
    assert pa.types.is_integer(table.schema.field("a").type)
    # Sweep FIX: a bare str field is a column name (Spark 4.1.2). DF's kernel
    # still requires a constant field *value*, so the discriminating pin is
    # unresolved-column vs F.lit('YEAR').
    with pytest.raises(AnalysisException, match="No field named"):
        frame.select(F.date_part("YEAR", "d")).to_arrow()


def test_to_unix_timestamp_aliases_unix_timestamp_loud_gap() -> None:
    assert callable(F.to_unix_timestamp)
    with pytest.raises(UnsupportedOperationException, match="unix_timestamp"):
        F.to_unix_timestamp()
    with pytest.raises(UnsupportedOperationException, match="unix_timestamp"):
        F.unix_timestamp()


# ==================================================================================================
# unix_date / unix_seconds / unix_millis / date_from_unix_date
# ==================================================================================================


def test_unix_date_days_since_epoch(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT DATE '1970-01-02' AS d, CAST(NULL AS DATE) AS n")
    table = _table(frame.select(F.unix_date("d").alias("u"), F.unix_date("n").alias("z")))
    assert table.column("u").to_pylist() == [1]
    assert table.column("z").to_pylist() == [None]
    assert pa.types.is_int32(table.schema.field("u").type)


def test_unix_seconds_truncates_toward_zero_not_floor(spark: ReparkSession) -> None:
    """Hazard: TZ-5 ``CAST(ts AS BIGINT)`` floors; Spark ``unix_seconds`` truncates toward 0."""
    frame = spark.sql(
        "SELECT TIMESTAMP '1970-01-01 00:00:01.5' AS p, TIMESTAMP '1969-12-31 23:59:58.5' AS n"
    )
    table = _table(
        frame.select(
            F.unix_seconds("p").alias("up"),
            F.unix_seconds("n").alias("un"),
            F.col("n").cast("long").alias("floor_n"),
        )
    )
    assert table.column("up").to_pylist() == [1]
    assert table.column("un").to_pylist() == [-1]
    assert table.column("floor_n").to_pylist() == [-2]
    assert pa.types.is_int64(table.schema.field("up").type)
    assert pa.types.is_int64(table.schema.field("un").type)


def test_unix_millis_truncates_toward_zero(spark: ReparkSession) -> None:
    frame = spark.sql(
        "SELECT TIMESTAMP '1970-01-01 00:00:01.5' AS p, TIMESTAMP '1969-12-31 23:59:58.5' AS n"
    )
    table = _table(frame.select(F.unix_millis("p").alias("p"), F.unix_millis("n").alias("n")))
    assert table.column("p").to_pylist() == [1500]
    assert table.column("n").to_pylist() == [-1500]
    assert pa.types.is_int64(table.schema.field("p").type)


def test_date_from_unix_date_epoch_offset(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS n, -1 AS m, CAST(NULL AS INT) AS z")
    table = _table(
        frame.select(
            F.date_from_unix_date("n").alias("a"),
            F.date_from_unix_date("m").alias("b"),
            F.date_from_unix_date(0).alias("z0"),
            F.date_from_unix_date("z").alias("nz"),
        )
    )
    assert table.column("a").to_pylist() == [datetime.date(1970, 1, 2)]
    assert table.column("b").to_pylist() == [datetime.date(1969, 12, 31)]
    assert table.column("z0").to_pylist() == [datetime.date(1970, 1, 1)]
    assert table.column("nz").to_pylist() == [None]
    assert pa.types.is_date(table.schema.field("a").type)


def test_unix_date_round_trips_date_from_unix_date(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT DATE '2020-02-29' AS d, 18263 AS n")
    table = _table(
        frame.select(
            F.unix_date(F.date_from_unix_date("n")).alias("n2"),
            F.date_from_unix_date(F.unix_date("d")).alias("d2"),
        )
    )
    assert table.column("n2").to_pylist() == [18263]
    assert table.column("d2").to_pylist() == [datetime.date(2020, 2, 29)]
    assert pa.types.is_integer(table.schema.field("n2").type)
    assert pa.types.is_date(table.schema.field("d2").type)


# ==================================================================================================
# current_timezone — Session-only, no env reads
# ==================================================================================================


def test_current_timezone_default_is_utc_not_host_tz(spark: ReparkSession) -> None:
    previous = os.environ.get("TZ")
    os.environ["TZ"] = "America/Chicago"
    try:
        table = _table(spark.range(1).select(F.current_timezone().alias("z")))
    finally:
        if previous is None:
            del os.environ["TZ"]
        else:
            os.environ["TZ"] = previous
    assert table.column("z").to_pylist() == ["UTC"]
    assert pa.types.is_string(table.schema.field("z").type) or pa.types.is_large_string(
        table.schema.field("z").type
    )


def test_current_timezone_follows_session_builder_zone() -> None:
    session = (
        ReparkSession.builder.appName("pytest-fn-d-ny")
        .config(SESSION_TIME_ZONE_KEY, "America/New_York")
        .getOrCreate()
    )
    try:
        table = _table(session.range(1).select(F.current_timezone().alias("z")))
    finally:
        session.stop()
    assert table.column("z").to_pylist() == ["America/New_York"]
    assert pa.types.is_string(table.schema.field("z").type) or pa.types.is_large_string(
        table.schema.field("z").type
    )


def test_current_timezone_stays_string_beside_an_aggregate(spark: ReparkSession) -> None:
    table = _table(
        spark.createDataFrame([(1,), (2,)], ["x"]).select(
            F.sum("x").alias("s"),
            F.current_timezone().alias("z"),
        )
    )
    assert table.column("s").to_pylist() == [3]
    assert table.column("z").to_pylist() == ["UTC"]
    assert pa.types.is_string(table.schema.field("z").type) or pa.types.is_large_string(
        table.schema.field("z").type
    )


# ==================================================================================================
# Honest-cut / charter ENGINE-WORK names stay absent
# ==================================================================================================


@pytest.mark.parametrize("name", _FN_D_DEFERRED)
def test_fn_d_deferred_names_are_absent(name: str) -> None:
    assert not hasattr(F, name)
