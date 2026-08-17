"""FN-GT2 — leftover THIN-WIRE datetime / collections / url / bitmap.

Each new ``functions`` name is pinned through ``ReparkSession`` on the Arrow
path (``to_arrow()``): value AND type. ``datediff`` stays the DISPOSED-STUB.
``shuffle`` pins type + length, not order.

Oracle: live PySpark 4.1.2 against the pinned OpenJDK 21.
"""

from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkException, UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.session.session_time_zone import SESSION_TIME_ZONE_KEY


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-gt2").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


def _is_string(field_type: pa.DataType) -> bool:
    return pa.types.is_string(field_type) or pa.types.is_large_string(field_type)


def _as_dict(value: object) -> dict[object, object]:
    if isinstance(value, dict):
        return value
    return dict(value)  # type: ignore[arg-type]


def _la_session() -> ReparkSession:
    return (
        ReparkSession.builder.appName("pytest-fn-gt2-la")
        .config(SESSION_TIME_ZONE_KEY, "America/Los_Angeles")
        .getOrCreate()
    )


def test_make_date(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.make_date(2020, 1, 2).alias("d"),
            F.make_date(2020, 2, 30).alias("bad"),
            F.make_date(F.lit(None), 1, 1).alias("n"),
        )
    )
    assert table.column("d").to_pylist() == [datetime.date(2020, 1, 2)]
    assert table.column("bad").to_pylist() == [None]
    assert table.column("n").to_pylist() == [None]
    assert table.schema.field("d").type == pa.date32()
    named = spark.createDataFrame([(2020, 1, 2)], ["y", "m", "day"])
    named_table = _table(named.select(F.make_date("y", "m", "day").alias("d2")))
    assert named_table.column("d2").to_pylist() == [datetime.date(2020, 1, 2)]


def test_make_interval_and_dt(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.make_interval(days=1).alias("i"),
            F.make_dt_interval(1, 0, 0, 0).alias("dt"),
            F.make_interval(days=F.lit(None)).alias("ni"),
            F.make_dt_interval(F.lit(None), 0, 0, 0).alias("ndt"),
        )
    )
    interval = table.column("i").to_pylist()[0]
    assert interval is not None
    assert (interval.months, interval.days, interval.nanoseconds) == (0, 1, 0)
    assert table.schema.field("i").type == pa.month_day_nano_interval()
    assert table.column("dt").to_pylist() == [datetime.timedelta(days=1)]
    assert table.schema.field("dt").type == pa.duration("us")
    assert table.column("ni").to_pylist() == [None]
    assert table.column("ndt").to_pylist() == [None]
    as_string = _table(frame.select(F.make_interval(days=1).cast("string").alias("s")))
    assert as_string.column("s").to_pylist() == ["1 days"]


def test_make_interval_str_is_column_name(spark: ReparkSession) -> None:
    """W2: ``str`` parts are column names (``F.make_interval("y")`` uses column ``y``).

    Spark 4.1.2 stringifies this as ``'2 years'``. Repark's calendar-interval
    display is ``'24 mons'`` (same 2-year span). The pin is the column-name
    direction, not the display spelling.
    """
    frame = spark.createDataFrame([(2,)], ["y"])
    table = _table(frame.select(F.make_interval("y").alias("i")))
    interval = table.column("i").to_pylist()[0]
    assert interval is not None
    assert (interval.months, interval.days, interval.nanoseconds) == (24, 0, 0)
    assert table.schema.field("i").type == pa.month_day_nano_interval()
    as_string = _table(frame.select(F.make_interval("y").cast("string").alias("s")))
    assert as_string.column("s").to_pylist() == ["24 mons"]


def test_unix_micros_and_date_diff(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.unix_micros(F.lit("1970-01-01 00:00:00")).alias("u"),
            F.unix_micros(F.lit(None).cast("timestamp")).alias("un"),
            F.date_diff(F.lit(datetime.date(2020, 1, 3)), F.lit(datetime.date(2020, 1, 1))).alias(
                "d"
            ),
            F.date_diff(F.lit(None).cast("date"), F.lit(datetime.date(2020, 1, 1))).alias("dn"),
        )
    )
    assert table.column("u").to_pylist() == [0]
    assert table.schema.field("u").type == pa.int64()
    assert table.column("un").to_pylist() == [None]
    assert table.column("d").to_pylist() == [2]
    assert table.schema.field("d").type == pa.int32()
    assert table.column("dn").to_pylist() == [None]
    ts_frame = spark.createDataFrame([("1970-01-01 00:00:00",)], ["ts"])
    ts_table = _table(ts_frame.select(F.unix_micros("ts").alias("from_col")))
    assert ts_table.column("from_col").to_pylist() == [0]


def test_unix_micros_non_utc_non_epoch() -> None:
    """P3 / W5: LA wall-clock is not the UTC-epoch fixed point."""
    session = _la_session()
    try:
        table = _table(
            session.range(1).select(
                F.unix_micros(F.lit("1970-01-01 00:00:00")).alias("epoch"),
                F.unix_micros(F.lit("2015-07-22 10:00:00")).alias("u"),
            )
        )
        col_frame = session.createDataFrame([("2015-07-22 10:00:00",)], ["ts"])
        col_table = _table(col_frame.select(F.unix_micros("ts").alias("from_col")))
    finally:
        session.stop()
    # Live 4.1.2: to_timestamp('1970-01-01 00:00:00') in America/Los_Angeles.
    assert table.column("epoch").to_pylist() == [28_800_000_000]
    assert table.column("u").to_pylist() == [1_437_584_400_000_000]
    assert table.schema.field("u").type == pa.int64()
    assert col_table.column("from_col").to_pylist() == [1_437_584_400_000_000]


def test_datetime_names_hold_under_non_utc_session() -> None:
    """W5: date/interval names are zone-invariant; unix_micros is not."""
    session = _la_session()
    try:
        table = _table(
            session.range(1).select(
                F.make_date(2020, 1, 2).alias("d"),
                F.date_diff(
                    F.lit(datetime.date(2020, 1, 3)), F.lit(datetime.date(2020, 1, 1))
                ).alias("dd"),
                F.make_interval(days=1).cast("string").alias("i"),
                F.make_dt_interval(1, 0, 0, 0).alias("dt"),
            )
        )
    finally:
        session.stop()
    assert table.column("d").to_pylist() == [datetime.date(2020, 1, 2)]
    assert table.column("dd").to_pylist() == [2]
    assert table.column("i").to_pylist() == ["1 days"]
    assert table.column("dt").to_pylist() == [datetime.timedelta(days=1)]


def test_datediff_stub_untouched(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException, match="datediff"):
        F.datediff(F.lit(datetime.date(2020, 1, 3)), F.lit(datetime.date(2020, 1, 1)))


def test_element_at_one_based_and_zero_refuses(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT array(10, 20, 30) AS a")
    table = _table(
        frame.select(
            F.element_at("a", 1).alias("e1"),
            F.element_at("a", 2).alias("e2"),
            F.element_at("a", None).alias("en"),
        )
    )
    assert table.column("e1").to_pylist() == [10]
    assert table.column("e2").to_pylist() == [20]
    assert table.column("en").to_pylist() == [None]
    null_frame = spark.sql("SELECT CAST(NULL AS ARRAY<INT>) AS a")
    null_table = _table(null_frame.select(F.element_at("a", 1).alias("an")))
    assert null_table.column("an").to_pylist() == [None]
    assert pa.types.is_integer(table.schema.field("e1").type)
    with pytest.raises(PySparkException, match="INVALID_INDEX_OF_ZERO"):
        frame.select(F.element_at("a", 0)).to_arrow()


def test_element_at_string_is_literal_map_key(spark: ReparkSession) -> None:
    """W1: ``'b'`` is the map key ``b``, not column ``b`` (value ``'a'``)."""
    frame = spark.sql(
        "SELECT map('a', CAST(1.0 AS DOUBLE), 'b', CAST(2.0 AS DOUBLE)) AS data, 'a' AS b"
    )
    table = _table(
        frame.select(
            F.element_at("data", "b").alias("lit_key"),
            F.element_at("data", F.col("b")).alias("col_key"),
        )
    )
    assert table.column("lit_key").to_pylist() == [2.0]
    assert table.column("col_key").to_pylist() == [1.0]


def test_array_compact_drops_nulls_only(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT array(1, CAST(NULL AS INT), 1) AS a")
    table = _table(frame.select(F.array_compact("a").alias("c")))
    assert table.column("c").to_pylist() == [[1, 1]]
    null_table = _table(
        spark.sql("SELECT CAST(NULL AS ARRAY<INT>) AS a").select(F.array_compact("a").alias("n"))
    )
    assert null_table.column("n").to_pylist() == [None]
    assert pa.types.is_list(table.schema.field("c").type) or pa.types.is_large_list(
        table.schema.field("c").type
    )


def test_shuffle_preserves_type_and_length(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(frame.select(F.shuffle(F.array(F.lit(1), F.lit(2), F.lit(3))).alias("s")))
    values = table.column("s").to_pylist()[0]
    assert sorted(values) == [1, 2, 3]
    assert pa.types.is_list(table.schema.field("s").type) or pa.types.is_large_list(
        table.schema.field("s").type
    )
    # NULL-array shuffle panics inside datafusion-spark's kernel (arrow-data
    # primitive transform). Residual — do not pin a crashing input.


def test_map_from_entries_and_str_to_map(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT array(named_struct('key', 'a', 'value', 1)) AS e")
    table = _table(
        frame.select(
            F.map_from_entries("e").alias("m"),
            F.str_to_map(F.lit("a:1,b:2")).alias("s"),
            F.str_to_map(F.lit(None).cast("string")).alias("sn"),
        )
    )
    assert _as_dict(table.column("m").to_pylist()[0]) == {"a": 1}
    assert pa.types.is_map(table.schema.field("m").type)
    assert _as_dict(table.column("s").to_pylist()[0]) == {"a": "1", "b": "2"}
    assert pa.types.is_map(table.schema.field("s").type)
    assert table.column("sn").to_pylist() == [None]
    null_entries = spark.sql("SELECT CAST(NULL AS ARRAY<STRUCT<key: STRING, value: INT>>) AS e")
    null_table = _table(null_entries.select(F.map_from_entries("e").alias("mn")))
    assert null_table.column("mn").to_pylist() == [None]


def test_str_to_map_delimiters_are_regex(spark: ReparkSession) -> None:
    """W4: both pair and key/value delimiters are regular expressions."""
    table = _table(
        spark.range(1).select(
            F.str_to_map(F.lit("a:1,b:2c:3"), F.lit("[,c]"), F.lit(":")).alias("r"),
            F.str_to_map(F.lit("a:1|b:2"), F.lit("\\|"), F.lit(":")).alias("p"),
            F.str_to_map(F.lit("ax1,bx2"), F.lit(","), F.lit("[x]")).alias("kv"),
            F.str_to_map(F.lit("a:1,,b:2")).alias("empty_pair"),
        )
    )
    assert _as_dict(table.column("r").to_pylist()[0]) == {"": "3", "a": "1", "b": "2"}
    assert _as_dict(table.column("p").to_pylist()[0]) == {"a": "1", "b": "2"}
    assert _as_dict(table.column("kv").to_pylist()[0]) == {"a": "1", "b": "2"}
    assert _as_dict(table.column("empty_pair").to_pylist()[0]) == {"": None, "a": "1", "b": "2"}
    assert pa.types.is_map(table.schema.field("r").type)
    raw = _table(
        spark.range(1).select(F.str_to_map(F.lit("a:1,b:2c:3"), "[,c]", ":").alias("raw_str"))
    )
    assert _as_dict(raw.column("raw_str").to_pylist()[0]) == {"": "3", "a": "1", "b": "2"}
    sql_table = _table(spark.sql("SELECT str_to_map('a:1,b:2c:3', '[,c]', ':') AS m"))
    assert _as_dict(sql_table.column("m").to_pylist()[0]) == {"": "3", "a": "1", "b": "2"}


def test_make_dt_interval_str_is_column_name(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,)], ["d"])
    table = _table(frame.select(F.make_dt_interval("d").alias("dt")))
    assert table.column("dt").to_pylist() == [datetime.timedelta(days=1)]
    assert table.schema.field("dt").type == pa.duration("us")


def test_parse_url_and_try(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.parse_url(F.lit("https://spark.apache.org/path"), "HOST").alias("h"),
            F.try_parse_url(F.lit("not a url"), "HOST").alias("bad"),
            F.try_parse_url(F.lit(None).cast("string"), "HOST").alias("n"),
        )
    )
    assert table.column("h").to_pylist() == ["spark.apache.org"]
    assert _is_string(table.schema.field("h").type)
    assert table.column("bad").to_pylist() == [None]
    assert _is_string(table.schema.field("bad").type)
    assert table.column("n").to_pylist() == [None]
    # Spark 4.1.2 raises INVALID_URL on schemeless text; DF HOST is NULL.
    schemeless = _table(frame.select(F.parse_url(F.lit("not a url"), "HOST").alias("inv")))
    assert schemeless.column("inv").to_pylist() == [None]
    # DF also raises on some ://-malformed URLs (same as Spark). Not "always NULL".
    with pytest.raises(PySparkException, match="url is invalid"):
        frame.select(F.parse_url(F.lit("inva lid://host"), "HOST")).to_arrow()
    try_malformed = _table(
        frame.select(F.try_parse_url(F.lit("inva lid://host"), "HOST").alias("try_mal"))
    )
    assert try_malformed.column("try_mal").to_pylist() == [None]
    null_parse = _table(
        frame.select(F.parse_url(F.lit(None).cast("string"), "HOST").alias("parse_null"))
    )
    assert null_parse.column("parse_null").to_pylist() == [None]
    query = _table(
        frame.select(
            F.parse_url(F.lit("https://spark.apache.org/path?query=1"), "QUERY", "query").alias("q")
        )
    )
    assert query.column("q").to_pylist() == ["1"]
    # Spark 4.1.2 compiles QUERY key as Java Pattern ('f.o' matches foo).
    # DF kernel is exact equality — pin the honest miss, do not claim regex.
    regex_key = _table(
        frame.select(F.parse_url(F.lit("https://x/?foo=1"), "QUERY", "f.o").alias("re"))
    )
    assert regex_key.column("re").to_pylist() == [None]


def test_url_encode_decode(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.url_encode(F.lit("a b")).alias("e"),
            F.url_decode(F.lit("a+b")).alias("d"),
            F.try_url_decode(F.lit("%ZZ")).alias("bad"),
            F.url_encode(F.lit(None).cast("string")).alias("en"),
            F.url_decode(F.lit(None).cast("string")).alias("dn"),
            F.try_url_decode(F.lit(None).cast("string")).alias("tn"),
        )
    )
    assert table.column("e").to_pylist() == ["a+b"]
    assert table.column("d").to_pylist() == ["a b"]
    assert table.column("bad").to_pylist() == [None]
    assert table.column("en").to_pylist() == [None]
    assert table.column("dn").to_pylist() == [None]
    assert table.column("tn").to_pylist() == [None]
    assert _is_string(table.schema.field("e").type)
    assert _is_string(table.schema.field("d").type)
    assert _is_string(table.schema.field("bad").type)
    with pytest.raises(PySparkException, match="percent-encoding"):
        frame.select(F.url_decode(F.lit("%ZZ"))).to_arrow()


def test_bitmap_scalars(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.bitmap_bit_position(F.lit(1)).alias("p"),
            F.bitmap_bucket_number(F.lit(1)).alias("b"),
            F.bitmap_bit_position(F.lit(123)).alias("p123"),
            F.bitmap_bucket_number(F.lit(123)).alias("b123"),
            F.bitmap_bit_position(F.lit(32769)).alias("p_wrap"),
            F.bitmap_bucket_number(F.lit(32769)).alias("b_wrap"),
            F.bitmap_bit_position(F.lit(0)).alias("p0"),
            F.bitmap_bucket_number(F.lit(0)).alias("b0"),
            F.bitmap_bit_position(F.lit(-1)).alias("pneg"),
            F.bitmap_bucket_number(F.lit(-1)).alias("bneg"),
            F.bitmap_count(F.unhex(F.lit("FF"))).alias("c"),
            F.bitmap_bit_position(F.lit(None).cast("long")).alias("pos_null"),
            F.bitmap_bucket_number(F.lit(None).cast("long")).alias("bucket_null"),
        )
    )
    assert table.column("p").to_pylist() == [0]
    assert table.column("b").to_pylist() == [1]
    assert table.column("p123").to_pylist() == [122]
    assert table.column("b123").to_pylist() == [1]
    assert table.column("p_wrap").to_pylist() == [0]
    assert table.column("b_wrap").to_pylist() == [2]
    assert table.column("p0").to_pylist() == [0]
    assert table.column("b0").to_pylist() == [0]
    assert table.column("pneg").to_pylist() == [1]
    assert table.column("bneg").to_pylist() == [0]
    assert table.schema.field("p").type == pa.int64()
    assert table.schema.field("b").type == pa.int64()
    assert table.column("c").to_pylist() == [8]
    assert table.schema.field("c").type == pa.int64()
    assert table.column("pos_null").to_pylist() == [None]
    assert table.column("bucket_null").to_pylist() == [None]
    null_count = _table(
        spark.range(1).select(F.bitmap_count(F.unhex(F.lit(None).cast("string"))).alias("cn"))
    )
    assert null_count.column("cn").to_pylist() == [None]


def test_gt2_docstring_examples_execute(spark: ReparkSession) -> None:
    """R1: happy-path results named in the GT2 docstrings also collect here."""
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.make_date(2020, 1, 2).alias("md"),
            F.make_interval(days=1).cast("string").alias("mi"),
            F.make_dt_interval(1, 0, 0, 0).alias("mdt"),
            F.unix_micros(F.lit("1970-01-01 00:00:00")).alias("um"),
            F.date_diff(F.lit(datetime.date(2020, 1, 3)), F.lit(datetime.date(2020, 1, 1))).alias(
                "dd"
            ),
            F.element_at(F.array(10, 20, 30), 1).alias("ea"),
            F.array_compact(F.array(1, None, 1)).alias("ac"),
            F.str_to_map(F.lit("a:1,b:2")).alias("stm"),
            F.parse_url(F.lit("https://spark.apache.org/x"), "HOST").alias("pu"),
            F.try_parse_url(F.lit("not a url"), "HOST").alias("tpu"),
            F.url_encode(F.lit("a b")).alias("encoded"),
            F.url_decode(F.lit("a+b")).alias("decoded"),
            F.try_url_decode(F.lit("%ZZ")).alias("try_bad"),
            F.bitmap_bit_position(F.lit(1)).alias("bp"),
            F.bitmap_bit_position(F.lit(123)).alias("bp123"),
            F.bitmap_bucket_number(F.lit(1)).alias("bb"),
            F.bitmap_count(F.unhex(F.lit("FF"))).alias("bc"),
            F.map_from_entries(
                F.array(F.struct(F.lit("a").alias("key"), F.lit(1).alias("value")))
            ).alias("mfe"),
        )
    )
    assert table.column("md").to_pylist() == [datetime.date(2020, 1, 2)]
    assert table.column("mi").to_pylist() == ["1 days"]
    assert table.column("mdt").to_pylist() == [datetime.timedelta(days=1)]
    assert table.column("um").to_pylist() == [0]
    assert table.column("dd").to_pylist() == [2]
    assert table.column("ea").to_pylist() == [10]
    assert table.column("ac").to_pylist() == [[1, 1]]
    assert _as_dict(table.column("stm").to_pylist()[0]) == {"a": "1", "b": "2"}
    assert table.column("pu").to_pylist() == ["spark.apache.org"]
    assert table.column("tpu").to_pylist() == [None]
    assert table.column("encoded").to_pylist() == ["a+b"]
    assert table.column("decoded").to_pylist() == ["a b"]
    assert table.column("try_bad").to_pylist() == [None]
    assert table.column("bp").to_pylist() == [0]
    assert table.column("bp123").to_pylist() == [122]
    assert table.column("bb").to_pylist() == [1]
    assert table.column("bc").to_pylist() == [8]
    assert _as_dict(table.column("mfe").to_pylist()[0]) == {"a": 1}
    shuffled = _table(frame.select(F.shuffle(F.array(1, 2, 3)).alias("s")))
    assert sorted(shuffled.column("s").to_pylist()[0]) == [1, 2, 3]
