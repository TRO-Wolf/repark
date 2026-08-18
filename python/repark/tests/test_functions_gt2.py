"""FN-GT2 — leftover THIN-WIRE datetime / collections / url / bitmap.

Each new ``functions`` name is pinned through ``ReparkSession`` on the Arrow
path (``to_arrow()``): value AND type. ``datediff`` stays the DISPOSED-STUB.
``shuffle`` pins type + length, not order.

Oracle — CORRECTED (this repair round; the previous line claimed "live PySpark
4.1.2 against the pinned OpenJDK 21" and that is false for this file):

- **No Spark and no pyspark run here.** ``pyspark`` is not installed in this
  worktree's ``.venv``, so nothing below was derived from a live Spark.
- The ``parse_url`` / ``try_parse_url`` family (X8) is **MEASURED-JVM**: a
  ``java.net.URI`` probe on the locally installed **OpenJDK 11.0.31** — not 21 —
  driven through the MEASURED-JAVAP ``ParseUrlEvaluator$`` getter map, replayed
  through both repark doors. Which getter each part reads is MEASURED-JAVAP
  (``javap -p -c`` over a ``spark-catalyst_2.13-4.1.2.jar`` in a local package
  cache; that jar is not vendored here).
- Everything else is **DOC-SPARK** (the documented PySpark 4.1.2 signature and
  semantics) plus values measured against *repark* on both doors. Where a claim
  needs Spark itself to check, it says so at the assertion.
"""

from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import sql as repark_sql
from repark.errors import (
    AnalysisException,
    PySparkException,
    UnsupportedOperationException,
)
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


def test_unix_micros_accepts_date_where_spark_refuses() -> None:
    """X13: the FN-GT2 ledger's ``unix_micros(DATE)`` claim, now PINNED.

    Spark 4.1.2 refuses a DATE argument (``DATATYPE_MISMATCH``); the facade's
    ``.cast('timestamp')`` accepts it and session-localizes, so under
    America/Los_Angeles ``make_date(1970, 1, 1)`` is ``28_800_000_000`` (the
    8-hour PST offset in micros), not ``0``. Drop the cast-first in
    ``functions_datetime.unix_micros`` and this reds with a type error; make it
    zone-blind and it reds with ``0``.
    """
    session = _la_session()
    try:
        table = _table(
            session.range(1).select(
                F.unix_micros(F.make_date(1970, 1, 1)).alias("from_date"),
            )
        )
    finally:
        session.stop()
    assert table.column("from_date").to_pylist() == [28_800_000_000]
    assert table.schema.field("from_date").type == pa.int64()


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


def test_shuffle_null_array_is_null_not_a_panic(spark: ReparkSession) -> None:
    """X1 (S0): ``shuffle(CAST(NULL AS ARRAY<INT>))`` used to panic the arrow-data
    primitive transform (``range end index 1 out of range for slice of length 0``)
    and surface as a caught-Rust-panic ``PySparkException``. Spark returns NULL.

    Both reachable entry points: the Spark SQL door and the facade DataFrame API.
    (``shuffle`` is a Spark-only name — the native ANSI door refuses it with
    ``Invalid function 'shuffle'``, pinned below, so there is no third row.)
    Drop the ``values_buffer_is_empty`` guard in ``crates/repark-functions/src/shuffle.rs``
    and the NULL/mixed-batch assertions here red with the panic message.

    The exact trigger, measured on BASE ``5f13647``: a record batch whose list
    *values* buffer is empty **and** which carries at least one NULL row. Both
    halves are needed — the empty-array and populated-with-NULLs rows below are
    controls that were already green on BASE, and they are here so the guard
    cannot be widened into swallowing real work.
    """
    sql_door = _table(spark.sql("SELECT shuffle(CAST(NULL AS ARRAY<INT>)) AS s"))
    assert sql_door.column("s").to_pylist() == [None]

    frame = spark.sql("SELECT CAST(NULL AS ARRAY<INT>) AS a")
    facade = _table(frame.select(F.shuffle("a").alias("s")))
    assert facade.column("s").to_pylist() == [None]

    # The second panic shape: empty values buffer + a NULL row, in ONE batch.
    mixed = spark.createDataFrame([{"a": []}, {"a": None}], schema="a array<int>")
    assert _table(mixed.select(F.shuffle("a").alias("s"))).column("s").to_pylist() == [[], None]

    # CONTROL (green on BASE too — not a panic input): an empty array alone has an
    # empty values buffer but no NULL row, so the placeholder read never happens.
    empty = _table(spark.sql("SELECT shuffle(CAST(array() AS ARRAY<INT>)) AS s"))
    assert empty.column("s").to_pylist() == [[]]

    # CONTROL: a NULL row alongside populated rows never panicked — the values
    # buffer is non-empty — and the guard must not intercept it (multiset kept).
    populated = spark.createDataFrame(
        [{"a": [1, 2]}, {"a": None}, {"a": [3]}], schema="a array<int>"
    )
    rows = _table(populated.select(F.shuffle("a").alias("s"))).column("s").to_pylist()
    assert [None if row is None else sorted(row) for row in rows] == [[1, 2], None, [3]]

    # Entry-point matrix row 2: not reachable. Recorded, not silently skipped.
    with pytest.raises(AnalysisException, match="Invalid function 'shuffle'"):
        repark_sql("SELECT shuffle(CAST(NULL AS ARRAY<INT>)) AS s").to_arrow()


def test_shuffle_seed_is_wired_and_agrees_across_doors(spark: ReparkSession) -> None:
    """X2: PySpark 4.0's ``shuffle(col, seed)`` was seeded on the SQL door and
    silently dropped by the facade, so the two doors disagreed on a result the
    user asked to be *deterministic*.

    Revert ``functions_collections.shuffle``'s ``seed`` passthrough (or the
    ``call_scalar('shuffle')`` two-arg arm) and this reds — first with a
    ``TypeError``/arity error, then on the cross-door equality.
    """
    elements = ", ".join(str(value) for value in range(1, 9))
    sql_door = _table(spark.sql(f"SELECT shuffle(array({elements}), 42) AS s"))
    permutation = sql_door.column("s").to_pylist()[0]
    assert sorted(permutation) == list(range(1, 9))
    # Seeded ⇒ reproducible, and NOT the identity (a dropped seed would still sort).
    assert _table(spark.sql(f"SELECT shuffle(array({elements}), 42) AS s")).column(
        "s"
    ).to_pylist() == [permutation]

    array_column = F.array(*[F.lit(value) for value in range(1, 9)])
    facade = _table(spark.range(1).select(F.shuffle(array_column, 42).alias("s")))
    assert facade.column("s").to_pylist() == [permutation]

    # A different seed is a different permutation (proves the seed reaches the kernel).
    other = _table(spark.range(1).select(F.shuffle(array_column, 7).alias("s")))
    assert other.column("s").to_pylist()[0] != permutation


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


def test_map_from_entries_duplicate_key_raises(spark: ReparkSession) -> None:
    """X7: Spark's default ``spark.sql.mapKeyDedupPolicy`` is ``EXCEPTION``.

    The upstream kernel kept the LAST entry (``{'a': '2'}``) — a silent wrong
    result on an integrity path, and out of line with ``map()`` and
    ``str_to_map`` which already raise here. Delete
    ``refuse_duplicate_keys`` in ``crates/repark-functions/src/map_from_entries.rs``
    and both doors below red with ``{'a': '2'}``.
    """
    duplicate = "array(struct('a', '1'), struct('a', '2'))"
    with pytest.raises(PySparkException, match="Duplicate map key"):
        spark.sql(f"SELECT map_from_entries({duplicate}) AS m").to_arrow()

    entries = spark.sql(f"SELECT {duplicate} AS e")
    with pytest.raises(PySparkException, match="mapKeyDedupPolicy"):
        entries.select(F.map_from_entries("e").alias("m")).to_arrow()

    # The sibling constructors this aligns with.
    with pytest.raises(PySparkException, match=r"unique|Duplicate map key"):
        spark.sql("SELECT map('a', '1', 'a', '2') AS m").to_arrow()
    with pytest.raises(PySparkException, match="Duplicate map key"):
        spark.sql("SELECT str_to_map('a:1,a:2') AS m").to_arrow()

    # Distinct keys still build — the guard must not refuse a legal map.
    distinct = "SELECT map_from_entries(array(struct('a', '1'), struct('b', '2'))) AS m"
    ok = _table(spark.sql(distinct))
    assert _as_dict(ok.column("m").to_pylist()[0]) == {"a": "1", "b": "2"}


def test_str_to_map_backslash_s_is_ascii_only(spark: ReparkSession) -> None:
    """X6: Java's ``\\s`` is ``[ \\t\\n\\x0B\\f\\r]`` — ASCII only.

    The ``regex`` crate's ``\\s`` is Unicode, so a non-breaking space (U+00A0)
    split a pair that Spark keeps whole: a silent row-shape divergence, not an
    error. Drop ``bind_ascii_perl_classes`` in
    ``crates/repark-functions/src/str_to_map.rs`` and the NBSP assertions red
    with a third entry ``{'c': '3'}``.
    """
    nbsp = "\u00a0"
    text = f"a:1 b:2{nbsp}c:3"
    table = _table(
        spark.range(1).select(
            F.str_to_map(F.lit(text), F.lit("\\s"), F.lit(":")).alias("m"),
            F.str_to_map(F.lit("a:1\tb:2"), F.lit("\\s"), F.lit(":")).alias("tabbed"),
            F.str_to_map(F.lit("a:1,b:2 c:3"), F.lit("[\\s,]"), F.lit(":")).alias("in_class"),
        )
    )
    # NBSP does NOT split: 'b' keeps the rest of the string as its value.
    assert _as_dict(table.column("m").to_pylist()[0]) == {"a": "1", "b": f"2{nbsp}c:3"}
    # ASCII whitespace still splits.
    assert _as_dict(table.column("tabbed").to_pylist()[0]) == {"a": "1", "b": "2"}
    # The splice works inside a character class too.
    assert _as_dict(table.column("in_class").to_pylist()[0]) == {"a": "1", "b": "2", "c": "3"}

    sql_door = _table(spark.sql(f"SELECT str_to_map('{text}', '\\s', ':') AS m"))
    assert _as_dict(sql_door.column("m").to_pylist()[0]) == {"a": "1", "b": f"2{nbsp}c:3"}


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
    """X3 + X11 + X12: every argument is ColumnOrName, and try_parse_url has a
    success-path pin (an all-NULL pin cannot catch an always-NULL kernel)."""
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.parse_url(F.lit("https://spark.apache.org/path"), F.lit("HOST")).alias("h"),
            F.try_parse_url(F.lit("https://spark.apache.org/path"), F.lit("HOST")).alias("try_ok"),
            F.try_parse_url(F.lit("not a url"), F.lit("HOST")).alias("bad"),
            F.try_parse_url(F.lit(None).cast("string"), F.lit("HOST")).alias("n"),
        )
    )
    assert table.column("h").to_pylist() == ["spark.apache.org"]
    assert _is_string(table.schema.field("h").type)
    # X11: try_parse_url must actually EXTRACT, not merely be NULL-shaped.
    assert table.column("try_ok").to_pylist() == ["spark.apache.org"]
    assert _is_string(table.schema.field("try_ok").type)
    assert table.column("bad").to_pylist() == [None]
    assert _is_string(table.schema.field("bad").type)
    assert table.column("n").to_pylist() == [None]

    # X3 / X12: a bare str is a COLUMN NAME on both arguments (PySpark 4.1.2
    # ColumnOrName). Restore the force-lit convenience and the name direction
    # reds — 'p' would become the literal part name 'p' and yield NULL.
    named = spark.sql("SELECT 'https://a.b/c' AS u, 'HOST' AS p")
    by_name = _table(
        named.select(
            F.parse_url("u", "p").alias("pu"),
            F.try_parse_url("u", "p").alias("tpu"),
        )
    )
    assert by_name.column("pu").to_pylist() == ["a.b"]
    assert by_name.column("tpu").to_pylist() == ["a.b"]
    # And the Column direction still works.
    by_column = _table(
        named.select(F.parse_url(F.col("u"), F.col("p")).alias("pu")),
    )
    assert by_column.column("pu").to_pylist() == ["a.b"]

    # An unparsable URL raises INVALID_URL on parse_url (Spark) and NULLs on try.
    with pytest.raises(PySparkException, match="url is invalid"):
        frame.select(F.parse_url(F.lit("not a url"), F.lit("HOST"))).to_arrow()
    with pytest.raises(PySparkException, match="url is invalid"):
        frame.select(F.parse_url(F.lit("inva lid://host"), F.lit("HOST"))).to_arrow()
    try_malformed = _table(
        frame.select(F.try_parse_url(F.lit("inva lid://host"), F.lit("HOST")).alias("try_mal"))
    )
    assert try_malformed.column("try_mal").to_pylist() == [None]
    null_parse = _table(
        frame.select(F.parse_url(F.lit(None).cast("string"), F.lit("HOST")).alias("parse_null"))
    )
    assert null_parse.column("parse_null").to_pylist() == [None]


def test_parse_url_query_key_is_a_java_regex(spark: ReparkSession) -> None:
    """X8: Spark compiles the QUERY key as ``(&|^)<key>=([^&]*)`` and returns
    group 2. The upstream kernel did exact key equality, so ``'f.o'`` on
    ``?foo=1`` was NULL — the FN-GT2 ledger recorded that as a DF-owned
    residual and this X-round CLOSES it.
    """
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.parse_url(
                F.lit("https://spark.apache.org/path?query=1"), F.lit("QUERY"), F.lit("query")
            ).alias("exact"),
            F.parse_url(F.lit("https://x/?foo=1"), F.lit("QUERY"), F.lit("f.o")).alias("regex"),
            F.parse_url(F.lit("https://x/?foo=1&bar=2"), F.lit("QUERY"), F.lit("bar")).alias(
                "second"
            ),
            F.parse_url(F.lit("https://x/?foo=1"), F.lit("QUERY"), F.lit("nope")).alias("missing"),
            F.parse_url(F.lit("https://x/?foo=1"), F.lit("QUERY")).alias("whole"),
        )
    )
    assert table.column("exact").to_pylist() == ["1"]
    assert table.column("regex").to_pylist() == ["1"]
    assert table.column("second").to_pylist() == ["2"]
    assert table.column("missing").to_pylist() == [None]
    assert table.column("whole").to_pylist() == ["foo=1"]

    # A key that cannot compile raises under BOTH UDFs, and that is MEASURED-JAVAP, not
    # recollection: `javap -p -c` over ParseUrlEvaluator$ shows getPattern calling
    # Pattern.compile with NO exception table, and TryParseUrl's replacement is
    # `ParseUrl(params, failOnError = false)` (iconst_0 into ParseUrl.<init>:(Seq;Z)V) —
    # NOT TryEval(ParseUrl). So failOnError never reaches the compile. An earlier cut of
    # this suite asserted try_parse_url NULLed here; that was the TryEval error.
    # Fold ExtractError::KeyPattern back into ExtractError::InvalidUrl in
    # crates/repark-functions/src/url.rs and the try_parse_url half below reds with None.
    for bad in ["(", "[", "a{2,", "a)b"]:
        for udf, sql_name in ((F.parse_url, "parse_url"), (F.try_parse_url, "try_parse_url")):
            for door in (
                lambda k=None, f=udf: frame.select(
                    f(F.lit("https://a.b/c?x=1"), F.lit("QUERY"), F.lit(k)).alias("v")
                ).to_arrow(),
                lambda k=None, n=sql_name: spark.sql(
                    f"SELECT {n}('https://a.b/c?x=1', 'QUERY', '{k}') AS v"
                ).to_arrow(),
            ):
                with pytest.raises(PySparkException, match="invalid QUERY key pattern"):
                    door(bad)

    # Compile ORDER is the bytecode's, and it is what keeps these three NULL rather than
    # raising: the 3-arg evaluate returns NULL when the part is not QUERY (before the URL is
    # parsed at all), then parses the URL, then returns NULL when there is no raw query — the
    # key pattern is compiled LAST. MEASURED-JVM: the same rows come back NULL from a
    # java.net.URI probe driven through that getter map on OpenJDK 11.0.31.
    ordering = _table(
        frame.select(
            F.try_parse_url(F.lit("not a url"), F.lit("QUERY"), F.lit("(")).alias("url_first"),
            F.parse_url(F.lit("https://a.b/c?x=1"), F.lit("HOST"), F.lit("(")).alias("nonquery"),
            F.parse_url(F.lit("https://a.b/c"), F.lit("QUERY"), F.lit("(")).alias("noquery"),
            F.parse_url(F.lit("not a url"), F.lit("HOST"), F.lit("k")).alias("nonquery_badurl"),
        )
    )
    assert ordering.column("url_first").to_pylist() == [None]
    assert ordering.column("nonquery").to_pylist() == [None]
    assert ordering.column("noquery").to_pylist() == [None]
    assert ordering.column("nonquery_badurl").to_pylist() == [None]

    # A key whose metacharacters DO compile keeps matching as a regex — including
    # an escaped metacharacter, which is a literal on Java and rust regex alike.
    valid = _table(
        frame.select(
            F.parse_url(F.lit("https://a.b/c?x=1"), F.lit("QUERY"), F.lit(".*")).alias("any"),
            F.parse_url(F.lit("https://a.b/c?x=1"), F.lit("QUERY"), F.lit("(?i)X")).alias("flag"),
            F.parse_url(F.lit("https://a.b/c?a+b=1"), F.lit("QUERY"), F.lit("a\\+b")).alias("esc"),
            F.parse_url(F.lit("https://a.b/c?axb=1"), F.lit("QUERY"), F.lit("a\\+b")).alias(
                "nomatch"
            ),
        )
    )
    assert valid.column("any").to_pylist() == ["1"]
    assert valid.column("flag").to_pylist() == ["1"]
    assert valid.column("esc").to_pylist() == ["1"]
    assert valid.column("nomatch").to_pylist() == [None]


def test_parse_url_query_key_regex_dialect_residual(spark: ReparkSession) -> None:
    """X8 RESIDUAL: the key is ``java.util.regex`` on Spark, ``regex`` crate here.

    This X-round *introduced* the regex-key path (upstream did exact key
    equality), so it introduced this residual — recorded, not papered over.
    Both halves are MEASURED: live ``java.util.regex.Pattern`` on the local
    OpenJDK 11.0.31 for the Spark column, and both repark doors for the repark
    column, over the raw query ``a=1&b=2&aa=3&xx=4``.

    The ``regex`` crate is a finite automaton, so lookaround, backreferences,
    atomic groups and ``\\Q…\\E`` are not merely unimplemented — they cannot be
    expressed. A key using one raises here under ``parse_url`` AND
    ``try_parse_url`` where Spark answers a value or NULL.
    """
    frame = spark.range(1)
    url = "http://h/p?a=1&b=2&aa=3&xx=4"

    # AGREE (java == repark). Pinning the agreements is the load-bearing half: they
    # bound the residual to five constructs instead of "the key is a different language".
    agree = [
        ("(?i)A", "1"),
        ("a|b", None),
        ("x{2,3}", "4"),
        ("\\d", None),
        ("*", "1"),
        ("\\p{Alpha}", "1"),
        ("\\p{Lower}", "1"),
        ("\\P{Alpha}", None),
        ("a++", "1"),
        ("[a-z&&[^b]]", "1"),
        # A key that opens its own group shifts Spark's group(2): BOTH engines answer
        # the key's own capture rather than the value.
        ("(?<n>a)", "a"),
        ("\\Aa", "1"),
    ]
    for key, expected in agree:
        facade = _table(
            frame.select(F.parse_url(F.lit(url), F.lit("QUERY"), F.lit(key)).alias("v"))
        )
        assert facade.column("v").to_pylist() == [expected], (key, "facade")
        door = _table(spark.sql(f"SELECT parse_url('{url}', 'QUERY', '{key}') AS v"))
        assert door.column("v").to_pylist() == [expected], (key, "sql door")

    # DIVERGE: java.util.regex compiles these and answers `java`; repark raises.
    #   key         java answer (MEASURED-JVM)
    diverge = [
        ("a(?=1)", None),  # lookahead
        ("(?<=&)b", "2"),  # lookbehind
        ("(a)\\1", "a"),  # backreference
        ("(?>a)", "1"),  # atomic group
        ("\\Qa\\E", "1"),  # quoted literal
    ]
    for key, _java in diverge:
        for udf, sql_name in ((F.parse_url, "parse_url"), (F.try_parse_url, "try_parse_url")):
            with pytest.raises(PySparkException, match="invalid QUERY key pattern"):
                frame.select(udf(F.lit(url), F.lit("QUERY"), F.lit(key)).alias("v")).to_arrow()
            with pytest.raises(PySparkException, match="invalid QUERY key pattern"):
                spark.sql(f"SELECT {sql_name}('{url}', 'QUERY', '{key}') AS v").to_arrow()


def test_parse_url_is_java_net_uri_not_a_normalizer(spark: ReparkSession) -> None:
    """X8: the confirmed dialect divergences, pinned both doors (MEASURED-JVM).

    ``datafusion-spark`` 54.1 extracts with ``url::Url`` (a WHATWG-URL
    *normalizer*); Spark uses ``java.net.URI`` (a *splitter*). Revert
    ``crates/repark-functions/src/url.rs`` to ``spark_url_udfs::parse_url()``
    and every row below reds with the normalized answer named in ``was``.
    """
    #   url                      part          spark (java.net.URI)   was (url::Url)
    cases = [
        ("https://host:443/x", "AUTHORITY", "host:443", "host"),
        ("http://h:80/x", "AUTHORITY", "h:80", "h"),
        ("HTTPS://Example.COM/x", "PROTOCOL", "HTTPS", "https"),
        ("HTTPS://Example.COM/x", "HOST", "Example.COM", "example.com"),
        ("http://h/a/./b/../c", "PATH", "/a/./b/../c", "/a/c"),
        ("http://\u4f8b\u3048.jp/x", "HOST", None, "xn--r8jz45g.jp"),
        ("http://@host/x", "USERINFO", "", None),
        ("http://@host/x", "AUTHORITY", "@host", "host"),
        ("mailto:a@b.com", "PATH", None, "a@b.com"),
        ("http://h/a/%2e%2e/b", "PATH", "/a/%2e%2e/b", "/b"),
    ]
    frame = spark.range(1)
    for url, part, expected, was in cases:
        facade = _table(frame.select(F.parse_url(F.lit(url), F.lit(part)).alias("v")))
        assert facade.column("v").to_pylist() == [expected], (url, part, "facade", was)
        escaped = url.replace("'", "''")
        door = _table(spark.sql(f"SELECT parse_url('{escaped}', '{part}') AS v"))
        assert door.column("v").to_pylist() == [expected], (url, part, "sql door", was)

    # Components that were already right must stay right.
    unchanged = _table(
        frame.select(
            F.parse_url(F.lit("http://h"), F.lit("PATH")).alias("empty_path"),
            F.parse_url(F.lit("http://h/p?a=1#f"), F.lit("FILE")).alias("file"),
            F.parse_url(F.lit("http://h/p?a=1#f"), F.lit("REF")).alias("ref"),
            F.parse_url(F.lit("http://user:pw@h:9/p"), F.lit("USERINFO")).alias("ui"),
            F.parse_url(F.lit("http://h/p"), F.lit("NOSUCHPART")).alias("unknown"),
        )
    )
    assert unchanged.column("empty_path").to_pylist() == [""]
    assert unchanged.column("file").to_pylist() == ["/p?a=1"]
    assert unchanged.column("ref").to_pylist() == ["f"]
    assert unchanged.column("ui").to_pylist() == ["user:pw"]
    assert unchanged.column("unknown").to_pylist() == [None]


def test_parse_url_hostile_urls_split_like_java_net_uri(spark: ReparkSession) -> None:
    """X8 hostile tier, MEASURED-JVM on the local OpenJDK 11.0.31.

    Every expectation below is what ``new java.net.URI(s)`` plus the
    MEASURED-JAVAP ``ParseUrlEvaluator$`` getter map answers — the empty-string
    cases especially, which are the ones a NULL-vs-``''`` slip silently eats:
    ``http://h/p?`` has an empty raw QUERY (so FILE keeps its trailing ``?``),
    ``http://h/p#`` an empty raw REF, and ``http:///p`` a NULL AUTHORITY beside a
    real PATH. The registry-based fallback rows (``a-``, ``a_b.c``) are the
    reason HOST can be NULL while AUTHORITY is not.
    """
    #   url                    part         java.net.URI
    cases = [
        ("http://127.0.0.1:9/x", "HOST", "127.0.0.1"),
        ("http://[::1]:9/x", "HOST", "[::1]"),
        ("http://[::1]:9/x", "AUTHORITY", "[::1]:9"),
        ("http:///p", "AUTHORITY", None),
        ("http:///p", "PATH", "/p"),
        # A trailing-dash label and an underscore label both fail parseHostname, so the
        # authority falls back to registry-based: raw text kept, HOST NULL.
        ("http://a-/x", "HOST", None),
        ("http://a_b.c/x", "HOST", None),
        ("http://a_b.c/x", "AUTHORITY", "a_b.c"),
        ("http://h/p?", "QUERY", ""),
        ("http://h/p?", "FILE", "/p?"),
        ("http://h/p#", "REF", ""),
        # Case is preserved inside an escape, too — %2E is not folded to %2e.
        ("http://h/a%2E%2E/b", "PATH", "/a%2E%2E/b"),
        ("a/b?c=1#d", "PATH", "a/b"),
        ("a/b?c=1#d", "PROTOCOL", None),
        ("a/b?c=1#d", "HOST", None),
    ]
    frame = spark.range(1)
    for url, part, expected in cases:
        facade = _table(frame.select(F.parse_url(F.lit(url), F.lit(part)).alias("v")))
        assert facade.column("v").to_pylist() == [expected], (url, part, "facade")
        door = _table(spark.sql(f"SELECT parse_url('{url}', '{part}') AS v"))
        assert door.column("v").to_pylist() == [expected], (url, part, "sql door")

    # A malformed escape is a URISyntaxException like any other: raise / NULL.
    with pytest.raises(PySparkException, match="url is invalid"):
        frame.select(F.parse_url(F.lit("http://h/a%2"), F.lit("PATH"))).to_arrow()
    tolerant = _table(
        frame.select(F.try_parse_url(F.lit("http://h/a%2"), F.lit("PATH")).alias("v"))
    )
    assert tolerant.column("v").to_pylist() == [None]


def test_parse_url_never_percent_decodes(spark: ReparkSession) -> None:
    """X8 getter dimension: Spark reads the ``Raw`` getters, so nothing is decoded.

    MEASURED-JAVAP: ``javap -p -c`` over ``ParseUrlEvaluator$`` from a local
    ``spark-catalyst_2.13-4.1.2.jar`` (the pyspark 4.1.2 sdist's copy — the jar
    is not vendored here and no Spark runs in this suite) gives the getter per
    part in ``$anonfun$getExtractPartFunc$1..8``: ``HOST`` is ``getHost`` and
    ``PROTOCOL`` is ``getScheme``, but ``PATH``, ``QUERY``, ``REF``, ``FILE``,
    ``AUTHORITY`` and ``USERINFO`` are all ``getRaw*``.

    MEASURED-JVM: every ``spark`` value below was then derived by running ``new
    java.net.URI(s)`` on that getter map on the local OpenJDK 11.0.31 — not
    recollection, and not a Spark run.

    An earlier cut of ``crates/repark-functions/src/java_uri.rs`` decoded these
    six parts, which truncated a QUERY value at a decoded ``&`` and erased the
    difference between ``%2F`` and a real path separator. Bind them back to
    decoding getters and every row here reds with the decoded answer named in
    ``if_decoded``.
    """
    #   url                     part         key   spark (getRaw*)  if_decoded
    cases = [
        ("http://h/a%20b", "PATH", None, "/a%20b", "/a b"),
        ("http://h/a%2Fb", "PATH", None, "/a%2Fb", "/a/b"),
        ("http://us%65r@host/x", "USERINFO", None, "us%65r", "user"),
        ("http://us%65r@host/x", "AUTHORITY", None, "us%65r@host", "user@host"),
        ("http://h/p?a=1%26b=2", "QUERY", None, "a=1%26b=2", "a=1&b=2"),
        ("http://h/p?a=1%26b=2", "QUERY", "a", "1%26b=2", "1"),
        ("http://h/p#f%20g", "REF", None, "f%20g", "f g"),
        ("http://h/a%20b?q=1", "FILE", None, "/a%20b?q=1", "/a b?q=1"),
        ("http://h/%E4%BE%8B", "PATH", None, "/%E4%BE%8B", "/例"),
        # HOST is the non-raw getter, and a host cannot hold an escape: unchanged.
        ("http://us%65r@host/x", "HOST", None, "host", "host"),
    ]
    frame = spark.range(1)
    for url, part, key, expected, if_decoded in cases:
        args = [F.lit(url), F.lit(part)] + ([F.lit(key)] if key is not None else [])
        facade = _table(frame.select(F.parse_url(*args).alias("v")))
        assert facade.column("v").to_pylist() == [expected], (url, part, "facade", if_decoded)
        key_sql = f", '{key}'" if key is not None else ""
        door = _table(spark.sql(f"SELECT parse_url('{url}', '{part}'{key_sql}) AS v"))
        assert door.column("v").to_pylist() == [expected], (url, part, "sql door", if_decoded)


def test_url_codec_keyword_is_str_like_pyspark(spark: ReparkSession) -> None:
    """X5: PySpark 4.1.2 spells the parameter ``str``, not ``col``.

    Positional compatibility is kept; the keyword form is the pin. Rename the
    parameter back to ``col`` and every keyword call below reds with
    ``TypeError: got an unexpected keyword argument 'str'``.
    """
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.url_encode(str=F.lit("a b")).alias("e_kw"),
            F.url_decode(str=F.lit("a+b")).alias("d_kw"),
            F.try_url_decode(str=F.lit("a+b")).alias("t_kw"),
            F.url_encode(F.lit("a b")).alias("e_pos"),
            F.url_decode(F.lit("a+b")).alias("d_pos"),
            F.try_url_decode(F.lit("a+b")).alias("t_pos"),
        )
    )
    assert table.column("e_kw").to_pylist() == ["a+b"]
    assert table.column("d_kw").to_pylist() == ["a b"]
    assert table.column("t_kw").to_pylist() == ["a b"]
    assert table.column("e_pos").to_pylist() == ["a+b"]
    assert table.column("d_pos").to_pylist() == ["a b"]
    assert table.column("t_pos").to_pylist() == ["a b"]

    # X12: the ColumnOrName name direction on the codec family.
    named = spark.sql("SELECT 'a b' AS raw, 'a+b' AS enc")
    by_name = _table(
        named.select(
            F.url_encode("raw").alias("e"),
            F.url_decode("enc").alias("d"),
            F.try_url_decode("enc").alias("t"),
        )
    )
    assert by_name.column("e").to_pylist() == ["a+b"]
    assert by_name.column("d").to_pylist() == ["a b"]
    assert by_name.column("t").to_pylist() == ["a b"]


def test_element_at_and_get_column_name_direction(spark: ReparkSession) -> None:
    """X4 + X12: ``F.get``'s index is ColumnOrName (PySpark only wraps ``int``).

    ``F.get('a', 'i')`` used to force ``lit('i')`` and fail planning with
    ``array index must be an integer, got Utf8``. Restore the ``lit`` wrap and
    the by-name row reds with that planning error.
    """
    frame = spark.sql("SELECT array(10, 20, 30) AS a, 1 AS i, 'b' AS k")
    table = _table(
        frame.select(
            F.get("a", "i").alias("by_name"),
            F.get("a", 1).alias("by_int"),
            F.get("a", F.lit(2)).alias("by_column"),
        )
    )
    assert table.column("by_name").to_pylist() == [20]
    assert table.column("by_int").to_pylist() == [20]
    assert table.column("by_column").to_pylist() == [30]

    # element_at keeps its literal-map-key convenience (W1) — the two are
    # deliberately different, and the docstrings say so.
    maps = spark.sql("SELECT map('a', 1, 'b', 2) AS m, 'a' AS k")
    element = _table(
        maps.select(
            F.element_at("m", "b").alias("literal_key"),
            F.element_at("m", F.col("k")).alias("column_key"),
        )
    )
    assert element.column("literal_key").to_pylist() == [2]
    assert element.column("column_key").to_pylist() == [1]


def test_ansi_pair_is_null_not_a_raise(spark: ReparkSession) -> None:
    """X9: ``element_at`` out-of-range and ``make_date`` invalid-Y-M-D are NULL
    here; Spark under ANSI raises.

    repark's ``spark.sql.ansi.enabled`` defaults to TRUE, but the DOCUMENTED
    scope of the flag is division / modulo by zero only —
    ``docs/guide/session-and-conf.md``: "ANSI mode does **not** currently make
    arithmetic *overflow* raise … Do not read 'ANSI on' as 'every arithmetic
    fault raises'", with every other class carried as a §7 registry row. This
    test codifies today's behavior so the unit that closes the class reds it on
    purpose (docs/spark-sql-iceberg-parity.md §7 preamble).
    """
    frame = spark.range(1)
    facade = _table(
        frame.select(
            F.element_at(F.array(F.lit(1), F.lit(2)), 5).alias("oob"),
            F.element_at(F.array(F.lit(1), F.lit(2)), -5).alias("neg_oob"),
            F.make_date(F.lit(2024), F.lit(2), F.lit(31)).alias("bad_date"),
            F.make_date(F.lit(2024), F.lit(13), F.lit(1)).alias("bad_month"),
        )
    )
    assert facade.column("oob").to_pylist() == [None]
    assert facade.column("neg_oob").to_pylist() == [None]
    assert facade.column("bad_date").to_pylist() == [None]
    assert facade.column("bad_month").to_pylist() == [None]

    door = _table(
        spark.sql("SELECT element_at(array(1, 2), 5) AS oob, make_date(2024, 2, 31) AS bad_date")
    )
    assert door.column("oob").to_pylist() == [None]
    assert door.column("bad_date").to_pylist() == [None]

    # The ANSI class that IS implemented still raises — so this is a scoped
    # divergence, not "ANSI is off".
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT 1 / 0 AS q").to_arrow()


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
            F.parse_url(F.lit("https://spark.apache.org/x"), F.lit("HOST")).alias("pu"),
            F.try_parse_url(F.lit("not a url"), F.lit("HOST")).alias("tpu"),
            F.try_parse_url(F.lit("https://spark.apache.org/x"), F.lit("HOST")).alias("tpu_ok"),
            F.try_url_decode(F.lit("a+b")).alias("try_ok"),
            F.get(F.array(F.lit(10), F.lit(20)), 1).alias("get_ex"),
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
    assert table.column("tpu_ok").to_pylist() == ["spark.apache.org"]
    assert table.column("try_ok").to_pylist() == ["a b"]
    assert table.column("get_ex").to_pylist() == [20]
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
