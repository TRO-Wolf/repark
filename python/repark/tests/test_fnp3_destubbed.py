"""FNP-3 — the stubs whose kernel the engine already had.

**The class.** Eleven names raised ``UnsupportedOperationException`` from the facade while
``spark.sql("SELECT <name>(...)")`` evaluated them correctly, because ``register_all`` installs
the ``datafusion-spark`` kernel by name and the facade's dispatch table simply had no arm for it.
The capability was present; only one of the two doors could reach it. One stub said so in its own
docstring — ``map_from_arrays`` read *"Unsupported as Column builder (SQL map_from_arrays may
work)"* — so the asymmetry was observed and disclosed rather than closed.

That is the same defect class as FNP-1's ``to_timestamp``/``avg``, pointing the other way: FNP-1
had both doors reachable but resolving different kernels, this had one door refusing a kernel the
other door used.

Every row below is pinned on the Arrow path, value AND type, and cross-checked against the SQL
door so the two cannot drift apart again.

Ledger: ``task/fnp-3-destub-ledger.md``.
"""

from __future__ import annotations

import datetime as dt

import pytest

from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp3-destubbed").getOrCreate()


# (name, facade recipe over a frame with columns s/e/t/k/v, SQL-door projection, expected value)
ROWS = [
    ("sha1", lambda: F.sha1("s"), "sha1(s)", "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"),
    ("sha", lambda: F.sha("s"), "sha(s)", "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"),
    ("crc32", lambda: F.crc32("s"), "crc32(s)", 907060870),
    ("soundex", lambda: F.soundex("s"), "soundex(s)", "H400"),
    (
        "format_string",
        lambda: F.format_string("%s-%s", "s", "s"),
        "format_string('%s-%s', s, s)",
        "hello-hello",
    ),
    (
        "printf",
        lambda: F.printf("%s-%s", "s", "s"),
        "format_string('%s-%s', s, s)",
        "hello-hello",
    ),
]


@pytest.mark.parametrize(("name", "recipe", "sql", "expected"), ROWS, ids=[r[0] for r in ROWS])
def test_destubbed_name_agrees_with_the_sql_door(name, recipe, sql, expected) -> None:
    spark = _session()
    frame = spark.createDataFrame([("hello",)], "s string")
    frame.createOrReplaceTempView("fnp3_v")

    facade = frame.select(recipe().alias("r")).toArrow()
    door = spark.sql(f"SELECT {sql} AS r FROM fnp3_v").toArrow()

    assert facade.column("r").to_pylist() == [expected], f"{name}: wrong value on the facade"
    assert facade.column("r").to_pylist() == door.column("r").to_pylist(), f"{name}: doors disagree"
    assert facade.schema.field("r").type == door.schema.field("r").type, f"{name}: types disagree"


def test_crc32_matches_the_reference_implementation() -> None:
    """CRC-32 is a fixed algorithm — check against zlib, not against ourselves."""
    import zlib

    spark = _session()
    frame = spark.createDataFrame([("hello",), ("repark",)], "s string")
    got = frame.select(F.crc32("s").alias("r")).toArrow().column("r").to_pylist()
    assert got == [zlib.crc32(b"hello"), zlib.crc32(b"repark")]


def test_sha1_matches_the_reference_implementation() -> None:
    """SHA-1 likewise — hashlib is the oracle, and `sha` must be the same function as `sha1`."""
    import hashlib

    spark = _session()
    frame = spark.createDataFrame([("hello",), ("repark",)], "s string")
    out = frame.select(F.sha1("s").alias("a"), F.sha("s").alias("b")).toArrow()
    expected = [hashlib.sha1(w).hexdigest() for w in (b"hello", b"repark")]
    assert out.column("a").to_pylist() == expected
    assert out.column("b").to_pylist() == expected


def test_datediff_is_the_older_spelling_of_date_diff() -> None:
    """PySpark 4.1.2 declares both with the same ``(end, start)`` order over one Catalyst expr.

    A prior unit left a "do not alias" note on ``datediff``; reading its ledger shows that was a
    SCOPE fence (FN-D could not touch the test asserting the refusal), not a semantic objection.
    """
    spark = _session()
    frame = spark.createDataFrame([(dt.date(2026, 3, 1), dt.date(2026, 1, 1))], "e date, s date")

    out = frame.select(F.datediff("e", "s").alias("a"), F.date_diff("e", "s").alias("b")).toArrow()
    assert out.column("a").to_pylist() == [59]
    assert out.column("a").to_pylist() == out.column("b").to_pylist()
    assert out.schema.field("a").type == out.schema.field("b").type


def test_utc_timestamp_pair_round_trips() -> None:
    """``to_utc_timestamp`` and ``from_utc_timestamp`` are inverses for a fixed-offset zone."""
    spark = _session()
    frame = spark.createDataFrame([(dt.datetime(2026, 3, 1, 0, 0),)], "t timestamp")

    out = frame.select(
        F.to_utc_timestamp("t", "Asia/Tokyo").alias("to_utc"),
        F.from_utc_timestamp("t", "Asia/Tokyo").alias("from_utc"),
    ).toArrow()

    # Tokyo is UTC+9 year round: reading 00:00 as Tokyo wall clock is 15:00 the previous day UTC.
    assert out.column("to_utc").to_pylist()[0].hour == 15
    assert out.column("to_utc").to_pylist()[0].day == 28
    assert out.column("from_utc").to_pylist()[0].hour == 9

    round_trip = frame.select(
        F.from_utc_timestamp(F.to_utc_timestamp("t", "Asia/Tokyo"), "Asia/Tokyo").alias("r")
    ).toArrow()
    assert round_trip.column("r").to_pylist() == [dt.datetime(2026, 3, 1, 0, 0, tzinfo=dt.UTC)]


def test_map_from_arrays_builds_a_map_column() -> None:
    """The stub's docstring said the SQL door "may work" — it did, and only the facade refused."""
    spark = _session()
    frame = spark.sql("SELECT array('a', 'b') AS k, array(1, 2) AS v")

    out = frame.select(F.map_from_arrays("k", "v").alias("m")).toArrow()
    assert out.column("m").to_pylist() == [[("a", 1), ("b", 2)]]
    assert str(out.schema.field("m").type).startswith("map<")


def test_xxhash64_is_stable_and_typed() -> None:
    """No independent oracle here — pin determinism, distinctness and the bigint return type."""
    spark = _session()
    frame = spark.createDataFrame([("hello",), ("hello",), ("repark",)], "s string")

    out = frame.select(F.xxhash64("s").alias("r")).toArrow()
    values = out.column("r").to_pylist()
    assert values[0] == values[1], "the same input must hash the same"
    assert values[0] != values[2]
    assert str(out.schema.field("r").type) == "int64"
