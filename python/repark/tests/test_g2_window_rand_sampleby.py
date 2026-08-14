"""G2 census r5 — window frames, seeded rand/randn, sampleBy XORShift, eagerEval.

Pins Apache FAIL-MISSING / FAIL-VALUE families owned by TRACK 2. Row-order pins on the
Arrow path (value AND type) per docs/testing.md.
"""

from __future__ import annotations

import pytest

from repark import Window
from repark import functions as F  # noqa: N812 — PySpark-style F alias
from repark.spark.session import ReparkSession


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.master("local[1]").appName("g2-census-r5").getOrCreate()
    yield session
    session.stop()


# ---- window frames ------------------------------------------------------------------------------


def test_rows_between_max_min_count_rank_family(spark: ReparkSession) -> None:
    """Apache ``test_window_functions`` shape (partition + rows frame + ranks)."""
    frame = spark.createDataFrame(
        [(1, "1"), (2, "2"), (1, "2"), (1, "2")],
        ["key", "value"],
    )
    window = Window.partitionBy("value").orderBy("key")
    selected = frame.select(
        frame.value,
        frame.key,
        F.max("key").over(window.rowsBetween(0, 1)),
        F.min("key").over(window.rowsBetween(0, 1)),
        F.count("key").over(window.rowsBetween(float("-inf"), float("inf"))),
        F.row_number().over(window),
        F.rank().over(window),
        F.dense_rank().over(window),
        F.ntile(2).over(window),
    )
    rows = sorted(tuple(row) for row in selected.collect())
    expected = [
        ("1", 1, 1, 1, 1, 1, 1, 1, 1),
        ("2", 1, 1, 1, 3, 1, 1, 1, 1),
        ("2", 1, 2, 1, 3, 2, 1, 1, 1),
        ("2", 2, 2, 2, 3, 3, 3, 2, 2),
    ]
    assert rows == expected


def test_rows_between_without_partition(spark: ReparkSession) -> None:
    """Apache ``test_window_functions_without_partitionBy``."""
    frame = spark.createDataFrame(
        [(1, "1"), (2, "2"), (1, "2"), (1, "2")],
        ["key", "value"],
    )
    window = Window.orderBy("key", frame.value)
    selected = frame.select(
        frame.value,
        frame.key,
        F.max("key").over(window.rowsBetween(0, 1)),
        F.min("key").over(window.rowsBetween(0, 1)),
        F.count("key").over(window.rowsBetween(float("-inf"), float("inf"))),
        F.row_number().over(window),
        F.rank().over(window),
        F.dense_rank().over(window),
        F.ntile(2).over(window),
    )
    rows = sorted(tuple(row) for row in selected.collect())
    expected = [
        ("1", 1, 1, 1, 4, 1, 1, 1, 1),
        ("2", 1, 1, 1, 4, 2, 2, 2, 1),
        ("2", 1, 2, 1, 4, 3, 2, 2, 2),
        ("2", 2, 2, 2, 4, 4, 4, 3, 2),
    ]
    assert rows == expected


def test_cumulative_sum_unbounded_preceding(spark: ReparkSession) -> None:
    """Apache ``test_window_functions_cumulative_sum`` (Window.rowsBetween static)."""
    frame = spark.createDataFrame([("one", 1), ("two", 2)], ["key", "value"])
    selected = frame.select(
        frame.key,
        F.sum(frame.value).over(Window.rowsBetween(Window.unboundedPreceding, 0)),
    )
    rows = sorted(tuple(row) for row in selected.collect())
    assert rows == [("one", 1), ("two", 3)]

    # Boundary clamp past JVM long min/max (Spark overflow guard).
    selected_lo = frame.select(
        frame.key,
        F.sum(frame.value).over(Window.rowsBetween(Window.unboundedPreceding - 1, 0)),
    )
    assert sorted(tuple(row) for row in selected_lo.collect()) == [("one", 1), ("two", 3)]

    frame_end = Window.unboundedFollowing + 1
    selected_hi = frame.select(
        frame.key,
        F.sum(frame.value).over(Window.rowsBetween(Window.currentRow, frame_end)),
    )
    assert sorted(tuple(row) for row in selected_hi.collect()) == [("one", 3), ("two", 2)]


def test_range_between_moving_average(spark: ReparkSession) -> None:
    """Apache ``test_window_functions_moving_average`` shape on a numeric order key.

    Live Spark: ``date.cast(\"timestamp\").cast(\"long\")`` is **epoch seconds**, and since the
    TZ-5 cast fix (2026-08-12, ``task/tz5-cast-seconds-ledger.md``) so is repark's — so this test
    now spells Spark's own expression with **no scale workaround**. It used to divide by 1e6,
    because DataFusion handed back the raw tick value (µs for this ``createDataFrame`` column,
    ns for a ``to_timestamp`` literal) and the ±3-day RANGE offsets are in seconds; that
    docstring called itself a "seed for the cast-unit track", and this is that track landing.

    Dropping the ``/ 1e6`` is therefore part of the fix's revert-red evidence: restore it and
    this test goes red, exactly as re-introducing the raw-tick cast would.

    The column is a NAIVE datetime, which repark reads as UTC and Spark as a session wall clock
    (registry row TZ-7) — immaterial here, because a RANGE frame only reads DIFFERENCES between
    order-key values and a constant offset cancels out of every one of them.
    """
    import datetime

    data = [
        (datetime.datetime(2023, 1, 1), 20),
        (datetime.datetime(2023, 1, 2), 22),
        (datetime.datetime(2023, 1, 3), 21),
        (datetime.datetime(2023, 1, 4), 23),
        (datetime.datetime(2023, 1, 5), 24),
        (datetime.datetime(2023, 1, 6), 26),
    ]
    frame = spark.createDataFrame(data, ["date", "temperature"])

    def to_sec(days: int) -> int:
        return days * 86400

    order_seconds = F.col("date").cast("timestamp").cast("long")
    window = Window.orderBy(order_seconds).rangeBetween(-to_sec(3), 0)
    result = frame.withColumn("avg3", F.avg("temperature").over(window))
    rows = sorted(tuple(row) for row in result.collect())
    expected_avgs = [20.0, 21.0, 21.0, 21.5, 22.5, 23.5]
    for row, expected in zip(rows, expected_avgs, strict=True):
        assert row[2] == pytest.approx(expected)


def test_range_between_integer_order_key(spark: ReparkSession) -> None:
    """RANGE on a pure integer ORDER BY (no timestamp cast unit ambiguity)."""
    frame = spark.createDataFrame([(1, 10), (2, 20), (5, 30), (6, 40)], ["k", "v"])
    window = Window.orderBy("k").rangeBetween(-1, 0)
    rows = sorted(tuple(row) for row in frame.select("k", F.sum("v").over(window)).collect())
    # k=1 → {1}; k=2 → {1,2}; k=5 → {5}; k=6 → {5,6}
    assert rows == [(1, 10), (2, 30), (5, 30), (6, 70)]


def test_range_value_offset_refuses_non_numeric_order(spark: ReparkSession) -> None:
    """Spark ``DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE`` on string ORDER BY.

    Live 4.1.2 oracle: rangeBetween(-1, 0) / (0, 1) on string fails; peer frames
    (unboundedPreceding, currentRow) stay legal.
    """
    from repark.errors import AnalysisException

    frame = spark.createDataFrame([("a", 1), ("b", 2), ("a", 3)], ["s", "v"])
    with pytest.raises(AnalysisException, match="SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE"):
        frame.select(F.sum("v").over(Window.orderBy("s").rangeBetween(-1, 0))).collect()
    with pytest.raises(AnalysisException, match="SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE"):
        frame.withColumn("sm", F.sum("v").over(Window.orderBy("s").rangeBetween(0, 1))).collect()
    # Sticky across .alias (select path must not drop the type check).
    with pytest.raises(AnalysisException, match="SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE"):
        frame.select(F.sum("v").over(Window.orderBy("s").rangeBetween(-1, 0)).alias("sm")).collect()
    # Peer-only RANGE on string remains legal (cumulative peers).
    peer = Window.orderBy("s").rangeBetween(Window.unboundedPreceding, Window.currentRow)
    peer_rows = sorted(tuple(row) for row in frame.select("s", F.sum("v").over(peer)).collect())
    assert peer_rows == [("a", 4), ("a", 4), ("b", 6)]


def test_range_value_offset_accepts_a_cast_numeric_order_key(spark: ReparkSession) -> None:
    """A CAST-to-numeric ORDER BY is a legal value-offset RANGE key — the guard's other side.

    The refusal above resolves the order key by NAME, and a cast chain keeps its base column's
    projection name (``col("s").cast("long")`` still projects as ``s``). Naming it made the guard
    read the SOURCE column's dtype and refuse a numeric key that Spark accepts. That over-reach
    was unreachable until the TZ-5 cast fix (``task/tz5-cast-seconds-ledger.md``) let the
    moving-average pin drop the arithmetic wrapper that had been hiding it, so the fix is pinned
    from both sides here: a bare non-numeric key is still refused (above), and a cast TO a numeric
    type is accepted (below).
    """
    frame = spark.createDataFrame([("1", 10), ("2", 20), ("5", 30), ("6", 40)], ["s", "v"])
    window = Window.orderBy(F.col("s").cast("long")).rangeBetween(-1, 0)
    rows = sorted(tuple(row) for row in frame.select("s", F.sum("v").over(window)).collect())
    # Same frame arithmetic as the integer-key pin: 1 → {1}; 2 → {1,2}; 5 → {5}; 6 → {5,6}.
    assert rows == [("1", 10), ("2", 30), ("5", 30), ("6", 70)]


def test_range_without_order_by_refuses(spark: ReparkSession) -> None:
    """Spark ``DATATYPE_MISMATCH.RANGE_FRAME_WITHOUT_ORDER``."""
    from repark.errors import AnalysisException

    frame = spark.createDataFrame([(1,)], ["v"])
    with pytest.raises(AnalysisException, match="RANGE_FRAME_WITHOUT_ORDER"):
        frame.select(F.sum("v").over(Window.rangeBetween(-1, 0))).collect()


def test_range_value_offset_refuses_multi_order(spark: ReparkSession) -> None:
    """Spark ``DATATYPE_MISMATCH.RANGE_FRAME_MULTI_ORDER`` (value-offset + multi ORDER BY)."""
    from repark.errors import AnalysisException

    frame = spark.createDataFrame([(1, 2, 10), (1, 3, 20)], ["a", "b", "v"])
    with pytest.raises(AnalysisException, match="RANGE_FRAME_MULTI_ORDER"):
        frame.select(F.sum("v").over(Window.orderBy("a", "b").rangeBetween(-1, 0))).collect()
    # Peer-only multi-order RANGE remains legal (no value offset).
    peer = Window.orderBy("a", "b").rangeBetween(Window.unboundedPreceding, Window.currentRow)
    assert frame.select(F.sum("v").over(peer)).count() == 2


def test_rank_family_requires_order_by(spark: ReparkSession) -> None:
    """Spark: ranking window functions require ORDER BY (not DF Internal error)."""
    from repark.errors import AnalysisException

    frame = spark.createDataFrame([(1, "g"), (2, "g")], ["k", "g"])
    part = Window.partitionBy("g")
    with pytest.raises(AnalysisException, match="requires window to be ordered"):
        frame.select(F.rank().over(part)).collect()
    with pytest.raises(AnalysisException, match="requires window to be ordered"):
        frame.select(F.dense_rank().over(part)).collect()
    with pytest.raises(AnalysisException, match="requires window to be ordered"):
        frame.select(F.row_number().over(part)).collect()
    with pytest.raises(AnalysisException, match="requires window to be ordered"):
        frame.select(F.ntile(2).over(part)).collect()


def test_frame_bound_finite_float_refused() -> None:
    """Finite float bounds must not silently int-truncate (Spark has no Double overload)."""
    from repark.errors import PySparkTypeError

    with pytest.raises(PySparkTypeError, match="finite float"):
        Window.rowsBetween(1.5, 2)
    with pytest.raises(PySparkTypeError, match="finite float"):
        Window.rangeBetween(-1.9, 0)
    # ±inf still clamps to JVM long extremes (PySpark threshold path).
    spec = Window.rowsBetween(float("-inf"), float("inf"))
    assert spec._frame_start == Window.unboundedPreceding
    assert spec._frame_end == Window.unboundedFollowing


def test_frame_start_greater_than_end_refused() -> None:
    """Facade refuses inverted frames (octo C3; was DF planning-only)."""
    from repark.errors import AnalysisException

    with pytest.raises(AnalysisException, match="start bound cannot be larger than end"):
        Window.rowsBetween(5, 1)
    with pytest.raises(AnalysisException, match="start bound cannot be larger than end"):
        Window.orderBy("k").rangeBetween(2, -2)


# ---- rand / randn -------------------------------------------------------------------------------


def test_rand_seed_zero_first_value_matches_spark(spark: ReparkSession) -> None:
    """Spark docs: ``SELECT rand(0)`` → 0.7604953758285915 (single row, partition 0)."""
    row = spark.range(1).select(F.rand(0)).collect()[0]
    assert row[0] == pytest.approx(0.7604953758285915, abs=1e-15)


def test_randn_seed_zero_first_value_matches_spark(spark: ReparkSession) -> None:
    """Live Spark 4.1.2: ``SELECT randn(0)`` first value (partition 0)."""
    row = spark.range(1).select(F.randn(0)).collect()[0]
    assert row[0] == pytest.approx(1.6034991609278433, abs=1e-15)


def test_rand_seeded_deterministic_same_layout(spark: ReparkSession) -> None:
    """Same seed ⇒ same values for the same partition layout (Apache seed-0 pin)."""
    frame = spark.createDataFrame([(i, str(i)) for i in range(20)], ["key", "value"])
    first = sorted(tuple(row) for row in frame.select("key", F.rand(0)).collect())
    second = sorted(tuple(row) for row in frame.select("key", F.rand(0)).collect())
    assert first == second
    for row in first:
        assert 0.0 <= row[1] < 1.0
    # Sequence pin (mutation-proof): first five rand(0) values on range(5).
    seq = [row[0] for row in spark.range(5).select(F.rand(0)).collect()]
    assert seq == pytest.approx(
        [
            0.7604953758285915,
            0.5234194256885571,
            0.0953472826424725,
            0.3163249920547614,
            0.7141011170991605,
        ],
        abs=1e-15,
    )


def test_randn_seeded_range_and_determinism(spark: ReparkSession) -> None:
    """Apache ``test_rand_functions`` randn band + seed-0 equality."""
    frame = spark.createDataFrame([(i, str(i)) for i in range(100)], ["key", "value"])
    rows = frame.select("key", F.randn(5)).collect()
    for row in rows:
        assert -4.0 <= row[1] <= 4.0
    a = sorted(tuple(row) for row in frame.select("key", F.randn(0)).collect())
    b = sorted(tuple(row) for row in frame.select("key", F.randn(0)).collect())
    assert a == b


# ---- sampleBy -----------------------------------------------------------------------------------


def test_sampleby_seed_zero_xorshift_keys_match_spark(spark: ReparkSession) -> None:
    """Apache ``test_sampleby`` XORShift: seed=0 → exact key set (live Spark 4.1.2 count 36).

    Mutation-proof: pins full sorted key list against pure Spark XORShift reimpl — not a
    count band any ~half sampler could pass.
    """
    frame = spark.createDataFrame([(i, i % 3) for i in range(100)], ["a", "b"])
    sampled = frame.stat.sampleBy("b", fractions={0: 0.5, 1: 0.5}, seed=0)
    keys = sorted(row.a for row in sampled.collect())
    expected = _xorshift_sampleby_keys()
    assert keys == expected
    assert len(keys) == 36
    assert keys[:10] == [3, 6, 9, 10, 12, 15, 16, 19, 22, 24]


def _xorshift_sampleby_keys() -> list[int]:
    """Pure-Python Spark XORShift sampleBy key list (seed=0, b=i%3, fractions 0/1 → 0.5)."""
    import struct

    def murmur3_x86_32(data: bytes, seed: int) -> int:
        c1, c2 = 0xCC9E2D51, 0x1B873593
        length = len(data)
        h1 = seed & 0xFFFFFFFF
        rounded_end = length & ~3
        index = 0
        while index < rounded_end:
            k1 = (
                data[index]
                | (data[index + 1] << 8)
                | (data[index + 2] << 16)
                | (data[index + 3] << 24)
            ) & 0xFFFFFFFF
            k1 = (k1 * c1) & 0xFFFFFFFF
            k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
            k1 = (k1 * c2) & 0xFFFFFFFF
            h1 ^= k1
            h1 = ((h1 << 13) | (h1 >> 19)) & 0xFFFFFFFF
            h1 = (h1 * 5 + 0xE6546B64) & 0xFFFFFFFF
            index += 4
        k1 = 0
        tail = length & 3
        if tail == 3:
            k1 ^= data[rounded_end + 2] << 16
        if tail >= 2:
            k1 ^= data[rounded_end + 1] << 8
        if tail >= 1:
            k1 ^= data[rounded_end]
            k1 = (k1 * c1) & 0xFFFFFFFF
            k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
            k1 = (k1 * c2) & 0xFFFFFFFF
            h1 ^= k1
        h1 ^= length
        h1 ^= h1 >> 16
        h1 = (h1 * 0x85EBCA6B) & 0xFFFFFFFF
        h1 ^= h1 >> 13
        h1 = (h1 * 0xC2B2AE35) & 0xFFFFFFFF
        h1 ^= h1 >> 16
        return h1

    def hash_seed(seed: int) -> int:
        payload = struct.pack(">q", seed)
        low = murmur3_x86_32(payload, 0x3C074A61)
        high = murmur3_x86_32(payload, low)
        value = ((high << 32) | (low & 0xFFFFFFFF)) & 0xFFFFFFFFFFFFFFFF
        return value - 0x10000000000000000 if value >= 0x8000000000000000 else value

    class XorShift:
        def __init__(self, init: int) -> None:
            self.seed = hash_seed(init)

        def next_bits(self, bits: int) -> int:
            next_seed = self.seed & 0xFFFFFFFFFFFFFFFF
            next_seed ^= (next_seed << 21) & 0xFFFFFFFFFFFFFFFF
            next_seed ^= next_seed >> 35
            next_seed ^= (next_seed << 4) & 0xFFFFFFFFFFFFFFFF
            if next_seed >= 0x8000000000000000:
                self.seed = next_seed - 0x10000000000000000
            else:
                self.seed = next_seed
            return int(next_seed & ((1 << bits) - 1))

        def next_double(self) -> float:
            high = self.next_bits(26)
            low = self.next_bits(27)
            return ((high << 27) + low) / float(1 << 53)

    rng = XorShift(0)
    keep: list[int] = []
    for index in range(100):
        stratum = index % 3
        sample = rng.next_double()
        fraction = 0.5 if stratum in (0, 1) else 0.0
        if sample < fraction:
            keep.append(index)
    return keep


# ---- eagerEval ----------------------------------------------------------------------------------


def test_eager_eval_repr_and_html(spark: ReparkSession) -> None:
    """Apache ``test_repr_behaviors`` shape (Spark showString packing + HTML table).

    Apache's expected strings carry indentation ``||`` artifacts and strip via
    ``re.sub(r'^ *\\|', …)``; we pin the **post-strip** showString form directly.
    """
    frame = spark.createDataFrame([(1, "1"), (22222, "22222")], ("key", "value"))

    spark.conf.set("spark.sql.repl.eagerEval.enabled", "true")
    expected1 = """+-----+-----+
|  key|value|
+-----+-----+
|    1|    1|
|22222|22222|
+-----+-----+"""
    assert frame.__repr__().strip() == expected1

    spark.conf.set("spark.sql.repl.eagerEval.truncate", "3")
    expected2 = """+---+-----+
|key|value|
+---+-----+
|  1|    1|
|222|  222|
+---+-----+"""
    assert frame.__repr__().strip() == expected2

    spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", "1")
    expected3 = """+---+-----+
|key|value|
+---+-----+
|  1|    1|
+---+-----+
only showing top 1 row"""
    assert frame.__repr__().strip() == expected3

    spark.conf.set("spark.sql.repl.eagerEval.truncate", "20")
    spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", "20")
    html = frame._repr_html_()
    assert html is not None
    assert "<table border='1'>" in html
    assert "<th>key</th>" in html
    assert "<td>22222</td>" in html

    # XSS safety (Spark html-escapes cells + headers; live 4.1.2 oracle).
    hostile = spark.createDataFrame(
        [("<script>alert(1)</script>", "a&b")],
        ["<img onerror=x>", "v"],
    )
    hostile_html = hostile._repr_html_()
    assert hostile_html is not None
    assert "<script>" not in hostile_html
    assert "&lt;script&gt;" in hostile_html
    assert "a&amp;b" in hostile_html
    assert "<img onerror=x>" not in hostile_html
    assert "&lt;img onerror=x&gt;" in hostile_html

    spark.conf.set("spark.sql.repl.eagerEval.enabled", "false")
    assert frame.__repr__().startswith("DataFrame[")
    assert frame._repr_html_() is None
