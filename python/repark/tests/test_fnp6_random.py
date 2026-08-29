"""FNP-6b — ``randstr`` and ``uniform`` over the Spark ``XORShiftRandom`` repark already owns.

``random.rs`` implements Spark's `XORShiftRandom` bit-exactly (already pinned for `rand` /
`randn`); these kernels draw from that same stream. DOC-SPARK, not MEASURED-SPARK: the
per-function derivation (pool order, one draw per character, `nextDouble()` scaling) is not
measured against live Spark. These rows pin the properties Spark's documentation states — length,
character pool, range, the integer-vs-double return rule, determinism per seed, loud refusal of
non-constant bounds — and deliberately do NOT assert generated values as an oracle. A value pin
here would look like parity evidence while being repark agreeing with itself. Ledger:
``task/fnp-6b-random-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.errors import PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

POOL = set("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp6-random").getOrCreate()


def test_randstr_length_and_character_pool() -> None:
    frame = _session().range(6)
    got = frame.select(F.randstr(8, 42).alias("s")).toArrow().column("s").to_pylist()

    assert len(got) == 6
    assert all(len(value) == 8 for value in got), got
    assert all(set(value) <= POOL for value in got), got


def test_randstr_is_deterministic_for_a_seed() -> None:
    """Same seed, same output — the property that makes a seeded PRNG useful at all."""
    frame = _session().range(4)
    first = frame.select(F.randstr(6, 7).alias("s")).toArrow().column("s").to_pylist()
    second = frame.select(F.randstr(6, 7).alias("s")).toArrow().column("s").to_pylist()
    assert first == second


def test_randstr_differs_across_seeds() -> None:
    frame = _session().range(4)
    a = frame.select(F.randstr(12, 1).alias("s")).toArrow().column("s").to_pylist()
    b = frame.select(F.randstr(12, 2).alias("s")).toArrow().column("s").to_pylist()
    assert a != b, "two seeds produced identical strings — the seed is not reaching the stream"


def test_uniform_return_type_follows_its_bounds() -> None:
    """Spark's documented rule: two integer bounds give an integer, anything else a double.

    A wrong rule is a silent type change, not an error — so it is pinned on the type, not only
    on the values.
    """
    frame = _session().range(4)
    table = frame.select(
        F.uniform(0, 10, 42).alias("both_int"),
        F.uniform(0.0, 1.0, 42).alias("both_float"),
        F.uniform(0, 1.0, 42).alias("mixed"),
    ).toArrow()

    assert str(table.schema.field("both_int").type) == "int64"
    assert str(table.schema.field("both_float").type) == "double"
    assert str(table.schema.field("mixed").type) == "double", "one float bound makes it a double"


def test_uniform_stays_within_its_range() -> None:
    frame = _session().range(20)
    table = frame.select(
        F.uniform(5, 9, 3).alias("i"),
        F.uniform(-1.5, 2.5, 3).alias("f"),
    ).toArrow()

    assert all(5 <= value < 9 for value in table.column("i").to_pylist())
    assert all(-1.5 <= value < 2.5 for value in table.column("f").to_pylist())


def test_uniform_integer_and_float_share_one_draw_sequence() -> None:
    """Internal consistency: both forms scale the SAME XORShift draws, so they must agree.

    Not a Spark oracle — a check that the integer path is the float path floored, rather than a
    second generator that happens to look plausible.
    """
    frame = _session().range(6)
    table = frame.select(
        F.uniform(0, 10, 42).alias("i"),
        F.uniform(0.0, 1.0, 42).alias("f"),
    ).toArrow()

    scaled = [int(value * 10) for value in table.column("f").to_pylist()]
    assert table.column("i").to_pylist() == scaled


@pytest.mark.parametrize(
    ("what", "build"),
    [
        ("randstr length", lambda: F.randstr(F.col("id"))),
        ("uniform min", lambda: F.uniform(F.col("id"), 10)),
    ],
    ids=["randstr", "uniform"],
)
def test_non_constant_bounds_are_refused_loudly(what: str, build) -> None:
    """Spark requires literals here. A column argument must raise, never read row zero quietly."""
    frame = _session().range(4)
    with pytest.raises(PySparkException, match="constant"):
        frame.select(build().alias("r")).toArrow()


def test_uniform_refuses_an_inverted_range() -> None:
    frame = _session().range(4)
    with pytest.raises(PySparkException, match="must not exceed"):
        frame.select(F.uniform(10, 0).alias("r")).toArrow()


def test_uniform_refuses_a_nan_bound_distinctly_from_an_inverted_one() -> None:
    """A NaN bound is INCOMPARABLE, not merely out of order, and gets its own message.

    A negated ``!(low <= high)`` would silently fold "NaN" into "min exceeds max"; the two are
    different mistakes and must say so.
    """
    frame = _session().range(4)
    with pytest.raises(PySparkException, match="NaN"):
        frame.select(F.uniform(float("nan"), 1.0).alias("r")).toArrow()
