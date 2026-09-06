"""FNP-5 — aggregates DataFusion already had registered and the facade could not reach.

The kernels are in ``all_default_aggregate_functions()``, so ``spark.sql(...)`` resolved them
while the facade's aggregate dispatch had no arm. The nine ``regr_*`` are pinned against an
**exact** fit (``y = 2x + 1``), so every statistic has a closed-form answer. Ledger:
``task/fnp-5-aggregates-ledger.md``.
"""

from __future__ import annotations

import pytest

from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp5-aggregates").getOrCreate()


def _exact_fit():
    """y = 2x + 1 over x = 1..4, so every regression statistic is known in closed form."""
    return _session().createDataFrame(
        [(3.0, 1.0), (5.0, 2.0), (7.0, 3.0), (9.0, 4.0)], "y double, x double"
    )


# (function, expected) for the exact fit above.
REGRESSION_ROWS = [
    ("regr_slope", 2.0),  # dy/dx
    ("regr_intercept", 1.0),  # y at x = 0
    ("regr_r2", 1.0),  # a perfect fit explains all variance
    ("regr_count", 4),  # non-null pairs
    ("regr_avgx", 2.5),  # mean of 1,2,3,4
    ("regr_avgy", 6.0),  # mean of 3,5,7,9
    ("regr_sxx", 5.0),  # sum of squared deviations: 2.25 + .25 + .25 + 2.25
    ("regr_syy", 20.0),  # slope squared, times sxx
    ("regr_sxy", 10.0),  # slope times sxx
]


@pytest.mark.parametrize(
    ("name", "expected"), REGRESSION_ROWS, ids=[row[0] for row in REGRESSION_ROWS]
)
def test_regression_aggregate_matches_the_closed_form(name: str, expected: float) -> None:
    frame = _exact_fit()
    got = frame.select(getattr(F, name)("y", "x").alias("r")).toArrow().column("r").to_pylist()
    assert got == [expected], f"{name} on y = 2x + 1"


# Names whose two doors reach the same kernel but hand back different TYPES, with the reason.
# RATCHETS DOWN ONLY. The facade casts a count-like aggregate to signed bigint (Spark has no
# unsigned integer type); the SQL door still returns the engine's `UInt64`. Fixing the door turns
# this test red — the row leaves, it does not get widened.
DOOR_RETURNS_UNSIGNED: set[str] = set()


def test_regression_aggregates_agree_with_the_sql_door() -> None:
    """C-012 at the aggregate layer: the facade must reach the kernel the SQL door reaches.

    Kernel identity is what the clause requires, and values are how it is checked here. Type
    equality is checked too, and the one name that is deliberately allowed to differ has to say so
    in ``DOOR_RETURNS_UNSIGNED`` above.
    """
    frame = _exact_fit()
    frame.createOrReplaceTempView("fnp5_fit")
    spark = _session()

    for name, _ in REGRESSION_ROWS:
        facade = frame.select(getattr(F, name)("y", "x").alias("r")).toArrow()
        door = spark.sql(f"SELECT {name}(y, x) AS r FROM fnp5_fit").toArrow()
        assert facade.column("r").to_pylist() == door.column("r").to_pylist(), name
        if name in DOOR_RETURNS_UNSIGNED:
            assert str(facade.schema.field("r").type) == "int64", name
            assert str(door.schema.field("r").type) == "uint64", (
                f"{name}: the SQL door no longer returns unsigned — drop it from "
                "DOOR_RETURNS_UNSIGNED, the table ratchets down"
            )
            continue
        assert facade.schema.field("r").type == door.schema.field("r").type, name


def test_regression_projection_name_is_spark_shaped() -> None:
    assert _exact_fit().select(F.regr_slope("y", "x")).columns == ["regr_slope(y, x)"]


def test_string_agg_and_listagg_are_the_same_aggregate() -> None:
    """PySpark exports both spellings; Spark's ``listagg`` is ``string_agg`` with a delimiter."""
    frame = _session().createDataFrame([("a",), ("b",), ("a",)], "k string")

    out = frame.select(
        F.listagg("k", ",").alias("lag"),
        F.string_agg("k", ",").alias("sag"),
    ).toArrow()
    assert out.column("lag").to_pylist() == ["a,b,a"]
    assert out.column("lag").to_pylist() == out.column("sag").to_pylist()


def test_grouping_marks_the_aggregated_level_of_a_cube() -> None:
    """Outside a grouping set every row is ungrouped, so ``grouping`` is only meaningful here."""
    frame = _session().createDataFrame([("a", 1), ("b", 2), ("a", 3)], "k string, v int")

    marks = frame.cube("k").agg(F.grouping("k").alias("g")).toArrow().column("g").to_pylist()
    assert sorted(marks) == [0, 0, 1]


def test_approx_count_distinct_counts_and_accepts_the_ignored_rsd() -> None:
    """Spark's estimator is HLL++ and DataFusion's is HLL, so ``rsd`` has nothing to tune.

    Accepted and ignored, like ``percentile_approx``'s accuracy argument. Pinned as a signature
    contract, not as a promise that the estimate matches Spark's bit for bit.
    """
    frame = _session().createDataFrame([("a",), ("b",), ("a",)], "k string")

    assert frame.select(F.approx_count_distinct("k").alias("r")).toArrow().column(
        "r"
    ).to_pylist() == [2]
    assert frame.select(F.approx_count_distinct("k", 0.01).alias("r")).toArrow().column(
        "r"
    ).to_pylist() == [2]
