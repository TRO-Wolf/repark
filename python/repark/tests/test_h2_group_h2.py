"""Group H long tail + naming polish.

Covers:
1. Non-origin duplicate projection names (select multi-name map).
2. Same-object self-join sugar (equi cardinality).
3. Wrapped aliased Column display names (``round(v, 2)``).
4. ``spark.app.name`` verify-only pin after bare ``getOrCreate()``.
"""

from __future__ import annotations

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import AnalysisException


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("test-h2-group-h2").getOrCreate()
    yield session
    session.stop()


# 1. Non-origin duplicate projection names


def test_h2_select_cast_duplicate_display_names(spark: ReparkSession) -> None:
    """``select(x, x.cast("double"))`` keeps two display names ``x`` (Spark-legal)."""
    frame = spark.createDataFrame([(1,), (2,)], ["x"])
    out = frame.select(frame.x, frame.x.cast("double"))
    assert out.columns == ["x", "x"]
    rows = out.collect()
    assert [(row[0], row[1]) for row in rows] == [(1, 1.0), (2, 2.0)]
    with pytest.raises(AnalysisException, match=r"AMBIGUOUS_REFERENCE"):
        _ = out["x"]


def test_h2_select_year_year_duplicate_function_names(spark: ReparkSession) -> None:
    """``select(year(d), year(d))`` — non-origin same display, unique engines."""
    frame = spark.createDataFrame([("2024-01-15",)], ["raw"]).withColumn(
        "d", F.col("raw").cast("date")
    )
    out = frame.select(F.year("d"), F.year("d"))
    assert out.columns == ["year(d)", "year(d)"]
    row = out.collect()[0]
    assert row[0] == 2024
    assert row[1] == 2024


def test_h2_select_origin_dups_still_work(spark: ReparkSession) -> None:
    """Origin path regression: ``select(x, x)`` both origin-bound."""
    frame = spark.createDataFrame([(7,)], ["x"])
    bound = frame["x"]
    out = frame.select(bound, bound)
    assert out.columns == ["x", "x"]
    assert out.collect()[0][0] == 7
    assert out.collect()[0][1] == 7


def test_h2_select_sum_sum_multi_name_display(spark: ReparkSession) -> None:
    """``select(sum(v), sum(v))`` keeps Spark-legal display names."""
    frame = spark.createDataFrame([(1, 10), (1, 20), (2, 5)], ["k", "v"])
    out = frame.select(F.sum("v"), F.sum("v"))
    assert out.columns == ["sum(v)", "sum(v)"]
    row = out.collect()[0]
    assert row[0] == 35
    assert row[1] == 35
    # Composed global-agg multi-name path also attaches the overlay.
    composed = frame.select(F.sum("v") + 1, F.sum("v") + 1)
    assert composed.columns == ["(sum(v) + 1)", "(sum(v) + 1)"]
    crow = composed.collect()[0]
    assert crow[0] == 36
    assert crow[1] == 36


# 2. Same-object self-join sugar


def test_h2_same_object_self_join_equi_count(spark: ReparkSession) -> None:
    """``df.join(df, df.x == df.x)`` is equi self-join (count=n), not cartesian n²."""
    frame = spark.createDataFrame([(1,), (2,), (3,)], ["x"])
    joined = frame.join(frame, frame.x == frame.x)
    assert joined.columns == ["x", "x"]
    assert joined.count() == 3
    pairs = sorted((row[0], row[1]) for row in joined.collect())
    assert pairs == [(1, 1), (2, 2), (3, 3)]


def test_h2_same_object_self_join_cross_fields(spark: ReparkSession) -> None:
    """``df.join(df, df.a == df.b)`` refuses 1182 (``P_h2_cross_fields``)."""
    frame = spark.createDataFrame([(1, 2), (2, 1), (3, 3)], ["a", "b"])
    with pytest.raises(AnalysisException) as refused:
        frame.join(frame, frame.a == frame.b).count()
    assert refused.value.getCondition() == "_LEGACY_ERROR_TEMP_1182"
    params = refused.value.getMessageParameters()
    assert params["config"] == "spark.sql.analyzer.failAmbiguousSelfJoin"
    assert params["ambiguousAttrs"] == "a, b"


def test_h2_alias_self_join_still_works(spark: ReparkSession) -> None:
    """``df.alias("l").join(df.alias("r"), …)`` remains the full-identity workaround."""
    frame = spark.createDataFrame([(1, "L"), (2, "R")], ["id", "label"])
    left = frame.alias("l")
    right = frame.alias("r")
    joined = left.join(right, left.id == right.id)
    assert joined.count() == 2
    assert joined.columns == ["id", "label", "id", "label"]


def test_h2_name_equi_same_object_unchanged(spark: ReparkSession) -> None:
    """Name equi-join ``df.join(df, on="a")`` already correct (G1)."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    joined = frame.join(frame, on="a")
    assert joined.count() == 2


def test_h2_same_object_compound_self_join_refuses_loud(spark: ReparkSession) -> None:
    """Compound self-join arms refuse 1182 (``P_h2_compound_same``).

    The aliased half refuses in Spark too (``P_h2_compound_alias``); free-name
    strings answer 3 on both engines.
    """
    frame = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["x", "y"])
    with pytest.raises(AnalysisException) as refused:
        _ = frame.join(frame, (frame.x + frame.y) == (frame.x + frame.y)).count()
    assert refused.value.getCondition() == "_LEGACY_ERROR_TEMP_1182"
    params = refused.value.getMessageParameters()
    assert params["config"] == "spark.sql.analyzer.failAmbiguousSelfJoin"
    assert params["ambiguousAttrs"] == "x, y, x, y"
    left = frame.alias("l")
    right = frame.alias("r")
    with pytest.raises(AnalysisException) as refused_alias:
        _ = left.join(right, (left.x + left.y) == (right.x + right.y)).count()
    assert refused_alias.value.getCondition() == "_LEGACY_ERROR_TEMP_1182"


@pytest.mark.xfail(
    strict=True,
    reason="R-18: qualified free names do not resolve in join conditions; "
    "Spark 4.1.2 answers 3 under both case rules",
)
def test_h2_compound_alias_free_names_answer(spark: ReparkSession) -> None:
    """Free-name compound alias join answers 3 (Spark-measured, R-18 gap)."""
    frame = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["x", "y"])
    left = frame.alias("l")
    right = frame.alias("r")
    free = left.join(right, (F.col("l.x") + F.col("l.y")) == (F.col("r.x") + F.col("r.y")))
    assert free.count() == 3


def test_h2_same_object_and_or_simple_leaves_still_equi(spark: ReparkSession) -> None:
    """AND/OR of simple leaf comparisons stays on the alternation sugar path."""
    frame = spark.createDataFrame([(1, 10), (2, 20)], ["x", "y"])
    joined = frame.join(frame, (frame.x == frame.x) | (frame.y == frame.y))
    assert joined.count() == 2


# 3. Column.round / wrapped aliased display names


def test_h2_round_alias_display_collapses_to_name(spark: ReparkSession) -> None:
    """``.alias("v").round(2)`` displays ``round(v, 2)`` not ``round((…) AS v, 2)``."""
    frame = spark.createDataFrame([(1, 1.234), (2, 2.5)], ["id", "v"])
    rounded = (frame.id * 1.234).alias("v").round(2)
    assert rounded.spark_display_part() == "round(v, 2)"
    out = frame.select(rounded)
    assert out.columns == ["round(v, 2)"]
    values = [row[0] for row in out.collect()]
    assert values == [pytest.approx(1.23), pytest.approx(2.47)]


def test_h2_f_round_alias_display(spark: ReparkSession) -> None:
    """``F.round(expr.alias("v"), 2)`` uses the same wrap-display builder."""
    frame = spark.createDataFrame([(1,)], ["id"])
    col = F.round((frame.id * 1.234).alias("v"), 2)
    assert col.spark_display_part() == "round(v, 2)"
    assert frame.select(col).columns == ["round(v, 2)"]


def test_h2_abs_and_binary_wrap_alias_display(spark: ReparkSession) -> None:
    """General wrap builder: abs / arithmetic also collapse ``… AS v``."""
    frame = spark.createDataFrame([(1,)], ["id"])
    aliased = (frame.id * 1.234).alias("v")
    assert F.abs(aliased).spark_display_part() == "abs(v)"
    assert (aliased + 1).spark_display_part() == "(v + 1)"
    assert aliased.cast("double").spark_display_part() == "CAST(v AS DOUBLE)"
    assert aliased.alias("w").spark_display_part() == "v AS w"


def test_h2_aggregate_alias_argument_keeps_as(spark: ReparkSession) -> None:
    """Aggregate argument embedding still uses full ``x AS y`` (not wrap collapse)."""
    frame = spark.createDataFrame([(1,)], ["x"])
    assert frame.agg(F.sum(frame.x.alias("y"))).columns == ["sum(x AS y)"]


# 4. spark.app.name verify-only


def test_h2_spark_app_name_default_repark_after_bare_get_or_create() -> None:
    """Bare ``getOrCreate()`` surfaces ``spark.app.name`` == ``repark`` via ``conf.get``."""
    # Clear any active session so bare getOrCreate is not a reuse of appName("…").
    prior = ReparkSession.getActiveSession()
    if prior is not None:
        prior.stop()
    session = ReparkSession.builder.getOrCreate()
    try:
        assert session.conf.get("spark.app.name") == "repark"
        # getAll includes the default key.
        assert session.conf.getAll.get("spark.app.name") == "repark"
    finally:
        session.stop()
