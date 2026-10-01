from __future__ import annotations

from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-attr-id-1-s3b").getOrCreate()


@pytest.fixture(params=[False, True], ids=["insensitive", "sensitive"])
def ruled_spark(spark: ReparkSession, request: Any) -> ReparkSession:
    spark.conf.set("spark.sql.caseSensitive", "true" if request.param else "false")
    return spark


def _frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 30), (2, 10), (3, 20)], "id BIGINT, v BIGINT")


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return [tuple(row) for row in frame.collect()]


def test_filter_str_of_twin_pair_binds_the_one_attribute(ruled_spark: ReparkSession) -> None:
    twins = _frame(ruled_spark).select("v", "v")
    filtered = twins.filter("v > 15")
    assert filtered.columns == ["v", "v"]
    assert _rows(filtered) == [(30, 30), (20, 20)]


def test_filter_str_of_two_attributes_sharing_a_name_is_ambiguous(
    ruled_spark: ReparkSession,
) -> None:
    frame = _frame(ruled_spark)
    aliased = frame.select(frame.v, frame.v.alias("v"))
    with pytest.raises(AnalysisException) as caught:
        aliased.filter("v > 15")
    assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_filter_str_with_qualifier_binds_one_join_side(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _frame(spark)
    twins = frame.select(frame.v, frame.v)
    joined = twins.alias("L").join(twins.alias("R"), functions.lit(True))
    filtered = joined.filter("L.v > 15")
    assert filtered.columns == ["v", "v", "v", "v"]
    assert sorted(_rows(filtered)) == [
        (20, 20, 10, 10),
        (20, 20, 20, 20),
        (20, 20, 30, 30),
        (30, 30, 10, 10),
        (30, 30, 20, 20),
        (30, 30, 30, 30),
    ]


def test_filter_column_of_twin_pair_binds_the_one_attribute(ruled_spark: ReparkSession) -> None:
    twins = _frame(ruled_spark).select("v", "v")
    filtered = twins.filter(functions.col("v") > 15)
    assert filtered.columns == ["v", "v"]
    assert _rows(filtered) == [(30, 30), (20, 20)]


def test_filter_column_of_twin_join_is_ambiguous(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    twins = frame.select(frame.v, frame.v)
    joined = twins.alias("L").join(twins.alias("R"), functions.lit(True))
    with pytest.raises(AnalysisException) as caught:
        joined.filter(functions.col("v") > 10)
    assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_filter_str_with_exact_and_folded_rival_follows_the_live_rule(
    ruled_spark: ReparkSession,
) -> None:
    frame = _frame(ruled_spark).select("id", "v", functions.col("v").alias("V"))
    if ruled_spark.conf.get("spark.sql.caseSensitive") == "true":
        filtered = frame.filter("v > 15")
        assert filtered.columns == ["id", "v", "V"]
        assert _rows(filtered) == [(1, 30, 30), (3, 20, 20)]
    else:
        with pytest.raises(AnalysisException) as caught:
            frame.filter("v > 15")
        assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_orderby_str_and_parent_column_on_twin_frame(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    twins = frame.select(frame.v, frame.v)
    assert _rows(twins.orderBy("v")) == [(10, 10), (20, 20), (30, 30)]
    assert _rows(twins.orderBy(frame.v)) == [(10, 10), (20, 20), (30, 30)]


def test_orderby_binds_oldest_on_project_dup(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    mixed = frame.select(frame.id.alias("v"), frame.v)
    assert _rows(mixed.orderBy("v")) == [(2, 10), (3, 20), (1, 30)]
    assert _rows(mixed.orderBy(functions.desc("v"))) == [(1, 30), (3, 20), (2, 10)]


def test_orderby_of_join_dup_is_unresolved(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    twins = frame.select(frame.v, frame.v)
    joined = twins.alias("L").join(twins.alias("R"), functions.lit(True))
    with pytest.raises(AnalysisException) as caught:
        joined.orderBy("v")
    assert caught.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_orderby_aggregate_key_matches_aggregate_output(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    aggregated = frame.groupBy("id").agg(functions.max("v"))
    ordered = aggregated.orderBy(functions.max("v"))
    assert ordered.columns == ["id", "max(v)"]
    assert _rows(ordered) == [(2, 10), (3, 20), (1, 30)]


def test_orderby_missing_key_skips_to_join_grandchild(spark: ReparkSession) -> None:
    left = spark.createDataFrame([(1, 30), (2, 10)], "id BIGINT, v BIGINT")
    right = spark.createDataFrame([(1, 100), (2, 200)], "id BIGINT, V BIGINT")
    spark.conf.set("spark.sql.caseSensitive", "true")
    narrowed = left.join(right, "id").select("id")
    assert _rows(narrowed.orderBy("v")) == [(2,), (1,)]
    spark.conf.set("spark.sql.caseSensitive", "false")
    narrowed = left.join(right, "id").select("id")
    with pytest.raises(AnalysisException) as caught:
        narrowed.orderBy("v")
    assert caught.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_filter_uses_live_rule_after_rule_change(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = _frame(spark)
    aliased = frame.select(frame.v, frame.v.alias("v"))
    spark.conf.set("spark.sql.caseSensitive", "false")
    with pytest.raises(AnalysisException) as caught:
        aliased.filter("V > 15")
    assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"


def _cross_join(ruled_spark: ReparkSession) -> Any:
    frame = _frame(ruled_spark)
    return frame.alias("a").crossJoin(frame.alias("b"))


def test_filter_str_of_aliased_join_dup_is_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as caught:
        _cross_join(ruled_spark).alias("q").filter("v > 10")
    assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_filter_column_of_aliased_join_dup_is_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as caught:
        _cross_join(ruled_spark).alias("q").filter(functions.col("v") > 10)
    assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"


def _corpus_cross_join(ruled_spark: ReparkSession) -> Any:
    frame = _frame(ruled_spark)
    left = frame.select("id", "v")
    right = frame.select(functions.col("v")).filter(functions.col("v") > 15)
    return left.crossJoin(right)


def test_filter_str_of_cross_join_dup_is_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as caught:
        _corpus_cross_join(ruled_spark).filter("v > 10")
    assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_filter_column_of_cross_join_dup_is_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as caught:
        _corpus_cross_join(ruled_spark).filter(functions.col("v") > 10)
    assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_orderby_missing_key_falls_through_to_engine(ruled_spark: ReparkSession) -> None:
    narrowed = _frame(ruled_spark).select("id")
    assert _rows(narrowed.orderBy("v")) == [(2,), (3,), (1,)]


def _lambda_frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame(
        [(1, [1, 5], 9, 0), (2, [2], 0, 2), (5, [5, 6, 7], 1, 7)],
        "id INT, arr ARRAY<INT>, x INT, v INT",
    )


def test_filter_exists_lambda_binds_outer_column(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    filtered = _lambda_frame(spark).filter("exists(arr, X -> X > 4)").select("id")
    assert _rows(filtered) == [(1,), (5,)]


def test_filter_transform_lambda_with_index_binds(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    filtered = _lambda_frame(spark).filter("transform(arr, (a, i) -> a + i)[0] > 1").select("id")
    assert _rows(filtered) == [(2,), (5,)]


def test_filter_lambda_shadowed_name_binds_outside(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = spark.createDataFrame(
        [(1, [1, 5], 9), (2, [2], 0), (5, [5, 6, 7], 1)],
        "id INT, arr ARRAY<INT>, v INT",
    )
    filtered = frame.filter("v < 100 AND exists(arr, v -> v > 4)").select("id")
    assert _rows(filtered) == [(1,), (5,)]


def _struct_frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame(
        [((1, 2), 3), ((4, 5), 6)],
        "T STRUCT<id:INT,x:INT>, id INT",
    )


def test_filter_struct_access_with_shared_name(ruled_spark: ReparkSession) -> None:
    filtered = _struct_frame(ruled_spark).filter("T.id > 3")
    assert filtered.columns == ["T", "id"]
    assert _rows(filtered) == [({"id": 4, "x": 5}, 6)]


def test_filter_facade_held_qualifier_raises_as_main(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    twins = frame.select("id", functions.col("v"), functions.col("v"))
    aliased = twins.alias("q").withColumn("w", functions.lit(1))
    with pytest.raises(AnalysisException) as caught:
        aliased.filter("q.v > 15")
    assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_filter_struct_qualifier_tie_prefers_qualifier(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([((10,), 1), ((2,), 9)], "T STRUCT<id:INT>, id INT")
    filtered = frame.alias("T").filter("T.id > 5").select("id")
    assert _rows(filtered) == [(9,)]


def test_filter_struct_qualifier_coincidence(ruled_spark: ReparkSession) -> None:
    filtered = _struct_frame(ruled_spark).alias("T").filter("T.id > 3").select("id")
    assert _rows(filtered) == [(6,)]
