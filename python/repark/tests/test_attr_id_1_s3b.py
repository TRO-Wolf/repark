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


def test_orderby_routes_ambiguous_project_key_through_input(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    flipped = frame.select((frame.v * -1).alias("v"), (frame.v + 1).alias("v"), frame.id)
    assert _rows(flipped.orderBy("v")) == [(-10, 11, 2), (-20, 21, 3), (-30, 31, 1)]
    assert _rows(flipped.orderBy(functions.desc("v"))) == [
        (-30, 31, 1),
        (-20, 21, 3),
        (-10, 11, 2),
    ]


def test_orderby_fcol_over_exact_project_dup_sorts_by_input(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    flipped = frame.select((frame.v * -1).alias("v"), (frame.v + 1).alias("v"), frame.id)
    assert _rows(flipped.orderBy(functions.col("v"))) == [(-10, 11, 2), (-20, 21, 3), (-30, 31, 1)]


def test_orderby_string_over_exact_project_dup_sorts_by_input(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    flipped = frame.select((frame.v + 1).alias("v"), (frame.v * -1).alias("v"), frame.id)
    assert _rows(flipped.sort("v", ascending=False)) == [(31, -30, 1), (21, -20, 3), (11, -10, 2)]
    assert _rows(flipped.orderBy("v")) == [(11, -10, 2), (21, -20, 3), (31, -30, 1)]


def test_orderby_expr_over_case_twins_is_ambiguous_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _frame(spark)
    twins = frame.select((frame.v * -1).alias("V"), (frame.v + 1).alias("v"), frame.id)
    with pytest.raises(AnalysisException) as caught:
        twins.orderBy(functions.col("v") + 0)
    assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_orderby_case_twins_bind_own_column_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = _frame(spark)
    twins = frame.select((frame.v * -1).alias("V"), (frame.v + 1).alias("v"), frame.id)
    assert _rows(twins.orderBy("v")) == [(-10, 11, 2), (-20, 21, 3), (-30, 31, 1)]
    assert _rows(twins.orderBy("V")) == [(-30, 31, 1), (-20, 21, 3), (-10, 11, 2)]
    assert _rows(twins.orderBy(functions.col("v"))) == [(-10, 11, 2), (-20, 21, 3), (-30, 31, 1)]
    assert _rows(twins.orderBy(functions.col("V"))) == [(-30, 31, 1), (-20, 21, 3), (-10, 11, 2)]


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


def test_filter_facade_held_qualifier_binds_single_id(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    twins = frame.select("id", functions.col("v"), functions.col("v"))
    aliased = twins.alias("q").withColumn("w", functions.lit(1))
    assert _rows(aliased.filter("q.v > 15")) == [(1, 30, 30, 1), (3, 20, 20, 1)]


def test_filter_struct_qualifier_tie_prefers_qualifier(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([((10,), 1), ((2,), 9)], "T STRUCT<id:INT>, id INT")
    filtered = frame.alias("T").filter("T.id > 5").select("id")
    assert _rows(filtered) == [(9,)]


def test_filter_struct_qualifier_coincidence(ruled_spark: ReparkSession) -> None:
    filtered = _struct_frame(ruled_spark).alias("T").filter("T.id > 3").select("id")
    assert _rows(filtered) == [(6,)]


def _nested_lambda_frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame(
        [(1, [1, 5], 9, 0), (2, [2], 0, 2), (5, [5, 6, 7], 1, 7), (6, [], 4, 3)],
        "id INT, arr ARRAY<INT>, T INT, x INT",
    )


_NESTED_COLLISION_PREDS = [
    "exists(arr, X -> exists(arr, x -> X > x))",
    "exists(arr, x -> exists(arr, X -> X > x))",
    "exists(arr, T -> exists(arr, t -> T > t))",
    "exists(arr, t -> exists(arr, T -> T > t))",
]


def test_filter_nested_lambda_collision_folds_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _nested_lambda_frame(spark).select("id", "arr", "T", "x")
    for pred in _NESTED_COLLISION_PREDS:
        assert _rows(frame.filter(pred).select("id")) == []


def test_filter_nested_lambda_reverse_collision_folds_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _nested_lambda_frame(spark).select("id", "arr", "T", "x")
    filtered = frame.filter("exists(arr, x -> exists(arr, X -> x > X))").select("id")
    assert _rows(filtered) == []


def test_filter_nested_lambda_vars_stay_distinct_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = _nested_lambda_frame(spark).select("id", "arr", "T", "x")
    for pred in _NESTED_COLLISION_PREDS:
        assert _rows(frame.filter(pred).select("id")) == [(1,), (5,)]


def test_filter_single_level_folded_lambda_ref_folds_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _nested_lambda_frame(spark).select("id", "arr", "T", "x")
    filtered = frame.filter("exists(arr, V -> v > 4)").select("id")
    assert _rows(filtered) == [(1,), (5,)]


def _lambda_corpus_frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame(
        [
            (1, [1, 5], 9, 0, 10, (1, 2), [(1, 2), (3, 4)], {"k": 1, "a": 5}, 3, [[1, 2], [5]]),
            (2, [2], 0, 2, 20, (5, 6), [(7, 1)], {"k": 2}, 1, [[2]]),
            (5, [5, 6, 7], 1, 7, 30, (0, 0), [], {"z": 9}, 8, [[5, 6], [7]]),
            (6, [], 4, 3, None, None, None, None, None, []),
        ],
        "id INT, arr ARRAY<INT>, T INT, x INT, v INT, s STRUCT<a:INT,B:INT>, "
        "sa ARRAY<STRUCT<a:INT,B:INT>>, m MAP<STRING,INT>, k INT, nest ARRAY<ARRAY<INT>>",
    ).select("id", "arr", "T", "x", "v", "sa", "m")


def test_filter_backticked_decl_folded_ref_stays_on_binder(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    filtered = _lambda_corpus_frame(spark).filter("exists(arr, `X` -> x > 4)").select("id")
    assert _rows(filtered) == [(1,), (5,)]


def test_filter_dotted_folded_head_stays_on_binder(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    filtered = _lambda_corpus_frame(spark).filter("exists(sa, S -> s.a > 2)").select("id")
    assert _rows(filtered) == [(1,), (2,)]


def test_filter_nested_no_collision_stays_on_binder(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _lambda_corpus_frame(spark).select("*", functions.lit(9).alias("X"))
    filtered = frame.filter("exists(arr, x -> exists(arr, y -> x > Y))").select("id")
    assert _rows(filtered) == [(1,), (5,)]


def _alias_dup_frame(spark: ReparkSession) -> Any:
    dd = spark.createDataFrame(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c"), (4, None, None)],
        "id INT, v INT, Data STRING",
    )
    right = dd.select(functions.col("v")).filter(functions.col("v") > 15)
    return dd.select("id", "v").crossJoin(right)


def test_filter_str_of_alias_dup_past_with_column_is_ambiguous(ruled_spark: ReparkSession) -> None:
    framed = _alias_dup_frame(ruled_spark).alias("q").withColumn("w", functions.lit(1))
    with pytest.raises(AnalysisException) as caught:
        framed.filter("v > 15")
    assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_filter_str_of_qualified_alias_dup_is_ambiguous(ruled_spark: ReparkSession) -> None:
    framed = _alias_dup_frame(ruled_spark).alias("q")
    with pytest.raises(AnalysisException) as caught:
        framed.filter("q.v > 15")
    assert caught.value.getCondition() == "AMBIGUOUS_REFERENCE"
