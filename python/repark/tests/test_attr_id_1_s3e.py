from __future__ import annotations

from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-attr-id-1-s3e").getOrCreate()


@pytest.fixture(params=[False, True], ids=["insensitive", "sensitive"])
def ruled_spark(spark: ReparkSession, request: Any) -> ReparkSession:
    spark.conf.set("spark.sql.caseSensitive", "true" if request.param else "false")
    return spark


def _frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 10), (2, 20)], "id BIGINT, v BIGINT")


def _using(spark: ReparkSession) -> Any:
    frame = _frame(spark)
    return frame.alias("a").join(frame.alias("b"), "id")


def _cond(spark: ReparkSession) -> Any:
    left = _frame(spark).alias("a")
    right = _frame(spark).alias("b")
    return left.join(right, left.id == right.id)


def _twins(spark: ReparkSession) -> Any:
    frame = _frame(spark)
    return frame.select(frame.v, frame.v.alias("v"))


def _struct(spark: ReparkSession) -> Any:
    frame = _frame(spark)
    return frame.withColumn("s", functions.struct((frame.id + 100).alias("x")))


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted((tuple(row) for row in frame.collect()), key=repr)


def test_select_qualified_binds_each_side(ruled_spark: ReparkSession) -> None:
    assert _rows(_using(ruled_spark).select("a.v")) == [(10,), (20,)]
    assert _rows(_using(ruled_spark).select("b.v")) == [(10,), (20,)]
    assert _rows(_cond(ruled_spark).select("a.v", "b.v")) == [(10, 10), (20, 20)]


def test_getitem_and_free_column_bind_qualified(ruled_spark: ReparkSession) -> None:
    joined = _using(ruled_spark)
    assert _rows(joined.select(joined["a.v"])) == [(10,), (20,)]
    assert _rows(joined.select(functions.col("b.v"))) == [(10,), (20,)]
    assert _rows(joined.select(functions.col("a.v") + functions.col("b.v"))) == [
        (20,),
        (40,),
    ]


def test_filter_qualified_binds_each_side(ruled_spark: ReparkSession) -> None:
    assert _rows(_using(ruled_spark).filter("a.v > 10")) == [(2, 20, 20)]
    assert _rows(_cond(ruled_spark).filter("b.v <= 10")) == [(1, 10, 1, 10)]


def test_orderby_qualified_sorts_each_side(ruled_spark: ReparkSession) -> None:
    assert _rows(_using(ruled_spark).orderBy("a.v")) == [(1, 10, 10), (2, 20, 20)]
    ordered = _cond(ruled_spark).orderBy(functions.col("b.v").desc()).collect()
    assert [tuple(row) for row in ordered] == [(2, 20, 2, 20), (1, 10, 1, 10)]


def test_drop_str_qualified_is_noop(ruled_spark: ReparkSession) -> None:
    assert _using(ruled_spark).drop("a.v").columns == ["id", "v", "v"]
    assert _cond(ruled_spark).drop("b.v").columns == ["id", "v", "id", "v"]


def test_drop_column_qualified_drops_one_side(ruled_spark: ReparkSession) -> None:
    assert _using(ruled_spark).drop(functions.col("a.v")).columns == ["id", "v"]
    assert _cond(ruled_spark).drop(functions.col("b.v")).columns == ["id", "v", "id"]
    assert _using(ruled_spark).drop(functions.col("a.v"), functions.col("b.v")).columns == ["id"]


def test_drop_column_qualified_miss_is_noop(ruled_spark: ReparkSession) -> None:
    assert _using(ruled_spark).drop(functions.col("a.zzz")).columns == ["id", "v", "v"]


def test_withcolumn_qualified_compound(ruled_spark: ReparkSession) -> None:
    assert _rows(_cond(ruled_spark).withColumn("n", functions.col("a.v") + 1)) == [
        (1, 10, 1, 10, 11),
        (2, 20, 2, 20, 21),
    ]
    assert _rows(_cond(ruled_spark).withColumn("n", functions.col("b.v") + 1).select("n")) == [
        (11,),
        (21,),
    ]


def test_withcolumn_using_join_dup_frame_is_ambiguous_divergence(
    ruled_spark: ReparkSession,
) -> None:
    with pytest.raises(AnalysisException) as refused:
        _using(ruled_spark).withColumn("n", functions.col("a.v") + 1).collect()
    assert refused.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_qualifier_case_folds_when_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _frame(spark).alias("T")
    assert _rows(frame.select("T.v")) == [(10,), (20,)]
    assert _rows(frame.select("t.v")) == [(10,), (20,)]
    assert _rows(frame.filter("t.v > 10")) == [(2, 20)]
    assert _rows(_using(spark).select("A.v", "b.V")) == [(10, 10), (20, 20)]


def test_qualifier_case_is_exact_when_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = _frame(spark).alias("T")
    assert _rows(frame.select("T.v")) == [(10,), (20,)]
    with pytest.raises(AnalysisException) as refused:
        frame.select("t.v").collect()
    assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    assert "`t`.`v`" in str(refused.value)
    with pytest.raises(AnalysisException) as refused:
        _using(spark).select("A.v").collect()
    assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_qualified_miss_raises_unresolved(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as refused:
        _using(ruled_spark).select("a.zzz").collect()
    assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    assert "`a`.`zzz`" in str(refused.value)
    with pytest.raises(AnalysisException) as refused:
        _cond(ruled_spark).select("zz.v").collect()
    assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    assert "`zz`.`v`" in str(refused.value)


def test_qualified_twins_refuse_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as refused:
        _twins(ruled_spark).alias("q").select("q.v").collect()
    assert refused.value.getCondition() == "AMBIGUOUS_REFERENCE"
    assert str(refused.value) == (
        "[AMBIGUOUS_REFERENCE] Reference `q`.`v` is ambiguous, "
        "could be: [`q`.`v`, `q`.`v`]. SQLSTATE: 42704"
    )


def test_sort_twins_raise_unresolved(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as refused:
        _twins(ruled_spark).alias("q").sortWithinPartitions("q.v").collect()
    assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_drop_twins_refuse_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as refused:
        _twins(ruled_spark).alias("q").drop(functions.col("q.v"))
    assert refused.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_struct_select_is_unresolved_divergence(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as refused:
        _struct(ruled_spark).select("s.x").collect()
    assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_struct_orderby_is_unresolved_divergence(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as refused:
        _struct(ruled_spark).orderBy("s.x").collect()
    assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_struct_filter_binds_struct_field(ruled_spark: ReparkSession) -> None:
    assert _rows(_struct(ruled_spark).filter("s.x > 101")) == [(2, 20, {"x": 102})]


def test_struct_named_like_qualifier_prefers_qualifier(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    tied = frame.withColumn("s", functions.struct((frame.id + 100).alias("v"))).alias("s")
    assert _rows(tied.select("s.v")) == [(10,), (20,)]
    assert _rows(tied.filter("s.v > 10")) == [(2, 20, {"v": 102})]
    assert _rows(tied.select("s.*")) == [(1, 10, {"v": 101}), (2, 20, {"v": 102})]


def test_realias_old_qualifier_misses_new_binds(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark).alias("a").alias("b")
    assert _rows(frame.select("b.v")) == [(10,), (20,)]
    with pytest.raises(AnalysisException) as refused:
        frame.select("a.v").collect()
    assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    assert "`a`.`v`" in str(refused.value)


def test_join_realias_drops_old_qualifiers(ruled_spark: ReparkSession) -> None:
    frame = _using(ruled_spark).alias("j")
    assert _rows(frame.select("j.id")) == [(1,), (2,)]
    with pytest.raises(AnalysisException) as refused:
        frame.select("a.v").collect()
    assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_case_rule_change_between_build_and_bind(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    built_sensitive = _frame(spark).alias("T").join(_frame(spark).alias("b"), "id")
    spark.conf.set("spark.sql.caseSensitive", "false")
    assert _rows(built_sensitive.select("t.v")) == [(10,), (20,)]
    spark.conf.set("spark.sql.caseSensitive", "false")
    built_insensitive = _frame(spark).alias("T").join(_frame(spark).alias("b"), "id")
    spark.conf.set("spark.sql.caseSensitive", "true")
    assert _rows(built_insensitive.select("T.v")) == [(10,), (20,)]


def test_qualified_star_expands_side(ruled_spark: ReparkSession) -> None:
    assert _rows(_using(ruled_spark).select("a.*")) == [(1, 10), (2, 20)]
    assert _using(ruled_spark).select("a.*").columns == ["id", "v"]
    assert _rows(_cond(ruled_spark).select("b.*")) == [(1, 10), (2, 20)]
    assert _rows(_frame(ruled_spark).alias("q").select(functions.col("q.*"))) == [
        (1, 10),
        (2, 20),
    ]
    assert _rows(_using(ruled_spark).select(functions.col("a.*"))) == [(1, 10), (2, 20)]


def test_qualified_star_folds_when_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    assert _rows(_frame(spark).alias("q").select("Q.*")) == [(1, 10), (2, 20)]


def test_unknown_star_falls_through_to_engine_divergence(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException):
        _using(ruled_spark).select("zzz.*").collect()


def test_selectexpr_single_qualified_names_bare_value(ruled_spark: ReparkSession) -> None:
    projected = _frame(ruled_spark).alias("q").selectExpr("q.v")
    assert projected.columns == ["v"]
    assert _rows(projected) == [(10,), (20,)]


def test_cross_join_sides_bind(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    crossed = frame.alias("l").crossJoin(frame.alias("r"))
    assert _rows(crossed.select("l.v", "r.v")) == [(10, 10), (10, 20), (20, 10), (20, 20)]


def test_qualifiers_survive_select_and_filter(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark).alias("q")
    assert _rows(frame.select("id", "v").select("q.v")) == [(10,), (20,)]
    assert _rows(frame.filter("id > 0").select("q.v")) == [(10,), (20,)]


def test_recomputed_columns_lose_qualifiers(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark).alias("q")
    with pytest.raises(AnalysisException) as refused:
        frame.withColumnRenamed("v", "w").select("q.w").collect()
    assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    with pytest.raises(AnalysisException) as refused:
        frame.withColumn("v", functions.lit(1)).select("q.v").collect()
    assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
