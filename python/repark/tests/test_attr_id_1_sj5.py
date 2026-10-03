from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions as F  # noqa: N812
from repark.spark.window import Window

_FAIL_KEY = "spark.sql.analyzer.failAmbiguousSelfJoin"
_AUTO_KEY = "spark.sql.selfJoinAutoResolveAmbiguity"

_TEMPLATE_TAIL = (
    " are ambiguous. It's probably because you joined several Datasets together, and some of"
    " these Datasets are the same. This column points to one of the Datasets but Spark is"
    " unable to figure out which one. Please alias the Datasets with different names via"
    " `Dataset.as` before joining them, and specify the column using qualified name, e.g."
    ' `df.as("a").join(df.as("b"), $"a.id" > $"b.id")`. You can also set'
    " spark.sql.analyzer.failAmbiguousSelfJoin to false to disable this check."
)


@pytest.fixture(params=["false", "true"], ids=["f", "t"])
def spark(request: pytest.FixtureRequest) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-attr-id-1-sj5").getOrCreate()
    session.conf.set("spark.sql.caseSensitive", request.param)
    session.conf.set(_FAIL_KEY, "true")
    session.conf.set(_AUTO_KEY, "true")
    return session


def _env(spark: ReparkSession) -> SimpleNamespace:
    d = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "v"])
    return SimpleNamespace(
        d=d,
        f=d.filter("id > 1"),
        e=spark.createDataFrame([(1, 100), (2, 200)], ["id", "w"]),
    )


def _refuses_1182(thunk: Callable[[], Any], names: list[str]) -> None:
    with pytest.raises(AnalysisException) as refused:
        thunk().collect()
    assert refused.value.getCondition() == "_LEGACY_ERROR_TEMP_1182"
    params = refused.value.getMessageParameters()
    assert params["config"] == _FAIL_KEY
    assert params["ambiguousAttrs"] == ", ".join(names)
    assert str(refused.value)[len("Column " + ", ".join(names)) :] == _TEMPLATE_TAIL


def _answers(thunk: Callable[[], Any], columns: list[str], rows: list[list[Any]]) -> None:
    frame = thunk()
    assert frame.columns == columns
    assert sorted(tuple(row) for row in frame.collect()) == sorted(tuple(row) for row in rows)


def _answers_value(thunk: Callable[[], Any], value: int) -> None:
    assert thunk() == value


_CROSS_9 = [
    [1, 10, 1, 10],
    [1, 10, 2, 20],
    [1, 10, 3, 30],
    [2, 20, 1, 10],
    [2, 20, 2, 20],
    [2, 20, 3, 30],
    [3, 30, 1, 10],
    [3, 30, 2, 20],
    [3, 30, 3, 30],
]


def test_sj5_xj_sel_parent(spark: ReparkSession) -> None:
    """Parent select over a cross self-join refuses (``xj_sel_parent``, ``C_xj_sel_parent``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.crossJoin(env.d).select(env.d.v), ["v"])


def test_sj5_xj_sel_getitem(spark: ReparkSession) -> None:
    """Getitem select over a cross self-join refuses (``xj_sel_getitem``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.crossJoin(env.d).select(env.d["v"]), ["v"])


def test_sj5_xj_filter_parent(spark: ReparkSession) -> None:
    """Parent filter over a cross self-join refuses (``xj_filter_parent``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.crossJoin(env.d).filter(env.d.v > 15), ["v"])


def test_sj5_xjf_sel_parent_d(spark: ReparkSession) -> None:
    """Parent select over a derived cross join refuses (``xjf_sel_parent_d``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.crossJoin(env.f).select(env.d.v), ["v"])


def test_sj5_xjf_sel_parent_f(spark: ReparkSession) -> None:
    """Child select over a derived cross join refuses (``xjf_sel_parent_f``, ``C_xjf_sel_f``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.crossJoin(env.f).select(env.f.v), ["v"])


def test_sj5_xjf_filter_parent_f(spark: ReparkSession) -> None:
    """Child filter over a derived cross join refuses (``xjf_filter_parent_f``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.crossJoin(env.f).filter(env.f.v > 25), ["v"])


def test_sj5_jn_cross_filter_parent(spark: ReparkSession) -> None:
    """Filter over ``join(how="cross")`` refuses (``jn_cross_filter_parent``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, how="cross").filter(env.f.v > 25), ["v"])


def test_sj5_xj_wcr_sel_parent(spark: ReparkSession) -> None:
    """Parent select past a cross rename refuses (``C_xj_wcr_sel_parent``)."""
    env = _env(spark)
    renamed = env.d.crossJoin(env.d).withColumnRenamed("v", "z")
    _refuses_1182(lambda: renamed.select(env.d.id), ["id"])


def test_sj5_xj_wcr(spark: ReparkSession) -> None:
    """Cross rename answers and keeps both sides (``C_xj_wcr``, ``xj_wcr`` guard)."""
    env = _env(spark)
    _answers(
        lambda: env.d.crossJoin(env.d).withColumnRenamed("v", "z"),
        ["id", "z", "id", "z"],
        _CROSS_9,
    )


def test_sj5_xj_wc(spark: ReparkSession) -> None:
    """Cross replace answers over both sides (``C_xj_wc``, ``xj_wc``)."""
    env = _env(spark)
    _answers(
        lambda: env.d.crossJoin(env.d).withColumn("v", F.lit(0)),
        ["id", "v", "id", "v"],
        [[row[0], 0, row[2], 0] for row in _CROSS_9],
    )


def test_sj5_xj_dd(spark: ReparkSession) -> None:
    """Cross dedup answers (``C_xj_dd``, ``xj_dd`` guard)."""
    env = _env(spark)
    _answers_value(lambda: env.d.crossJoin(env.d).dropDuplicates(["v"]).count(), 9)


def test_sj5_xjf_wcr(spark: ReparkSession) -> None:
    """Derived cross rename answers 6 rows (``C_xjf_wcr``)."""
    env = _env(spark)
    _answers(
        lambda: env.d.crossJoin(env.f).withColumnRenamed("v", "z"),
        ["id", "z", "id", "z"],
        [
            [1, 10, 2, 20],
            [1, 10, 3, 30],
            [2, 20, 2, 20],
            [2, 20, 3, 30],
            [3, 30, 2, 20],
            [3, 30, 3, 30],
        ],
    )


def test_sj5_join_none_wcr(spark: ReparkSession) -> None:
    """Conditionless join rename answers 9 rows (``C_join_none_wcr``)."""
    env = _env(spark)
    _answers(
        lambda: env.d.join(env.d).withColumnRenamed("v", "z"), ["id", "z", "id", "z"], _CROSS_9
    )


def test_sj5_xj_todf_sel(spark: ReparkSession) -> None:
    """Cross rename-all then select answers (``C_xj_todf_sel`` guard)."""
    env = _env(spark)
    _answers(
        lambda: env.d.crossJoin(env.d).toDF("a", "b", "c", "e").select("c"),
        ["c"],
        [[1], [1], [1], [2], [2], [2], [3], [3], [3]],
    )


def test_sj5_xj_drop_parent(spark: ReparkSession) -> None:
    """Cross parent drop removes the left ``v`` (``xj_drop_parent``)."""
    env = _env(spark)
    _answers(
        lambda: env.d.crossJoin(env.d).drop(env.d.v),
        ["id", "id", "v"],
        [
            [1, 1, 10],
            [1, 2, 20],
            [1, 3, 30],
            [2, 1, 10],
            [2, 2, 20],
            [2, 3, 30],
            [3, 1, 10],
            [3, 2, 20],
            [3, 3, 30],
        ],
    )


def test_sj5_xj_count(spark: ReparkSession) -> None:
    """Cross count answers 9 (``C_xj_count`` guard)."""
    env = _env(spark)
    _answers_value(lambda: env.d.crossJoin(env.d).count(), 9)


def test_sj5_xj_drop_str(spark: ReparkSession) -> None:
    """Cross name drop answers 9 rows (``C_xj_drop_str``, ``xj_drop_str`` guard)."""
    env = _env(spark)
    _answers(
        lambda: env.d.crossJoin(env.d).drop("v"),
        ["id", "id"],
        [
            [1, 1],
            [1, 2],
            [1, 3],
            [2, 1],
            [2, 2],
            [2, 3],
            [3, 1],
            [3, 2],
            [3, 3],
        ],
    )


def test_sj5_xje_sel(spark: ReparkSession) -> None:
    """Cross select over disjoint names answers 6 rows (``C_xje_sel`` guard)."""
    env = _env(spark)
    _answers(
        lambda: env.d.crossJoin(env.e).select(env.d.v, env.e.w),
        ["v", "w"],
        [[10, 100], [10, 200], [20, 100], [20, 200], [30, 100], [30, 200]],
    )


def _refuses_ambiguous_params(thunk: Callable[[], Any], name: str, references: str) -> None:
    with pytest.raises(AnalysisException) as refused:
        thunk().collect()
    assert refused.value.getCondition() == "AMBIGUOUS_REFERENCE"
    assert refused.value.getMessageParameters() == {"name": name, "referenceNames": references}
    assert str(refused.value) == (
        f"[AMBIGUOUS_REFERENCE] Reference {name} is ambiguous, could be: {references}."
    )


def _cross_v(spark: ReparkSession) -> Any:
    d = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "v"])
    return d.select("id", "v").crossJoin(d.select(F.col("v")).filter(F.col("v") > 15))


def _inner_v(spark: ReparkSession) -> Any:
    d = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "v"])
    left = d.select("id", "v")
    right = d.select(F.col("v")).filter(F.col("v") > 15)
    return left.join(right, left.v == right.v)


def test_sj5_f1_bq_cross(spark: ReparkSession) -> None:
    """Backquoted filter over a cross join refuses (``r5p6.*|j_cross|*|bq``)."""
    frame = _cross_v(spark).distinct()
    _refuses_ambiguous_params(lambda: frame.filter("`v` > 15"), "`v`", "[`v`, `v`]")


def test_sj5_f1_bq_inner(spark: ReparkSession) -> None:
    """Backquoted filter over an inner join refuses (F1 inner twin)."""
    frame = _inner_v(spark)
    _refuses_ambiguous_params(lambda: frame.filter("`v` > 15"), "`v`", "[`v`, `v`]")


def test_sj5_f1_sexpr_cross(spark: ReparkSession) -> None:
    """SelectExpr over a cross join refuses (``r5p7.*|j_cross|star|sexpr``)."""
    frame = _cross_v(spark).select("*")
    _refuses_ambiguous_params(lambda: frame.selectExpr("v + 1 AS z"), "`v`", "[`v`, `v`]")


def test_sj5_f1_sexpr_inner(spark: ReparkSession) -> None:
    """SelectExpr over an inner join refuses (F1 inner twin)."""
    frame = _inner_v(spark)
    _refuses_ambiguous_params(lambda: frame.selectExpr("v + 1 AS z"), "`v`", "[`v`, `v`]")


def test_sj5_f1_wcz_cross(spark: ReparkSession) -> None:
    """WithColumn compound over a cross join refuses (``r5p6.*|j_cross|*|wcz``)."""
    frame = _cross_v(spark).distinct()
    _refuses_ambiguous_params(lambda: frame.withColumn("z", F.col("v") + 1), "`v`", "[`v`, `v`]")


def test_sj5_f1_wcz_inner(spark: ReparkSession) -> None:
    """WithColumn compound over an inner join refuses (F1 inner twin)."""
    frame = _inner_v(spark)
    _refuses_ambiguous_params(lambda: frame.withColumn("z", F.col("v") + 1), "`v`", "[`v`, `v`]")


def test_sj5_f1_summ_cross(spark: ReparkSession) -> None:
    """Summary over a cross join refuses (``r5p6.*|j_cross|*|summ``)."""
    frame = _cross_v(spark).distinct()
    _refuses_ambiguous_params(lambda: frame.summary("count"), "`v`", "[`v`, `v`]")


def test_sj5_f1_summ_inner(spark: ReparkSession) -> None:
    """Summary over an inner join refuses (F1 inner twin)."""
    frame = _inner_v(spark)
    _refuses_ambiguous_params(lambda: frame.summary("count"), "`v`", "[`v`, `v`]")


def test_sj5_f1_qcol_cross(spark: ReparkSession) -> None:
    """Qualified filter over an aliased cross join refuses (``r5p7.*|j_cross|wc_alias|q_col``)."""
    frame = _cross_v(spark).withColumn("w", F.lit(1)).alias("q")
    _refuses_ambiguous_params(
        lambda: frame.filter(F.col("q.v") > 15), "`q`.`v`", "[`q`.`v`, `q`.`v`]"
    )


def test_sj5_f1_qcol_inner(spark: ReparkSession) -> None:
    """Qualified filter over an aliased inner join refuses (F1 inner twin)."""
    frame = _inner_v(spark).withColumn("w", F.lit(1)).alias("q")
    _refuses_ambiguous_params(
        lambda: frame.filter(F.col("q.v") > 15), "`q`.`v`", "[`q`.`v`, `q`.`v`]"
    )


def _dd_alias_left(spark: ReparkSession) -> Any:
    d = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "v"])
    other = d.select("id", (F.col("v") * 2).alias("v"))
    joined = d.select("id", "v").join(other.filter(F.col("id") < 3), "id", "left")
    return joined.alias("q")


def test_sj5_f1_dropdup_alias_answers(spark: ReparkSession) -> None:
    """DropDuplicates over an aliased dup-display join answers (``r5p6.*|dd|j_*|alias|dd``)."""
    _answers_value(lambda: _dd_alias_left(spark).dropDuplicates(["v"]).count(), 3)


def test_sj5_f1_window_free_ref_refuses(spark: ReparkSession) -> None:
    """Window over a free dup display refuses (``win_probe.py`` user-window)."""
    frame = _cross_v(spark)
    window = Window.partitionBy(F.col("v")).orderBy(F.col("id"))
    _refuses_ambiguous_params(
        lambda: frame.withColumn("z", F.row_number().over(window)), "`v`", "[`v`, `v`]"
    )
