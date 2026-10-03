from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions as F  # noqa: N812

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


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-attr-id-1-sj4").getOrCreate()
    session.conf.set("spark.sql.caseSensitive", "false")
    session.conf.set(_FAIL_KEY, "true")
    session.conf.set(_AUTO_KEY, "true")
    return session


def _env(spark: ReparkSession) -> SimpleNamespace:
    d = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "v"])
    return SimpleNamespace(
        d=d,
        f=d.filter("id > 1"),
        g=d.filter("id < 3"),
        f2=d.filter("id > 2"),
        a=d.alias("a"),
        b=d.alias("b"),
        w=d.withColumn("id", d.id + 1),
        r=d.withColumnRenamed("v", "z"),
        s=d.select("id"),
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


def _answers(thunk: Callable[[], Any], rows: list[list[Any]]) -> None:
    assert sorted(tuple(row) for row in thunk().collect()) == sorted(tuple(row) for row in rows)


def _refuses_ambiguous(thunk: Callable[[], Any]) -> None:
    with pytest.raises(AnalysisException, match="AMBIGUOUS_REFERENCE"):
        thunk().collect()


_MISSING_OP = "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION"
_MISSING_IN = "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT"


def _refuses_missing(thunk: Callable[[], Any], cond: str, name: str) -> None:
    with pytest.raises(AnalysisException) as refused:
        thunk().collect()
    assert refused.value.getCondition() == cond
    assert f'"{name}"' in str(refused.value)


def _off(spark: ReparkSession) -> None:
    spark.conf.set(_FAIL_KEY, "false")


def test_sj4_a_inner_sel_f_v(spark: ReparkSession) -> None:
    """Inner self-join select on the child refuses (``A_inner_sel_f_v``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, env.d.id == env.f.id).select(env.f.v), ["v"])


def test_sj4_a_inner_sel_d_v(spark: ReparkSession) -> None:
    """Inner self-join select on the parent refuses (``A_inner_sel_d_v``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, env.d.id == env.f.id).select(env.d.v), ["v"])


def test_sj4_a_inner_sel_both_id(spark: ReparkSession) -> None:
    """Both id references refuse with multiplicity two (``A_inner_sel_both_id``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).select(env.d.id, env.f.id), ["id", "id"]
    )


def test_sj4_a_inner_getitem_f(spark: ReparkSession) -> None:
    """Getitem select on the child refuses (``A_inner_getitem_f``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, env.d.id == env.f.id).select(env.f["v"]), ["v"])


def test_sj4_a_inner_filter_f(spark: ReparkSession) -> None:
    """Filter on the child refuses (``A_inner_filter_f``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, env.d.id == env.f.id).filter(env.f.v > 25), ["v"])


def test_sj4_a_inner_filter_d(spark: ReparkSession) -> None:
    """Filter on the parent refuses (``A_inner_filter_d``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, env.d.id == env.f.id).filter(env.d.v > 25), ["v"])


def test_sj4_a_inner_group_f(spark: ReparkSession) -> None:
    """Group key on the child refuses (``A_inner_group_f``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, env.d.id == env.f.id).groupBy(env.f.v).count(), ["v"])


@pytest.mark.xfail(strict=True, reason="eager groupBy reports keys only; ruling picks the model")
def test_sj4_a_inner_agg_f(spark: ReparkSession) -> None:
    """Group key plus aggregate expression refuse together (``A_inner_agg_f``)."""
    env = _env(spark)
    with pytest.raises(AnalysisException) as refused:
        env.d.join(env.f, env.d.id == env.f.id).groupBy(env.d.id).agg(F.sum(env.f.v).alias("s"))
    assert refused.value.getCondition() == "_LEGACY_ERROR_TEMP_1182"
    assert sorted(refused.value.getMessageParameters()["ambiguousAttrs"].split(", ")) == ["id", "v"]


def test_sj4_a_inner_order_f(spark: ReparkSession) -> None:
    """Order key on the child refuses (``A_inner_order_f``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).orderBy(env.f.v.desc()).select(env.d.id),
        ["v"],
    )


def test_sj4_a_inner_wc_f(spark: ReparkSession) -> None:
    """WithColumn over the child refuses (``A_inner_wc_f``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).withColumn("z", env.f.v + 1), ["v"]
    )


def test_sj4_a_inner_limit_sel_f(spark: ReparkSession) -> None:
    """Select past a limit still refuses (``A_inner_limit_sel_f``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, env.d.id == env.f.id).limit(10).select(env.f.v), ["v"])


def test_sj4_a_inner_star_sel_f(spark: ReparkSession) -> None:
    """Select past a star still refuses (``A_inner_star_sel_f``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).select("*").select(env.f.v), ["v"]
    )


def test_sj4_a_left_sel(spark: ReparkSession) -> None:
    """Left-join id pair refuses with multiplicity two (``A_left_sel``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id, "left").select(env.d.id, env.f.id),
        ["id", "id"],
    )


def test_sj4_a_rev_left_sel_d(spark: ReparkSession) -> None:
    """Reversed join select on the right parent refuses (``A_rev_left_sel_d``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.f.join(env.d, env.f.id == env.d.id).select(env.d.v), ["v"])


def test_sj4_a_alias_left_sel_d(spark: ReparkSession) -> None:
    """Aliased join select on the right frame refuses (``A_alias_left_sel_d``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.a.join(env.d, env.a.id == env.d.id).select(env.d.v), ["v"])


def test_sj4_a_wc_sel_w_v(spark: ReparkSession) -> None:
    """Select on the withColumn child refuses (``A_wc_sel_w_v``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.w, env.d.id == env.w.id).select(env.w.v), ["v"])


def test_sj4_a_wcr_sel_r_id(spark: ReparkSession) -> None:
    """Renamed-child id pair refuses with multiplicity two (``A_wcr_sel_r_id``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.r, env.d.id == env.r.id + 0).select(env.r.id), ["id", "id"]
    )


def test_sj4_a_sel_only_id_left_sel_d_v(spark: ReparkSession) -> None:
    """Projected-child join select on the parent refuses (``A_sel_only_id_left_sel_d_v``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.s.join(env.d, env.s.id == env.d.id).select(env.d.v), ["v"])


def test_sj4_a_sel_only_id_using_sel_d_v(spark: ReparkSession) -> None:
    """Projected-child USING join select refuses (``A_sel_only_id_using_sel_d_v``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.s.join(env.d, "id").select(env.d.v), ["v"])


def test_sj4_a_cache_sel(spark: ReparkSession) -> None:
    """Cached-child join select refuses (``A_cache_sel``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f.cache(), env.d.id == env.f.id).select(env.f.v), ["v"])


def test_sj4_f_anti_idiom(spark: ReparkSession) -> None:
    """The left-join isNull anti-join idiom refuses (``F_anti_idiom``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: (
            env.d.join(env.f, env.d.id == env.f.id, "left")
            .where(env.f.id.isNull())
            .select(env.d.id)
        ),
        ["id"],
    )


def test_sj4_i_repartition_f(spark: ReparkSession) -> None:
    """Repartition over the child refuses (``I_repartition_f``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, env.d.id == env.f.id).repartition(2, env.f.v), ["v"])


def test_sj4_i_sortwithin_f(spark: ReparkSession) -> None:
    """Sort-within-partitions over the child refuses (``I_sortwithin_f``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).sortWithinPartitions(env.f.v), ["v"]
    )


def test_sj4_i_withcolumns_f(spark: ReparkSession) -> None:
    """WithColumns over the child refuses (``I_withcolumns_f``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).withColumns({"z": env.f.v}), ["v"]
    )


def test_sj4_i_when_f(spark: ReparkSession) -> None:
    """A conditional over the child refuses (``I_when_f``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).select(
            F.when(env.f.v > 1, 1).otherwise(0).alias("k")
        ),
        ["v"],
    )


def test_sj4_i_alias_d(spark: ReparkSession) -> None:
    """An aliased parent id refuses (``I_alias_d``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).select(env.d.id.alias("k")), ["id"]
    )


def test_sj4_i_agg_then_sel_f(spark: ReparkSession) -> None:
    """Aggregate key on the parent refuses (``I_agg_then_sel_f``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).groupBy(env.d.id).count().select(env.f.v),
        ["id"],
    )


def test_sj4_i_checkpoint_sel_c(spark: ReparkSession) -> None:
    """Checkpoint-child join select on the child refuses (``I_checkpoint_sel_c``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: (lambda c: env.d.join(c, env.d.id == c.id).select(c.v))(env.d.localCheckpoint()),
        ["v"],
    )


def test_sj4_i_join_then_join_unrelated_ref(spark: ReparkSession) -> None:
    """A parent ref past a second join refuses (``I_join_then_join_unrelated_ref``)."""
    env = _env(spark)
    other = spark.createDataFrame([(9,)], ["q"])
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).crossJoin(other).select(env.f.v),
        ["v"],
    )


def test_sj4_i_union_after_join(spark: ReparkSession) -> None:
    """A parent ref past a union refuses (``I_union_after_join``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: (lambda j: j.union(j).select(env.f.v))(env.d.join(env.f, env.d.id == env.f.id)),
        ["v"],
    )


def _view_env(spark: ReparkSession) -> SimpleNamespace:
    d = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "v"])
    d.createOrReplaceTempView("sjv3")
    return SimpleNamespace(d=d, t=spark.table("sjv3"), q=spark.sql("SELECT * FROM sjv3"))


def test_sj4_j_view_sel_t(spark: ReparkSession) -> None:
    """View-child join select on the view refuses (``J_view_sel_t``)."""
    env = _view_env(spark)
    _refuses_1182(lambda: env.d.join(env.t, env.d.id == env.t.id).select(env.t.v), ["v"])


def test_sj4_j_sql_sel_q(spark: ReparkSession) -> None:
    """SQL-child join select on the query refuses (``J_sql_sel_q``)."""
    env = _view_env(spark)
    _refuses_1182(lambda: env.d.join(env.q, env.d.id == env.q.id).select(env.q.v), ["v"])


def test_sj4_k_on_union_first_input(spark: ReparkSession) -> None:
    """Union-child join select on the union refuses (``K_on_union_first_input``)."""
    env = _env(spark)
    u = env.d.union(env.d.filter("id > 1"))
    _refuses_1182(lambda: env.d.join(u, env.d.id == u.id).select(u.v), ["v"])


def test_sj4_k_off_shared_sel(spark: ReparkSession) -> None:
    """Conf-off select on a renewed-absent parent is missing (``K_off_shared_sel``)."""
    _off(spark)
    env = _env(spark)
    _refuses_missing(
        lambda: env.s.join(env.d, env.s.id == env.d.id).select(env.d.v), _MISSING_OP, "v"
    )


def test_sj4_k_off_shared_filter(spark: ReparkSession) -> None:
    """Conf-off filter on a renewed-absent parent is missing (``K_off_shared_filter``)."""
    _off(spark)
    env = _env(spark)
    _refuses_missing(
        lambda: env.s.join(env.d, env.s.id == env.d.id).filter(env.d.v > 15), _MISSING_OP, "v"
    )


def test_sj4_k_off_shared_order(spark: ReparkSession) -> None:
    """Conf-off order on a renewed-absent parent is missing (``K_off_shared_order``)."""
    _off(spark)
    env = _env(spark)
    _refuses_missing(
        lambda: env.s.join(env.d, env.s.id == env.d.id).orderBy(env.d.v).select("id"),
        _MISSING_OP,
        "v",
    )


def test_sj4_k_drop_sel_missing_from_input(spark: ReparkSession) -> None:
    """Select past a column drop misses without the appear clause (``sj4_k_drop_sel``)."""
    env = _env(spark)
    _refuses_missing(
        lambda: env.s.join(env.d, env.s.id == env.d.id).drop("v").select(env.d.v),
        _MISSING_IN,
        "v",
    )


def test_sj4_k_drop_filter_missing_from_input(spark: ReparkSession) -> None:
    """Filter past a column drop misses without the appear clause (``sj4_k_drop_filter``)."""
    env = _env(spark)
    _refuses_missing(
        lambda: env.s.join(env.d, env.s.id == env.d.id).drop("v").filter(env.d.v > 15),
        _MISSING_IN,
        "v",
    )


def test_sj4_k_drop_order_missing_from_input(spark: ReparkSession) -> None:
    """Order past a column drop misses without the appear clause (``sj4_k_drop_order``)."""
    env = _env(spark)
    _refuses_missing(
        lambda: env.s.join(env.d, env.s.id == env.d.id).drop("v").orderBy(env.d.v),
        _MISSING_IN,
        "v",
    )


def test_sj4_k_on_sel_prefers_1182(spark: ReparkSession) -> None:
    """Conf-on select on a renewed-absent parent refuses 1182 first (``sj4_k_sel_on``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.s.join(env.d, env.s.id == env.d.id).select(env.d.v), ["v"])


def test_sj4_k_on_filter_prefers_1182(spark: ReparkSession) -> None:
    """Conf-on filter on a renewed-absent parent refuses 1182 first (``sj4_k_filter_on``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.s.join(env.d, env.s.id == env.d.id).filter(env.d.v > 15), ["v"])


def test_sj4_k_on_order_prefers_1182(spark: ReparkSession) -> None:
    """Conf-on order on a renewed-absent parent refuses 1182 first (``sj4_k_order_on``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.s.join(env.d, env.s.id == env.d.id).orderBy(env.d.v).select("id"), ["v"]
    )


def test_sj4_range_f(spark: ReparkSession) -> None:
    """Repartition-by-range over the child refuses (``sj4_range_f``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: (
            env.d.join(env.f, env.d.id == env.f.id).repartitionByRange(2, env.f.v).select(env.d.id)
        ),
        ["v"],
    )


def test_sj4_cube_f(spark: ReparkSession) -> None:
    """Cube key on the child refuses (``sj4_cube_f``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, env.d.id == env.f.id).cube(env.f.v).count(), ["v"])


def test_sj4_rollup_f(spark: ReparkSession) -> None:
    """Rollup key on the child refuses (``sj4_rollup_f``)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, env.d.id == env.f.id).rollup(env.f.v).count(), ["v"])


def test_sj4_dfagg_f(spark: ReparkSession) -> None:
    """DataFrame aggregation over the child refuses (``sj4_dfagg_f``)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).agg(F.sum(env.f.v).alias("s")), ["v"]
    )


def test_sj4_sort_mirror_f(spark: ReparkSession) -> None:
    """The sort alias refuses like orderBy (``A_inner_order_f`` mirror)."""
    env = _env(spark)
    _refuses_1182(
        lambda: env.d.join(env.f, env.d.id == env.f.id).sort(env.f.v.desc()).select(env.d.id),
        ["v"],
    )


def test_sj4_where_mirror_f(spark: ReparkSession) -> None:
    """The where alias refuses like filter (``A_inner_filter_f`` mirror)."""
    env = _env(spark)
    _refuses_1182(lambda: env.d.join(env.f, env.d.id == env.f.id).where(env.f.v > 25), ["v"])


def test_sj4_a_inner_sel_str_v(spark: ReparkSession) -> None:
    """String select over duplicate names stays ambiguous (``A_inner_sel_str_v``)."""
    env = _env(spark)
    _refuses_ambiguous(lambda: env.d.join(env.f, env.d.id == env.f.id).select("v"))


def test_sj4_a_inner_sel_fcol_v(spark: ReparkSession) -> None:
    """Free-name select over duplicate names stays ambiguous (``A_inner_sel_fcol_v``)."""
    env = _env(spark)
    _refuses_ambiguous(lambda: env.d.join(env.f, env.d.id == env.f.id).select(F.col("v")))


def test_sj4_a_joined_self_ref(spark: ReparkSession) -> None:
    """A ref bound on the joined frame stays ambiguous (``A_joined_self_ref``)."""
    env = _env(spark)
    _refuses_ambiguous(lambda: (lambda j: j.select(j.v))(env.d.join(env.f, env.d.id == env.f.id)))


def test_sj4_a_todf_sel_d(spark: ReparkSession) -> None:
    """ToDF-child select keeps its ambiguous class (``A_todf_sel_d``)."""
    env = _env(spark)
    _refuses_ambiguous(lambda: env.d.join(env.d.toDF("id", "v"), "id").select(env.d.v))


def test_sj4_a_alias_left_sel_a(spark: ReparkSession) -> None:
    """Aliased join select on the left frame answers (``A_alias_left_sel_a``)."""
    env = _env(spark)
    _answers(
        lambda: env.a.join(env.d, env.a.id == env.d.id).select(env.a.v),
        [[10], [20], [30]],
    )


def test_sj4_a_rev_left_sel_f(spark: ReparkSession) -> None:
    """Reversed join select on the left frame answers (``A_rev_left_sel_f``)."""
    env = _env(spark)
    _answers(lambda: env.f.join(env.d, env.f.id == env.d.id).select(env.f.v), [[20], [30]])


def test_sj4_a_wc_sel_d_id(spark: ReparkSession) -> None:
    """WithColumn-child join select on the parent id answers (``A_wc_sel_d_id``)."""
    env = _env(spark)
    _answers(lambda: env.d.join(env.w, env.d.id == env.w.id).select(env.d.id), [[2], [3]])


def test_sj4_a_wc_sel_w_id(spark: ReparkSession) -> None:
    """WithColumn-child join select on the child id answers (``A_wc_sel_w_id``)."""
    env = _env(spark)
    _answers(lambda: env.d.join(env.w, env.d.id == env.w.id).select(env.w.id), [[2], [3]])


def test_sj4_a_wcr_sel_d_v(spark: ReparkSession) -> None:
    """Renamed-child join select on the parent answers (``A_wcr_sel_d_v``)."""
    env = _env(spark)
    _answers(lambda: env.d.join(env.r, env.d.id == env.r.id).select(env.d.v), [[10], [20], [30]])


def test_sj4_a_wcr_sel_r_z(spark: ReparkSession) -> None:
    """Renamed-child join select on the renamed column answers (``A_wcr_sel_r_z``)."""
    env = _env(spark)
    _answers(lambda: env.d.join(env.r, env.d.id == env.r.id).select(env.r.z), [[10], [20], [30]])


def test_sj4_a_sel_only_id_left_sel_s_id(spark: ReparkSession) -> None:
    """Projected-child join select on the left id answers (``A_sel_only_id_left_sel_s_id``)."""
    env = _env(spark)
    _answers(lambda: env.s.join(env.d, env.s.id == env.d.id).select(env.s.id), [[1], [2], [3]])


def test_sj4_a_sibling_absent(spark: ReparkSession) -> None:
    """A ref from outside both join inputs answers (``A_sibling_absent``)."""
    env = _env(spark)
    _answers(lambda: env.d.join(env.f2, env.d.id == env.f2.id).select(env.f.v), [[30]])


def test_sj4_a_unrelated(spark: ReparkSession) -> None:
    """A join of unrelated frames answers (``A_unrelated``)."""
    env = _env(spark)
    _answers(
        lambda: env.d.join(env.e, env.d.id == env.e.id).select(env.e.w, env.d.v),
        [[100, 10], [200, 20]],
    )


def test_sj4_a_inner_count(spark: ReparkSession) -> None:
    """Count over a self-join answers (``A_inner_count``)."""
    env = _env(spark)
    assert env.d.join(env.f, env.d.id == env.f.id).count() == 2


def test_sj4_a_left_count(spark: ReparkSession) -> None:
    """Count over a left self-join answers (``A_left_count``)."""
    env = _env(spark)
    assert env.d.join(env.f, env.d.id == env.f.id, "left").count() == 3


def test_sj4_a_inner_drop_f(spark: ReparkSession) -> None:
    """Drop of a Column never checks (``A_inner_drop_f``)."""
    env = _env(spark)
    _answers(
        lambda: env.d.join(env.f, env.d.id == env.f.id).drop(env.f.v),
        [[2, 2, 20], [3, 3, 30]],
    )


def test_sj4_f_left_anti_using(spark: ReparkSession) -> None:
    """USING anti join answers (``F_left_anti_using``)."""
    env = _env(spark)
    _answers(lambda: env.d.join(env.f, "id", "left_anti"), [[1, 10]])


def test_sj4_f_left_anti_cond(spark: ReparkSession) -> None:
    """Condition anti join answers (``F_left_anti_cond``)."""
    env = _env(spark)
    _answers(lambda: env.d.join(env.f, env.d.id == env.f.id, "left_anti"), [[1, 10]])


def test_sj4_f_left_semi_sel_f(spark: ReparkSession) -> None:
    """Semi-join select on the excluded side answers from the left (``F_left_semi_sel_f``)."""
    env = _env(spark)
    _answers(
        lambda: env.d.join(env.f, env.d.id == env.f.id, "left_semi").select(env.f.v),
        [[20], [30]],
    )


def test_sj4_f_left_anti_sel_d(spark: ReparkSession) -> None:
    """Anti-join select on the kept side answers (``F_left_anti_sel_d``)."""
    env = _env(spark)
    _answers(lambda: env.d.join(env.f, env.d.id == env.f.id, "left_anti").select(env.d.v), [[10]])


def test_sj4_i_agg_then_sel_str(spark: ReparkSession) -> None:
    """String select past an aggregation answers (``I_agg_then_sel_str``)."""
    env = _env(spark)
    _answers(lambda: env.d.join(env.f, "id").groupBy("id").count().select(env.d.id), [[2], [3]])


def test_sj4_i_dropdup_f(spark: ReparkSession) -> None:
    """Drop-duplicates never checks (``I_dropdup_f``)."""
    env = _env(spark)
    assert env.d.join(env.f, env.d.id == env.f.id).dropDuplicates(["v"]).count() == 2


def test_sj4_i_fillna_f(spark: ReparkSession) -> None:
    """Fillna never checks (``I_fillna_f``)."""
    env = _env(spark)
    assert env.d.join(env.f, env.d.id == env.f.id, "left").fillna(0).count() == 3


def test_sj4_j_view_left_sel_t(spark: ReparkSession) -> None:
    """Reversed view join select on the left frame answers (``J_view_left_sel_t``)."""
    env = _view_env(spark)
    _answers(lambda: env.t.join(env.d, env.t.id == env.d.id).select(env.t.v), [[10], [20], [30]])


def test_sj4_k_on_union_second_input(spark: ReparkSession) -> None:
    """A ref from outside a union join answers (``K_on_union_second_input``)."""
    env = _env(spark)
    u = env.d.union(env.d.filter("id > 1"))
    _answers(
        lambda: env.d.join(u, env.d.id == u.id).select(env.f.v),
        [[10], [20], [20], [30], [30]],
    )


def test_sj4_h_off_left_sel(spark: ReparkSession) -> None:
    """Conf-off left-join id pair answers by id (``H_off_left_sel``)."""
    _off(spark)
    env = _env(spark)
    _answers(
        lambda: env.d.join(env.f, env.d.id == env.f.id, "left").select(env.d.id, env.f.id),
        [[1, 1], [2, 2], [3, 3]],
    )


def test_sj4_h_off_anti_idiom(spark: ReparkSession) -> None:
    """Conf-off anti-join idiom answers empty (``H_off_anti_idiom``)."""
    _off(spark)
    env = _env(spark)
    _answers(
        lambda: (
            env.d.join(env.f, env.d.id == env.f.id, "left")
            .where(env.f.id.isNull())
            .select(env.d.id)
        ),
        [],
    )


def test_sj4_h_off_alias_gt_id_v(spark: ReparkSession) -> None:
    """Conf-off aliased non-equi join answers empty (``H_off_alias_gt_id_v``)."""
    _off(spark)
    env = _env(spark)
    _answers(lambda: env.a.join(env.d, env.d.id > env.a.v), [])


def test_sj4_h_off_lt_rev(spark: ReparkSession) -> None:
    """Conf-off reversed non-equi join answers empty (``H_off_lt_rev``)."""
    _off(spark)
    env = _env(spark)
    _answers(lambda: env.d.join(env.f, env.f.id < env.d.id), [])


def test_sj4_h_off_wc_sel_w_v(spark: ReparkSession) -> None:
    """Conf-off withColumn-child select answers (``H_off_wc_sel_w_v``)."""
    _off(spark)
    env = _env(spark)
    _answers(lambda: env.d.join(env.w, env.d.id == env.w.id).select(env.w.v), [[20], [30]])


def test_sj4_h_off_single_left_tok(spark: ReparkSession) -> None:
    """Conf-off single-token condition counts (``H_off_single_left_tok``)."""
    _off(spark)
    env = _env(spark)
    assert env.d.join(env.f, env.d.id > 1).count() == 4


def test_sj4_h_off_inner_sel_f_v(spark: ReparkSession) -> None:
    """Conf-off inner select answers by id (``H_off_inner_sel_f_v``)."""
    _off(spark)
    env = _env(spark)
    _answers(lambda: env.d.join(env.f, env.d.id == env.f.id).select(env.f.v), [[20], [30]])


def test_sj4_h_off_self_eq(spark: ReparkSession) -> None:
    """Conf-off self equi-join counts (``H_off_self_eq``)."""
    _off(spark)
    env = _env(spark)
    assert env.d.join(env.d, env.d.id == env.d.id).count() == 3


def test_sj4_h_off_self_eq_and_gt(spark: ReparkSession) -> None:
    """Conf-off self-join with conjunct answers (``H_off_self_eq_and_gt``)."""
    _off(spark)
    env = _env(spark)
    _answers(
        lambda: env.d.join(env.d, (env.d.id == env.d.id) & (env.d.v > 15)),
        [[2, 20, 2, 20], [3, 30, 3, 30]],
    )


def test_sj4_h_off_using_sel_f(spark: ReparkSession) -> None:
    """Conf-off USING select keeps its pre-existing class (``H_off_using_sel_f``).

    Spark answers by id; the downstream binder stays ambiguous since before
    SJ-1. The funnel must not fire here.
    """
    _off(spark)
    env = _env(spark)
    _refuses_ambiguous(lambda: env.d.join(env.f, "id").select(env.f.v))


def test_sj4_h_off_xj_sel_parent(spark: ReparkSession) -> None:
    """Conf-off crossJoin select keeps its pre-existing class (``H_off_xj_sel_parent``).

    Spark answers; the downstream binder stays ambiguous since before SJ-1.
    The funnel must not fire here.
    """
    _off(spark)
    env = _env(spark)
    _refuses_ambiguous(lambda: env.d.crossJoin(env.d).select(env.d.v))


@pytest.mark.xfail(strict=True, reason="V-3 missing-reference resolution lands in SJ-5")
def test_sj4_v1_replaced_parent_filter(spark: ReparkSession) -> None:
    """Filter past a replaced column answers on the parent attribute (V-1)."""
    env = _env(spark)
    _answers(lambda: env.d.withColumn("v", -env.d.v).filter(env.d.v > 15), [[2, -20], [3, -30]])


@pytest.mark.xfail(strict=True, reason="V-3 missing-reference resolution lands in SJ-5")
def test_sj4_v1_swapped_alias_parent_select(spark: ReparkSession) -> None:
    """Select past swapped aliases misses the parent attribute (V-1)."""
    env = _env(spark)
    _refuses_missing(
        lambda: env.d.select(env.d.v.alias("id"), env.d.id.alias("v")).select(env.d.v),
        _MISSING_OP,
        "v",
    )
