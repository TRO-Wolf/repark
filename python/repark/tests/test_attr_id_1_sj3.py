from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkAttributeError

_FAIL_KEY = "spark.sql.analyzer.failAmbiguousSelfJoin"
_AUTO_KEY = "spark.sql.selfJoinAutoResolveAmbiguity"


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-attr-id-1-sj3").getOrCreate()


@pytest.fixture(params=[False, True], ids=["insensitive", "sensitive"])
def ruled_spark(spark: ReparkSession, request: Any) -> ReparkSession:
    spark.conf.set("spark.sql.caseSensitive", "true" if request.param else "false")
    return spark


def _d(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "v"])


def _e(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 100), (2, 200)], ["id", "w"])


def _xy(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 2), (2, 1), (3, 3)], ["x", "y"])


def _xyz(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["x", "y"])


def _ab(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 2), (2, 1), (3, 3)], ["a", "b"])


def _p(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 2), (2, 3), (3, 1)], ["id", "v"])


def _refuses_1182(thunk: Callable[[], Any], names: list[str]) -> None:
    with pytest.raises(AnalysisException) as refused:
        thunk().collect()
    assert refused.value.getCondition() == "_LEGACY_ERROR_TEMP_1182"
    params = refused.value.getMessageParameters()
    assert params["config"] == _FAIL_KEY
    assert params["ambiguousAttrs"] == ", ".join(names)


def _refuses(thunk: Callable[[], Any], cond: str) -> AnalysisException:
    with pytest.raises(AnalysisException) as refused:
        thunk().collect()
    assert refused.value.getCondition() == cond
    return refused.value


def test_sj3_b_alias_gt_id_v(ruled_spark: ReparkSession) -> None:
    """``a.join(d, d.id > a.v)`` refuses 1182 (``B_alias_gt_id_v``)."""
    spark = ruled_spark
    d = _d(spark)
    a = d.alias("a")
    _refuses_1182(lambda: a.join(d, d.id > a.v), ["id"])


def test_sj3_b_single_left_tok(ruled_spark: ReparkSession) -> None:
    """``d.join(f, d.id > 1)`` refuses 1182 (``B_single_left_tok``)."""
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    _refuses_1182(lambda: d.join(f, d.id > 1), ["id"])


def test_sj3_b_single_right_tok(ruled_spark: ReparkSession) -> None:
    """``d.join(f, f.v > 15)`` refuses 1182 (``B_single_right_tok``)."""
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    _refuses_1182(lambda: d.join(f, f.v > 15), ["v"])


def test_sj3_b_derived_eq_lit(ruled_spark: ReparkSession) -> None:
    """``d.join(f, d.id == 1)`` refuses 1182 (``B_derived_eq_lit``)."""
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    _refuses_1182(lambda: d.join(f, d.id == 1), ["id"])


def test_sj3_b_self_gt_lit(ruled_spark: ReparkSession) -> None:
    """``d.join(d, d.id > 1)`` refuses 1182 (``B_self_gt_lit``)."""
    d = _d(ruled_spark)
    _refuses_1182(lambda: d.join(d, d.id > 1), ["id"])


def test_sj3_b_lt_rev(ruled_spark: ReparkSession) -> None:
    """``d.join(f, f.id < d.id)`` refuses 1182 (``B_lt_rev``)."""
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    _refuses_1182(lambda: d.join(f, f.id < d.id), ["id", "id"])


def test_sj3_d_self_eq_and_gt(ruled_spark: ReparkSession) -> None:
    """Exempt equi plus a range arm refuses 1182 (``D_self_eq_and_gt``)."""
    d = _d(ruled_spark)
    _refuses_1182(lambda: d.join(d, (d.id == d.id) & (d.v > 15)), ["v"])


def test_sj3_d_self_id_eq_v(ruled_spark: ReparkSession) -> None:
    """``d.join(d, d.id == d.v)`` refuses 1182 (``D_self_id_eq_v``)."""
    d = _d(ruled_spark)
    _refuses_1182(lambda: d.join(d, d.id == d.v), ["id", "v"])


def test_sj3_d_derived_eq_plus0(ruled_spark: ReparkSession) -> None:
    """``d.join(f, d.id == f.id + 0)`` refuses 1182 (``D_derived_eq_plus0``)."""
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    _refuses_1182(lambda: d.join(f, d.id == f.id + 0), ["id", "id"])


def test_sj3_e_parent_alias_gt(ruled_spark: ReparkSession) -> None:
    """``a.join(b, a.id > b.id)`` refuses 1182 (``E_parent_alias_gt``)."""
    d = _d(ruled_spark)
    a = d.alias("a")
    b = d.alias("b")
    _refuses_1182(lambda: a.join(b, a.id > b.id), ["id"])


def test_sj3_g_eq3_cross_tok(ruled_spark: ReparkSession) -> None:
    """A mid-frame token in a three-way join refuses 1182 (``G_eq3_cross_tok``)."""
    spark = ruled_spark
    d = _d(spark)
    e = _e(spark)
    f = d.filter("id > 1")
    _refuses_1182(lambda: d.join(e, d.id == e.id).join(f, e.id == f.id), ["id"])


def test_sj3_f_left_anti_gt(ruled_spark: ReparkSession) -> None:
    """An anti condition is still checked (``F_left_anti_gt``)."""
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    _refuses_1182(lambda: d.join(f, d.id > f.id, "left_anti"), ["id", "id"])


def test_sj3_p_v_single_shared_token(ruled_spark: ReparkSession) -> None:
    """``a.join(d, d.id > a.v)`` refuses 1182 (``P_v_single_shared_token``)."""
    p = _p(ruled_spark)
    a = p.alias("a")
    _refuses_1182(lambda: a.join(p, p.id > a.v), ["id"])


def test_sj3_b_self_eq_lit(ruled_spark: ReparkSession) -> None:
    """``d.join(d, d.id == 1)`` answers 3 (``B_self_eq_lit``)."""
    d = _d(ruled_spark)
    assert d.join(d, d.id == 1).count() == 3


def test_sj3_b_unrelated_single(ruled_spark: ReparkSession) -> None:
    """``d.join(e, d.id > 1)`` answers 4 (``B_unrelated_single``)."""
    spark = ruled_spark
    d = _d(spark)
    assert d.join(_e(spark), d.id > 1).count() == 4


def test_sj3_d_self_eq(ruled_spark: ReparkSession) -> None:
    """The trivial equi rewrite answers the diagonal (``D_self_eq``)."""
    d = _d(ruled_spark)
    assert sorted(tuple(row) for row in d.join(d, d.id == d.id).collect()) == [
        (1, 10, 1, 10),
        (2, 20, 2, 20),
        (3, 30, 3, 30),
    ]


def test_sj3_d_self_eq_left(ruled_spark: ReparkSession) -> None:
    """``d.join(d, d.id == d.id, 'left')`` answers 3 (``D_self_eq_left``)."""
    d = _d(ruled_spark)
    assert d.join(d, d.id == d.id, "left").count() == 3


def test_sj3_d_self_eqns(ruled_spark: ReparkSession) -> None:
    """Null-safe self-equi answers 3 (``D_self_eqns``)."""
    d = _d(ruled_spark)
    assert d.join(d, d.id.eqNullSafe(d.id)).count() == 3


def test_sj3_d_self_eq_cast(ruled_spark: ReparkSession) -> None:
    """Cast-wrapped self-equi stays trivially true: 9 (``D_self_eq_cast``)."""
    d = _d(ruled_spark)
    assert d.join(d, d.id.cast("string") == d.id.cast("string")).count() == 9


def test_sj3_d_self_eq_and_eq(ruled_spark: ReparkSession) -> None:
    """AND of exempt equis answers 3 (``D_self_eq_and_eq``)."""
    d = _d(ruled_spark)
    assert d.join(d, (d.id == d.id) & (d.v == d.v)).count() == 3


def test_sj3_d_self_eq_or_eq(ruled_spark: ReparkSession) -> None:
    """OR of exempt equis answers 3 (``D_self_eq_or_eq``)."""
    d = _d(ruled_spark)
    assert d.join(d, (d.id == d.id) | (d.v == d.v)).count() == 3


def test_sj3_d_derived_eq(ruled_spark: ReparkSession) -> None:
    """``d.join(f, d.id == f.id)`` answers two rows (``D_derived_eq``)."""
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    assert sorted(tuple(row) for row in d.join(f, d.id == f.id).collect()) == [
        (2, 20, 2, 20),
        (3, 30, 3, 30),
    ]


def test_sj3_d_derived_eq_rev(ruled_spark: ReparkSession) -> None:
    """Operands go left-first whatever their frames (``D_derived_eq_rev``)."""
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    assert sorted(tuple(row) for row in d.join(f, f.id == d.id, "left").collect()) == [
        (1, 10, None, None),
        (2, 20, 2, 20),
        (3, 30, 3, 30),
    ]


def test_sj3_d_wc_eq_v(ruled_spark: ReparkSession) -> None:
    """Equi over a replaced column answers shifted rows (``D_wc_eq_v``)."""
    d = _d(ruled_spark)
    w = d.withColumn("id", d.id + 1)
    assert sorted(tuple(row) for row in d.join(w, d.v == w.v).collect()) == [
        (1, 10, 2, 10),
        (2, 20, 3, 20),
        (3, 30, 4, 30),
    ]


def test_sj3_d_self_eq_noauto(ruled_spark: ReparkSession) -> None:
    """With auto-resolve off the equi is trivially true: 9 (``D_self_eq_noauto``)."""
    ruled_spark.conf.set(_AUTO_KEY, "false")
    d = _d(ruled_spark)
    assert d.join(d, d.id == d.id).count() == 9


def test_sj3_d_derived_eq_noauto(ruled_spark: ReparkSession) -> None:
    """With auto-resolve off the derived equi gives 6 (``D_derived_eq_noauto``)."""
    ruled_spark.conf.set(_AUTO_KEY, "false")
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    assert d.join(f, d.id == f.id).count() == 6


def test_sj3_e_parent_alias_eq(ruled_spark: ReparkSession) -> None:
    """``a.join(b, a.id == b.id)`` answers paired rows (``E_parent_alias_eq``)."""
    d = _d(ruled_spark)
    a = d.alias("a")
    b = d.alias("b")
    assert sorted(tuple(row) for row in a.join(b, a.id == b.id).select("a.v", "b.v").collect()) == [
        (10, 10),
        (20, 20),
        (30, 30),
    ]


def test_sj3_g_eq3_count(ruled_spark: ReparkSession) -> None:
    """The rewrite resolves by name: ambiguous ``id`` (``G_eq3_count``)."""
    spark = ruled_spark
    d = _d(spark)
    f = d.filter("id > 1")
    g = d.filter("id < 3")
    refused = _refuses(lambda: d.join(f, d.id == f.id).join(g, d.id == g.id), "AMBIGUOUS_REFERENCE")
    assert "Reference `id` is ambiguous, could be: [`id`, `id`]" in str(refused)


def test_sj3_g_eq3_unrel_mid(ruled_spark: ReparkSession) -> None:
    """Unrelated middle frame still rewrites ambiguous (``G_eq3_unrel_mid``)."""
    spark = ruled_spark
    d = _d(spark)
    e = _e(spark)
    f = d.filter("id > 1")
    refused = _refuses(lambda: d.join(e, d.id == e.id).join(f, d.id == f.id), "AMBIGUOUS_REFERENCE")
    assert "Reference `id` is ambiguous, could be: [`id`, `id`]" in str(refused)


def test_sj3_g_eq3_unrel_mid_sel_e(ruled_spark: ReparkSession) -> None:
    """The ambiguous rewrite refuses before any select (``G_eq3_unrel_mid_sel_e``)."""
    spark = ruled_spark
    d = _d(spark)
    e = _e(spark)
    f = d.filter("id > 1")
    refused = _refuses(
        lambda: d.join(e, d.id == e.id).join(f, d.id == f.id).select(e.w),
        "AMBIGUOUS_REFERENCE",
    )
    assert "Reference `id` is ambiguous, could be: [`id`, `id`]" in str(refused)


def test_sj3_g_eq3_e_first(ruled_spark: ReparkSession) -> None:
    """Unrelated first frame still rewrites ambiguous (``G_eq3_e_first``)."""
    spark = ruled_spark
    d = _d(spark)
    e = _e(spark)
    f = d.filter("id > 1")
    refused = _refuses(lambda: e.join(d, e.id == d.id).join(f, d.id == f.id), "AMBIGUOUS_REFERENCE")
    assert "Reference `id` is ambiguous, could be: [`id`, `id`]" in str(refused)


def test_sj3_i_rewrite_name_missing(ruled_spark: ReparkSession) -> None:
    """A rewritten name missing on one side is unresolved (``I_rewrite_name_missing``)."""
    d = _d(ruled_spark)
    r = d.withColumnRenamed("v", "z")
    refused = _refuses(lambda: d.join(r, d.v == d.v), "UNRESOLVED_COLUMN.WITH_SUGGESTION")
    assert "name `v` cannot be resolved" in str(refused)
    assert "[`id`, `z`]" in str(refused)


def test_sj3_p_s4_third_frame(ruled_spark: ReparkSession) -> None:
    """A third frame's id is missing but appears (``P_s4_third_frame``)."""
    spark = ruled_spark
    k1 = spark.createDataFrame([(1,)], ["k"])
    k2 = spark.createDataFrame([(1,)], ["k"])
    k3 = spark.createDataFrame([(1,)], ["k"])
    refused = _refuses(
        lambda: k1.join(k2, k3.k == 1), "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION"
    )
    assert '"k"' in str(refused)
    assert 'missing from "k", "k"' in str(refused)


def test_sj3_k_off_cond_missing(ruled_spark: ReparkSession) -> None:
    """A name on no side is missing from the input (``K_off_cond_missing``)."""
    ruled_spark.conf.set(_FAIL_KEY, "false")
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    other = ruled_spark.createDataFrame([(1,)], ["k"])
    refused = _refuses(
        lambda: d.join(f, d.id == other.k),
        "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT",
    )
    assert '"k"' in str(refused)
    assert 'missing from "id", "v", "id", "v"' in str(refused)


def test_sj3_i_rewrite_case_getattr_refuses(ruled_spark: ReparkSession) -> None:
    """``d.ID`` raises at access under both rules (``I_rewrite_case``)."""
    d = _d(ruled_spark)
    with pytest.raises(PySparkAttributeError, match=r"ATTRIBUTE_NOT_SUPPORTED"):
        _ = d.ID == d.id  # noqa: SIM300 — probe-cell order; the raising access leads


def test_sj3_i_rewrite_case_getitem(ruled_spark: ReparkSession) -> None:
    """``d["ID"]`` follows the session rule (``I_rewrite_case`` getitem half)."""
    spark = ruled_spark
    d = _d(spark)
    if spark.conf.get("spark.sql.caseSensitive") == "true":
        with pytest.raises(AnalysisException) as refused:
            _ = d.join(d, d["ID"] == d.id).count()
        assert refused.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    else:
        assert d.join(d, d["ID"] == d.id).count() == 3


def test_sj3_h_off_lt_rev(spark: ReparkSession) -> None:
    """Conf off: the reversed range binds left and empties (``H_off_lt_rev``)."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set(_FAIL_KEY, "false")
    d = _d(spark)
    f = d.filter("id > 1")
    joined = d.join(f, f.id < d.id)
    assert joined.columns == ["id", "v", "id", "v"]
    assert joined.collect() == []


def test_sj3_h_off_alias_gt_id_v(spark: ReparkSession) -> None:
    """Conf off: the aliased range binds left and empties (``H_off_alias_gt_id_v``)."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set(_FAIL_KEY, "false")
    d = _d(spark)
    a = d.alias("a")
    joined = a.join(d, d.id > a.v)
    assert joined.columns == ["id", "v", "id", "v"]
    assert joined.collect() == []


def test_sj3_h_off_single_left_tok(spark: ReparkSession) -> None:
    """Conf off: the single token binds left: 4 (``H_off_single_left_tok``)."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set(_FAIL_KEY, "false")
    d = _d(spark)
    f = d.filter("id > 1")
    assert d.join(f, d.id > 1).count() == 4


def test_sj3_h_off_self_eq(spark: ReparkSession) -> None:
    """Conf off: the rewrite still applies: 3 (``H_off_self_eq``)."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set(_FAIL_KEY, "false")
    d = _d(spark)
    assert d.join(d, d.id == d.id).count() == 3


def test_sj3_h_off_self_eq_and_gt(spark: ReparkSession) -> None:
    """Conf off: exempt equi plus left-bound range (``H_off_self_eq_and_gt``)."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set(_FAIL_KEY, "false")
    d = _d(spark)
    assert sorted(tuple(row) for row in d.join(d, (d.id == d.id) & (d.v > 15)).collect()) == [
        (2, 20, 2, 20),
        (3, 30, 3, 30),
    ]


def test_sj3_p_s4_filter_lineage_simple(ruled_spark: ReparkSession) -> None:
    """Filter-lineage equi rewrites and answers (``P_s4_filter_lineage_simple``)."""
    frame = _xy(ruled_spark)
    child = frame.filter(frame.y > 1)
    assert sorted(tuple(row) for row in frame.join(child, frame.x == child.x).collect()) == [
        (1, 2, 1, 2),
        (3, 3, 3, 3),
    ]


def test_sj3_p_h2_equi_count(ruled_spark: ReparkSession) -> None:
    """Same-object single-column equi answers paired rows (``P_h2_equi_count``)."""
    frame = ruled_spark.createDataFrame([(1,), (2,), (3,)], ["x"])
    assert sorted(tuple(row) for row in frame.join(frame, frame.x == frame.x).collect()) == [
        (1, 1),
        (2, 2),
        (3, 3),
    ]


@pytest.mark.xfail(strict=True, reason="SJ-4 wires the post-join refusal funnel")
def test_sj3_d_self_eq_sel_d(ruled_spark: ReparkSession) -> None:
    """Post-join parent select refuses 1182 (``D_self_eq_sel_d``; SJ-4)."""
    d = _d(ruled_spark)
    _refuses_1182(lambda: d.join(d, d.id == d.id).select(d.id), ["id"])


@pytest.mark.xfail(strict=True, reason="SJ-4 wires the post-join refusal funnel")
def test_sj3_p_v_left_join_right_parent(ruled_spark: ReparkSession) -> None:
    """Right-parent select after a left join refuses (``P_v_left_join_right_parent``; SJ-4)."""
    ruled_spark.conf.set("spark.sql.caseSensitive", "false")
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    _refuses_1182(lambda: d.join(f, d.id == f.id, "left").select(d.id, f.id), ["id", "id"])


@pytest.mark.xfail(strict=True, reason="SJ-4 wires the post-join refusal funnel")
def test_sj3_p_v_anti_idiom(ruled_spark: ReparkSession) -> None:
    """The anti-join idiom refuses at the parent select (``P_v_anti_idiom``; SJ-4)."""
    ruled_spark.conf.set("spark.sql.caseSensitive", "false")
    d = _d(ruled_spark)
    f = d.filter("id > 1")
    _refuses_1182(lambda: d.join(f, d.id == f.id, "left").where(f.id.isNull()).select(d.id), ["id"])
