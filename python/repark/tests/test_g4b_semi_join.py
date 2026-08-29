"""G4b — DataFrame-API ``leftsemi`` / ``leftanti`` binding: the non-differential surface.

The Spark-parity rows for this widening live in ``test_join_parity.py``. This module holds the
surface that is NOT a differential row:

1. Accepted spellings: the semi/anti family folds case- and underscore-insensitively; each
   spelling is its own alias-map key, so each gets an input.
2. A conditionless semi/anti join is a declared DIVERGENCE: live Spark runs it (every left row
   kept iff the right side is non-empty), repark refuses loud — the Cartesian fallback would
   answer a different result set.
3. An unknown ``how`` must advertise the semi family.
4. The origin map: after a semi/anti join the right side contributes no columns, so
   ``select``/``filter``/``withColumn`` of a right-parent Column raise ``MISSING_ATTRIBUTES``
   and ``drop`` of that Column is a Spark 4.1.2 no-op. ``_spawn`` copies the not-emitted set;
   a later emitting join of the same right subtracts those ids.

These are repark-only assertions (a refusal has no Spark golden), which is why they are not
corpus rows.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark.errors import AnalysisException

if TYPE_CHECKING:
    from collections.abc import Iterator

SEMI_SPELLINGS = ("semi", "left_semi", "leftsemi", "LeftSemi", "LEFT_SEMI")
ANTI_SPELLINGS = ("anti", "left_anti", "leftanti", "LeftAnti", "LEFT_ANTI")

# Live PySpark 4.1.2 oracle: MISSING_ATTRIBUTES classes, not UNRESOLVED_COLUMN.
_MISSING_APPEAR = "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION"
_MISSING_ABSENT = "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT"


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("test-g4b-semi-join").getOrCreate()
    yield session
    session.stop()


def _pair(session: ReparkSession) -> tuple[object, object]:
    """Left ``(k, a)`` with a matched, an unmatched and a NULL key; right ``(k)`` matching k=1."""
    left = session.createDataFrame([(1, "a"), (2, "b"), (None, "n")], ["k", "a"])
    right = session.createDataFrame([(1,), (9,)], ["k"])
    return left, right


@pytest.mark.parametrize("how", SEMI_SPELLINGS)
def test_every_semi_spelling_reaches_the_same_left_semi_binding(
    spark: ReparkSession, how: str
) -> None:
    """Each accepted ``leftsemi`` spelling is its own alias-map key, so each gets an input."""
    left, right = _pair(spark)
    table = left.join(right, on="k", how=how).to_arrow()
    assert table.column_names == ["k", "a"], "semi output is the LEFT schema only"
    assert table.to_pydict() == {"k": [1], "a": ["a"]}


@pytest.mark.parametrize("how", ANTI_SPELLINGS)
def test_every_anti_spelling_reaches_the_same_left_anti_binding(
    spark: ReparkSession, how: str
) -> None:
    """The anti complement of the spelling battery - an all-pass bug reds one of the two."""
    left, right = _pair(spark)
    table = left.join(right, on="k", how=how).to_arrow()
    assert table.column_names == ["k", "a"]
    # k=2 is unmatched and the NULL key never matches, so both survive; k=1 does not.
    assert sorted(table.to_pydict()["a"]) == ["b", "n"]


def test_semi_join_result_is_still_a_usable_frame(spark: ReparkSession) -> None:
    """The semi output is a normal frame: projectable, filterable, countable (not a dead handle)."""
    left, right = _pair(spark)
    joined = left.join(right, on="k", how="leftsemi")
    assert joined.columns == ["k", "a"]
    assert joined.count() == 1
    assert joined.select("a").to_arrow().to_pydict() == {"a": ["a"]}
    assert joined.filter("k = 1").count() == 1


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize("on", [None, []], ids=["on_none", "on_empty_list"])
def test_conditionless_semi_family_refuses_loud(
    spark: ReparkSession, how: str, on: list[str] | None
) -> None:
    """DIVERGENCE (declared): a conditionless semi/anti join refuses instead of cross-joining.

    Live Spark 4.1.2 runs it (``on=None`` keeps every left row iff the right side is non-empty;
    ``on=[]`` raises); the facade's Cartesian path would silently return an m*n cross join — the
    wrong-row failure mode this refusal exists to prevent.
    """
    left, right = _pair(spark)
    with pytest.raises(AnalysisException) as excinfo:
        left.join(right, on, how)
    message = str(excinfo.value)
    assert "requires an `on` condition" in message
    assert "not a Cartesian product" in message


def test_conditionless_refusal_does_not_leak_into_other_join_types(
    spark: ReparkSession,
) -> None:
    """The guard is semi-family only: ``how='inner'`` with no ``on`` still takes the cross path."""
    left, right = _pair(spark)
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    crossed = left.join(right, None, "inner")
    assert crossed.count() == 6, "3 left rows x 2 right rows - the Cartesian path is untouched"


def test_unsupported_join_type_message_advertises_the_semi_family(spark: ReparkSession) -> None:
    """An unknown ``how`` lists what IS supported; the list must not lag the alias map."""
    left, right = _pair(spark)
    with pytest.raises(AnalysisException) as excinfo:
        left.join(right, on="k", how="bogus")
    message = str(excinfo.value)
    assert "Unsupported join type 'bogus'" in message
    advertised_types = (
        "'semi'",
        "'leftsemi'",
        "'left_semi'",
        "'anti'",
        "'leftanti'",
        "'left_anti'",
    )
    for advertised in advertised_types:
        assert advertised in message, f"refusal message must advertise {advertised}: {message}"


def _semi_family_join(
    session: ReparkSession, how: str, on_mode: str
) -> tuple[object, object, object]:
    """Left ``(k, a)`` / right ``(k)`` joined by name or Column condition."""
    left, right = _pair(session)
    if on_mode == "name":
        return left, right, left.join(right, on="k", how=how)
    if on_mode == "name_list":
        return left, right, left.join(right, on=["k"], how=how)
    return left, right, left.join(right, left["k"] == right["k"], how)


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize("on_mode", ["name", "name_list", "condition"])
def test_right_ref_select_raises_missing_attributes_same_key(
    spark: ReparkSession, how: str, on_mode: str
) -> None:
    """``select(right["k"])`` after semi/anti raises the same-name MISSING_ATTRIBUTES class
    (live Spark 4.1.2), not ``UNRESOLVED_COLUMN``."""
    _left, right, joined = _semi_family_join(spark, how, on_mode)
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR) as excinfo:
        joined.select(right["k"])
    assert 'Resolved attribute(s) "k"' in str(excinfo.value)


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize("on_mode", ["name", "condition"])
def test_right_ref_filter_raises_missing_attributes_same_key(
    spark: ReparkSession, how: str, on_mode: str
) -> None:
    """``filter(right["k"] == 1)`` reaches the same origin map as select (not select-only)."""
    _left, right, joined = _semi_family_join(spark, how, on_mode)
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR):
        joined.filter(right["k"] == 1)


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize("on_mode", ["name", "condition"])
def test_right_ref_with_column_raises_missing_attributes_same_key(
    spark: ReparkSession, how: str, on_mode: str
) -> None:
    """``withColumn("x", right["k"])`` reaches the same origin map as select."""
    _left, right, joined = _semi_family_join(spark, how, on_mode)
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR):
        joined.withColumn("x", right["k"])


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize("on_mode", ["name", "condition"])
def test_right_ref_drop_is_spark_noop(spark: ReparkSession, how: str, on_mode: str) -> None:
    """``drop(right["k"])`` after semi/anti is a Spark 4.1.2 no-op: no raise, and the same-name
    LEFT ``k`` is kept."""
    _left, right, joined = _semi_family_join(spark, how, on_mode)
    dropped = joined.drop(right["k"])
    assert dropped.columns == ["k", "a"]
    if how == "leftsemi":
        assert dropped.count() == 1
    else:
        assert dropped.count() == 2


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize("on_mode", ["name", "condition"])
def test_left_refs_still_resolve_after_semi_family(
    spark: ReparkSession, how: str, on_mode: str
) -> None:
    """Left-parent Columns and bare names still bind after semi/anti (the output IS left)."""
    left, _right, joined = _semi_family_join(spark, how, on_mode)
    assert joined.select(left["k"]).columns == ["k"]
    assert joined.filter(left["k"] == 1).count() == (1 if how == "leftsemi" else 0)
    assert joined.drop(left["a"]).columns == ["k"]
    assert joined.select("k").columns == ["k"]


@pytest.mark.parametrize("how", ["inner"])
@pytest.mark.parametrize("on_mode", ["name", "condition"])
def test_inner_join_right_ref_still_resolves(spark: ReparkSession, how: str, on_mode: str) -> None:
    """Regression guard: inner-join origin resolution is unchanged by the semi-family map."""
    _left, right, joined = _semi_family_join(spark, how, on_mode)
    table = joined.select(right["k"]).to_arrow()
    assert table.column_names == ["k"]
    assert table.to_pydict()["k"] == [1]
    assert joined.filter(right["k"] == 1).count() == 1


@pytest.mark.parametrize("on_mode", ["name", "name_list", "condition"])
def test_semi_then_inner_join_emits_the_same_right(spark: ReparkSession, on_mode: str) -> None:
    """A later inner join of the same right emits it; ``select(right["k"])`` resolves.

    Without the emitting-join subtract of the copied not-emitted set, the select would still
    raise after the right side is in the output.
    """
    _left, right, semi = _semi_family_join(spark, "leftsemi", on_mode)
    if on_mode == "condition":
        joined = semi.join(right, semi["k"] == right["k"], "inner")
    elif on_mode == "name_list":
        joined = semi.join(right, on=["k"], how="inner")
    else:
        joined = semi.join(right, on="k", how="inner")
    table = joined.select(right["k"]).to_arrow()
    assert table.column_names == ["k"]
    assert table.to_pydict()["k"] == [1]
    assert joined.filter(right["k"] == 1).count() == 1
    # Subtract that right, not clear the whole set: another unemitted origin must still raise.
    third = spark.createDataFrame([(1,)], ["k"])
    other_inner = semi.join(third, on="k", how="inner")
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR):
        other_inner.select(right["k"])


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize("on_mode", ["name", "condition"])
def test_spawn_descendant_still_refuses_unemitted_right(
    spark: ReparkSession, how: str, on_mode: str
) -> None:
    """``_spawn`` copies the not-emitted set; descendants must not name-fall-back to the left."""
    left, right, joined = _semi_family_join(spark, how, on_mode)
    filtered = joined.filter(left["k"].isNotNull())
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR):
        filtered.select(right["k"])
    projected = joined.select("k", "a")
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR):
        projected.select(right["k"])
    assert filtered.drop(right["k"]).columns == ["k", "a"]


@pytest.mark.parametrize("on_mode", ["name", "condition"])
def test_self_semi_exclusive_set_resolves_df_column(spark: ReparkSession, on_mode: str) -> None:
    """``df.join(df, …, "leftsemi").select(df["k"])`` still works.

    Exclusive-set: the right-minus-left plan-id set is empty, so the output *is* that origin;
    a sloppy "all non-self plan ids" remember would refuse it.
    """
    frame = spark.createDataFrame([(1, "a"), (2, "b")], ["k", "a"])
    if on_mode == "name":
        joined = frame.join(frame, on="k", how="leftsemi")
    else:
        joined = frame.join(frame, frame["k"] == frame["k"], "leftsemi")
    table = joined.select(frame["k"]).to_arrow()
    assert table.column_names == ["k"]
    assert sorted(table.to_pydict()["k"]) == [1, 2]


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
def test_distinct_name_right_ref_raises_missing_from_input(spark: ReparkSession, how: str) -> None:
    """Right-only name (``rk``) after a condition semi/anti uses the MISSING_FROM_INPUT class.

    Live Spark 4.1.2 switches subclass when the missing attribute's spelling is not in the
    output; no invented ``UNRESOLVED_COLUMN``.
    """
    left = spark.createDataFrame([(1, "a"), (2, "b")], ["k", "a"])
    right = spark.createDataFrame([(1, "x"), (9, "y")], ["rk", "v"])
    joined = left.join(right, left["k"] == right["rk"], how)
    with pytest.raises(AnalysisException, match=_MISSING_ABSENT) as excinfo:
        joined.select(right["rk"])
    message = str(excinfo.value)
    assert 'Resolved attribute(s) "rk"' in message
    with pytest.raises(AnalysisException, match=_MISSING_ABSENT):
        joined.filter(right["v"] == "x")
    with pytest.raises(AnalysisException, match=_MISSING_ABSENT):
        joined.withColumn("x", right["rk"])
    # drop of a distinct-name right origin is still a Spark no-op.
    assert joined.drop(right["rk"]).columns == ["k", "a"]
    assert joined.select(left["k"]).columns == ["k"]


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize("on_mode", ["name", "name_list", "condition"])
def test_right_ref_abs_raises_missing_attributes_same_key(
    spark: ReparkSession, how: str, on_mode: str
) -> None:
    """``F.abs(right["k"])`` after semi/anti raises the same-name MISSING_ATTRIBUTES class."""
    _left, right, joined = _semi_family_join(spark, how, on_mode)
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR) as excinfo:
        joined.select(F.abs(right["k"]))
    assert 'Resolved attribute(s) "k"' in str(excinfo.value)
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR):
        joined.filter(F.abs(right["k"]) == 1)
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR):
        joined.withColumn("x", F.abs(right["k"]))


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize("on_mode", ["name", "condition"])
def test_left_abs_still_resolves_after_semi_family(
    spark: ReparkSession, how: str, on_mode: str
) -> None:
    """``F.abs(left["k"])`` is the output column — origin thread must not refuse left."""
    left, _right, joined = _semi_family_join(spark, how, on_mode)
    table = joined.select(F.abs(left["k"]).alias("ak")).to_arrow()
    assert table.column_names == ["ak"]
    values = table.to_pydict()["ak"]
    if how == "leftsemi":
        assert values == [1]
    else:
        # leftanti keeps k=2 and the NULL key; abs(NULL) stays NULL.
        assert sorted(value for value in values if value is not None) == [2]
        assert None in values


@pytest.mark.parametrize("on_mode", ["name", "condition"])
def test_inner_join_abs_right_ref_still_resolves(spark: ReparkSession, on_mode: str) -> None:
    """Regression: origin-thread on ``F.abs`` must not break inner-join right refs."""
    _left, right, joined = _semi_family_join(spark, "inner", on_mode)
    table = joined.select(F.abs(right["k"]).alias("ak")).to_arrow()
    assert table.column_names == ["ak"]
    assert table.to_pydict()["ak"] == [1]


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
def test_distinct_name_abs_raises_missing_from_input(spark: ReparkSession, how: str) -> None:
    """``F.abs(right["rk"])`` after a condition semi uses the MISSING_FROM_INPUT subclass."""
    left = spark.createDataFrame([(1, "a"), (2, "b")], ["k", "a"])
    right = spark.createDataFrame([(1,), (9,)], ["rk"])
    joined = left.join(right, left["k"] == right["rk"], how)
    with pytest.raises(AnalysisException, match=_MISSING_ABSENT) as excinfo:
        joined.select(F.abs(right["rk"]))
    assert 'Resolved attribute(s) "rk"' in str(excinfo.value)


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
def test_right_ref_lower_raises_missing_attributes_same_key(spark: ReparkSession, how: str) -> None:
    """``_scalar`` ride-along: ``F.lower(right["a"])`` after semi must not bind left ``a``."""
    left = spark.createDataFrame([(1, "A")], ["k", "a"])
    right = spark.createDataFrame([(1, "Z")], ["k", "a"])
    joined = left.join(right, on="k", how=how)
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR) as excinfo:
        joined.select(F.lower(right["a"]))
    assert 'Resolved attribute(s) "a"' in str(excinfo.value)


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
def test_coalesce_left_then_right_still_raises_unemitted_right(
    spark: ReparkSession, how: str
) -> None:
    """``join_sql`` QCOL scan, not first-origin-only: left-then-right coalesce must raise.

    ``_thread_origin`` copies the first origin-bearing arg (the emitted left ``k``); without
    ``join_sql_expr`` carrying the right QCOL this would silently bind left.
    """
    left, right, joined = _semi_family_join(spark, how, "condition")
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR) as excinfo:
        joined.select(F.coalesce(left["k"], right["k"]))
    assert 'Resolved attribute(s) "k"' in str(excinfo.value)


def test_abs_string_name_still_resolves_after_semi(spark: ReparkSession) -> None:
    """``F.abs("k")`` is a name, not a right-parent origin — still the left ``k``."""
    _left, _right, joined = _semi_family_join(spark, "leftsemi", "name")
    table = joined.select(F.abs("k").alias("ak")).to_arrow()
    assert table.to_pydict()["ak"] == [1]


@pytest.mark.parametrize("on_mode", ["name", "condition"])
def test_inner_join_abs_keeps_the_abs_on_a_negative_key(spark: ReparkSession, on_mode: str) -> None:
    """``F.abs`` after an emitting join must stay ``abs``, not rebound to the leaf.

    Seed ``k=1`` makes ``abs(k) == k``, so a rebind-to-bare-column mutant stays green; a negative
    key turns it red.
    """
    left = spark.createDataFrame([(-3, "a")], ["k", "a"])
    right = spark.createDataFrame([(-3,)], ["k"])
    if on_mode == "condition":
        joined = left.join(right, left["k"] == right["k"], "inner")
    else:
        joined = left.join(right, on="k", how="inner")
    table = joined.select(F.abs(right["k"]).alias("ak")).to_arrow()
    assert table.to_pydict()["ak"] == [3]
    assert joined.filter(F.abs(right["k"]) > 0).count() == 1


# Aggregate builders thread origin + join_sql_expr (same hole as F.abs).

_AGG_BUILDERS = (
    ("sum", F.sum),
    ("count", F.count),
    ("avg", F.avg),
    ("min", F.min),
    ("max", F.max),
    ("count_distinct", F.count_distinct),
    ("first", F.first),
    ("last", F.last),
)


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize("on_mode", ["name", "condition"])
@pytest.mark.parametrize("builder_name,builder", _AGG_BUILDERS)
def test_right_ref_agg_raises_missing_attributes_same_key(
    spark: ReparkSession,
    how: str,
    on_mode: str,
    builder_name: str,
    builder: object,
) -> None:
    """``F.<agg>(right["k"])`` after semi/anti raises the same-name MISSING_ATTRIBUTES class.

    Each named builder is its own pin: reverting the origin thread on that builder reds it.
    """
    _left, right, joined = _semi_family_join(spark, how, on_mode)
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR) as excinfo:
        joined.select(builder(right["k"]))  # type: ignore[operator]
    assert 'Resolved attribute(s) "k"' in str(excinfo.value), builder_name


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
@pytest.mark.parametrize("builder_name,builder", _AGG_BUILDERS)
def test_left_agg_still_resolves_after_semi_family(
    spark: ReparkSession, how: str, builder_name: str, builder: object
) -> None:
    """``F.<agg>(left["k"])`` is the output column — origin thread must not refuse left."""
    left, _right, joined = _semi_family_join(spark, how, "name")
    table = joined.select(builder(left["k"]).alias("ak")).to_arrow()  # type: ignore[operator]
    assert table.column_names == ["ak"], builder_name
    assert table.num_rows == 1, f"{builder_name} is a global agg (one row), not a refuse"


def test_inner_join_sum_right_ref_still_resolves(spark: ReparkSession) -> None:
    """Regression: origin-thread on ``F.sum`` must not break an emitting join.

    Name-key inner join only: a condition join of two ``k`` columns is DataFusion-ambiguous on
    the native aggregate handle and is not this pin.
    """
    _left, right, joined = _semi_family_join(spark, "inner", "name")
    table = joined.select(F.sum(right["k"]).alias("sk")).to_arrow()
    assert table.to_pydict()["sk"] == [1]


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
def test_distinct_name_sum_raises_missing_from_input(spark: ReparkSession, how: str) -> None:
    """``F.sum(right["rk"])`` after a condition semi uses the MISSING_FROM_INPUT subclass."""
    left = spark.createDataFrame([(1, "a"), (2, "b")], ["k", "a"])
    right = spark.createDataFrame([(1,), (9,)], ["rk"])
    joined = left.join(right, left["k"] == right["rk"], how)
    with pytest.raises(AnalysisException, match=_MISSING_ABSENT) as excinfo:
        joined.select(F.sum(right["rk"]))
    assert 'Resolved attribute(s) "rk"' in str(excinfo.value)


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
def test_count_distinct_left_then_right_still_raises_unemitted_right(
    spark: ReparkSession, how: str
) -> None:
    """``join_sql`` QCOL scan: left-then-right ``count_distinct`` must raise on the right."""
    left, right, joined = _semi_family_join(spark, how, "condition")
    with pytest.raises(AnalysisException, match=_MISSING_APPEAR) as excinfo:
        joined.select(F.count_distinct(left["k"], right["k"]))
    assert 'Resolved attribute(s) "k"' in str(excinfo.value)


def test_sum_string_name_still_resolves_after_semi(spark: ReparkSession) -> None:
    """``F.sum("k")`` is a name, not a right-parent origin — still the left ``k``."""
    _left, _right, joined = _semi_family_join(spark, "leftsemi", "name")
    table = joined.select(F.sum("k").alias("sk")).to_arrow()
    assert table.to_pydict()["sk"] == [1]
