"""G4b - DataFrame-API ``leftsemi`` / ``leftanti`` binding: the non-differential surface.

The recorded Spark-parity rows for this widening live in ``test_join_parity.py`` (the G4 joins
corpus): result sets on the Arrow path, value AND Arrow type AND nullability, across every
``on`` shape. This module holds the parts of the surface that are **not** a differential row:

1. **Accepted spellings.** PySpark takes ``semi`` / ``left_semi`` / ``leftsemi`` (and the anti
   family) case-insensitively; the facade folds them with ``.lower().replace("_", "")``. Live
   PySpark 4.1.2 was checked for the accepted set (2026-08-11) - including ``"LeftSemi"``, which
   only the case fold reaches. Each spelling is a separate dict key, so each needs an input.
2. **The conditionless refusal.** ``df.join(other, how="leftsemi")`` with no ``on`` is a repark
   DIVERGENCE, pinned here rather than silently absorbed: live Spark runs it (keeping every left
   row iff the right side is non-empty; the anti side is the complement), while repark refuses
   loud. The facade's Cartesian fallback would answer with an m*n cross join - a different result
   set - so refusing is deliberate. See ``task/g4b-join-widening-ledger.md`` for the recorded
   live-Spark behaviour and the queued disclosure.
3. **The refusal message** for an unknown ``how``, which must now advertise the semi family.
4. **G4b-R2 origin map.** After a semi/anti join the right side contributes no columns.
   ``select`` / ``filter`` / ``withColumn`` of a right-parent Column must raise Spark's
   ``MISSING_ATTRIBUTES`` class (same-name → ``RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION``;
   distinct-name → ``RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT``). ``drop`` of that Column is
   a Spark 4.1.2 **no-op** (does not raise, does not drop the same-name left column) —
   the brief guessed a raise; live probe 2026-08-12 falsified that. Left refs and inner
   joins stay resolved. See ``task/y5-origin-map-ledger.md``.

These are repark-only assertions (a refusal has no Spark golden to compare against), which is
exactly why they are not corpus rows: the corpus asserts differential equality.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

if TYPE_CHECKING:
    from collections.abc import Iterator

SEMI_SPELLINGS = ("semi", "left_semi", "leftsemi", "LeftSemi", "LEFT_SEMI")
ANTI_SPELLINGS = ("anti", "left_anti", "leftanti", "LeftAnti", "LEFT_ANTI")

# Live PySpark 4.1.2 (2026-08-12, /tmp/y5-spark-probe.py) — not UNRESOLVED_COLUMN.
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
    """The semi output is a normal frame: projectable, filterable, countable (not a dead handle).

    A join that returns a plan the rest of the facade cannot consume would pass a result-set
    assertion and still be unusable, so the follow-on operations are part of the pin.
    """
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

    Both conditionless shapes fall through to the facade's Cartesian path, which would answer an
    m*n cross join - not Spark's answer (live Spark 4.1.2: ``on=None`` + ``leftsemi`` keeps every
    left row when the right side is non-empty and none when it is empty; ``on=[]`` raises a
    PySpark ``IndexError``). Returning the wrong rows silently is the failure mode this refusal
    exists to prevent, so the refusal - not a cross join - is the pinned behaviour.
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
    """The guard is semi-family only: ``how='inner'`` with no ``on`` still takes the cross path.

    Without this the new branch could widen silently into every join type and the semi pins
    above would not notice.
    """
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
    """``select(right["k"])`` after semi/anti raises Spark's same-name MISSING_ATTRIBUTES.

    Pre-fix this resolved the LEFT ``k`` (silent wrong attribution). Live Spark 4.1.2
    class is ``MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION``, not
    ``UNRESOLVED_COLUMN``.
    """
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
    """``drop(right["k"])`` after semi/anti is a Spark 4.1.2 no-op.

    Live probe: does not raise, does not drop the same-name LEFT ``k``. Pre-fix repark
    dropped the left column by name — silently wrong attribution, the drop twin of the
    select bug. Matching Spark means keep ``k``.
    """
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


@pytest.mark.parametrize("how", ["leftsemi", "leftanti"])
def test_distinct_name_right_ref_raises_missing_from_input(spark: ReparkSession, how: str) -> None:
    """Right-only name (``rk``) after a condition semi/anti uses the MISSING_FROM_INPUT class.

    Live Spark 4.1.2 switches subclass when the missing attribute's spelling is not in
    the output. The origin map must not invent ``UNRESOLVED_COLUMN`` or the same-name
    subclass for this edge.
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
