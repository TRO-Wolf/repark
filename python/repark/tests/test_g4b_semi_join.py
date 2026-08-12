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
