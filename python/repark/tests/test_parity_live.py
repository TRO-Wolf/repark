"""The live PySpark oracle tier (L1) + its detector proofs (L6a).

Two layers over the shared scenario registry in :mod:`_live_parity`:

* **routine (JVM-free, every PR)** — ``test_scenario_recipe_matches_golden_on_repark`` runs each
  engine-agnostic recipe on repark and asserts ``repark == pinned golden``. This gives the shared
  recipes no-JVM coverage and never touches Spark, so it runs in routine CI.
* **live (``REPARK_PARITY_LIVE=1``, nightly / dispatch / parity-live.yml)** — the ``*_live_*`` tests
  spin up ONE shared session-scoped local SparkSession and assert the full triple
  **repark == pinned golden == live Spark** for every scenario, plus that every recorded
  divergence (disclosure) STILL diverges. With the flag unset every live test SKIPs with a visible
  reason — it never silently passes.

Pyspark is imported lazily (inside the session fixture), so this file collects on a runner with no
pyspark and no JVM (the routine-CI contract, L3).
"""

from __future__ import annotations

from collections.abc import Iterator

import _live_parity as lp
import pytest

from repark_parity import assert_frames_equal

# ==================================================================================================
# Shared live SparkSession — built ONCE per session, only when the flag is armed
# ==================================================================================================


@pytest.fixture(scope="session")
def spark_engine() -> Iterator[lp.Engine]:
    """The single shared live PySpark oracle engine (session-scoped). Skips (never fails) when the
    live flag is unset, so requesting it outside live mode is a visible skip."""
    if not lp.LIVE:
        pytest.skip(lp.LIVE_SKIP_REASON)
    engine = lp.build_spark_engine()
    try:
        yield engine
    finally:
        engine.session.stop()


# ==================================================================================================
# Routine (JVM-free) — the shared recipes reproduce the goldens on repark
# ==================================================================================================


@pytest.mark.parametrize("scenario", lp.SCENARIOS, ids=[s.name for s in lp.SCENARIOS])
def test_scenario_recipe_matches_golden_on_repark(scenario: lp.Scenario) -> None:
    """Every registry recipe produces its pinned golden on repark — no JVM. This is the routine-CI
    home of the shared recipes (proves them before the live tier layers Spark on top)."""
    engine = lp.build_repark_engine()
    actual = lp.run_scenario(scenario, engine)
    assert_frames_equal(actual, scenario.golden, order_sensitive=scenario.order_sensitive)


# ==================================================================================================
# Live — repark == pinned golden == live Spark (value AND Arrow-path type/nullability)
# ==================================================================================================


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("scenario", lp.SCENARIOS, ids=[s.name for s in lp.SCENARIOS])
def test_live_scenario_matches_repark_golden_and_spark(
    scenario: lp.Scenario, spark_engine: lp.Engine
) -> None:
    """The drift detector: the pinned golden is re-derived from LIVE Spark and must equal both the
    pin and repark. A stale/hand-edited pin (golden drift) OR a Spark semantics change (oracle
    drift) makes the Spark leg — or the repark leg — diverge from the pin, going RED here.
    """
    order = scenario.order_sensitive

    # repark == pinned golden
    repark_out = lp.run_scenario(scenario, lp.build_repark_engine())
    assert_frames_equal(repark_out, scenario.golden, order_sensitive=order)

    # pinned golden == LIVE Spark (the leg routine CI can never run) — re-derived, not re-asserted
    spark_out = lp.run_scenario(scenario, spark_engine)
    assert_frames_equal(spark_out, scenario.golden, order_sensitive=order)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("disclosure", lp.DISCLOSURES, ids=[d.name for d in lp.DISCLOSURES])
def test_live_disclosure_still_diverges(disclosure: lp.Disclosure, spark_engine: lp.Engine) -> None:
    """Each recorded divergence still holds on BOTH live engines: repark keeps its (divergent)
    behavior and live Spark keeps the behavior it differs from. If either engine converged toward
    the other, its check flips RED — forcing the disclosure to be revisited rather than silently
    laundered into 'parity'.
    """
    disclosure.repark_check(lp.build_repark_engine())
    disclosure.spark_check(spark_engine)


# ==================================================================================================
# L6(a) detector — the flag gate. Deterministic, always runs (no JVM): proves the tier is OFF for
# any value other than exactly "1", so an unarmed run SKIPs rather than silently passing.
# ==================================================================================================


def test_live_flag_predicate_gates_on_exact_env_value() -> None:
    assert lp.live_enabled({}) is False, "unset -> off"
    assert lp.live_enabled({"REPARK_PARITY_LIVE": ""}) is False, "empty -> off"
    assert lp.live_enabled({"REPARK_PARITY_LIVE": "0"}) is False, "'0' -> off"
    assert lp.live_enabled({"REPARK_PARITY_LIVE": "true"}) is False, "only exact '1' arms it"
    assert lp.live_enabled({"REPARK_PARITY_LIVE": "1"}) is True, "'1' -> on"


def test_registry_covers_the_mandated_golden_family() -> None:
    """Guard against accidental registry shrinkage: the mandated coverage floor is the 23-golden
    family (Group E group-agg/na/union + columns + dates + compound-agg display name) plus the two
    Group L-write division goldens (union + bare) plus the two audit-G2 filter-rewriter goldens,
    plus the four load-bearing disclosures.
    """
    assert len(lp.SCENARIOS) == 27, (
        "the 23-golden family + 2 Group L-write division goldens + 2 audit-G2 filter goldens"
    )
    assert len({s.name for s in lp.SCENARIOS}) == 27, "scenario names are unique"
    assert {d.name for d in lp.DISCLOSURES} == {
        "int_union_string",
        "fillna_scalar_numeric_nullability",
        "filter_case_collision_bypasses",
        "filter_backtick_identifier",
    }, "every load-bearing disclosure is present"
