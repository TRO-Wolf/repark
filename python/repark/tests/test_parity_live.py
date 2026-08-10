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

A third, always-on layer sits beside them: ``test_disclosures_mirror_the_registry`` checks the
``DISCLOSURES`` list against the divergence registry (``docs/spark-sql-iceberg-parity.md`` §6),
which is the SSOT for divergence *semantics*. The list is the machine-checked mirror of the
registry rows that claim one — never the other way round.

Pyspark is imported lazily (inside the session fixture), so this file collects on a runner with no
pyspark and no JVM (the routine-CI contract, L3).
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

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
    home of the shared recipes (proves them before the live tier layers Spark on top).

    A scenario's `session_conf` is applied by BUILDING the repark session with it (repark resolves
    build-time knobs once at `getOrCreate`), so the JVM-free leg runs under the same session
    configuration the oracle leg will.
    """
    engine = lp.build_repark_engine(scenario.session_conf)
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

    # repark == pinned golden (the scenario's session conf is applied at session BUILD)
    repark_out = lp.run_scenario(scenario, lp.build_repark_engine(scenario.session_conf))
    assert_frames_equal(repark_out, scenario.golden, order_sensitive=order)

    # pinned golden == LIVE Spark (the leg routine CI can never run) — re-derived, not re-asserted.
    # The oracle session is shared, so the scenario's conf is set around this leg and restored.
    with lp.spark_session_conf(spark_engine, scenario.session_conf):
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
    Group L-write division goldens (union + bare) plus the two audit-G2 filter-rewriter goldens
    plus the two H-1a non-UTC-oracle goldens, plus the four load-bearing disclosures.

    The size moved 27 -> 29 DELIBERATELY in the same diff as the two scenarios it counts.
    """
    assert len(lp.SCENARIOS) == 29, (
        "the 23-golden family + 2 Group L-write division goldens + 2 audit-G2 filter goldens "
        "+ 2 H-1a non-UTC-oracle goldens"
    )
    assert len({s.name for s in lp.SCENARIOS}) == 29, "scenario names are unique"
    assert {d.name for d in lp.DISCLOSURES} == {
        "int_union_string",
        "fillna_scalar_numeric_nullability",
        "filter_case_collision_bypasses",
        "filter_backtick_identifier",
    }, "every load-bearing disclosure is present"


def test_registry_runs_at_least_two_scenarios_under_a_non_utc_oracle() -> None:
    """H-1a acceptance: the registry can put the ORACLE in a non-UTC session, and does.

    A registry pinned to one zone is structurally incapable of catching a session-timezone
    divergence — so the count is a pin, not a comment. Every override names the ONE session
    timezone key spelling; a second spelling would fail here rather than quietly configure nothing.
    """
    overridden = [scenario for scenario in lp.SCENARIOS if scenario.session_conf]
    assert len(overridden) >= 2, "at least two scenarios must run under a non-UTC oracle session"
    zones = {
        value
        for scenario in overridden
        for key, value in scenario.session_conf
        if key == lp.SESSION_TIME_ZONE_KEY
    }
    assert zones == {lp.ZONE_NEW_YORK, lp.ZONE_TOKYO}, (
        "the overrides must span both sides of UTC, so a sign error cannot pass both"
    )
    assert lp.DEFAULT_SESSION_TIME_ZONE not in zones, "an override to UTC is not an override"
    for scenario in overridden:
        assert all(key == lp.SESSION_TIME_ZONE_KEY for key, _ in scenario.session_conf), (
            "session-timezone overrides use the one authoritative key spelling"
        )


def test_build_repark_engine_override_stops_the_active_session_and_rebuilds() -> None:
    """The `active.stop()` branch inside `build_repark_engine`, exercised directly (JVM-free).

    Every scenario runs behind conftest's `_isolate_active_session`, which clears the process-wide
    registry before each test — so under the suite the branch never fires and the override looks
    load-bearing without being covered. It IS load-bearing: repark resolves the session zone once
    at construction, so with a session already active `getOrCreate` would hand that one back and
    the scenario would silently run under the PREVIOUS zone with only the soft
    "some configuration may not..." warning. This test creates that state on purpose.

    Reds if the stop is removed (the returned engine reports `UTC`) or if the override stops
    reaching the builder.
    """
    import repark

    first = lp.build_repark_engine()
    assert first.session.conf.get(lp.SESSION_TIME_ZONE_KEY) == lp.DEFAULT_SESSION_TIME_ZONE
    assert repark.ReparkSession.getActiveSession() is first.session

    second = lp.build_repark_engine(((lp.SESSION_TIME_ZONE_KEY, lp.ZONE_TOKYO),))
    assert second.session is not first.session, "an override must BUILD, never reuse"
    assert second.session.conf.get(lp.SESSION_TIME_ZONE_KEY) == lp.ZONE_TOKYO, (
        "the override zone must reach the freshly built session"
    )
    assert repark.ReparkSession.getActiveSession() is second.session

    # The documented cost (ledger residual 5): the previous handle is STOPPED, not left alive.
    with pytest.raises(Exception, match=r"stopped|SparkSession|alive"):
        first.session.sql("SELECT 1").to_arrow()
    second.session.stop()


# ==================================================================================================
# The divergence registry mirror — `DISCLOSURES` is the machine-checked mirror of the registry rows
# that declare a live mirror (docs/spark-sql-iceberg-parity.md §6). JVM-free: always runs.
# ==================================================================================================

#: The divergence registry — the SSOT for divergence *semantics* (this list mirrors it, never
#: the other way round). Resolved from this file so the check is cwd-independent.
_REGISTRY = Path(__file__).resolve().parents[3] / "docs" / "spark-sql-iceberg-parity.md"

#: A registry row opts into the live tier with a `live-mirror: <name>` bullet on its own line.
#: The backticks and the line anchor are load-bearing: prose that merely *mentions* the field
#: (the §1 and §6 explanations write `` `live-mirror: <name>` `` with a placeholder) must not
#: register as a row. This exact spelling is documented for row authors in the registry's §6
#: ("The exact spelling this gate parses") — the two must move together.
_LIVE_MIRROR_RE = re.compile(r"(?m)^- `live-mirror: ([a-z0-9_]+)`$")

#: The fail-closed half of the strict pattern above. A strict-only match is silently satisfied by
#: ZERO matches, so a row whose bullet is *nearly* right (bolded, indented, hyphenated name,
#: trailing comment) would claim a mirror that is never checked — and if the named ``Disclosure``
#: does not exist, nothing reds. Every ``-`` bullet that mentions the field is therefore probed
#: first and must satisfy the strict form. Restricted to ``-`` bullets on purpose: §1's
#: ``**`live-mirror:`**`` heading starts with ``*`` and §6's prose lines start with words, so
#: neither is a candidate.
_LIVE_MIRROR_PROBE = re.compile(r"(?m)^[ \t]*-.*live-mirror.*$")


def test_disclosures_mirror_the_registry() -> None:
    """Every registry row that claims a live mirror has a ``Disclosure`` of that name, and every
    ``Disclosure`` is claimed by a row. Both directions, because both failures are real: a row
    whose mirror was deleted quietly loses its drift detector (the divergence could converge and
    nothing would red), and a disclosure with no row is a divergence the registry does not
    describe — the exact "no discoverable list" state this registry exists to end.

    The registry is the SSOT for the *semantics*; this list is the checked mirror
    (``docs/spark-sql-iceberg-parity.md`` §6). Fix a RED by editing whichever side is wrong — never
    by relaxing this assertion.

    The check is **fail-closed on a near-miss**: a bullet that mentions ``live-mirror`` but does
    not match the exact spelling is a loud failure naming the line, not a silent zero-match. A
    near-miss is otherwise invisible in exactly the case that matters — the row advertises a drift
    detector it does not have.
    """
    assert _REGISTRY.is_file(), (
        f"the divergence registry is missing at {_REGISTRY} — every citation in this repository "
        "resolves to it, and this mirror cannot be checked without it"
    )
    registry_text = _REGISTRY.read_text(encoding="utf-8")
    malformed = [
        line
        for line in _LIVE_MIRROR_PROBE.findall(registry_text)
        if _LIVE_MIRROR_RE.fullmatch(line) is None
    ]
    assert not malformed, (
        "a registry bullet claims a live mirror in a spelling this gate does not parse, so the "
        "row would advertise a drift detector that is never checked. The exact required form is "
        'a top-level bullet: "- " then `live-mirror: <name>` in backticks, <name> matching '
        f"[a-z0-9_]+, nothing before or after it on the line. Offending line(s): {malformed}"
    )
    declared = _LIVE_MIRROR_RE.findall(registry_text)
    assert len(declared) == len(set(declared)), (
        f"a live-mirror name is claimed by two registry rows: {sorted(declared)}"
    )

    disclosed = {d.name for d in lp.DISCLOSURES}
    assert set(declared) == disclosed, (
        "the registry's live-mirrored rows and the DISCLOSURES list disagree — "
        f"registry-only: {sorted(set(declared) - disclosed)}; "
        f"disclosure-only: {sorted(disclosed - set(declared))}"
    )
