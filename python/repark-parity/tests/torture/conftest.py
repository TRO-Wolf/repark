"""Fixtures for the torture suite: one shared repark session and tier-aware family data."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

pytest.importorskip(
    "repark",
    reason="the torture suite reads through the product; run it with the native module built, "
    "via `make py-test-torture`. The isolated parity job (`make py-test`, ci.yml) has no native "
    "build, so it collects nothing here.",
)

from _support import family_output

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests
from repark_parity.torture import FAMILIES, FamilyOutput
from repark_parity.torture.tiers import CI_TIER, FULL_ROOT, tier_from_env, tier_rows

SEED = 7


@pytest.fixture(scope="session")
def spark() -> Iterator[ReparkSession]:
    """One shared repark session for every torture cell."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("repark-parity-torture").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _family_output(family_name: str, tmp_path_factory: pytest.TempPathFactory) -> FamilyOutput:
    """Generate one family at the active tier, reusing /tmp/torture on the full tier."""
    family = FAMILIES[family_name]
    tier = tier_from_env()
    rows = tier_rows(tier)
    if tier == CI_TIER:
        out = tmp_path_factory.mktemp(f"torture-{family_name}")
        return family_output(family, rows=rows, seed=SEED, out=out)
    out = FULL_ROOT / family_name
    out.mkdir(parents=True, exist_ok=True)
    return family_output(family, rows=rows, seed=SEED, out=out)


@pytest.fixture(scope="session")
def nested_data(tmp_path_factory: pytest.TempPathFactory) -> FamilyOutput:
    """The nested family's generated files at the active tier."""
    return _family_output("nested", tmp_path_factory)


@pytest.fixture(scope="session")
def inference_data(tmp_path_factory: pytest.TempPathFactory) -> FamilyOutput:
    """The inference family's generated files at the active tier."""
    return _family_output("inference", tmp_path_factory)


@pytest.fixture(scope="session")
def extreme_types_data(tmp_path_factory: pytest.TempPathFactory) -> FamilyOutput:
    """The extreme_types family's generated files at the active tier."""
    return _family_output("extreme_types", tmp_path_factory)


@pytest.fixture(scope="session")
def smartcsv_data(tmp_path_factory: pytest.TempPathFactory) -> FamilyOutput:
    """The smartcsv family's generated files at the active tier."""
    return _family_output("smartcsv", tmp_path_factory)


@pytest.fixture(scope="session")
def temporal_data(tmp_path_factory: pytest.TempPathFactory) -> FamilyOutput:
    """The temporal family's generated files at the active tier."""
    return _family_output("temporal", tmp_path_factory)


@pytest.fixture(scope="session")
def decimal_overflow_data(tmp_path_factory: pytest.TempPathFactory) -> FamilyOutput:
    """The decimal_overflow family's generated files at the active tier."""
    return _family_output("decimal_overflow", tmp_path_factory)
