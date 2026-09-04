"""Shared pytest fixtures for the repark facade suite."""

from __future__ import annotations

from collections.abc import Iterator

import _live_parity as lp
import pytest

from repark.spark.session import _reset_active_session_for_tests


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


@pytest.fixture(autouse=True)
def _isolate_active_session() -> None:
    """Clear the process-wide getOrCreate registry around every test.

    ``getOrCreate`` returns a process-wide active session, so tests that build independent
    sessions need a clean slate per case.
    """
    _reset_active_session_for_tests()
    yield
    _reset_active_session_for_tests()
