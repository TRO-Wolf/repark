"""Shared pytest fixtures for the repark facade suite."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from typing import Any

import _live_parity as lp
import pytest

from repark.spark.session import _reset_active_session_for_tests

os.environ.setdefault("REPARK_DISPLAY_STYLE", "spark")

_seen_oracle_context: Any = None


@pytest.fixture(scope="session")
def spark_engine() -> Iterator[lp.Engine]:
    """The single shared live PySpark oracle engine (session-scoped). Skips (never fails) when the
    live flag is unset, so requesting it outside live mode is a visible skip. Left running for the
    whole run: every PySpark session shares one SparkContext, so a stop anywhere kills it."""
    if not lp.LIVE:
        pytest.skip(lp.LIVE_SKIP_REASON)
    yield lp.build_spark_engine()


@pytest.fixture(autouse=True)
def _shared_oracle_context_guard(request: pytest.FixtureRequest) -> Iterator[None]:
    """Fail the test that stopped or replaced the shared live-oracle SparkContext."""
    yield
    if not lp.LIVE or "pyspark" not in sys.modules:
        return
    from pyspark.sql import SparkSession

    global _seen_oracle_context
    session = SparkSession.getActiveSession()
    jsc = session.sparkContext._jsc if session is not None else None
    stopped = jsc is not None and bool(jsc.sc().isStopped())
    if _seen_oracle_context is None:
        if jsc is not None and not stopped:
            _seen_oracle_context = jsc
        return
    if jsc is not _seen_oracle_context or stopped:
        pytest.fail(
            f"{request.node.nodeid} stopped the shared PySpark SparkContext the live oracle "
            "runs on. Every PySpark session shares the one context, so no test in this suite "
            "may call .stop() on a PySpark session."
        )


@pytest.fixture(autouse=True)
def _isolate_active_session() -> None:
    """Clear the process-wide getOrCreate registry around every test.

    ``getOrCreate`` returns a process-wide active session, so tests that build independent
    sessions need a clean slate per case.
    """
    _reset_active_session_for_tests()
    yield
    _reset_active_session_for_tests()
