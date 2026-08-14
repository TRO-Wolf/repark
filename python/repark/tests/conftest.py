"""Shared pytest fixtures for the repark facade suite."""

from __future__ import annotations

import pytest

from repark.spark.session import _reset_active_session_for_tests


@pytest.fixture(autouse=True)
def _isolate_active_session() -> None:
    """Clear the process-wide getOrCreate registry around every test.

    WU-4 makes ``getOrCreate`` return a process-wide active session. Tests that build
    independent sessions (catalog configs, knobs) need a clean slate per case.
    """
    _reset_active_session_for_tests()
    yield
    _reset_active_session_for_tests()
