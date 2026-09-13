"""Guard for the eager_own suite: the worker builds a session, so the native module is needed."""

from __future__ import annotations

import pytest

pytest.importorskip(
    "repark",
    reason="the eager_own pins run the product in a worker subprocess; build the native "
    "module first. The isolated parity job has no native build, so it collects nothing here.",
)
