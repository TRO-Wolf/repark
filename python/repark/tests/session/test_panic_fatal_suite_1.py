"""PANIC-FATAL-SUITE-1 — the facade suite runs with ``REPARK_PANIC_FATAL=1`` armed.

``conftest.py`` sets the variable before the first ``repark`` import and the native module reads
it once when it initialises. This pin asks the module which policy it initialised with, so it
fails when the line moves below the first ``repark`` import or when the value is not ``1``.
"""

from __future__ import annotations

import os

from repark import _native

_VARIABLE = "REPARK_PANIC_FATAL"


def test_the_native_module_initialised_with_the_suite_flag_armed() -> None:
    """The module read ``REPARK_PANIC_FATAL=1`` at import, so ``conftest.py`` set it first."""
    assert os.environ.get(_VARIABLE) == "1"
    assert _native.__panic_fatal__ is True
