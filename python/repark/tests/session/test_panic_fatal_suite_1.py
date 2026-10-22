"""PANIC-FATAL-SUITE-1 — the facade suite runs with ``REPARK_PANIC_FATAL=1`` armed.

``conftest.py`` sets the variable before the first ``repark`` import and the native module reads
it once when it initialises. These pins prove the order (the module reports the policy it
initialised with), prove that a caught panic ends a child interpreter that inherits the suite
environment, and prove that the same panic keeps its ``repark internal error`` message when the
variable is cleared. The panic comes from the test-only ``_native.__panic_probe__('method')``,
which panics inside a method fence. The Arrow C stream fence is pinned by the Rust child-process
tests in ``fence.rs``.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from repark import _native

_VARIABLE = "REPARK_PANIC_FATAL"
_CONVERTED_PREFIX = "repark internal error in "
_CONVERTED_SUFFIX = "a Rust panic was caught at the Python boundary"
_PANIC_TEXT = "repark panic probe"
_TIMEOUT_SECONDS = 300

_PREAMBLE = """
from repark import _native
"""

_SHAPES: dict[str, tuple[str, str]] = {
    "method": ("_native.__panic_probe__('method')", "PanicProbe.method"),
}


def _child_source(statement: str) -> str:
    """Return the child source that runs ``statement`` once and reports a caught error."""
    return (
        f"{_PREAMBLE}\n"
        "try:\n"
        f"    {statement}\n"
        "except Exception as error:\n"
        "    print('child-caught', error)\n"
        "else:\n"
        "    print('child-survived')\n"
    )


def _run_child(source: str, *, armed: bool) -> subprocess.CompletedProcess[str]:
    """Run ``source`` in a fresh interpreter with the suite environment, or without the flag."""
    environment = dict(os.environ)
    if not armed:
        environment.pop(_VARIABLE, None)
    return subprocess.run(
        [sys.executable, "-c", source],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT_SECONDS,
        env=environment,
        check=False,
    )


def _combined(completed: subprocess.CompletedProcess[str]) -> str:
    """Return the child's stdout and stderr as one string."""
    return completed.stdout + completed.stderr


def test_the_native_module_initialised_with_the_suite_flag_armed() -> None:
    """The module read ``REPARK_PANIC_FATAL=1`` at import, so ``conftest.py`` set it first."""
    assert os.environ.get(_VARIABLE) == "1"
    assert _native.__panic_fatal__ is True


@pytest.mark.parametrize("shape", sorted(_SHAPES))
def test_the_known_panic_keeps_its_message_when_the_flag_is_cleared(shape: str) -> None:
    """With the flag removed the fence still turns the panic into ``PySparkException``."""
    statement, operation = _SHAPES[shape]
    completed = _run_child(_child_source(statement), armed=False)
    text = _combined(completed)
    assert completed.returncode == 0, text
    assert f"child-caught {_CONVERTED_PREFIX}{operation}: {_PANIC_TEXT} (" in text, text
    assert _CONVERTED_SUFFIX in text, text


@pytest.mark.parametrize("shape", sorted(_SHAPES))
def test_the_known_panic_ends_a_child_that_inherits_the_suite_environment(shape: str) -> None:
    """The suite environment makes the same panic fatal: the child dies with no converted text."""
    statement, _ = _SHAPES[shape]
    completed = _run_child(_child_source(statement), armed=True)
    text = _combined(completed)
    assert completed.returncode != 0, text
    assert _PANIC_TEXT in text, text
    assert _CONVERTED_PREFIX not in text, text
    assert "child-caught" not in text, text
    assert "child-survived" not in text, text
