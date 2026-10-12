"""Measure the ICE-TSNS-COERCION-1 gate-open rate across one facade suite run.

Loaded with ``PYTHONPATH=task/wo/ice-tsns-coercion-1 pytest -p gate_rate_plugin``.
An autouse fixture snapshots the native gate counters around every test item and
``pytest_sessionfinish`` writes the per-item deltas plus the run totals as JSON to
``GATE_RATE_OUT`` (default ``gate-rate.json`` beside this file).
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import _native

DELTA: list[dict[str, Any]] = []
START: list[int] = []


def _counts() -> tuple[int, int]:
    """Return the gate-open and fallback counters."""
    opened: int
    fallback: int
    opened, fallback = _native._tsns_gate_counts()
    return opened, fallback


def pytest_configure() -> None:
    """Snapshot the counters before the first test item runs."""
    del DELTA[:]
    del START[:]
    opened, fallback = _counts()
    START.extend([opened, fallback])


@pytest.fixture(autouse=True)
def _record_gate_delta(request: pytest.FixtureRequest) -> Iterator[None]:
    """Append this item's counter deltas to the run record."""
    before_open, before_fallback = _counts()
    yield
    opened, fallback = _counts()
    DELTA.append(
        {
            "nodeid": request.node.nodeid,
            "opened": opened - before_open,
            "fallback": fallback - before_fallback,
        }
    )


def pytest_sessionfinish() -> None:
    """Write the per-item deltas and the run totals as JSON."""
    target = os.environ.get("GATE_RATE_OUT", "")
    out = Path(target) if target else Path(__file__).resolve().parent / "gate-rate.json"
    opened, fallback = _counts()
    payload = {
        "items": len(DELTA),
        "gate_open_total": opened - START[0],
        "fallback_total": fallback - START[1],
        "deltas": DELTA,
    }
    out.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8")
