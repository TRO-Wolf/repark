"""Record the narrow census: every pin-6 cell as main, twin or neither.

Runs the full 9,600-cell pin-6 space on this head and classifies each cell
against the slice-0 fixture roles (statement with round 2's live text, twin).
Any cell that equals neither role fails the run. Writes
``ice_tsns_coercion_1_narrow_census.json`` mapping each cell to its role.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _ice_tsns_coercion_1_doors as doors
from test_ice_tsns_coercion_1_slice1 import (
    PIN6_FORMS,
    SIBLING,
    _denoised,
    _load_fixture,
    _role_outcome,
)
from test_ice_tsns_coercion_1_slice1_round2 import _live_text

TESTS = Path(__file__).resolve().parent
OUT = TESTS / "ice_tsns_coercion_1_narrow_census.json"


def _main_outcome(
    fixture: dict[str, Any],
    zone: str,
    target: str,
    source: str,
    form: str,
    door: str,
) -> dict[str, Any]:
    """Return the recorded statement role, with stale text repaired."""
    want = _role_outcome(fixture, "statement", zone, target, source, form, SIBLING, door)
    if source == "tzns_utc" and "error" in want:
        want = {**want, "error": _live_text(want["error"])}
    return want


def _classify(
    outcome: dict[str, Any],
    fixture: dict[str, Any],
    zone: str,
    target: str,
    source: str,
    form: str,
    door: str,
) -> str:
    """Return ``main``, ``twin`` or ``neither`` for one measured cell."""
    seen = _denoised(outcome)
    if seen == _denoised(_main_outcome(fixture, zone, target, source, form, door)):
        return "main"
    if seen == _denoised(_role_outcome(fixture, "twin", zone, target, source, form, SIBLING, door)):
        return "twin"
    return "neither"


def measure_all(scratch: Path) -> tuple[dict[str, dict[str, str]], list[str]]:
    """Return the role of every census cell plus the neither list."""
    fixture = _load_fixture()
    roles: dict[str, dict[str, str]] = {}
    neither: list[str] = []
    for zone_index, zone in enumerate(fixture["zones"]):
        warehouse = scratch / f"census-{zone_index}"
        warehouse.mkdir()
        spark = doors.open_session(zone, warehouse)
        try:
            for key in sorted(fixture["twin"]):
                zone_key, target, source, form, sibling = key.split("|")
                if zone_key != zone or form not in PIN6_FORMS or sibling != SIBLING:
                    continue
                for door in fixture["doors"]:
                    cell = f"{key}|{door}"
                    outcome = doors.measure(
                        spark, warehouse, target, source, form, SIBLING, "statement", door
                    )
                    role = _classify(outcome, fixture, zone, target, source, form, door)
                    roles[cell] = {"role": role}
                    if role == "neither":
                        neither.append(cell)
        finally:
            spark.stop()
    return roles, neither


def main() -> None:
    """Record every census cell and report the split."""
    with tempfile.TemporaryDirectory() as scratch:
        roles, neither = measure_all(Path(scratch))
    OUT.write_text(json.dumps(roles, indent=1, sort_keys=True) + "\n")
    kept = sum(1 for outcome in roles.values() if outcome["role"] == "twin")
    mains = sum(1 for outcome in roles.values() if outcome["role"] == "main")
    print(f"cells={len(roles)} twin={kept} main={mains} neither={len(neither)}")
    for cell in neither[:20]:
        print(f"neither: {cell}")
    if neither or len(roles) != 9600:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
