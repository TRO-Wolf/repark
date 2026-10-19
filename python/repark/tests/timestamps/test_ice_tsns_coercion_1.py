"""The ICE-TSNS-COERCION-1 slice 0 fixture is complete and matches its summary.

pins: ice-tsns-coercion-1/C-001, C-002
"""

from __future__ import annotations

import json
from pathlib import Path

import _ice_tsns_coercion_1_spellings as spellings
import _summarize_ice_tsns_coercion_1 as summarize

ROOT = Path(__file__).resolve().parents[4]
TESTS = Path(__file__).resolve().parent
FIXTURE = TESTS / "ice_tsns_coercion_1_twins.json"
LATTICE = TESTS / "ice_tsns_coercion_1_spark_lattice.json"
SUMMARY = ROOT / "task" / "wo" / "ice-tsns-coercion-1" / "evidence" / "twins-summary.md"
COMMIT = "572130dda35f739795578134348f99fd7b530cfd"


def _load(path: Path) -> dict:
    """Return one JSON object."""
    return json.loads(path.read_text(encoding="utf-8"))


def _slots(mapping: dict) -> list[int]:
    """Return every door slot."""
    found: list[int] = []
    for slots in mapping.values():
        found.extend(slots)
    return found


def test_twin_fixture_covers_every_door_zone_and_sibling() -> None:
    """pins: ice-tsns-coercion-1/C-001 — main 572130dd, five zones, every door."""
    fixture = _load(FIXTURE)
    assert fixture["commit"] == COMMIT
    assert fixture["zones"] == list(spellings.ZONES)
    assert fixture["doors"] == list(spellings.DOORS)
    assert fixture["forms"] == list(spellings.FORMS)
    assert fixture["siblings"] == list(spellings.SIBLINGS)
    assert fixture["targets"] == list(spellings.TARGETS)
    assert len(fixture["doors"]) == 64
    assert -1 not in _slots(fixture["statement"])
    assert -1 not in _slots(fixture["twin"])
    assert -1 not in _slots(fixture["plain"])
    assert len(fixture["plain"]) == 5 * 3 * 2
    assert len(fixture["twin"]) == 5 * 3 * 2 * 7 * 8
    assert len(fixture["statement"]) == len(fixture["twin"])
    reads = fixture["reads"]
    for zone in spellings.ZONES:
        for source in spellings.SOURCES:
            for form in spellings.FORMS:
                for sibling in spellings.SIBLINGS:
                    for role in ("statement", "twin"):
                        assert f"{zone}|{source}|{form}|{sibling}|{role}" in reads
    ranged = fixture["range"]
    for zone in spellings.ZONES:
        for ansi in ("true", "false"):
            for source in spellings.SOURCES:
                for form in spellings.FORMS:
                    for sibling in spellings.RANGE_SIBLINGS:
                        for role in ("statement", "twin"):
                            key = f"{zone}|{ansi}|{source}|{form}|{sibling}|{role}"
                            assert key in ranged


def test_spark_lattice_covers_both_ansi_modes() -> None:
    """pins: ice-tsns-coercion-1/C-002 — Spark 4.1.2, both ANSI modes, five zones."""
    lattice = _load(LATTICE)
    assert lattice["spark"] == "4.1.2"
    assert lattice["zones"] == list(spellings.ZONES)
    cells = lattice["cells"]
    siblings = list(spellings.SIBLINGS) + list(spellings.RANGE_SIBLINGS)
    sources = ("ns", "tzns")
    assert len(cells) == 5 * 2 * len(sources) * 7 * len(siblings)
    for zone in spellings.ZONES:
        for ansi in ("true", "false"):
            for source in sources:
                for form in spellings.FORMS:
                    for sibling in siblings:
                        cell = cells[f"{zone}|{ansi}|{source}|{form}|{sibling}"]
                        assert "error" not in cell


def test_summary_is_the_script_output() -> None:
    """The committed summary is what the summarizer writes from the two fixtures."""
    fixture = _load(FIXTURE)
    lattice = _load(LATTICE)
    assert SUMMARY.read_text(encoding="utf-8") == summarize.build_summary(fixture, lattice)
