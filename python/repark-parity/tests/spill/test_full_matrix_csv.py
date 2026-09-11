"""Pins on the committed full-tier spill-coverage CSV: 27 cells and the three R-1 notes."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Final

from matrix_cells import FULL_CELLS, FULL_MULTIPLIERS
from matrix_harness import COMPLETED, REFUSED, SPILLED

_FULL_MATRIX_CSV: Final[Path] = (
    Path(__file__).resolve().parents[4] / "docs" / "perf" / "spill-coverage-matrix-2026-09-11.csv"
)
_STABLE: Final[frozenset[str]] = frozenset({SPILLED, COMPLETED, REFUSED})
_ISSUE_22758: Final[str] = "https://github.com/apache/datafusion/issues/22758"
_ISSUE_24768: Final[str] = "https://github.com/apache/datafusion/issues/24768"
_R1_CELLS: Final[dict[tuple[str, int], tuple[str, str]]] = {
    ("hash_join", 4): ("KILLED", _ISSUE_24768),
    ("hash_aggregate", 2): ("UNSTABLE", _ISSUE_22758),
    ("window_unbounded", 2): ("UNSTABLE", _ISSUE_22758),
}


def test_full_matrix_csv_has_27_cells() -> None:
    """The committed full-tier CSV has 27 cells; the three R-1 rows name their upstream issues."""
    with _FULL_MATRIX_CSV.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 27
    roster_operators = {cell.operator for cell in FULL_CELLS}
    assert {row["operator"] for row in rows} == roster_operators
    seen: set[tuple[str, int]] = set()
    for row in rows:
        key = (row["operator"], int(row["multiple"]))
        seen.add(key)
        outcome = row["outcome"]
        notes = row["notes"]
        if key in _R1_CELLS:
            expected_outcome, issue_url = _R1_CELLS[key]
            assert outcome == expected_outcome
            assert issue_url in notes
            assert outcome not in _STABLE
        else:
            assert outcome in _STABLE
    assert seen == {(cell.operator, cell.multiplier) for cell in FULL_CELLS}
    assert {multiple for _, multiple in seen} == set(FULL_MULTIPLIERS)
    assert set(_R1_CELLS) <= seen
