"""Pins on the PERF-CAST-1 CAST-cost golden and the 2500-cast standalone budget.

pins: perf-cast-1/C-001, C-002, C-003, C-004
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Final

import pytest

_REPO: Final[Path] = Path(__file__).resolve().parents[4]
_GOLDEN_CSV: Final[Path] = _REPO / "docs" / "perf" / "cast-cost-2026-09-12.csv"
_SHAPES: Final[frozenset[str]] = frozenset(
    {"standalone", "over_aggregate", "projection_over_aggregate"}
)
_CAST_COUNTS: Final[frozenset[int]] = frozenset({50, 250, 2500})
_STANDALONE_2500: Final[tuple[str, int]] = ("standalone", 2500)
_BUDGET_MULTIPLE: Final[float] = 1.5


def _golden_rows() -> list[dict[str, str]]:
    with _GOLDEN_CSV.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _cell(rows: list[dict[str, str]], shape: str, cast_count: int) -> dict[str, str]:
    for row in rows:
        if row["shape"] == shape and int(row["cast_count"]) == cast_count:
            return row
    raise AssertionError(f"missing golden cell {shape} {cast_count}")


def test_cast_cost_golden_csv_has_nine_cells() -> None:
    """The committed CAST-cost CSV has one row per shape at 50, 250, and 2500."""
    assert _GOLDEN_CSV.is_file(), f"CAST-cost golden is missing: {_GOLDEN_CSV}"
    rows = _golden_rows()
    assert len(rows) == 9
    seen: set[tuple[str, int]] = set()
    for row in rows:
        shape = row["shape"]
        cast_count = int(row["cast_count"])
        seen.add((shape, cast_count))
        assert shape in _SHAPES
        assert cast_count in _CAST_COUNTS
        assert float(row["median_seconds"]) > 0.0
        assert float(row["median_plan_seconds"]) >= 0.0
        assert float(row["median_execute_seconds"]) >= 0.0
    assert seen == {(shape, count) for shape in _SHAPES for count in _CAST_COUNTS}


def test_cast_2500_standalone_under_regression_budget() -> None:
    """The 2500-cast standalone projection stays under 1.5x the committed median."""
    sys.path.insert(0, str(_REPO / "python" / "repark-parity" / "bench"))
    from cast.measure import measure_one_cell

    rows = _golden_rows()
    golden = _cell(rows, *_STANDALONE_2500)
    budget = _BUDGET_MULTIPLE * float(golden["median_seconds"])
    record = measure_one_cell(
        shape="standalone",
        cast_count=2500,
        row_count=200_000,
        warmup=1,
        repeats=3,
    )
    assert record.median_seconds <= budget, (
        f"2500-cast standalone median {record.median_seconds:.4f}s "
        f"exceeds {budget:.4f}s (1.5x golden {golden['median_seconds']})"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "PERF-CAST-1 step 2: CAST-over-aggregate planning is superlinear "
        "(exponent 1.535); a fix or DataFusion bump that restores linear "
        "scaling from the 50-cast plan median flips this pin (strict XPASS). "
        "pins: perf-cast-1/C-003, C-004"
    ),
)
def test_cast_2500_over_aggregate_plan_under_linear_budget() -> None:
    """A future speedup flips this pin when 2500-cast aggregate planning is linear."""
    rows = _golden_rows()
    fifty = _cell(rows, "over_aggregate", 50)
    twenty_five_hundred = _cell(rows, "over_aggregate", 2500)
    linear_budget = float(fifty["median_plan_seconds"]) * (2500 / 50)
    assert float(twenty_five_hundred["median_plan_seconds"]) <= linear_budget
