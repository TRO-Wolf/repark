"""CI-tier spill-matrix pins: the sort cell end to end, and the golden comparison."""

from __future__ import annotations

import csv
from pathlib import Path

from matrix_cells import (
    CI_CELL_TIMEOUT_S,
    CI_CELLS,
    CI_LIMIT_BYTES,
    CI_PARTITIONS,
    RosterRow,
    json_out_path,
    worker_argv,
)
from matrix_harness import SPILLED, CellRecord, require_no_killed_cells, run_cell

_GOLDEN_CSV = Path(__file__).resolve().parent / "ci_golden.csv"


def _sort_cell() -> RosterRow:
    for cell in CI_CELLS:
        if cell.operator == "sort":
            return cell
    raise AssertionError("sort is missing from CI_CELLS")


def _ci_cell(operator: str, multiple: int) -> RosterRow:
    for cell in CI_CELLS:
        if cell.operator == operator and cell.multiplier == multiple:
            return cell
    raise AssertionError(f"{operator} {multiple}x is missing from CI_CELLS")


def _run_ci_cell(spec: RosterRow, scratch_dir: Path) -> CellRecord:
    json_out = json_out_path(scratch_dir, spec)
    return run_cell(
        worker_argv(spec, json_out, CI_PARTITIONS, CI_LIMIT_BYTES),
        json_out,
        scratch_dir,
        CI_CELL_TIMEOUT_S,
        operator=spec.operator,
        multiplier=spec.multiplier,
        refusal_names=spec.refusal_names,
    )


def test_ci_tier_sort_cell_end_to_end(tmp_path: Path) -> None:
    """The sort cell spills under the 64 MB pool, measured in a subprocess under the cap."""
    spec = _sort_cell()
    record = _run_ci_cell(spec, tmp_path)
    assert record.outcome == SPILLED
    assert record.returncode == 0
    assert record.spill_bytes is not None and record.spill_bytes > 0
    assert record.spill_count is not None and record.spill_count > 0
    assert record.vm_size_at_cap is not None
    assert record.rlimit_as_bytes == record.vm_size_at_cap + 3 * CI_LIMIT_BYTES
    require_no_killed_cells([record])


def test_ci_tier_matches_golden(tmp_path: Path) -> None:
    """Each CI golden cell's run_cell outcome matches the committed golden row."""
    assert _GOLDEN_CSV.is_file(), f"CI golden is missing: {_GOLDEN_CSV}"
    with _GOLDEN_CSV.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    assert list(rows[0].keys()) == ["operator", "multiple", "outcome"]
    records: list[CellRecord] = []
    for row in rows:
        spec = _ci_cell(row["operator"], int(row["multiple"]))
        record = _run_ci_cell(spec, tmp_path)
        assert record.outcome == row["outcome"]
        if spec.operator == "sort":
            assert record.returncode == 0
            assert record.spill_bytes is not None and record.spill_bytes > 0
            assert record.spill_count is not None and record.spill_count > 0
            assert record.vm_size_at_cap is not None
            assert record.rlimit_as_bytes == record.vm_size_at_cap + 3 * CI_LIMIT_BYTES
        records.append(record)
    require_no_killed_cells(records)
