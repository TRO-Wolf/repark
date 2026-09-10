"""The one CI-tier cell: sort at twice the 64 MB limit, end to end in a worker subprocess."""

from __future__ import annotations

from pathlib import Path

from matrix_cells import (
    CI_CELL_TIMEOUT_S,
    CI_CELLS,
    CI_LIMIT_BYTES,
    CI_PARTITIONS,
    json_out_path,
    worker_argv,
)
from matrix_harness import SPILLED, require_no_killed_cells, run_cell


def test_ci_tier_sort_cell_end_to_end(tmp_path: Path) -> None:
    """The sort cell spills under the 64 MB pool, measured in a subprocess under the cap."""
    spec = CI_CELLS[0]
    json_out = json_out_path(tmp_path, spec)
    argv = worker_argv(spec, json_out, CI_PARTITIONS, CI_LIMIT_BYTES)
    record = run_cell(
        argv,
        json_out,
        tmp_path,
        CI_CELL_TIMEOUT_S,
        operator=spec.operator,
        multiplier=spec.multiplier,
        refusal_names=spec.refusal_names,
    )
    assert record.outcome == SPILLED
    assert record.returncode == 0
    assert record.spill_bytes is not None and record.spill_bytes > 0
    assert record.spill_count is not None and record.spill_count > 0
    assert record.vm_size_at_cap is not None
    assert record.rlimit_as_bytes == record.vm_size_at_cap + 3 * CI_LIMIT_BYTES
    require_no_killed_cells([record])
