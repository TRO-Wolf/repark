"""The spill-matrix roster: the limit constants, the CI-tier cells, and the worker argv."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Final

from matrix_harness import CellSpec

CI_LIMIT_BYTES: Final[int] = 64 * 1024 * 1024
CI_PARTITIONS: Final[int] = 1
CI_CELL_TIMEOUT_S: Final[float] = 600.0

_SORT_SQL: Final[str] = "SELECT id, payload FROM base ORDER BY payload"
_SORT_REFUSAL_NAMES: Final[tuple[str, ...]] = ("SortExec", "ExternalSorter")

CI_CELLS: Final[tuple[CellSpec, ...]] = (
    CellSpec(operator="sort", multiplier=2, sql=_SORT_SQL, refusal_names=_SORT_REFUSAL_NAMES),
)

_WORKER_PATH: Final[Path] = Path(__file__).resolve().parent / "matrix_worker.py"


def worker_argv(spec: CellSpec, json_out: Path, partitions: int, limit_bytes: int) -> list[str]:
    """Build the worker subprocess argv for one cell."""
    return [
        sys.executable,
        str(_WORKER_PATH),
        "--operator",
        spec.operator,
        "--multiplier",
        str(spec.multiplier),
        "--sql",
        spec.sql,
        "--refusal-names",
        ",".join(spec.refusal_names),
        "--limit-bytes",
        str(limit_bytes),
        "--partitions",
        str(partitions),
        "--json-out",
        str(json_out),
    ]


def json_out_path(scratch_dir: Path, spec: CellSpec) -> Path:
    """Return the worker's result-JSON path for one cell."""
    return scratch_dir / f"{spec.operator}-{spec.multiplier}x.json"
