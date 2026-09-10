"""The spill-matrix roster: the limit constants and the CI-tier cells wired so far."""

from __future__ import annotations

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
