"""The spill-matrix roster: the limit constants, the cell lists, and the worker argv."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Final

from matrix_harness import CellSpec
from pydantic import Field

CI_LIMIT_BYTES: Final[int] = 64 * 1024 * 1024
CI_PARTITIONS: Final[int] = 1
CI_CELL_TIMEOUT_S: Final[float] = 600.0

FULL_LIMIT_BYTES: Final[int] = 1024 * 1024 * 1024
FULL_PARTITIONS: Final[int] = 4
FULL_CELL_TIMEOUT_S: Final[float] = 3600.0
FULL_REPS: Final[int] = 3

_SORT_SQL: Final[str] = "SELECT id, payload FROM base ORDER BY payload"
_AGG_SQL: Final[str] = "SELECT payload, count(*) AS c FROM base GROUP BY payload"
_EQUI_JOIN_SQL: Final[str] = (
    "SELECT l.id, l.payload, r.payload FROM base l JOIN other r ON l.id = r.id"
)
_NLJ_SQL: Final[str] = (
    "SELECT l.id, l.payload, r.v FROM "
    "(SELECT id, payload, cast(id as double) * 1.5 AS v FROM base) l "
    "JOIN other r ON l.v < r.v"
)
_WIN_SLIDING_SQL: Final[str] = (
    "SELECT id, payload, "
    "sum(id) OVER (ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS s FROM base"
)
_WIN_UNBOUNDED_SQL: Final[str] = (
    "SELECT id, payload, sum(id) OVER (PARTITION BY (id % 1024)) AS s FROM base"
)
_COLLECT_SQL: Final[str] = "SELECT id, payload FROM base"
_FLAT_SQL: Final[str] = "SELECT * FROM flat"

_SORT_NAMES: Final[tuple[str, ...]] = ("SortExec", "ExternalSorter", "SortPreservingMergeExec")
_AGG_NAMES: Final[tuple[str, ...]] = ("GroupedHashAggregateStream", "AggregateExec")
_HASH_JOIN_NAMES: Final[tuple[str, ...]] = ("HashJoinInput", "HashJoinExec")
_SMJ_NAMES: Final[tuple[str, ...]] = (
    "SortMergeJoinExec",
    "ExternalSorter",
    "SortExec",
    "SortPreservingMergeExec",
)
_WIN_SLIDING_NAMES: Final[tuple[str, ...]] = (
    "BoundedWindowAggExec",
    "SortExec",
    "ExternalSorter",
    "SortPreservingMergeExec",
)
_WIN_UNBOUNDED_NAMES: Final[tuple[str, ...]] = (
    "WindowAggExec",
    "SortExec",
    "ExternalSorter",
    "SortPreservingMergeExec",
)
_FLAT_NAMES: Final[tuple[str, ...]] = ("UnnestExec",)
_NLJ_NAMES: Final[tuple[str, ...]] = ("NestedLoopJoinLoad", "NestedLoopJoinExec")

_SMJ_CONF: Final[dict[str, str]] = {"datafusion.optimizer.prefer_hash_join": "false"}


class RosterRow(CellSpec):
    """One matrix cell plus its dispatch shape: kind, extra session conf, right input."""

    multiplier: int = 0
    kind: str = "sql"
    conf: dict[str, str] = Field(default_factory=dict)
    right: str | None = None


_ROSTER: Final[tuple[RosterRow, ...]] = (
    RosterRow(
        operator="sort",
        sql=_SORT_SQL,
        refusal_names=_SORT_NAMES,
    ),
    RosterRow(
        operator="hash_aggregate",
        sql=_AGG_SQL,
        refusal_names=_AGG_NAMES,
    ),
    RosterRow(
        operator="hash_join",
        sql=_EQUI_JOIN_SQL,
        refusal_names=_HASH_JOIN_NAMES,
        right="sized",
    ),
    RosterRow(
        operator="sort_merge_join",
        sql=_EQUI_JOIN_SQL,
        refusal_names=_SMJ_NAMES,
        conf=_SMJ_CONF,
        right="sized",
    ),
    RosterRow(
        operator="window_sliding",
        sql=_WIN_SLIDING_SQL,
        refusal_names=_WIN_SLIDING_NAMES,
    ),
    RosterRow(
        operator="window_unbounded",
        sql=_WIN_UNBOUNDED_SQL,
        refusal_names=_WIN_UNBOUNDED_NAMES,
    ),
    RosterRow(
        operator="dynamic_flatten",
        kind="flatten",
        sql=_FLAT_SQL,
        refusal_names=_FLAT_NAMES,
    ),
    RosterRow(
        operator="nested_loop_join",
        sql=_NLJ_SQL,
        refusal_names=_NLJ_NAMES,
        right="key64",
    ),
    RosterRow(
        operator="collect",
        kind="collect",
        sql=_COLLECT_SQL,
        refusal_names=(),
    ),
)

FULL_MULTIPLIERS: Final[tuple[int, ...]] = (2, 4, 8)

FULL_SESSION_CONF: Final[dict[str, str]] = {"datafusion.execution.batch_size": "8192"}

FULL_CELLS: Final[tuple[RosterRow, ...]] = tuple(
    row.model_copy(
        update={
            "multiplier": multiplier,
            "conf": {**FULL_SESSION_CONF, **row.conf},
        }
    )
    for row in _ROSTER
    for multiplier in FULL_MULTIPLIERS
)

_CI_OPERATORS: Final[frozenset[str]] = frozenset({"sort", "hash_aggregate", "hash_join"})

CI_CELLS: Final[tuple[RosterRow, ...]] = tuple(
    row.model_copy(update={"multiplier": 2}) for row in _ROSTER if row.operator in _CI_OPERATORS
)

_WORKER_PATH: Final[Path] = Path(__file__).resolve().parent / "matrix_worker.py"


def worker_argv(spec: CellSpec, json_out: Path, partitions: int, limit_bytes: int) -> list[str]:
    """Build the worker subprocess argv for one cell."""
    row = spec if isinstance(spec, RosterRow) else RosterRow(**spec.model_dump())
    return [
        sys.executable,
        str(_WORKER_PATH),
        "--operator",
        row.operator,
        "--multiplier",
        str(row.multiplier),
        "--kind",
        row.kind,
        "--sql",
        row.sql,
        "--refusal-names",
        ",".join(row.refusal_names),
        "--conf",
        json.dumps(row.conf),
        "--right",
        row.right or "",
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


def plan_out_path(scratch_dir: Path, spec: CellSpec) -> Path:
    """Return the worker's plan-text path for one cell."""
    return scratch_dir / f"{spec.operator}-{spec.multiplier}x.plan.txt"
