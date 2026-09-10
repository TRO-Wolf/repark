"""Parent-side spill-matrix core: the outcome vocabulary, the classifier, the subprocess runner."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Final

from pydantic import BaseModel, ConfigDict

COMPLETED: Final[str] = "completed"
SPILLED: Final[str] = "spilled"
REFUSED: Final[str] = "refused"
KILLED: Final[str] = "KILLED"
OUTCOME_VOCABULARY: Final[frozenset[str]] = frozenset({COMPLETED, SPILLED, REFUSED})
REFUSAL_ERROR_TYPES: Final[frozenset[str]] = frozenset(
    {"MemoryError", "PySparkException", "AnalysisException"}
)

_HARNESS_DIR: Final[Path] = Path(__file__).resolve().parent
_BENCH_DIR: Final[Path] = _HARNESS_DIR.parents[1] / "bench"
_STDERR_TAIL: Final[int] = 600


class CellSpec(BaseModel):
    """One matrix cell: the operator, its input multiple, its SQL and its refusal node names."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operator: str
    multiplier: int
    sql: str
    refusal_names: tuple[str, ...]


class CellRecord(BaseModel):
    """One cell's folded outcome with the observability the ledger cites."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operator: str = ""
    multiplier: int = 0
    outcome: str = KILLED
    returncode: int | None = None
    spill_bytes: int | None = None
    spill_count: int | None = None
    error_type: str | None = None
    message: str | None = None
    wall_ms: float = 0.0
    rlimit_as_bytes: int | None = None
    vm_size_at_cap: int | None = None


class MatrixKilledError(RuntimeError):
    """A cell was KILLED: the matrix fails and the verdict is never folded into an outcome."""


def classify_cell(
    returncode: int, payload: dict[str, Any] | None, refusal_names: tuple[str, ...]
) -> str:
    """Fold one subprocess result into spilled / completed / refused, else KILLED."""
    if payload is None or returncode != 0:
        return KILLED
    outcome = payload.get("outcome")
    if outcome == COMPLETED:
        spill_bytes = payload.get("spill_bytes")
        if not isinstance(spill_bytes, int) or isinstance(spill_bytes, bool) or spill_bytes < 0:
            return KILLED
        return SPILLED if spill_bytes > 0 else COMPLETED
    if outcome == REFUSED:
        error_type = payload.get("error_type")
        message = payload.get("message")
        if error_type not in REFUSAL_ERROR_TYPES or not isinstance(message, str):
            return KILLED
        if error_type != "MemoryError" and not any(name in message for name in refusal_names):
            return KILLED
        return REFUSED
    return KILLED


def require_no_killed_cells(records: list[CellRecord]) -> None:
    """Raise unless every record folded to one of the three outcomes."""
    killed = [
        f"{record.operator or 'unknown'}-{record.multiplier}x"
        for record in records
        if record.outcome not in OUTCOME_VOCABULARY
    ]
    if killed:
        raise MatrixKilledError(f"KILLED cells never fold into an outcome: {', '.join(killed)}")


def is_loud_refusal(error: BaseException, refusal_names: tuple[str, ...]) -> bool:
    """True when the worker may report refused: a MemoryError, or the family naming the operator."""
    if isinstance(error, MemoryError):
        return True
    try:
        from repark.errors import PySparkException
    except ImportError:
        return False
    return isinstance(error, PySparkException) and any(name in str(error) for name in refusal_names)


def _int_or_none(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _str_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _read_payload(json_out: Path) -> dict[str, Any] | None:
    if not json_out.is_file():
        return None
    try:
        payload = json.loads(json_out.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def _worker_env(scratch_dir: Path) -> dict[str, str]:
    env = dict(os.environ)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(_BENCH_DIR) if not existing else f"{_BENCH_DIR}{os.pathsep}{existing}"
    tmp = scratch_dir / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    env["TMPDIR"] = str(tmp)
    return env


def run_cell(
    argv: list[str],
    json_out: Path,
    scratch_dir: Path,
    timeout_s: float,
    *,
    operator: str = "",
    multiplier: int = 0,
    refusal_names: tuple[str, ...] = (),
) -> CellRecord:
    """Run one cell subprocess to completion or its timeout, then fold the outcome."""
    started = time.perf_counter()
    proc = subprocess.Popen(
        argv,
        env=_worker_env(scratch_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _, stderr_bytes = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        _, stderr_bytes = proc.communicate()
    wall_ms = (time.perf_counter() - started) * 1000.0
    payload = _read_payload(json_out)
    returncode = proc.returncode if proc.returncode is not None else -1
    fields: dict[str, Any] = {
        "operator": operator,
        "multiplier": multiplier,
        "outcome": classify_cell(returncode, payload, refusal_names),
        "returncode": returncode,
        "message": _str_or_none((stderr_bytes or b"").decode("utf-8", "replace")[-_STDERR_TAIL:]),
        "wall_ms": wall_ms,
    }
    if payload is not None:
        fields["spill_bytes"] = _int_or_none(payload.get("spill_bytes"))
        fields["spill_count"] = _int_or_none(payload.get("spill_count"))
        fields["error_type"] = _str_or_none(payload.get("error_type"))
        fields["rlimit_as_bytes"] = _int_or_none(payload.get("rlimit_as_bytes"))
        fields["vm_size_at_cap"] = _int_or_none(payload.get("vm_size_at_cap"))
        worker_message = _str_or_none(payload.get("message"))
        if worker_message is not None:
            fields["message"] = worker_message[:_STDERR_TAIL]
    return CellRecord(**fields)
