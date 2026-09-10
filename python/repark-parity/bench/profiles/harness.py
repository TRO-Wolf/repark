"""PROFILES-1 timing harness: quiet-box guard, cell timing, CSV rows, medians."""

from __future__ import annotations

import csv
import statistics
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Final

from pydantic import BaseModel

CSV_COLUMNS: Final[tuple[str, ...]] = ("dataset", "query", "knob", "value", "repetition", "seconds")

DEFAULT_REPEATS: Final[int] = 3

BASELINE_VALUE: Final[str] = "@default"


class BoxBusyError(RuntimeError):
    """The box is not quiet enough for a timed run."""


class SmokeConfig(BaseModel):
    """Tiny scale for one `--smoke` proof run."""

    repeats: int
    tpch_sf: float
    iceberg_files: int
    futures_rows: int
    append_files: int


def smoke_config() -> SmokeConfig:
    """Return the tiny scale `--smoke` runs at."""
    from profiles import datasets, queries

    return SmokeConfig(
        repeats=1,
        tpch_sf=datasets.SMOKE_TPCH_SF,
        iceberg_files=datasets.SMOKE_ICEBERG_FILES,
        futures_rows=datasets.SMOKE_FUTURES_ROWS,
        append_files=queries.SMOKE_APPEND_FILES,
    )


def csv_header() -> tuple[str, ...]:
    """Return the CSV column names in write order."""
    return CSV_COLUMNS


def median_seconds(values: list[float]) -> float:
    """Return the median of one cell's repetitions."""
    return statistics.median(values)


def require_quiet_box() -> None:
    """Refuse unless no JVM runs, checked with `pgrep -f java`."""
    try:
        found = subprocess.run(["pgrep", "-f", "java"], capture_output=True, text=True, check=False)
    except FileNotFoundError as error:
        msg = "pgrep not found; cannot prove the box is quiet, refusing the timed run"
        raise BoxBusyError(msg) from error
    if found.returncode == 0 and found.stdout.strip():
        msg = (
            "refusing the timed run: `pgrep -f java` found a JVM "
            f"(pids: {found.stdout.strip()}); stop it first (one-JVM rule)"
        )
        raise BoxBusyError(msg)


def measure(operation: Callable[[], None], repeats: int) -> list[float]:
    """Time one operation per repetition with a quiet-box check first."""
    seconds: list[float] = []
    for _ in range(repeats):
        require_quiet_box()
        start = time.perf_counter()
        operation()
        seconds.append(time.perf_counter() - start)
    return seconds


def write_cell_rows(
    path: Path,
    dataset: str,
    query: str,
    knob: str,
    value: str,
    seconds: list[float],
) -> None:
    """Append one CSV row per repetition, writing the header once."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fresh = not path.is_file() or path.stat().st_size == 0
    with path.open("a", newline="") as handle:
        writer = csv.writer(handle)
        if fresh:
            writer.writerow(list(CSV_COLUMNS))
        for index, elapsed in enumerate(seconds, start=1):
            writer.writerow([dataset, query, knob, value, index, f"{elapsed:.6f}"])
