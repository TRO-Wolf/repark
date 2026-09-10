"""The Family protocol every torture family implements and its shared output record."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Protocol, runtime_checkable

import pyarrow as pa
from pydantic import BaseModel, ConfigDict

PARQUET_NAME = "data.parquet"
CSV_NAME = "data.csv"
MAX_ROWS = 10_000_000
_REPO_MARKER = "AGENTS.md"


class FamilyOutput(BaseModel):
    """The files one generate call wrote and the row count they carry."""

    model_config = ConfigDict(extra="forbid")

    rows: int
    parquet_path: Path
    csv_path: Path


@runtime_checkable
class Family(Protocol):
    """What every torture family implementor exposes to the CLI and the suite."""

    @property
    def name(self) -> str:
        """The registry key the CLI and the suite address this family by."""
        ...

    def generate(self, rows: int, seed: int, out: Path) -> FamilyOutput:
        """Write the family's Parquet and CSV files under out and report what was written."""
        ...

    def expected_read_schema(self, fmt: Literal["parquet", "csv"]) -> pa.Schema | None:
        """The declared read expectation for one format, or None when no schema contract."""
        ...


def refuse_repository_output(out: Path) -> None:
    """Refuse an output directory inside the repository tree so data is never committed."""
    resolved = out.resolve()
    for parent in (resolved, *resolved.parents):
        if (parent / _REPO_MARKER).is_file():
            raise ValueError(f"torture output must stay outside the repository: {out}")


def refuse_bad_rows(rows: int) -> None:
    """Refuse a row budget the generators cannot honor."""
    if not 1 <= rows <= MAX_ROWS:
        raise ValueError(f"rows must be between 1 and {MAX_ROWS}: {rows}")


def refuse_bad_seed(seed: int) -> None:
    """Refuse a negative seed so every RNG stays deterministic."""
    if seed < 0:
        raise ValueError(f"seed must be non-negative: {seed}")
