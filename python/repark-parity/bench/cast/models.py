"""Pydantic records for the PERF-CAST-1 CAST-cost cells."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class CellRecord(BaseModel):
    """One shape by CAST-count cell: repetitions, medians, and SQL size."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    shape: str
    cast_count: int
    row_count: int
    native_debug: bool
    repetitions: list[float]
    plan_repetitions: list[float]
    execute_repetitions: list[float]
    median_seconds: float
    median_plan_seconds: float
    median_execute_seconds: float
    sql_bytes: int
    cast_in_sql: int
    execute_kind: str


class PhaseSample(BaseModel):
    """One named wall-clock sample from an attribution probe."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    seconds: float
    note: str = ""


class AttributionRecord(BaseModel):
    """Where the wall goes for one shape at one CAST count."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    shape: str
    cast_count: int
    row_count: int
    native_debug: bool
    phases: list[PhaseSample]
    optimizer_pass_names: list[str] = Field(default_factory=list)
    elapsed_compute_seconds: float | None = None
    physical_cast_count: int | None = None
    plan_text_bytes: int | None = None
    profiler: str = ""
    profiler_summary: str = ""
