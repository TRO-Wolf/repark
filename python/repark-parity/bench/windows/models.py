"""Pydantic records for W-0 results."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ProbeRow(BaseModel):
    """Live class of one roster name on a sliding frame."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    sql_expr: str
    intake_class: str
    outcome: str
    message: str | None = None


class CellTiming(BaseModel):
    """Wall milliseconds for one engine on one cell."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    engine: str
    outcome: str
    warmup: int
    iterations: int
    samples_ms: list[float] = Field(default_factory=list)
    median_ms: float | None = None
    peak_rss_bytes: int | None = None
    plan_tokens: list[str] = Field(default_factory=list)
    answer: Any = None
    message: str | None = None
    version: str | None = None


class CellResult(BaseModel):
    """One named shape (sliding / constant / unpartitioned / lead-lag / memory)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    label: str
    sql: str
    rows: int
    timings: list[CellTiming]


class RunResult(BaseModel):
    """One W-0 run: versions, machine profile, probe, cells, dataset sizes."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    scale: str
    seed: int
    engine_version: str
    duckdb_version: str | None
    pyspark_version: str | None
    pyspark_skip_reason: str | None
    duckdb_skip_reason: str | None
    native_build: str
    machine: dict[str, str]
    dataset_bytes: dict[str, int]
    probe: list[ProbeRow]
    cells: list[CellResult]
    peak_rss_bytes: int
    wall_seconds: float
    scratch_deleted: bool
