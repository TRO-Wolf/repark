"""Pydantic records for one H3-SPILL-1 matrix cell."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

OUTCOMES: tuple[str, ...] = (
    "ok",
    "spilled",
    "degraded",
    "clean_error",
    "abort",
    "abort_at_cap",
    "internal_error",
    "timeout",
    "wrong",
    "error",
)


class NodeMetrics(BaseModel):
    """Aggregated `EXPLAIN ANALYZE` metrics for one physical-operator class."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    node: str
    instances: int = 0
    spill_count: int = 0
    spilled_bytes: int = 0
    spilled_rows: int = 0
    peak_mem_used: int = 0
    skipped_aggregation_rows: int = 0
    output_rows: int = 0
    output_bytes: int = 0


class CellRecord(BaseModel):
    """One matrix cell: operator x pool x scale, measured in its own subprocess."""

    model_config = ConfigDict(extra="forbid")

    operator: str
    pool: str
    scale: int
    outcome: str
    spill_count: int = 0
    spilled_bytes: int = 0
    degraded_rows: int = 0
    peak_rss_bytes: int | None = None
    wall_ms: float | None = None
    load_start: float | None = None
    load_end: float | None = None
    as_cap_bytes: int | None = None
    rss_cap_bytes: int | None = None
    answer_digest: str | None = None
    run_digests: list[str | None] = Field(default_factory=list)
    digest_kind: str = "none"
    digest_error: str | None = None
    rows_out: int | None = None
    message: str | None = None
    returncode: int | None = None
    nodes: list[NodeMetrics] = Field(default_factory=list)
    runs: list[str] = Field(default_factory=list)
    version: str | None = None


class MatrixReport(BaseModel):
    """Every measured cell plus the host header a baseline needs."""

    model_config = ConfigDict(extra="forbid")

    host: dict[str, Any]
    cells: list[CellRecord] = Field(default_factory=list)
