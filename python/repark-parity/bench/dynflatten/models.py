"""Pydantic records for the dynamicFlatten measurement run."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EngineTiming(BaseModel):
    """Wall and resource sample for one engine on one fixture."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    engine: str
    outcome: str
    warmup: int
    iterations: int
    rewrite_ms: list[float] = Field(default_factory=list)
    execute_ms: list[float] = Field(default_factory=list)
    median_rewrite_ms: float | None = None
    median_execute_ms: float | None = None
    median_wall_ms: float | None = None
    min_rewrite_ms: float | None = None
    min_execute_ms: float | None = None
    min_wall_ms: float | None = None
    target_partitions: int | None = None
    peak_rss_bytes: int | None = None
    rows_out: int | None = None
    plan_nodes: int | None = None
    rewrite_passes: int | None = None
    schema_walks: int | None = None
    struct_expansions: int | None = None
    list_explodes: int | None = None
    unnest_nodes: int | None = None
    projection_nodes: int | None = None
    message: str | None = None
    version: str | None = None


class FixtureResult(BaseModel):
    """One shape at one row count, both engines."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    shape: str
    kind: str
    struct_depth: int
    list_width: int | None
    rows_in: int
    parquet_bytes: int
    digest: str
    repark: EngineTiming
    spark: EngineTiming
    repark_all_cores: EngineTiming | None = None
    row_set_equal: bool | None = None
    wall_ratio_repark_over_spark: float | None = None
    isolation: bool = False


class CandidateShare(BaseModel):
    """Measured wall share for one H-3 intake candidate."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    wall_share: float
    evidence: str
    verdict: str
    projected_gain: str | None = None
    isolated_cost_ms: float | None = None
    noise_floor_ms: float | None = None
    cost_over_noise: float | None = None


class RunResult(BaseModel):
    """One measurement run: machine, fixtures, candidate ranking."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    scale: str
    seed: int
    native_build: str
    machine: dict[str, str]
    engine_version: str
    pyspark_version: str | None
    pyspark_skip_reason: str | None
    fixtures: list[FixtureResult]
    candidates: list[CandidateShare]
    peak_rss_bytes: int
    wall_seconds: float
    run_date: str
    native_is_release: bool
    target_partitions: int
    spark_threads: int
    noise_floor_ms: float | None = None
    noise_floor_shape: str | None = None
    noise_samples: list[float] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)
