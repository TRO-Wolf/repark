"""MERGE wall-clock matrix: 1M/10M x narrow/wide x K (local-fs Iceberg).

Measurement-only extension of the write bench. Pins resource knobs (rule 10):
``spark.sql.shuffle.partitions`` + ``repark.write.max-concurrent-files``.
Never AWS. MoR MERGE is the load-bearing path (RePark-owned); COW is recorded
per cell for ratio disclosure.
"""

from __future__ import annotations

import json
import logging
import platform
import shutil
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from .runner import (
    StageTiming,
    _warehouse_stats,
    max_rss_kb,
    probe_release_build,
)
from .schemas import (
    WIDE_FLOAT_COLS,
    Width,
    bytes_per_row_estimate,
    expected_rows_after_merge,
    merge_source_plan,
    write_synthetic_parquet,
)

LOGGER = logging.getLogger(__name__)

CATALOG: Final[str] = "merge_bench"
NAMESPACE: Final[str] = "bench"
DEFAULT_MERGE_K: Final[tuple[int, ...]] = (1, 2, 4, 8)
DEFAULT_ROW_COUNTS: Final[tuple[int, ...]] = (1_000_000, 10_000_000)
DEFAULT_WIDTHS: Final[tuple[Width, ...]] = ("narrow", "wide")
# Rule 10: pin shuffle partitions — never inherit host/test defaults.
PINNED_SHUFFLE_PARTITIONS: Final[int] = 8
# Fixed file-size so K is the sole write-concurrency axis for MERGE.
PINNED_TARGET_FILE_SIZE: Final[int] = 256 * 1024 * 1024

MergeMode = Literal["mor", "cow"]


class MergeCellResult(BaseModel):
    """One (rows, width, K) MERGE cell - MoR + optional COW stages."""

    model_config = ConfigDict(extra="forbid")

    target_rows: int
    source_rows: int
    width: str
    concurrency: int
    stages: list[StageTiming]
    wall_total_s: float
    rss_peak_kb: int
    warehouse_bytes: int
    data_file_count: int
    row_count_after_merge: int | None
    expected_rows: int
    error: str | None = None

    @property
    def merge_mor_s(self) -> float | None:
        return _stage(self.stages, "merge_mor")

    @property
    def merge_cow_s(self) -> float | None:
        return _stage(self.stages, "merge_cow")

    @property
    def ctas_s(self) -> float | None:
        return _stage(self.stages, "ctas_target_mor")


class MergeBoard(BaseModel):
    """MERGE matrix scoreboard."""

    model_config = ConfigDict(extra="forbid")

    row_counts: list[int]
    widths: list[str]
    k_values: list[int]
    source_fraction: float
    cells: list[MergeCellResult] = Field(default_factory=list)
    environment: dict[str, str] = Field(default_factory=dict)
    findings: list[str] = Field(default_factory=list)
    release_build_disclosed: bool = False
    pinned_knobs: dict[str, str] = Field(default_factory=dict)


def _stage(stages: Sequence[StageTiming], name: str) -> float | None:
    for stage in stages:
        if stage.name == name:
            return stage.seconds
    return None


def _fresh_warehouse(root: Path, label: str) -> Path:
    path = root / label
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_merge_cell(
    *,
    target_rows: int,
    width: Width,
    concurrency: int,
    warehouse_root: Path,
    cell_label: str,
    source_fraction: float = 0.10,
    run_cow: bool = True,
) -> MergeCellResult:
    """Seed target -> MoR MERGE (and optional COW) for one cell.

    Source plan: ~half UPDATE overlap, ~half NOT MATCHED INSERT.
    Knobs pinned per rule 10 (shuffle partitions + K + target-file-size).
    """
    from repark import ReparkSession

    source_rows, id_start = merge_source_plan(target_rows, source_fraction=source_fraction)
    expected = expected_rows_after_merge(
        target_rows=target_rows,
        source_rows=source_rows,
        id_start_source=id_start,
    )
    warehouse = _fresh_warehouse(warehouse_root, cell_label)
    stages: list[StageTiming] = []
    error: str | None = None
    row_count: int | None = None
    rss_before = max_rss_kb()
    seed_dir = warehouse / "_seed"
    seed_dir.mkdir(parents=True, exist_ok=True)
    target_parquet = seed_dir / "target.parquet"
    source_parquet = seed_dir / "source.parquet"
    write_synthetic_parquet(target_parquet, rows=target_rows, width=width, value_offset=0.0)
    write_synthetic_parquet(
        source_parquet,
        rows=source_rows,
        width=width,
        id_start=id_start,
        value_offset=0.5,
    )

    builder = (
        ReparkSession.builder.appName(f"merge-bench-k{concurrency}-{width}-{target_rows}")
        .config("repark.write.max-concurrent-files", str(concurrency))
        .config("spark.sql.shuffle.partitions", str(PINNED_SHUFFLE_PARTITIONS))
    )
    spark = builder.getOrCreate()
    try:
        spark.register_memory_catalog(CATALOG, str(warehouse))
        namespace_location = (warehouse / NAMESPACE).resolve()
        namespace_location.mkdir(parents=True, exist_ok=True)
        from repark.spark._idents import sql_string_literal

        location_sql = sql_string_literal(str(namespace_location))
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE} LOCATION {location_sql}")
        target_mor = f"{CATALOG}.{NAMESPACE}.target_mor"
        target_cow = f"{CATALOG}.{NAMESPACE}.target_cow"
        props_base = (
            f"'format-version' = '2', 'write.target-file-size-bytes' = '{PINNED_TARGET_FILE_SIZE}'"
        )
        props_mor = (
            f"{props_base}, "
            "'write.delete.mode'='merge-on-read', "
            "'write.update.mode'='merge-on-read', "
            "'write.merge.mode'='merge-on-read'"
        )

        t0 = time.perf_counter()
        spark.read.parquet(str(target_parquet)).createOrReplaceTempView("seed_target")
        spark.read.parquet(str(source_parquet)).createOrReplaceTempView("src")
        stages.append(StageTiming("seed_parquet_views", time.perf_counter() - t0))

        t0 = time.perf_counter()
        spark.sql(
            f"CREATE TABLE {target_mor} USING iceberg "
            f"TBLPROPERTIES ({props_mor}) AS SELECT * FROM seed_target"
        )
        stages.append(StageTiming("ctas_target_mor", time.perf_counter() - t0))

        set_clause = _merge_set_clause(width)
        merge_sql_mor = (
            f"MERGE INTO {target_mor} AS t USING src AS s ON t.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET {set_clause} "
            "WHEN NOT MATCHED THEN INSERT *"
        )
        t0 = time.perf_counter()
        spark.sql(merge_sql_mor)
        stages.append(StageTiming("merge_mor", time.perf_counter() - t0))

        t0 = time.perf_counter()
        count_rows = spark.sql(f"SELECT count(*) AS c FROM {target_mor}").collect()
        row_count = int(count_rows[0][0])
        stages.append(StageTiming("count_after_mor", time.perf_counter() - t0))
        if row_count != expected:
            error = (
                f"row_count_mismatch_mor: got {row_count} expected {expected} "
                f"(target={target_rows} source={source_rows} id_start={id_start})"
            )
            LOGGER.error("cell %s %s", cell_label, error)

        if run_cow and error is None:
            t0 = time.perf_counter()
            spark.sql(
                f"CREATE TABLE {target_cow} USING iceberg "
                f"TBLPROPERTIES ({props_base}) AS SELECT * FROM seed_target"
            )
            stages.append(StageTiming("ctas_target_cow", time.perf_counter() - t0))
            merge_sql_cow = (
                f"MERGE INTO {target_cow} AS t USING src AS s ON t.id = s.id "
                f"WHEN MATCHED THEN UPDATE SET {set_clause} "
                "WHEN NOT MATCHED THEN INSERT *"
            )
            t0 = time.perf_counter()
            spark.sql(merge_sql_cow)
            stages.append(StageTiming("merge_cow", time.perf_counter() - t0))
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        LOGGER.exception("merge cell %s failed", cell_label)
    finally:
        try:
            spark.stop()
        except Exception:
            LOGGER.warning("spark.stop failed for %s", cell_label, exc_info=True)

    wall_total = sum(stage.seconds for stage in stages)
    warehouse_bytes, data_file_count = _warehouse_stats(warehouse)
    # Seed parquet lives under warehouse/_seed, which _warehouse_stats counts;
    # recompute bytes and data files over the namespace directory only.
    ns_path = warehouse / NAMESPACE
    if ns_path.is_dir():
        warehouse_bytes, data_file_count = _warehouse_stats(ns_path)
    return MergeCellResult(
        target_rows=target_rows,
        source_rows=source_rows,
        width=width,
        concurrency=concurrency,
        stages=stages,
        wall_total_s=wall_total,
        rss_peak_kb=max(max_rss_kb(), rss_before),
        warehouse_bytes=warehouse_bytes,
        data_file_count=data_file_count,
        row_count_after_merge=row_count,
        expected_rows=expected,
        error=error,
    )


def _merge_set_clause(width: Width) -> str:
    if width == "narrow":
        return "t.v = s.v"
    cols = [f"t.f{index} = s.f{index}" for index in range(WIDE_FLOAT_COLS)]
    return ", ".join(cols)


def run_merge_matrix(
    *,
    row_counts: Sequence[int] = DEFAULT_ROW_COUNTS,
    widths: Sequence[Width] = DEFAULT_WIDTHS,
    k_values: Sequence[int] = DEFAULT_MERGE_K,
    source_fraction: float = 0.10,
    warehouse_root: Path | None = None,
    assert_release: bool = False,
    run_cow: bool = True,
) -> MergeBoard:
    """Full MERGE matrix: rows x width x K."""
    for count in row_counts:
        if count < 1:
            msg = f"row count must be >= 1; got {count}"
            raise ValueError(msg)
    for k_value in k_values:
        if k_value < 1:
            msg = f"concurrency K must be >= 1; got {k_value}"
            raise ValueError(msg)
    for width in widths:
        if width not in ("narrow", "wide"):
            msg = f"width must be narrow|wide; got {width!r}"
            raise ValueError(msg)

    if warehouse_root is None:
        warehouse_root = Path.home() / ".cache" / "repark-write-bench" / "merge"
    warehouse_root = warehouse_root.expanduser()
    warehouse_root.mkdir(parents=True, exist_ok=True)

    release_ok, release_reason = probe_release_build(assert_release=assert_release)
    board = MergeBoard(
        row_counts=list(row_counts),
        widths=list(widths),
        k_values=list(k_values),
        source_fraction=source_fraction,
        environment={
            "machine": platform.node(),
            "platform": platform.platform(),
            "python": platform.python_version(),
            "release_build_reason": release_reason,
        },
        release_build_disclosed=release_ok,
        pinned_knobs={
            "repark.write.max-concurrent-files": "per-cell K",
            "spark.sql.shuffle.partitions": str(PINNED_SHUFFLE_PARTITIONS),
            "write.target-file-size-bytes": str(PINNED_TARGET_FILE_SIZE),
            "write.merge.mode (MoR)": "merge-on-read",
        },
    )
    board.findings.extend(
        [
            "Local-fs Iceberg MERGE (memory catalog). No AWS / no S3 / no Glue.",
            f"Release disclosed={release_ok}: {release_reason}",
            "Rule 10 knobs pinned: spark.sql.shuffle.partitions="
            f"{PINNED_SHUFFLE_PARTITIONS}; write.target-file-size-bytes="
            f"{PINNED_TARGET_FILE_SIZE}; K = repark.write.max-concurrent-files.",
            f"Source plan: ~{source_fraction:.0%} of target rows; half UPDATE overlap, "
            "half NOT MATCHED INSERT past target id space.",
            "MoR MERGE is RePark-owned (repark-write); K is load-bearing on the write path. "
            "COW MERGE recorded for mor/cow ratio when run_cow=True.",
            "Peak RSS is process-lifetime ru_maxrss (KiB); non-decreasing across sequential "
            "cells - not independent per-cell samples.",
            f"Narrow = id+v (~{bytes_per_row_estimate('narrow')} B/row raw); "
            f"wide = id+{32} f64 (~{bytes_per_row_estimate('wide')} B/row raw).",
        ]
    )
    if not release_ok:
        LOGGER.warning("release build UNVERIFIED: %s", release_reason)

    for rows in row_counts:
        for width in widths:
            for k_value in k_values:
                label = f"r{rows}_{width}_k{k_value}"
                LOGGER.info("merge cell start %s", label)
                cell = run_merge_cell(
                    target_rows=rows,
                    width=width,  # type: ignore[arg-type]
                    concurrency=k_value,
                    warehouse_root=warehouse_root,
                    cell_label=label,
                    source_fraction=source_fraction,
                    run_cow=run_cow,
                )
                board.cells.append(cell)
                LOGGER.info(
                    "merge cell done %s wall=%.3fs mor=%.3fs cow=%.3fs rss_kb=%s err=%s",
                    label,
                    cell.wall_total_s,
                    cell.merge_mor_s or -1.0,
                    cell.merge_cow_s or -1.0,
                    cell.rss_peak_kb,
                    cell.error,
                )
    return board


def render_merge_markdown(board: MergeBoard) -> str:
    """Human MERGE report section (tables + findings)."""
    lines: list[str] = [
        "# R-WRITE-BENCH extension - MERGE matrix (local-fs)",
        "",
        "**Axes:** target rows x width x K (`repark.write.max-concurrent-files`).",
        "",
        "## Environment",
        "",
        f"- Machine: `{board.environment.get('machine', '?')}`",
        f"- Platform: `{board.environment.get('platform', '?')}`",
        f"- Python: `{board.environment.get('python', '?')}`",
        f"- Row counts: {board.row_counts}",
        f"- Widths: {board.widths}",
        f"- K axis: {board.k_values}",
        f"- Source fraction: {board.source_fraction}",
        f"- Release build disclosed: **{board.release_build_disclosed}** "
        f"({board.environment.get('release_build_reason', '')})",
        f"- Pinned knobs (rule 10): `{board.pinned_knobs}`",
        "- Object store: **local filesystem**. **No AWS.**",
        "",
        "## Findings / disclosures",
        "",
    ]
    for finding in board.findings:
        lines.append(f"- {finding}")
    lines.extend(
        [
            "",
            "## Matrix - MERGE wall + RSS",
            "",
            "| rows | width | K | source_rows | seed_s | ctas_mor_s | merge_mor_s | "
            "merge_cow_s | wall_total_s | rss_peak_KiB | warehouse_bytes | data_files | "
            "rows | expected | error |",
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for cell in board.cells:
        err = cell.error.replace("|", "\\|") if cell.error else ""
        lines.append(
            f"| {cell.target_rows} | {cell.width} | {cell.concurrency} | {cell.source_rows} | "
            f"{_fmt(_stage(cell.stages, 'seed_parquet_views'))} | "
            f"{_fmt(cell.ctas_s)} | {_fmt(cell.merge_mor_s)} | {_fmt(cell.merge_cow_s)} | "
            f"{_fmt(cell.wall_total_s)} | {cell.rss_peak_kb} | {cell.warehouse_bytes} | "
            f"{cell.data_file_count} | "
            f"{cell.row_count_after_merge if cell.row_count_after_merge is not None else ''} | "
            f"{cell.expected_rows} | {err} |"
        )

    lines.extend(["", "## MoR MERGE wall (seconds) - pivot K", ""])
    lines.append("| rows | width | " + " | ".join(f"K={k}" for k in board.k_values) + " |")
    lines.append("|---:|---|" + "|".join(["---:" for _ in board.k_values]) + "|")
    for rows in board.row_counts:
        for width in board.widths:
            row = [f"| {rows} | {width} |"]
            for k_value in board.k_values:
                cell = _find(board, rows, width, k_value)
                if cell is None or cell.merge_mor_s is None:
                    row.append(" - |")
                else:
                    row.append(f" {cell.merge_mor_s:.3f} |")
            lines.append("".join(row))

    lines.extend(["", "## K scaling notes (MoR MERGE wall vs K=1)", ""])
    for rows in board.row_counts:
        for width in board.widths:
            base = _find(board, rows, width, 1)
            if base is None or base.merge_mor_s is None or base.merge_mor_s <= 0:
                lines.append(f"- rows={rows} {width}: no K=1 baseline")
                continue
            parts = []
            for k_value in board.k_values:
                cell = _find(board, rows, width, k_value)
                if cell is None or cell.merge_mor_s is None:
                    continue
                speedup = base.merge_mor_s / cell.merge_mor_s if cell.merge_mor_s > 0 else 0.0
                parts.append(f"K={k_value} {cell.merge_mor_s:.3f}s ({speedup:.2f}x)")
            lines.append(f"- rows={rows} {width}: " + "; ".join(parts))

    lines.extend(
        [
            "",
            "## Out of scope",
            "",
            "- No product / fork / knob-default changes.",
            "- Local FS does not prove S3 MERGE upload stall.",
            "",
        ]
    )
    return "\n".join(lines)


def _find(board: MergeBoard, rows: int, width: str, concurrency: int) -> MergeCellResult | None:
    for cell in board.cells:
        if cell.target_rows == rows and cell.width == width and cell.concurrency == concurrency:
            return cell
    return None


def _fmt(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.3f}"


def board_to_dict(board: MergeBoard) -> dict[str, Any]:
    return {
        "row_counts": board.row_counts,
        "widths": board.widths,
        "k_values": board.k_values,
        "source_fraction": board.source_fraction,
        "environment": board.environment,
        "findings": board.findings,
        "release_build_disclosed": board.release_build_disclosed,
        "pinned_knobs": board.pinned_knobs,
        "cells": [
            {
                **{key: value for key, value in cell.model_dump().items() if key != "stages"},
                "stages": [{"name": stage.name, "seconds": stage.seconds} for stage in cell.stages],
                "merge_mor_s": cell.merge_mor_s,
                "merge_cow_s": cell.merge_cow_s,
                "ctas_s": cell.ctas_s,
            }
            for cell in board.cells
        ],
    }


def write_merge_json(board: MergeBoard, path: Path) -> None:
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(board_to_dict(board), indent=2, sort_keys=True) + "\n")
