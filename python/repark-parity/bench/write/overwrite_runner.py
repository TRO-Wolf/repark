"""INSERT OVERWRITE peak-RSS bench (OTH-004 evidence).

Non-empty INSERT OVERWRITE materializes the source via ``collect()`` -> MemTable
(``insert_overwrite_from_materialized_source`` in repark-sql). Peak RSS should
scale with source size; CTAS does not take this path.

Axes: source rows ∈ {1M, 10M} x width ∈ {narrow, wide}. K is pinned (not swept)
because the materialize path is single-collect, not FanoutWriter-bound.
Rule 10: pin shuffle partitions + K + target-file-size.
"""

from __future__ import annotations

import json
import logging
import platform
import shutil
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Final

from .runner import (
    StageTiming,
    _warehouse_stats,
    format_bytes,
    max_rss_kb,
    probe_release_build,
)
from .schemas import (
    Width,
    bytes_per_row_estimate,
    write_synthetic_parquet,
)

LOGGER = logging.getLogger(__name__)

CATALOG: Final[str] = "ow_bench"
NAMESPACE: Final[str] = "bench"
DEFAULT_ROW_COUNTS: Final[tuple[int, ...]] = (1_000_000, 10_000_000)
DEFAULT_WIDTHS: Final[tuple[Width, ...]] = ("narrow", "wide")
PINNED_SHUFFLE_PARTITIONS: Final[int] = 8
PINNED_K: Final[int] = 4
PINNED_TARGET_FILE_SIZE: Final[int] = 256 * 1024 * 1024
# Small baseline target so overwrite cost is dominated by source materialize.
BASELINE_TARGET_ROWS: Final[int] = 1_000


@dataclass
class OverwriteCellResult:
    """One (source_rows, width) INSERT OVERWRITE cell."""

    source_rows: int
    width: str
    baseline_target_rows: int
    stages: list[StageTiming]
    wall_total_s: float
    rss_before_overwrite_kb: int
    rss_peak_kb: int
    rss_delta_kb: int
    source_parquet_bytes: int
    warehouse_bytes: int
    data_file_count: int
    row_count_after: int | None
    error: str | None = None

    @property
    def overwrite_s(self) -> float | None:
        for stage in self.stages:
            if stage.name == "insert_overwrite":
                return stage.seconds
        return None

    @property
    def ctas_baseline_s(self) -> float | None:
        for stage in self.stages:
            if stage.name == "ctas_baseline":
                return stage.seconds
        return None


@dataclass
class OverwriteBoard:
    """INSERT OVERWRITE RSS scoreboard."""

    row_counts: list[int]
    widths: list[str]
    cells: list[OverwriteCellResult] = field(default_factory=list)
    environment: dict[str, str] = field(default_factory=dict)
    findings: list[str] = field(default_factory=list)
    release_build_disclosed: bool = False
    pinned_knobs: dict[str, str] = field(default_factory=dict)


def _fresh_warehouse(root: Path, label: str) -> Path:
    path = root / label
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_overwrite_cell(
    *,
    source_rows: int,
    width: Width,
    warehouse_root: Path,
    cell_label: str,
    baseline_target_rows: int = BASELINE_TARGET_ROWS,
) -> OverwriteCellResult:
    """CTAS tiny baseline -> INSERT OVERWRITE from large source; capture RSS delta."""
    from repark import ReparkSession

    warehouse = _fresh_warehouse(warehouse_root, cell_label)
    stages: list[StageTiming] = []
    error: str | None = None
    row_count: int | None = None
    seed_dir = warehouse / "_seed"
    seed_dir.mkdir(parents=True, exist_ok=True)
    baseline_parquet = seed_dir / "baseline.parquet"
    source_parquet = seed_dir / "source.parquet"
    write_synthetic_parquet(
        baseline_parquet, rows=baseline_target_rows, width=width, value_offset=0.0
    )
    write_synthetic_parquet(source_parquet, rows=source_rows, width=width, value_offset=1.0)
    source_parquet_bytes = source_parquet.stat().st_size

    builder = (
        ReparkSession.builder.appName(f"ow-bench-{width}-{source_rows}")
        .config("repark.write.max-concurrent-files", str(PINNED_K))
        .config("spark.sql.shuffle.partitions", str(PINNED_SHUFFLE_PARTITIONS))
    )
    spark = builder.getOrCreate()
    rss_before_ow = max_rss_kb()
    rss_after = rss_before_ow
    try:
        spark.register_memory_catalog(CATALOG, str(warehouse))
        namespace_location = (warehouse / NAMESPACE).resolve()
        namespace_location.mkdir(parents=True, exist_ok=True)
        location_sql = str(namespace_location).replace("'", "''")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE} LOCATION '{location_sql}'")
        table_fq = f"{CATALOG}.{NAMESPACE}.t_ow"
        props = (
            f"'format-version' = '2', 'write.target-file-size-bytes' = '{PINNED_TARGET_FILE_SIZE}'"
        )

        t0 = time.perf_counter()
        spark.read.parquet(str(baseline_parquet)).createOrReplaceTempView("baseline")
        spark.read.parquet(str(source_parquet)).createOrReplaceTempView("src_ow")
        stages.append(StageTiming("seed_parquet_views", time.perf_counter() - t0))

        t0 = time.perf_counter()
        spark.sql(
            f"CREATE TABLE {table_fq} USING iceberg "
            f"TBLPROPERTIES ({props}) AS SELECT * FROM baseline"
        )
        stages.append(StageTiming("ctas_baseline", time.perf_counter() - t0))

        # RSS sample immediately before the OTH-004 materialize path.
        rss_before_ow = max_rss_kb()
        t0 = time.perf_counter()
        spark.sql(f"INSERT OVERWRITE {table_fq} SELECT * FROM src_ow")
        stages.append(StageTiming("insert_overwrite", time.perf_counter() - t0))
        rss_after = max_rss_kb()

        t0 = time.perf_counter()
        count_rows = spark.sql(f"SELECT count(*) AS c FROM {table_fq}").collect()
        row_count = int(count_rows[0][0])
        stages.append(StageTiming("count_verify", time.perf_counter() - t0))
        if row_count != source_rows:
            error = (
                f"row_count_mismatch: got {row_count} expected {source_rows} "
                "(whole-table overwrite should match source)"
            )
            LOGGER.error("cell %s %s", cell_label, error)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        LOGGER.exception("overwrite cell %s failed", cell_label)
        rss_after = max_rss_kb()
    finally:
        try:
            spark.stop()
        except Exception:
            LOGGER.warning("spark.stop failed for %s", cell_label, exc_info=True)

    wall_total = sum(stage.seconds for stage in stages)
    ns_path = warehouse / NAMESPACE
    warehouse_bytes, data_file_count = _warehouse_stats(ns_path) if ns_path.is_dir() else (0, 0)
    rss_peak = max(max_rss_kb(), rss_after, rss_before_ow)
    rss_delta = max(0, rss_peak - rss_before_ow)
    return OverwriteCellResult(
        source_rows=source_rows,
        width=width,
        baseline_target_rows=baseline_target_rows,
        stages=stages,
        wall_total_s=wall_total,
        rss_before_overwrite_kb=rss_before_ow,
        rss_peak_kb=rss_peak,
        rss_delta_kb=rss_delta,
        source_parquet_bytes=source_parquet_bytes,
        warehouse_bytes=warehouse_bytes,
        data_file_count=data_file_count,
        row_count_after=row_count,
        error=error,
    )


def run_overwrite_matrix(
    *,
    row_counts: Sequence[int] = DEFAULT_ROW_COUNTS,
    widths: Sequence[Width] = DEFAULT_WIDTHS,
    warehouse_root: Path | None = None,
    assert_release: bool = False,
    baseline_target_rows: int = BASELINE_TARGET_ROWS,
) -> OverwriteBoard:
    """RSS matrix for INSERT OVERWRITE at each (rows, width)."""
    for count in row_counts:
        if count < 1:
            msg = f"row count must be >= 1; got {count}"
            raise ValueError(msg)
    for width in widths:
        if width not in ("narrow", "wide"):
            msg = f"width must be narrow|wide; got {width!r}"
            raise ValueError(msg)

    if warehouse_root is None:
        warehouse_root = Path.home() / ".cache" / "repark-write-bench" / "overwrite"
    warehouse_root = warehouse_root.expanduser()
    warehouse_root.mkdir(parents=True, exist_ok=True)

    release_ok, release_reason = probe_release_build(assert_release=assert_release)
    board = OverwriteBoard(
        row_counts=list(row_counts),
        widths=list(widths),
        environment={
            "machine": platform.node(),
            "platform": platform.platform(),
            "python": platform.python_version(),
            "release_build_reason": release_reason,
            "baseline_target_rows": str(baseline_target_rows),
        },
        release_build_disclosed=release_ok,
        pinned_knobs={
            "repark.write.max-concurrent-files": str(PINNED_K),
            "spark.sql.shuffle.partitions": str(PINNED_SHUFFLE_PARTITIONS),
            "write.target-file-size-bytes": str(PINNED_TARGET_FILE_SIZE),
        },
    )
    board.findings.extend(
        [
            "OTH-004 evidence: non-empty INSERT OVERWRITE collects source into MemTable "
            "(repark-sql insert_overwrite_from_materialized_source) - peak memory O(source).",
            "CTAS streams; overwrite materialize path does not. Compare RSS delta vs source size.",
            f"Release disclosed={release_ok}: {release_reason}",
            "Rule 10 knobs pinned: "
            f"spark.sql.shuffle.partitions={PINNED_SHUFFLE_PARTITIONS}, "
            f"K={PINNED_K}, target-file-size={PINNED_TARGET_FILE_SIZE}. "
            "K is NOT swept (materialize path is collect-bound).",
            f"Baseline target = {baseline_target_rows} rows (tiny); overwrite source = axis rows.",
            "rss_delta_kb = rss_peak - rss_before_overwrite (same process; cumulative "
            "ru_maxrss means delta may under-count if prior cells already raised the high-water).",
            "Each cell uses a fresh session; still process-lifetime ru_maxrss across the matrix.",
            f"Narrow ~{bytes_per_row_estimate('narrow')} B/row; "
            f"wide ~{bytes_per_row_estimate('wide')} B/row raw Arrow.",
            "Local-fs only. No AWS.",
        ]
    )
    if not release_ok:
        LOGGER.warning("release build UNVERIFIED: %s", release_reason)

    for rows in row_counts:
        for width in widths:
            label = f"ow_r{rows}_{width}"
            LOGGER.info("overwrite cell start %s", label)
            cell = run_overwrite_cell(
                source_rows=rows,
                width=width,  # type: ignore[arg-type]
                warehouse_root=warehouse_root,
                cell_label=label,
                baseline_target_rows=baseline_target_rows,
            )
            board.cells.append(cell)
            LOGGER.info(
                "overwrite cell done %s wall=%.3fs ow=%.3fs rss_before=%s peak=%s delta=%s err=%s",
                label,
                cell.wall_total_s,
                cell.overwrite_s or -1.0,
                cell.rss_before_overwrite_kb,
                cell.rss_peak_kb,
                cell.rss_delta_kb,
                cell.error,
            )
    return board


def render_overwrite_markdown(board: OverwriteBoard) -> str:
    """Human INSERT OVERWRITE RSS report section."""
    lines: list[str] = [
        "# R-WRITE-BENCH extension - INSERT OVERWRITE peak RSS (OTH-004)",
        "",
        "**Axes:** source rows x width. K pinned (not swept).",
        "",
        "## Environment",
        "",
        f"- Machine: `{board.environment.get('machine', '?')}`",
        f"- Platform: `{board.environment.get('platform', '?')}`",
        f"- Python: `{board.environment.get('python', '?')}`",
        f"- Source row counts: {board.row_counts}",
        f"- Widths: {board.widths}",
        f"- Baseline target rows: {board.environment.get('baseline_target_rows', '?')}",
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
            "## Matrix - INSERT OVERWRITE wall + peak RSS",
            "",
            "| source_rows | width | source_parquet | seed_s | ctas_s | overwrite_s | "
            "wall_total_s | rss_before_KiB | rss_peak_KiB | rss_delta_KiB | "
            "est_raw_source_MiB | rows | error |",
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for cell in board.cells:
        err = cell.error.replace("|", "\\|") if cell.error else ""
        est_mib = (cell.source_rows * bytes_per_row_estimate(cell.width)) / (1024 * 1024)  # type: ignore[arg-type]
        seed_s = next((s.seconds for s in cell.stages if s.name == "seed_parquet_views"), None)
        lines.append(
            f"| {cell.source_rows} | {cell.width} | {format_bytes(cell.source_parquet_bytes)} | "
            f"{_fmt(seed_s)} | {_fmt(cell.ctas_baseline_s)} | {_fmt(cell.overwrite_s)} | "
            f"{_fmt(cell.wall_total_s)} | {cell.rss_before_overwrite_kb} | {cell.rss_peak_kb} | "
            f"{cell.rss_delta_kb} | {est_mib:.1f} | "
            f"{cell.row_count_after if cell.row_count_after is not None else ''} | {err} |"
        )

    lines.extend(
        [
            "",
            "## RSS scaling notes (OTH-004)",
            "",
        ]
    )
    # Compare 1M vs 10M per width when both present.
    for width in board.widths:
        cells = [cell for cell in board.cells if cell.width == width and cell.error is None]
        if len(cells) < 2:
            for cell in cells:
                lines.append(
                    f"- {width} source={cell.source_rows}: peak={cell.rss_peak_kb} KiB "
                    f"delta={cell.rss_delta_kb} KiB overwrite={_fmt(cell.overwrite_s)}s"
                )
            continue
        cells_sorted = sorted(cells, key=lambda cell: cell.source_rows)
        small, large = cells_sorted[0], cells_sorted[-1]
        row_ratio = large.source_rows / small.source_rows if small.source_rows else 0.0
        peak_ratio = large.rss_peak_kb / small.rss_peak_kb if small.rss_peak_kb > 0 else 0.0
        delta_ratio = large.rss_delta_kb / small.rss_delta_kb if small.rss_delta_kb > 0 else 0.0
        growth = (
            "consistent with growth"
            if peak_ratio > 1.2 or delta_ratio > 1.5
            else "weak/noisy signal on process ru_maxrss"
        )
        lines.append(
            f"- {width}: source rows {small.source_rows}->{large.source_rows} "
            f"({row_ratio:.1f}x); peak RSS {small.rss_peak_kb}->{large.rss_peak_kb} KiB "
            f"({peak_ratio:.2f}x); delta {small.rss_delta_kb}->{large.rss_delta_kb} KiB "
            f"({delta_ratio:.2f}x). OTH-004 claims O(source) materialize - {growth}."
        )

    lines.extend(
        [
            "",
            "## Out of scope",
            "",
            "- No product fix for OTH-004 (measurement only; r23 seed if indicted).",
            "- No empty-overwrite wipe path timed here.",
            "",
        ]
    )
    return "\n".join(lines)


def _fmt(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.3f}"


def board_to_dict(board: OverwriteBoard) -> dict[str, Any]:
    return {
        "row_counts": board.row_counts,
        "widths": board.widths,
        "environment": board.environment,
        "findings": board.findings,
        "release_build_disclosed": board.release_build_disclosed,
        "pinned_knobs": board.pinned_knobs,
        "cells": [
            {
                **{key: value for key, value in asdict(cell).items() if key != "stages"},
                "stages": [{"name": stage.name, "seconds": stage.seconds} for stage in cell.stages],
                "overwrite_s": cell.overwrite_s,
                "ctas_baseline_s": cell.ctas_baseline_s,
            }
            for cell in board.cells
        ],
    }


def write_overwrite_json(board: OverwriteBoard, path: Path) -> None:
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(board_to_dict(board), indent=2, sort_keys=True) + "\n")
