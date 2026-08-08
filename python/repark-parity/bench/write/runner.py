"""Write-path bench matrix: CTAS + append over local-fs Iceberg (R-WRITE-BENCH).

Axes
----
1. ``repark.write.max-concurrent-files`` K in {1, 2, 4, 8, 16}
2. ``write.target-file-size-bytes`` (2-3 values; default 64 / 256 / 512 MiB)

Metrics per cell: wall total + per-stage timings, peak RSS (Linux ``ru_maxrss`` KiB),
warehouse bytes, data-file count under the local warehouse.

**No MERGE.** **No AWS.** Local FS stands in for S3 - upload-latency conclusions are
bounded and must be disclosed in every report.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import resource
import shutil
import statistics
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Final

from .datagen import DEFAULT_SOURCE_TABLE, ensure_source_parquet

LOGGER = logging.getLogger(__name__)

# Operator must assert a release wheel (debug-wheel trap). See probe_release_build().
RELEASE_ASSERT_ENV: Final[str] = "REPARK_WRITE_BENCH_RELEASE"

# Defaults match the charter (K sweep) and Iceberg / acceptance file-size knobs.
DEFAULT_K_VALUES: Final[tuple[int, ...]] = (1, 2, 4, 8, 16)
# 64 MiB (more rolls under SF1), 256 MiB (acceptance default), 512 MiB (Iceberg default).
DEFAULT_FILE_SIZE_BYTES: Final[tuple[int, ...]] = (
    64 * 1024 * 1024,
    256 * 1024 * 1024,
    512 * 1024 * 1024,
)
CATALOG: Final[str] = "write_bench"
NAMESPACE: Final[str] = "bench"
SOURCE_VIEW: Final[str] = "src_lineitem"

# Stall decision thresholds (local-fs encode path only - see verdict text).
# Speedup of best-K vs K=1 for CTAS wall.
STALL_SPEEDUP_THRESHOLD: Final[float] = 1.15  # <15% gain -> no clear K benefit
# Plateau: K=16 not better than best of K=2..8 by this ratio -> may be stalled.
PLATEAU_RATIO: Final[float] = 1.05


@dataclass
class StageTiming:
    """One named stage wall-clock sample (seconds)."""

    name: str
    seconds: float


@dataclass
class CellResult:
    """One (K, target_file_size) matrix cell."""

    concurrency: int
    target_file_size_bytes: int
    stages: list[StageTiming]
    wall_total_s: float
    rss_peak_kb: int
    warehouse_bytes: int
    data_file_count: int
    row_count_after_append: int | None
    error: str | None = None
    repeats: int = 1

    @property
    def ctas_s(self) -> float | None:
        return _stage_seconds(self.stages, "ctas")

    @property
    def append_s(self) -> float | None:
        return _stage_seconds(self.stages, "append")

    @property
    def seed_s(self) -> float | None:
        return _stage_seconds(self.stages, "seed_parquet_view")


@dataclass
class MatrixBoard:
    """Full K x file-size scoreboard + environment disclosure."""

    scale_factor: float
    source_table: str
    source_parquet: str
    source_bytes: int
    k_values: list[int]
    file_size_bytes: list[int]
    cells: list[CellResult] = field(default_factory=list)
    environment: dict[str, str] = field(default_factory=dict)
    findings: list[str] = field(default_factory=list)
    verdict: str = ""
    release_build_disclosed: bool = True


def _stage_seconds(stages: Sequence[StageTiming], name: str) -> float | None:
    for stage in stages:
        if stage.name == name:
            return stage.seconds
    return None


def max_rss_kb() -> int:
    """Peak RSS of this process in kibibytes (Linux ``ru_maxrss`` unit)."""
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def probe_release_build(*, assert_release: bool = False) -> tuple[bool, str]:
    """Whether the operator asserted a release wheel for timed runs.

    The extension module cannot be reliably classified debug vs release from Python
    alone (symbols often remain). Honesty rule: report ``True`` only when the
    operator asserts via ``--assert-release`` or env ``REPARK_WRITE_BENCH_RELEASE=1``
    after ``maturin develop --release``.

    Returns:
        ``(disclosed_true, reason)`` - ``disclosed_true`` is only True on assertion.
    """
    if assert_release:
        return True, "operator --assert-release (expected after maturin develop --release)"
    raw = os.environ.get(RELEASE_ASSERT_ENV, "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return (
            True,
            f"operator {RELEASE_ASSERT_ENV}={raw} (expected after maturin develop --release)",
        )
    return (
        False,
        "UNVERIFIED - run `maturin develop --release` then pass --assert-release "
        f"or export {RELEASE_ASSERT_ENV}=1 before timed runs (debug-wheel trap)",
    )


def source_row_count(source_parquet: Path) -> int:
    """Row count of the source parquet (metadata only; no full scan).

    Raises:
        RuntimeError: unreadable parquet or non-positive row count.
    """
    import pyarrow.parquet as pq

    path = Path(source_parquet)
    metadata = pq.read_metadata(path)
    rows = int(metadata.num_rows)
    if rows <= 0:
        msg = f"source parquet has non-positive row count: {path} rows={rows}"
        raise RuntimeError(msg)
    return rows


def expected_rows_after_ctas_append(source_rows: int) -> int:
    """CTAS copies source once; INSERT appends the same source -> 2x rows."""
    return source_rows * 2


def parse_file_size(text: str) -> int:
    """Parse ``64MiB`` / ``256MB`` / ``512m`` / raw integer bytes.

    Raises:
        ValueError: empty or unparsable size string.
    """
    raw = text.strip().replace("_", "")
    if not raw:
        msg = "empty file-size string"
        raise ValueError(msg)
    lower = raw.lower()
    multipliers: dict[str, int] = {
        "kib": 1024,
        "kb": 1000,
        "mib": 1024 * 1024,
        "mb": 1000 * 1000,
        "gib": 1024 * 1024 * 1024,
        "gb": 1000 * 1000 * 1000,
        "k": 1024,
        "m": 1024 * 1024,
        "g": 1024 * 1024 * 1024,
        "b": 1,
    }
    for suffix, mult in sorted(multipliers.items(), key=lambda item: -len(item[0])):
        if lower.endswith(suffix):
            number = lower[: -len(suffix)].strip()
            if not number:
                msg = f"missing number in file-size {text!r}"
                raise ValueError(msg)
            return int(float(number) * mult)
    return int(raw)


def format_bytes(n_bytes: int) -> str:
    """Human-readable binary size (e.g. ``256 MiB``)."""
    if n_bytes >= 1024 * 1024 * 1024:
        return f"{n_bytes / (1024 * 1024 * 1024):.2f} GiB"
    if n_bytes >= 1024 * 1024:
        return f"{n_bytes / (1024 * 1024):.1f} MiB"
    if n_bytes >= 1024:
        return f"{n_bytes / 1024:.1f} KiB"
    return f"{n_bytes} B"


def _warehouse_stats(warehouse: Path) -> tuple[int, int]:
    """Return (total_bytes, data_file_count) under a local Iceberg warehouse.

    Data files are counted as ``*.parquet`` under the warehouse (metadata JSON excluded).
    """
    total = 0
    data_files = 0
    if not warehouse.is_dir():
        return 0, 0
    for path in warehouse.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        size = path.stat().st_size
        total += size
        if path.suffix.lower() == ".parquet":
            data_files += 1
    return total, data_files


def _fresh_warehouse(root: Path, label: str) -> Path:
    """Create an empty warehouse directory for one cell (idempotent wipe)."""
    path = root / label
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_one_cell(
    *,
    source_parquet: Path,
    concurrency: int,
    target_file_size_bytes: int,
    warehouse_root: Path,
    cell_label: str,
    expected_row_count: int | None = None,
) -> CellResult:
    """Run seed -> CTAS -> INSERT append for one (K, file-size) cell.

    Uses a fresh local memory-catalog warehouse. Never sets AWS envs.

    Stage timings cover seed/CTAS/append/count only - session build + catalog
    register are excluded from ``wall_total_s`` (constant overhead; K compare uses
    CTAS stage). When ``expected_row_count`` is set, a count mismatch becomes a
    cell error (silent partial write must not look like a successful cell).
    """
    from repark import ReparkSession

    warehouse = _fresh_warehouse(warehouse_root, cell_label)
    stages: list[StageTiming] = []
    error: str | None = None
    row_count: int | None = None
    rss_before = max_rss_kb()

    builder = ReparkSession.builder.appName(
        f"write-bench-k{concurrency}-{target_file_size_bytes}"
    ).config("repark.write.max-concurrent-files", str(concurrency))
    spark = builder.getOrCreate()
    try:
        spark.register_memory_catalog(CATALOG, str(warehouse))
        # Namespace LOCATION is required so CTAS/append data land under *this* warehouse
        # (without it, repark-sql TempFallbackAllowed writes under $TMPDIR/repark_ctas/…).
        namespace_location = (warehouse / NAMESPACE).resolve()
        namespace_location.mkdir(parents=True, exist_ok=True)
        location_sql = str(namespace_location).replace("'", "''")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE} LOCATION '{location_sql}'")
        table_fq = f"{CATALOG}.{NAMESPACE}.lineitem"

        t0 = time.perf_counter()
        spark.read.parquet(str(source_parquet)).createOrReplaceTempView(SOURCE_VIEW)
        stages.append(StageTiming("seed_parquet_view", time.perf_counter() - t0))

        props = (
            f"'format-version' = '2', 'write.target-file-size-bytes' = '{target_file_size_bytes}'"
        )
        t0 = time.perf_counter()
        spark.sql(
            f"CREATE TABLE {table_fq} USING iceberg "
            f"TBLPROPERTIES ({props}) "
            f"AS SELECT * FROM {SOURCE_VIEW}"
        )
        stages.append(StageTiming("ctas", time.perf_counter() - t0))

        t0 = time.perf_counter()
        # INSERT INTO is DataFusion/fork TableProvider passthrough - session K is
        # load-bearing on the CTAS path (repark-sql write_ctas_stream); append wall
        # is still timed but K effect on INSERT is not guaranteed (see findings).
        spark.sql(f"INSERT INTO {table_fq} SELECT * FROM {SOURCE_VIEW}")
        stages.append(StageTiming("append", time.perf_counter() - t0))

        t0 = time.perf_counter()
        count_rows = spark.sql(f"SELECT count(*) AS c FROM {table_fq}").collect()
        row_count = int(count_rows[0][0])
        stages.append(StageTiming("count_verify", time.perf_counter() - t0))
        if expected_row_count is not None and row_count != expected_row_count:
            error = (
                f"row_count_mismatch: got {row_count} expected {expected_row_count} "
                f"(CTAS+append should be 2x source)"
            )
            LOGGER.error("cell K=%s file_size=%s %s", concurrency, target_file_size_bytes, error)
    except Exception as exc:
        # Bench cell error recorded on the cell; matrix continues.
        error = f"{type(exc).__name__}: {exc}"
        LOGGER.exception("cell K=%s file_size=%s failed", concurrency, target_file_size_bytes)
    finally:
        try:
            spark.stop()
        except Exception:
            LOGGER.warning("spark.stop failed for cell %s", cell_label, exc_info=True)

    wall_total = sum(stage.seconds for stage in stages)
    warehouse_bytes, data_file_count = _warehouse_stats(warehouse)
    rss_peak = max(max_rss_kb(), rss_before)
    return CellResult(
        concurrency=concurrency,
        target_file_size_bytes=target_file_size_bytes,
        stages=stages,
        wall_total_s=wall_total,
        rss_peak_kb=rss_peak,
        warehouse_bytes=warehouse_bytes,
        data_file_count=data_file_count,
        row_count_after_append=row_count,
        error=error,
    )


def _median_cell(cells: Sequence[CellResult]) -> CellResult:
    """Collapse repeats into a median-wall cell (RSS = max of repeats)."""
    if len(cells) == 1:
        return cells[0]
    first = cells[0]
    stage_names = [stage.name for stage in first.stages]
    median_stages: list[StageTiming] = []
    for name in stage_names:
        values = [_stage_seconds(cell.stages, name) for cell in cells]
        present = [value for value in values if value is not None]
        if present:
            median_stages.append(StageTiming(name, statistics.median(present)))
    walls = [cell.wall_total_s for cell in cells]
    return CellResult(
        concurrency=first.concurrency,
        target_file_size_bytes=first.target_file_size_bytes,
        stages=median_stages,
        wall_total_s=statistics.median(walls),
        rss_peak_kb=max(cell.rss_peak_kb for cell in cells),
        warehouse_bytes=int(statistics.median([cell.warehouse_bytes for cell in cells])),
        data_file_count=int(statistics.median([cell.data_file_count for cell in cells])),
        row_count_after_append=first.row_count_after_append,
        error=next((cell.error for cell in cells if cell.error), None),
        repeats=len(cells),
    )


def run_matrix(
    *,
    scale_factor: float = 1.0,
    k_values: Sequence[int] = DEFAULT_K_VALUES,
    file_size_bytes: Sequence[int] = DEFAULT_FILE_SIZE_BYTES,
    data_root: Path | None = None,
    warehouse_root: Path | None = None,
    source_table: str = DEFAULT_SOURCE_TABLE,
    repeats: int = 1,
    assert_release: bool = False,
) -> MatrixBoard:
    """Run the full K x file-size matrix; return a scoreboard with verdict."""
    if repeats < 1:
        msg = f"repeats must be >= 1; got {repeats}"
        raise ValueError(msg)
    for k_value in k_values:
        if k_value < 1:
            msg = f"concurrency K must be >= 1; got {k_value}"
            raise ValueError(msg)
    for size in file_size_bytes:
        if size < 1:
            msg = f"target file size must be >= 1 byte; got {size}"
            raise ValueError(msg)

    source_path = ensure_source_parquet(scale_factor, data_root=data_root, table=source_table)
    source_bytes = source_path.stat().st_size
    source_rows = source_row_count(source_path)
    expected_rows = expected_rows_after_ctas_append(source_rows)

    if warehouse_root is None:
        cache_home = Path.home() / ".cache" / "repark-write-bench"
        warehouse_root = cache_home / f"sf{_sf_label(scale_factor)}"
    warehouse_root = warehouse_root.expanduser()
    warehouse_root.mkdir(parents=True, exist_ok=True)

    release_ok, release_reason = probe_release_build(assert_release=assert_release)
    board = MatrixBoard(
        scale_factor=scale_factor,
        source_table=source_table,
        source_parquet=str(source_path),
        source_bytes=source_bytes,
        k_values=list(k_values),
        file_size_bytes=list(file_size_bytes),
        environment=_environment_snapshot(),
        release_build_disclosed=release_ok,
    )
    board.environment["source_rows"] = str(source_rows)
    board.environment["expected_rows_after_ctas_append"] = str(expected_rows)
    board.environment["release_build_reason"] = release_reason
    board.findings.append(
        "Local-fs object store stands in for S3 - upload-latency / flush-stall "
        "conclusions are BOUNDED (no AWS this unit). S3 leg is a follow-up."
    )
    board.findings.append(
        "maturin develop --release required before timed runs (debug-wheel trap). "
        f"Release disclosed={release_ok}: {release_reason}"
    )
    board.findings.append(
        "K axis (`repark.write.max-concurrent-files`) is load-bearing on the CTAS path "
        "(repark-sql write_ctas_stream -> concurrency_from_ctx). Plain INSERT INTO is "
        "DataFusion/fork TableProvider passthrough - append wall is timed but K effect "
        "on INSERT is NOT guaranteed. Verdict uses CTAS wall only."
    )
    board.findings.append(
        "Peak RSS is process-lifetime `ru_maxrss` (Linux KiB) and is non-decreasing "
        "across sequential cells - per-cell RSS is not an independent sample."
    )
    board.findings.append(
        f"Row integrity: after CTAS+append expect {expected_rows} rows "
        f"(2x source_rows={source_rows}); mismatch marks the cell error."
    )
    if not release_ok:
        LOGGER.warning("release build UNVERIFIED: %s", release_reason)

    for size in file_size_bytes:
        for k_value in k_values:
            label = f"k{k_value}_fs{size}"
            LOGGER.info(
                "cell start K=%s target_file_size=%s (%s) sf=%s",
                k_value,
                size,
                format_bytes(size),
                scale_factor,
            )
            repeat_cells: list[CellResult] = []
            for rep in range(repeats):
                cell = run_one_cell(
                    source_parquet=source_path,
                    concurrency=k_value,
                    target_file_size_bytes=size,
                    warehouse_root=warehouse_root,
                    cell_label=f"{label}_r{rep}",
                    expected_row_count=expected_rows,
                )
                repeat_cells.append(cell)
                LOGGER.info(
                    "cell done K=%s fs=%s rep=%s wall=%.3fs ctas=%.3fs append=%.3fs "
                    "rss_kb=%s data_files=%s error=%s",
                    k_value,
                    size,
                    rep,
                    cell.wall_total_s,
                    cell.ctas_s or -1.0,
                    cell.append_s or -1.0,
                    cell.rss_peak_kb,
                    cell.data_file_count,
                    cell.error,
                )
            board.cells.append(_median_cell(repeat_cells))

    board.verdict = compute_verdict(board)
    return board


def _sf_label(scale_factor: float) -> str:
    if scale_factor == int(scale_factor):
        return str(int(scale_factor))
    return f"{scale_factor:g}"


def _environment_snapshot() -> dict[str, str]:
    return {
        "machine": platform.node(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "processor": platform.processor() or "unknown",
    }


def compute_verdict(board: MatrixBoard) -> str:
    """Stall-or-not verdict per R-WRITE-BENCH seed decision tree (local-fs only).

    Decision tree (seed / todo.md R-WRITE-BENCH)::

        (1) Measure K sweep wall + stages + RSS (this report).
        (2) If stalls / no scaling on the write path -> tune knobs
            (AsyncArrowWriter buffer / row-group size + raise K default) first.
        (3) ONLY if numbers still demand it -> fork-side encode↔upload pipeline.

    Local FS cannot prove S3 upload stalls; verdict classifies encode-side
    scaling only and routes the seed tree for the orchestrator.
    """
    ok_cells = [cell for cell in board.cells if cell.error is None and cell.ctas_s is not None]
    if not ok_cells:
        return (
            "VERDICT: NO_DATA - every cell errored; cannot classify stall. "
            "Do not proceed to knob tune or pipeline until the harness runs green."
        )

    lines: list[str] = []
    # Per file-size: CTAS wall vs K
    by_size: dict[int, list[CellResult]] = {}
    for cell in ok_cells:
        by_size.setdefault(cell.target_file_size_bytes, []).append(cell)

    size_summaries: list[str] = []
    # Per file-size flags (avoid whole-matrix overclaim when only one size scales).
    clear_scaling_flags: list[bool] = []
    plateau_flags: list[bool] = []
    for size, cells in sorted(by_size.items()):
        by_k = {cell.concurrency: cell for cell in cells}
        if 1 not in by_k:
            size_summaries.append(f"{format_bytes(size)}: missing K=1 baseline")
            clear_scaling_flags.append(False)
            continue
        baseline = by_k[1].ctas_s or by_k[1].wall_total_s
        best_k = 1
        best_wall = baseline
        for k_value, cell in by_k.items():
            wall = cell.ctas_s if cell.ctas_s is not None else cell.wall_total_s
            if wall < best_wall:
                best_wall = wall
                best_k = k_value
        speedup = baseline / best_wall if best_wall > 0 else 0.0
        high_k = max(by_k)
        high_wall = by_k[high_k].ctas_s or by_k[high_k].wall_total_s
        clear = speedup >= STALL_SPEEDUP_THRESHOLD
        clear_scaling_flags.append(clear)
        # Plateau: high K not better than best by PLATEAU_RATIO
        plateau = best_k < high_k and high_wall > best_wall * PLATEAU_RATIO
        if clear and plateau:
            plateau_flags.append(True)
        size_summaries.append(
            f"{format_bytes(size)}: K=1 CTAS={baseline:.3f}s best=K{best_k} "
            f"CTAS={best_wall:.3f}s speedup={speedup:.2f}x; K={high_k} CTAS={high_wall:.3f}s"
        )

    lines.append("Per target-file-size (CTAS wall scaling):")
    lines.extend(f"  - {summary}" for summary in size_summaries)

    any_clear_scaling = any(clear_scaling_flags)
    all_clear_scaling = bool(clear_scaling_flags) and all(clear_scaling_flags)
    any_plateau_at_high_k = any(plateau_flags)

    # Append vs CTAS ratio (serial commit overhead vs write)
    append_ratios: list[float] = []
    for cell in ok_cells:
        if cell.ctas_s and cell.append_s and cell.ctas_s > 0:
            append_ratios.append(cell.append_s / cell.ctas_s)
    if append_ratios:
        med_ratio = statistics.median(append_ratios)
        lines.append(
            f"Median append/CTAS wall ratio = {med_ratio:.2f} "
            f"(append reuses open table; ratio ~1 expected if both are write-bound)."
        )

    # RSS note
    rss_values = [cell.rss_peak_kb for cell in ok_cells]
    lines.append(
        f"Peak RSS across cells: min={min(rss_values)} KiB max={max(rss_values)} KiB "
        f"(process cumulative ru_maxrss; non-decreasing across sequential cells; no auto-abort)."
    )

    # Decision tree routing - NO_STALL requires clear scaling on EVERY file-size group
    # (mixed size-axis results route to PARTIAL to avoid whole-matrix overclaim).
    lines.append("")
    lines.append("Seed decision-tree routing (local-fs encode path only):")
    if all_clear_scaling and not any_plateau_at_high_k:
        lines.append(
            "VERDICT: NO_STALL_ON_LOCAL_FS - CTAS wall improves with K on every file-size "
            "group and high-K does not regress. Encode-side concurrency is productive on "
            "local disk. Step (2) knob-tune is NOT forced by these numbers. "
            "Step (3) fork encode↔upload pipeline is NOT indicated from local-fs alone. "
            "S3 upload-overlap stall remains UNMEASURED (follow-up)."
        )
    elif any_clear_scaling:
        # Includes: all-clear+plateau, or mixed clear/no-clear across file sizes.
        detail = (
            "high-K plateau/regression on at least one file-size group"
            if any_plateau_at_high_k
            else "clear K gain on some file-size groups but not all"
        )
        lines.append(
            "VERDICT: PARTIAL_SCALING_PLATEAU - "
            f"{detail} on local FS. Step (2) cheap knob tune (buffer / row-group / K default) "
            "is the next lever IF product wants more; step (3) pipeline NOT yet indicated. "
            "S3 may shift the plateau - follow-up."
        )
    else:
        need_pct = round((STALL_SPEEDUP_THRESHOLD - 1.0) * 100)
        lines.append(
            "VERDICT: NO_K_BENEFIT_ON_LOCAL_FS - CTAS wall does not improve by >="
            f"{need_pct}% (speedup < {STALL_SPEEDUP_THRESHOLD:.2f}x) with K>1 "
            "on any file-size group "
            "(encode-bound or single-file path on local FS). "
            "This does NOT prove S3 flush/upload stall (local FS hides network latency). "
            "Step (2) knob tune is the cheap next experiment IF product prioritizes write "
            "throughput; step (3) fork pipeline stays gated on S3-leg evidence + post-tune "
            "numbers. Do not change K default from this local-only matrix."
        )
    return "\n".join(lines)


def board_to_dict(board: MatrixBoard) -> dict[str, Any]:
    """JSON-serializable scoreboard."""
    return {
        "scale_factor": board.scale_factor,
        "source_table": board.source_table,
        "source_parquet": board.source_parquet,
        "source_bytes": board.source_bytes,
        "k_values": board.k_values,
        "file_size_bytes": board.file_size_bytes,
        "environment": board.environment,
        "findings": board.findings,
        "verdict": board.verdict,
        "release_build_disclosed": board.release_build_disclosed,
        "cells": [
            {
                **{key: value for key, value in asdict(cell).items() if key != "stages"},
                "stages": [{"name": stage.name, "seconds": stage.seconds} for stage in cell.stages],
                "ctas_s": cell.ctas_s,
                "append_s": cell.append_s,
                "seed_s": cell.seed_s,
            }
            for cell in board.cells
        ],
    }


def render_markdown_report(board: MatrixBoard, *, title: str | None = None) -> str:
    """Render the human write-bench report (tables + verdict + scale disclosure)."""
    sf = board.scale_factor
    sf_label = _sf_label(sf)
    header = title or f"R-WRITE-BENCH - SF{sf_label} local-fs CTAS+append matrix"
    lines: list[str] = [
        f"# {header}",
        "",
        f"**Scale factor: SF{sf_label}** (disclosed in every table header below).",
        "",
        "## Environment",
        "",
        f"- Machine: `{board.environment.get('machine', '?')}`",
        f"- Platform: `{board.environment.get('platform', '?')}`",
        f"- Python: `{board.environment.get('python', '?')}`",
        f"- Source: `{board.source_table}` @ `{board.source_parquet}` "
        f"({format_bytes(board.source_bytes)})",
        f"- K axis: {board.k_values}",
        "- `write.target-file-size-bytes` axis: "
        + ", ".join(f"{size} ({format_bytes(size)})" for size in board.file_size_bytes),
        f"- Release build disclosed: **{board.release_build_disclosed}** "
        f"(`maturin develop --release` first - debug-wheel trap"
        + (
            f"; {board.environment.get('release_build_reason', '')}"
            if board.environment.get("release_build_reason")
            else ""
        )
        + ").",
        f"- Source rows: `{board.environment.get('source_rows', '?')}` -> expected after "
        f"CTAS+append: `{board.environment.get('expected_rows_after_ctas_append', '?')}`",
        "- Object store: **local filesystem** (memory catalog warehouse). "
        "**No AWS / no S3 / no Glue.**",
        "- `wall_total_s` = sum of seed/CTAS/append/count stages (excludes session build).",
        "",
        "## Findings / disclosures",
        "",
    ]
    for finding in board.findings:
        lines.append(f"- {finding}")
    lines.extend(["", f"## Matrix - SF{sf_label} wall + stages + RSS", ""])

    # Wide table: one row per cell
    lines.append(
        "| SF | K | target_file_size | seed_s | ctas_s | append_s | count_s | "
        "wall_total_s | rss_peak_KiB | warehouse_bytes | data_files | rows | error |"
    )
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for cell in board.cells:
        count_s = _stage_seconds(cell.stages, "count_verify")
        err = cell.error.replace("|", "\\|") if cell.error else ""
        lines.append(
            f"| {sf_label} | {cell.concurrency} | {format_bytes(cell.target_file_size_bytes)} | "
            f"{_fmt(cell.seed_s)} | {_fmt(cell.ctas_s)} | {_fmt(cell.append_s)} | "
            f"{_fmt(count_s)} | {_fmt(cell.wall_total_s)} | {cell.rss_peak_kb} | "
            f"{cell.warehouse_bytes} | {cell.data_file_count} | "
            f"{cell.row_count_after_append if cell.row_count_after_append is not None else ''} | "
            f"{err} |"
        )

    # Pivot: CTAS seconds by K x file size
    lines.extend(["", f"## CTAS wall (seconds) - SF{sf_label} pivot K x file-size", ""])
    sizes = board.file_size_bytes
    lines.append("| SF | K \\ file-size | " + " | ".join(format_bytes(s) for s in sizes) + " |")
    lines.append("|---:|---:|" + "|".join(["---:" for _ in sizes]) + "|")
    for k_value in board.k_values:
        row = [f"| {sf_label} | {k_value} |"]
        for size in sizes:
            cell = _find_cell(board, k_value, size)
            if cell is None or cell.ctas_s is None:
                row.append(" - |")
            else:
                row.append(f" {cell.ctas_s:.3f} |")
        lines.append("".join(row))

    lines.extend(["", f"## Append wall (seconds) - SF{sf_label} pivot K x file-size", ""])
    lines.append("| SF | K \\ file-size | " + " | ".join(format_bytes(s) for s in sizes) + " |")
    lines.append("|---:|---:|" + "|".join(["---:" for _ in sizes]) + "|")
    for k_value in board.k_values:
        row = [f"| {sf_label} | {k_value} |"]
        for size in sizes:
            cell = _find_cell(board, k_value, size)
            if cell is None or cell.append_s is None:
                row.append(" - |")
            else:
                row.append(f" {cell.append_s:.3f} |")
        lines.append("".join(row))

    lines.extend(
        [
            "",
            "## Verdict (seed decision tree)",
            "",
            "```",
            board.verdict,
            "```",
            "",
            "## Out of scope / non-conclusions",
            "",
            "- No engine, fork, or knob-default changes were made.",
            "- No claim about S3 PUT latency or Glue commit cost.",
            "- No go/no-go on fork encode↔upload pipelining beyond the verdict routing above.",
            "- MERGE was not exercised (charter: append + CTAS only).",
            "- K session conf is proven on the CTAS path only; INSERT INTO K effect is not "
            "guaranteed (fork TableProvider passthrough).",
            "",
        ]
    )
    return "\n".join(lines)


def _find_cell(board: MatrixBoard, concurrency: int, file_size: int) -> CellResult | None:
    for cell in board.cells:
        if cell.concurrency == concurrency and cell.target_file_size_bytes == file_size:
            return cell
    return None


def _fmt(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.3f}"


def write_json(board: MatrixBoard, path: Path) -> None:
    """Write scoreboard JSON atomically-ish (write then replace)."""
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(board_to_dict(board), indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")
