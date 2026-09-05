"""Render the measured H3-SPILL-1 cells as the markdown tables the baseline doc carries."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from spill.models import CellRecord, MatrixReport  # noqa: E402
from spill.roster import POOLS, ROSTER  # noqa: E402

SHORT: dict[str, str] = {
    "ok": "ok",
    "spilled": "spill",
    "degraded": "degr",
    "clean_error": "clean-err",
    "abort": "ABORT",
    "abort_at_cap": "ABORT-CAP",
    "internal_error": "PANIC",
    "timeout": "TIMEOUT",
    "wrong": "WRONG",
    "error": "ERR",
}


def mib(value: int | None) -> str:
    """Render bytes as MiB with no decimals, or a dash."""
    if not value:
        return "-"
    return f"{value / (1024 * 1024):.0f}"


def load_cells(paths: list[Path]) -> list[CellRecord]:
    """Read every lane report and return one flat, ordered cell list."""
    cells: list[CellRecord] = []
    for path in paths:
        report = MatrixReport.model_validate_json(path.read_text(encoding="utf-8"))
        cells.extend(report.cells)
    return cells


def _cell_text(record: CellRecord | None) -> str:
    """One outcome-matrix cell: the short outcome plus the number that earned it."""
    if record is None:
        return "—"
    short = SHORT.get(record.outcome, record.outcome)
    if record.outcome == "spilled":
        return f"{short} {record.spill_count}/{mib(record.spilled_bytes)}M"
    if record.outcome == "degraded":
        return f"{short} {record.degraded_rows / 1e6:.1f}M"
    return short


def outcome_table(cells: list[CellRecord], scale: int) -> str:
    """The outcome matrix for one scale: operators down, pools across."""
    index = {(cell.operator, cell.pool): cell for cell in cells if cell.scale == scale}
    lines = ["| operator | " + " | ".join(POOLS) + " |", "|---|" + "---|" * len(POOLS)]
    for spec in ROSTER:
        row = [_cell_text(index.get((spec.operator, pool))) for pool in POOLS]
        lines.append(f"| `{spec.operator}` | " + " | ".join(row) + " |")
    return "\n".join(lines)


def numbers_table(cells: list[CellRecord], scale: int) -> str:
    """Peak RSS, wall, spilled bytes and load for every cell at one scale."""
    lines = [
        "| operator | pool | outcome | spills | spilled MiB | peak RSS MiB | wall ms | load |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]
    order = {pool: position for position, pool in enumerate(POOLS)}
    chosen = [cell for cell in cells if cell.scale == scale]
    chosen.sort(key=lambda cell: (cell.operator, order.get(cell.pool, 9)))
    for cell in chosen:
        lines.append(
            f"| `{cell.operator}` | {cell.pool} | {SHORT.get(cell.outcome, cell.outcome)} | "
            f"{cell.spill_count} | {mib(cell.spilled_bytes)} | {mib(cell.peak_rss_bytes)} | "
            f"{cell.wall_ms:.0f} | {cell.load_start:.0f} |"
        )
    return "\n".join(lines)


def summary(cells: list[CellRecord]) -> dict[str, int]:
    """Count cells per outcome for the ledger and the hand-back."""
    counts: dict[str, int] = {"cells": len(cells)}
    for cell in cells:
        counts[cell.outcome] = counts.get(cell.outcome, 0) + 1
    return counts


def main(argv: list[str] | None = None) -> int:
    """CLI: merge lane reports and print the tables plus the outcome census."""
    parser = argparse.ArgumentParser(description="H3-SPILL-1 report renderer")
    parser.add_argument("--reports", nargs="+", required=True)
    parser.add_argument("--scales", nargs="*", type=int, default=[1_000_000, 10_000_000])
    args = parser.parse_args(argv)
    cells = load_cells([Path(path) for path in args.reports])
    for scale in args.scales:
        print(f"\n### outcome matrix — {scale:,} rows\n")
        print(outcome_table(cells, scale))
        print(f"\n### numbers — {scale:,} rows\n")
        print(numbers_table(cells, scale))
    print("\n" + json.dumps(summary(cells), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
