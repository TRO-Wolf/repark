#!/usr/bin/env python3
"""W-0 window-shape bench CLI (local filesystem only; never AWS).

Usage::

    python python/repark-parity/bench/windows/run_w0.py \\
        --scale quick --scratch /tmp/w0-scratch \\
        --out /tmp/w0.json --report task/window-bench-report-2026-08-31.md
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from windows.measure import run_window_measurement
from windows.report import render_markdown
from windows.roster import DEFAULT_SEED, FULL_UNPARTITIONED_ROWS


def build_parser() -> argparse.ArgumentParser:
    """CLI parser for the W-0 driver."""
    parser = argparse.ArgumentParser(description="W-0 window-shape measurement (no engine edits).")
    parser.add_argument(
        "--scale",
        choices=("quick", "full", "gate"),
        default="quick",
        help="quick is CI-adjacent; full is the charter 1e7 unpartitioned cell",
    )
    parser.add_argument(
        "--scratch", type=Path, required=True, help="working directory for generated data"
    )
    parser.add_argument("--out", type=Path, required=True, help="JSON RunResult path")
    parser.add_argument("--report", type=Path, default=None, help="optional markdown report path")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--keep-scratch", action="store_true", help="leave generated files in place"
    )
    parser.add_argument("--skip-duckdb", action="store_true")
    parser.add_argument("--skip-pyspark", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the battery and write JSON / markdown.

    Args:
        argv: optional CLI arguments.

    Returns:
        Process exit code (0 on a completed record, including recorded crashes).
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.scale == "full" and FULL_UNPARTITIONED_ROWS != 10_000_000:
        parser.error("full-scale unpartitioned default drifted from 10_000_000")
    result = run_window_measurement(
        args.scratch,
        scale=args.scale,
        seed=args.seed,
        keep_scratch=args.keep_scratch,
        skip_duckdb=args.skip_duckdb,
        skip_pyspark=args.skip_pyspark,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(render_markdown(result), encoding="utf-8")
    refuse = [row.name for row in result.probe if row.outcome == "refuse"]
    print(
        f"W0_DONE scale={result.scale} wall_s={result.wall_seconds:.1f} "
        f"refuse={len(refuse)} scratch_deleted={result.scratch_deleted} out={args.out}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
