#!/usr/bin/env python3
"""dynamicFlatten measurement CLI (local filesystem only; never AWS)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dynflatten.measure import render_markdown, run_measurement


def build_parser() -> argparse.ArgumentParser:
    """CLI parser for the dynamicFlatten measurement driver."""
    parser = argparse.ArgumentParser(description="dynamicFlatten measurement (no engine edits).")
    parser.add_argument("--scale", choices=("gate", "quick", "full"), default="gate")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("/tmp/oc-dynflatten-bed"),
        help="bed directory (parquet written once per run)",
    )
    parser.add_argument("--json", type=Path, default=None, help="JSON RunResult path")
    parser.add_argument("--report", type=Path, default=None, help="markdown report path")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--skip-pyspark", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the battery and write JSON / markdown."""
    parser = build_parser()
    args = parser.parse_args(argv)
    result = run_measurement(
        scale=args.scale,
        out_dir=args.out,
        seed=args.seed,
        warmup=args.warmup,
        iterations=args.iterations,
        skip_pyspark=args.skip_pyspark,
    )
    json_path = args.json if args.json is not None else args.out / "run.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(render_markdown(result), encoding="utf-8")
    print(
        f"DYNFLATTEN_DONE scale={result.scale} wall_s={result.wall_seconds:.1f} "
        f"fixtures={len(result.fixtures)} out={args.out} json={json_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
