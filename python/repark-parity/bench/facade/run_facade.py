#!/usr/bin/env python3
"""Facade-boundary measurement CLI (local filesystem only; never AWS)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from facade.measure import CELL_GROUPS, render_markdown, run_battery


def build_parser() -> argparse.ArgumentParser:
    """CLI parser for the facade-boundary measurement driver."""
    parser = argparse.ArgumentParser(description="facade boundary measurement (no engine edits).")
    parser.add_argument(
        "--out", type=Path, default=Path("/tmp/oc-facade-bed"), help="fixture directory"
    )
    parser.add_argument(
        "--cells",
        default=",".join(CELL_GROUPS),
        help=f"comma-separated cell groups from {','.join(CELL_GROUPS)}",
    )
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--floor-repeats", type=int, default=5)
    parser.add_argument("--json", type=Path, default=None, help="JSON run record path")
    parser.add_argument("--report", type=Path, default=None, help="markdown report path")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the battery, print the table, and write JSON / markdown."""
    args = build_parser().parse_args(argv)
    groups = tuple(name.strip() for name in args.cells.split(",") if name.strip())
    unknown = [name for name in groups if name not in CELL_GROUPS]
    if unknown:
        parser_error = f"unknown cell group(s): {', '.join(unknown)}; choose from {CELL_GROUPS}"
        raise SystemExit(parser_error)
    result = run_battery(
        bed=args.out,
        groups=groups,
        iterations=args.iterations,
        floor_repeats=args.floor_repeats,
    )
    report = render_markdown(result)
    print(report)
    json_path = args.json if args.json is not None else args.out / "run.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report, encoding="utf-8")
    print(
        f"FACADE_DONE cells={len(result['cells'])} wall_s={result['wall_seconds']:.1f} "
        f"out={args.out} json={json_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
