"""Time PERF-CAST-1 CAST cells and write one CSV row per cell."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cast.attribute import capture_py_entry_spans, run_attribution
from cast.measure import exponent_fit, load1, measure_grid, write_cells_csv
from cast.shapes import CAST_COUNTS, ROW_COUNT, SHAPES


def parse_csv_ints(raw: str, allowed: tuple[int, ...]) -> tuple[int, ...]:
    """Parse a comma-separated int list and refuse unknown sizes."""
    values = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    unknown = [value for value in values if value not in allowed]
    if unknown:
        raise SystemExit(f"unknown CAST counts {unknown}; choose from {allowed}")
    return values


def parse_csv_shapes(raw: str) -> tuple[str, ...]:
    """Parse a comma-separated shape list and refuse unknown names."""
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    unknown = [value for value in values if value not in SHAPES]
    if unknown:
        raise SystemExit(f"unknown shapes {unknown}; choose from {SHAPES}")
    return values


def build_parser() -> argparse.ArgumentParser:
    """CLI parser for the CAST-cost measurement driver."""
    parser = argparse.ArgumentParser(description="CAST-cost measurement (no engine edits).")
    parser.add_argument("--out", type=Path, required=True, help="CSV path, one row per cell")
    parser.add_argument("--shapes", default=",".join(SHAPES), help="comma-separated plan shapes")
    parser.add_argument(
        "--sizes",
        default=",".join(str(count) for count in CAST_COUNTS),
        help="comma-separated CAST counts",
    )
    parser.add_argument("--rows", type=int, default=ROW_COUNT, help="source row count")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument(
        "--attribute",
        action="store_true",
        help="also probe the largest requested size of each shape",
    )
    parser.add_argument(
        "--attribute-json",
        type=Path,
        default=None,
        help="attribution JSON path (default: <out>.attr.json)",
    )
    parser.add_argument("--spans-log", type=Path, default=None, help="REPARK_LOG=info stderr path")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the grid, write the CSV, and optionally the attribution JSON."""
    args = build_parser().parse_args(argv)
    shapes = parse_csv_shapes(args.shapes)
    sizes = parse_csv_ints(args.sizes, CAST_COUNTS)
    print(f"CAST_COST_START load1={load1():.2f} shapes={shapes} sizes={sizes}", flush=True)
    records = measure_grid(
        shapes=shapes,
        cast_counts=sizes,
        row_count=args.rows,
        warmup=args.warmup,
        repeats=args.repeats,
    )
    write_cells_csv(args.out, records)
    for record in records:
        print(
            f"{record.shape:28s} n={record.cast_count:4d} "
            f"median={record.median_seconds:.4f}s "
            f"plan={record.median_plan_seconds:.4f}s "
            f"exec={record.median_execute_seconds:.4f}s",
            flush=True,
        )
    for shape in shapes:
        shape_rows = [record for record in records if record.shape == shape]
        if len(shape_rows) < 2:
            continue
        exponent = exponent_fit(
            [row.cast_count for row in shape_rows],
            [row.median_seconds for row in shape_rows],
        )
        plan_exponent = exponent_fit(
            [row.cast_count for row in shape_rows],
            [row.median_plan_seconds for row in shape_rows],
        )
        execute_exponent = exponent_fit(
            [row.cast_count for row in shape_rows],
            [row.median_execute_seconds for row in shape_rows],
        )
        print(
            f"exponent {shape}: total={exponent:.3f} plan={plan_exponent:.3f} "
            f"exec={execute_exponent:.3f}",
            flush=True,
        )
    if args.attribute:
        largest = max(sizes)
        attr_path = (
            args.attribute_json
            if args.attribute_json is not None
            else Path(str(args.out) + ".attr.json")
        )
        payloads = [
            run_attribution(shape=shape, cast_count=largest, row_count=args.rows).model_dump()
            for shape in shapes
        ]
        attr_path.parent.mkdir(parents=True, exist_ok=True)
        attr_path.write_text(json.dumps(payloads, indent=2), encoding="utf-8")
        print(f"CAST_COST_ATTR {attr_path}", flush=True)
        if args.spans_log is not None:
            stderr_text = capture_py_entry_spans(
                shape=shapes[0],
                cast_count=largest,
                row_count=args.rows,
            )
            args.spans_log.write_text(stderr_text, encoding="utf-8")
            print(f"CAST_COST_SPANS {args.spans_log}", flush=True)
    print(f"CAST_COST_DONE cells={len(records)} out={args.out} load1={load1():.2f}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
